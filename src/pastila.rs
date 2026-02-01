use aes_gcm::{
    Aes128Gcm, KeyInit, Nonce,
    aead::{Aead, generic_array::GenericArray},
};
use anyhow::{Result, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use clickhouse_rs::{Block, Options, Pool};
use rand::RngCore;
use regex::Regex;
use std::str::FromStr;
use url::Url;

/// ClickHouse's SipHash-2-4 implementation (128-bit version)
/// See https://github.com/ClickHouse/ClickHouse/pull/46065 for details
pub struct ClickHouseSipHash {
    v0: u64,
    v1: u64,
    v2: u64,
    v3: u64,
    cnt: u64,
    current_word: u64,
    current_bytes_len: usize,
}

impl ClickHouseSipHash {
    pub fn new() -> Self {
        Self {
            v0: 0x736f6d6570736575u64,
            v1: 0x646f72616e646f6du64,
            v2: 0x6c7967656e657261u64,
            v3: 0x7465646279746573u64,
            cnt: 0,
            current_word: 0,
            current_bytes_len: 0,
        }
    }

    #[inline]
    fn sipround(&mut self) {
        self.v0 = self.v0.wrapping_add(self.v1);
        self.v1 = self.v1.rotate_left(13);
        self.v1 ^= self.v0;
        self.v0 = self.v0.rotate_left(32);

        self.v2 = self.v2.wrapping_add(self.v3);
        self.v3 = self.v3.rotate_left(16);
        self.v3 ^= self.v2;

        self.v0 = self.v0.wrapping_add(self.v3);
        self.v3 = self.v3.rotate_left(21);
        self.v3 ^= self.v0;

        self.v2 = self.v2.wrapping_add(self.v1);
        self.v1 = self.v1.rotate_left(17);
        self.v1 ^= self.v2;
        self.v2 = self.v2.rotate_left(32);
    }

    pub fn write(&mut self, data: &[u8]) {
        for &byte in data {
            let byte_idx = self.current_bytes_len;
            self.current_word |= (byte as u64) << (byte_idx * 8);
            self.current_bytes_len += 1;
            self.cnt += 1;

            if self.current_bytes_len == 8 {
                self.v3 ^= self.current_word;
                self.sipround();
                self.sipround();
                self.v0 ^= self.current_word;

                self.current_word = 0;
                self.current_bytes_len = 0;
            }
        }
    }

    pub fn finish128(mut self) -> u128 {
        let cnt_byte = (self.cnt % 256) as u8;
        self.current_word |= (cnt_byte as u64) << 56;

        self.v3 ^= self.current_word;
        self.sipround();
        self.sipround();
        self.v0 ^= self.current_word;

        self.v2 ^= 0xff;
        self.sipround();
        self.sipround();
        self.sipround();
        self.sipround();

        let low = self.v0 ^ self.v1;
        let high = self.v2 ^ self.v3;

        ((high as u128) << 64) | (low as u128)
    }
}

pub fn calculate_hash(text: &str) -> String {
    let mut hasher = ClickHouseSipHash::new();
    hasher.write(text.as_bytes());
    let hash = hasher.finish128();
    format!("{:032x}", hash.swap_bytes())
}

pub fn get_fingerprint(text: &str) -> String {
    let re = Regex::new(r"\b\w{4,100}\b").unwrap();
    let words: Vec<&str> = re.find_iter(text).map(|m| m.as_str()).collect();

    if words.len() < 3 {
        return "ffffffff".to_string();
    }

    let mut min_hash: Option<u128> = None;

    for i in 0..words.len().saturating_sub(2) {
        let triplet = format!("{} {} {}", words[i], words[i + 1], words[i + 2]);
        let mut hasher = ClickHouseSipHash::new();
        hasher.write(triplet.as_bytes());
        let hash_value = hasher.finish128();

        min_hash = Some(min_hash.map_or(hash_value, |current| current.min(hash_value)));
    }

    let full_hash = match min_hash {
        Some(hash) => format!("{:032x}", hash.swap_bytes()),
        None => "ffffffffffffffffffffffffffffffff".to_string(),
    };
    full_hash[..8].to_string()
}

fn encrypt_content(content: &str, key: &[u8; 16]) -> Result<String> {
    let cipher = Aes128Gcm::new(GenericArray::from_slice(key));
    let nonce = Nonce::from_slice(&key[..12]);

    let ciphertext = cipher
        .encrypt(nonce, content.as_bytes())
        .map_err(|e| anyhow!("Encryption failed: {}", e))?;

    Ok(BASE64.encode(&ciphertext))
}

async fn get_pastila_client(pastila_clickhouse_host: &str) -> Result<clickhouse_rs::ClientHandle> {
    let url = {
        let http_url = Url::parse(pastila_clickhouse_host)?;
        let host = http_url
            .host_str()
            .ok_or_else(|| anyhow!("No host in pastila_clickhouse_host"))?;

        let user = if !http_url.username().is_empty() {
            http_url.username().to_string()
        } else {
            http_url
                .query_pairs()
                .find(|(k, _)| k == "user")
                .map(|(_, v)| v.to_string())
                .unwrap_or_else(|| "default".to_string())
        };

        let secure = http_url.scheme() == "https";
        let port = if secure { 9440 } else { 9000 };

        format!(
            "tcp://{}@{}:{}/?secure={}&connection_timeout=5s",
            user, host, port, secure
        )
    };
    let options = Options::from_str(&url)?;
    let pool = Pool::new(options);
    let client = pool.get_handle().await?;
    Ok(client)
}

pub async fn upload_encrypted(
    content: &str,
    pastila_clickhouse_host: &str,
    pastila_url: &str,
) -> Result<String> {
    let mut key = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut key);
    let encrypted = encrypt_content(content, &key)?;

    let fingerprint_hex = get_fingerprint(&encrypted);
    let hash_hex = calculate_hash(&encrypted);

    log::info!(
        "Uploading {} bytes ({} bytes encrypted) to {}",
        content.len(),
        encrypted.len(),
        pastila_clickhouse_host
    );

    {
        let mut client = get_pastila_client(pastila_clickhouse_host).await?;
        let block = Block::new()
            .column("fingerprint_hex", vec![fingerprint_hex.as_str()])
            .column("hash_hex", vec![hash_hex.as_str()])
            .column("content", vec![encrypted.as_str()])
            .column("is_encrypted", vec![1_u8]);
        client.insert("paste.data", block).await?;
    }

    let pastila_url = pastila_url.trim_end_matches('/');
    let key_fragment = format!("#{}", BASE64.encode(key));
    let pastila_page_url = format!(
        "{}/?{}/{}{}GCM",
        pastila_url, fingerprint_hex, hash_hex, key_fragment
    );

    Ok(pastila_page_url)
}
