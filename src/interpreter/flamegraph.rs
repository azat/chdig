use crate::interpreter::clickhouse::Columns;
use crate::utils::open_url_command;
use anyhow::{Error, Result};
use crossterm::event::{self, Event as CrosstermEvent, KeyEventKind};
use flamelens::app::{App, AppResult};
use flamelens::flame::FlameGraph;
use flamelens::handler::handle_key_events;
use flamelens::ui;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use regex::Regex;
use serde_json::json;
use std::io;
use urlencoding::encode;

/// ClickHouse's SipHash-2-4 implementation (128-bit version)
/// See https://github.com/ClickHouse/ClickHouse/pull/46065 for details
struct ClickHouseSipHash {
    v0: u64,
    v1: u64,
    v2: u64,
    v3: u64,
    cnt: u64,
    current_word: u64,
    current_bytes_len: usize,
}

impl ClickHouseSipHash {
    fn new() -> Self {
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

    fn write(&mut self, data: &[u8]) {
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

    fn finish128(mut self) -> u128 {
        // Set the last byte to cnt % 256
        let cnt_byte = (self.cnt % 256) as u8;
        self.current_word |= (cnt_byte as u64) << 56;

        self.v3 ^= self.current_word;
        self.sipround();
        self.sipround();
        self.v0 ^= self.current_word;

        // ClickHouse uses 0xff instead of 0xee
        self.v2 ^= 0xff;
        self.sipround();
        self.sipround();
        self.sipround();
        self.sipround();

        // Combine v0, v1, v2, v3 into 128-bit result
        let low = self.v0 ^ self.v1;
        let high = self.v2 ^ self.v3;

        ((high as u128) << 64) | (low as u128)
    }
}

fn calculate_hash(text: &str) -> String {
    let mut hasher = ClickHouseSipHash::new();
    hasher.write(text.as_bytes());
    let hash = hasher.finish128();
    format!("{:032x}", hash.swap_bytes())
}

fn get_fingerprint(text: &str) -> String {
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

async fn upload_to_pastila(content: &str) -> Result<String> {
    // FIXME: apparently the driver cannot work with async_insert, since the following does not
    // work (simply hangs, since server expects more data)
    //
    // const PASTILA_HOST: &str = "uzg8q0g12h.eu-central-1.aws.clickhouse.cloud";
    // const PASTILA_USER: &str = "paste";
    // let fingerprint_hex = get_fingerprint(content);
    // let hash_hex = calculate_hash(content);
    //
    // let options = Options::from_str(&format!(
    //     "tcp://{}@{}:9440/?secure=true&connection_timeout=5s",
    //     PASTILA_USER, PASTILA_HOST
    // ))?;
    // let pool = Pool::new(options);
    // let mut client = pool.get_handle().await?;
    //
    // let block = Block::new()
    //     .column("fingerprint_hex", vec![fingerprint_hex.as_str()])
    //     .column("hash_hex", vec![hash_hex.as_str()])
    //     .column("content", vec![content])
    //     .column("is_encrypted", vec![0_u8]);
    // client.insert("paste.data", block).await?;

    const PASTILA_URL: &str = "https://uzg8q0g12h.eu-central-1.aws.clickhouse.cloud/?user=paste";
    let fingerprint_hex = get_fingerprint(content);
    let hash_hex = calculate_hash(content);

    let json_data = json!({
        "fingerprint_hex": fingerprint_hex,
        "hash_hex": hash_hex,
        "content": content,
        "is_encrypted": false
    });

    let insert_query = format!(
        "INSERT INTO data (fingerprint_hex, hash_hex, content, is_encrypted) FORMAT JSONEachRow\n{}",
        serde_json::to_string(&json_data)?
    );
    log::info!("Uploading {} bytes to {}", content.len(), PASTILA_URL);

    let client = reqwest::Client::new();
    let response = client
        .post(PASTILA_URL)
        .body(insert_query)
        .send()
        .await?
        .error_for_status()?;

    // Note, this is not 100% guarantee due to async_insert.
    if !response.status().is_success() {
        return Err(Error::msg(format!(
            "Failed to upload flamegraph data: {}",
            response.status()
        )));
    }

    let pastila_page_url = format!("https://pastila.nl/?{}/{}", fingerprint_hex, hash_hex);
    log::info!("Pastila URL: {}", pastila_page_url);

    let select_query = format!(
        "SELECT content FROM data_view(fingerprint = '{}', hash = '{}') FORMAT TabSeparatedRaw",
        fingerprint_hex, hash_hex
    );
    let clickhouse_url = format!("{}&query={}", PASTILA_URL, &select_query);
    log::info!("Pastila ClickHouse URL: {}", clickhouse_url);

    Ok(clickhouse_url)
}

pub fn show(block: Columns) -> AppResult<()> {
    let data = block
        .rows()
        .map(|x| {
            [
                x.get::<String, _>(0).unwrap(),
                x.get::<u64, _>(1).unwrap().to_string(),
            ]
            .join(" ")
        })
        .collect::<Vec<String>>()
        .join("\n");

    if data.trim().is_empty() {
        return Err(Error::msg("Flamegraph is empty").into());
    }

    let flamegraph = FlameGraph::from_string(data, true);
    let mut app = App::with_flamegraph("Query", flamegraph);

    let backend = CrosstermBackend::new(io::stderr());
    let mut terminal = Terminal::new(backend)?;
    let timeout = std::time::Duration::from_secs(1);

    terminal.clear()?;

    // Start the main loop.
    while app.running {
        terminal.draw(|frame| {
            ui::render(&mut app, frame);
            if let Some(input_buffer) = &app.input_buffer
                && let Some(cursor) = input_buffer.cursor
            {
                frame.set_cursor_position((cursor.0, cursor.1));
            }
        })?;

        // FIXME: note, right now I cannot use EventHandle with Tui, since EventHandle is not
        // terminated gracefully
        if event::poll(timeout).expect("failed to poll new events") {
            match event::read().expect("unable to read event") {
                CrosstermEvent::Key(e) => {
                    if e.kind == KeyEventKind::Press {
                        handle_key_events(e, &mut app)?
                    }
                }
                CrosstermEvent::Mouse(_e) => {}
                CrosstermEvent::Resize(_w, _h) => {}
                CrosstermEvent::FocusGained => {}
                CrosstermEvent::FocusLost => {}
                CrosstermEvent::Paste(_) => {}
            }
        }
    }

    terminal.clear()?;

    Ok(())
}

pub async fn open_in_speedscope(block: Columns) -> Result<()> {
    let data = block
        .rows()
        .map(|x| {
            [
                x.get::<String, _>(0).unwrap(),
                x.get::<u64, _>(1).unwrap().to_string(),
            ]
            .join(" ")
        })
        .collect::<Vec<String>>()
        .join("\n");

    if data.trim().is_empty() {
        return Err(Error::msg("Flamegraph is empty"));
    }

    let pastila_url = upload_to_pastila(&data).await?;

    let url = format!(
        "https://www.speedscope.app/#profileURL={}",
        encode(&pastila_url)
    );

    let mut child = open_url_command(&url)
        .spawn()
        .map_err(|e| Error::msg(format!("Cannot open URL: {}", e)))?;

    let result = child.wait()?;
    if !result.success() {
        return Err(Error::msg(format!(
            "Error while opening flamegraph in browser: {:?} (Do you have some browser installed?)",
            result
        )));
    }

    return Ok(());
}
