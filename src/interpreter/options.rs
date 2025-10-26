use crate::common::RelativeDateTime;
use anyhow::{Result, anyhow};
use clap::{ArgAction, Args, CommandFactory, Parser, Subcommand, builder::ArgPredicate};
use clap_complete::{Shell, generate};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use quick_xml::de::Deserializer as XmlDeserializer;
use serde::Deserialize;
use serde_yaml::Deserializer as YamlDeserializer;
use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path;
use std::process;
use std::str::FromStr;
use std::time;

#[derive(Deserialize, Debug, PartialEq)]
struct ClickHouseClientConfigOpenSSLClient {
    #[serde(rename = "verificationMode")]
    verification_mode: Option<String>,
    #[serde(rename = "certificateFile")]
    certificate_file: Option<String>,
    #[serde(rename = "privateKeyFile")]
    private_key_file: Option<String>,
    #[serde(rename = "caConfig")]
    ca_config: Option<String>,
}
#[derive(Deserialize, Debug, PartialEq)]
struct ClickHouseClientConfigOpenSSL {
    client: Option<ClickHouseClientConfigOpenSSLClient>,
}

#[derive(Deserialize, Debug, PartialEq)]
struct ClickHouseClientConfigConnectionsCredentials {
    name: String,
    hostname: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    secure: Option<bool>,
    // chdig analog for accept_invalid_certificate
    skip_verify: Option<bool>,
    #[serde(rename = "accept-invalid-certificate")]
    accept_invalid_certificate: Option<bool>,
    ca_certificate: Option<String>,
    client_certificate: Option<String>,
    client_private_key: Option<String>,
}
#[derive(Deserialize, Default, Debug, PartialEq)]
struct ClickHouseClientConfig {
    user: Option<String>,
    password: Option<String>,
    secure: Option<bool>,
    // chdig analog for accept_invalid_certificate
    skip_verify: Option<bool>,
    #[serde(rename = "accept-invalid-certificate")]
    accept_invalid_certificate: Option<bool>,
    open_ssl: Option<ClickHouseClientConfigOpenSSL>,
    connections_credentials: Vec<ClickHouseClientConfigConnectionsCredentials>,
}

#[derive(Deserialize, Default)]
struct XmlClickHouseClientConfigConnectionsCredentialsConnection {
    connection: Option<Vec<ClickHouseClientConfigConnectionsCredentials>>,
}
#[derive(Deserialize)]
struct XmlClickHouseClientConfig {
    user: Option<String>,
    password: Option<String>,
    secure: Option<bool>,
    // chdig analog for accept_invalid_certificate
    skip_verify: Option<bool>,
    #[serde(rename = "accept-invalid-certificate")]
    accept_invalid_certificate: Option<bool>,
    #[serde(rename = "openSSL")]
    open_ssl: Option<ClickHouseClientConfigOpenSSL>,
    connections_credentials: Option<XmlClickHouseClientConfigConnectionsCredentialsConnection>,
}

#[derive(Deserialize)]
struct YamlClickHouseClientConfig {
    user: Option<String>,
    password: Option<String>,
    secure: Option<bool>,
    // chdig analog for accept_invalid_certificate
    skip_verify: Option<bool>,
    #[serde(rename = "accept-invalid-certificate")]
    accept_invalid_certificate: Option<bool>,
    #[serde(rename = "openSSL")]
    open_ssl: Option<ClickHouseClientConfigOpenSSL>,
    connections_credentials: Option<HashMap<String, ClickHouseClientConfigConnectionsCredentials>>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum ChDigViews {
    /// Show now running queries (from system.processes)
    Queries,
    /// Show last running queries (from system.query_log)
    LastQueries,
    /// Show slow (slower then 1 second, ordered by duration) queries (from system.query_log)
    SlowQueries,
    /// Show merges for MergeTree engine (system.merges)
    Merges,
    /// Show S3 Queue (system.s3queue)
    S3Queue,
    /// Show mutations for MergeTree engine (system.mutations)
    Mutations,
    /// Show replication queue for ReplicatedMergeTree engine (system.replication_queue)
    ReplicationQueue,
    /// Show fetches for ReplicatedMergeTree engine (system.replicated_fetches)
    ReplicatedFetches,
    /// Show information about replicas (system.replicas)
    Replicas,
    /// Show all errors that happened in a server since start (system.errors)
    Errors,
    /// Show information about backups (system.backups)
    Backups,
    /// Show information about dictionaries (system.dictionaries)
    Dictionaries,
    /// Show server logs (system.text_log)
    ServerLogs,
}

#[derive(Parser, Clone)]
#[command(name = "chdig")]
#[command(author, version, about, long_about = None)]
pub struct ChDigOptions {
    #[command(flatten)]
    pub clickhouse: ClickHouseOptions,
    #[command(flatten)]
    pub view: ViewOptions,
    #[command(subcommand)]
    pub start_view: Option<ChDigViews>,
    #[command(flatten)]
    pub service: ServiceOptions,
}

#[derive(Args, Clone, Default)]
pub struct ClickHouseOptions {
    #[arg(short('u'), long, value_name = "URL", env = "CHDIG_URL")]
    pub url: Option<String>,
    /// Overrides host in --url (for clickhouse-client compatibility)
    #[arg(long, env = "CLICKHOUSE_HOST")]
    pub host: Option<String>,
    /// Overrides port in --url (for clickhouse-client compatibility)
    #[arg(long)]
    pub port: Option<u16>,
    /// Overrides user in --url (for clickhouse-client compatibility)
    #[arg(long, env = "CLICKHOUSE_USER")]
    pub user: Option<String>,
    /// Overrides password in --url (for clickhouse-client compatibility)
    #[arg(long, env = "CLICKHOUSE_PASSWORD")]
    pub password: Option<String>,
    /// Overrides secure=1 in --url (for clickhouse-client compatibility)
    #[arg(long, action = ArgAction::SetTrue)]
    pub secure: bool,
    /// ClickHouse like config (with some advanced features)
    #[arg(long, env = "CLICKHOUSE_CONFIG")]
    pub config: Option<String>,
    #[arg(short('C'), long)]
    pub connection: Option<String>,
    // Safe version for "url" (to show in UI)
    #[clap(skip)]
    pub url_safe: String,
    #[arg(short('c'), long)]
    pub cluster: Option<String>,
    /// Aggregate system.*_log historical data, using merge()
    #[arg(long, action = ArgAction::SetTrue)]
    pub history: bool,
    #[arg(long, action = ArgAction::SetTrue, overrides_with = "history")]
    pub no_history: bool,
    /// Do not hide internal (spawned by chdig) queries
    #[arg(long, action = ArgAction::SetTrue)]
    pub internal_queries: bool,
    #[arg(long, action = ArgAction::SetTrue, overrides_with = "internal_queries")]
    pub no_internal_queries: bool,
}

#[derive(Args, Clone)]
pub struct ViewOptions {
    #[arg(
        short('d'),
        long,
        value_parser = |arg: &str| -> Result<time::Duration> {Ok(time::Duration::from_millis(arg.parse()?))},
        default_value = "3000",
    )]
    pub delay_interval: time::Duration,

    #[arg(short('g'), long, action = ArgAction::SetTrue, default_value_if("cluster", ArgPredicate::IsPresent, Some("true")))]
    /// Grouping distributed queries (turned on by default in --cluster mode)
    pub group_by: bool,
    #[arg(short('G'), long, action = ArgAction::SetTrue, overrides_with = "group_by")]
    no_group_by: bool,

    #[arg(long, action = ArgAction::SetTrue)]
    /// Do not accumulate metrics for subqueries in the initial query
    pub no_subqueries: bool,

    // Use short option -b, like atop(1) has
    #[arg(long, short('b'), default_value = "1hour")]
    /// Begin of the time interval to look at
    pub start: RelativeDateTime,
    #[arg(long, short('e'), default_value = "")]
    /// End of the time interval
    pub end: RelativeDateTime,

    /// Wrap long lines (more CPU greedy)
    #[arg(long, action = ArgAction::SetTrue)]
    pub wrap: bool,
    // TODO: --mouse/--no-mouse (see EXIT_MOUSE_SEQUENCE in termion)
}

#[derive(Args, Clone)]
pub struct ServiceOptions {
    #[arg(long, value_enum)]
    completion: Option<Shell>,
    #[arg(long)]
    /// Log (for debugging chdig itself)
    pub log: Option<String>,
}

fn read_yaml_clickhouse_client_config(path: &str) -> Result<ClickHouseClientConfig> {
    let file = fs::File::open(path)?;
    let reader = io::BufReader::new(file);
    let doc = YamlDeserializer::from_reader(reader);
    let yaml_config = YamlClickHouseClientConfig::deserialize(doc)?;

    let config = ClickHouseClientConfig {
        user: yaml_config.user,
        password: yaml_config.password,
        secure: yaml_config.secure,
        skip_verify: yaml_config.skip_verify,
        accept_invalid_certificate: yaml_config.accept_invalid_certificate,
        open_ssl: yaml_config.open_ssl,
        connections_credentials: yaml_config
            .connections_credentials
            .unwrap_or_default()
            .into_values()
            .collect(),
    };
    return Ok(config);
}
fn read_xml_clickhouse_client_config(path: &str) -> Result<ClickHouseClientConfig> {
    let file = fs::File::open(path)?;
    let reader = io::BufReader::new(file);
    let mut doc = XmlDeserializer::from_reader(reader);
    let xml_config = XmlClickHouseClientConfig::deserialize(&mut doc)?;

    let config = ClickHouseClientConfig {
        user: xml_config.user,
        password: xml_config.password,
        secure: xml_config.secure,
        skip_verify: xml_config.skip_verify,
        accept_invalid_certificate: xml_config.accept_invalid_certificate,
        open_ssl: xml_config.open_ssl,
        connections_credentials: xml_config
            .connections_credentials
            .unwrap_or_default()
            .connection
            .unwrap_or_default(),
    };
    return Ok(config);
}
macro_rules! try_xml {
    ( $path:expr ) => {
        if path::Path::new($path).exists() {
            log::info!("Loading {}", $path);
            return Some(read_xml_clickhouse_client_config($path));
        }
    };
}
macro_rules! try_yaml {
    ( $path:expr ) => {
        if path::Path::new($path).exists() {
            log::info!("Loading {}", $path);
            return Some(read_yaml_clickhouse_client_config($path));
        }
    };
}
fn try_default_clickhouse_client_config() -> Option<Result<ClickHouseClientConfig>> {
    if let Ok(home) = env::var("HOME") {
        try_xml!(&format!("{}/.clickhouse-client/config.xml", home));
        try_yaml!(&format!("{}/.clickhouse-client/config.yml", home));
        try_yaml!(&format!("{}/.clickhouse-client/config.yaml", home));
    }

    try_xml!("/etc/clickhouse-client/config.xml");
    try_yaml!("/etc/clickhouse-client/config.yml");
    try_yaml!("/etc/clickhouse-client/config.yaml");

    return None;
}

fn parse_url(options: &ClickHouseOptions) -> Result<url::Url> {
    let url_str = options.url.clone().unwrap_or_default();
    let url = if url_str.contains("://") {
        // url::Url::scheme() does not works as we want,
        // since for "foo:bar@127.1" the scheme will be "foo",
        url::Url::parse(&url_str)?
    } else {
        url::Url::parse(&format!("tcp://{}", &url_str))?
    };
    Ok(url)
}

fn is_local_address(host: &str) -> bool {
    let localhost = SocketAddr::from(([127, 0, 0, 1], 0));
    let addresses = format!("{}:0", host).to_socket_addrs();
    log::trace!("Resolving: {} -> {:?}", host, addresses);
    if let Ok(addresses) = addresses {
        for address in addresses {
            if address != localhost {
                log::trace!("Address {:?} is not local", address);
                return false;
            }
        }
        log::trace!("Host {} is local", host);
        return true;
    }
    return false;
}

fn set_password_from_opt(url: &mut url::Url, password: Option<String>, force: bool) -> Result<()> {
    if let Some(password) = password
        && (url.password().is_none() || force)
    {
        url.set_password(Some(
            &utf8_percent_encode(&password, NON_ALPHANUMERIC).to_string(),
        ))
        .map_err(|_| anyhow!("password is invalid"))?;
    }
    Ok(())
}

fn clickhouse_url_defaults(
    options: &mut ClickHouseOptions,
    config: Option<ClickHouseClientConfig>,
) -> Result<()> {
    let mut url = parse_url(options)?;
    let connection = &options.connection;
    let mut secure: Option<bool>;
    let mut skip_verify: Option<bool>;
    let mut ca_certificate: Option<String>;
    let mut client_certificate: Option<String>;
    let mut client_private_key: Option<String>;

    {
        let pairs: HashMap<_, _> = url.query_pairs().into_owned().collect();
        secure = pairs.get("secure").and_then(|v| bool::from_str(v).ok());
        skip_verify = pairs
            .get("skip_verify")
            .and_then(|v| bool::from_str(v).ok());
        ca_certificate = pairs.get("ca_certificate").cloned();
        client_certificate = pairs.get("client_certificate").cloned();
        client_private_key = pairs.get("client_private_key").cloned();
    }

    // host should be set first, since url crate does not allow to set user/password without host.
    let has_host = url.host().is_some();
    if !has_host {
        url.set_host(Some("127.1"))?;
    }

    // Apply clickhouse-client compatible options
    if let Some(host) = &options.host {
        url.set_host(Some(host))?;
    }
    if let Some(port) = options.port {
        url.set_port(Some(port))
            .map_err(|_| anyhow!("port is invalid"))?;
    }
    if let Some(user) = &options.user {
        url.set_username(user)
            .map_err(|_| anyhow!("username is invalid"))?;
    }
    set_password_from_opt(&mut url, options.password.clone(), true)?;
    if options.secure {
        secure = Some(true);
    }

    //
    // config
    //
    if let Some(config) = config {
        if url.username().is_empty()
            && let Some(user) = config.user
        {
            url.set_username(user.as_str())
                .map_err(|_| anyhow!("username is invalid"))?;
        }
        set_password_from_opt(&mut url, config.password, false)?;
        if secure.is_none()
            && let Some(conf_secure) = config.secure
        {
            secure = Some(conf_secure);
        }

        let ssl_client = config.open_ssl.and_then(|ssl| ssl.client);
        if skip_verify.is_none()
            && let Some(conf_skip_verify) = config
                .skip_verify
                .or(config.accept_invalid_certificate)
                .or_else(|| {
                    ssl_client
                        .as_ref()
                        .map(|client| client.verification_mode == Some("none".to_string()))
                })
        {
            skip_verify = Some(conf_skip_verify);
        }
        if ca_certificate.is_none()
            && let Some(conf_ca_certificate) = ssl_client.as_ref().map(|v| v.ca_config.clone())
        {
            ca_certificate = conf_ca_certificate.clone();
        }
        if client_certificate.is_none()
            && let Some(conf_client_certificate) =
                ssl_client.as_ref().map(|v| v.certificate_file.clone())
        {
            client_certificate = conf_client_certificate.clone();
        }
        if client_private_key.is_none()
            && let Some(conf_client_private_key) =
                ssl_client.as_ref().map(|v| v.private_key_file.clone())
        {
            client_private_key = conf_client_private_key.clone();
        }

        //
        // connections_credentials section from config
        //
        let mut connection_found = false;
        if let Some(connection) = connection {
            for c in config.connections_credentials.iter() {
                if &c.name != connection {
                    continue;
                }
                if connection_found {
                    panic!("Multiple connections had been matched. Fix you config.xml");
                }

                connection_found = true;
                if !has_host && let Some(hostname) = &c.hostname {
                    url.set_host(Some(hostname.as_str()))?;
                }
                if url.port().is_none()
                    && let Some(port) = c.port
                {
                    url.set_port(Some(port))
                        .map_err(|_| anyhow!("Cannot set port"))?;
                }
                if url.username().is_empty()
                    && let Some(user) = &c.user
                {
                    url.set_username(user.as_str())
                        .map_err(|_| anyhow!("username is invalid"))?;
                }
                set_password_from_opt(&mut url, c.password.clone(), false)?;
                if secure.is_none()
                    && let Some(con_secure) = c.secure
                {
                    secure = Some(con_secure);
                }
                if skip_verify.is_none()
                    && let Some(con_skip_verify) = c.skip_verify
                {
                    skip_verify = Some(con_skip_verify);
                }
                if ca_certificate.is_none()
                    && let Some(con_ca_certificate) = &c.ca_certificate
                {
                    ca_certificate = Some(con_ca_certificate.clone());
                }
                if client_certificate.is_none()
                    && let Some(con_client_certificate) = &c.client_certificate
                {
                    client_certificate = Some(con_client_certificate.clone());
                }
                if client_private_key.is_none()
                    && let Some(con_client_private_key) = &c.client_private_key
                {
                    client_private_key = Some(con_client_private_key.clone());
                }
            }

            if !connection_found {
                panic!("Connection {} was not found", connection);
            }
        }
    } else if connection.is_some() {
        panic!("No client config had been read, while --connection was set");
    }

    // - 9000 for non secure
    // - 9440 for secure
    if url.port().is_none() {
        url.set_port(Some(if secure.unwrap_or_default() {
            9440
        } else {
            9000
        }))
        .map_err(|_| anyhow!("Cannot set port"))?;
    }

    let mut url_safe = url.clone();

    // url_safe
    if url_safe.password().is_some() {
        url_safe
            .set_password(None)
            .map_err(|_| anyhow!("Cannot hide password"))?;
    }
    options.url_safe = url_safe.to_string();

    // Switch database to "system", since "default" may not be present.
    if url_safe.path().is_empty() || url_safe.path() == "/" {
        url.set_path("/system");
    }

    // some default settings in URL
    {
        let pairs: HashMap<_, _> = url_safe.query_pairs().into_owned().collect();
        let is_local = is_local_address(&url.host().ok_or_else(|| anyhow!("No host"))?.to_string());
        let mut mut_pairs = url.query_pairs_mut();
        // Enable compression in non-local network (in the same way as clickhouse does by default)
        if !pairs.contains_key("compression") && !is_local {
            mut_pairs.append_pair("compression", "lz4");
        }
        // default is: 500ms (too small)
        if !pairs.contains_key("connection_timeout") {
            mut_pairs.append_pair("connection_timeout", "5s");
        }
        // Note, right now even on a big clusters, everything works within default timeout (180s),
        // but just to make it "user friendly" even for some obscure setups, let's increase the
        // timeout still.
        if !pairs.contains_key("query_timeout") {
            mut_pairs.append_pair("query_timeout", "600s");
        }
        if let Some(secure) = secure {
            mut_pairs.append_pair("secure", secure.to_string().as_str());
        }
        if let Some(skip_verify) = skip_verify {
            mut_pairs.append_pair("skip_verify", skip_verify.to_string().as_str());
        }
        if let Some(ca_certificate) = ca_certificate {
            mut_pairs.append_pair("ca_certificate", &ca_certificate);
        }
        if let Some(client_certificate) = client_certificate {
            mut_pairs.append_pair("client_certificate", &client_certificate);
        }
        if let Some(client_private_key) = client_private_key {
            mut_pairs.append_pair("client_private_key", &client_private_key);
        }
    }

    options.url = Some(url.to_string());

    return Ok(());
}

fn adjust_defaults(options: &mut ChDigOptions) -> Result<()> {
    let config = if let Some(user_config) = &options.clickhouse.config {
        if user_config.to_lowercase().ends_with(".xml") {
            Some(read_xml_clickhouse_client_config(user_config)?)
        } else {
            Some(read_yaml_clickhouse_client_config(user_config)?)
        }
    } else if let Some(config) = try_default_clickhouse_client_config() {
        Some(config?)
    } else {
        None
    };
    clickhouse_url_defaults(&mut options.clickhouse, config)?;

    // FIXME: overrides_with works before default_value_if, hence --no-group-by never works
    if options.view.no_group_by {
        options.view.group_by = false;
    }

    return Ok(());
}

// TODO:
// - config, I tried twelf but it is too buggy for now [1], let track [2] instead, I've also tried
//   viperus for the first version of this program, but it was even more buggy and does not support
//   new clap, and also it is not maintained anymore.
//
//     [1]: https://github.com/clap-rs/clap/discussions/2763
//     [2]: https://github.com/bnjjj/twelf/issues/15
pub fn parse_from<I, T>(itr: I) -> Result<ChDigOptions>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let mut options = ChDigOptions::parse_from(itr);

    // Generate autocompletion
    if let Some(shell) = options.service.completion {
        let mut cmd = ChDigOptions::command();
        let name = cmd.get_name().to_string();
        generate(shell, &mut cmd, name, &mut io::stdout());
        process::exit(0);
    }

    adjust_defaults(&mut options)?;

    return Ok(options);
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_url_parse_no_proto() {
        assert_eq!(
            parse_url(&ClickHouseOptions::default()).unwrap(),
            url::Url::parse("tcp://").unwrap()
        );
    }

    #[test]
    fn test_url_parse_user() {
        let mut options = ClickHouseOptions {
            user: Some("foo".into()),
            ..Default::default()
        };
        clickhouse_url_defaults(&mut options, None).unwrap();
        assert_eq!(
            parse_url(&options).unwrap(),
            url::Url::parse("tcp://foo@127.1:9000/system?connection_timeout=5s&query_timeout=600s")
                .unwrap()
        );
    }

    #[test]
    fn test_url_parse_password() {
        let mut options = ClickHouseOptions {
            password: Some("foo".into()),
            ..Default::default()
        };
        clickhouse_url_defaults(&mut options, None).unwrap();
        assert_eq!(
            parse_url(&options).unwrap(),
            url::Url::parse(
                "tcp://:foo@127.1:9000/system?connection_timeout=5s&query_timeout=600s"
            )
            .unwrap()
        );
    }

    #[test]
    fn test_url_parse_password_with_special_chars() {
        let password = "!@#$%41^&*(%)";
        let mut options = ClickHouseOptions {
            password: Some(password.into()),
            ..Default::default()
        };
        clickhouse_url_defaults(&mut options, None).unwrap();
        assert_eq!(
            parse_url(&options).unwrap(),
            url::Url::parse("tcp://:%21%40%23%24%2541%5E%26%2A%28%25%29@127.1:9000/system?connection_timeout=5s&query_timeout=600s").
            unwrap()
        );
    }

    #[test]
    fn test_url_parse_port() {
        let mut options = ClickHouseOptions {
            port: Some(9440),
            ..Default::default()
        };
        clickhouse_url_defaults(&mut options, None).unwrap();
        assert_eq!(
            parse_url(&options).unwrap(),
            url::Url::parse("tcp://127.1:9440/system?connection_timeout=5s&query_timeout=600s")
                .unwrap()
        );
    }

    #[test]
    fn test_url_parse_secure() {
        let mut options = ClickHouseOptions {
            secure: true,
            ..Default::default()
        };
        clickhouse_url_defaults(&mut options, None).unwrap();
        assert_eq!(
            parse_url(&options).unwrap(),
            url::Url::parse(
                "tcp://127.1:9440/system?connection_timeout=5s&query_timeout=600s&secure=true"
            )
            .unwrap()
        );
    }

    #[test]
    fn test_config_empty() {
        assert_eq!(
            read_xml_clickhouse_client_config("tests/configs/empty.xml").is_ok(),
            true
        );
        assert_eq!(
            read_yaml_clickhouse_client_config("tests/configs/empty.yaml").is_ok(),
            true
        );
    }

    #[test]
    fn test_config_unknown_directives() {
        assert_eq!(
            read_xml_clickhouse_client_config("tests/configs/unknown_directives.xml").is_ok(),
            true
        );
        assert_eq!(
            read_yaml_clickhouse_client_config("tests/configs/unknown_directives.yaml").is_ok(),
            true
        );
    }

    #[test]
    fn test_config_basic() {
        let xml_config = read_xml_clickhouse_client_config("tests/configs/basic.xml").unwrap();
        let yaml_config = read_yaml_clickhouse_client_config("tests/configs/basic.yaml").unwrap();
        let config = ClickHouseClientConfig {
            user: Some("foo".into()),
            password: Some("bar".into()),
            ..Default::default()
        };
        assert_eq!(config, xml_config);
        assert_eq!(config, yaml_config);
    }

    #[test]
    fn test_config_tls() {
        let xml_config = read_xml_clickhouse_client_config("tests/configs/tls.xml").unwrap();
        let yaml_config = read_yaml_clickhouse_client_config("tests/configs/tls.yaml").unwrap();
        let config = ClickHouseClientConfig {
            secure: Some(true),
            open_ssl: Some(ClickHouseClientConfigOpenSSL {
                client: Some(ClickHouseClientConfigOpenSSLClient {
                    verification_mode: Some("strict".into()),
                    certificate_file: Some("cert".into()),
                    private_key_file: Some("key".into()),
                    ca_config: Some("ca".into()),
                }),
            }),
            ..Default::default()
        };
        assert_eq!(config, xml_config);
        assert_eq!(config, yaml_config);
    }

    #[test]
    fn test_config_tls_applying_config_to_connection_url() {
        let config = read_yaml_clickhouse_client_config("tests/configs/tls.yaml").ok();
        let mut options = ClickHouseOptions {
            ..Default::default()
        };
        clickhouse_url_defaults(&mut options, config).unwrap();
        let url = parse_url(&options).unwrap();
        let args: HashMap<_, _> = url.query_pairs().into_owned().collect();

        assert_eq!(args.get("secure"), Some(&"true".into()));
        assert_eq!(args.get("ca_certificate"), Some(&"ca".into()));
        assert_eq!(args.get("client_certificate"), Some(&"cert".into()));
        assert_eq!(args.get("client_private_key"), Some(&"key".into()));
        assert_eq!(args.get("skip_verify"), Some(&"false".into()));
    }

    #[test]
    fn test_config_connections_applying_config_to_connection_url_play() {
        let config = read_yaml_clickhouse_client_config("tests/configs/connections.yaml").ok();
        let mut options = ClickHouseOptions {
            connection: Some("play".into()),
            ..Default::default()
        };
        clickhouse_url_defaults(&mut options, config).unwrap();
        let url = parse_url(&options).unwrap();
        let args: HashMap<_, _> = url.query_pairs().into_owned().collect();

        assert_eq!(url.host().unwrap().to_string(), "play.clickhouse.com");
        assert_eq!(args.get("secure"), Some(&"true".into()));
        assert_eq!(args.contains_key("skip_verify"), false);
    }

    #[test]
    fn test_config_connections_applying_config_to_connection_url_play_tls() {
        let config = read_yaml_clickhouse_client_config("tests/configs/connections.yaml").ok();
        let mut options = ClickHouseOptions {
            connection: Some("play-tls".into()),
            ..Default::default()
        };
        clickhouse_url_defaults(&mut options, config).unwrap();
        let url = parse_url(&options).unwrap();
        let args: HashMap<_, _> = url.query_pairs().into_owned().collect();

        assert_eq!(url.host().unwrap().to_string(), "play.clickhouse.com");
        assert_eq!(args.get("secure"), Some(&"true".into()));
        assert_eq!(args.get("ca_certificate"), Some(&"ca".into()));
        assert_eq!(args.get("client_certificate"), Some(&"cert".into()));
        assert_eq!(args.get("client_private_key"), Some(&"key".into()));
        assert_eq!(args.get("skip_verify"), Some(&"true".into()));
    }

    #[test]
    fn test_config_apply_accept_invalid_certificate() {
        let config =
            read_yaml_clickhouse_client_config("tests/configs/accept_invalid_certificate.yaml")
                .unwrap();
        assert_eq!(config.accept_invalid_certificate, Some(true));

        let mut options = ClickHouseOptions {
            ..Default::default()
        };
        clickhouse_url_defaults(&mut options, Some(config)).unwrap();

        let url = parse_url(&options).unwrap();
        let args: HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(args.get("skip_verify"), Some(&"true".into()));
    }
}
