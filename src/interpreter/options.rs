use anyhow::Result;
use chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime};
use clap::{builder::ArgPredicate, ArgAction, Args, CommandFactory, Parser, Subcommand};
use clap_complete::{generate, Shell};
use quick_xml::de::Deserializer as XmlDeserializer;
use serde::Deserialize;
use serde_yaml::Deserializer as YamlDeserializer;
use std::collections::HashMap;
use std::env;
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
    // NOTE: the following options are not supported in the clickhouse-client config (yet).
    skip_verify: Option<bool>,
    ca_certificate: Option<String>,
    client_certificate: Option<String>,
    client_private_key: Option<String>,
}
#[derive(Deserialize, Default, Debug, PartialEq)]
struct ClickHouseClientConfig {
    user: Option<String>,
    password: Option<String>,
    secure: Option<bool>,
    skip_verify: Option<bool>,
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
    skip_verify: Option<bool>,
    #[serde(rename = "openSSL")]
    open_ssl: Option<ClickHouseClientConfigOpenSSL>,
    connections_credentials: Option<XmlClickHouseClientConfigConnectionsCredentialsConnection>,
}

#[derive(Deserialize)]
struct YamlClickHouseClientConfig {
    user: Option<String>,
    password: Option<String>,
    secure: Option<bool>,
    skip_verify: Option<bool>,
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
    service: ServiceOptions,
}

#[derive(Args, Clone, Default)]
pub struct ClickHouseOptions {
    #[arg(short('u'), long, value_name = "URL", env = "CHDIG_URL")]
    pub url: Option<String>,
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
}

pub fn parse_datetime_or_date(value: &str) -> Result<DateTime<Local>, String> {
    let mut errors = Vec::new();
    // Parse without timezone
    match value.parse::<NaiveDateTime>() {
        Ok(datetime) => return Ok(datetime.and_local_timezone(Local).unwrap()),
        Err(err) => errors.push(err),
    }
    // Parse *with* timezone
    match value.parse::<DateTime<Local>>() {
        Ok(datetime) => return Ok(datetime),
        Err(err) => errors.push(err),
    }
    // Parse as date
    match value.parse::<NaiveDate>() {
        Ok(date) => {
            return Ok(date
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_local_timezone(Local)
                .unwrap())
        }
        Err(err) => errors.push(err),
    }
    return Err(format!(
        "Valid RFC3339-formatted (YYYY-MM-DDTHH:MM:SS[.ssssss][±hh:mm|Z]) datetime or date:\n{}",
        errors
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>()
            .join("\n")
    ));
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

    #[arg(long, default_value_t = false)]
    /// Do not accumulate metrics for subqueries in the initial query
    pub no_subqueries: bool,

    // Use short option -b, like atop(1) has
    #[arg(long, short('b'), value_parser = parse_datetime_or_date, default_value_t = Local::now() - Duration::try_hours(1).unwrap())]
    /// Begin of the time interval to look at
    pub start: DateTime<Local>,
    #[arg(long, short('e'), value_parser = parse_datetime_or_date, default_value_t = Local::now())]
    /// End of the time interval
    pub end: DateTime<Local>,

    /// Wrap long lines (more CPU greedy)
    #[arg(long, default_value_t = false)]
    pub wrap: bool,
    // TODO: --mouse/--no-mouse (see EXIT_MOUSE_SEQUENCE in termion)
}

#[derive(Args, Clone)]
pub struct ServiceOptions {
    #[arg(long, value_enum)]
    completion: Option<Shell>,
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
            return Some(read_xml_clickhouse_client_config($path).unwrap());
        }
    };
}
macro_rules! try_yaml {
    ( $path:expr ) => {
        if path::Path::new($path).exists() {
            log::info!("Loading {}", $path);
            return Some(read_yaml_clickhouse_client_config($path).unwrap());
        }
    };
}
fn try_default_clickhouse_client_config() -> Option<ClickHouseClientConfig> {
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

fn parse_url(url_str: &str) -> url::Url {
    // url::Url::scheme() does not works as we want,
    // since for "foo:bar@127.1" the scheme will be "foo",
    if url_str.contains("://") {
        return url::Url::parse(url_str).unwrap();
    }

    return url::Url::parse(&format!("tcp://{}", url_str)).unwrap();
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

fn clickhouse_url_defaults(
    options: &mut ClickHouseOptions,
    config: Option<ClickHouseClientConfig>,
) {
    let mut url = parse_url(&options.url.clone().unwrap_or_default());
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
        url.set_host(Some("127.1")).unwrap();
    }

    //
    // env
    //
    if url.username().is_empty() {
        if let Ok(env_user) = env::var("CLICKHOUSE_USER") {
            url.set_username(env_user.as_str()).unwrap();
        }
    }
    if url.password().is_none() {
        if let Ok(env_password) = env::var("CLICKHOUSE_PASSWORD") {
            url.set_password(Some(env_password.as_str())).unwrap();
        }
    }

    //
    // config
    //
    if let Some(config) = config {
        if url.username().is_empty() {
            if let Some(user) = config.user {
                url.set_username(user.as_str()).unwrap();
            }
        }
        if url.password().is_none() {
            if let Some(password) = config.password {
                url.set_password(Some(password.as_str())).unwrap();
            }
        }
        if secure.is_none() {
            if let Some(conf_secure) = config.secure {
                secure = Some(conf_secure);
            }
        }

        let ssl_client = config.open_ssl.and_then(|ssl| ssl.client);
        if skip_verify.is_none() {
            if let Some(conf_skip_verify) = config.skip_verify.or_else(|| {
                ssl_client
                    .as_ref()
                    .map(|client| client.verification_mode == Some("none".to_string()))
            }) {
                skip_verify = Some(conf_skip_verify);
            }
        }
        if ca_certificate.is_none() {
            if let Some(conf_ca_certificate) = ssl_client.as_ref().map(|v| v.ca_config.clone()) {
                ca_certificate = conf_ca_certificate.clone();
            }
        }
        if client_certificate.is_none() {
            if let Some(conf_client_certificate) =
                ssl_client.as_ref().map(|v| v.certificate_file.clone())
            {
                client_certificate = conf_client_certificate.clone();
            }
        }
        if client_private_key.is_none() {
            if let Some(conf_client_private_key) =
                ssl_client.as_ref().map(|v| v.private_key_file.clone())
            {
                client_private_key = conf_client_private_key.clone();
            }
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
                if !has_host {
                    if let Some(hostname) = &c.hostname {
                        url.set_host(Some(hostname.as_str())).unwrap();
                    }
                }
                if url.port().is_none() {
                    if let Some(port) = c.port {
                        url.set_port(Some(port)).unwrap();
                    }
                }
                if url.username().is_empty() {
                    if let Some(user) = &c.user {
                        url.set_username(user.as_str()).unwrap();
                    }
                }
                if url.password().is_none() {
                    if let Some(password) = &c.password {
                        url.set_password(Some(password.as_str())).unwrap();
                    }
                }
                if secure.is_none() {
                    if let Some(con_secure) = c.secure {
                        secure = Some(con_secure);
                    }
                }
                if skip_verify.is_none() {
                    if let Some(con_skip_verify) = c.skip_verify {
                        skip_verify = Some(con_skip_verify);
                    }
                }
                if ca_certificate.is_none() {
                    if let Some(con_ca_certificate) = &c.ca_certificate {
                        ca_certificate = Some(con_ca_certificate.clone());
                    }
                }
                if client_certificate.is_none() {
                    if let Some(con_client_certificate) = &c.client_certificate {
                        client_certificate = Some(con_client_certificate.clone());
                    }
                }
                if client_private_key.is_none() {
                    if let Some(con_client_private_key) = &c.client_private_key {
                        client_private_key = Some(con_client_private_key.clone());
                    }
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
        .unwrap();
    }

    let mut url_safe = url.clone();

    // url_safe
    if url_safe.password().is_some() {
        url_safe.set_password(None).unwrap();
    }
    options.url_safe = url_safe.to_string();

    // some default settings in URL
    {
        let pairs: HashMap<_, _> = url_safe.query_pairs().into_owned().collect();
        let is_local = is_local_address(&url.host().unwrap().to_string());
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
}

fn adjust_defaults(options: &mut ChDigOptions) {
    let config = if let Some(user_config) = &options.clickhouse.config {
        if user_config.to_lowercase().ends_with(".xml") {
            Some(read_xml_clickhouse_client_config(user_config).unwrap())
        } else {
            Some(read_yaml_clickhouse_client_config(user_config).unwrap())
        }
    } else {
        try_default_clickhouse_client_config()
    };
    clickhouse_url_defaults(&mut options.clickhouse, config);

    // FIXME: overrides_with works before default_value_if, hence --no-group-by never works
    if options.view.no_group_by {
        options.view.group_by = false;
    }
}

// TODO:
// - config, I tried twelf but it is too buggy for now [1], let track [2] instead, I've also tried
//   viperus for the first version of this program, but it was even more buggy and does not support
//   new clap, and also it is not maintained anymore.
//
//     [1]: https://github.com/clap-rs/clap/discussions/2763
//     [2]: https://github.com/bnjjj/twelf/issues/15
pub fn parse() -> ChDigOptions {
    let mut options = ChDigOptions::parse();

    // Generate autocompletion
    if let Some(shell) = options.service.completion {
        let mut cmd = ChDigOptions::command();
        let name = cmd.get_name().to_string();
        generate(shell, &mut cmd, name, &mut io::stdout());
        process::exit(0);
    }

    adjust_defaults(&mut options);

    return options;
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_url_parse_no_proto() {
        assert_eq!(
            parse_url("localhost"),
            url::Url::parse("tcp://localhost").unwrap()
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
        clickhouse_url_defaults(&mut options, config);
        let url = parse_url(&options.url.clone().unwrap_or_default());
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
        clickhouse_url_defaults(&mut options, config);
        let url = parse_url(&options.url.clone().unwrap_or_default());
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
        clickhouse_url_defaults(&mut options, config);
        let url = parse_url(&options.url.clone().unwrap_or_default());
        let args: HashMap<_, _> = url.query_pairs().into_owned().collect();

        assert_eq!(url.host().unwrap().to_string(), "play.clickhouse.com");
        assert_eq!(args.get("secure"), Some(&"true".into()));
        assert_eq!(args.get("ca_certificate"), Some(&"ca".into()));
        assert_eq!(args.get("client_certificate"), Some(&"cert".into()));
        assert_eq!(args.get("client_private_key"), Some(&"key".into()));
        assert_eq!(args.get("skip_verify"), Some(&"true".into()));
    }
}
