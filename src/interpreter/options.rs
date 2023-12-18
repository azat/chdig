use anyhow::Result;
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
use std::time::Duration;
use url;

#[derive(Deserialize)]
struct ClickHouseClientConfigConnectionsCredentials {
    name: String,
    hostname: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    secure: Option<bool>,
}
#[derive(Deserialize, Default)]
struct ClickHouseClientConfig {
    user: Option<String>,
    password: Option<String>,
    secure: Option<bool>,
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
    connections_credentials: Option<XmlClickHouseClientConfigConnectionsCredentialsConnection>,
}

#[derive(Deserialize)]
struct YamlClickHouseClientConfig {
    user: Option<String>,
    password: Option<String>,
    secure: Option<bool>,
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
    /// Show all errors that happend in a server since start (system.errors)
    Errors,
    /// Show information about backups (system.backups)
    Backups,
    /// Show information about dictionaries (system.dictionaries)
    Dictionaries,
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
    internal: InternalOptions,
}

#[derive(Args, Clone)]
pub struct ClickHouseOptions {
    #[arg(short('u'), long, value_name = "URL", env = "CHDIG_URL")]
    pub url: Option<String>,
    #[arg(short('C'), long)]
    pub connection: Option<String>,
    // Safe version for "url" (to show in UI)
    #[clap(skip)]
    pub url_safe: String,
    #[arg(short('c'), long)]
    pub cluster: Option<String>,
}

#[derive(Args, Clone)]
pub struct ViewOptions {
    #[arg(
        short('d'),
        long,
        value_parser = |arg: &str| -> Result<Duration> {Ok(Duration::from_millis(arg.parse()?))},
        default_value = "3000",
    )]
    pub delay_interval: Duration,

    #[arg(short('g'), long, action = ArgAction::SetTrue, default_value_if("cluster", ArgPredicate::IsPresent, Some("true")))]
    /// Grouping distributed queries (turned on by default in --cluster mode)
    pub group_by: bool,
    #[arg(short('G'), long, action = ArgAction::SetTrue, overrides_with = "group_by")]
    no_group_by: bool,

    #[arg(long, default_value_t = false)]
    /// Do not accumulate metrics for subqueries in the initial query
    pub no_subqueries: bool,
    // TODO: --mouse/--no-mouse (see EXIT_MOUSE_SEQUENCE in termion)
}

#[derive(Args, Clone)]
pub struct InternalOptions {
    #[arg(long, value_enum)]
    completion: Option<Shell>,
}

fn read_yaml_clickhouse_client_config(path: &str) -> Result<ClickHouseClientConfig> {
    let file = fs::File::open(path)?;
    let reader = io::BufReader::new(file);
    let doc = YamlDeserializer::from_reader(reader);
    let yaml_config = YamlClickHouseClientConfig::deserialize(doc)?;

    let mut config = ClickHouseClientConfig::default();
    config.user = yaml_config.user;
    config.password = yaml_config.password;
    config.secure = yaml_config.secure;
    config.connections_credentials = yaml_config
        .connections_credentials
        .unwrap_or_default()
        .into_values()
        .collect();

    return Ok(config);
}
fn read_xml_clickhouse_client_config(path: &str) -> Result<ClickHouseClientConfig> {
    let file = fs::File::open(path)?;
    let reader = io::BufReader::new(file);
    let mut doc = XmlDeserializer::from_reader(reader);
    let xml_config = XmlClickHouseClientConfig::deserialize(&mut doc)?;

    let mut config = ClickHouseClientConfig::default();
    config.user = xml_config.user;
    config.password = xml_config.password;
    config.secure = xml_config.secure;
    config.connections_credentials = xml_config
        .connections_credentials
        .unwrap_or_default()
        .connection
        .unwrap_or_default();

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
fn read_clickhouse_client_config() -> Option<ClickHouseClientConfig> {
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
    let localhost = Some(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
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

fn clickhouse_url_defaults(options: &mut ChDigOptions) {
    let mut url = parse_url(&options.clickhouse.url.clone().unwrap_or_default());
    let config: Option<ClickHouseClientConfig> = read_clickhouse_client_config();
    let connection = &options.clickhouse.connection;
    let mut has_secure: Option<bool> = None;

    {
        let pairs: HashMap<_, _> = url.query_pairs().into_owned().collect();
        if pairs.contains_key("secure") {
            has_secure = Some(true);
        }
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
            if let Some(user) = &config.user {
                url.set_username(user.as_str()).unwrap();
            }
        }
        if url.password().is_none() {
            if let Some(password) = &config.password {
                url.set_password(Some(password.as_str())).unwrap();
            }
        }
        if has_secure.is_none() {
            if let Some(secure) = &config.secure {
                has_secure = Some(*secure);
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
                if has_secure.is_none() {
                    if let Some(secure) = &c.secure {
                        has_secure = Some(*secure);
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
        url.set_port(Some(if has_secure.unwrap_or_default() {
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
    options.clickhouse.url_safe = url_safe.to_string();

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
        if let Some(secure) = has_secure {
            mut_pairs.append_pair("secure", secure.to_string().as_str());
        }
    }

    options.clickhouse.url = Some(url.to_string());
}

fn adjust_defaults(options: &mut ChDigOptions) {
    clickhouse_url_defaults(options);

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
    if let Some(shell) = options.internal.completion {
        let mut cmd = ChDigOptions::command();
        let name = cmd.get_name().to_string();
        generate(shell, &mut cmd, name, &mut io::stdout());
        process::exit(0);
    }

    adjust_defaults(&mut options);

    return options;
}
