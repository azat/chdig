use clap::{builder::ArgPredicate, ArgAction, Args, CommandFactory, Parser, Subcommand};
use clap_complete::{generate, Shell};
use quick_xml::de::Deserializer;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::num::ParseIntError;
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
}
#[derive(Deserialize)]
struct ClickHouseClientConfigConnectionsCredentialsConnection {
    connection: Vec<ClickHouseClientConfigConnectionsCredentials>,
}
#[derive(Deserialize)]
struct ClickHouseClientConfig {
    user: Option<String>,
    password: Option<String>,
    connections_credentials: Option<ClickHouseClientConfigConnectionsCredentialsConnection>,
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
        value_parser = |arg: &str| -> Result<Duration, ParseIntError> {Ok(Duration::from_millis(arg.parse()?))},
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

    #[arg(short('m'), long, action = ArgAction::SetTrue, default_value_t = true)]
    /// Mouse support (turned on by default)
    pub mouse: bool,
    #[arg(short('M'), long, action = ArgAction::SetTrue, overrides_with = "mouse")]
    no_mouse: bool,
}

#[derive(Args, Clone)]
pub struct InternalOptions {
    #[arg(long, value_enum)]
    completion: Option<Shell>,
}

fn xml_from_str<'de, T>(content: &str) -> T
where
    T: Deserialize<'de>,
{
    let mut de = Deserializer::from_reader(content.as_bytes());
    let result = T::deserialize(&mut de).unwrap();
    return result;
}
fn read_one_clickhouse_client_config<'de, T>(path: &str) -> Option<T>
where
    T: Deserialize<'de>,
{
    let content = fs::read_to_string(path);
    match content {
        Ok(content) => {
            return xml_from_str(content.as_ref());
        }
        Err(err) => {
            log::error!("Error while reading {}", err);
        }
    }
    return None;
}
fn read_clickhouse_client_config<'de, T>() -> Option<T>
where
    T: Deserialize<'de>,
{
    if let Ok(home) = env::var("HOME") {
        let path = &format!("{}/.clickhouse-client/config.xml", home);
        let config = read_one_clickhouse_client_config(path);
        if config.is_some() {
            return config;
        }
    }

    {
        let path = "/etc/clickhouse-client/config.xml";
        let config = read_one_clickhouse_client_config(path);
        if config.is_some() {
            return config;
        }
    }

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
            if let Some(user) = &config.user {
                url.set_username(user.as_str()).unwrap();
            }
        }
        if url.password().is_none() {
            if let Some(password) = &config.password {
                url.set_password(Some(password.as_str())).unwrap();
            }
        }

        //
        // connections_credentials section from config
        //
        let mut connection_found = false;
        if let Some(connection) = connection {
            if let Some(connections_credentials) = config.connections_credentials {
                for c in connections_credentials.connection.iter() {
                    if &c.name != connection {
                        continue;
                    }
                    if connection_found {
                        panic!("Multiple connections had been matched. Fix you config.xml");
                    }

                    connection_found = true;
                    if url.host().is_none() {
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
                }
            }

            if !connection_found {
                panic!("Connection {} was not found", connection);
            }
        }
    } else if connection.is_some() {
        panic!("No client config had been read, while --connection was set");
    }

    if url.host().is_none() {
        url.set_host(Some("127.1")).unwrap();
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
    }

    options.clickhouse.url = Some(url.to_string());
}

fn adjust_defaults(options: &mut ChDigOptions) {
    clickhouse_url_defaults(options);

    // FIXME: overrides_with works before default_value_if, hence --no-group-by never works
    if options.view.no_group_by {
        options.view.group_by = false;
    }

    // FIXME: apparently overrides_with works before default_value_t
    if options.view.no_mouse {
        options.view.mouse = false;
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
