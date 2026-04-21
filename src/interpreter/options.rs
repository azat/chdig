use crate::common::RelativeDateTime;
use anyhow::{Result, anyhow};
use clap::{ArgAction, Args, CommandFactory, Parser, Subcommand, ValueEnum, builder::ArgPredicate};
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
    history_file: Option<String>,
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
    history_file: Option<String>,
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
    history_file: Option<String>,
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
    history_file: Option<String>,
    connections_credentials: Option<HashMap<String, ClickHouseClientConfigConnectionsCredentials>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Subcommand)]
pub enum ChDigViews {
    /// Show now running queries (from system.processes)
    Queries,
    /// Show last running queries (from system.query_log)
    LastQueries,
    /// Show slow (slower then 1 second, ordered by duration) queries (from system.query_log)
    SlowQueries,
    /// Show merges for MergeTree engine (system.merges)
    Merges,
    /// Show S3 Queue (system.s3queue_metadata_cache)
    S3Queue,
    /// Show Azure Queue (system.azure_queue_metadata_cache)
    AzureQueue,
    /// Show mutations for MergeTree engine (system.mutations)
    Mutations,
    /// Show replication queue for ReplicatedMergeTree engine (system.replication_queue)
    ReplicationQueue,
    /// Show fetches for ReplicatedMergeTree engine (system.replicated_fetches)
    ReplicatedFetches,
    /// Show information about replicas (system.replicas)
    Replicas,
    /// Tables
    Tables,
    /// Show all errors that happened in a server since start (system.errors)
    Errors,
    /// Show information about backups (system.backups)
    Backups,
    /// Show information about dictionaries (system.dictionaries)
    Dictionaries,
    /// Show server logs (system.text_log)
    ServerLogs,
    /// Show loggers (system.text_log)
    Loggers,
    /// Show background schedule pool tasks (system.background_schedule_pool)
    BackgroundSchedulePool,
    /// Show background schedule pool logs (system.background_schedule_pool_log)
    BackgroundSchedulePoolLog,
    /// Show table parts (system.parts)
    TableParts,
    /// Show asynchronous inserts (system.asynchronous_inserts)
    AsynchronousInserts,
    /// Show part log (system.part_log)
    PartLog,
    /// Spawn client inside chdig
    Client,
}

#[derive(Args, Debug, Clone)]
pub struct PerfettoCommand {
    #[arg(long, action = ArgAction::SetTrue, conflicts_with = "query_id")]
    /// Export server-wide Perfetto trace for the specified time window
    pub server: bool,
    #[arg(
        long = "query-id",
        alias = "query_id",
        value_name = "QUERY_ID",
        conflicts_with = "server"
    )]
    /// Export query-scoped Perfetto trace for the specified query_id
    pub query_id: Option<String>,
    #[arg(long, value_name = "PATH")]
    /// Output path for CLI Perfetto export
    pub output: Option<String>,
    #[arg(long, short('b'), default_value = "1hour")]
    /// Begin of the time interval to look at (used with --server)
    pub start: RelativeDateTime,
    #[arg(long, short('e'), default_value = "")]
    /// End of the time interval (used with --server)
    pub end: RelativeDateTime,
}

#[derive(Debug, Clone, Subcommand)]
pub enum ChDigCommand {
    Queries,
    LastQueries,
    SlowQueries,
    Merges,
    S3Queue,
    AzureQueue,
    Mutations,
    ReplicationQueue,
    ReplicatedFetches,
    Replicas,
    Tables,
    Errors,
    Backups,
    Dictionaries,
    ServerLogs,
    Loggers,
    BackgroundSchedulePool,
    BackgroundSchedulePoolLog,
    TableParts,
    AsynchronousInserts,
    PartLog,
    Client,
    Perfetto(PerfettoCommand),
}

impl ChDigCommand {
    pub fn as_view(&self) -> Option<ChDigViews> {
        match self {
            ChDigCommand::Queries => Some(ChDigViews::Queries),
            ChDigCommand::LastQueries => Some(ChDigViews::LastQueries),
            ChDigCommand::SlowQueries => Some(ChDigViews::SlowQueries),
            ChDigCommand::Merges => Some(ChDigViews::Merges),
            ChDigCommand::S3Queue => Some(ChDigViews::S3Queue),
            ChDigCommand::AzureQueue => Some(ChDigViews::AzureQueue),
            ChDigCommand::Mutations => Some(ChDigViews::Mutations),
            ChDigCommand::ReplicationQueue => Some(ChDigViews::ReplicationQueue),
            ChDigCommand::ReplicatedFetches => Some(ChDigViews::ReplicatedFetches),
            ChDigCommand::Replicas => Some(ChDigViews::Replicas),
            ChDigCommand::Tables => Some(ChDigViews::Tables),
            ChDigCommand::Errors => Some(ChDigViews::Errors),
            ChDigCommand::Backups => Some(ChDigViews::Backups),
            ChDigCommand::Dictionaries => Some(ChDigViews::Dictionaries),
            ChDigCommand::ServerLogs => Some(ChDigViews::ServerLogs),
            ChDigCommand::Loggers => Some(ChDigViews::Loggers),
            ChDigCommand::BackgroundSchedulePool => Some(ChDigViews::BackgroundSchedulePool),
            ChDigCommand::BackgroundSchedulePoolLog => Some(ChDigViews::BackgroundSchedulePoolLog),
            ChDigCommand::TableParts => Some(ChDigViews::TableParts),
            ChDigCommand::AsynchronousInserts => Some(ChDigViews::AsynchronousInserts),
            ChDigCommand::PartLog => Some(ChDigViews::PartLog),
            ChDigCommand::Client => Some(ChDigViews::Client),
            ChDigCommand::Perfetto(_) => None,
        }
    }
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
    pub command: Option<ChDigCommand>,
    #[command(flatten)]
    pub service: ServiceOptions,
    #[clap(skip)]
    pub perfetto: ChDigPerfettoConfig,
}

impl ChDigOptions {
    pub fn start_view(&self) -> Option<ChDigViews> {
        self.command.as_ref().and_then(ChDigCommand::as_view)
    }

    pub fn perfetto_command(&self) -> Option<&PerfettoCommand> {
        match &self.command {
            Some(ChDigCommand::Perfetto(cmd)) => Some(cmd),
            _ => None,
        }
    }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogsOrder {
    #[default]
    Asc,
    Desc,
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
    /// Limit for logs
    #[arg(long, default_value_t = 100000)]
    pub limit: u64,
    /// Sort order for logs (desc returns the newest --limit rows, useful for long backups)
    #[arg(long, value_enum, default_value_t = LogsOrder::Asc)]
    pub logs_order: LogsOrder,
    /// Override server version (for dev builds with features already available). Should include
    /// at least three components (maj.min.patch)
    #[arg(long, hide = true)]
    pub server_version: Option<String>,
    /// Skip unavailable shards in distributed queries
    #[arg(long, action = ArgAction::SetTrue)]
    pub skip_unavailable_shards: bool,
    #[clap(skip)]
    pub history_file: Option<String>,
}

impl ClickHouseOptions {
    pub fn connection_info(&self) -> String {
        if let Some(ref connection) = self.connection {
            connection.clone()
        } else if let Ok(url) = url::Url::parse(&self.url_safe) {
            url.host_str().unwrap_or("localhost").to_string()
        } else {
            self.url_safe.clone()
        }
    }
}

#[derive(Args, Clone)]
pub struct ViewOptions {
    #[arg(
        short('d'),
        long,
        value_parser = |arg: &str| -> Result<time::Duration> {Ok(time::Duration::from_millis(arg.parse()?))},
        default_value = "30000",
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

    /// Use short option -b, like atop(1) has
    #[arg(long, short('b'), default_value = "1hour")]
    /// Begin of the time interval to look at
    pub start: RelativeDateTime,
    #[arg(long, short('e'), default_value = "")]
    /// End of the time interval
    pub end: RelativeDateTime,

    /// Wrap long lines
    #[arg(long, action = ArgAction::SetTrue)]
    pub wrap: bool,

    /// Disable stripping common hostname prefix and suffix in queries and logs views
    #[arg(long, action = ArgAction::SetTrue)]
    pub no_strip_hostname_suffix: bool,

    /// Limit for number of queries to render in queries views
    #[arg(long, default_value_t = 10000)]
    pub queries_limit: u64,
    // TODO: --mouse/--no-mouse (see EXIT_MOUSE_SEQUENCE in termion)
}

#[derive(Args, Clone)]
pub struct ServiceOptions {
    #[arg(long, value_enum)]
    completion: Option<Shell>,
    #[arg(long)]
    /// Log (for debugging chdig itself)
    pub log: Option<String>,
    #[arg(
        long,
        default_value = "https://uzg8q0g12h.eu-central-1.aws.clickhouse.cloud/?user=paste"
    )]
    /// Pastila ClickHouse backend for uploading and sharing flamegraphs
    pub pastila_clickhouse_host: String,
    #[arg(long, default_value = "https://pastila.nl/")]
    /// pastila.nl URL (only to show direct link to pastila in logs)
    pub pastila_url: String,
    /// Path to chdig config file (YAML)
    #[arg(long, env = "CHDIG_CONFIG")]
    pub chdig_config: Option<String>,
}

#[derive(Deserialize, Clone)]
#[serde(default)]
pub struct ChDigPerfettoConfig {
    pub opentelemetry_span_log: bool,
    pub trace_log: bool,
    pub query_metric_log: bool,
    pub part_log: bool,
    pub query_thread_log: bool,
    pub text_log: bool,
    pub text_log_android: bool,
    pub per_server: bool,
    pub metric_log: bool,
    pub asynchronous_metric_log: bool,
    pub asynchronous_insert_log: bool,
    pub error_log: bool,
    pub s3_queue_log: bool,
    pub azure_queue_log: bool,
    pub blob_storage_log: bool,
    pub background_schedule_pool_log: bool,
    pub session_log: bool,
    pub aggregated_zookeeper_log: bool,
}

impl Default for ChDigPerfettoConfig {
    fn default() -> Self {
        Self {
            opentelemetry_span_log: true,
            trace_log: true,
            query_metric_log: false,
            part_log: true,
            query_thread_log: true,
            text_log: true,
            text_log_android: true,
            per_server: true,
            metric_log: true,
            asynchronous_metric_log: false,
            asynchronous_insert_log: true,
            error_log: true,
            s3_queue_log: true,
            azure_queue_log: true,
            blob_storage_log: true,
            background_schedule_pool_log: true,
            session_log: true,
            aggregated_zookeeper_log: false,
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ChDigConfig {
    clickhouse: ChDigClickHouseConfig,
    view: ChDigViewConfig,
    service: ChDigServiceConfig,
    perfetto: ChDigPerfettoConfig,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ChDigClickHouseConfig {
    url: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    secure: Option<bool>,
    config: Option<String>,
    connection: Option<String>,
    cluster: Option<String>,
    history: Option<bool>,
    internal_queries: Option<bool>,
    limit: Option<u64>,
    logs_order: Option<LogsOrder>,
    skip_unavailable_shards: Option<bool>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ChDigViewConfig {
    delay_interval: Option<u64>,
    group_by: Option<bool>,
    no_subqueries: Option<bool>,
    start: Option<String>,
    end: Option<String>,
    wrap: Option<bool>,
    no_strip_hostname_suffix: Option<bool>,
    queries_limit: Option<u64>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ChDigServiceConfig {
    log: Option<String>,
    pastila_clickhouse_host: Option<String>,
    pastila_url: Option<String>,
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
        history_file: yaml_config.history_file,
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
        history_file: xml_config.history_file,
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
    // Try XDG standard directory first
    if let Ok(xdg_config_home) = env::var("XDG_CONFIG_HOME") {
        try_xml!(&format!("{}/clickhouse/config.xml", xdg_config_home));
        try_yaml!(&format!("{}/clickhouse/config.yml", xdg_config_home));
        try_yaml!(&format!("{}/clickhouse/config.yaml", xdg_config_home));
    }

    // Try HOME-based locations
    if let Ok(home) = env::var("HOME") {
        // XDG fallback: ~/.config
        try_xml!(&format!("{}/.config/clickhouse/config.xml", home));
        try_yaml!(&format!("{}/.config/clickhouse/config.yml", home));
        try_yaml!(&format!("{}/.config/clickhouse/config.yaml", home));

        // Legacy location: ~/.clickhouse-client
        try_xml!(&format!("{}/.clickhouse-client/config.xml", home));
        try_yaml!(&format!("{}/.clickhouse-client/config.yml", home));
        try_yaml!(&format!("{}/.clickhouse-client/config.yaml", home));
    }

    // System-wide configuration
    try_xml!("/etc/clickhouse-client/config.xml");
    try_yaml!("/etc/clickhouse-client/config.yml");
    try_yaml!("/etc/clickhouse-client/config.yaml");

    return None;
}

fn read_chdig_config(path: &str) -> Result<ChDigConfig> {
    let file = fs::File::open(path)?;
    let reader = io::BufReader::new(file);
    let doc = YamlDeserializer::from_reader(reader);
    let config = ChDigConfig::deserialize(doc)?;
    return Ok(config);
}

macro_rules! try_chdig_yaml {
    ( $path:expr ) => {
        if path::Path::new($path).exists() {
            log::info!("Loading chdig config {}", $path);
            return Some(read_chdig_config($path));
        }
    };
}

fn try_default_chdig_config() -> Option<Result<ChDigConfig>> {
    if let Ok(xdg_config_home) = env::var("XDG_CONFIG_HOME") {
        try_chdig_yaml!(&format!("{}/chdig/config.yaml", xdg_config_home));
        try_chdig_yaml!(&format!("{}/chdig/config.yml", xdg_config_home));
    }

    if let Ok(home) = env::var("HOME") {
        try_chdig_yaml!(&format!("{}/.config/chdig/config.yaml", home));
        try_chdig_yaml!(&format!("{}/.config/chdig/config.yml", home));

        try_chdig_yaml!(&format!("{}/.chdig.yaml", home));
        try_chdig_yaml!(&format!("{}/.chdig.yml", home));
    }

    try_chdig_yaml!("/etc/chdig/config.yaml");
    try_chdig_yaml!("/etc/chdig/config.yml");

    return None;
}

fn apply_chdig_config(options: &mut ChDigOptions, config: &ChDigConfig) {
    // clickhouse section
    let ch = &config.clickhouse;
    if options.clickhouse.url.is_none() {
        options.clickhouse.url = ch.url.clone();
    }
    if options.clickhouse.host.is_none() {
        options.clickhouse.host = ch.host.clone();
    }
    if options.clickhouse.port.is_none() {
        options.clickhouse.port = ch.port;
    }
    if options.clickhouse.user.is_none() {
        options.clickhouse.user = ch.user.clone();
    }
    if options.clickhouse.password.is_none() {
        options.clickhouse.password = ch.password.clone();
    }
    if !options.clickhouse.secure
        && let Some(secure) = ch.secure
    {
        options.clickhouse.secure = secure;
    }
    if options.clickhouse.config.is_none() {
        options.clickhouse.config = ch.config.clone();
    }
    if options.clickhouse.connection.is_none() {
        options.clickhouse.connection = ch.connection.clone();
    }
    if options.clickhouse.cluster.is_none() {
        options.clickhouse.cluster = ch.cluster.clone();
    }
    if !options.clickhouse.history
        && let Some(history) = ch.history
    {
        options.clickhouse.history = history;
    }
    if !options.clickhouse.internal_queries
        && let Some(internal_queries) = ch.internal_queries
    {
        options.clickhouse.internal_queries = internal_queries;
    }
    if let Some(limit) = ch.limit {
        options.clickhouse.limit = limit;
    }
    if options.clickhouse.logs_order == LogsOrder::Asc
        && let Some(logs_order) = ch.logs_order
    {
        options.clickhouse.logs_order = logs_order;
    }
    if !options.clickhouse.skip_unavailable_shards
        && let Some(skip) = ch.skip_unavailable_shards
    {
        options.clickhouse.skip_unavailable_shards = skip;
    }

    // view section
    let view = &config.view;
    if let Some(delay) = view.delay_interval {
        options.view.delay_interval = time::Duration::from_millis(delay);
    }
    if !options.view.group_by
        && let Some(group_by) = view.group_by
    {
        options.view.group_by = group_by;
    }
    if !options.view.no_subqueries
        && let Some(no_subqueries) = view.no_subqueries
    {
        options.view.no_subqueries = no_subqueries;
    }
    if let Some(ref start) = view.start
        && let Ok(parsed) = RelativeDateTime::from_str(start)
    {
        options.view.start = parsed;
    }
    if let Some(ref end) = view.end
        && let Ok(parsed) = RelativeDateTime::from_str(end)
    {
        options.view.end = parsed;
    }
    if !options.view.wrap
        && let Some(wrap) = view.wrap
    {
        options.view.wrap = wrap;
    }
    if !options.view.no_strip_hostname_suffix
        && let Some(no_strip) = view.no_strip_hostname_suffix
    {
        options.view.no_strip_hostname_suffix = no_strip;
    }
    if let Some(queries_limit) = view.queries_limit {
        options.view.queries_limit = queries_limit;
    }

    // service section
    let svc = &config.service;
    if options.service.log.is_none() {
        options.service.log = svc.log.clone();
    }
    if let Some(ref host) = svc.pastila_clickhouse_host {
        options.service.pastila_clickhouse_host = host.clone();
    }
    if let Some(ref url) = svc.pastila_url {
        options.service.pastila_url = url.clone();
    }

    // perfetto section
    options.perfetto = config.perfetto.clone();
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

pub fn is_cloud_host(host: &str) -> bool {
    let host = host.to_lowercase();
    host.ends_with(".clickhouse.cloud")
        || host.ends_with(".clickhouse-staging.com")
        || host.ends_with(".clickhouse-dev.com")
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
    let mut has_host = url.host().is_some();
    if !has_host {
        url.set_host(Some("127.1"))?;
    }

    // Apply clickhouse-client compatible options
    if let Some(host) = &options.host {
        url.set_host(Some(host))?;
        has_host = true;
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

        if options.history_file.is_none() {
            options.history_file = config.history_file;
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
                if ca_certificate.is_none() {
                    ca_certificate = c.ca_certificate.clone();
                }
                if client_certificate.is_none() {
                    client_certificate = c.client_certificate.clone();
                }
                if client_private_key.is_none() {
                    client_private_key = c.client_private_key.clone();
                }
                if options.history_file.is_none() {
                    options.history_file = c.history_file.clone();
                }
            }

            if !connection_found {
                panic!("Connection {} was not found", connection);
            }
        }
    } else if connection.is_some() {
        panic!("No client config had been read, while --connection was set");
    }

    // Cloud hosts always use secure connections unless explicitly disabled
    if secure.is_none() && is_cloud_host(&url.host().ok_or_else(|| anyhow!("No host"))?.to_string())
    {
        secure = Some(true);
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
        let host_str = url.host().ok_or_else(|| anyhow!("No host"))?.to_string();
        let pairs: HashMap<_, _> = url_safe.query_pairs().into_owned().collect();
        let is_local = is_local_address(&host_str);
        let is_cloud = is_cloud_host(&host_str);
        let mut mut_pairs = url.query_pairs_mut();
        // Enable compression in non-local network (in the same way as clickhouse does by default)
        if !pairs.contains_key("compression") && !is_local {
            mut_pairs.append_pair("compression", "lz4");
        }
        if !pairs.contains_key("connection_timeout") {
            if is_cloud {
                // Cloud services may need time to wake up from idle state
                mut_pairs.append_pair("connection_timeout", "600s");
            } else {
                // default is: 500ms (too small)
                mut_pairs.append_pair("connection_timeout", "5s");
            }
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
        if options.skip_unavailable_shards {
            mut_pairs.append_pair("skip_unavailable_shards", "1");
        }
    }

    options.url = Some(url.to_string());

    return Ok(());
}

fn adjust_defaults(options: &mut ChDigOptions) -> Result<()> {
    // Load and apply chdig config before clickhouse client config,
    // so that e.g. clickhouse.config from chdig config feeds into the client config loading.
    let chdig_config = if let Some(ref path) = options.service.chdig_config {
        Some(read_chdig_config(path)?)
    } else if let Some(config) = try_default_chdig_config() {
        Some(config?)
    } else {
        None
    };
    if let Some(ref chdig_config) = chdig_config {
        apply_chdig_config(options, chdig_config);
    }

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

    if let Some(cmd) = options.perfetto_command()
        && cmd.query_id.is_none()
        && !cmd.server
    {
        return Err(anyhow!(
            "perfetto command requires --query-id/--query_id or --server"
        ));
    }

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
    fn test_config_connections_host() {
        let config = read_yaml_clickhouse_client_config("tests/configs/connections.yaml").ok();
        let mut options = ClickHouseOptions {
            connection: Some("play-tls".into()),
            host: Some("localhost".into()),
            ..Default::default()
        };
        clickhouse_url_defaults(&mut options, config).unwrap();
        assert_eq!(
            parse_url(&options).unwrap().host().unwrap().to_string(),
            "localhost"
        );
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

    #[test]
    fn test_cloud_defaults() {
        {
            let mut options = ClickHouseOptions {
                host: Some("uzg8q0g12h.eu-central-1.aws.clickhouse.cloud".into()),
                ..Default::default()
            };
            clickhouse_url_defaults(&mut options, None).unwrap();
            let url = parse_url(&options).unwrap();
            let args: HashMap<_, _> = url.query_pairs().into_owned().collect();

            assert_eq!(args.get("secure"), Some(&"true".into()));
            assert_eq!(args.get("connection_timeout"), Some(&"600s".into()));
        }

        // Note, checking for ClickHouseOptions{secure: false} does not make sense, since it is the default

        {
            let mut options = ClickHouseOptions {
                url: Some("uzg8q0g12h.eu-central-1.aws.clickhouse.cloud/?secure=false&connection_timeout=1ms".into()),
                ..Default::default()
            };
            clickhouse_url_defaults(&mut options, None).unwrap();
            let url = parse_url(&options).unwrap();
            let args: HashMap<_, _> = url.query_pairs().into_owned().collect();

            assert_eq!(args.get("secure"), Some(&"false".into()));
            assert_eq!(args.get("connection_timeout"), Some(&"1ms".into()));
        }
    }

    #[test]
    fn test_chdig_config_empty() {
        let config = read_chdig_config("tests/configs/chdig_empty.yaml").unwrap();
        assert!(config.clickhouse.url.is_none());
        assert!(config.clickhouse.host.is_none());
        assert!(config.view.delay_interval.is_none());
        assert!(config.service.log.is_none());
    }

    #[test]
    fn test_chdig_config_basic() {
        let config = read_chdig_config("tests/configs/chdig_basic.yaml").unwrap();

        assert_eq!(
            config.clickhouse.url.as_deref(),
            Some("tcp://config-host:9000")
        );
        assert_eq!(config.clickhouse.host.as_deref(), Some("config-host"));
        assert_eq!(config.clickhouse.port, Some(9440));
        assert_eq!(config.clickhouse.user.as_deref(), Some("config_user"));
        assert_eq!(config.clickhouse.password.as_deref(), Some("config_pass"));
        assert_eq!(config.clickhouse.secure, Some(true));
        assert_eq!(config.clickhouse.cluster.as_deref(), Some("my_cluster"));
        assert_eq!(config.clickhouse.history, Some(true));
        assert_eq!(config.clickhouse.internal_queries, Some(true));
        assert_eq!(config.clickhouse.limit, Some(50000));
        assert_eq!(config.clickhouse.skip_unavailable_shards, Some(true));

        assert_eq!(config.view.delay_interval, Some(5000));
        assert_eq!(config.view.group_by, Some(true));
        assert_eq!(config.view.no_subqueries, Some(true));
        assert_eq!(config.view.start.as_deref(), Some("2hours"));
        assert_eq!(config.view.end.as_deref(), Some("30min"));
        assert_eq!(config.view.wrap, Some(true));
        assert_eq!(config.view.no_strip_hostname_suffix, Some(true));
        assert_eq!(config.view.queries_limit, Some(500));

        assert_eq!(config.service.log.as_deref(), Some("/tmp/chdig.log"));
        assert_eq!(
            config.service.pastila_clickhouse_host.as_deref(),
            Some("https://custom.host/")
        );
        assert_eq!(
            config.service.pastila_url.as_deref(),
            Some("https://custom.pastila/")
        );
    }

    #[test]
    fn test_chdig_config_partial() {
        let config = read_chdig_config("tests/configs/chdig_partial.yaml").unwrap();

        assert_eq!(config.clickhouse.host.as_deref(), Some("partial-host"));
        assert_eq!(config.clickhouse.user.as_deref(), Some("partial_user"));
        assert!(config.clickhouse.url.is_none());
        assert!(config.clickhouse.port.is_none());
        assert!(config.clickhouse.secure.is_none());

        assert_eq!(config.view.delay_interval, Some(10000));
        assert!(config.view.group_by.is_none());
        assert!(config.view.wrap.is_none());

        assert!(config.service.log.is_none());
    }

    #[test]
    fn test_chdig_config_apply_clickhouse() {
        let config = read_chdig_config("tests/configs/chdig_basic.yaml").unwrap();
        let mut options = ChDigOptions::parse_from(["chdig"]);
        apply_chdig_config(&mut options, &config);

        assert_eq!(options.clickhouse.host.as_deref(), Some("config-host"));
        assert_eq!(options.clickhouse.user.as_deref(), Some("config_user"));
        assert_eq!(options.clickhouse.password.as_deref(), Some("config_pass"));
        assert_eq!(options.clickhouse.port, Some(9440));
        assert_eq!(options.clickhouse.secure, true);
        assert_eq!(options.clickhouse.cluster.as_deref(), Some("my_cluster"));
        assert_eq!(options.clickhouse.history, true);
        assert_eq!(options.clickhouse.internal_queries, true);
        assert_eq!(options.clickhouse.limit, 50000);
        assert_eq!(options.clickhouse.skip_unavailable_shards, true);
    }

    #[test]
    fn test_chdig_config_apply_view() {
        let config = read_chdig_config("tests/configs/chdig_basic.yaml").unwrap();
        let mut options = ChDigOptions::parse_from(["chdig"]);
        apply_chdig_config(&mut options, &config);

        assert_eq!(
            options.view.delay_interval,
            time::Duration::from_millis(5000)
        );
        assert_eq!(options.view.group_by, true);
        assert_eq!(options.view.no_subqueries, true);
        assert_eq!(options.view.wrap, true);
        assert_eq!(options.view.no_strip_hostname_suffix, true);
        assert_eq!(options.view.queries_limit, 500);
        assert_eq!(options.service.log.as_deref(), Some("/tmp/chdig.log"));
        assert_eq!(
            options.service.pastila_clickhouse_host,
            "https://custom.host/"
        );
        assert_eq!(options.service.pastila_url, "https://custom.pastila/");
    }

    #[test]
    fn test_chdig_config_perfetto() {
        let config = read_chdig_config("tests/configs/chdig_basic.yaml").unwrap();

        assert_eq!(config.perfetto.opentelemetry_span_log, true);
        assert_eq!(config.perfetto.trace_log, true);
        assert_eq!(config.perfetto.query_metric_log, true);
        assert_eq!(config.perfetto.part_log, false);
        assert_eq!(config.perfetto.query_thread_log, true);
        assert_eq!(config.perfetto.text_log, false);

        let mut options = ChDigOptions::parse_from(["chdig"]);
        apply_chdig_config(&mut options, &config);

        assert_eq!(options.perfetto.opentelemetry_span_log, true);
        assert_eq!(options.perfetto.part_log, false);
        assert_eq!(options.perfetto.query_metric_log, true);
    }

    #[test]
    fn test_chdig_config_perfetto_defaults() {
        let config = read_chdig_config("tests/configs/chdig_empty.yaml").unwrap();

        assert_eq!(config.perfetto.opentelemetry_span_log, true);
        assert_eq!(config.perfetto.trace_log, true);
        assert_eq!(config.perfetto.query_metric_log, false);
        assert_eq!(config.perfetto.part_log, true);
        assert_eq!(config.perfetto.query_thread_log, true);
        assert_eq!(config.perfetto.text_log, true);
    }

    #[test]
    fn test_chdig_config_cli_overrides_config() {
        let config = read_chdig_config("tests/configs/chdig_basic.yaml").unwrap();
        let mut options = ChDigOptions::parse_from([
            "chdig",
            "--host",
            "cli-host",
            "--user",
            "cli_user",
            "--secure",
            "--log",
            "/tmp/cli.log",
        ]);
        apply_chdig_config(&mut options, &config);

        // Option<T> fields: CLI wins when set
        assert_eq!(options.clickhouse.host.as_deref(), Some("cli-host"));
        assert_eq!(options.clickhouse.user.as_deref(), Some("cli_user"));
        assert_eq!(options.service.log.as_deref(), Some("/tmp/cli.log"));

        // Bool flags: CLI true wins
        assert_eq!(options.clickhouse.secure, true);

        // Option<T> fields not set on CLI come from config
        assert_eq!(options.clickhouse.password.as_deref(), Some("config_pass"));
        assert_eq!(options.clickhouse.cluster.as_deref(), Some("my_cluster"));

        // Non-Option fields: config always applies
        assert_eq!(options.clickhouse.limit, 50000);
        assert_eq!(
            options.view.delay_interval,
            time::Duration::from_millis(5000)
        );
    }

    #[test]
    fn test_perfetto_query_cli_options() {
        let options = parse_from([
            "chdig",
            "perfetto",
            "--query_id",
            "query-123",
            "--output",
            "/tmp/query.pftrace",
        ])
        .unwrap();

        let cmd = options.perfetto_command().unwrap();
        assert_eq!(cmd.query_id.as_deref(), Some("query-123"));
        assert!(!cmd.server);
        assert_eq!(cmd.output.as_deref(), Some("/tmp/query.pftrace"));
    }

    #[test]
    fn test_perfetto_server_cli_options() {
        let options = parse_from([
            "chdig", "perfetto", "--server", "--start", "10minute", "--end", "5minute",
        ])
        .unwrap();

        let cmd = options.perfetto_command().unwrap();
        assert!(cmd.server);
        assert!(cmd.query_id.is_none());
    }

    #[test]
    fn test_perfetto_output_requires_export_mode() {
        let err = match parse_from(["chdig", "perfetto", "--output", "/tmp/out.pftrace"]) {
            Ok(_) => panic!("expected parse_from() to fail"),
            Err(err) => err,
        };
        assert_eq!(
            err.to_string(),
            "perfetto command requires --query-id/--query_id or --server"
        );
    }
}
