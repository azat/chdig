use crate::interpreter::{clickhouse::Columns, options::ChDigOptions, ClickHouse, Worker};
use std::sync;

pub type ContextArc = sync::Arc<sync::Mutex<Context>>;

pub struct Context {
    pub options: ChDigOptions,

    pub clickhouse: ClickHouse,
    pub server_version: String,
    pub worker: Worker,

    pub cb_sink: cursive::CbSink,

    //
    // Events specific
    //
    pub processes: Option<Columns>,
    // For get_query_logs()
    // TODO: move this logic into the Worker itself (to the Event somehow)
    pub query_id: String,
    pub query_logs: Option<Columns>,
}

impl Context {
    pub async fn new(options: ChDigOptions, cb_sink: cursive::CbSink) -> ContextArc {
        let mut clickhouse = ClickHouse::new(options.clickhouse.clone());
        let server_version = clickhouse.version().await;
        let worker = Worker::new();

        let context = sync::Arc::new(sync::Mutex::new(Context {
            options,
            clickhouse,
            server_version,
            worker,
            cb_sink,

            processes: None,
            query_id: "".to_string(),
            query_logs: None,
        }));

        context.lock().unwrap().worker.start(context.clone());

        return context;
    }
}
