// Registered in providers::register() once the views menu wiring is ported.

use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    tui::{App, ViewProvider},
};
use std::collections::HashMap;

fn show_queue(app: &mut App, context: ContextArc, table: &'static [&'static str]) {
    let columns = vec![
        "file_name",
        "rows_processed",
        "status",
        "assumeNotNull(processing_start_time) start_time",
        "exception",
    ];

    // TODO: on_submit show last related log messages
    super::render_from_clickhouse_query(
        app,
        super::RenderFromClickHouseQueryArguments {
            context,
            table,
            join: None,
            filter: None,
            sort_by: "start_time",
            columns,
            columns_to_compare: vec!["file_name"],
            on_submit: Some(super::query_result_show_row),
            settings: HashMap::<&str, i32>::new(),
        },
    );
}

pub struct S3QueueViewProvider;

impl ViewProvider for S3QueueViewProvider {
    fn name(&self) -> &'static str {
        "S3Queue"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::S3Queue
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        show_queue(app, context, &["s3queue_metadata_cache", "s3queue"]);
    }
}

pub struct AzureQueueViewProvider;

impl ViewProvider for AzureQueueViewProvider {
    fn name(&self) -> &'static str {
        "AzureQueue"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::AzureQueue
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        show_queue(app, context, &["azure_queue_metadata_cache", "azure_queue"]);
    }
}
