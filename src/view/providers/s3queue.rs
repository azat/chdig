use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::ViewProvider,
};
use cursive::Cursive;
use std::collections::HashMap;

pub struct S3QueueViewProvider;

impl ViewProvider for S3QueueViewProvider {
    fn name(&self) -> &'static str {
        "S3Queue"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::S3Queue
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        let columns = vec![
            "file_name",
            "rows_processed",
            "status",
            "assumeNotNull(processing_start_time) start_time",
            "exception",
        ];

        // TODO: on_submit show last related log messages
        super::render_from_clickhouse_query(
            siv,
            super::RenderFromClickHouseQueryArguments {
                context,
                table: "s3queue",
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
}
