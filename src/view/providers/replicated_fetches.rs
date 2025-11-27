use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::ViewProvider,
};
use cursive::Cursive;
use std::collections::HashMap;

pub struct ReplicatedFetchesViewProvider;

impl ViewProvider for ReplicatedFetchesViewProvider {
    fn name(&self) -> &'static str {
        "Fetches"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::ReplicatedFetches
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        let columns = vec![
            "database",
            "table",
            "result_part_name part",
            "elapsed",
            "progress",
            "total_size_bytes_compressed size",
            "bytes_read_compressed bytes",
        ];

        // TODO: on_submit show last related log messages
        super::render_from_clickhouse_query(
            siv,
            super::RenderFromClickHouseQueryArguments {
                context,
                table: "replicated_fetches",
                join: None,
                filter: None,
                sort_by: "elapsed",
                columns,
                columns_to_compare: vec!["database", "table", "part"],
                on_submit: Some(super::query_result_show_row),
                settings: HashMap::<&str, i32>::new(),
            },
        );
    }
}
