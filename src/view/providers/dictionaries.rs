use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::ViewProvider,
};
use cursive::Cursive;
use std::collections::HashMap;

pub struct DictionariesViewProvider;

impl ViewProvider for DictionariesViewProvider {
    fn name(&self) -> &'static str {
        "Dictionaries"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Dictionaries
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        let columns = vec![
            "name",
            "status::String status",
            "source",
            "bytes_allocated memory",
            "query_count queries",
            "found_rate",
            "load_factor",
            "last_successful_update_time last_update",
            "loading_duration",
            "last_exception",
            "origin",
        ];

        super::show_query_result_view(
            siv,
            super::QueryResultViewParams {
                context,
                table: "dictionaries",
                join: None,
                filter: None,
                sort_by: "memory",
                columns,
                columns_to_compare: 1,
                on_submit: Some(super::query_result_show_row),
                settings: HashMap::new(),
            },
        );
    }
}
