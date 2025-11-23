use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::ViewProvider,
};
use cursive::Cursive;
use std::collections::HashMap;

pub struct MutationsViewProvider;

impl ViewProvider for MutationsViewProvider {
    fn name(&self) -> &'static str {
        "Mutations"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Mutations
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        let columns = vec![
            "database",
            "table",
            "mutation_id",
            "command",
            "create_time",
            "parts_to_do parts",
            "is_done",
            "latest_fail_reason",
            "latest_fail_time",
        ];

        // TODO:
        // - on_submit show assigned merges (but first, need to expose enough info in system tables)
        // - sort by create_time OR latest_fail_time
        super::show_query_result_view(
            siv,
            super::QueryResultViewParams {
                context,
                table: "mutations",
                join: None,
                filter: Some("is_done = 0"),
                sort_by: "latest_fail_time",
                columns,
                columns_to_compare: 3,
                on_submit: Some(super::query_result_show_row),
                settings: HashMap::new(),
            },
        );
    }
}
