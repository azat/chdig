use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::ViewProvider,
};
use cursive::Cursive;
use std::collections::HashMap;

pub struct ReplicationQueueViewProvider;

impl ViewProvider for ReplicationQueueViewProvider {
    fn name(&self) -> &'static str {
        "Replication queue"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::ReplicationQueue
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        let mut columns = vec![
            "database",
            "table",
            "type",
            "new_part_name part",
            "create_time",
            "is_currently_executing executing",
            "num_tries tries",
            "last_exception exception",
            "num_postponed postponed",
            "postpone_reason reason",
        ];

        // TODO: on_submit show last related log messages
        super::show_query_result_view(
            siv,
            context,
            "replication_queue",
            None,
            None,
            "tries",
            &mut columns,
            3,
            Some(super::query_result_show_row),
            &HashMap::new(),
        );
    }
}
