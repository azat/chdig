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
        let mut columns = vec![
            "file_name",
            "rows_processed",
            "status",
            "assumeNotNull(processing_start_time) start_time",
            "exception",
        ];

        // TODO: on_submit show last related log messages
        super::show_query_result_view(
            siv,
            context,
            "s3queue",
            None,
            None,
            "start_time",
            &mut columns,
            1,
            Some(super::query_result_show_row),
            &HashMap::new(),
        );
    }
}
