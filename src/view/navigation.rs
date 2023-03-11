use crate::{interpreter::ContextArc, view};
use cursive::{
    theme::Style,
    utils::span::SpannedString,
    view::View as _,
    view::{Nameable, Resizable},
    views::{
        Dialog, DummyView, FixedLayout, Layer, LinearLayout, OnLayoutView, TextContent, TextView,
    },
    Cursive, {Rect, Vec2},
};

pub trait Navigation {
    fn statusbar(&mut self, main_content: impl Into<SpannedString<Style>>);
    fn set_statusbar_content(&mut self, content: impl Into<SpannedString<Style>>);

    fn show_clickhouse_processes(&mut self, context: ContextArc);
    fn show_clickhouse_merges(&mut self, context: ContextArc);
    fn show_clickhouse_replication_queue(&mut self, context: ContextArc);
    fn show_clickhouse_replicated_fetches(&mut self, context: ContextArc);
    fn show_clickhouse_replicas(&mut self, context: ContextArc);

    fn show_query_result_view(
        &mut self,
        context: ContextArc,
        table: &'static str,
        sort_by: &'static str,
        columns: &mut Vec<&'static str>,
    );
}

impl Navigation for Cursive {
    fn statusbar(&mut self, main_content: impl Into<SpannedString<Style>>) {
        // NOTE: This is a copy-paste from cursive examples
        let main_text_content = TextContent::new(main_content);
        self.screen_mut().add_transparent_layer(
            OnLayoutView::new(
                FixedLayout::new().child(
                    Rect::from_point(Vec2::zero()),
                    Layer::new(
                        LinearLayout::horizontal()
                            .child(
                                TextView::new_with_content(main_text_content.clone())
                                    .with_name("main_status"),
                            )
                            .child(DummyView.fixed_width(1))
                            .child(TextView::new("").with_name("status")),
                    )
                    .full_width(),
                ),
                |layout, size| {
                    layout.set_child_position(0, Rect::from_size((0, size.y - 1), (size.x, 1)));
                    layout.layout(size);
                },
            )
            .full_screen(),
        );
    }
    fn set_statusbar_content(&mut self, content: impl Into<SpannedString<Style>>) {
        self.call_on_name("status", |text_view: &mut TextView| {
            text_view.set_content(content);
        })
        .expect("set_status")
    }

    fn show_clickhouse_processes(&mut self, context: ContextArc) {
        if self.find_name::<view::ProcessesView>("processes").is_some() {
            return;
        }

        while self.screen_mut().len() > 1 {
            self.pop_layer();
        }

        self.add_layer(
            LinearLayout::vertical()
                // TODO: show summary for all views
                .child(view::SummaryView::new().with_name("summary"))
                .child(
                    Dialog::around(
                        view::ProcessesView::new(context.clone())
                            .expect("Cannot get processlist")
                            .with_name("processes")
                            .min_size((500, 200)),
                    )
                    .title("Queries"),
                ),
        );
    }

    fn show_clickhouse_merges(&mut self, context: ContextArc) {
        let table = "system.merges";
        let mut columns = vec![
            "database",
            "table",
            "result_part_name part",
            "elapsed",
            "progress",
            "num_parts parts",
            "is_mutation mutation",
            "total_size_bytes_compressed size",
            "rows_read",
            "rows_written",
            "memory_usage memory",
        ];

        // TODO: on_submit show last related log messages
        self.show_query_result_view(context, table, "elapsed", &mut columns);
    }

    fn show_clickhouse_replication_queue(&mut self, context: ContextArc) {
        let table = "system.replication_queue";
        let mut columns = vec![
            "database",
            "table",
            "create_time",
            "new_part_name part",
            "is_currently_executing executing",
            "num_tries tries",
            "last_exception exception",
            "num_postponed postponed",
            "postpone_reason reason",
        ];

        // TODO: on_submit show last related log messages
        self.show_query_result_view(context, table, "tries", &mut columns);
    }

    fn show_clickhouse_replicated_fetches(&mut self, context: ContextArc) {
        let table = "system.replicated_fetches";
        let mut columns = vec![
            "database",
            "table",
            "result_part_name part",
            "elapsed",
            "progress",
            "total_size_bytes_compressed size",
            "bytes_read_compressed bytes",
        ];

        // TODO: on_submit show last related log messages
        self.show_query_result_view(context, table, "elapsed", &mut columns);
    }

    fn show_clickhouse_replicas(&mut self, context: ContextArc) {
        let table = "system.replicas";
        let mut columns = vec![
            "database",
            "table",
            "is_readonly readonly",
            "parts_to_check",
            "queue_size queue",
            "absolute_delay delay",
            "last_queue_update last_update",
        ];

        // TODO: on_submit show last related log messages
        self.show_query_result_view(context, table, "queue", &mut columns);
    }

    fn show_query_result_view(
        &mut self,
        context: ContextArc,
        table: &'static str,
        sort_by: &'static str,
        columns: &mut Vec<&'static str>,
    ) {
        if self.find_name::<view::QueryResultView>(table).is_some() {
            return;
        }

        while self.screen_mut().len() > 1 {
            self.pop_layer();
        }

        let cluster = context.lock().unwrap().options.clickhouse.cluster.is_some();
        if cluster {
            columns.insert(0, "hostName() host");
        }

        let dbtable = context.lock().unwrap().clickhouse.get_table_name(table);
        let query = format!("select {} from {}", columns.join(", "), dbtable);

        self.add_layer(
            Dialog::around(
                view::QueryResultView::new(context.clone(), table, sort_by, columns.clone(), query)
                    .expect(&format!("Cannot get {}", table))
                    .with_name(table)
                    .min_size((500, 200)),
            )
            .title(table),
        );
    }
}
