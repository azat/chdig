use crate::{
    common::RelativeDateTime,
    interpreter::{ContextArc, options::ChDigViews},
    tui::{
        App, Dialog, DummyView, LinearLayout, NamedView, Resizable, TextView, ViewProvider,
        views::sql_query_view::Row as QueryResultRow, views::text_log_view::TextLogView,
    },
};
use std::collections::HashMap;

pub struct BackupsViewProvider;

impl ViewProvider for BackupsViewProvider {
    fn name(&self) -> &'static str {
        "Backups"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Backups
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        let columns = vec![
            "name",
            "status",
            "error",
            "start_time",
            "end_time",
            "total_size",
            "query_id _query_id",
        ];

        let backups_logs_callback =
            move |app: &mut App, columns: Vec<&'static str>, row: QueryResultRow| {
                let mut map = HashMap::new();
                columns.iter().zip(row.0.iter()).for_each(|(c, r)| {
                    map.insert(c.to_string(), r);
                });

                let context = app.user_data::<ContextArc>().unwrap().clone();
                app.add_layer(Dialog::around(
                    LinearLayout::vertical()
                        .child(TextView::new("Logs:").center())
                        .child(DummyView.fixed_height(1))
                        .child(NamedView::new(
                            "backups_logs",
                            TextLogView::new(
                                "backups_logs",
                                context,
                                crate::interpreter::TextLogArguments {
                                    query_ids: Some(vec![map["_query_id"].to_string()]),
                                    logger_names: None,
                                    hostname: None,
                                    message_filter: None,
                                    max_level: None,
                                    start: map["start_time"].as_datetime().unwrap(),
                                    end: RelativeDateTime::from(map["end_time"].as_datetime()),
                                },
                            ),
                        )),
                ));
                app.focus_name("backups_logs");
            };

        // TODO:
        // - order by elapsed time
        super::render_from_clickhouse_query(
            app,
            super::RenderFromClickHouseQueryArguments {
                context,
                table: &["backups"],
                join: None,
                filter: None,
                sort_by: "total_size",
                columns,
                columns_to_compare: vec!["name"],
                on_submit: Some(backups_logs_callback),
                settings: HashMap::<&str, i32>::new(),
            },
        );
    }
}
