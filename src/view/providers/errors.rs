use crate::{
    common::RelativeDateTime,
    interpreter::{ContextArc, options::ChDigViews},
    view::{self, QueryResultRow, TextLogView, ViewProvider, navigation::Navigation},
};
use chrono::{DateTime, Duration, Local};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
    views::{Dialog, DummyView, LinearLayout, NamedView, TextView},
};
use std::collections::HashMap;

pub struct ErrorsViewProvider;

impl ViewProvider for ErrorsViewProvider {
    fn name(&self) -> &'static str {
        "Errors"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Errors
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        if siv.has_view("errors") {
            return;
        }

        let columns = vec![
            "name",
            "sum(value) total",
            "total bar",
            "max(last_error_time) error_time",
            // "toValidUTF8(last_error_message) _error_message",
            "arrayStringConcat(arrayMap(addr -> concat(addressToLine(addr), '::', demangle(addressToSymbol(addr))), argMax(last_error_trace, last_error_time)), '\n') _error_trace",
        ];
        let columns_to_compare = vec!["name"];

        let (dbtable, clickhouse, selected_host) = {
            let ctx = context.lock().unwrap();
            (
                ctx.clickhouse.get_table_name("system", "errors"),
                ctx.clickhouse.clone(),
                ctx.selected_host.clone(),
            )
        };

        let host_filter = clickhouse.get_host_filter_clause(selected_host.as_ref());
        let where_clause = if host_filter.is_empty() {
            String::new()
        } else {
            format!("WHERE 1 {}", host_filter)
        };

        let query = format!(
            "SELECT {} FROM {} {} GROUP BY name SETTINGS allow_introspection_functions=1",
            columns.join(", "),
            dbtable,
            where_clause,
        );

        siv.drop_main_view();

        let errors_logs_callback =
            |siv: &mut Cursive, columns: Vec<&'static str>, row: QueryResultRow| {
                let row_data = row.0;

                let mut map = HashMap::<String, String>::new();
                columns.iter().zip(row_data.iter()).for_each(|(c, r)| {
                    map.insert(c.to_string(), r.to_string());
                });

                let error_time = map
                    .get("error_time")
                    .and_then(|t| t.parse::<DateTime<Local>>().ok())
                    .unwrap_or_else(Local::now);
                let error_name = map.get("name").map(|s| s.to_string()).unwrap_or_default();

                let context = siv.user_data::<ContextArc>().unwrap().clone();

                // Show logs for 1 minute before and after the error time
                // (Note, we need to add at least 1 second to error_time, otherwise it will be
                // filtered out by event_time_microseconds condition)
                let offset = Duration::try_minutes(1).unwrap_or_default();
                let end_time = error_time + offset;
                let start_time = error_time - offset;

                siv.add_layer(Dialog::around(
                    LinearLayout::vertical()
                        .child(TextView::new(format!("Logs for error: {}", error_name)).center())
                        .child(DummyView.fixed_height(1))
                        .child(NamedView::new(
                            "error_logs",
                            TextLogView::new(
                                "error_logs",
                                context,
                                crate::interpreter::TextLogArguments {
                                    query_ids: None,
                                    logger_names: None,
                                    hostname: None,
                                    message_filter: Some(error_name),
                                    max_level: Some("Warning".to_string()),
                                    start: start_time,
                                    end: RelativeDateTime::from(end_time),
                                },
                            ),
                        )),
                ));
                siv.focus_name("error_logs").unwrap();
            };

        let mut view = view::SQLQueryView::new(
            context.clone(),
            "errors",
            "total",
            columns,
            columns_to_compare,
            query,
        )
        .unwrap_or_else(|_| panic!("Cannot get errors"));
        view.get_inner_mut().set_on_submit(errors_logs_callback);
        view.get_inner_mut().set_title("errors");
        view.get_inner_mut().set_bar_columns(vec![("bar", "total")]);

        siv.set_main_view(view.with_name("errors").full_screen());
        siv.focus_name("errors").unwrap();
    }
}
