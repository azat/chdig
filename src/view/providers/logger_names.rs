use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::{self, Navigation, TextLogView, ViewProvider},
};
use chrono::{DateTime, Local};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
    views::{Dialog, DummyView, LinearLayout, NamedView, TextView},
};
use std::collections::HashMap;

pub struct LoggerNamesViewProvider;

impl ViewProvider for LoggerNamesViewProvider {
    fn name(&self) -> &'static str {
        "Loggers"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Loggers
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        if siv.has_view("logger_names") {
            return;
        }

        let (view_options, cluster, selected_host_check) = {
            let ctx = context.lock().unwrap();
            (
                ctx.options.view.clone(),
                ctx.options.clickhouse.cluster.is_some(),
                ctx.selected_host.clone(),
            )
        };
        let start = DateTime::<Local>::from(view_options.start);
        let end = view_options.end;

        let mut columns = vec![
            "logger_name::String logger_name",
            "count() count",
            "countIf(level = 'Fatal') fatal",
            "countIf(level = 'Critical') critical",
            "countIf(level = 'Error') error",
            "countIf(level = 'Warning') warning",
            "countIf(level = 'Notice') notice",
            "countIf(level = 'Information') information",
            "countIf(level = 'Debug') debug",
            "countIf(level = 'Trace') trace",
        ];

        // Only show hostname column when in cluster mode AND no host filter is active
        let columns_to_compare = if cluster && selected_host_check.is_none() {
            columns.insert(0, "hostName() host");
            vec!["host", "logger_name"]
        } else {
            vec!["logger_name"]
        };

        let logger_names_callback =
            move |siv: &mut Cursive, columns: Vec<&'static str>, row: view::QueryResultRow| {
                let row = row.0;
                let mut map = HashMap::<String, String>::new();
                columns.iter().zip(row.iter()).for_each(|(c, r)| {
                    map.insert(c.to_string(), r.to_string());
                });

                let logger_name = map.get("logger_name").unwrap().clone();
                let context = siv.user_data::<ContextArc>().unwrap().clone();
                let view_options = context.lock().unwrap().options.view.clone();

                siv.add_layer(Dialog::around(
                    LinearLayout::vertical()
                        .child(TextView::new(format!("Logs for logger: {}", logger_name)).center())
                        .child(DummyView.fixed_height(1))
                        .child(NamedView::new(
                            "logger_logs",
                            TextLogView::new(
                                "logger_logs",
                                context,
                                crate::interpreter::TextLogArguments {
                                    query_ids: None,
                                    logger_names: Some(vec![logger_name]),
                                    hostname: None,
                                    message_filter: None,
                                    max_level: None,
                                    start: DateTime::<Local>::from(view_options.start),
                                    end: view_options.end,
                                },
                            ),
                        )),
                ));
                siv.focus_name("logger_logs").unwrap();
            };

        // Build the query with time filtering
        let (dbtable, clickhouse, selected_host, limit) = {
            let ctx = context.lock().unwrap();
            (
                ctx.clickhouse.get_table_name("system", "text_log"),
                ctx.clickhouse.clone(),
                ctx.selected_host.clone(),
                ctx.options.clickhouse.limit,
            )
        };

        let start_nanos = start
            .timestamp_nanos_opt()
            .ok_or(anyhow::anyhow!("Invalid start time"))
            .unwrap();
        let end_datetime = end.to_sql_datetime_64().unwrap_or_default();

        let host_filter = clickhouse.get_host_filter_clause(selected_host.as_ref());
        let host_where = if host_filter.is_empty() {
            String::new()
        } else {
            format!("\n                {}", host_filter)
        };

        let query = format!(
            r#"
            WITH
                fromUnixTimestamp64Nano({}) AS start_time_,
                {} AS end_time_
            SELECT {}
            FROM {}
            WHERE
                event_date >= toDate(start_time_) AND event_time >= toDateTime(start_time_) AND event_time_microseconds > start_time_
                AND event_date <= toDate(end_time_) AND event_time <= toDateTime(end_time_) AND event_time_microseconds <= end_time_{}
            GROUP BY {}
            ORDER BY count DESC
            LIMIT {}
            "#,
            start_nanos,
            end_datetime,
            columns.join(", "),
            dbtable,
            host_where,
            if cluster {
                "host, logger_name"
            } else {
                "logger_name"
            },
            limit,
        );

        siv.drop_main_view();

        let mut view = view::SQLQueryView::new(
            context.clone(),
            "logger_names",
            "count",
            columns.clone(),
            columns_to_compare,
            query,
        )
        .unwrap_or_else(|_| panic!("Cannot get logger_names"));
        view.get_inner_mut().set_on_submit(logger_names_callback);
        view.get_inner_mut().set_title("Loggers");

        siv.set_main_view(view.with_name("logger_names").full_screen());
        siv.focus_name("logger_names").unwrap();
    }
}
