use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    tui::{
        App, Nameable, NamedView, Navigation, Resizable, ViewProvider,
        views::sql_query_view::{Row as QueryResultRow, SQLQueryView},
        views::text_log_view::TextLogView,
    },
};
use chrono::{DateTime, Local};
use std::collections::HashMap;

pub struct LoggerNamesViewProvider;

impl ViewProvider for LoggerNamesViewProvider {
    fn name(&self) -> &'static str {
        "Loggers"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Loggers
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        if app.has_view("logger_names") {
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
            "logger_name",
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
            move |app: &mut App, columns: Vec<&'static str>, row: QueryResultRow| {
                let row = row.0;
                let mut map = HashMap::<String, String>::new();
                columns.iter().zip(row.iter()).for_each(|(c, r)| {
                    map.insert(c.to_string(), r.to_string());
                });

                let logger_name = map.get("logger_name").unwrap().clone();
                let context = app.user_data::<ContextArc>().unwrap().clone();
                let view_options = context.lock().unwrap().options.view.clone();

                app.present_logs(
                    "logger_logs",
                    &format!("Logs for logger: {}", logger_name),
                    NamedView::new(
                        "logger_logs",
                        TextLogView::new(
                            "logger_logs",
                            context,
                            crate::interpreter::TextLogArguments {
                                query_ids: None,
                                logger_names: Some(vec![logger_name.clone()]),
                                hostname: None,
                                message_filter: None,
                                max_level: None,
                                start: DateTime::<Local>::from(view_options.start),
                                end: view_options.end,
                            },
                        ),
                    ),
                );
            };

        // Build the query with time filtering
        let (dbtable, clickhouse, selected_host, limit) = {
            let ctx = context.lock().unwrap();
            (
                ctx.clickhouse.get_log_table_name("text_log"),
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

        let host_filter = clickhouse.get_log_host_filter_clause(selected_host.as_ref());
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

        let mut view = SQLQueryView::new(
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

        app.present_view("logger_names", view.with_name("logger_names").full_screen());
    }
}
