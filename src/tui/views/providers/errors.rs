use crate::{
    common::RelativeDateTime,
    interpreter::{ContextArc, options::ChDigViews},
    tui::{
        App, Dialog, DummyView, LinearLayout, Nameable, NamedView, Navigation, Resizable, TextView,
        ViewProvider,
        views::sql_query_view::{Row as QueryResultRow, SQLQueryView},
        views::text_log_view::TextLogView,
    },
};
use chrono::{DateTime, Duration, Local};
use std::collections::HashMap;

pub struct ErrorsViewProvider;

// Shared with the error_log view (expects "name" and "error_time" columns)
pub(super) fn errors_logs_callback(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
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

    let context = app.user_data::<ContextArc>().unwrap().clone();
    let view_options = context.lock().unwrap().options.view.clone();
    let view_start = DateTime::<Local>::from(view_options.start);
    let view_end = DateTime::<Local>::from(view_options.end.clone());

    // Show logs for 1 minute before and after the error time, clamped to
    // --start/--end. system.errors is cumulative, so last_error_time may be
    // outside the requested interval - search the whole interval then.
    // (Note, we need to add at least 1 second to error_time, otherwise it will be
    // filtered out by event_time_microseconds condition)
    let offset = Duration::try_minutes(1).unwrap_or_default();
    let (start_time, end_time) = if error_time >= view_start && error_time <= view_end {
        let start = std::cmp::max(error_time - offset, view_start);
        let end = if error_time + offset < view_end {
            RelativeDateTime::from(error_time + offset)
        } else {
            view_options.end
        };
        (start, end)
    } else {
        (view_start, view_options.end)
    };

    app.add_layer(Dialog::around(
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
                        limit: None,
                        start: start_time,
                        end: end_time,
                    },
                ),
            )),
    ));
    app.focus_name("error_logs");
}

impl ViewProvider for ErrorsViewProvider {
    fn name(&self) -> &'static str {
        "Errors"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Errors
    }

    fn show(&self, app: &mut App, context: ContextArc, _instance: Option<&str>) {
        if app.focus_name("errors") {
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
                ctx.clickhouse.get_table_name("errors"),
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

        let mut view = SQLQueryView::new(
            context.clone(),
            "errors",
            "total",
            columns,
            columns_to_compare,
            vec!["name"],
            query,
        )
        .unwrap_or_else(|_| panic!("Cannot get errors"));
        view.get_inner_mut().set_on_submit(errors_logs_callback);
        view.get_inner_mut().set_title("errors");
        view.get_inner_mut().set_bar_columns(vec![("bar", "total")]);

        app.present_view("errors", view.with_name("errors").full_screen());
    }
}
