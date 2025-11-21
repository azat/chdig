use crate::{
    common::RelativeDateTime,
    interpreter::{ContextArc, options::ChDigViews},
    view::{QueryResultRow, TextLogView, ViewProvider},
};
use chrono::{DateTime, Duration, Local};
use cursive::{
    Cursive,
    view::Resizable,
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
        let mut columns = vec![
            "name",
            "value",
            "last_error_time error_time",
            // "toValidUTF8(last_error_message) _error_message",
            "arrayStringConcat(arrayMap(addr -> concat(addressToLine(addr), '::', demangle(addressToSymbol(addr))), last_error_trace), '\n') _error_trace",
        ];

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
                                start_time,
                                RelativeDateTime::from(end_time),
                                None,
                                None,
                                Some(error_name),
                                Some("Warning".to_string()),
                            ),
                        )),
                ));
                siv.focus_name("error_logs").unwrap();
            };

        super::show_query_result_view(
            siv,
            context,
            "errors",
            None,
            None,
            "value",
            &mut columns,
            1,
            Some(errors_logs_callback),
            &HashMap::from([("allow_introspection_functions", "1")]),
        );
    }
}
