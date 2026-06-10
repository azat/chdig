use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::{self, Navigation, ViewProvider},
};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
};

pub struct ErrorLogViewProvider;

impl ViewProvider for ErrorLogViewProvider {
    fn name(&self) -> &'static str {
        "Error log"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::ErrorLog
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        let view_name = "error_log";

        if siv.has_view(view_name) {
            return;
        }

        let columns = vec![
            "error::String name",
            "any(code) code",
            "sum(value) total",
            "total bar",
            "max(event_time) error_time",
        ];
        let columns_to_compare = vec!["name"];

        let (view_options, dbtable, clickhouse, selected_host) = {
            let ctx = context.lock().unwrap();
            (
                ctx.options.view.clone(),
                ctx.clickhouse.get_log_table_name("system", "error_log"),
                ctx.clickhouse.clone(),
                ctx.selected_host.clone(),
            )
        };

        let start_sql = view_options
            .start
            .to_sql_datetime_64()
            .unwrap_or_else(|| "now() - INTERVAL 1 HOUR".to_string());
        let end_sql = view_options
            .end
            .to_sql_datetime_64()
            .unwrap_or_else(|| "now()".to_string());

        let query = format!(
            r#"
            WITH {start} AS start_, {end} AS end_
            SELECT {columns}
            FROM {dbtable}
            WHERE
                event_date BETWEEN toDate(start_) AND toDate(end_) AND
                event_time BETWEEN toDateTime(start_) AND toDateTime(end_)
                {host_filter}
            GROUP BY error
            "#,
            start = start_sql,
            end = end_sql,
            columns = columns.join(", "),
            dbtable = dbtable,
            host_filter = clickhouse.get_log_host_filter_clause(selected_host.as_ref()),
        );

        siv.drop_main_view();

        let mut view = view::SQLQueryView::new(
            context.clone(),
            view_name,
            "total",
            columns,
            columns_to_compare,
            query,
        )
        .unwrap_or_else(|_| panic!("Cannot get {}", view_name));
        view.get_inner_mut()
            .set_on_submit(super::errors::errors_logs_callback);
        view.get_inner_mut().set_title(view_name);
        view.get_inner_mut().set_bar_columns(vec![("bar", "total")]);

        siv.set_main_view(view.with_name(view_name).full_screen());
        siv.focus_name(view_name).unwrap();
    }
}
