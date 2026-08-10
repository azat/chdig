use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    tui::{
        App, Nameable, Navigation, Resizable, ViewProvider,
        views::sql_query_view::{Row as QueryResultRow, SQLQueryView},
    },
};

pub struct AsynchronousMetricLogViewProvider;

const SPARKLINE_BUCKETS: u32 = 16;

impl ViewProvider for AsynchronousMetricLogViewProvider {
    fn name(&self) -> &'static str {
        "Asynchronous metric log"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::AsynchronousMetricLog
    }

    fn show(&self, app: &mut App, context: ContextArc) {
        show_asynchronous_metric_log(app, context);
    }
}

fn build_query(context: &ContextArc) -> String {
    let (view_options, dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.view.clone(),
            ctx.clickhouse.get_log_table_name("asynchronous_metric_log"),
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

    // Counter-derived asynchronous metrics (network, disks, OS CPU time, ...)
    // hold raw deltas per update interval (NOT normalized by the elapsed
    // time), so they are summed - the total over the range (and over the
    // bucket for the sparkline). Unlike metric_log, gauges (memory, ...)
    // cannot be told apart by name, so they are summed too.
    format!(
        r#"
        WITH {start} AS start_, {end} AS end_
        SELECT
            name,
            value_ AS value,
            max,
            dyn,
            if(arrayMax(heights_) <= 0,
               repeat('▁', {buckets}),
               arrayStringConcat(
                   arrayMap(
                       h -> ['▁','▂','▃','▄','▅','▆','▇','█'][toUInt32(least(8, greatest(1, ceil(h / arrayMax(heights_) * 8))))],
                       heights_),
                   '')) AS spark
        FROM
        (
            SELECT
                metric AS name,
                sum(value) AS value_,
                max(value) AS max,
                if(avg(value) != 0, stddevPop(value) / abs(avg(value)), 0) AS dyn,
                sumMap(map(toUInt16(least({buckets} - 1, floor((toUnixTimestamp(event_time) - toUnixTimestamp(toDateTime(start_))) * {buckets} / greatest(1, toUnixTimestamp(toDateTime(end_)) - toUnixTimestamp(toDateTime(start_)))))), value)) AS m_,
                arrayMap(i -> m_[toUInt16(i)], range({buckets})) AS heights_
            FROM {dbtable}
            WHERE
                event_date BETWEEN toDate(start_) AND toDate(end_) AND
                event_time BETWEEN toDateTime(start_) AND toDateTime(end_)
                {host_filter}
            GROUP BY metric
            HAVING max != 0 OR min(value) != 0
        )
        "#,
        start = start_sql,
        end = end_sql,
        buckets = SPARKLINE_BUCKETS,
        dbtable = dbtable,
        host_filter = clickhouse.get_log_host_filter_clause(selected_host.as_ref()),
    )
}

fn show_chart(app: &mut App, columns: Vec<&'static str>, row: QueryResultRow) {
    let Some(name) = columns
        .iter()
        .zip(row.0.iter())
        .find_map(|(c, r)| (*c == "name").then(|| r.to_string()))
    else {
        return;
    };
    super::show_metric_chart(
        app,
        "asynchronous_metric_log",
        // avg() per time bucket - same as the system.dashboards queries
        "avg(value)".to_string(),
        Some(format!("metric = '{}'", name.replace('\'', "''"))),
        name,
    );
}

fn show_asynchronous_metric_log(app: &mut App, context: ContextArc) {
    let view_name = "asynchronous_metric_log";

    if app.focus_name(view_name) {
        return;
    }

    let query = build_query(&context);
    let columns = vec!["name", "value", "max", "dyn", "spark"];
    let columns_to_compare = vec!["name"];

    let mut view = SQLQueryView::new(
        context.clone(),
        view_name,
        "dyn",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    view.get_inner_mut().set_on_submit(show_chart);
    view.get_inner_mut().set_title("Asynchronous metric log");

    app.present_view(view_name, view.with_name(view_name).full_screen());
}
