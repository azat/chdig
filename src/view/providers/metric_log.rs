use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    view::{self, Navigation, ViewProvider},
};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
};

pub struct MetricLogViewProvider;

const SPARKLINE_BUCKETS: u32 = 16;

impl ViewProvider for MetricLogViewProvider {
    fn name(&self) -> &'static str {
        "Metric log"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::MetricLog
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        show_metric_log(siv, context);
    }
}

fn build_query(context: &ContextArc) -> String {
    let (view_options, dbtable, clickhouse, selected_host) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.view.clone(),
            ctx.clickhouse.get_log_table_name("system", "metric_log"),
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

    // ProfileEvent_* columns hold raw deltas per collection interval, so they
    // are summed - the total over the range (and over the bucket for the
    // sparkline); CurrentMetric_* gauges are averaged instead (with max() per
    // sparkline bucket to preserve the peaks).
    //
    // tupleToNameValuePairs(tuple(COLUMNS(...))) unpivots the columns into
    // (name, value) pairs; the element names require
    // enable_named_columns_in_function_tuple (ClickHouse >= 24.7), and two
    // matchers are needed since tupleToNameValuePairs() requires uniform
    // element types (UInt64 vs Int64).
    format!(
        r#"
        WITH {start} AS start_, {end} AS end_
        SELECT
            name,
            value,
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
                pair_.1 AS name,
                startsWith(name, 'ProfileEvent_') AS is_delta_,
                if(is_delta_, sum(pair_.2), avg(pair_.2)) AS value,
                max(pair_.2) AS max,
                if(avg(pair_.2) != 0, stddevPop(pair_.2) / abs(avg(pair_.2)), 0) AS dyn,
                if(is_delta_, sumMap(map(bucket_, pair_.2)), maxMap(map(bucket_, pair_.2))) AS m_,
                arrayMap(i -> m_[toUInt16(i)], range({buckets})) AS heights_
            FROM
            (
                SELECT
                    arrayJoin(arrayConcat(
                        CAST(tupleToNameValuePairs(tuple(COLUMNS('^ProfileEvent_'))), 'Array(Tuple(String, Float64))'),
                        CAST(tupleToNameValuePairs(tuple(COLUMNS('^CurrentMetric_'))), 'Array(Tuple(String, Float64))')
                    )) AS pair_,
                    toUInt16(least({buckets} - 1, floor((toUnixTimestamp(event_time) - toUnixTimestamp(toDateTime(start_))) * {buckets} / greatest(1, toUnixTimestamp(toDateTime(end_)) - toUnixTimestamp(toDateTime(start_)))))) AS bucket_
                FROM {dbtable}
                WHERE
                    event_date BETWEEN toDate(start_) AND toDate(end_) AND
                    event_time BETWEEN toDateTime(start_) AND toDateTime(end_)
                    {host_filter}
            )
            GROUP BY name
            HAVING max != 0
        )
        SETTINGS enable_named_columns_in_function_tuple=1
        "#,
        start = start_sql,
        end = end_sql,
        buckets = SPARKLINE_BUCKETS,
        dbtable = dbtable,
        host_filter = clickhouse.get_log_host_filter_clause(selected_host.as_ref()),
    )
}

fn show_metric_log(siv: &mut Cursive, context: ContextArc) {
    let view_name = "metric_log";

    if siv.has_view(view_name) {
        return;
    }

    let query = build_query(&context);
    let columns = vec!["name", "value", "max", "dyn", "spark"];
    let columns_to_compare = vec!["name"];

    let mut view = view::SQLQueryView::new(
        context.clone(),
        view_name,
        "dyn",
        columns,
        columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot create {}", view_name));

    view.get_inner_mut()
        .set_on_submit(super::query_result_show_row);
    view.get_inner_mut().set_title("Metric log");

    siv.drop_main_view();
    siv.set_main_view(view.with_name(view_name).full_screen());
    siv.focus_name(view_name).unwrap();
}
