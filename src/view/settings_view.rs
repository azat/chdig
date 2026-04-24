use crate::interpreter::{ContextArc, options::ChDigViews};
use cursive::{
    Cursive,
    theme::Effect,
    utils::markup::StyledString,
    view::{Nameable, Resizable},
    views::{Checkbox, Dialog, DummyView, EditView, LinearLayout, ScrollView, TextView},
};

pub fn show_settings_dialog(siv: &mut Cursive) {
    if siv.find_name::<Dialog>("settings").is_some() {
        siv.pop_layer();
        return;
    }

    let context = siv.user_data::<ContextArc>().unwrap().clone();
    let (opts, server_version, selected_host, current_view, queries_filter) = {
        let ctx = context.lock().unwrap();
        (
            ctx.options.clone(),
            ctx.server_version.clone(),
            ctx.selected_host.clone(),
            ctx.current_view,
            ctx.queries_filter.lock().unwrap().clone(),
        )
    };

    let bold = |s: &str| TextView::new(StyledString::styled(s, Effect::Bold));
    let checkbox_row = |label: &str, name: &str, checked: bool| {
        LinearLayout::horizontal()
            .child(DummyView.fixed_width(2))
            .child(Checkbox::new().with_checked(checked).with_name(name))
            .child(TextView::new(format!(" {}", label)))
    };
    let edit_row = |label: &str, name: &str, value: &str, width: usize| {
        LinearLayout::horizontal()
            .child(TextView::new(format!("  {}: ", label)))
            .child(
                EditView::new()
                    .content(value)
                    .with_name(name)
                    .fixed_width(width),
            )
    };

    let mut layout = LinearLayout::vertical();

    // ClickHouse
    layout.add_child(bold("ClickHouse:"));
    layout.add_child(TextView::new(format!(
        "  url: {}",
        opts.clickhouse.url_safe
    )));
    if let Some(ref cluster) = opts.clickhouse.cluster {
        layout.add_child(TextView::new(format!("  cluster: {}", cluster)));
    }
    layout.add_child(checkbox_row(
        "history",
        "set_history",
        opts.clickhouse.history,
    ));
    layout.add_child(checkbox_row(
        "internal_queries",
        "set_internal_queries",
        opts.clickhouse.internal_queries,
    ));
    layout.add_child(edit_row(
        "limit",
        "set_limit",
        &opts.clickhouse.limit.to_string(),
        12,
    ));
    layout.add_child(checkbox_row(
        "logs_order=desc (newest first)",
        "set_logs_order_desc",
        opts.clickhouse.logs_order == crate::interpreter::options::LogsOrder::Desc,
    ));
    layout.add_child(checkbox_row(
        "skip_unavailable_shards",
        "set_skip_unavailable_shards",
        opts.clickhouse.skip_unavailable_shards,
    ));
    layout.add_child(TextView::new(format!(
        "  server_version: {}",
        server_version
    )));
    layout.add_child(DummyView);

    // View
    layout.add_child(bold("View:"));
    layout.add_child(edit_row(
        "delay_interval (ms)",
        "set_delay_interval",
        &opts.view.delay_interval.as_millis().to_string(),
        12,
    ));
    layout.add_child(checkbox_row("group_by", "set_group_by", opts.view.group_by));
    layout.add_child(checkbox_row(
        "no_subqueries",
        "set_no_subqueries",
        opts.view.no_subqueries,
    ));
    layout.add_child(checkbox_row("wrap", "set_wrap", opts.view.wrap));
    layout.add_child(checkbox_row(
        "no_strip_hostname_suffix",
        "set_no_strip_hostname_suffix",
        opts.view.no_strip_hostname_suffix,
    ));
    layout.add_child(edit_row(
        "queries_filter",
        "set_queries_filter",
        &queries_filter,
        30,
    ));
    layout.add_child(edit_row(
        "queries_limit",
        "set_queries_limit",
        &opts.view.queries_limit.to_string(),
        12,
    ));
    layout.add_child(edit_row(
        "start",
        "set_start",
        &opts.view.start.to_editable_string(),
        22,
    ));
    layout.add_child(edit_row(
        "end",
        "set_end",
        &opts.view.end.to_editable_string(),
        22,
    ));
    layout.add_child(DummyView);

    // Service (read-only)
    layout.add_child(bold("Service:"));
    layout.add_child(TextView::new(format!(
        "  log: {}",
        opts.service.log.as_deref().unwrap_or("(none)")
    )));
    layout.add_child(TextView::new(format!(
        "  chdig_config: {}",
        opts.service.chdig_config.as_deref().unwrap_or("(none)")
    )));
    layout.add_child(DummyView);

    // Perfetto (query)
    layout.add_child(bold("Perfetto (query):"));
    layout.add_child(checkbox_row(
        "opentelemetry_span_log",
        "set_otel",
        opts.perfetto.opentelemetry_span_log,
    ));
    layout.add_child(checkbox_row(
        "trace_log",
        "set_trace_log",
        opts.perfetto.trace_log,
    ));
    layout.add_child(checkbox_row(
        "query_metric_log",
        "set_query_metric_log",
        opts.perfetto.query_metric_log,
    ));
    layout.add_child(checkbox_row(
        "part_log",
        "set_part_log",
        opts.perfetto.part_log,
    ));
    layout.add_child(checkbox_row(
        "query_thread_log",
        "set_query_thread_log",
        opts.perfetto.query_thread_log,
    ));
    layout.add_child(checkbox_row(
        "text_log",
        "set_text_log",
        opts.perfetto.text_log,
    ));
    layout.add_child(checkbox_row(
        "text_log_android",
        "set_text_log_android",
        opts.perfetto.text_log_android,
    ));
    layout.add_child(checkbox_row(
        "per_server",
        "set_per_server",
        opts.perfetto.per_server,
    ));
    layout.add_child(DummyView);

    // Perfetto (server)
    layout.add_child(bold("Perfetto (server):"));
    layout.add_child(checkbox_row(
        "metric_log",
        "set_metric_log",
        opts.perfetto.metric_log,
    ));
    layout.add_child(checkbox_row(
        "asynchronous_metric_log",
        "set_async_metric_log",
        opts.perfetto.asynchronous_metric_log,
    ));
    layout.add_child(checkbox_row(
        "asynchronous_insert_log",
        "set_async_insert_log",
        opts.perfetto.asynchronous_insert_log,
    ));
    layout.add_child(checkbox_row(
        "error_log",
        "set_error_log",
        opts.perfetto.error_log,
    ));
    layout.add_child(checkbox_row(
        "s3_queue_log",
        "set_s3_queue_log",
        opts.perfetto.s3_queue_log,
    ));
    layout.add_child(checkbox_row(
        "azure_queue_log",
        "set_azure_queue_log",
        opts.perfetto.azure_queue_log,
    ));
    layout.add_child(checkbox_row(
        "blob_storage_log",
        "set_blob_storage_log",
        opts.perfetto.blob_storage_log,
    ));
    layout.add_child(checkbox_row(
        "background_schedule_pool_log",
        "set_bg_pool_log",
        opts.perfetto.background_schedule_pool_log,
    ));
    layout.add_child(checkbox_row(
        "session_log",
        "set_session_log",
        opts.perfetto.session_log,
    ));
    layout.add_child(checkbox_row(
        "aggregated_zookeeper_log",
        "set_zk_log",
        opts.perfetto.aggregated_zookeeper_log,
    ));
    layout.add_child(DummyView);

    // Runtime (read-only)
    layout.add_child(bold("Runtime:"));
    layout.add_child(TextView::new(format!(
        "  selected_host: {}",
        selected_host.as_deref().unwrap_or("(all)")
    )));
    layout.add_child(TextView::new(format!(
        "  current_view: {:?}",
        current_view.unwrap_or(ChDigViews::Queries)
    )));

    let context_for_apply = context;
    let dialog = Dialog::new()
        .title("Settings")
        .content(ScrollView::new(layout))
        .button("Apply", move |siv| {
            let history = siv
                .call_on_name("set_history", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let internal_queries = siv
                .call_on_name("set_internal_queries", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let limit_str = siv
                .call_on_name("set_limit", |v: &mut EditView| v.get_content())
                .unwrap();
            let logs_order_desc = siv
                .call_on_name("set_logs_order_desc", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let skip_unavailable_shards = siv
                .call_on_name("set_skip_unavailable_shards", |v: &mut Checkbox| {
                    v.is_checked()
                })
                .unwrap();

            let delay_str = siv
                .call_on_name("set_delay_interval", |v: &mut EditView| v.get_content())
                .unwrap();
            let group_by = siv
                .call_on_name("set_group_by", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let no_subqueries = siv
                .call_on_name("set_no_subqueries", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let wrap = siv
                .call_on_name("set_wrap", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let no_strip = siv
                .call_on_name("set_no_strip_hostname_suffix", |v: &mut Checkbox| {
                    v.is_checked()
                })
                .unwrap();
            let queries_filter = siv
                .call_on_name("set_queries_filter", |v: &mut EditView| {
                    (*v.get_content()).clone()
                })
                .unwrap();
            let queries_limit_str = siv
                .call_on_name("set_queries_limit", |v: &mut EditView| v.get_content())
                .unwrap();
            let start_str = siv
                .call_on_name("set_start", |v: &mut EditView| v.get_content())
                .unwrap();
            let end_str = siv
                .call_on_name("set_end", |v: &mut EditView| v.get_content())
                .unwrap();

            let otel = siv
                .call_on_name("set_otel", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let trace_log = siv
                .call_on_name("set_trace_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let query_metric = siv
                .call_on_name("set_query_metric_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let part_log = siv
                .call_on_name("set_part_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let query_thread = siv
                .call_on_name("set_query_thread_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let text_log = siv
                .call_on_name("set_text_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let text_log_android = siv
                .call_on_name("set_text_log_android", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let per_server = siv
                .call_on_name("set_per_server", |v: &mut Checkbox| v.is_checked())
                .unwrap();

            let metric_log = siv
                .call_on_name("set_metric_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let async_metric_log = siv
                .call_on_name("set_async_metric_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let async_insert_log = siv
                .call_on_name("set_async_insert_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let error_log = siv
                .call_on_name("set_error_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let s3_queue_log = siv
                .call_on_name("set_s3_queue_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let azure_queue_log = siv
                .call_on_name("set_azure_queue_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let blob_storage_log = siv
                .call_on_name("set_blob_storage_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let bg_pool_log = siv
                .call_on_name("set_bg_pool_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let session_log = siv
                .call_on_name("set_session_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();
            let zk_log = siv
                .call_on_name("set_zk_log", |v: &mut Checkbox| v.is_checked())
                .unwrap();

            let limit: u64 = match limit_str.parse() {
                Ok(v) => v,
                Err(_) => {
                    siv.add_layer(Dialog::info("Invalid limit value"));
                    return;
                }
            };
            let delay_ms: u64 = match delay_str.parse() {
                Ok(v) => v,
                Err(_) => {
                    siv.add_layer(Dialog::info("Invalid delay_interval value"));
                    return;
                }
            };
            let queries_limit: u64 = match queries_limit_str.parse() {
                Ok(v) => v,
                Err(_) => {
                    siv.add_layer(Dialog::info("Invalid queries_limit value"));
                    return;
                }
            };
            let new_start = match start_str.parse::<crate::common::RelativeDateTime>() {
                Ok(v) => v,
                Err(err) => {
                    siv.add_layer(Dialog::info(format!("Invalid start: {}", err)));
                    return;
                }
            };
            let new_end = match end_str.parse::<crate::common::RelativeDateTime>() {
                Ok(v) => v,
                Err(err) => {
                    siv.add_layer(Dialog::info(format!("Invalid end: {}", err)));
                    return;
                }
            };

            {
                let mut ctx = context_for_apply.lock().unwrap();
                ctx.options.clickhouse.history = history;
                ctx.options.clickhouse.internal_queries = internal_queries;
                ctx.options.clickhouse.limit = limit;
                ctx.options.clickhouse.logs_order = if logs_order_desc {
                    crate::interpreter::options::LogsOrder::Desc
                } else {
                    crate::interpreter::options::LogsOrder::Asc
                };
                ctx.options.clickhouse.skip_unavailable_shards = skip_unavailable_shards;

                ctx.options.view.delay_interval = std::time::Duration::from_millis(delay_ms);
                ctx.options.view.group_by = group_by;
                ctx.options.view.no_subqueries = no_subqueries;
                ctx.options.view.wrap = wrap;
                ctx.options.view.no_strip_hostname_suffix = no_strip;
                *ctx.queries_filter.lock().unwrap() = queries_filter;
                ctx.options.view.queries_limit = queries_limit;
                *ctx.queries_limit.lock().unwrap() = queries_limit;
                ctx.options.view.start = new_start;
                ctx.options.view.end = new_end;

                ctx.options.perfetto.opentelemetry_span_log = otel;
                ctx.options.perfetto.trace_log = trace_log;
                ctx.options.perfetto.query_metric_log = query_metric;
                ctx.options.perfetto.part_log = part_log;
                ctx.options.perfetto.query_thread_log = query_thread;
                ctx.options.perfetto.text_log = text_log;
                ctx.options.perfetto.text_log_android = text_log_android;
                ctx.options.perfetto.per_server = per_server;
                ctx.options.perfetto.metric_log = metric_log;
                ctx.options.perfetto.asynchronous_metric_log = async_metric_log;
                ctx.options.perfetto.asynchronous_insert_log = async_insert_log;
                ctx.options.perfetto.error_log = error_log;
                ctx.options.perfetto.s3_queue_log = s3_queue_log;
                ctx.options.perfetto.azure_queue_log = azure_queue_log;
                ctx.options.perfetto.blob_storage_log = blob_storage_log;
                ctx.options.perfetto.background_schedule_pool_log = bg_pool_log;
                ctx.options.perfetto.session_log = session_log;
                ctx.options.perfetto.aggregated_zookeeper_log = zk_log;

                ctx.trigger_view_refresh();
            }
            siv.pop_layer();
        })
        .button("Cancel", |siv| {
            siv.pop_layer();
        });
    siv.add_layer(dialog.with_name("settings"));
}
