use super::Navigation;
use super::queries_view::{AVAILABLE_QUERY_COLUMNS, query_column_id};
use crate::interpreter::{ContextArc, options::ChDigViews};
use cursive::{
    Cursive,
    theme::Effect,
    utils::markup::StyledString,
    view::{Nameable, Resizable},
    views::{
        Checkbox, Dialog, DummyView, EditView, LinearLayout, OnEventView, ScrollView, TextView,
    },
};
use std::sync::{Arc, Mutex};

fn apply_settings(siv: &mut Cursive, context: &ContextArc) {
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
    let no_color = siv
        .call_on_name("set_no_color", |v: &mut Checkbox| v.is_checked())
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

    let mut query_columns: Vec<String> = Vec::new();
    for &col in AVAILABLE_QUERY_COLUMNS {
        let Some(label) = query_column_id(col) else {
            continue;
        };
        let name = format!("set_qcol_{}", label);
        let checked = siv
            .call_on_name(&name, |v: &mut Checkbox| v.is_checked())
            .unwrap_or(true);
        if checked {
            query_columns.push(label.to_string());
        }
    }

    {
        let mut ctx = context.lock().unwrap();
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
        ctx.options.view.no_color = no_color;
        *ctx.queries_filter.lock().unwrap() = queries_filter;
        ctx.options.view.queries_limit = queries_limit;
        *ctx.queries_limit.lock().unwrap() = queries_limit;
        ctx.options.view.start = new_start;
        ctx.options.view.end = new_end;
        ctx.options.view.query_columns = query_columns;

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

    // Re-create the current view so option changes that only take effect at
    // view construction time (e.g. query_columns) are picked up immediately.
    let (provider, current_view) = {
        let ctx = context.lock().unwrap();
        let current_view = ctx
            .current_view
            .or(ctx.options.start_view())
            .unwrap_or(ChDigViews::Queries);
        (
            ctx.view_registry.get_by_view_type(current_view),
            current_view,
        )
    };
    log::info!("Reopen {:?} view after settings change", current_view);
    siv.drop_main_view();
    provider.show(siv, context.clone());
    context.lock().unwrap().trigger_view_refresh();
}

struct SearchTarget {
    label: String,
    section: usize,
    focus_name: String,
}

struct SearchableLayout {
    layout: LinearLayout,
    sections: Vec<String>,
    targets: Vec<SearchTarget>,
}

impl SearchableLayout {
    fn new() -> Self {
        Self {
            layout: LinearLayout::vertical(),
            sections: Vec::new(),
            targets: Vec::new(),
        }
    }

    fn target(&mut self, label: &str, focus_name: &str) {
        self.targets.push(SearchTarget {
            label: label.to_string(),
            section: self.sections.len().saturating_sub(1),
            focus_name: focus_name.to_string(),
        });
    }

    fn section(&mut self, title: &str) {
        self.sections.push(title.trim_end_matches(':').to_string());
        self.layout
            .add_child(TextView::new(StyledString::styled(title, Effect::Bold)));
    }

    fn separator(&mut self) {
        self.layout.add_child(DummyView);
    }

    fn text(&mut self, label: &str, value: impl std::fmt::Display) {
        // Named so that the search can focus (and thus scroll to) read-only rows
        let name = format!("settings_row_{}", self.targets.len());
        self.layout
            .add_child(TextView::new(format!("  {}: {}", label, value)).with_name(&name));
        self.target(label, &name);
    }

    fn checkbox(&mut self, label: &str, name: &str, checked: bool) {
        self.layout.add_child(
            LinearLayout::horizontal()
                .child(DummyView.fixed_width(2))
                .child(Checkbox::new().with_checked(checked).with_name(name))
                .child(TextView::new(format!(" {}", label))),
        );
        self.target(label, name);
    }

    fn edit(&mut self, label: &str, name: &str, value: &str, width: usize) {
        self.layout.add_child(
            LinearLayout::horizontal()
                .child(TextView::new(format!("  {}: ", label)))
                .child(
                    EditView::new()
                        .content(value)
                        .with_name(name)
                        .fixed_width(width),
                ),
        );
        self.target(label, name);
    }
}

#[derive(Default)]
struct SearchState {
    query: String,
    position: usize,
}

struct SettingsSearch {
    sections: Vec<String>,
    targets: Vec<SearchTarget>,
    state: Mutex<SearchState>,
}

fn apply_settings_search(siv: &mut Cursive, search: &SettingsSearch, query: &str) {
    let query = query.trim().to_lowercase();
    if query.is_empty() {
        return;
    }

    // A section title match (e.g. "perfetto") cycles through all its options
    let matches: Vec<usize> = search
        .targets
        .iter()
        .enumerate()
        .filter(|(_, target)| {
            target.label.to_lowercase().contains(&query)
                || search.sections[target.section]
                    .to_lowercase()
                    .contains(&query)
        })
        .map(|(i, _)| i)
        .collect();
    let Some(&first) = matches.first() else {
        return;
    };

    let next = {
        let mut state = search.state.lock().unwrap();
        // Repeating the same query advances to the next match (wrapping around)
        let start = if state.query == query {
            state.position + 1
        } else {
            0
        };
        let next = matches
            .iter()
            .copied()
            .find(|&i| i >= start)
            .unwrap_or(first);
        state.query = query;
        state.position = next;
        next
    };

    if let Ok(result) = siv.focus_name(&search.targets[next].focus_name) {
        result.process(siv);
    }
}

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

    let mut layout = SearchableLayout::new();

    layout.section("ClickHouse:");
    layout.text("url", &opts.clickhouse.url_safe);
    if let Some(ref cluster) = opts.clickhouse.cluster {
        layout.text("cluster", cluster);
    }
    layout.checkbox("history", "set_history", opts.clickhouse.history);
    layout.checkbox(
        "internal_queries",
        "set_internal_queries",
        opts.clickhouse.internal_queries,
    );
    layout.edit("limit", "set_limit", &opts.clickhouse.limit.to_string(), 12);
    layout.checkbox(
        "logs_order=desc (newest first)",
        "set_logs_order_desc",
        opts.clickhouse.logs_order == crate::interpreter::options::LogsOrder::Desc,
    );
    layout.checkbox(
        "skip_unavailable_shards",
        "set_skip_unavailable_shards",
        opts.clickhouse.skip_unavailable_shards,
    );
    layout.text("server_version", &server_version);
    layout.separator();

    layout.section("View:");
    layout.edit(
        "delay_interval (ms)",
        "set_delay_interval",
        &opts.view.delay_interval.as_millis().to_string(),
        12,
    );
    layout.checkbox("group_by", "set_group_by", opts.view.group_by);
    layout.checkbox(
        "no_subqueries",
        "set_no_subqueries",
        opts.view.no_subqueries,
    );
    layout.checkbox("wrap", "set_wrap", opts.view.wrap);
    layout.checkbox(
        "no_strip_hostname_suffix",
        "set_no_strip_hostname_suffix",
        opts.view.no_strip_hostname_suffix,
    );
    layout.checkbox("no_color", "set_no_color", opts.view.no_color);
    layout.edit("queries_filter", "set_queries_filter", &queries_filter, 30);
    layout.edit(
        "queries_limit",
        "set_queries_limit",
        &opts.view.queries_limit.to_string(),
        12,
    );
    layout.edit(
        "start",
        "set_start",
        &opts.view.start.to_editable_string(),
        22,
    );
    layout.edit("end", "set_end", &opts.view.end.to_editable_string(), 22);
    layout.separator();

    layout.section("Queries columns:");
    for &col in AVAILABLE_QUERY_COLUMNS {
        let Some(label) = query_column_id(col) else {
            continue;
        };
        let visible = opts.view.query_columns.iter().any(|h| h == label);
        let name = format!("set_qcol_{}", label);
        layout.checkbox(label, &name, visible);
    }
    layout.separator();

    layout.section("Service:");
    layout.text("log", opts.service.log.as_deref().unwrap_or("(none)"));
    layout.text(
        "chdig_config",
        opts.service.chdig_config.as_deref().unwrap_or("(none)"),
    );
    layout.separator();

    layout.section("Perfetto (query):");
    layout.checkbox(
        "opentelemetry_span_log",
        "set_otel",
        opts.perfetto.opentelemetry_span_log,
    );
    layout.checkbox("trace_log", "set_trace_log", opts.perfetto.trace_log);
    layout.checkbox(
        "query_metric_log",
        "set_query_metric_log",
        opts.perfetto.query_metric_log,
    );
    layout.checkbox("part_log", "set_part_log", opts.perfetto.part_log);
    layout.checkbox(
        "query_thread_log",
        "set_query_thread_log",
        opts.perfetto.query_thread_log,
    );
    layout.checkbox("text_log", "set_text_log", opts.perfetto.text_log);
    layout.checkbox(
        "text_log_android",
        "set_text_log_android",
        opts.perfetto.text_log_android,
    );
    layout.checkbox("per_server", "set_per_server", opts.perfetto.per_server);
    layout.separator();

    layout.section("Perfetto (server):");
    layout.checkbox("metric_log", "set_metric_log", opts.perfetto.metric_log);
    layout.checkbox(
        "asynchronous_metric_log",
        "set_async_metric_log",
        opts.perfetto.asynchronous_metric_log,
    );
    layout.checkbox(
        "asynchronous_insert_log",
        "set_async_insert_log",
        opts.perfetto.asynchronous_insert_log,
    );
    layout.checkbox("error_log", "set_error_log", opts.perfetto.error_log);
    layout.checkbox(
        "s3_queue_log",
        "set_s3_queue_log",
        opts.perfetto.s3_queue_log,
    );
    layout.checkbox(
        "azure_queue_log",
        "set_azure_queue_log",
        opts.perfetto.azure_queue_log,
    );
    layout.checkbox(
        "blob_storage_log",
        "set_blob_storage_log",
        opts.perfetto.blob_storage_log,
    );
    layout.checkbox(
        "background_schedule_pool_log",
        "set_bg_pool_log",
        opts.perfetto.background_schedule_pool_log,
    );
    layout.checkbox("session_log", "set_session_log", opts.perfetto.session_log);
    layout.checkbox(
        "aggregated_zookeeper_log",
        "set_zk_log",
        opts.perfetto.aggregated_zookeeper_log,
    );
    layout.separator();

    layout.section("Runtime:");
    layout.text("selected_host", selected_host.as_deref().unwrap_or("(all)"));
    layout.text(
        "current_view",
        format!("{:?}", current_view.unwrap_or(ChDigViews::Queries)),
    );

    let SearchableLayout {
        layout,
        sections,
        targets,
    } = layout;
    let search = Arc::new(SettingsSearch {
        sections,
        targets,
        state: Mutex::new(SearchState::default()),
    });

    let context_for_apply = context.clone();
    let context_for_enter = context;

    let content = crate::view::submit_on_enter(ScrollView::new(layout), move |siv| {
        apply_settings(siv, &context_for_enter);
    });

    let dialog = Dialog::new()
        .title("Settings")
        .content(content)
        .button("Apply", move |siv| {
            apply_settings(siv, &context_for_apply);
        })
        .button("Cancel", |siv| {
            siv.pop_layer();
        });

    // '/' opens a search prompt that focuses the matching option; EditViews
    // consume characters first, so typing '/' inside one still works.
    let dialog = OnEventView::new(dialog.with_name("settings")).on_event('/', move |siv| {
        let search = search.clone();
        crate::view::show_bottom_prompt(siv, "/", move |siv, text| {
            siv.pop_layer();
            apply_settings_search(siv, &search, text);
        });
    });
    siv.add_layer(dialog);
}
