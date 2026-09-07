use chrono::{DateTime, Local};
use humantime::format_duration;
use ratatui::layout::{Rect, Size};
use size::{Base, SizeFormatter, Style};
use std::time::Duration;

use crate::common::sparkline::SparklineBuffer;
use crate::common::{BAR_EMPTY, BAR_FILLED, bar_filled};
use crate::interpreter::{
    BackgroundRunner, ContextArc, WorkerEvent,
    clickhouse::{ClickHouseHostSummary, ClickHouseServerSummary},
};
use crate::tui::component::{Canvas, Component, DummyView, Nameable, call_on_name};
use crate::tui::event::{Event, EventResult, MouseButton, MouseEvent};
use crate::tui::linear::LinearLayout;
use crate::tui::resize::Resizable;
use crate::tui::style::{Color, Style as TextStyle, StyledString, pad_column, print_str};
use crate::tui::text::TextView;
use crate::utils::{find_common_hostname_prefix_and_suffix, strip_hostname};

const SPARKLINE_CAPACITY: usize = 60;
const SPARKLINE_WIDTH: usize = 8;

struct SparklineSet {
    cpu: SparklineBuffer,
    memory: SparklineBuffer,
    queries: SparklineBuffer,
    merges: SparklineBuffer,
}

impl SparklineSet {
    fn new() -> Self {
        Self {
            cpu: SparklineBuffer::new(SPARKLINE_CAPACITY),
            memory: SparklineBuffer::new(SPARKLINE_CAPACITY),
            queries: SparklineBuffer::new(SPARKLINE_CAPACITY),
            merges: SparklineBuffer::new(SPARKLINE_CAPACITY),
        }
    }
}

/// Index of the per-host table in `layout` (after the 4 aggregated rows); it
/// is added only while shown, an empty TextView would still take a row.
const PER_HOST_CHILD: usize = 4;
const PER_HOST_MIN_ROWS: usize = 4;
/// Rows the panes below keep when the host table is resized
const PANES_MIN_HEIGHT: usize = 6;
/// Aggregated rows + table header + separator row
const PER_HOST_FIXED_ROWS: usize = 4 + 1 + 1;
/// (header, right aligned)
const PER_HOST_COLUMNS: &[(&str, bool)] = &[
    ("host", false),
    ("up", false),
    ("cpu", true),
    ("mem", true),
    ("queries", true),
    ("thr", true),
    ("net recv/sent", true),
    ("disk r/w", true),
];
/// Width of the cpu/mem usage bars of the per-host table
const PER_HOST_BAR_WIDTH: usize = 10;

/// One host of the per-host table (cells follow PER_HOST_COLUMNS)
struct HostRow {
    host: String,
    cells: Vec<StyledString>,
}

pub struct SummaryView {
    context: ContextArc,

    prev_summary: Option<ClickHouseServerSummary>,
    prev_update_time: Option<DateTime<Local>>,

    layout: LinearLayout,
    sparklines: SparklineSet,

    // Per-host table (cluster mode, toggled with '1')
    per_host_enabled: bool,
    host_rows: Vec<HostRow>,
    /// Table body lines (host rows, incl. the "... more" line) for the last
    /// seen height (see required_size)
    last_row_cap: usize,
    /// Body lines chosen by the user ([ ] or dragging the separator); None =
    /// a third of the screen
    host_rows_limit: Option<usize>,
    last_area: Rect,
    resizing: bool,

    bg_runner: BackgroundRunner,
}

/// How many table body lines fit by default: a third of the height, at least
/// PER_HOST_MIN_ROWS.
fn row_cap(height: u16) -> usize {
    (height as usize / 3).max(PER_HOST_MIN_ROWS)
}

/// The most body lines the table may take at `height` while the panes keep
/// PANES_MIN_HEIGHT rows.
fn max_row_cap(height: u16) -> usize {
    (height as usize)
        .saturating_sub(PER_HOST_FIXED_ROWS + PANES_MIN_HEIGHT)
        .max(1)
}

/// The table text: header and at most `cap` body lines, the last one being
/// "... and K more hosts" when the rows do not fit.
fn render_host_rows(rows: &[HostRow], cap: usize) -> StyledString {
    let shown = if rows.len() > cap {
        cap.saturating_sub(1)
    } else {
        rows.len()
    };
    let widths: Vec<usize> = PER_HOST_COLUMNS
        .iter()
        .enumerate()
        .map(|(i, (header, _))| {
            rows.iter()
                .map(|r| r.cells[i].width())
                .max()
                .unwrap_or(0)
                .max(header.len())
        })
        .collect();

    // One StyledString per line: pad_column() measures the widest line
    let mut header = StyledString::new();
    for (i, (title, _)) in PER_HOST_COLUMNS.iter().enumerate() {
        if i > 0 {
            header.append_plain("  ");
        }
        let start = header.width();
        header.append_styled(*title, Color::Cyan);
        pad_column(&mut header, start, widths[i]);
    }
    let mut text = header;
    for row in rows.iter().take(shown) {
        let mut line = StyledString::new();
        for (i, (_, right)) in PER_HOST_COLUMNS.iter().enumerate() {
            if i > 0 {
                line.append_plain("  ");
            }
            let start = line.width();
            let cell = &row.cells[i];
            if *right {
                line.append_plain(" ".repeat(widths[i].saturating_sub(cell.width())));
            }
            line.append(cell.clone());
            pad_column(&mut line, start, widths[i]);
        }
        text.append_plain("\n");
        text.append(line);
    }
    if rows.len() > shown {
        text.append_plain("\n");
        text.append_styled(
            format!("... and {} more hosts", rows.len() - shown),
            Color::Gray,
        );
    }
    text
}

fn get_color_for_ratio(used: u64, total: u64) -> Color {
    let q = used as f64 / total as f64;
    if q > 0.90 {
        Color::Red
    } else if q > 0.5 {
        Color::Yellow
    } else {
        Color::Green
    }
}

/// "used/total ████░░░░░░": the bar is on the capacity scale, so the hosts
/// can be compared at a glance.
fn usage_cell(used: String, total: String, used_n: u64, total_n: u64) -> StyledString {
    let color = get_color_for_ratio(used_n, total_n);
    let filled = bar_filled(used_n as f64, total_n as f64, PER_HOST_BAR_WIDTH);
    let mut cell = StyledString::new();
    cell.append_styled(used, color);
    cell.append_plain(format!("/{} ", total));
    cell.append_styled(BAR_FILLED.to_string().repeat(filled), color);
    cell.append_styled(
        BAR_EMPTY.to_string().repeat(PER_HOST_BAR_WIDTH - filled),
        Color::Gray,
    );
    cell
}

fn get_color_for_bytes(bytes: u64) -> Color {
    const TB: u64 = 1 << 40;
    const PB: u64 = 1 << 50;
    if bytes > PB {
        Color::LightYellow
    } else if bytes > 100 * TB {
        Color::Magenta
    } else if bytes > TB {
        Color::Cyan
    } else {
        Color::Gray
    }
}

fn label(text: &str) -> TextView {
    TextView::new(StyledString::styled(text, Color::Cyan))
}

// TODO add new information:
// - page cache usage (should be diffed)
impl SummaryView {
    pub fn new(context: ContextArc) -> Self {
        let delay = context.lock().unwrap().options.view.delay_interval;

        let update_callback_context = context.clone();
        let update_callback = move |force: bool| {
            update_callback_context
                .lock()
                .unwrap()
                .worker
                .send(force, WorkerEvent::Summary);
        };

        let layout = LinearLayout::vertical()
            .child(
                LinearLayout::horizontal()
                    .child(label("Uptime:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("uptime"))
                    .child(DummyView.fixed_width(1))
                    .child(label("Servers:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("servers"))
                    .child(DummyView.fixed_width(1))
                    .child(label("Data:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("total_data"))
                    .child(DummyView.fixed_width(1))
                    .child(label("CPU:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("cpu"))
                    .child(DummyView.fixed_width(1))
                    .child(label("Queries:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("queries"))
                    .child(TextView::new("").with_name("optional_metrics")),
            )
            .child(
                LinearLayout::horizontal()
                    .child(label("Net recv:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("net_recv"))
                    .child(DummyView.fixed_width(1))
                    .child(label("Net sent:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("net_sent"))
                    .child(DummyView.fixed_width(1))
                    .child(label("Read:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("disk_read"))
                    .child(DummyView.fixed_width(1))
                    .child(label("Write:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("disk_write"))
                    .child(DummyView.fixed_width(1))
                    .child(label("Selected rows:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("selected_rows"))
                    .child(DummyView.fixed_width(1))
                    .child(label("Inserted rows:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("inserted_rows")),
            )
            .child(
                LinearLayout::horizontal()
                    .child(label("Threads:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("threads"))
                    .child(DummyView.fixed_width(1))
                    .child(label("Pools:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("pools")),
            )
            .child(
                LinearLayout::horizontal()
                    .child(label("Memory:"))
                    .child(DummyView.fixed_width(1))
                    .child(TextView::new("").with_name("mem")),
            );

        // The summary generation is bumped only by trigger_full_refresh(), not
        // by trigger_view_refresh(): the summary does not depend on the
        // current view, so switching views must not force its update.
        let (bg_runner_cv, bg_runner_generation) = {
            let ctx = context.lock().unwrap();
            (
                ctx.background_runner_cv.clone(),
                ctx.background_runner_summary_generation.clone(),
            )
        };
        let mut bg_runner = BackgroundRunner::new(delay, bg_runner_cv, bg_runner_generation);
        bg_runner.start(update_callback);

        let per_host_enabled = {
            let ctx = context.lock().unwrap();
            ctx.options.clickhouse.cluster.is_some() && ctx.options.view.summary_per_host
        };

        Self {
            context,
            prev_summary: None,
            prev_update_time: None,
            layout,
            sparklines: SparklineSet::new(),
            per_host_enabled,
            host_rows: Vec::new(),
            last_row_cap: PER_HOST_MIN_ROWS,
            host_rows_limit: None,
            last_area: Rect::default(),
            resizing: false,
            bg_runner,
        }
    }

    fn table_shown(&self) -> bool {
        self.layout.len() > PER_HOST_CHILD
    }

    /// Body lines of the table at `height`: the user's choice (clamped so the
    /// panes keep their minimum), else a third of the screen.
    fn row_cap_at(&self, height: u16) -> usize {
        self.host_rows_limit
            .unwrap_or_else(|| row_cap(height))
            .clamp(1, max_row_cap(height))
    }

    /// `[`/`]`: one body line less/more.
    pub fn adjust_host_rows(&mut self, delta: i32) {
        let rows = (self.last_row_cap as i32 + delta).max(1) as usize;
        self.host_rows_limit = Some(rows);
    }

    fn set_row_cap(&mut self, cap: usize) {
        if cap != self.last_row_cap {
            self.last_row_cap = cap;
            if self.table_shown() {
                self.set_view_content("per_host", render_host_rows(&self.host_rows, cap));
            }
        }
    }

    /// Shows/hides the per-host table (the worker fetches it only while enabled).
    pub fn set_per_host(&mut self, enabled: bool) {
        if self.per_host_enabled == enabled {
            return;
        }
        self.per_host_enabled = enabled;
        if enabled {
            self.bg_runner.schedule();
        } else {
            self.host_rows.clear();
            self.show_host_table(false);
        }
    }

    fn show_host_table(&mut self, shown: bool) {
        let present = self.layout.len() > PER_HOST_CHILD;
        if shown && !present {
            self.layout.add_child(
                TextView::new(render_host_rows(&self.host_rows, self.last_row_cap))
                    .no_wrap()
                    .with_name("per_host"),
            );
        } else if shown {
            self.set_view_content(
                "per_host",
                render_host_rows(&self.host_rows, self.last_row_cap),
            );
        } else if present {
            self.layout.remove_child(PER_HOST_CHILD);
        }
    }

    fn update_hosts(&mut self, hosts: Vec<ClickHouseHostSummary>) {
        let fmt = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);

        let strip = {
            let no_strip = self
                .context
                .lock()
                .unwrap()
                .options
                .view
                .no_strip_hostname_suffix;
            (!no_strip && hosts.len() > 1).then(|| {
                find_common_hostname_prefix_and_suffix(hosts.iter().map(|h| h.host.as_str()))
            })
        };

        let mut rows = Vec::with_capacity(hosts.len());
        for host in &hosts {
            // update_interval is available only since 23.3
            let update_interval = if host.update_interval > 0. {
                host.update_interval
            } else {
                1.
            };
            let used_cpus = host.cpu.user + host.cpu.system;

            let cpu = usage_cell(
                used_cpus.to_string(),
                host.cpu.count.to_string(),
                used_cpus,
                host.cpu.count,
            );
            let mem = usage_cell(
                fmt.format(host.memory_resident as i64),
                fmt.format(host.memory_total as i64),
                host.memory_resident,
                host.memory_total,
            );

            let rate = |bytes: u64| fmt.format((bytes as f64 / update_interval) as i64);
            rows.push(HostRow {
                host: host.host.clone(),
                cells: vec![
                    StyledString::plain(strip_hostname(&host.host, strip.as_ref())),
                    StyledString::plain(
                        format_duration(Duration::from_secs(host.uptime - host.uptime % 60))
                            .to_string(),
                    ),
                    cpu,
                    mem,
                    StyledString::styled(
                        host.queries.to_string(),
                        get_color_for_ratio(host.queries, 100),
                    ),
                    StyledString::plain(format!(
                        "{}/{}",
                        host.threads_runnable, host.threads_total
                    )),
                    StyledString::plain(format!(
                        "{}/{}",
                        rate(host.network.receive_bytes),
                        rate(host.network.send_bytes)
                    )),
                    StyledString::plain(format!(
                        "{}/{}",
                        rate(host.blkdev.read_bytes),
                        rate(host.blkdev.write_bytes)
                    )),
                ],
            });
        }
        // By name, so that a host keeps its line between refreshes
        rows.sort_by(|a, b| a.host.cmp(&b.host));
        self.host_rows = rows;
        self.show_host_table(self.per_host_enabled && !self.host_rows.is_empty());
    }

    pub fn set_view_content<S>(&mut self, view_name: &str, content: S)
    where
        S: Into<StyledString>,
    {
        let content = content.into();
        call_on_name(&mut self.layout, view_name, move |view: &mut TextView| {
            view.set_content(content);
        });
    }

    /// `hosts` is None when the per-host table is off (or its query failed:
    /// the previous table stays).
    pub fn update(
        &mut self,
        summary: ClickHouseServerSummary,
        hosts: Option<Vec<ClickHouseHostSummary>>,
    ) {
        if let Some(hosts) = hosts {
            self.update_hosts(hosts);
        } else if !self.per_host_enabled {
            self.show_host_table(false);
        }

        let fmt = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);
        let fmt_ref = &fmt;

        // update_interval is available only since 23.3
        let update_interval = if summary.update_interval > 0. {
            summary.update_interval
        } else {
            1.
        };
        let now = Local::now();
        let mut since_prev_us = (now - self.prev_update_time.unwrap_or(Local::now()))
            .num_microseconds()
            .unwrap_or_default() as u64;
        if since_prev_us == 0 {
            since_prev_us = 1;
        }

        {
            let mut description = StyledString::new();
            let mut add_description = |prefix: &str, value: u64, dirty: u64| {
                if value.max(dirty) > 100_000_000 {
                    if !description.is_empty() {
                        description.append_plain(" ");
                    }
                    description.append_plain(format!("{}: ", prefix));
                    description.append_styled(
                        fmt_ref.format(value as i64),
                        get_color_for_ratio(value, summary.memory.resident),
                    );
                    if dirty > 100_000_000 {
                        description.append_plain(" (dirty: ");
                        description.append_styled(
                            fmt_ref.format(dirty as i64),
                            get_color_for_ratio(dirty, summary.memory.resident),
                        );
                        description.append_plain(")");
                    }
                }
            };

            let mut memory_io = summary.memory.io / summary.uptime.server.max(1);
            if let Some(prev_summary) = &self.prev_summary {
                memory_io = (summary.memory.io.saturating_sub(prev_summary.memory.io)) * 1_000_000
                    / since_prev_us;
            }

            add_description("Fragmentation", summary.memory.fragmentation, 0);
            add_description(
                "MergeTree",
                summary.memory.mergetree_arena_active,
                summary.memory.mergetree_arena_dirty,
            );
            add_description(
                "JIT",
                summary.memory.jit_arena_active,
                summary.memory.jit_arena_dirty,
            );

            add_description("Tracked", summary.memory.tracked, 0);
            add_description("Tables", summary.memory.tables, 0);
            add_description("Caches", summary.memory.caches, 0);
            add_description("Queries", summary.memory.queries, 0);
            add_description("Merges Mutations", summary.memory.merges_mutations, 0);
            add_description("Active Merges", summary.memory.active_merges, 0);
            add_description("Dictionaries", summary.memory.dictionaries, 0);
            add_description("Indexes", summary.memory.primary_keys, 0);
            add_description("Index Granulas", summary.memory.index_granularity, 0);
            add_description("IO", memory_io, 0);
            add_description("Async Inserts", summary.memory.async_inserts, 0);

            let memory_no_category = summary
                .memory
                .tracked
                .saturating_sub(summary.memory.tables)
                .saturating_sub(summary.memory.caches)
                .saturating_sub(summary.memory.queries)
                .saturating_sub(summary.memory.active_merges)
                .saturating_sub(summary.memory.dictionaries)
                // Primary keys and index granularity are loaded within the MergeTree jemalloc
                // arena, so they are subsets of its active_bytes (which is zero on servers
                // without the arena)
                .saturating_sub(std::cmp::max(
                    summary.memory.mergetree_arena_active,
                    summary.memory.primary_keys + summary.memory.index_granularity,
                ))
                // CompiledExpressionCacheBytes is a subset of the JIT arena active_bytes, but it
                // is already counted in "Caches", so subtract only the remainder
                .saturating_sub(
                    summary
                        .memory
                        .jit_arena_active
                        .saturating_sub(summary.memory.compiled_expression_cache),
                )
                .saturating_sub(memory_io)
                .saturating_sub(summary.memory.async_inserts);
            add_description("Unknown", memory_no_category, 0);

            self.sparklines.memory.push(summary.memory.resident as f64);
            let mut content = StyledString::plain("");
            content.append_styled(
                fmt_ref.format(summary.memory.resident as i64),
                get_color_for_ratio(summary.memory.resident, summary.memory.os_total),
            );
            content.append_plain(" / ");
            content.append_plain(fmt_ref.format(summary.memory.os_total as i64));
            let spark = self.sparklines.memory.render(SPARKLINE_WIDTH);
            if !spark.is_empty() {
                content.append_plain(" ");
                content.append_styled(spark, Color::Gray);
            }
            content.append_plain(" (");
            content.append(description);
            content.append_plain(")");

            self.set_view_content("mem", content);
        }

        {
            let used_cpus = summary.cpu.user + summary.cpu.system;
            self.sparklines.cpu.push(used_cpus as f64);
            let mut content = StyledString::plain("");
            content.append_styled(
                used_cpus.to_string(),
                get_color_for_ratio(used_cpus, summary.cpu.count),
            );
            content.append_plain(" / ");
            content.append_plain(summary.cpu.count.to_string());
            let spark = self.sparklines.cpu.render(SPARKLINE_WIDTH);
            if !spark.is_empty() {
                content.append_plain(" ");
                content.append_styled(spark, Color::Gray);
            }

            self.set_view_content("cpu", content);
        }

        {
            let mut basic: Vec<String> = Vec::new();
            let mut add_basic = |prefix: &str, value: u64| {
                if value > 0 {
                    basic.push(format!("{}: {}", prefix, value));
                }
            };
            add_basic("HTTP", summary.threads.http);
            add_basic("TCP", summary.threads.tcp);
            add_basic("Interserver", summary.threads.interserver);

            self.set_view_content(
                "threads",
                format!(
                    "{} / {} ({})",
                    summary.threads.os_runnable,
                    summary.threads.os_total,
                    basic.join(", "),
                ),
            );
        }

        {
            let mut pools = StyledString::new();
            let mut add_pool = |prefix: &str, value: u64| {
                if value > 0 {
                    pools.append(StyledString::styled(
                        format!("{}: {} ", prefix, value),
                        get_color_for_ratio(value, summary.cpu.count),
                    ));
                }
            };
            add_pool("Merges", summary.threads.pools.merges_mutations);
            add_pool("Fetches", summary.threads.pools.fetches);
            add_pool("Common", summary.threads.pools.common);
            add_pool("Moves", summary.threads.pools.moves);
            add_pool("Schedule", summary.threads.pools.schedule);
            add_pool("Buffer", summary.threads.pools.buffer_flush);
            add_pool("Distributed", summary.threads.pools.distributed);
            add_pool("Brokers", summary.threads.pools.message_broker);
            add_pool("Backups", summary.threads.pools.backups);
            add_pool("IO", summary.threads.pools.io);
            add_pool("RemoteIO", summary.threads.pools.remote_io);
            add_pool("Queries", summary.threads.pools.queries);

            self.set_view_content("pools", pools);
        }

        self.set_view_content(
            "net_recv",
            fmt_ref.format((summary.network.receive_bytes as f64 / update_interval) as i64),
        );
        self.set_view_content(
            "net_sent",
            fmt_ref.format((summary.network.send_bytes as f64 / update_interval) as i64),
        );

        self.set_view_content(
            "disk_read",
            fmt_ref.format((summary.blkdev.read_bytes as f64 / update_interval) as i64),
        );
        self.set_view_content(
            "disk_write",
            fmt_ref.format((summary.blkdev.write_bytes as f64 / update_interval) as i64),
        );

        let mut selected_rows = summary.rows.selected / summary.uptime.server.max(1);
        let mut inserted_rows = summary.rows.inserted / summary.uptime.server.max(1);
        if let Some(prev_summary) = &self.prev_summary {
            selected_rows = (summary
                .rows
                .selected
                .saturating_sub(prev_summary.rows.selected))
                * 1_000_000
                / since_prev_us;
            inserted_rows = (summary
                .rows
                .inserted
                .saturating_sub(prev_summary.rows.inserted))
                * 1_000_000
                / since_prev_us;
        }
        self.set_view_content("selected_rows", fmt_ref.format(selected_rows as i64));
        self.set_view_content("inserted_rows", fmt_ref.format(inserted_rows as i64));

        self.set_view_content(
            "uptime",
            format_duration(Duration::from_secs(summary.uptime.server)).to_string(),
        );

        self.set_view_content("servers", summary.servers.to_string());
        {
            let fmt_rows = SizeFormatter::new()
                .with_base(Base::Base10)
                .with_style(Style::Abbreviated);
            let mut content = StyledString::new();
            content.append_styled(
                fmt_rows.format(summary.storages.total_rows as i64),
                get_color_for_bytes(summary.storages.total_bytes),
            );
            content.append_plain(" / ");
            content.append_styled(
                fmt_ref.format(summary.storages.total_bytes as i64),
                get_color_for_bytes(summary.storages.total_bytes),
            );
            self.set_view_content("total_data", content);
        }

        {
            self.sparklines.queries.push(summary.queries as f64);
            let mut content = StyledString::plain("");
            content.append_styled(
                summary.queries.to_string(),
                get_color_for_ratio(summary.queries, summary.servers * 100),
            );
            let spark = self.sparklines.queries.render(SPARKLINE_WIDTH);
            if !spark.is_empty() {
                content.append_plain(" ");
                content.append_styled(spark, Color::Gray);
            }
            self.set_view_content("queries", content);
        }

        {
            self.sparklines.merges.push(summary.merges as f64);

            let mut opt = StyledString::new();
            let mut add_opt = |label: &str, content: StyledString| {
                if !opt.is_empty() {
                    opt.append_plain(" ");
                }
                opt.append_styled(label, Color::Cyan);
                opt.append_plain(" ");
                opt.append(content);
            };

            if summary.merges > 0 {
                let mut c = StyledString::new();
                c.append_styled(
                    summary.merges.to_string(),
                    get_color_for_ratio(summary.merges, summary.servers * 20),
                );
                let spark = self.sparklines.merges.render(SPARKLINE_WIDTH);
                if !spark.is_empty() {
                    c.append_plain(" ");
                    c.append_styled(spark, Color::Gray);
                }
                add_opt("Merges:", c);
            }

            if summary.mutations > 0 {
                let mut c = StyledString::new();
                c.append_styled(
                    summary.mutations.to_string(),
                    get_color_for_ratio(summary.mutations, summary.servers * 8),
                );
                add_opt("Mutations:", c);
            }

            if summary.fetches > 0 {
                let mut c = StyledString::new();
                c.append_styled(
                    summary.fetches.to_string(),
                    get_color_for_ratio(summary.fetches, summary.servers * 20),
                );
                add_opt("Fetches:", c);
            }

            if summary.replication_max_absolute_delay > 0 {
                let mut c = StyledString::new();
                c.append_styled(
                    format_duration(Duration::from_secs(summary.replication_max_absolute_delay))
                        .to_string(),
                    get_color_for_ratio(summary.replication_max_absolute_delay, 60),
                );
                add_opt("Lag:", c);
            }

            if summary.replication_queue > 0 {
                let mut c = StyledString::new();
                c.append_styled(
                    summary.replication_queue.to_string(),
                    get_color_for_ratio(summary.replication_queue, summary.servers * 20),
                );
                c.append_plain(" (");
                c.append_styled(
                    summary.replication_queue_tries.to_string(),
                    get_color_for_ratio(
                        summary.replication_queue_tries,
                        summary.replication_queue * 2,
                    ),
                );
                c.append_plain(")");
                add_opt("RepQueue:", c);
            }

            if summary.storages.buffer_bytes > 0 {
                let mut c = StyledString::new();
                c.append_styled(
                    fmt_ref.format(summary.storages.buffer_bytes as i64),
                    get_color_for_ratio(summary.storages.buffer_bytes, summary.memory.os_total),
                );
                add_opt("Buffers:", c);
            }

            if summary.storages.distributed_insert_files > 0 {
                let mut c = StyledString::new();
                c.append_styled(
                    summary.storages.distributed_insert_files.to_string(),
                    get_color_for_ratio(summary.storages.distributed_insert_files, 10000),
                );
                add_opt("DistInserts:", c);
            }

            self.set_view_content("optional_metrics", opt);
        }

        self.prev_summary = Some(summary);
        self.prev_update_time = Some(now);
    }
}

impl Component for SummaryView {
    fn draw(&mut self, canvas: &mut Canvas<'_>, area: Rect, focused: bool) {
        self.last_area = area;
        if !self.table_shown() || area.height < 2 {
            self.layout.draw(canvas, area, focused);
            return;
        }
        // The last row is the separator (the drag handle), like the pane ones
        let inner = Rect::new(area.x, area.y, area.width, area.height - 1);
        self.layout.draw(canvas, inner, focused);
        let y = area.bottom() - 1;
        for x in area.left()..area.right() {
            print_str(canvas.buf, x, y, area, "\u{2500}", TextStyle::default());
        }
    }

    fn required_size(&mut self, max: Size) -> Size {
        // The layout hands the summary the whole remaining screen height, so
        // the cap follows terminal resizes (and keeps the panes' minimum)
        self.set_row_cap(self.row_cap_at(max.height));
        let mut size = self.layout.required_size(max);
        if self.table_shown() {
            size.height = (size.height + 1).min(max.height);
        }
        size
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        if let Event::Mouse {
            position,
            event: mouse,
        } = event
            && self.table_shown()
        {
            let separator_y = self.last_area.bottom().saturating_sub(1);
            match mouse {
                MouseEvent::Press(MouseButton::Left) if position.y == separator_y => {
                    self.resizing = true;
                    return EventResult::consumed();
                }
                MouseEvent::Hold(MouseButton::Left) if self.resizing => {
                    // Body lines between the header and the pointer (the
                    // separator lands where the pointer is)
                    let body_top = self.last_area.y as i32 + 4 + 1;
                    let rows = (position.y as i32 - body_top).max(1) as usize;
                    self.host_rows_limit = Some(rows);
                    return EventResult::consumed();
                }
                MouseEvent::Release(MouseButton::Left) if self.resizing => {
                    self.resizing = false;
                    return EventResult::consumed();
                }
                _ => {}
            }
        }
        self.layout.on_event(event)
    }

    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        f(&mut self.layout);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(host: &str) -> HostRow {
        HostRow {
            host: host.to_string(),
            cells: PER_HOST_COLUMNS
                .iter()
                .enumerate()
                .map(|(i, _)| StyledString::plain(format!("{}{}", host, "x".repeat(i))))
                .collect(),
        }
    }

    #[test]
    fn test_row_cap() {
        assert_eq!(row_cap(10), 4);
        assert_eq!(row_cap(24), 8);
        assert_eq!(row_cap(60), 20);
    }

    #[test]
    fn test_cap() {
        let rows: Vec<HostRow> = ["a", "b", "c", "d", "e", "f"]
            .into_iter()
            .map(row)
            .collect();

        // 4 body lines: 3 rows and the "more" line
        let text = render_host_rows(&rows, 4).source();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 1 + 4);
        assert!(lines[1].starts_with("a "));
        assert_eq!(lines[4], "... and 3 more hosts");
        // Columns are aligned: every line has the same width
        let width = lines[0].chars().count();
        for line in &lines[..4] {
            assert_eq!(line.chars().count(), width, "{line:?}");
        }
        // Everything fits: no "more" line
        let text = render_host_rows(&rows, 6).source();
        assert_eq!(text.lines().count(), 1 + 6);
    }

    #[test]
    fn test_max_row_cap() {
        // 4 rows + header + separator + 6 for the panes = 12 fixed
        assert_eq!(max_row_cap(30), 18);
        assert_eq!(max_row_cap(12), 1);
        assert_eq!(max_row_cap(5), 1);
    }
}
