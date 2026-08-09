use chrono::{DateTime, Local};
use humantime::format_duration;
use ratatui::layout::{Rect, Size};
use size::{Base, SizeFormatter, Style};
use std::time::Duration;

use crate::common::sparkline::SparklineBuffer;
use crate::interpreter::{
    BackgroundRunner, ContextArc, WorkerEvent, clickhouse::ClickHouseServerSummary,
};
use crate::tui::component::{Canvas, Component, DummyView, Nameable, call_on_name};
use crate::tui::event::{Event, EventResult};
use crate::tui::linear::LinearLayout;
use crate::tui::resize::Resizable;
use crate::tui::style::{Color, StyledString};
use crate::tui::text::TextView;

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

pub struct SummaryView {
    prev_summary: Option<ClickHouseServerSummary>,
    prev_update_time: Option<DateTime<Local>>,

    layout: LinearLayout,
    sparklines: SparklineSet,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
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

        Self {
            prev_summary: None,
            prev_update_time: None,
            layout,
            sparklines: SparklineSet::new(),
            bg_runner,
        }
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

    pub fn update(&mut self, summary: ClickHouseServerSummary) {
        let fmt = SizeFormatter::new()
            .with_base(Base::Base2)
            .with_style(Style::Abbreviated);
        let fmt_ref = &fmt;

        // update_interval is available only since 23.3
        let update_interval = if summary.update_interval > 0 {
            summary.update_interval
        } else {
            1
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
            fmt_ref.format((summary.network.receive_bytes / update_interval) as i64),
        );
        self.set_view_content(
            "net_sent",
            fmt_ref.format((summary.network.send_bytes / update_interval) as i64),
        );

        self.set_view_content(
            "disk_read",
            fmt_ref.format((summary.blkdev.read_bytes / update_interval) as i64),
        );
        self.set_view_content(
            "disk_write",
            fmt_ref.format((summary.blkdev.write_bytes / update_interval) as i64),
        );

        let mut selected_rows = summary.rows.selected / summary.uptime.server;
        let mut inserted_rows = summary.rows.inserted / summary.uptime.server;
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
        self.layout.draw(canvas, area, focused);
    }

    fn required_size(&mut self, max: Size) -> Size {
        self.layout.required_size(max)
    }

    fn on_event(&mut self, event: &Event) -> EventResult {
        self.layout.on_event(event)
    }

    fn for_each_child(&mut self, f: &mut dyn FnMut(&mut dyn Component)) {
        f(&mut self.layout);
    }
}
