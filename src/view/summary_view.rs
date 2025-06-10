use chrono::{DateTime, Local};
use cursive::{
    event::{AnyCb, Event, EventResult},
    theme::BaseColor,
    utils::markup::StyledString,
    view::{Finder, Nameable, Resizable, Selector, View},
    views, Printer, Vec2,
};
use humantime::format_duration;
use size::{Base, SizeFormatter, Style};
use std::rc::Rc;
use std::time::Duration;

use crate::interpreter::{
    clickhouse::ClickHouseServerSummary, BackgroundRunner, ContextArc, WorkerEvent,
};

pub struct SummaryView {
    prev_summary: Option<ClickHouseServerSummary>,
    prev_update_time: Option<DateTime<Local>>,

    layout: views::LinearLayout,

    #[allow(unused)]
    bg_runner: BackgroundRunner,
}

fn get_color_for_ratio(used: u64, total: u64) -> cursive::theme::Color {
    let q = used as f64 / total as f64;
    return if q > 0.90 {
        BaseColor::Red.dark()
    } else if q > 0.5 {
        BaseColor::Yellow.dark()
    } else {
        BaseColor::Green.dark()
    };
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
                .send(force, WorkerEvent::UpdateSummary);
        };

        let layout = views::LinearLayout::vertical()
            .child(
                views::LinearLayout::horizontal()
                    .child(views::TextView::new(StyledString::styled(
                        "Uptime:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("uptime"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Servers:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("servers"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "CPU:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("cpu"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Queries:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("queries"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Merges:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("merges"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Mutations:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("mutations"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Fetches:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("fetches"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "RepQueue:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("replication_queue"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Buffers:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("storage_buffer_bytes"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "DistInserts:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("storage_distributed_insert_files"))
                    .child(views::DummyView.fixed_width(1)),
            )
            .child(
                views::LinearLayout::horizontal()
                    .child(views::TextView::new(StyledString::styled(
                        "Net recv:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("net_recv"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Net sent:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("net_sent"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Read:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("disk_read"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Write:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("disk_write"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Selected rows:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("selected_rows"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Inserted rows:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("inserted_rows")),
            )
            .child(
                views::LinearLayout::horizontal()
                    .child(views::TextView::new(StyledString::styled(
                        "Threads:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("threads"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Pools:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("pools")),
            )
            .child(
                views::LinearLayout::horizontal()
                    .child(views::TextView::new(StyledString::styled(
                        "Memory:",
                        BaseColor::Cyan.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("mem")),
            );

        let bg_runner_cv = context.lock().unwrap().background_runner_cv.clone();
        let bg_runner_force = context
            .lock()
            .unwrap()
            .background_runner_summary_force
            .clone();
        let mut bg_runner = BackgroundRunner::new(delay, bg_runner_cv, bg_runner_force);
        bg_runner.start(update_callback);

        return Self {
            prev_summary: None,
            prev_update_time: None,
            layout,
            bg_runner,
        };
    }

    pub fn set_view_content<S>(&mut self, view_name: &str, content: S)
    where
        S: Into<StyledString> + Clone,
    {
        self.call_on_name(view_name, move |view: &mut views::TextView| {
            view.set_content(content);
        });
    }

    pub fn update(&mut self, summary: ClickHouseServerSummary) {
        let fmt = Rc::new(
            SizeFormatter::new()
                .with_base(Base::Base2)
                .with_style(Style::Abbreviated),
        );
        let fmt_ref = fmt.as_ref();

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
            let mut add_description = |prefix: &str, value: u64| {
                if value > 100_000_000 {
                    if !description.is_empty() {
                        description.append_plain(" ");
                    }
                    description.append_plain(format!("{}: ", prefix));
                    description.append_styled(
                        fmt_ref.format(value as i64),
                        get_color_for_ratio(value, summary.memory.resident),
                    );
                }
            };

            add_description("Fragmentation", summary.memory.fragmentation);

            add_description("Tracked", summary.memory.tracked);
            add_description("Tables", summary.memory.tables);
            add_description("Caches", summary.memory.caches);
            add_description("Queries", summary.memory.processes);
            add_description("Merges", summary.memory.merges);
            add_description("Dictionaries", summary.memory.dictionaries);
            add_description("Indexes", summary.memory.primary_keys);
            add_description("Index Granulas", summary.memory.index_granularity);
            add_description("Async Inserts", summary.memory.async_inserts);

            let memory_no_category = summary
                .memory
                .tracked
                .saturating_sub(summary.memory.tables)
                .saturating_sub(summary.memory.caches)
                .saturating_sub(summary.memory.processes)
                .saturating_sub(summary.memory.merges)
                .saturating_sub(summary.memory.dictionaries)
                .saturating_sub(summary.memory.primary_keys)
                .saturating_sub(summary.memory.index_granularity)
                .saturating_sub(summary.memory.async_inserts);
            add_description("Unknown", memory_no_category);

            let mut content = StyledString::plain("");
            content.append_styled(
                fmt_ref.format(summary.memory.resident as i64),
                get_color_for_ratio(summary.memory.resident, summary.memory.os_total),
            );
            content.append_plain(" / ");
            content.append_plain(fmt_ref.format(summary.memory.os_total as i64));
            content.append_plain(" (");
            content.append(description);
            content.append_plain(")");

            self.set_view_content("mem", content);
        }

        {
            let mut content = StyledString::plain("");
            let used_cpus = summary.cpu.user + summary.cpu.system;
            content.append_styled(
                used_cpus.to_string(),
                get_color_for_ratio(used_cpus, summary.cpu.count),
            );
            content.append_plain(" / ");
            content.append_plain(summary.cpu.count.to_string());

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
            let mut content = StyledString::plain("");
            content.append_styled(
                summary.processes.to_string(),
                get_color_for_ratio(summary.processes, summary.servers * 100),
            );
            self.set_view_content("queries", content);
        }

        {
            let mut content = StyledString::plain("");
            content.append_styled(
                summary.merges.to_string(),
                get_color_for_ratio(summary.merges, summary.servers * 20),
            );
            self.set_view_content("merges", content);
        }

        {
            let mut content = StyledString::plain("");
            content.append_styled(
                summary.mutations.to_string(),
                get_color_for_ratio(summary.mutations, summary.servers * 8),
            );
            self.set_view_content("mutations", content);
        }

        {
            let mut content = StyledString::plain("");
            content.append_styled(
                summary.replication_queue.to_string(),
                get_color_for_ratio(summary.replication_queue, summary.servers * 20),
            );
            content.append(" (");
            content.append_styled(
                summary.replication_queue_tries.to_string(),
                get_color_for_ratio(
                    summary.replication_queue_tries,
                    summary.replication_queue * 2,
                ),
            );
            content.append(")");
            self.set_view_content("replication_queue", content);
        }

        {
            let mut content = StyledString::plain("");
            content.append_styled(
                summary.fetches.to_string(),
                get_color_for_ratio(summary.fetches, summary.servers * 20),
            );
            self.set_view_content("fetches", content);
        }

        {
            let mut content = StyledString::plain("");
            content.append_styled(
                fmt_ref.format(summary.storages.buffer_bytes as i64),
                get_color_for_ratio(summary.storages.buffer_bytes, summary.memory.os_total),
            );
            self.set_view_content("storage_buffer_bytes", content);
        }

        {
            let mut content = StyledString::plain("");
            content.append_styled(
                summary.storages.distributed_insert_files.to_string(),
                get_color_for_ratio(summary.storages.distributed_insert_files, 10000),
            );
            self.set_view_content("storage_distributed_insert_files", content);
        }

        self.prev_summary = Some(summary);
        self.prev_update_time = Some(now);
    }
}

impl View for SummaryView {
    fn draw(&self, printer: &Printer<'_, '_>) {
        self.layout.draw(printer);
    }

    fn needs_relayout(&self) -> bool {
        return self.layout.needs_relayout();
    }

    fn layout(&mut self, size: Vec2) {
        self.layout.layout(size);
    }

    fn required_size(&mut self, req: Vec2) -> Vec2 {
        return self.layout.required_size(req);
    }

    fn on_event(&mut self, event: Event) -> EventResult {
        return self.layout.on_event(event);
    }

    fn call_on_any(&mut self, selector: &Selector<'_>, callback: AnyCb<'_>) {
        self.layout.call_on_any(selector, callback);
    }

    // FIXME: do we need other methods?
}
