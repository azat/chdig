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
        let update_callback = move || {
            update_callback_context
                .lock()
                .unwrap()
                .worker
                .send(WorkerEvent::UpdateSummary);
        };

        let layout = views::LinearLayout::vertical()
            .child(
                views::LinearLayout::horizontal()
                    .child(views::TextView::new(StyledString::styled(
                        "Uptime:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("uptime"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Servers:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("servers"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "CPU:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("cpu"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Queries:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("queries"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Merges:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("merges"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Buffers:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("storage_buffer_bytes"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "DistInserts:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("storage_distributed_insert_files"))
                    .child(views::DummyView.fixed_width(1)),
            )
            .child(
                views::LinearLayout::horizontal()
                    .child(views::TextView::new(StyledString::styled(
                        "Net recv:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("net_recv"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Net sent:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("net_sent"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Read:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("disk_read"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Write:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("disk_write")),
            )
            .child(
                views::LinearLayout::horizontal()
                    .child(views::TextView::new(StyledString::styled(
                        "Threads:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("threads"))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new(StyledString::styled(
                        "Pools:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("pools")),
            )
            .child(
                views::LinearLayout::horizontal()
                    .child(views::TextView::new(StyledString::styled(
                        "Memory:",
                        BaseColor::Red.dark(),
                    )))
                    .child(views::DummyView.fixed_width(1))
                    .child(views::TextView::new("").with_name("mem")),
            );

        let mut bg_runner = BackgroundRunner::new(delay);
        bg_runner.start(update_callback);

        return Self { layout, bg_runner };
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

        {
            let mut description: Vec<String> = Vec::new();
            let mut add_description = |prefix: &str, value: u64| {
                if value > 100_000_000 {
                    description.push(format!("{}: {}", prefix, fmt_ref.format(value as i64)));
                }
            };
            add_description("Tracked", summary.memory.tracked);
            add_description("Tables", summary.memory.tables);
            add_description("Caches", summary.memory.caches);
            add_description("Queries", summary.memory.processes);
            add_description("Merges", summary.memory.merges);
            add_description("Dictionaries", summary.memory.dictionaries);
            add_description("Indexes", summary.memory.primary_keys);

            let mut content = StyledString::plain("");
            content.append_styled(
                fmt_ref.format(summary.memory.resident as i64),
                get_color_for_ratio(summary.memory.resident, summary.memory.os_total),
            );
            content.append_plain(" / ");
            content.append_plain(fmt_ref.format(summary.memory.os_total as i64));
            content.append_plain(format!(" ({})", description.join(", ")));

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
            let mut pools: Vec<String> = Vec::new();
            let mut add_pool = |prefix: &str, value: u64| {
                if value > 0 {
                    pools.push(format!("{}: {}", prefix, value));
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

            self.set_view_content("pools", pools.join(", "));
        }

        self.set_view_content(
            "net_recv",
            fmt_ref.format(summary.network.receive_bytes as i64),
        );
        self.set_view_content(
            "net_sent",
            fmt_ref.format(summary.network.send_bytes as i64),
        );

        self.set_view_content(
            "disk_read",
            fmt_ref.format(summary.blkdev.read_bytes as i64),
        );
        self.set_view_content(
            "disk_write",
            fmt_ref.format(summary.blkdev.write_bytes as i64),
        );

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
    }
}

impl View for SummaryView {
    fn draw(&self, printer: &Printer) {
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

    fn call_on_any(&mut self, selector: &Selector, callback: AnyCb) {
        self.layout.call_on_any(selector, callback);
    }

    // FIXME: do we need other methods?
}
