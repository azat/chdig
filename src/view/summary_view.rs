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

use crate::interpreter::clickhouse::ClickHouseServerSummary;

pub struct SummaryView {
    layout: views::LinearLayout,
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
    pub fn new() -> Self {
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
                    .child(views::TextView::new("").with_name("queries")),
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

        return Self { layout };
    }

    pub fn update(&mut self, summary: ClickHouseServerSummary) {
        let fmt = Rc::new(
            SizeFormatter::new()
                .with_base(Base::Base2)
                .with_style(Style::Abbreviated),
        );
        let fmt_ref = fmt.as_ref();

        self.call_on_all(&Selector::Name("mem"), move |view: &mut views::TextView| {
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
            view.set_content(content);
        });

        self.call_on_all(&Selector::Name("cpu"), move |view: &mut views::TextView| {
            let mut content = StyledString::plain("");
            let used_cpus = summary.cpu.user + summary.cpu.system;
            content.append_styled(
                used_cpus.to_string(),
                get_color_for_ratio(used_cpus, summary.cpu.count),
            );
            content.append_plain(" / ");
            content.append_plain(summary.cpu.count.to_string());
            view.set_content(content);
        });

        self.call_on_all(
            &Selector::Name("threads"),
            move |view: &mut views::TextView| {
                let mut basic: Vec<String> = Vec::new();
                let mut add_basic = |prefix: &str, value: u64| {
                    if value > 0 {
                        basic.push(format!("{}: {}", prefix, value));
                    }
                };
                add_basic("HTTP", summary.threads.http);
                add_basic("TCP", summary.threads.tcp);
                add_basic("Interserver", summary.threads.interserver);

                view.set_content(format!(
                    "{} / {} ({})",
                    summary.threads.os_runnable,
                    summary.threads.os_total,
                    basic.join(", "),
                ));
            },
        );

        self.call_on_all(
            &Selector::Name("pools"),
            move |view: &mut views::TextView| {
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

                view.set_content(pools.join(", "));
            },
        );

        self.call_on_all(
            &Selector::Name("net_recv"),
            move |view: &mut views::TextView| {
                view.set_content(fmt_ref.format(summary.network.receive_bytes as i64));
            },
        );
        self.call_on_all(
            &Selector::Name("net_sent"),
            move |view: &mut views::TextView| {
                view.set_content(fmt_ref.format(summary.network.send_bytes as i64));
            },
        );

        self.call_on_all(
            &Selector::Name("disk_read"),
            move |view: &mut views::TextView| {
                view.set_content(fmt_ref.format(summary.blkdev.read_bytes as i64));
            },
        );
        self.call_on_all(
            &Selector::Name("disk_write"),
            move |view: &mut views::TextView| {
                view.set_content(fmt_ref.format(summary.blkdev.write_bytes as i64));
            },
        );

        self.call_on_all(
            &Selector::Name("uptime"),
            move |view: &mut views::TextView| {
                view.set_content(
                    format_duration(Duration::from_secs(summary.uptime.server)).to_string(),
                );
            },
        );

        self.call_on_all(
            &Selector::Name("servers"),
            move |view: &mut views::TextView| {
                view.set_content(summary.servers.to_string());
            },
        );

        self.call_on_all(
            &Selector::Name("queries"),
            move |view: &mut views::TextView| {
                view.set_content(summary.processes.to_string());
            },
        );
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
