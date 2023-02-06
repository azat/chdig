use crate::interpreter::{flamegraph, ContextArc};
use cursive::views;
use humantime::format_duration;
use size::{Base, SizeFormatter, Style};
use std::rc::Rc;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

pub enum Event {
    UpdateProcessList,
    GetQueryTextLog(String),
    ShowServerFlameGraph,
    ShowQueryFlameGraph(String),
    UpdateSummary,
}

type ReceiverArc = Arc<Mutex<mpsc::Receiver<Event>>>;
type Sender = mpsc::Sender<Event>;

pub struct Worker {
    pub context: Option<ContextArc>,
    sender: Sender,
    receiver: ReceiverArc,
    thread: Option<thread::JoinHandle<()>>,
}

// TODO: can we simplify things with callbacks? (EnumValue(Type))
impl Worker {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel::<Event>();
        let receiver = Arc::new(Mutex::new(receiver));

        return Worker {
            context: None,
            sender,
            receiver,
            thread: None,
        };
    }

    pub fn start(&mut self, context: ContextArc) {
        let receiver = self.receiver.clone();
        self.thread = Some(std::thread::spawn(move || {
            start_tokio(context, receiver);
        }));
    }

    pub fn send(&mut self, event: Event) {
        self.sender.send(event).unwrap();
    }
}

#[tokio::main]
async fn start_tokio(context: ContextArc, receiver: ReceiverArc) {
    while let Ok(event) = receiver.lock().unwrap().recv() {
        let mut context_locked = context.lock().unwrap();
        let mut need_clear = false;

        match event {
            Event::UpdateProcessList => {
                context_locked.processes = Some(context_locked.clickhouse.get_processlist().await);
            }
            Event::GetQueryTextLog(query_id) => {
                context_locked.query_logs = Some(
                    context_locked
                        .clickhouse
                        .get_query_logs(query_id.as_str())
                        .await,
                );
            }
            Event::ShowServerFlameGraph => {
                let flamegraph_block = context_locked.clickhouse.get_server_flamegraph().await;
                // NOTE: should we do this via cursive, to block the UI?
                flamegraph::show(flamegraph_block);
                need_clear = true;
            }
            Event::ShowQueryFlameGraph(query_id) => {
                let flamegraph_block = context_locked
                    .clickhouse
                    .get_query_flamegraph(query_id.as_str())
                    .await;
                // NOTE: should we do this via cursive, to block the UI?
                flamegraph::show(flamegraph_block);
                need_clear = true;
            }
            Event::UpdateSummary => {
                let summary = context_locked.clickhouse.get_summary().await.unwrap();
                // FIXME: "leaky abstractions"
                context_locked
                    .cb_sink
                    .send(Box::new(move |siv: &mut cursive::Cursive| {
                        let fmt = Rc::new(
                            SizeFormatter::new()
                                .with_base(Base::Base10)
                                .with_style(Style::Abbreviated),
                        );
                        let fmt_ref = fmt.as_ref();

                        siv.call_on_name("mem", move |view: &mut views::TextView| {
                            let mut description: Vec<String> = Vec::new();

                            let mut add_description = |prefix: &str, value: u64| {
                                if value > 100_000_000 {
                                    description.push(format!(
                                        "{}: {}",
                                        prefix,
                                        fmt_ref.format(value as i64)
                                    ));
                                }
                            };
                            add_description("T", summary.memory.tracked);
                            add_description("t", summary.memory.tables);
                            add_description("C", summary.memory.caches);
                            add_description("P", summary.memory.processes);
                            add_description("M", summary.memory.merges);
                            add_description("D", summary.memory.dictionaries);
                            add_description("K", summary.memory.primary_keys);

                            view.set_content(format!(
                                "{} / {} ({})",
                                fmt_ref.format(summary.memory.resident as i64),
                                fmt_ref.format(summary.memory.os_total as i64),
                                description.join(", "),
                            ));
                        })
                        .expect("No such view 'mem'");

                        siv.call_on_name("cpu", move |view: &mut views::TextView| {
                            view.set_content(format!(
                                "{} / {}",
                                summary.cpu.user + summary.cpu.system,
                                summary.cpu.count,
                            ));
                        })
                        .expect("No such view 'cpu'");

                        siv.call_on_name("threads", move |view: &mut views::TextView| {
                            let mut basic: Vec<String> = Vec::new();
                            let mut add_basic = |prefix: &str, value: u64| {
                                if value > 0 {
                                    basic.push(format!("{}: {}", prefix, value));
                                }
                            };
                            add_basic("H", summary.threads.http);
                            add_basic("T", summary.threads.tcp);
                            add_basic("I", summary.threads.interserver);

                            let mut pools: Vec<String> = Vec::new();
                            let mut add_pool = |prefix: &str, value: u64| {
                                if value > 0 {
                                    pools.push(format!("{}: {}", prefix, value));
                                }
                            };
                            add_pool("M", summary.threads.pools.merges_mutations);
                            add_pool("F", summary.threads.pools.fetches);
                            add_pool("C", summary.threads.pools.common);
                            add_pool("m", summary.threads.pools.moves);
                            add_pool("S", summary.threads.pools.schedule);
                            add_pool("F", summary.threads.pools.buffer_flush);
                            add_pool("D", summary.threads.pools.distributed);
                            add_pool("B", summary.threads.pools.message_broker);

                            view.set_content(format!(
                                "{} / {} ({}) P({})",
                                summary.threads.os_runnable,
                                summary.threads.os_total,
                                basic.join(", "),
                                pools.join(", "),
                            ));
                        })
                        .expect("No such view 'threads'");

                        siv.call_on_name("net", move |view: &mut views::TextView| {
                            view.set_content(format!(
                                "IN {} / OUT {}",
                                fmt_ref.format(summary.network.receive_bytes as i64),
                                fmt_ref.format(summary.network.send_bytes as i64)
                            ));
                        })
                        .expect("No such view 'net'");

                        siv.call_on_name("disk", move |view: &mut views::TextView| {
                            view.set_content(format!(
                                "READ {} / WRITE {}",
                                fmt_ref.format(summary.blkdev.read_bytes as i64),
                                fmt_ref.format(summary.blkdev.write_bytes as i64)
                            ));
                        })
                        .expect("No such view 'disk'");

                        siv.call_on_name("uptime", move |view: &mut views::TextView| {
                            view.set_content(format!(
                                "{}",
                                format_duration(Duration::from_secs(summary.uptime.server)),
                            ));
                        })
                        .expect("No such view 'uptime'");
                    }))
                    .unwrap_or_default();
            }
        }

        context_locked
            .cb_sink
            .send(Box::new(move |siv: &mut cursive::Cursive| {
                if need_clear {
                    siv.clear();
                }
                siv.on_event(cursive::event::Event::Refresh);
            }))
            // Ignore errors on exit
            .unwrap_or_default();
    }
}
