use crate::interpreter::{flamegraph, ContextArc};
use cursive::views;
use size::{Base, SizeFormatter, Style};
use std::rc::Rc;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

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
                            view.set_content(format!(
                                "{} / {}",
                                fmt_ref.format(summary.memory_resident as i64),
                                fmt_ref.format(summary.os_memory_total as i64)
                            ));
                        })
                        .expect("No such view 'mem'");

                        siv.call_on_name("cpu", move |view: &mut views::TextView| {
                            view.set_content(format!(
                                "{:.2} % ({} cpus)",
                                (summary.cpu_user + summary.cpu_system) / summary.os_uptime * 100,
                                summary.cpu_count,
                            ));
                        })
                        .expect("No such view 'cpu'");

                        siv.call_on_name("net", move |view: &mut views::TextView| {
                            view.set_content(format!(
                                "IN {} / OUT {}",
                                fmt_ref.format(summary.net_receive_bytes as i64),
                                fmt_ref.format(summary.net_send_bytes as i64)
                            ));
                        })
                        .expect("No such view 'net'");

                        siv.call_on_name("disk", move |view: &mut views::TextView| {
                            view.set_content(format!(
                                "READ {} / WRITE {}",
                                fmt_ref.format(summary.block_read_bytes as i64),
                                fmt_ref.format(summary.block_write_bytes as i64)
                            ));
                        })
                        .expect("No such view 'disk'");
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
