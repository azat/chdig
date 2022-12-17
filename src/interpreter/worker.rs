use crate::interpreter::{flamegraph, ContextArc};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

pub enum Event {
    UpdateProcessList,
    GetQueryTextLog,
    ShowServerFlameGraph,
    ShowQueryFlameGraph,
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
            Event::GetQueryTextLog => {
                let query_id = context_locked.query_id.clone();
                context_locked.query_logs = Some(
                    context_locked
                        .clickhouse
                        .get_query_logs(query_id.as_str())
                        .await,
                );
            }
            Event::ShowServerFlameGraph => {
                let flamegraph_block = context_locked.clickhouse.get_server_flamegraph().await;
                // NOTE: should be do this via cursive, to block the UI?
                flamegraph::show(flamegraph_block);
                need_clear = true;
            }
            Event::ShowQueryFlameGraph => {
                let query_id = context_locked.query_id.clone();
                let flamegraph_block = context_locked
                    .clickhouse
                    .get_query_flamegraph(query_id.as_str())
                    .await;
                // NOTE: should be do this via cursive, to block the UI?
                flamegraph::show(flamegraph_block);
                need_clear = true;
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
            .unwrap();
    }
}
