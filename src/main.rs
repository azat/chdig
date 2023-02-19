use anyhow::Result;
use backtrace::Backtrace;
use crossterm::{
    event::DisableMouseCapture,
    execute,
    style::Print,
    terminal::{disable_raw_mode, LeaveAlternateScreen},
};
use cursive::event::Key;
use cursive::view::{Nameable, Resizable};
use cursive::views;
use cursive_flexi_logger_view::toggle_flexi_logger_debug_console;
use flexi_logger::Logger;
use std::io;
use std::panic::{self, PanicInfo};

mod interpreter;
mod view;

use crate::interpreter::{options, Context, ContextArc, WorkerEvent};

fn panic_hook(info: &PanicInfo<'_>) {
    if cfg!(debug_assertions) {
        let location = info.location().unwrap();

        let msg = match info.payload().downcast_ref::<&'static str>() {
            Some(s) => *s,
            None => match info.payload().downcast_ref::<String>() {
                Some(s) => &s[..],
                None => "Box<Any>",
            },
        };

        // TODO:
        // - print only if RUST_BACKTRACE was set
        // - trim panic frames
        let stacktrace: String = format!("{:?}", Backtrace::new()).replace('\n', "\n\r");

        disable_raw_mode().unwrap();
        execute!(
            io::stdout(),
            LeaveAlternateScreen,
            Print(format!(
                "thread '<unnamed>' panicked at '{}', {}\n\r{}",
                msg, location, stacktrace
            )),
            DisableMouseCapture
        )
        .unwrap();
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut siv = cursive::default();

    // Override with RUST_LOG
    Logger::try_with_env_or_str("trace,cursive=info,clickhouse_rs=info")
        .expect("Could not create Logger from environment")
        .log_to_writer(cursive_flexi_logger_view::cursive_flexi_logger(&siv))
        // FIXME: there is some non interpreted pattern - "%T%.3f"
        .format(flexi_logger::colored_with_thread)
        .start()
        .expect("Failed to initialize logger");

    let options = options::parse();

    let context: ContextArc = Context::new(options, siv.cb_sink().clone()).await?;
    let context_ref = context.clone();

    let theme = view::utils::make_cursive_theme_from_therminal(&siv);
    siv.set_theme(theme);

    view::utils::add_menu(&mut siv);
    view::utils::add_fuzzy_shortcuts(&mut siv);

    // TODO: Bindings:
    // - C-J - show the end of the queries (like in top(1))
    siv.add_global_callback('q', view::utils::pop_ui);
    siv.add_global_callback('F', |siv: &mut cursive::Cursive| {
        siv.user_data::<ContextArc>()
            .unwrap()
            .lock()
            .unwrap()
            .worker
            .send(WorkerEvent::ShowServerFlameGraph);
    });
    siv.add_global_callback(Key::Backspace, view::utils::pop_ui);
    // NOTE: Do not find to Esc, since this breaks other bindings (Home/End/...)`
    siv.add_global_callback(Key::F1, view::utils::show_help_dialog);
    siv.add_global_callback('~', toggle_flexi_logger_debug_console);
    siv.set_user_data(context);
    // TODO: disable mouse support (sigh)

    // TODO: Bindings:
    // - space - multiquery selection (KILL, flamegraphs, logs, ...)
    //
    // TODO:
    // - update the table
    siv.add_fullscreen_layer(
        views::LinearLayout::vertical()
            .child(view::SummaryView::new().with_name("summary"))
            .child(
                views::Dialog::around(
                    view::ProcessesView::new(context_ref.clone())
                        .expect("Cannot get processlist")
                        .with_name("processes")
                        .min_size((500, 200)),
                )
                .title(format!(
                    "processlist ({})",
                    context_ref.lock().unwrap().server_version
                )),
            ),
    );

    context_ref
        .lock()
        .unwrap()
        .worker
        .send(WorkerEvent::UpdateSummary);

    panic::set_hook(Box::new(|info| {
        panic_hook(info);
    }));

    log::info!("chdig started");
    siv.run();

    return Ok(());
}
