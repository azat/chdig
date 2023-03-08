use anyhow::Result;
use backtrace::Backtrace;
use cursive::event::Key;
use cursive_flexi_logger_view::toggle_flexi_logger_debug_console;
use flexi_logger::Logger;
use ncurses;
use std::panic::{self, PanicInfo};

mod interpreter;
mod view;

use crate::{
    interpreter::{clickhouse::TraceType, options, Context, ContextArc, WorkerEvent},
    view::Navigation,
};

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

        ncurses::noraw();
        ncurses::clear();
        ncurses::refresh();
        print!(
            "thread '<unnamed>' panicked at '{}', {}\n\r{}",
            msg, location, stacktrace
        );
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

    let theme = view::utils::make_cursive_theme_from_therminal(&siv);
    siv.set_theme(theme);

    view::utils::add_menu(&mut siv);
    view::utils::add_fuzzy_shortcuts(&mut siv);

    // TODO: Bindings:
    // - C-J - show the end of the queries (like in top(1))
    siv.add_global_callback('q', view::utils::pop_ui);
    // TODO: add other variants of flamegraphs
    siv.add_global_callback('F', |siv: &mut cursive::Cursive| {
        siv.user_data::<ContextArc>()
            .unwrap()
            .lock()
            .unwrap()
            .worker
            .send(WorkerEvent::ShowServerFlameGraph(TraceType::CPU));
    });
    siv.add_global_callback(Key::Backspace, view::utils::pop_ui);
    // NOTE: Do not find to Esc, since this breaks other bindings (Home/End/...)`
    siv.add_global_callback(Key::F1, view::utils::show_help_dialog);
    siv.add_global_callback('~', toggle_flexi_logger_debug_console);
    siv.set_user_data(context.clone());
    // TODO: disable mouse support (sigh)

    siv.statusbar(format!(
        "Connected to {}.",
        context.lock().unwrap().server_version
    ));
    siv.show_clickhouse_processes(context.clone());

    panic::set_hook(Box::new(|info| {
        panic_hook(info);
    }));

    log::info!("chdig started");
    siv.run();

    return Ok(());
}
