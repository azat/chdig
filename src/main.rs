use anyhow::Result;
use backtrace::Backtrace;
use flexi_logger::{LogSpecification, Logger};
use ncurses;
use std::panic::{self, PanicInfo};

mod interpreter;
mod view;

use crate::{
    interpreter::{options, Context, ContextArc},
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
    let mut logger =
        Logger::try_with_env_or_str("trace,cursive=info,clickhouse_rs=info,skim=info,tuikit=info")?
            .log_to_writer(cursive_flexi_logger_view::cursive_flexi_logger(&siv))
            .format(flexi_logger::colored_with_thread)
            .start()?;

    let options = options::parse();

    let context: ContextArc = Context::new(options, siv.cb_sink().clone()).await?;

    siv.chdig(context.clone());

    panic::set_hook(Box::new(|info| {
        panic_hook(info);
    }));

    if !context.lock().unwrap().options.view.mouse {
        siv.cb_sink()
            .send(Box::new(move |_: &mut cursive::Cursive| {
                ncurses::mousemask(0, None);
            }))
            .unwrap();
    }

    log::info!("chdig started");
    siv.run();

    // Suppress error from the cursive_flexi_logger_view - "cursive callback sink is closed!"
    // Note, cursive_flexi_logger_view does not implements shutdown() so it will not help.
    logger.set_new_spec(LogSpecification::parse("none").unwrap());

    return Ok(());
}
