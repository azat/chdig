use anyhow::Result;
use backtrace::Backtrace;
use flexi_logger::{LogSpecification, Logger};
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

        // FIXME: restore the terminal back from raw mode (see Backend::drop for termion)

        print!(
            "thread '<unnamed>' panicked at '{}', {}\n\r{}",
            msg, location, stacktrace
        );
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let options = options::parse();

    let backend = cursive::backends::termion::Backend::init()?;
    let buffered_backend = Box::new(cursive_buffered_backend::BufferedBackend::new(backend));
    let mut siv = cursive::CursiveRunner::new(cursive::Cursive::new(), buffered_backend);

    // Override with RUST_LOG
    //
    // NOTE: hyper also has trace_span() which will not be overwritten
    //
    // FIXME: "flexi_logger has to work with UTC rather than with local time, caused by IndeterminateOffset"
    //
    // FIXME: should be initialize before options, but options prints completion that should be
    // done before terminal switched to raw mode.
    let mut logger = Logger::try_with_env_or_str(
        "trace,cursive=info,clickhouse_rs=info,skim=info,tuikit=info,hyper=info",
    )?
    .log_to_writer(cursive_flexi_logger_view::cursive_flexi_logger(&siv))
    .format(flexi_logger::colored_with_thread)
    .start()?;

    // FIXME: should be initialized before cursive, otherwise on error it clears the terminal, and
    // writes without proper echo (see panic_hook()).
    let context: ContextArc = Context::new(options, siv.cb_sink().clone()).await?;

    siv.chdig(context.clone());

    panic::set_hook(Box::new(|info| {
        panic_hook(info);
    }));

    log::info!("chdig started");
    siv.run();

    // Suppress error from the cursive_flexi_logger_view - "cursive callback sink is closed!"
    // Note, cursive_flexi_logger_view does not implements shutdown() so it will not help.
    logger.set_new_spec(LogSpecification::parse("none").unwrap());

    return Ok(());
}
