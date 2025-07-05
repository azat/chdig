use anyhow::{anyhow, Result};
use backtrace::Backtrace;
use flexi_logger::{LogSpecification, Logger};
use std::ffi::OsString;
use std::panic::{self, PanicHookInfo};
use std::sync::Arc;

use crate::{
    interpreter::{options, ClickHouse, Context, ContextArc},
    view::Navigation,
};

fn panic_hook(info: &PanicHookInfo<'_>) {
    let location = info.location().unwrap();

    let msg = match info.payload().downcast_ref::<&'static str>() {
        Some(s) => *s,
        None => match info.payload().downcast_ref::<String>() {
            Some(s) => &s[..],
            None => "Box<Any>",
        },
    };

    // NOTE: we need to add \r since the terminal is in raw mode.
    // (another option is to restore the terminal state with termios)
    let stacktrace: String = format!("{:?}", Backtrace::new()).replace('\n', "\n\r");

    print!(
        "\n\rthread '<unnamed>' panicked at '{}', {}\n\r{}",
        msg, location, stacktrace
    );
}

pub async fn chdig_main_async<I, T>(itr: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let options = options::parse_from(itr)?;

    // Initialize it before any backends (otherwise backend will prepare terminal for TUI app, and
    // panic hook will clear the screen).
    let clickhouse = Arc::new(ClickHouse::new(options.clickhouse.clone()).await?);

    panic::set_hook(Box::new(|info| {
        panic_hook(info);
    }));

    let backend = cursive::backends::try_default().map_err(|e| anyhow!(e.to_string()))?;
    let mut siv = cursive::CursiveRunner::new(cursive::Cursive::new(), backend);

    // Override with RUST_LOG
    //
    // NOTE: hyper also has trace_span() which will not be overwritten
    //
    // FIXME: should be initialize before options, but options prints completion that should be
    // done before terminal switched to raw mode.
    let logger = Logger::try_with_env_or_str(
        "trace,cursive=info,clickhouse_rs=info,skim=info,tuikit=info,hyper=info,rustls=info",
    )?
    .log_to_writer(cursive_flexi_logger_view::cursive_flexi_logger(&siv))
    .format(flexi_logger::colored_with_thread)
    .start()?;

    // FIXME: should be initialized before cursive, otherwise on error it clears the terminal.
    let context: ContextArc = Context::new(options, clickhouse, siv.cb_sink().clone()).await?;

    siv.chdig(context.clone());

    log::info!("chdig started");
    siv.run();

    // Suppress error from the cursive_flexi_logger_view - "cursive callback sink is closed!"
    // Note, cursive_flexi_logger_view does not implements shutdown() so it will not help.
    logger.set_new_spec(LogSpecification::parse("none")?);

    return Ok(());
}

fn collect_args(argc: c_int, argv: *const *const c_char) -> Vec<OsString> {
    use std::ffi::CStr;
    unsafe {
        std::slice::from_raw_parts(argv, argc as usize)
            .iter()
            .map(|&ptr| {
                let c_str = CStr::from_ptr(ptr);
                let string = c_str.to_string_lossy().into_owned();
                OsString::from(string)
            })
            .collect()
    }
}

use std::os::raw::{c_char, c_int};
#[no_mangle]
pub extern "C" fn chdig_main(argc: c_int, argv: *const *const c_char) -> c_int {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(chdig_main_async(collect_args(argc, argv)))
        .unwrap_or_else(|e| {
            eprintln!("{}", e);
            std::process::exit(1);
        });
    return 0;
}
