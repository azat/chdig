use anyhow::Result;
use backtrace::Backtrace;
use chrono::TimeDelta;
use flexi_logger::{FileSpec, LogSpecification, Logger};
use std::ffi::OsString;
use std::panic::{self, PanicHookInfo};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{
    interpreter::{
        ClickHouse, Context, ContextArc, fetch_and_populate_perfetto_trace,
        fetch_server_perfetto_sources, options, perfetto::PerfettoTraceBuilder,
        stream_queries_into_perfetto_trace,
    },
    tui::{App, Dialog, Navigation, Resizable, Scrollable, TerminalGuard, TextView},
};

// NOTE: hyper also has trace_span() which will not be overwritten
//
// FIXME: should be initialize before options, but options prints completion that should be
// done before terminal switched to raw mode.
const DEFAULT_RUST_LOG: &str = "trace,clickhouse_rs=info,hyper=info,rustls=info";

fn panic_hook(info: &PanicHookInfo<'_>) {
    let location = info.location().unwrap();

    let msg = if let Some(s) = info.payload().downcast_ref::<&'static str>() {
        *s
    } else if let Some(s) = info.payload().downcast_ref::<String>() {
        &s[..]
    } else {
        "Box<Any>"
    };

    // NOTE: we need to add \r since the terminal is in raw mode.
    // (another option is to restore the terminal state with termios)
    let stacktrace: String = format!("{:?}", Backtrace::new()).replace('\n', "\n\r");

    print!(
        "\n\rthread '<unnamed>' panicked at '{}', {}\n\r{}",
        msg, location, stacktrace
    );
}

fn derive_output_path(
    user_path: Option<&Path>,
    is_server_scope: bool,
    query_id: Option<&str>,
) -> PathBuf {
    if let Some(p) = user_path {
        return p.to_path_buf();
    }
    if is_server_scope {
        PathBuf::from("server_perfetto_trace.pftrace")
    } else {
        // query_id presence is guaranteed by the !is_server_scope branch in the caller
        PathBuf::from(format!("{}.pftrace", query_id.unwrap()))
    }
}

async fn run_cli_perfetto_export(
    options: &options::ChDigOptions,
    clickhouse: &Arc<ClickHouse>,
) -> Result<()> {
    let cmd = options
        .perfetto_command()
        .expect("run_cli_perfetto_export requires the perfetto export subcommand");

    let perfetto_options = cmd.apply(options.perfetto.clone());

    let is_server_scope = options.view.query_id.is_none();
    let view_start = options.view.start.clone().into();
    let view_end = options.view.end.clone().into();
    let mut scope = match &options.view.query_id {
        Some(query_id) => {
            clickhouse
                .get_perfetto_query_scope(query_id, view_start, view_end)
                .await?
        }
        None => crate::interpreter::clickhouse::PerfettoQueryScope {
            start: view_start,
            end: view_end,
            query_ids: None,
        },
    };
    // Match TUI behavior: include events that arrived in the same second as the query end.
    scope.end += TimeDelta::seconds(1);

    let output = derive_output_path(
        options.view.output.as_deref(),
        is_server_scope,
        options.view.query_id.as_deref(),
    );

    let mut builder = PerfettoTraceBuilder::new(
        output.clone(),
        perfetto_options.per_server,
        perfetto_options.text_log_android,
        perfetto_options.stack_traces_by_thread,
        perfetto_options.compress,
    )?;
    stream_queries_into_perfetto_trace(
        clickhouse,
        &mut builder,
        &scope.query_ids,
        scope.start,
        scope.end,
    )
    .await;
    fetch_and_populate_perfetto_trace(
        clickhouse,
        &mut builder,
        &perfetto_options,
        scope.query_ids.as_deref(),
        scope.start,
        scope.end,
    )
    .await;

    if is_server_scope {
        fetch_server_perfetto_sources(
            clickhouse,
            &mut builder,
            &perfetto_options,
            scope.start,
            scope.end,
        )
        .await;
    }

    builder.build()?;
    println!("Perfetto trace exported to {}", output.display());
    Ok(())
}

pub async fn chdig_main_async<I, T>(itr: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let options = options::parse_from(itr)?;

    let mut logger_handle = None;
    // We start logging to file earlier for better introspection.
    if let Some(log) = &options.service.log {
        logger_handle = Some(
            Logger::try_with_env_or_str(DEFAULT_RUST_LOG)?
                .log_to_file(FileSpec::try_from(log)?)
                .format(flexi_logger::with_thread)
                .start()?,
        );
    }

    // Initialize it before any backends (otherwise backend will prepare terminal for TUI app, and
    // panic hook will clear the screen).
    let clickhouse = Arc::new(ClickHouse::new(options.clickhouse.clone()).await?);

    if options.perfetto_command().is_some() {
        run_cli_perfetto_export(&options, &clickhouse).await?;
        return Ok(());
    }

    let server_warnings = match clickhouse.get_warnings().await {
        Ok(w) => w,
        Err(e) => {
            log::warn!("Failed to fetch system.warnings: {}", e);
            Vec::new()
        }
    };

    panic::set_hook(Box::new(|info| {
        panic_hook(info);
    }));

    return chdig_tui_async(options, clickhouse, server_warnings, logger_handle).await;
}

/// Builds the fully wired App without touching the terminal (integration
/// tests drive it headlessly against a ratatui TestBackend).
pub async fn chdig_build_app(
    options: options::ChDigOptions,
    clickhouse: Arc<ClickHouse>,
    server_warnings: Vec<String>,
    logger_handle: &mut Option<flexi_logger::LoggerHandle>,
) -> Result<(App, ContextArc)> {
    let mut app = App::new();

    if options.service.log.is_none() && logger_handle.is_none() {
        *logger_handle = Some(
            Logger::try_with_env_or_str(DEFAULT_RUST_LOG)?
                .log_to_writer(crate::tui::logger::log_writer())
                .format(flexi_logger::with_thread)
                .start()?,
        );
    }

    let context: ContextArc = Context::new(options, clickhouse, app.cb_sink().clone()).await?;

    app.chdig(context.clone());

    if !server_warnings.is_empty() {
        let text = server_warnings.join("\n");
        app.add_layer(
            Dialog::around(TextView::new(text).scrollable())
                .title("Server warnings")
                .button("OK", |app| {
                    app.pop_layer();
                })
                .max_width(80),
        );
    }

    Ok((app, context))
}

pub async fn chdig_tui_async(
    options: options::ChDigOptions,
    clickhouse: Arc<ClickHouse>,
    server_warnings: Vec<String>,
    mut logger_handle: Option<flexi_logger::LoggerHandle>,
) -> Result<()> {
    let (mut app, _context) =
        chdig_build_app(options, clickhouse, server_warnings, &mut logger_handle).await?;

    let _terminal_guard = TerminalGuard::enter()?;
    let mut terminal =
        ratatui::Terminal::new(ratatui::backend::CrosstermBackend::new(std::io::stdout()))?;

    log::info!("chdig started");
    app.run(&mut terminal)?;

    if let Some(logger_handle) = logger_handle {
        // The UI is gone: stop feeding the (now invisible) debug console.
        logger_handle.set_new_spec(LogSpecification::parse("none")?);
    }

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
#[unsafe(no_mangle)]
pub extern "C" fn chdig_main(argc: c_int, argv: *const *const c_char) -> c_int {
    #[cfg(feature = "tokio-console")]
    console_subscriber::init();

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
