use anyhow::{Result, anyhow};
use backtrace::Backtrace;
use chrono::{DateTime, Local};
use flexi_logger::{FileSpec, LogSpecification, Logger};
use std::ffi::OsString;
use std::panic::{self, PanicHookInfo};
use std::sync::Arc;

use cursive::view::Resizable;

use crate::{
    interpreter::{
        ClickHouse, Context, ContextArc, Query, fetch_and_populate_perfetto_trace,
        fetch_server_perfetto_sources, options, perfetto::PerfettoTraceBuilder,
    },
    view::Navigation,
};

// NOTE: hyper also has trace_span() which will not be overwritten
//
// FIXME: should be initialize before options, but options prints completion that should be
// done before terminal switched to raw mode.
const DEFAULT_RUST_LOG: &str = "trace,cursive=info,clickhouse_rs=info,hyper=info,rustls=info";

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

fn write_perfetto_trace(path: &str, data: Vec<u8>) -> Result<()> {
    std::fs::write(path, &data)?;
    println!("Perfetto trace exported to {}", path);
    Ok(())
}

async fn run_cli_perfetto_export(
    options: &options::ChDigOptions,
    clickhouse: &Arc<ClickHouse>,
) -> Result<bool> {
    
    let output = options
        .view
        .output
        .clone()
        .unwrap_or_else(|| "/tmp/chdig_perfetto.pftrace".to_string());
    
    let mut perfetto_options = options.perfetto.clone();

    perfetto_options.aggregated_zookeeper_log = true;
    perfetto_options.query_metric_log = true;
    perfetto_options.asynchronous_metric_log = true;

    if let Some(query_id) = options.view.query_id.as_deref() {
        let scope = clickhouse.get_perfetto_query_scope(query_id).await?;
        let start = scope.start;
        let end = scope.end;

        let end_time = end + chrono::TimeDelta::seconds(1);
        let query_block = clickhouse.get_queries_for_perfetto(start, end_time, &Some(scope.query_ids.clone())).await?;
        let mut queries = Vec::new();

        for i in 0..query_block.row_count() {
            match Query::from_clickhouse_block(&query_block, i, false) {
                Ok(q) => queries.push(q),
                Err(e) => log::warn!("Perfetto: failed to parse query row {}: {}", i, e),
            }
        }

        let mut builder = PerfettoTraceBuilder::new(
            perfetto_options.per_server,
            perfetto_options.text_log_android,
        );
        builder.add_queries(&queries);
        fetch_and_populate_perfetto_trace(
            clickhouse,
            &mut builder,
            &perfetto_options,
            Some(&scope.query_ids),
            scope.start,
            end_time,
        )
        .await;
        write_perfetto_trace(&output, builder.build())?;
        return Ok(true);
    }

    if options.view.server {
        let start: DateTime<Local> = options.view.start.clone().into();
        let end: DateTime<Local> = options.view.end.clone().into();

        let end_time = end + chrono::TimeDelta::seconds(1);
        let query_block = clickhouse.get_queries_for_perfetto(start, end_time, &None).await?;
        let mut queries = Vec::new();

        for i in 0..query_block.row_count() {
            match Query::from_clickhouse_block(&query_block, i, false) {
                Ok(q) => queries.push(q),
                Err(e) => log::warn!("Perfetto: failed to parse query row {}: {}", i, e),
            }
        }

        let mut builder = PerfettoTraceBuilder::new(
            perfetto_options.per_server,
            perfetto_options.text_log_android,
        );
        builder.add_queries(&queries);
        fetch_and_populate_perfetto_trace(
            clickhouse,
            &mut builder,
            &perfetto_options,
            None,
            start,
            end_time,
        )
        .await;
        fetch_server_perfetto_sources(clickhouse, &mut builder, &perfetto_options, start, end_time)
            .await;
        write_perfetto_trace(&output, builder.build())?;
        return Ok(true);
    }

    Ok(false)
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

    let backend = cursive::backends::try_default().map_err(|e| anyhow!(e.to_string()))?;
    let mut siv = cursive::CursiveRunner::new(cursive::Cursive::new(), backend);

    if options.service.log.is_none() {
        logger_handle = Some(
            Logger::try_with_env_or_str(DEFAULT_RUST_LOG)?
                .log_to_writer(cursive_flexi_logger_view::cursive_flexi_logger(&siv))
                .format(flexi_logger::colored_with_thread)
                .start()?,
        );
    }

    // FIXME: should be initialized before cursive, otherwise on error it clears the terminal.
    let context: ContextArc = Context::new(options, clickhouse, siv.cb_sink().clone()).await?;

    siv.chdig(context.clone());

    if !server_warnings.is_empty() {
        let text = server_warnings.join("\n");
        siv.add_layer(
            cursive::views::Dialog::around(cursive::views::ScrollView::new(
                cursive::views::TextView::new(text),
            ))
            .title("Server warnings")
            .button("OK", |s| {
                s.pop_layer();
            })
            .max_width(80),
        );
    }

    log::info!("chdig started");
    siv.run();

    if let Some(logger_handle) = logger_handle {
        // Suppress error from the cursive_flexi_logger_view - "cursive callback sink is closed!"
        // Note, cursive_flexi_logger_view does not implements shutdown() so it will not help.
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
