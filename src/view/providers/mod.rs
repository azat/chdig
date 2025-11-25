mod backups;
mod client;
mod dictionaries;
mod errors;
mod logger_names;
mod merges;
mod mutations;
mod queries;
mod replicas;
mod replicated_fetches;
mod replication_queue;
mod s3queue;
mod server_logs;
mod tables;

pub use backups::BackupsViewProvider;
pub use client::ClientViewProvider;
pub use dictionaries::DictionariesViewProvider;
pub use errors::ErrorsViewProvider;
pub use logger_names::LoggerNamesViewProvider;
pub use merges::MergesViewProvider;
pub use mutations::MutationsViewProvider;
pub use queries::{LastQueryLogViewProvider, ProcessesViewProvider, SlowQueryLogViewProvider};
pub use replicas::ReplicasViewProvider;
pub use replicated_fetches::ReplicatedFetchesViewProvider;
pub use replication_queue::ReplicationQueueViewProvider;
pub use s3queue::S3QueueViewProvider;
pub use server_logs::ServerLogsViewProvider;
pub use tables::TablesViewProvider;

use crate::{
    interpreter::ContextArc,
    view::{self, QueryResultRow, TextLogView},
};
use chrono::{DateTime, Local};
use cursive::{
    Cursive,
    view::{Nameable, Resizable},
    views::{Dialog, DummyView, LinearLayout, NamedView, TextView},
};
use std::collections::HashMap;

pub fn query_result_show_logs_for_row(
    siv: &mut Cursive,
    columns: Vec<&'static str>,
    row: view::QueryResultRow,
    logger_names_patterns: &[&'static str],
    view_name: &'static str,
) {
    let row = row.0;

    let mut map = HashMap::<String, String>::new();
    columns.iter().zip(row.iter()).for_each(|(c, r)| {
        map.insert(c.to_string(), r.to_string());
    });

    let context = siv.user_data::<ContextArc>().unwrap().clone();
    let view_options = context.clone().lock().unwrap().options.view.clone();
    let logger_names = logger_names_patterns
        .iter()
        .map(|p| strfmt::strfmt(p, &map).unwrap())
        .collect::<Vec<_>>();

    siv.add_layer(Dialog::around(
        LinearLayout::vertical()
            .child(TextView::new("Logs:").center())
            .child(DummyView.fixed_height(1))
            .child(NamedView::new(
                view_name,
                TextLogView::new(
                    view_name,
                    context,
                    crate::interpreter::TextLogArguments {
                        query_ids: None,
                        logger_names: Some(logger_names),
                        message_filter: None,
                        max_level: None,
                        start: DateTime::<Local>::from(view_options.start),
                        end: view_options.end,
                    },
                ),
            )),
    ));
    siv.focus_name(view_name).unwrap();
}

pub struct RenderFromClickHouseQueryArguments<F> {
    pub context: ContextArc,
    pub table: &'static str,
    pub join: Option<String>,
    pub filter: Option<&'static str>,
    pub sort_by: &'static str,
    pub columns: Vec<&'static str>,
    pub columns_to_compare: Vec<&'static str>,
    pub on_submit: Option<F>,
    pub settings: HashMap<&'static str, &'static str>,
}

pub fn render_from_clickhouse_query<F>(
    siv: &mut Cursive,
    mut params: RenderFromClickHouseQueryArguments<F>,
) where
    F: Fn(&mut Cursive, Vec<&'static str>, view::QueryResultRow) + Send + Sync + 'static,
{
    use crate::view::Navigation;

    if siv.has_view(params.table) {
        return;
    }

    let cluster = params
        .context
        .lock()
        .unwrap()
        .options
        .clickhouse
        .cluster
        .is_some();
    if cluster {
        params.columns.insert(0, "hostName() host");
        // Add "host" to the beginning of columns to compare
        params.columns_to_compare.insert(0, "host");
    }

    let dbtable = params
        .context
        .lock()
        .unwrap()
        .clickhouse
        .get_table_name("system", params.table);
    let settings_str = if params.settings.is_empty() {
        "".to_string()
    } else {
        format!(
            " SETTINGS {}",
            params
                .settings
                .iter()
                .map(|kv| format!("{}='{}'", kv.0, kv.1.replace('\'', "\\\'")))
                .collect::<Vec<String>>()
                .join(",")
        )
        .to_string()
    };
    let query = format!(
        "select {} from {} as {} {}{}{}",
        params.columns.join(", "),
        dbtable,
        params.table,
        params.join.unwrap_or_default(),
        params
            .filter
            .map(|x| format!(" WHERE {}", x))
            .unwrap_or_default(),
        settings_str,
    );

    siv.drop_main_view();

    let mut view = view::SQLQueryView::new(
        params.context.clone(),
        params.table,
        params.sort_by,
        params.columns.clone(),
        params.columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot get {}", params.table));
    if let Some(on_submit) = params.on_submit {
        view.get_inner_mut().set_on_submit(on_submit);
    }
    let view = view.with_name(params.table).full_screen();

    siv.set_main_view(Dialog::around(view).title(params.table));
    siv.focus_name(params.table).unwrap();
}

pub fn query_result_show_row(siv: &mut Cursive, columns: Vec<&'static str>, row: QueryResultRow) {
    let row = row.0;
    let width = columns.iter().map(|c| c.len()).max().unwrap_or_default();
    let info = columns
        .iter()
        .zip(row.iter())
        .map(|(c, r)| (*c, r.to_string()))
        .map(|(c, r)| format!("{:<width$}: {}", c, r, width = width))
        .collect::<Vec<_>>()
        .join("\n");
    siv.add_layer(Dialog::info(info).title("Details"));
}
