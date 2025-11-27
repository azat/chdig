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

fn is_valid_identifier_begin(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_word_char_ascii(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn is_valid_identifier(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    let mut chars = s.chars();
    if !is_valid_identifier_begin(chars.next().unwrap()) {
        return false;
    }

    if !chars.all(is_word_char_ascii) {
        return false;
    }

    // NULL is not a valid identifier in SQL, any case
    if s.eq_ignore_ascii_case("null") {
        return false;
    }

    true
}

// backQuoteIfNeed() from ClickHouse
fn backquote_if_needed(s: &str) -> String {
    if is_valid_identifier(s)
        && !s.eq_ignore_ascii_case("distinct")
        && !s.eq_ignore_ascii_case("all")
        && !s.eq_ignore_ascii_case("table")
    {
        s.to_string()
    } else {
        format!("`{}`", s.replace('`', "\\`"))
    }
}

fn escape_for_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('_', "\\_")
        .replace('%', "\\%")
}

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
        let value = r.to_string();
        let quoted = backquote_if_needed(&value);
        let escaped_value = escape_for_like(&quoted);
        map.insert(c.to_string(), escaped_value);

        // Also provide unquoted version with "_raw" suffix for literal values
        let escaped_literal = escape_for_like(&value);
        map.insert(format!("{}_raw", c), escaped_literal);
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

pub trait ClickHouseSettingValue {
    fn format_for_query(&self) -> String;
}

impl ClickHouseSettingValue for &str {
    fn format_for_query(&self) -> String {
        format!("'{}'", self.replace('\'', "\\'"))
    }
}

impl ClickHouseSettingValue for String {
    fn format_for_query(&self) -> String {
        format!("'{}'", self.replace('\'', "\\'"))
    }
}

impl ClickHouseSettingValue for i32 {
    fn format_for_query(&self) -> String {
        self.to_string()
    }
}

impl ClickHouseSettingValue for i64 {
    fn format_for_query(&self) -> String {
        self.to_string()
    }
}

impl ClickHouseSettingValue for u32 {
    fn format_for_query(&self) -> String {
        self.to_string()
    }
}

impl ClickHouseSettingValue for u64 {
    fn format_for_query(&self) -> String {
        self.to_string()
    }
}

pub struct RenderFromClickHouseQueryArguments<F, T> {
    pub context: ContextArc,
    pub table: &'static str,
    pub join: Option<String>,
    pub filter: Option<&'static str>,
    pub sort_by: &'static str,
    pub columns: Vec<&'static str>,
    pub columns_to_compare: Vec<&'static str>,
    pub on_submit: Option<F>,
    pub settings: HashMap<&'static str, T>,
}

pub fn render_from_clickhouse_query<F, T>(
    siv: &mut Cursive,
    mut params: RenderFromClickHouseQueryArguments<F, T>,
) where
    F: Fn(&mut Cursive, Vec<&'static str>, view::QueryResultRow) + Send + Sync + 'static,
    T: ClickHouseSettingValue,
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
                .map(|kv| format!("{}={}", kv.0, kv.1.format_for_query()))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backquote_if_needed_valid_identifiers() {
        // Valid simple identifiers should not be quoted
        assert_eq!(backquote_if_needed("my_table"), "my_table");
        assert_eq!(backquote_if_needed("database1"), "database1");
        assert_eq!(backquote_if_needed("_private"), "_private");
        assert_eq!(backquote_if_needed("Table123"), "Table123");
        assert_eq!(backquote_if_needed("column_name_1"), "column_name_1");
    }

    #[test]
    fn test_backquote_if_needed_reserved_keywords() {
        // Reserved keywords should be quoted (case insensitive)
        assert_eq!(backquote_if_needed("table"), "`table`");
        assert_eq!(backquote_if_needed("TABLE"), "`TABLE`");
        assert_eq!(backquote_if_needed("Table"), "`Table`");
        assert_eq!(backquote_if_needed("distinct"), "`distinct`");
        assert_eq!(backquote_if_needed("DISTINCT"), "`DISTINCT`");
        assert_eq!(backquote_if_needed("all"), "`all`");
        assert_eq!(backquote_if_needed("ALL"), "`ALL`");
        assert_eq!(backquote_if_needed("null"), "`null`");
        assert_eq!(backquote_if_needed("NULL"), "`NULL`");
    }

    #[test]
    fn test_backquote_if_needed_special_characters() {
        // Identifiers with special characters should be quoted
        assert_eq!(backquote_if_needed("my-table"), "`my-table`");
        assert_eq!(backquote_if_needed("table.name"), "`table.name`");
        assert_eq!(backquote_if_needed("table name"), "`table name`");
        assert_eq!(backquote_if_needed("table@host"), "`table@host`");
        assert_eq!(backquote_if_needed("123table"), "`123table`");
        assert_eq!(backquote_if_needed("my$table"), "`my$table`");
    }

    #[test]
    fn test_backquote_if_needed_backtick_escaping() {
        // Backticks in identifiers should be escaped and quoted
        assert_eq!(backquote_if_needed("my`table"), "`my\\`table`");
        assert_eq!(backquote_if_needed("`table`"), "`\\`table\\``");
        assert_eq!(backquote_if_needed("tab`le`name"), "`tab\\`le\\`name`");
    }
}
