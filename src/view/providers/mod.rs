pub mod asynchronous_inserts;
mod background_schedule_pool;
mod background_schedule_pool_log;
mod backups;
mod client;
mod dictionaries;
mod errors;
mod logger_names;
pub mod merges;
pub mod mutations;
mod object_storage_queue;
pub mod part_log;
mod queries;
mod replicas;
mod replicated_fetches;
mod replication_queue;
mod server_logs;
pub mod table_parts;
mod tables;

pub use asynchronous_inserts::AsynchronousInsertsViewProvider;
pub use background_schedule_pool::BackgroundSchedulePoolViewProvider;
pub use background_schedule_pool_log::BackgroundSchedulePoolLogViewProvider;
pub use backups::BackupsViewProvider;
pub use client::ClientViewProvider;
pub use dictionaries::DictionariesViewProvider;
pub use errors::ErrorsViewProvider;
pub use logger_names::LoggerNamesViewProvider;
pub use merges::MergesViewProvider;
pub use mutations::MutationsViewProvider;
pub use object_storage_queue::{AzureQueueViewProvider, S3QueueViewProvider};
pub use part_log::PartLogViewProvider;
pub use queries::{LastQueryLogViewProvider, ProcessesViewProvider, SlowQueryLogViewProvider};
pub use replicas::ReplicasViewProvider;
pub use replicated_fetches::ReplicatedFetchesViewProvider;
pub use replication_queue::ReplicationQueueViewProvider;
pub use server_logs::ServerLogsViewProvider;
pub use table_parts::TablePartsViewProvider;
pub use tables::TablesViewProvider;

use crate::{
    interpreter::ContextArc,
    view::{self, QueryResultRow, TextLogView},
};
use chrono::{DateTime, Local};
use cursive::{
    Cursive,
    theme::{BaseColor, Color, Effect, Style},
    utils::markup::StyledString,
    view::{Nameable, Resizable},
    views::{Dialog, DummyView, LinearLayout, NamedView, TextView},
};
use std::collections::HashMap;

/// Create a styled title with cyan bold text and decorative borders
pub fn styled_title(title: &str) -> StyledString {
    let mut styled = StyledString::new();
    styled.append_plain("─── ");
    styled.append_styled(
        title,
        Style::from(Color::Dark(BaseColor::Cyan)).combine(Effect::Bold),
    );
    styled.append_plain(" ───");
    styled
}

pub struct TableFilterParams {
    pub database: Option<String>,
    pub table: Option<String>,
    view_name_prefix: &'static str,
    display_name: &'static str,
    display_name_lower: &'static str,
    table_prefix: Option<&'static str>,
}

impl TableFilterParams {
    pub fn new(
        database: Option<String>,
        table: Option<String>,
        view_name_prefix: &'static str,
        display_name: &'static str,
    ) -> Self {
        Self {
            database,
            table,
            view_name_prefix,
            display_name,
            display_name_lower: Box::leak(display_name.to_lowercase().into_boxed_str()),
            table_prefix: None,
        }
    }

    pub fn with_table_prefix(mut self, prefix: &'static str) -> Self {
        self.table_prefix = Some(prefix);
        self
    }

    pub fn build_where_clauses(&self) -> Vec<String> {
        let mut clauses = vec![];
        let prefix = self
            .table_prefix
            .map(|p| format!("{}.", p))
            .unwrap_or_default();

        if let Some(ref database) = self.database {
            clauses.push(format!(
                "{}database = '{}'",
                prefix,
                database.replace('\'', "''")
            ));
        }
        if let Some(ref table) = self.table {
            clauses.push(format!("{}table = '{}'", prefix, table.replace('\'', "''")));
        }

        clauses
    }

    pub fn build_title(&self, for_dialog: bool) -> String {
        match (&self.database, &self.table) {
            (Some(db), Some(tbl)) => {
                if for_dialog {
                    format!("{} for: {}.{}", self.display_name, db, tbl)
                } else {
                    format!("{}: {}.{}", self.display_name, db, tbl)
                }
            }
            (Some(db), None) => {
                if for_dialog {
                    format!("{} for database: {}", self.display_name, db)
                } else {
                    format!("{}: {}", self.display_name, db)
                }
            }
            (None, Some(tbl)) => {
                if for_dialog {
                    format!("{} for table: {}", self.display_name_lower, tbl)
                } else {
                    format!("{}: table {}", self.display_name, tbl)
                }
            }
            (None, None) => self.display_name.to_string(),
        }
    }

    pub fn generate_view_name(&self) -> String {
        format!(
            "{}_{}_{}",
            self.view_name_prefix,
            self.database.as_deref().unwrap_or("any"),
            self.table.as_deref().unwrap_or("any"),
        )
    }
}

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
                        hostname: None,
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
    pub table: &'static [&'static str],
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

    let table_alias = params.table[0];

    if siv.has_view(table_alias) {
        return;
    }

    let (cluster, selected_host, clickhouse) = {
        let ctx = params.context.lock().unwrap();
        (
            ctx.options.clickhouse.cluster.is_some(),
            ctx.selected_host.clone(),
            ctx.clickhouse.clone(),
        )
    };

    // Only show hostname column when in cluster mode AND no host filter is active
    if cluster && selected_host.is_none() {
        params.columns.insert(0, "hostName() host");
        // Add "host" to the beginning of columns to compare
        params.columns_to_compare.insert(0, "host");
    }

    let dbtable = if params.table.len() == 1 {
        params
            .context
            .lock()
            .unwrap()
            .clickhouse
            .get_table_name("system", table_alias)
    } else {
        let pattern = params.table.join("|");
        format!("merge('system', '^({pattern})$')")
    };
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

    let host_filter = clickhouse.get_host_filter_clause(selected_host.as_ref());
    let where_clause = match (params.filter, host_filter.is_empty()) {
        (Some(filter), true) => format!(" WHERE {}", filter),
        (Some(filter), false) => format!(" WHERE {} {}", filter, host_filter),
        (None, false) => format!(" WHERE 1 {}", host_filter),
        (None, true) => String::new(),
    };

    let query = format!(
        "select {} from {} as {} {}{}{}",
        params.columns.join(", "),
        dbtable,
        table_alias,
        params.join.unwrap_or_default(),
        where_clause,
        settings_str,
    );

    siv.drop_main_view();

    let mut view = view::SQLQueryView::new(
        params.context.clone(),
        table_alias,
        params.sort_by,
        params.columns.clone(),
        params.columns_to_compare,
        query,
    )
    .unwrap_or_else(|_| panic!("Cannot get {}", table_alias));
    if let Some(on_submit) = params.on_submit {
        view.get_inner_mut().set_on_submit(on_submit);
    }
    let view = view.with_name(table_alias).full_screen();

    siv.set_main_view(
        cursive::views::LinearLayout::vertical()
            .child(cursive::views::TextView::new(styled_title(table_alias)).center())
            .child(view),
    );
    siv.focus_name(table_alias).unwrap();
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
