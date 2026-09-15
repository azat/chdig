//! The `/` filter of the SQL-backed table views: whitespace separated terms
//! (quotes keep spaces) that must all match a row, evaluated over the loaded
//! rows. `column<op>value` compares the typed cell of a visible column
//! (numbers take the column's unit suffixes, strings `=`/`~` LIKE),
//! `ProfileEvents.<Name>` (`pe.<Name>`) a key of the row's `_profile_events`
//! map, any other term is a case-insensitive substring of the rendered row.

use crate::interpreter::queries_filter::{
    Field as QueriesField, Op, parse_quantity, split_token, token_under_cursor, tokenize,
};
use crate::interpreter::{ProfileEventUnit, profile_event_unit};
use crate::tui::views::sql_query_view::{Field, Unit, field_to_f64};

pub const PROFILE_EVENTS_COLUMN: &str = "_profile_events";

enum Term {
    /// Lowercase substring of any rendered cell
    Text(String),
    Cell {
        column: usize,
        op: Op,
        value: String,
        /// `value` as a number (None: never matches a numeric cell)
        number: Option<f64>,
    },
    /// `pe.<Name>`; without the map column the term never matches
    MapValue {
        column: Option<usize>,
        key: String,
        op: Op,
        number: Option<f64>,
    },
}

#[derive(Default)]
pub struct RowFilter {
    terms: Vec<Term>,
}

/// The number suffixes a column's value unit takes.
fn quantity_unit(unit: Option<Unit>) -> ProfileEventUnit {
    match unit {
        Some(Unit::Bytes) => ProfileEventUnit::Bytes,
        Some(Unit::Microseconds) => ProfileEventUnit::Time { per_second: 1e6 },
        Some(Unit::Milliseconds) => ProfileEventUnit::Time { per_second: 1e3 },
        Some(Unit::Seconds) => ProfileEventUnit::Time { per_second: 1. },
        Some(Unit::Count) | None => ProfileEventUnit::Count,
    }
}

fn is_numeric(field: &Field) -> bool {
    !matches!(
        field,
        Field::String(_) | Field::DateTime(_) | Field::UInt64Map(_)
    )
}

impl RowFilter {
    /// `columns` are the rendered column names (the hidden `_` ones are not
    /// addressable), `unit` the value unit of a column (its number suffixes).
    pub fn parse(text: &str, columns: &[&str], unit: impl Fn(&str) -> Option<Unit>) -> Self {
        let map_column = columns.iter().position(|c| *c == PROFILE_EVENTS_COLUMN);
        let mut terms = Vec::new();
        for token in tokenize(text) {
            let text_term = || Term::Text(token.to_lowercase());
            let Some((name, op, value)) = split_token(&token) else {
                // An event name being typed (`pe.Sel`) is not free text
                if !QueriesField::has_profile_events_prefix(&token) {
                    terms.push(text_term());
                }
                continue;
            };
            if let Some(key) = QueriesField::profile_event_key(name) {
                // Incomplete while typing (`pe.X>`), do not filter everything out
                if value.is_empty() {
                    continue;
                }
                terms.push(Term::MapValue {
                    column: map_column,
                    key: key.to_string(),
                    op,
                    number: parse_quantity(value, profile_event_unit(key)),
                });
                continue;
            }
            let column = columns
                .iter()
                .position(|c| !c.starts_with('_') && c.eq_ignore_ascii_case(name));
            let Some(column) = column else {
                terms.push(text_term());
                continue;
            };
            if value.is_empty() {
                continue;
            }
            terms.push(Term::Cell {
                column,
                op,
                value: value.to_string(),
                number: parse_quantity(value, quantity_unit(unit(columns[column]))),
            });
        }
        Self { terms }
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    pub fn matches(&self, fields: &[Field]) -> bool {
        self.terms.iter().all(|term| match term {
            Term::Text(text) => fields
                .iter()
                .any(|field| field.to_string().to_lowercase().contains(text)),
            Term::Cell {
                column,
                op,
                value,
                number,
            } => {
                let Some(field) = fields.get(*column) else {
                    return false;
                };
                if is_numeric(field) {
                    number.is_some_and(|expected| op.matches_number(field_to_f64(field), expected))
                } else {
                    op.matches_str(&field.to_string(), value)
                }
            }
            Term::MapValue {
                column,
                key,
                op,
                number,
            } => {
                let Some(Field::UInt64Map(map)) = column.and_then(|column| fields.get(column))
                else {
                    return false;
                };
                let actual = map.get(key).copied().unwrap_or(0) as f64;
                number.is_some_and(|expected| op.matches_number(actual, expected))
            }
        })
    }
}

/// Completion of the token under the cursor: the visible column names (with
/// their first operator, `>` for numeric columns) and `pe.<Name>` from
/// `map_keys` (empty when the view has no `_profile_events`).
pub fn suggest(
    text: &str,
    cursor: usize,
    columns: &[(String, bool)],
    map_keys: &[String],
) -> Vec<String> {
    let token = token_under_cursor(text, cursor);
    if split_token(token).is_some() {
        return Vec::new();
    }
    let token_lower = token.to_ascii_lowercase();
    if let Some(prefix) = QueriesField::PROFILE_EVENT_PREFIXES
        .iter()
        .find(|prefix| token_lower.starts_with(*prefix))
    {
        let typed_lower = &token_lower[prefix.len()..];
        return map_keys
            .iter()
            .filter(|key| key.to_ascii_lowercase().starts_with(typed_lower))
            .map(|key| format!("{}{}>", &token[..prefix.len()], key))
            .collect();
    }
    let mut candidates: Vec<String> = columns
        .iter()
        .filter(|(column, _)| column.to_ascii_lowercase().starts_with(&token_lower))
        .map(|(column, numeric)| format!("{}{}", column, if *numeric { ">" } else { "=" }))
        .collect();
    if !map_keys.is_empty() {
        candidates.extend(
            ["pe.", "ProfileEvents."]
                .into_iter()
                .filter(|prefix| prefix.to_ascii_lowercase().starts_with(&token_lower))
                .map(str::to_string),
        );
    }
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    const COLUMNS: &[&str] = &[
        "event_type",
        "part_name",
        "rows",
        "size",
        PROFILE_EVENTS_COLUMN,
    ];

    fn unit(column: &str) -> Option<Unit> {
        (column == "size").then_some(Unit::Bytes)
    }

    fn row(event_type: &str, part: &str, rows: u64, size: f64, selected_rows: u64) -> Vec<Field> {
        let mut events = BTreeMap::new();
        events.insert("SelectedRows".to_string(), selected_rows);
        vec![
            Field::String(event_type.to_string()),
            Field::String(part.to_string()),
            Field::UInt64(rows),
            Field::Quantity(size, Unit::Bytes),
            Field::UInt64Map(Arc::new(events)),
        ]
    }

    fn matches(text: &str, fields: &[Field]) -> bool {
        RowFilter::parse(text, COLUMNS, unit).matches(fields)
    }

    #[test]
    fn test_predicates() {
        let merge = row(
            "MergeParts",
            "all_1_10_2",
            5_000_000,
            3. * 1024f64.powi(3),
            7000,
        );
        let new_part = row("NewPart", "all_11_11_0", 100, 2048., 0);
        assert!(matches("rows>1M", &merge));
        assert!(!matches("rows>1M", &new_part));
        assert!(matches("size>=2G size<4G", &merge));
        assert!(matches("size=2k", &new_part));
        assert!(matches("event_type=NewPart", &new_part));
        assert!(matches("EVENT_TYPE~Merge", &merge));
        assert!(matches("part_name!~all_1_10%", &new_part));
        // Map values, also without the column shown
        assert!(matches("pe.SelectedRows>5k", &merge));
        assert!(matches("ProfileEvents.SelectedRows=0", &new_part));
        assert!(!matches("pe.NoSuchEvent>0", &merge));
        assert!(!RowFilter::parse("pe.SelectedRows>1", &["rows"], unit).matches(&merge[2..3]));
        // Free text (substring of the rendered row), unknown names included
        assert!(matches("all_1_10", &merge));
        assert!(matches("nosuch=1", &row("nosuch=1", "", 0, 0., 0)));
        assert!(!matches("nosuch=1", &merge));
        // A non-number never matches a numeric cell, incomplete terms are skipped
        assert!(!matches("rows>abc", &merge));
        assert!(matches("rows> pe.SelectedRows>", &merge));
        assert!(matches("pe.Sel ProfileEvents.", &merge));
        assert!(RowFilter::parse("  ", COLUMNS, unit).is_empty());
    }

    #[test]
    fn test_units() {
        let parse = |value, unit| parse_quantity(value, quantity_unit(unit));
        assert_eq!(parse("2G", Some(Unit::Bytes)), Some(2. * 1024f64.powi(3)));
        assert_eq!(parse("1.5s", Some(Unit::Milliseconds)), Some(1500.));
        assert_eq!(parse("2m", Some(Unit::Seconds)), Some(120.));
        assert_eq!(parse("2m", Some(Unit::Count)), Some(2e6));
        assert_eq!(parse("2m", None), Some(2e6));
        assert_eq!(parse("10x", None), None);
    }

    #[test]
    fn test_suggest() {
        let columns: Vec<(String, bool)> = [("event_type", false), ("rows", true)]
            .iter()
            .map(|(c, n)| (c.to_string(), *n))
            .collect();
        let keys = vec!["SelectedBytes".to_string(), "SelectedRows".to_string()];
        assert_eq!(suggest("ro", 2, &columns, &keys), vec!["rows>"]);
        assert_eq!(suggest("x ev", 4, &columns, &keys), vec!["event_type="]);
        assert_eq!(
            suggest("pe.selectedr", 12, &columns, &keys),
            vec!["pe.SelectedRows>"]
        );
        assert_eq!(
            suggest("PE.", 3, &columns, &keys),
            vec!["PE.SelectedBytes>", "PE.SelectedRows>"]
        );
        assert_eq!(
            suggest("p", 1, &columns, &keys),
            vec!["pe.", "ProfileEvents."]
        );
        assert_eq!(suggest("p", 1, &columns, &[]), Vec::<String>::new());
        assert_eq!(suggest("rows>1", 6, &columns, &keys), Vec::<String>::new());
    }
}
