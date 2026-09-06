//! The queries views filter: free text and `field<op>value` predicates.
//!
//! One filter string is evaluated twice: instantly against the queries already
//! loaded (matches()) and as SQL for the next server refresh (to_sql()), so
//! that the list narrows while typing and fills up with rows that were beyond
//! the LIMIT once the server answers.
//!
//! Syntax: whitespace separated tokens, quotes ('...' or "...") keep spaces.
//! A token with an operator (`=`, `!=`, `~`, `!~`, `>`, `>=`, `<`, `<=`) after
//! a known field name is a predicate, anything else is free text matched as
//! LIKE against the usual columns (`%...%` unless the text has a `%`).
//! `~` is LIKE too, `=` an exact match. Numbers take units: elapsed
//! `500ms 10s 2m 1h`, mem `100M 2G` (binary), cpu `50%`.

use crate::interpreter::Query;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    /// Free text (no field), matched against several columns
    Text,
    User,
    InitialUser,
    Host,
    Database,
    QueryId,
    InitialQueryId,
    Hash,
    Query,
    LogComment,
    Exception,
    Elapsed,
    Memory,
    Cpu,
    Threads,
    Cancelled,
    Initial,
    Kind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    String,
    Number,
    Bool,
}

impl Field {
    /// Canonical name first, then aliases.
    const NAMES: &'static [(Field, &'static [&'static str])] = &[
        (Field::User, &["user", "u"]),
        (Field::InitialUser, &["initial_user", "iuser"]),
        (Field::Host, &["host", "hostname"]),
        (Field::Database, &["db", "database"]),
        (Field::QueryId, &["query_id", "qid", "id"]),
        (Field::InitialQueryId, &["initial_query_id", "iqid"]),
        (Field::Hash, &["hash", "qhash", "normalized_query_hash"]),
        (Field::Query, &["query", "q", "sql"]),
        (Field::LogComment, &["log_comment", "comment"]),
        (Field::Exception, &["exception", "error"]),
        (Field::Elapsed, &["elapsed", "duration", "time"]),
        (Field::Memory, &["mem", "memory"]),
        (Field::Cpu, &["cpu"]),
        (Field::Threads, &["thr", "threads"]),
        (Field::Cancelled, &["cancelled", "killed"]),
        (Field::Initial, &["initial", "is_initial"]),
        (Field::Kind, &["kind", "query_kind"]),
    ];

    pub fn parse(name: &str) -> Option<Field> {
        let name = name.to_ascii_lowercase();
        Self::NAMES
            .iter()
            .find(|(_, names)| names.contains(&name.as_str()))
            .map(|(field, _)| *field)
    }

    pub fn name(self) -> &'static str {
        Self::NAMES
            .iter()
            .find(|(field, _)| *field == self)
            .map(|(_, names)| names[0])
            .unwrap_or("")
    }

    /// Canonical names of all fields.
    pub fn all() -> impl Iterator<Item = Field> {
        Self::NAMES.iter().map(|(field, _)| *field)
    }

    pub fn kind(self) -> Kind {
        match self {
            Field::Elapsed | Field::Memory | Field::Cpu | Field::Threads => Kind::Number,
            Field::Cancelled | Field::Initial => Kind::Bool,
            _ => Kind::String,
        }
    }

    /// Example values shown as a hint when there is nothing to complete from.
    pub fn hint(self) -> &'static str {
        match self {
            Field::Elapsed => "10s, 500ms, 2m, 1h",
            Field::Memory => "100M, 2G",
            Field::Cpu => "50 (percent)",
            Field::Threads => "8",
            Field::Cancelled | Field::Initial => "1 or 0",
            Field::Query | Field::Exception | Field::LogComment => "text (LIKE with ~ or %)",
            _ => "",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Like,
    NotLike,
    Gt,
    Ge,
    Lt,
    Le,
}

impl Op {
    /// Longest first, so that `!=`/`>=` win over `=`/`>`.
    const ALL: &'static [(&'static str, Op)] = &[
        ("!=", Op::Ne),
        ("!~", Op::NotLike),
        (">=", Op::Ge),
        ("<=", Op::Le),
        ("=", Op::Eq),
        ("~", Op::Like),
        (">", Op::Gt),
        ("<", Op::Lt),
    ];

    fn sql(self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Ne => "!=",
            Op::Like => "LIKE",
            Op::NotLike => "NOT LIKE",
            Op::Gt => ">",
            Op::Ge => ">=",
            Op::Lt => "<",
            Op::Le => "<=",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Predicate {
    pub field: Field,
    pub op: Op,
    pub value: String,
    /// Parsed `value` for Number/Bool fields (None = not a number, never matches)
    number: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Filter {
    predicates: Vec<Predicate>,
}

/// Splits on whitespace, keeping quoted parts ('...' or "...") together
/// (quotes are removed).
pub fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut in_token = false;
    for c in text.chars() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => current.push(c),
            // A quote opens only at the start of a token or of a value
            // (`user='a b'`), an apostrophe inside a word is literal
            None if (c == '\'' || c == '"')
                && (current.is_empty() || current.ends_with(['=', '~', '>', '<'])) =>
            {
                quote = Some(c);
                in_token = true;
            }
            None if c.is_whitespace() => {
                if in_token {
                    tokens.push(std::mem::take(&mut current));
                    in_token = false;
                }
            }
            None => {
                current.push(c);
                in_token = true;
            }
        }
    }
    if in_token {
        tokens.push(current);
    }
    tokens
}

/// `(field, op, value)` of a predicate token, None for free text.
pub fn split_predicate(token: &str) -> Option<(Field, Op, &str)> {
    let op_pos = token.find(['=', '!', '~', '>', '<'])?;
    let field = Field::parse(&token[..op_pos])?;
    let rest = &token[op_pos..];
    let (op_str, op) = Op::ALL.iter().find(|(s, _)| rest.starts_with(s))?;
    Some((field, *op, &rest[op_str.len()..]))
}

fn parse_number(field: Field, value: &str) -> Option<f64> {
    let value = value.trim();
    if field.kind() == Kind::Bool {
        return match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "y" => Some(1.),
            "0" | "false" | "no" | "n" => Some(0.),
            _ => None,
        };
    }
    let split = value
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(value.len());
    let number: f64 = value[..split].parse().ok()?;
    let unit = value[split..].trim().to_ascii_lowercase();
    let multiplier = match field {
        Field::Elapsed => match unit.as_str() {
            "" | "s" | "sec" => 1.,
            "ms" => 1e-3,
            "us" => 1e-6,
            "m" | "min" => 60.,
            "h" => 3600.,
            "d" => 86400.,
            _ => return None,
        },
        Field::Memory => match unit.as_str() {
            "" | "b" => 1.,
            "k" | "kb" | "kib" => 1024.,
            "m" | "mb" | "mib" => 1024f64.powi(2),
            "g" | "gb" | "gib" => 1024f64.powi(3),
            "t" | "tb" | "tib" => 1024f64.powi(4),
            _ => return None,
        },
        Field::Cpu => match unit.as_str() {
            "" | "%" => 1.,
            _ => return None,
        },
        _ => match unit.as_str() {
            "" => 1.,
            "k" => 1e3,
            _ => return None,
        },
    };
    Some(number * multiplier)
}

/// SQL LIKE (`%` any, `_` one char) over chars.
pub fn like_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    // dp over the pattern with the classic two-pointer + backtrack to the last %
    let (mut pi, mut ti) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star = Some((pi, ti));
            pi += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

/// The LIKE pattern of a text value: as is with a `%`, `%text%` otherwise.
fn like_pattern(value: &str) -> String {
    if value.contains('%') {
        value.to_string()
    } else {
        format!("%{}%", value)
    }
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// Column expressions of the table the filter is translated for.
pub struct FilterColumns<'a> {
    pub host: &'a str,
    pub elapsed: &'a str,
    pub memory: &'a str,
    pub threads: &'a str,
    pub cpu: &'a str,
    pub hash: &'a str,
    pub log_comment: &'a str,
    pub exception: &'a str,
    pub cancelled: &'a str,
    /// None = the table has no query_kind (the predicate is skipped)
    pub kind: Option<&'a str>,
    /// Columns matched by the free text
    pub text: &'a [&'a str],
}

impl Predicate {
    fn string_matches(&self, actual: &str) -> bool {
        match self.op {
            Op::Eq => actual == self.value,
            Op::Ne => actual != self.value,
            Op::Like => like_match(&like_pattern(&self.value), actual),
            Op::NotLike => !like_match(&like_pattern(&self.value), actual),
            Op::Gt => actual > self.value.as_str(),
            Op::Ge => actual >= self.value.as_str(),
            Op::Lt => actual < self.value.as_str(),
            Op::Le => actual <= self.value.as_str(),
        }
    }

    fn number_matches(&self, actual: f64) -> bool {
        let Some(expected) = self.number else {
            return false;
        };
        match self.op {
            Op::Eq | Op::Like => actual == expected,
            Op::Ne | Op::NotLike => actual != expected,
            Op::Gt => actual > expected,
            Op::Ge => actual >= expected,
            Op::Lt => actual < expected,
            Op::Le => actual <= expected,
        }
    }

    pub fn matches(&self, query: &Query) -> bool {
        let log_comment = || {
            query
                .settings
                .get("log_comment")
                .map(String::as_str)
                .unwrap_or("")
        };
        match self.field {
            Field::Text => {
                let pattern = like_pattern(&self.value);
                let hash = query.normalized_query_hash.to_string();
                [
                    query.user.as_str(),
                    query.initial_user.as_str(),
                    query.query_id.as_str(),
                    query.original_query.as_str(),
                    query.current_database.as_str(),
                    query.host_name.as_str(),
                    log_comment(),
                    hash.as_str(),
                ]
                .iter()
                .any(|s| like_match(&pattern, s))
            }
            Field::User => self.string_matches(&query.user),
            Field::InitialUser => self.string_matches(&query.initial_user),
            Field::Host => self.string_matches(&query.host_name),
            Field::Database => self.string_matches(&query.current_database),
            Field::QueryId => self.string_matches(&query.query_id),
            Field::InitialQueryId => self.string_matches(&query.initial_query_id),
            Field::Hash => self.string_matches(&query.normalized_query_hash.to_string()),
            Field::Query => self.string_matches(&query.original_query),
            Field::LogComment => self.string_matches(log_comment()),
            Field::Exception => self.string_matches(&query.exception),
            Field::Elapsed => self.number_matches(query.elapsed),
            Field::Memory => self.number_matches(query.memory as f64),
            Field::Cpu => self.number_matches(query.cpu()),
            Field::Threads => self.number_matches(query.threads as f64),
            Field::Cancelled => self.number_matches(query.is_cancelled as u8 as f64),
            Field::Initial => self.number_matches(query.is_initial_query as u8 as f64),
            // Not available in the rows, the server filters by it
            Field::Kind => true,
        }
    }

    /// The SQL condition, None when the table cannot evaluate it.
    fn to_sql(&self, columns: &FilterColumns<'_>) -> Option<String> {
        let string_condition = |column: &str| -> String {
            let value = match self.op {
                Op::Like | Op::NotLike => like_pattern(&self.value),
                _ => self.value.clone(),
            };
            format!("{} {} {}", column, self.op.sql(), sql_quote(&value))
        };
        let number_condition = |column: &str| -> Option<String> {
            let number = self.number?;
            let op = match self.op {
                Op::Like => "=",
                Op::NotLike => "!=",
                op => op.sql(),
            };
            Some(format!("{} {} {}", column, op, number))
        };
        let condition = match self.field {
            Field::Text => {
                let pattern = sql_quote(&like_pattern(&self.value));
                columns
                    .text
                    .iter()
                    .map(|c| format!("{} LIKE {}", c, pattern))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            }
            Field::User => string_condition("user"),
            Field::InitialUser => string_condition("initial_user"),
            Field::Host => string_condition(columns.host),
            Field::Database => string_condition("current_database"),
            Field::QueryId => string_condition("query_id"),
            Field::InitialQueryId => string_condition("initial_query_id"),
            Field::Hash => string_condition(columns.hash),
            Field::Query => string_condition("query"),
            Field::LogComment => string_condition(columns.log_comment),
            Field::Exception => string_condition(columns.exception),
            Field::Elapsed => number_condition(columns.elapsed)?,
            Field::Memory => number_condition(columns.memory)?,
            Field::Cpu => number_condition(columns.cpu)?,
            Field::Threads => number_condition(columns.threads)?,
            Field::Cancelled => number_condition(columns.cancelled)?,
            Field::Initial => number_condition("is_initial_query")?,
            Field::Kind => {
                let column = columns.kind?;
                let value = self.value.to_ascii_lowercase();
                match self.op {
                    Op::Ne | Op::NotLike => {
                        format!("lower({}) != {}", column, sql_quote(&value))
                    }
                    _ => format!("lower({}) = {}", column, sql_quote(&value)),
                }
            }
        };
        Some(format!("({})", condition))
    }
}

impl Filter {
    pub fn parse(text: &str) -> Filter {
        let predicates = tokenize(text)
            .iter()
            .filter_map(|token| {
                let (field, op, value) = match split_predicate(token) {
                    Some(p) => p,
                    None => (Field::Text, Op::Like, token.as_str()),
                };
                // Incomplete while typing (`user=`), do not filter everything out
                if value.is_empty() {
                    return None;
                }
                let number = match field.kind() {
                    Kind::String => None,
                    _ => parse_number(field, value),
                };
                Some(Predicate {
                    field,
                    op,
                    value: value.to_string(),
                    number,
                })
            })
            .collect();
        Filter { predicates }
    }

    pub fn is_empty(&self) -> bool {
        self.predicates.is_empty()
    }

    pub fn matches(&self, query: &Query) -> bool {
        self.predicates.iter().all(|p| p.matches(query))
    }

    /// ` AND (...) AND (...)` for the WHERE of `columns`' table (empty for an
    /// empty filter).
    pub fn to_sql(&self, columns: &FilterColumns<'_>) -> String {
        self.predicates
            .iter()
            .filter_map(|p| p.to_sql(columns))
            .map(|c| format!(" AND {}", c))
            .collect()
    }
}

/// Completion of the token under the cursor (see the prompt): the candidates
/// replace the whole token. `values(field)` yields the values seen so far for
/// a string field, most frequent first.
pub fn suggest(
    text: &str,
    cursor: usize,
    mut values: impl FnMut(Field) -> Vec<String>,
) -> Vec<String> {
    let cursor = cursor.min(text.len());
    let before = &text[..cursor];
    let token_start = before
        .rfind(char::is_whitespace)
        .map(|i| i + 1)
        .unwrap_or(0);
    let token = &before[token_start..];

    if let Some((field, op, value)) = split_predicate(token) {
        let prefix = format!("{}{}", &token[..token.len() - value.len()], "");
        let _ = op;
        return match field.kind() {
            Kind::String => {
                let value_lower = value.to_ascii_lowercase();
                values(field)
                    .into_iter()
                    .filter(|v| v.to_ascii_lowercase().starts_with(&value_lower))
                    .map(|v| {
                        if v.contains(char::is_whitespace) {
                            format!("{}'{}'", prefix, v)
                        } else {
                            format!("{}{}", prefix, v)
                        }
                    })
                    .collect()
            }
            _ => {
                if value.is_empty() {
                    vec![format!("{}<{}>", prefix, field.hint())]
                } else {
                    Vec::new()
                }
            }
        };
    }

    // A field name (with its first operator); free text has no completion
    let token_lower = token.to_ascii_lowercase();
    Field::all()
        .filter(|f| f.name().starts_with(&token_lower))
        .map(|f| {
            let op = if f.kind() == Kind::Number { ">" } else { "=" };
            format!("{}{}", f.name(), op)
        })
        .collect()
}

/// `text` with the token under `cursor` replaced by `completion`; returns the
/// new text and cursor.
pub fn complete(text: &str, cursor: usize, completion: &str) -> (String, usize) {
    let cursor = cursor.min(text.len());
    let before = &text[..cursor];
    let token_start = before
        .rfind(char::is_whitespace)
        .map(|i| i + 1)
        .unwrap_or(0);
    let mut result = String::new();
    result.push_str(&text[..token_start]);
    result.push_str(completion);
    let new_cursor = result.len();
    result.push_str(&text[cursor..]);
    (result, new_cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize() {
        assert_eq!(tokenize("a  b"), vec!["a", "b"]);
        assert_eq!(tokenize("user='a b' x"), vec!["user=a b", "x"]);
        assert_eq!(tokenize("q~\"select 1\""), vec!["q~select 1"]);
        assert_eq!(tokenize("it's q~it's"), vec!["it's", "q~it's"]);
        assert_eq!(tokenize(""), Vec::<String>::new());
    }

    #[test]
    fn test_split_predicate() {
        assert_eq!(
            split_predicate("user!=default"),
            Some((Field::User, Op::Ne, "default"))
        );
        assert_eq!(
            split_predicate("elapsed>=10s"),
            Some((Field::Elapsed, Op::Ge, "10s"))
        );
        assert_eq!(split_predicate("nosuch=1"), None);
        assert_eq!(split_predicate("plain"), None);
        assert_eq!(split_predicate("user="), Some((Field::User, Op::Eq, "")));
    }

    #[test]
    fn test_numbers() {
        assert_eq!(parse_number(Field::Elapsed, "500ms"), Some(0.5));
        assert_eq!(parse_number(Field::Elapsed, "2m"), Some(120.));
        assert_eq!(
            parse_number(Field::Memory, "2G"),
            Some(2. * 1024f64.powi(3))
        );
        assert_eq!(parse_number(Field::Cpu, "50%"), Some(50.));
        assert_eq!(parse_number(Field::Cancelled, "yes"), Some(1.));
        assert_eq!(parse_number(Field::Elapsed, "10x"), None);
    }

    #[test]
    fn test_like_match() {
        assert!(like_match("%sel%", "SELECT select"));
        assert!(like_match("a_c", "abc"));
        assert!(!like_match("a_c", "abbc"));
        assert!(like_match("%", ""));
        assert!(like_match("abc%", "abcdef"));
        assert!(!like_match("abc", "abcdef"));
    }

    fn columns() -> FilterColumns<'static> {
        FilterColumns {
            host: "hostName()",
            elapsed: "elapsed",
            memory: "memory_usage",
            threads: "length(thread_ids)",
            cpu: "cpu_",
            hash: "toString(normalizedQueryHash(query))",
            log_comment: "Settings['log_comment']",
            exception: "''",
            cancelled: "is_cancelled",
            kind: None,
            text: &["user", "query"],
        }
    }

    #[test]
    fn test_to_sql() {
        let filter = Filter::parse("user=default elapsed>10s q~insert kind=select foo it's");
        assert_eq!(
            filter.to_sql(&columns()),
            " AND (user = 'default') AND (elapsed > 10) AND (query LIKE '%insert%') \
             AND (user LIKE '%foo%' OR query LIKE '%foo%') \
             AND (user LIKE '%it\\'s%' OR query LIKE '%it\\'s%')"
        );
        // Incomplete predicate and empty filter
        assert_eq!(Filter::parse("user=").to_sql(&columns()), "");
        assert!(Filter::parse("  ").is_empty());
        // A raw LIKE pattern stays as is
        assert_eq!(
            Filter::parse("it-proc-%").to_sql(&columns()),
            " AND (user LIKE 'it-proc-%' OR query LIKE 'it-proc-%')"
        );
    }

    #[test]
    fn test_suggest() {
        let values = |field: Field| match field {
            Field::User => vec!["default".to_string(), "dev ops".to_string()],
            _ => Vec::new(),
        };
        assert_eq!(suggest("us", 2, values), vec!["user="]);
        assert_eq!(
            suggest("user=d", 6, values),
            vec!["user=default", "user='dev ops'"]
        );
        assert_eq!(
            suggest("user=D", 6, values),
            vec!["user=default", "user='dev ops'"]
        );
        assert_eq!(
            suggest("x elapsed>", 10, values),
            vec!["elapsed><10s, 500ms, 2m, 1h>"]
        );
        assert_eq!(suggest("plain", 5, values), Vec::<String>::new());
        assert_eq!(
            complete("x user=d", 8, "user=default"),
            ("x user=default".to_string(), 14)
        );
        assert_eq!(
            complete("us more", 2, "user="),
            ("user= more".to_string(), 5)
        );
    }
}
