use crate::sql_lexer::{Lexer, Token, TokenType};

use super::style::{Color, Modifier, Style, StyledString};

/// Keywords recognized for highlighting. The lexer reports them as `BareWord`, so they are
/// disambiguated from identifiers here.
///
/// Ported from ClickHouse's `src/Client/SQLKeywordsForHighlighting.inc` (itself generated from
/// `programs/server/play.html`).
const KEYWORDS: &[&str] = &[
    "ADD",
    "AFTER",
    "ALL",
    "ALTER",
    "AND",
    "ANTI",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "ASCENDING",
    "ASOF",
    "AST",
    "ASYNC",
    "ATTACH",
    "BACKUP",
    "BEGIN",
    "BETWEEN",
    "BOTH",
    "BY",
    "CACHE",
    "CASCADE",
    "CASE",
    "CAST",
    "CHANGE",
    "CHANGED",
    "CHECK",
    "CLEAR",
    "CLUSTER",
    "CODEC",
    "COLLATE",
    "COLUMN",
    "COLUMNS",
    "COMMENT",
    "COMMIT",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "DATABASE",
    "DATABASES",
    "DAY",
    "DEDUPLICATE",
    "DEFAULT",
    "DELETE",
    "DESC",
    "DESCENDING",
    "DESCRIBE",
    "DETACH",
    "DICTIONARIES",
    "DICTIONARY",
    "DISK",
    "DISTINCT",
    "DISTRIBUTED",
    "DROP",
    "ELSE",
    "END",
    "ENGINE",
    "ESTIMATE",
    "EXCEPT",
    "EXCHANGE",
    "EXISTS",
    "EXPLAIN",
    "EXPRESSION",
    "EXTENDED",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FETCHES",
    "FILE",
    "FILESYSTEM",
    "FINAL",
    "FIRST",
    "FLUSH",
    "FOLLOWING",
    "FOR",
    "FOREIGN",
    "FORMAT",
    "FREEZE",
    "FROM",
    "FULL",
    "FUNCTION",
    "GLOBAL",
    "GRANT",
    "GROUP",
    "GROUPS",
    "HAVING",
    "HIERARCHICAL",
    "HOUR",
    "ID",
    "IDENTIFIED",
    "IF",
    "ILIKE",
    "IN",
    "INDEX",
    "INF",
    "INHERIT",
    "INJECTIVE",
    "INNER",
    "INSERT",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "INVISIBLE",
    "IS",
    "IS_OBJECT_ID",
    "JOIN",
    "KEY",
    "KEYED",
    "KILL",
    "LAST",
    "LATERAL",
    "LAYOUT",
    "LEADING",
    "LEFT",
    "LIFETIME",
    "LIKE",
    "LIMIT",
    "LIMITS",
    "LIVE",
    "LOCAL",
    "LOGS",
    "MATERIALIZE",
    "MATERIALIZED",
    "MAX",
    "MERGES",
    "MICROSECOND",
    "MILLISECOND",
    "MIN",
    "MINUTE",
    "MODIFY",
    "MONTH",
    "MOVE",
    "MUTATION",
    "NAN_SQL",
    "NEXT",
    "NO",
    "NONE",
    "NOT",
    "NULL",
    "NULLS",
    "OFFSET",
    "ON",
    "ONLY",
    "OPTIMIZE",
    "OPTION",
    "OR",
    "ORDER",
    "OUTER",
    "OUTFILE",
    "OVER",
    "PARTITION",
    "PASTE",
    "PERMANENTLY",
    "PLAN",
    "POPULATE",
    "PRECEDING",
    "PRECISION",
    "PREWHERE",
    "PRIMARY",
    "PROFILE",
    "PROJECTION",
    "QUARTER",
    "QUERY",
    "QUOTA",
    "RANDOMIZED",
    "RANGE",
    "RECURSIVE",
    "REFRESH",
    "REGEXP",
    "RELOAD",
    "REMOTE",
    "RENAME",
    "REPLACE",
    "REPLICA",
    "REPLICAS",
    "RESET",
    "RESTORE",
    "RESTRICT",
    "RESTRICTIVE",
    "RETURNS",
    "REVOKE",
    "RIGHT",
    "ROLE",
    "ROLLBACK",
    "ROLLUP",
    "ROW",
    "ROWS",
    "SAMPLE",
    "SECOND",
    "SELECT",
    "SEMI",
    "SENDS",
    "SET",
    "SETS",
    "SETTINGS",
    "SHARD",
    "SHOW",
    "SIGNED",
    "SOURCE",
    "SQL_SECURITY",
    "START",
    "STEP",
    "STORAGE",
    "STRICT",
    "SUBPARTITION",
    "SUBSTRING",
    "SUSPEND",
    "SYNC",
    "SYNTAX",
    "SYSTEM",
    "TABLE",
    "TABLES",
    "TEMPORARY",
    "TEST",
    "THEN",
    "TIES",
    "TIMESTAMP",
    "TO",
    "TOP",
    "TOTALS",
    "TRACKING",
    "TRAILING",
    "TRANSACTION",
    "TRIGGER",
    "TRIM",
    "TRUE",
    "TRUNCATE",
    "TYPE",
    "UNBOUNDED",
    "UNFREEZE",
    "UNION",
    "UNIQUE",
    "UNSIGNED",
    "UPDATE",
    "USE",
    "USING",
    "UUID",
    "VALUES",
    "VARYING",
    "VIEW",
    "VIRTUAL",
    "VISIBLE",
    "WEEK",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
    "WORK",
    "WRITABLE",
    "XOR",
    "YEAR",
    "ZKPATH",
];

/// replxx's `color::rgb666()`: an index into the 6x6x6 cube of the 256-color palette.
const fn rgb666(r: u8, g: u8, b: u8) -> Color {
    Color::Indexed(16 + 36 * r + 6 * g + b)
}

fn is_keyword(word: &str) -> bool {
    // Compare against the (sorted) list without upper-casing `word` into a temporary.
    KEYWORDS
        .binary_search_by(|kw| kw.bytes().cmp(word.bytes().map(|c| c.to_ascii_uppercase())))
        .is_ok()
}

/// A bare word immediately followed by `(` is a function; otherwise an identifier.
fn is_function_call(tokens: &[Token], i: usize) -> bool {
    tokens[i + 1..]
        .iter()
        .find(|t| t.is_significant())
        .is_some_and(|t| t.token_type == TokenType::OpeningRoundBracket)
}

/// ClickHouse SQL syntax highlighting.
///
/// A port of `highlightWithLexer()` from ClickHouse's `src/Client/ClientBaseHelpers.cpp`, so that
/// the colors match `clickhouse-client` and the Web UI (`programs/server/play.html`). Unlike a
/// parser-based path it never fails: it just tokenizes the input and colors each token by its kind.
pub fn highlight_sql(query: &str) -> StyledString {
    let tokens: Vec<Token> = Lexer::new(query).collect();

    let mut result = StyledString::new();
    let mut offset = 0;
    for (i, token) in tokens.iter().enumerate() {
        let text = &query[token.begin..token.end];

        let style = match token.token_type {
            TokenType::Comment => Style::new().fg(Color::DarkGray),
            TokenType::Number => Style::new().fg(rgb666(0, 4, 0)),
            TokenType::StringLiteral | TokenType::HereDoc => Style::new().fg(Color::Green),
            TokenType::QuotedIdentifier => Style::new().fg(rgb666(0, 4, 4)),
            TokenType::BareWord => {
                if is_keyword(text) {
                    Style::new().add_modifier(Modifier::BOLD)
                } else if is_function_call(&tokens, i) {
                    Style::new().fg(Color::Yellow)
                } else {
                    Style::new().fg(Color::Cyan)
                }
            }
            _ if token.is_error() => Style::new().fg(Color::Red),
            // Operators, brackets and punctuation keep the default color.
            _ => Style::default(),
        };

        result.append_styled(text, style);
        offset = token.end;
    }

    // The lexer stops at the first error token, so the rest of the query (if any) is left plain.
    if offset < query.len() {
        result.append_plain(&query[offset..]);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keywords_are_recognized_case_insensitively() {
        assert!(is_keyword("SELECT"));
        assert!(is_keyword("select"));
        assert!(is_keyword("SeLeCt"));
        assert!(is_keyword("ZKPATH"));
        assert!(is_keyword("ADD"));
        assert!(!is_keyword("hostName"));
        assert!(!is_keyword("SELECTED"));
        assert!(!is_keyword("SELEC"));
        assert!(!is_keyword(""));
        assert!(!is_keyword("привет"));
    }

    #[test]
    fn keyword_list_is_sorted() {
        assert!(KEYWORDS.windows(2).all(|w| w[0] < w[1]), "{KEYWORDS:?}");
    }

    #[test]
    fn highlighting_preserves_the_query() {
        for query in [
            "SELECT * FROM system.query_log WHERE event_date >= today() - 1 LIMIT 10",
            "SELECT hostName() AS h, count() FROM system.parts GROUP BY h -- комментарий\n",
            "CREATE TABLE t (`колонка` String) ENGINE = MergeTree ORDER BY tuple()",
            // stops the lexer on an error token, the tail must still be kept
            "SELECT 'unterminated FROM t",
            "SELECT #hello",
            "",
        ] {
            assert_eq!(highlight_sql(query).source(), query, "{query:?}");
        }
    }
}
