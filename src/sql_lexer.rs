//! A port of ClickHouse's `src/Parsers/Lexer.{h,cpp}`.
//!
//! Kept structurally identical to the C++ original (same token types in the same order, same
//! branch layout, same edge cases) so that it can be diffed against upstream when it changes.
//! Offsets are byte indices into the input instead of pointers.

/// Must stay in the upstream order: `EndOfStream` separates regular tokens from error tokens,
/// which is what [`Token::is_error`] relies on.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum TokenType {
    Whitespace,
    Comment,

    /// Either keyword (SELECT) or identifier (column)
    BareWord,

    /// Always non-negative. No leading plus. 123 or something like 123.456e12, 0x123p12
    Number,
    /// 'hello word', 'hello''word', 'hello\'word\\'
    StringLiteral,

    /// "x", `x`
    QuotedIdentifier,

    OpeningRoundBracket,
    ClosingRoundBracket,

    OpeningSquareBracket,
    ClosingSquareBracket,

    OpeningCurlyBrace,
    ClosingCurlyBrace,

    Comma,
    Semicolon,
    /// Vertical delimiter \G
    VerticalDelimiter,
    /// Compound identifiers, like a.b or tuple access operator a.1, (x, y).2.
    /// Need to be distinguished from floating point number with omitted integer part: .1
    Dot,

    /// Could be used as multiplication operator or on it's own: "SELECT *"
    Asterisk,

    HereDoc,

    DollarSign,
    Plus,
    Minus,
    Slash,
    Percent,
    /// ->. Should be distinguished from minus operator.
    Arrow,
    QuestionMark,
    Colon,
    Caret,
    DoubleColon,
    Equals,
    NotEquals,
    Less,
    Greater,
    LessOrEquals,
    GreaterOrEquals,
    /// <=>. Used in MySQL for NULL-safe equality comparison.
    Spaceship,
    PipeMark,
    /// |>. Pipe operator: FROM t |> WHERE x |> SELECT y
    PipeOperator,
    /// String concatenation operator: ||
    Concatenation,
    /// ~. Used in PostgreSQL as the regular expression match operator.
    Tilde,
    /// ~*. Used in PostgreSQL as the case-insensitive regular expression match operator.
    TildeAsterisk,
    /// !~. Used in PostgreSQL as the regular expression not-match operator.
    NotTilde,
    /// !~*. Used in PostgreSQL as the case-insensitive regular expression not-match operator.
    NotTildeAsterisk,

    /// @. Used for specifying user names and also for MySQL-style variables.
    At,
    /// @@. Used for MySQL-style global variables.
    DoubleAt,

    // Order is important. EndOfStream goes after all usual tokens, and special error tokens goes
    // after EndOfStream.
    EndOfStream,

    /// Something unrecognized.
    Error,
    // Something is wrong and we have more information.
    ErrorMultilineCommentIsNotClosed,
    ErrorSingleQuoteIsNotClosed,
    ErrorDoubleQuoteIsNotClosed,
    ErrorBackQuoteIsNotClosed,
    ErrorSingleExclamationMark,
    #[allow(dead_code)] // never produced by the lexer, kept to mirror upstream
    ErrorSinglePipeMark,
    ErrorWrongNumber,
    ErrorMaxQuerySizeExceeded,
}

#[derive(Clone, Copy, Debug)]
pub struct Token {
    pub token_type: TokenType,
    pub begin: usize,
    pub end: usize,
}

impl Token {
    fn new(token_type: TokenType, begin: usize, end: usize) -> Self {
        Self {
            token_type,
            begin,
            end,
        }
    }

    pub fn is_significant(&self) -> bool {
        !matches!(self.token_type, TokenType::Whitespace | TokenType::Comment)
    }

    pub fn is_error(&self) -> bool {
        self.token_type > TokenType::EndOfStream
    }

    pub fn is_end(&self) -> bool {
        self.token_type == TokenType::EndOfStream
    }
}

pub struct Lexer<'a> {
    input: &'a [u8],
    pos: usize,
    max_query_size: usize,
    /// This is needed to disambiguate tuple access operator from floating point number (.1).
    prev_significant_token_type: TokenType,
    /// Set once [`Lexer::next_token`] returned an end or error token, so that the [`Iterator`]
    /// implementation stops there (as every upstream caller does).
    finished: bool,
}

/// Some reasonable size to at least avoid pointer overflows.
const MAX_QUERY_SIZE_LIMIT: usize = 1_000_000_000;

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self::with_max_query_size(input, 0)
    }

    pub fn with_max_query_size(input: &'a str, max_query_size: usize) -> Self {
        Self {
            input: input.as_bytes(),
            pos: 0,
            max_query_size: max_query_size.min(MAX_QUERY_SIZE_LIMIT),
            prev_significant_token_type: TokenType::Whitespace, // no previous token
            finished: false,
        }
    }

    pub fn next_token(&mut self) -> Token {
        let mut res = self.next_token_impl();
        if self.max_query_size != 0 && res.end > self.max_query_size {
            res.token_type = TokenType::ErrorMaxQuerySizeExceeded;
        }
        if res.is_significant() {
            self.prev_significant_token_type = res.token_type;
        }
        res
    }

    fn next_token_impl(&mut self) -> Token {
        let input = self.input;
        let len = input.len();

        if self.pos >= len {
            return Token::new(TokenType::EndOfStream, len, len);
        }

        let token_begin = self.pos;

        match input[self.pos] {
            b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b => {
                self.pos += 1;
                while self.pos < len && is_whitespace_ascii(input[self.pos]) {
                    self.pos += 1;
                }
                Token::new(TokenType::Whitespace, token_begin, self.pos)
            }

            b'0'..=b'9' => self.number(token_begin),

            b'\'' => self.quoted_string(
                b'\'',
                token_begin,
                TokenType::StringLiteral,
                TokenType::ErrorSingleQuoteIsNotClosed,
            ),
            b'"' => self.quoted_string(
                b'"',
                token_begin,
                TokenType::QuotedIdentifier,
                TokenType::ErrorDoubleQuoteIsNotClosed,
            ),
            b'`' => self.quoted_string(
                b'`',
                token_begin,
                TokenType::QuotedIdentifier,
                TokenType::ErrorBackQuoteIsNotClosed,
            ),

            b'(' => self.single(TokenType::OpeningRoundBracket, token_begin),
            b')' => self.single(TokenType::ClosingRoundBracket, token_begin),
            b'[' => self.single(TokenType::OpeningSquareBracket, token_begin),
            b']' => self.single(TokenType::ClosingSquareBracket, token_begin),
            b'{' => self.single(TokenType::OpeningCurlyBrace, token_begin),
            b'}' => self.single(TokenType::ClosingCurlyBrace, token_begin),
            b',' => self.single(TokenType::Comma, token_begin),
            b';' => self.single(TokenType::Semicolon, token_begin),

            // qualifier, tuple access operator or start of floating point number
            b'.' => self.dot(token_begin),

            b'+' => self.single(TokenType::Plus, token_begin),
            // minus (-), arrow (->) or start of comment (--)
            b'-' => {
                self.pos += 1;
                if self.pos < len && input[self.pos] == b'>' {
                    return self.single(TokenType::Arrow, token_begin);
                }
                if self.pos < len && input[self.pos] == b'-' {
                    self.pos += 1;
                    return self.comment_until_end_of_line(token_begin);
                }
                Token::new(TokenType::Minus, token_begin, self.pos)
            }
            b'*' => self.single(TokenType::Asterisk, token_begin),
            // division (/) or start of comment (//, /*)
            b'/' => {
                self.pos += 1;
                if self.pos < len && (input[self.pos] == b'/' || input[self.pos] == b'*') {
                    if input[self.pos] == b'/' {
                        self.pos += 1;
                        return self.comment_until_end_of_line(token_begin);
                    }

                    self.pos += 1;

                    // Nested multiline comments are supported according to the SQL standard.
                    let mut nesting_level = 1;

                    while self.pos + 2 <= len {
                        if input[self.pos] == b'/' && input[self.pos + 1] == b'*' {
                            self.pos += 2;
                            nesting_level += 1;
                        } else if input[self.pos] == b'*' && input[self.pos + 1] == b'/' {
                            self.pos += 2;
                            nesting_level -= 1;

                            if nesting_level == 0 {
                                return Token::new(TokenType::Comment, token_begin, self.pos);
                            }
                        } else {
                            self.pos += 1;
                        }
                    }
                    self.pos = len;
                    return Token::new(
                        TokenType::ErrorMultilineCommentIsNotClosed,
                        token_begin,
                        self.pos,
                    );
                }
                Token::new(TokenType::Slash, token_begin, self.pos)
            }
            // start of single line comment, MySQL style.
            // PostgreSQL has some operators using '#' character. For less ambiguity, we will
            // recognize a comment only if # is followed by whitespace, or #! as a special case for
            // "shebang".
            //   #hello - not a comment
            //   # hello - a comment
            //   #!/usr/bin/clickhouse-local --queries-file - a comment
            b'#' => {
                self.pos += 1;
                if self.pos < len && (input[self.pos] == b' ' || input[self.pos] == b'!') {
                    return self.comment_until_end_of_line(token_begin);
                }
                Token::new(TokenType::Error, token_begin, self.pos)
            }
            b'%' => self.single(TokenType::Percent, token_begin),
            // =, ==
            b'=' => {
                self.pos += 1;
                if self.pos < len && input[self.pos] == b'=' {
                    self.pos += 1;
                }
                Token::new(TokenType::Equals, token_begin, self.pos)
            }
            // !=, !~, !~*
            b'!' => {
                self.pos += 1;
                if self.pos < len && input[self.pos] == b'=' {
                    return self.single(TokenType::NotEquals, token_begin);
                }
                if self.pos < len && input[self.pos] == b'~' {
                    self.pos += 1;
                    if self.pos < len && input[self.pos] == b'*' {
                        return self.single(TokenType::NotTildeAsterisk, token_begin);
                    }
                    return Token::new(TokenType::NotTilde, token_begin, self.pos);
                }
                Token::new(TokenType::ErrorSingleExclamationMark, token_begin, self.pos)
            }
            // ~, ~* - regular expression match operators, PostgreSQL-style
            b'~' => {
                self.pos += 1;
                if self.pos < len && input[self.pos] == b'*' {
                    return self.single(TokenType::TildeAsterisk, token_begin);
                }
                Token::new(TokenType::Tilde, token_begin, self.pos)
            }
            // <, <=, <>, <=>
            b'<' => {
                self.pos += 1;
                if self.pos + 1 < len && input[self.pos] == b'=' && input[self.pos + 1] == b'>' {
                    self.pos += 2;
                    return Token::new(TokenType::Spaceship, token_begin, self.pos);
                }
                if self.pos < len && input[self.pos] == b'=' {
                    return self.single(TokenType::LessOrEquals, token_begin);
                }
                if self.pos < len && input[self.pos] == b'>' {
                    return self.single(TokenType::NotEquals, token_begin);
                }
                Token::new(TokenType::Less, token_begin, self.pos)
            }
            // >, >=
            b'>' => {
                self.pos += 1;
                if self.pos < len && input[self.pos] == b'=' {
                    return self.single(TokenType::GreaterOrEquals, token_begin);
                }
                Token::new(TokenType::Greater, token_begin, self.pos)
            }
            b'?' => self.single(TokenType::QuestionMark, token_begin),
            b'^' => self.single(TokenType::Caret, token_begin),
            b':' => {
                self.pos += 1;
                if self.pos < len && input[self.pos] == b':' {
                    return self.single(TokenType::DoubleColon, token_begin);
                }
                Token::new(TokenType::Colon, token_begin, self.pos)
            }
            b'|' => {
                self.pos += 1;
                if self.pos < len && input[self.pos] == b'|' {
                    return self.single(TokenType::Concatenation, token_begin);
                }
                if self.pos < len && input[self.pos] == b'>' {
                    return self.single(TokenType::PipeOperator, token_begin);
                }
                Token::new(TokenType::PipeMark, token_begin, self.pos)
            }
            b'@' => {
                self.pos += 1;
                if self.pos < len && input[self.pos] == b'@' {
                    return self.single(TokenType::DoubleAt, token_begin);
                }
                Token::new(TokenType::At, token_begin, self.pos)
            }
            b'\\' => {
                self.pos += 1;
                if self.pos < len && input[self.pos] == b'G' {
                    return self.single(TokenType::VerticalDelimiter, token_begin);
                }
                Token::new(TokenType::Error, token_begin, self.pos)
            }
            0xe2 => {
                // Mathematical minus symbol, UTF-8
                if self.pos + 3 <= len && input[self.pos + 1] == 0x88 && input[self.pos + 2] == 0x92
                {
                    self.pos += 3;
                    return Token::new(TokenType::Minus, token_begin, self.pos);
                }
                // Unicode quoted string, ‘Hello’ or “World”.
                if self.pos + 5 < len
                    && input[self.pos + 1] == 0x80
                    && (input[self.pos + 2] == 0x98 || input[self.pos + 2] == 0x9c)
                {
                    let expected_end_byte = input[self.pos + 2] + 1;
                    let (success_token, error_token) = if input[self.pos + 2] == 0x98 {
                        (
                            TokenType::StringLiteral,
                            TokenType::ErrorSingleQuoteIsNotClosed,
                        )
                    } else {
                        (
                            TokenType::QuotedIdentifier,
                            TokenType::ErrorDoubleQuoteIsNotClosed,
                        )
                    };
                    self.pos += 3;
                    return self.quoted_string_with_unicode_quotes(
                        token_begin,
                        expected_end_byte,
                        success_token,
                        error_token,
                    );
                }
                // Other characters starting at E2 can be parsed, see skip_whitespaces_utf8
                self.default_token(token_begin)
            }
            _ => self.default_token(token_begin),
        }
    }

    /// A single-character token whose character has already been consumed by the caller (mirrors
    /// the `++pos` inside upstream's `Token(type, token_begin, ++pos)`).
    fn single(&mut self, token_type: TokenType, token_begin: usize) -> Token {
        self.pos += 1;
        Token::new(token_type, token_begin, self.pos)
    }

    fn comment_until_end_of_line(&mut self, token_begin: usize) -> Token {
        // This means that newline in single-line comment cannot be escaped.
        self.pos = find(self.input, self.pos, b'\n');
        Token::new(TokenType::Comment, token_begin, self.pos)
    }

    fn number(&mut self, token_begin: usize) -> Token {
        let input = self.input;
        let len = input.len();

        // The task is not to parse a number or check correctness, but only to skip it.

        // Disambiguation: if previous token was dot, then we could parse only simple integer,
        // for chained tuple access operators (x.1.1) to work.
        // Otherwise it will be tokenized as x . 1.1, not as x . 1 . 1
        if self.prev_significant_token_type == TokenType::Dot {
            self.pos += 1;
            while self.pos < len
                && (input[self.pos].is_ascii_digit()
                    || is_number_separator(false, false, input, self.pos))
            {
                self.pos += 1;
            }
        } else {
            let mut start_of_block = false;
            // 0x, 0b
            let mut hex = false;
            if self.pos + 2 < len
                && input[self.pos] == b'0'
                && matches!(input[self.pos + 1], b'x' | b'b' | b'X' | b'B')
            {
                let mut is_valid = false;
                if matches!(input[self.pos + 1], b'x' | b'X') {
                    if input[self.pos + 2].is_ascii_hexdigit() {
                        hex = true;
                        is_valid = true; // hex
                    }
                } else if matches!(input[self.pos + 2], b'0' | b'1') {
                    is_valid = true; // bin
                }
                if is_valid {
                    self.pos += 2;
                    start_of_block = true;
                } else {
                    self.pos += 1; // consume the leading zero - could be an identifier
                }
            } else {
                self.pos += 1;
            }

            while self.pos < len
                && (is_digit(hex, input[self.pos])
                    || is_number_separator(start_of_block, hex, input, self.pos))
            {
                self.pos += 1;
                start_of_block = false;
            }

            // decimal point
            if self.pos < len && input[self.pos] == b'.' {
                start_of_block = true;
                self.pos += 1;
                while self.pos < len
                    && (is_digit(hex, input[self.pos])
                        || is_number_separator(start_of_block, hex, input, self.pos))
                {
                    self.pos += 1;
                    start_of_block = false;
                }
            }

            // exponentiation (base 10 or base 2)
            if self.pos + 1 < len
                && (if hex {
                    matches!(input[self.pos], b'p' | b'P')
                } else {
                    matches!(input[self.pos], b'e' | b'E')
                })
            {
                start_of_block = true;
                self.pos += 1;

                // sign of exponent. It is always decimal.
                if self.pos + 1 < len && matches!(input[self.pos], b'-' | b'+') {
                    self.pos += 1;
                }

                while self.pos < len
                    && (input[self.pos].is_ascii_digit()
                        || is_number_separator(start_of_block, false, input, self.pos))
                {
                    self.pos += 1;
                    start_of_block = false;
                }
            }
        }

        // Try to parse it to a identifier(1identifier_name), otherwise it return ErrorWrongNumber
        if self.pos < len && is_word_char_ascii(input[self.pos]) {
            self.pos += 1;
            while self.pos < len && is_word_char_ascii(input[self.pos]) {
                self.pos += 1;
            }

            for &c in &input[token_begin..self.pos] {
                if !is_word_char_ascii(c) && c != b'$' {
                    return Token::new(TokenType::ErrorWrongNumber, token_begin, self.pos);
                }
            }

            return Token::new(TokenType::BareWord, token_begin, self.pos);
        }

        Token::new(TokenType::Number, token_begin, self.pos)
    }

    fn dot(&mut self, token_begin: usize) -> Token {
        let input = self.input;
        let len = input.len();

        // Just after identifier or complex expression or number (for chained tuple access like
        // x.1.1 to work properly).
        if self.pos > 0
            && (!(self.pos + 1 < len && input[self.pos + 1].is_ascii_digit())
                || matches!(
                    self.prev_significant_token_type,
                    TokenType::ClosingRoundBracket
                        | TokenType::ClosingSquareBracket
                        | TokenType::BareWord
                        | TokenType::QuotedIdentifier
                        | TokenType::Number
                ))
        {
            return self.single(TokenType::Dot, token_begin);
        }

        let mut start_of_block = true;
        self.pos += 1;
        while self.pos < len
            && (input[self.pos].is_ascii_digit()
                || is_number_separator(start_of_block, false, input, self.pos))
        {
            self.pos += 1;
            start_of_block = false;
        }

        // exponentiation
        if self.pos + 1 < len && matches!(input[self.pos], b'e' | b'E') {
            start_of_block = true;
            self.pos += 1;

            // sign of exponent
            if self.pos + 1 < len && matches!(input[self.pos], b'-' | b'+') {
                self.pos += 1;
            }

            while self.pos < len
                && (input[self.pos].is_ascii_digit()
                    || is_number_separator(start_of_block, false, input, self.pos))
            {
                self.pos += 1;
                start_of_block = false;
            }
        }

        Token::new(TokenType::Number, token_begin, self.pos)
    }

    fn default_token(&mut self, token_begin: usize) -> Token {
        let input = self.input;
        let len = input.len();

        if input[self.pos] == b'$' {
            // Try to capture a dollar sign as a start of heredoc
            let tag_end = find(input, self.pos + 1, b'$');
            if tag_end != len {
                let heredoc_size = tag_end + 1 - self.pos;

                let is_valid_name = input[self.pos + 1..tag_end]
                    .iter()
                    .all(|&c| is_word_char_ascii(c));

                if is_valid_name {
                    let tag = &input[self.pos..self.pos + heredoc_size];
                    if let Some(heredoc_end_position) = find_subslice(&input[tag_end + 1..], tag) {
                        self.pos = tag_end + 1 + heredoc_end_position + heredoc_size;
                        return Token::new(TokenType::HereDoc, token_begin, self.pos);
                    }
                }
            }

            if (self.pos + 1 < len && !is_word_char_ascii(input[self.pos + 1]))
                || self.pos + 1 == len
            {
                // Capture a standalone dollar sign
                return self.single(TokenType::DollarSign, token_begin);
            }
        }

        if self.pos + 2 < len
            && input[self.pos + 1] == b'\''
            && matches!(input[self.pos], b'x' | b'b' | b'X' | b'B')
        {
            return self.quoted_hex_or_bin_string(token_begin);
        }

        if is_word_char_ascii(input[self.pos]) || input[self.pos] == b'$' {
            self.pos += 1;
            while self.pos < len && (is_word_char_ascii(input[self.pos]) || input[self.pos] == b'$')
            {
                self.pos += 1;
            }
            return Token::new(TokenType::BareWord, token_begin, self.pos);
        }

        // We will also skip unicode whitespaces in UTF-8 to support for queries copy-pasted from
        // MS Word and similar.
        self.pos = skip_whitespaces_utf8(input, self.pos);
        if self.pos > token_begin {
            return Token::new(TokenType::Whitespace, token_begin, self.pos);
        }

        self.pos += 1;
        while self.pos < len && is_continuation_octet(input[self.pos]) {
            self.pos += 1;
        }

        Token::new(TokenType::Error, token_begin, self.pos)
    }

    /// This must be consistent with functions in ReadHelpers.h
    fn quoted_string(
        &mut self,
        quote: u8,
        token_begin: usize,
        success_token: TokenType,
        error_token: TokenType,
    ) -> Token {
        let input = self.input;
        let len = input.len();

        self.pos += 1;
        loop {
            self.pos = find_either(input, self.pos, quote, b'\\');
            if self.pos >= len {
                return Token::new(error_token, token_begin, len);
            }

            if input[self.pos] == quote {
                self.pos += 1;
                if self.pos < len && input[self.pos] == quote {
                    self.pos += 1;
                    continue;
                }
                return Token::new(success_token, token_begin, self.pos);
            }

            // input[self.pos] == b'\\'
            self.pos += 1;
            if self.pos >= len {
                return Token::new(error_token, token_begin, len);
            }
            self.pos += 1;
        }
    }

    fn quoted_string_with_unicode_quotes(
        &mut self,
        token_begin: usize,
        expected_end_byte: u8,
        success_token: TokenType,
        error_token: TokenType,
    ) -> Token {
        // ‘: e2 80 98
        // ’: e2 80 99
        // “: e2 80 9c
        // ”: e2 80 9d
        let input = self.input;
        let len = input.len();

        loop {
            self.pos = find(input, self.pos, 0xe2);
            if self.pos + 2 >= len {
                return Token::new(error_token, token_begin, len);
            }

            if input[self.pos] == 0xe2
                && input[self.pos + 1] == 0x80
                && input[self.pos + 2] == expected_end_byte
            {
                self.pos += 3;
                return Token::new(success_token, token_begin, self.pos);
            }

            self.pos += 1;
        }
    }

    fn quoted_hex_or_bin_string(&mut self, token_begin: usize) -> Token {
        const QUOTE: u8 = b'\'';

        let input = self.input;
        let len = input.len();

        debug_assert_eq!(input[self.pos + 1], QUOTE);

        let hex = matches!(input[self.pos], b'x' | b'X');

        self.pos += 2;

        if hex {
            while self.pos < len && input[self.pos].is_ascii_hexdigit() {
                self.pos += 1;
            }
        } else {
            while self.pos < len && matches!(input[self.pos], b'0' | b'1') {
                self.pos += 1;
            }
        }

        if self.pos >= len || input[self.pos] != QUOTE {
            self.pos = len;
            return Token::new(TokenType::ErrorSingleQuoteIsNotClosed, token_begin, len);
        }

        self.pos += 1;
        Token::new(TokenType::StringLiteral, token_begin, self.pos)
    }
}

/// Yields tokens up to and including the first error token; `EndOfStream` terminates the
/// iteration instead of being yielded.
impl Iterator for Lexer<'_> {
    type Item = Token;

    fn next(&mut self) -> Option<Token> {
        if self.finished {
            return None;
        }
        let token = self.next_token();
        if token.is_end() {
            self.finished = true;
            return None;
        }
        if token.is_error() {
            self.finished = true;
        }
        Some(token)
    }
}

fn is_whitespace_ascii(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b)
}

fn is_word_char_ascii(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

fn is_continuation_octet(c: u8) -> bool {
    (c & 0xc0) == 0x80
}

fn is_digit(hex: bool, c: u8) -> bool {
    if hex {
        c.is_ascii_hexdigit()
    } else {
        c.is_ascii_digit()
    }
}

fn is_number_separator(is_start_of_block: bool, is_hex: bool, input: &[u8], pos: usize) -> bool {
    if input[pos] != b'_' {
        return false;
    }
    if is_start_of_block {
        return false; // e.g. _123, 12e_3
    }
    let len = input.len();
    if pos + 1 < len && !is_digit(is_hex, input[pos + 1]) {
        return false; // e.g. 1__2, 1_., 1_e, 1_p, 1_;
    }
    if pos + 1 == len {
        return false; // e.g. 12_
    }
    true
}

/// Index of the first `needle` at or after `from`, or `input.len()` (mirrors `find_first_symbols`).
fn find(input: &[u8], from: usize, needle: u8) -> usize {
    match input[from.min(input.len())..]
        .iter()
        .position(|&c| c == needle)
    {
        Some(i) => from + i,
        None => input.len(),
    }
}

fn find_either(input: &[u8], from: usize, a: u8, b: u8) -> usize {
    match input[from.min(input.len())..]
        .iter()
        .position(|&c| c == a || c == b)
    {
        Some(i) => from + i,
        None => input.len(),
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// https://en.wikipedia.org/wiki/Whitespace_character with some adjustments.
///
/// Code points: 0085 00A0 180E 2000..200A 2028..2029 200B..200D 202F 205F 2060 3000 FEFF
/// The corresponding UTF-8 is: C285 C2A0 E1A08E E28080..E2808A E280A8..E280A9 E2808B..E2808D
/// E280AF E2819F E281A0 E38080 EFBBBF
///
/// We check for these bytes directly in UTF8 for simplicity reasons.
fn skip_whitespaces_utf8(input: &[u8], mut pos: usize) -> usize {
    let len = input.len();
    while pos < len {
        if is_whitespace_ascii(input[pos]) {
            pos += 1;
        } else if pos + 1 < len && input[pos] == 0xc2 && matches!(input[pos + 1], 0x85 | 0xa0) {
            pos += 2;
        } else if pos + 2 < len
            && ((input[pos] == 0xe1 && input[pos + 1] == 0xa0 && input[pos + 2] == 0x8e)
                || (input[pos] == 0xe2
                    && ((input[pos + 1] == 0x80
                        && (matches!(input[pos + 2], 0x80..=0x8a)
                            || matches!(input[pos + 2], 0xa8..=0xa9)
                            || matches!(input[pos + 2], 0x8b..=0x8d)
                            || input[pos + 2] == 0xaf))
                        || (input[pos + 1] == 0x81 && matches!(input[pos + 2], 0x9f | 0xa0))))
                || (input[pos] == 0xe3 && input[pos + 1] == 0x80 && input[pos + 2] == 0x80)
                || (input[pos] == 0xef && input[pos + 1] == 0xbb && input[pos + 2] == 0xbf))
        {
            pos += 3;
        } else {
            break;
        }
    }
    pos
}

#[cfg(test)]
mod tests {
    use super::TokenType::*;
    use super::*;

    fn lex(query: &str) -> Vec<(TokenType, &str)> {
        Lexer::new(query)
            .map(|t| (t.token_type, &query[t.begin..t.end]))
            .collect()
    }

    fn kinds(query: &str) -> Vec<TokenType> {
        lex(query).into_iter().map(|(t, _)| t).collect()
    }

    #[test]
    fn tokens_cover_the_whole_input() {
        for query in [
            "SELECT * FROM system.query_log WHERE event_date >= today() - 1 LIMIT 10",
            "SELECT hostName() h FROM clusterAllReplicas(default, system.parts) SETTINGS skip_unavailable_shards = 1",
            "SELECT user, groupArray(query) FROM system.processes GROUP BY user LIMIT 3 BY user",
            "CREATE TABLE t (`a` LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY a",
            "WITH cte AS (SELECT 1 AS x) SELECT x::String FROM cte FORMAT JSONEachRow",
            "SELECT 'строка', `колонка` FROM t -- комментарий\n",
            "SELECT a |> b, x <=> y, p !~* 'q', ~1, @@v, [1,2][1], {'a':1}\\G",
        ] {
            let mut offset = 0;
            for token in Lexer::new(query) {
                assert_eq!(token.begin, offset, "gap or overlap in {query:?}");
                assert!(token.end > token.begin, "empty token in {query:?}");
                offset = token.end;
            }
            assert_eq!(offset, query.len(), "input not fully consumed: {query:?}");
        }
    }

    #[test]
    fn numbers() {
        assert_eq!(kinds("123"), [Number]);
        assert_eq!(kinds("0x1f"), [Number]);
        assert_eq!(kinds("0b1011"), [Number]);
        assert_eq!(kinds("1_000_000"), [Number]);
        assert_eq!(kinds("123.456e12"), [Number]);
        assert_eq!(kinds("0x123p12"), [Number]);
        assert_eq!(kinds(".1"), [Number]);
        // leading zero of an invalid 0x/0b is an identifier continuation
        assert_eq!(kinds("0xz"), [BareWord]);
        // 1identifier_name
        assert_eq!(kinds("1abc"), [BareWord]);
        assert_eq!(kinds("1.5e3abc"), [ErrorWrongNumber]);
        // a trailing separator makes the whole thing an identifier
        assert_eq!(lex("12_"), [(BareWord, "12_")]);
    }

    #[test]
    fn chained_tuple_access() {
        assert_eq!(
            lex("x.1.1"),
            [
                (BareWord, "x"),
                (Dot, "."),
                (Number, "1"),
                (Dot, "."),
                (Number, "1"),
            ]
        );
        assert_eq!(lex("(1, 2).2").last().unwrap(), &(Number, "2"));
        // not after an expression: a floating point number
        assert_eq!(lex("SELECT 1, .1").last().unwrap(), &(Number, ".1"));
    }

    #[test]
    fn strings() {
        assert_eq!(kinds("'hello world'"), [StringLiteral]);
        assert_eq!(kinds("'hello''word'"), [StringLiteral]);
        assert_eq!(kinds(r"'hello\'word\\'"), [StringLiteral]);
        assert_eq!(kinds("'unterminated"), [ErrorSingleQuoteIsNotClosed]);
        assert_eq!(kinds(r"'trailing escape\"), [ErrorSingleQuoteIsNotClosed]);
        assert_eq!(kinds("\"x\""), [QuotedIdentifier]);
        assert_eq!(kinds("`x`"), [QuotedIdentifier]);
        assert_eq!(kinds("\"unterminated"), [ErrorDoubleQuoteIsNotClosed]);
        assert_eq!(kinds("`unterminated"), [ErrorBackQuoteIsNotClosed]);
        // hex/bin string literals
        assert_eq!(kinds("x'0fAb'"), [StringLiteral]);
        assert_eq!(kinds("b'0101'"), [StringLiteral]);
        assert_eq!(kinds("x'0f"), [ErrorSingleQuoteIsNotClosed]);
    }

    #[test]
    fn comments() {
        assert_eq!(lex("-- c\n"), [(Comment, "-- c"), (Whitespace, "\n")]);
        assert_eq!(lex("// c\n"), [(Comment, "// c"), (Whitespace, "\n")]);
        assert_eq!(kinds("/* c */"), [Comment]);
        // nested multiline comments are supported according to the SQL standard
        assert_eq!(kinds("/* a /* b */ c */"), [Comment]);
        assert_eq!(kinds("/* a /* b */"), [ErrorMultilineCommentIsNotClosed]);
        // '#' is a comment only before a space or '!'
        assert_eq!(lex("# c\n"), [(Comment, "# c"), (Whitespace, "\n")]);
        assert_eq!(kinds("#!/usr/bin/clickhouse-local"), [Comment]);
        assert_eq!(kinds("#hello"), [Error]);
    }

    #[test]
    fn heredoc() {
        assert_eq!(kinds("$doc$hello$doc$"), [HereDoc]);
        assert_eq!(kinds("$$hello$$"), [HereDoc]);
        // unterminated: falls back to a bare word starting with '$'
        assert_eq!(kinds("$doc$hello"), [BareWord]);
        assert_eq!(
            lex("$ x"),
            [(DollarSign, "$"), (Whitespace, " "), (BareWord, "x")]
        );
        // an invalid tag name is not a heredoc
        assert_eq!(
            lex("$a b$"),
            [(BareWord, "$a"), (Whitespace, " "), (BareWord, "b$")]
        );
    }

    #[test]
    fn operators() {
        assert_eq!(kinds("->"), [Arrow]);
        assert_eq!(kinds("-"), [Minus]);
        assert_eq!(kinds("::"), [DoubleColon]);
        assert_eq!(kinds(":"), [Colon]);
        assert_eq!(kinds("=="), [Equals]);
        assert_eq!(kinds("!="), [NotEquals]);
        assert_eq!(kinds("<>"), [NotEquals]);
        assert_eq!(kinds("<=>"), [Spaceship]);
        assert_eq!(kinds("<="), [LessOrEquals]);
        assert_eq!(kinds(">="), [GreaterOrEquals]);
        assert_eq!(kinds("!~*"), [NotTildeAsterisk]);
        assert_eq!(kinds("!~"), [NotTilde]);
        assert_eq!(kinds("~*"), [TildeAsterisk]);
        assert_eq!(kinds("~"), [Tilde]);
        assert_eq!(kinds("||"), [Concatenation]);
        assert_eq!(kinds("|>"), [PipeOperator]);
        assert_eq!(kinds("|"), [PipeMark]);
        assert_eq!(kinds("@@"), [DoubleAt]);
        assert_eq!(kinds("@"), [At]);
        assert_eq!(kinds("\\G"), [VerticalDelimiter]);
        assert_eq!(kinds("\\x"), [Error]);
        assert_eq!(kinds("!"), [ErrorSingleExclamationMark]);
    }

    #[test]
    fn unicode() {
        // mathematical minus
        assert_eq!(lex("1 − 2")[2], (Minus, "−"));
        // unicode quotes
        assert_eq!(kinds("‘Hello’"), [StringLiteral]);
        assert_eq!(kinds("“World”"), [QuotedIdentifier]);
        assert_eq!(kinds("‘Hello"), [ErrorSingleQuoteIsNotClosed]);
        // non-breaking space and friends are whitespace
        assert_eq!(kinds("1\u{a0}2"), [Number, Whitespace, Number]);
        assert_eq!(kinds("1\u{feff}2"), [Number, Whitespace, Number]);
        // and anything else is an error token covering the whole code point
        assert_eq!(lex("SELECT ≠")[2], (Error, "≠"));
    }

    #[test]
    fn max_query_size() {
        let mut lexer = Lexer::with_max_query_size("SELECT 1", 6);
        assert_eq!(lexer.next_token().token_type, BareWord); // "SELECT" fits exactly
        assert_eq!(lexer.next_token().token_type, ErrorMaxQuerySizeExceeded);
    }

    #[test]
    fn empty() {
        assert!(lex("").is_empty());
        let mut lexer = Lexer::new("");
        assert!(lexer.next_token().is_end());
    }
}
