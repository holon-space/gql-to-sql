use logos::Logos;
use std::fmt;

use crate::ast::Span;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    All,
    AllShortestPaths,
    Analyze,
    And,
    Any,
    As,
    Asc,
    Ascending,
    By,
    Call,
    Case,
    Coalesce,
    Contains,
    Count,
    Create,
    Delete,
    Desc,
    Descending,
    Detach,
    Distinct,
    Else,
    End,
    Ends,
    Exists,
    Explain,
    False,
    For,
    From,
    In,
    Is,
    Limit,
    Match,
    None,
    Not,
    Null,
    On,
    Operator,
    Optional,
    Or,
    Order,
    Pattern,
    Reduce,
    Return,
    Set,
    ShortestPath,
    Single,
    Skip,
    Starts,
    Then,
    True,
    Union,
    Verbose,
    When,
    Where,
    With,
    Xor,
    Yield,

    // Literals
    Integer(i64),
    Float(f64),
    StringLit(String),

    // Identifiers
    Identifier(String),
    BacktickIdent(String),
    Parameter(String),

    // Operators
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    RegexMatch,
    PlusEq,

    // Punctuation
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Dot,
    DotDot,
    Comma,
    Colon,
    Semicolon,
    Pipe,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::All => write!(f, "ALL"),
            Token::AllShortestPaths => write!(f, "ALLSHORTESTPATHS"),
            Token::Analyze => write!(f, "ANALYZE"),
            Token::And => write!(f, "AND"),
            Token::Any => write!(f, "ANY"),
            Token::As => write!(f, "AS"),
            Token::Asc => write!(f, "ASC"),
            Token::Ascending => write!(f, "ASCENDING"),
            Token::By => write!(f, "BY"),
            Token::Call => write!(f, "CALL"),
            Token::Case => write!(f, "CASE"),
            Token::Coalesce => write!(f, "COALESCE"),
            Token::Contains => write!(f, "CONTAINS"),
            Token::Count => write!(f, "COUNT"),
            Token::Create => write!(f, "CREATE"),
            Token::Delete => write!(f, "DELETE"),
            Token::Desc => write!(f, "DESC"),
            Token::Descending => write!(f, "DESCENDING"),
            Token::Detach => write!(f, "DETACH"),
            Token::Distinct => write!(f, "DISTINCT"),
            Token::Else => write!(f, "ELSE"),
            Token::End => write!(f, "END"),
            Token::Ends => write!(f, "ENDS"),
            Token::Exists => write!(f, "EXISTS"),
            Token::Explain => write!(f, "EXPLAIN"),
            Token::False => write!(f, "FALSE"),
            Token::For => write!(f, "FOR"),
            Token::From => write!(f, "FROM"),
            Token::In => write!(f, "IN"),
            Token::Is => write!(f, "IS"),
            Token::Limit => write!(f, "LIMIT"),
            Token::Match => write!(f, "MATCH"),
            Token::None => write!(f, "NONE"),
            Token::Not => write!(f, "NOT"),
            Token::Null => write!(f, "NULL"),
            Token::On => write!(f, "ON"),
            Token::Operator => write!(f, "OPERATOR"),
            Token::Optional => write!(f, "OPTIONAL"),
            Token::Or => write!(f, "OR"),
            Token::Order => write!(f, "ORDER"),
            Token::Pattern => write!(f, "PATTERN"),
            Token::Reduce => write!(f, "REDUCE"),
            Token::Return => write!(f, "RETURN"),
            Token::Set => write!(f, "SET"),
            Token::ShortestPath => write!(f, "SHORTESTPATH"),
            Token::Single => write!(f, "SINGLE"),
            Token::Skip => write!(f, "SKIP"),
            Token::Starts => write!(f, "STARTS"),
            Token::Then => write!(f, "THEN"),
            Token::True => write!(f, "TRUE"),
            Token::Union => write!(f, "UNION"),
            Token::Verbose => write!(f, "VERBOSE"),
            Token::When => write!(f, "WHEN"),
            Token::Where => write!(f, "WHERE"),
            Token::With => write!(f, "WITH"),
            Token::Xor => write!(f, "XOR"),
            Token::Yield => write!(f, "YIELD"),
            Token::Integer(n) => write!(f, "{n}"),
            Token::Float(n) => write!(f, "{n}"),
            Token::StringLit(s) => write!(f, "\"{s}\""),
            Token::Identifier(s) => write!(f, "{s}"),
            Token::BacktickIdent(s) => write!(f, "`{s}`"),
            Token::Parameter(s) => write!(f, "${s}"),
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::Star => write!(f, "*"),
            Token::Slash => write!(f, "/"),
            Token::Percent => write!(f, "%"),
            Token::Eq => write!(f, "="),
            Token::NotEq => write!(f, "<>"),
            Token::Lt => write!(f, "<"),
            Token::Gt => write!(f, ">"),
            Token::LtEq => write!(f, "<="),
            Token::GtEq => write!(f, ">="),
            Token::RegexMatch => write!(f, "=~"),
            Token::PlusEq => write!(f, "+="),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LBracket => write!(f, "["),
            Token::RBracket => write!(f, "]"),
            Token::LBrace => write!(f, "{{"),
            Token::RBrace => write!(f, "}}"),
            Token::Dot => write!(f, "."),
            Token::DotDot => write!(f, ".."),
            Token::Comma => write!(f, ","),
            Token::Colon => write!(f, ":"),
            Token::Semicolon => write!(f, ";"),
            Token::Pipe => write!(f, "|"),
        }
    }
}

fn keyword_lookup(s: &str) -> Option<Token> {
    match s.to_ascii_lowercase().as_str() {
        "all" => Some(Token::All),
        "allshortestpaths" => Some(Token::AllShortestPaths),
        "analyze" => Some(Token::Analyze),
        "and" => Some(Token::And),
        "any" => Some(Token::Any),
        "as" => Some(Token::As),
        "asc" => Some(Token::Asc),
        "ascending" => Some(Token::Ascending),
        "by" => Some(Token::By),
        "call" => Some(Token::Call),
        "case" => Some(Token::Case),
        "coalesce" => Some(Token::Coalesce),
        "contains" => Some(Token::Contains),
        "count" => Some(Token::Count),
        "create" => Some(Token::Create),
        "delete" => Some(Token::Delete),
        "desc" => Some(Token::Desc),
        "descending" => Some(Token::Descending),
        "detach" => Some(Token::Detach),
        "distinct" => Some(Token::Distinct),
        "else" => Some(Token::Else),
        "end" => Some(Token::End),
        "ends" => Some(Token::Ends),
        "exists" => Some(Token::Exists),
        "explain" => Some(Token::Explain),
        "false" => Some(Token::False),
        "for" => Some(Token::For),
        "from" => Some(Token::From),
        "in" => Some(Token::In),
        "is" => Some(Token::Is),
        "limit" => Some(Token::Limit),
        "match" => Some(Token::Match),
        "none" => Some(Token::None),
        "not" => Some(Token::Not),
        "null" => Some(Token::Null),
        "on" => Some(Token::On),
        "operator" => Some(Token::Operator),
        "optional" => Some(Token::Optional),
        "or" => Some(Token::Or),
        "order" => Some(Token::Order),
        "pattern" => Some(Token::Pattern),
        "reduce" => Some(Token::Reduce),
        "return" => Some(Token::Return),
        "set" => Some(Token::Set),
        "shortestpath" => Some(Token::ShortestPath),
        "single" => Some(Token::Single),
        "skip" => Some(Token::Skip),
        "starts" => Some(Token::Starts),
        "then" => Some(Token::Then),
        "true" => Some(Token::True),
        "union" => Some(Token::Union),
        "verbose" => Some(Token::Verbose),
        "when" => Some(Token::When),
        "where" => Some(Token::Where),
        "with" => Some(Token::With),
        "xor" => Some(Token::Xor),
        "yield" => Some(Token::Yield),
        _ => Option::None,
    }
}

#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t\r\n]+")]
#[logos(skip r"//[^\n\r]*")]
#[logos(skip r"/\*[^*]*\*+(?:[^/*][^*]*\*+)*/")]
enum RawToken {
    // Multi-char operators (must come before single-char rules)
    #[token("<>")]
    #[token("!=")]
    NotEq,

    #[token("<=")]
    LtEq,

    #[token(">=")]
    GtEq,

    #[token("..")]
    DotDot,

    #[token("+=")]
    PlusEq,

    #[token("=~")]
    RegexMatch,

    // Single-char operators and punctuation
    #[token("+")]
    Plus,

    #[token("-")]
    Minus,

    #[token("*")]
    Star,

    #[token("/")]
    Slash,

    #[token("%")]
    Percent,

    #[token("=")]
    Eq,

    #[token("<")]
    Lt,

    #[token(">")]
    Gt,

    #[token("(")]
    LParen,

    #[token(")")]
    RParen,

    #[token("[")]
    LBracket,

    #[token("]")]
    RBracket,

    #[token("{")]
    LBrace,

    #[token("}")]
    RBrace,

    #[token(".")]
    Dot,

    #[token(",")]
    Comma,

    #[token(":")]
    Colon,

    #[token(";")]
    Semicolon,

    #[token("|")]
    Pipe,

    #[regex(r"0[xX][0-9A-Fa-f]+", lex_hex_integer)]
    HexInteger(i64),

    #[regex(r"[0-9]+\.[0-9]+([eE][+-]?[0-9]+)?", lex_float)]
    Float(f64),

    #[regex(r"[0-9]+", lex_integer)]
    Integer(i64),

    #[regex(r#""([^"\\]|\\.)*""#, lex_double_quoted_string)]
    DoubleQuotedString(String),

    #[regex(r"'([^'\\]|\\.)*'", lex_single_quoted_string)]
    SingleQuotedString(String),

    #[regex(r"\$[A-Za-z_][A-Za-z0-9_]*", lex_named_param)]
    NamedParam(String),

    #[regex(r"\$[0-9]+", lex_numeric_param)]
    NumericParam(String),

    #[regex(r"\$\{[A-Za-z_][A-Za-z0-9_]*\}", lex_braced_param)]
    BracedParam(String),

    #[regex(r"`[^`]*`", lex_backtick_ident)]
    BacktickIdent(String),

    #[regex(r"[A-Za-z_][A-Za-z0-9_]*")]
    Ident,
}

fn lex_integer(lex: &mut logos::Lexer<RawToken>) -> Result<i64, ()> {
    lex.slice().parse().map_err(|_| ())
}

fn lex_hex_integer(lex: &mut logos::Lexer<RawToken>) -> Result<i64, ()> {
    i64::from_str_radix(&lex.slice()[2..], 16).map_err(|_| ())
}

fn lex_float(lex: &mut logos::Lexer<RawToken>) -> Result<f64, ()> {
    lex.slice().parse().map_err(|_| ())
}

fn unescape_string(s: &str) -> String {
    let inner = &s[1..s.len() - 1];
    let mut result = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('t') => result.push('\t'),
                Some('b') => result.push('\u{0008}'),
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('f') => result.push('\u{000C}'),
                Some('\'') => result.push('\''),
                Some('"') => result.push('"'),
                Some('\\') => result.push('\\'),
                Some('u') => {
                    let hex: String = chars.by_ref().take(4).collect();
                    if hex.len() == 4 {
                        if let Ok(cp) = u32::from_str_radix(&hex, 16) {
                            if let Some(ch) = char::from_u32(cp) {
                                result.push(ch);
                                continue;
                            }
                        }
                    }
                    result.push('\\');
                    result.push('u');
                    result.push_str(&hex);
                }
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                Option::None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn lex_double_quoted_string(lex: &mut logos::Lexer<RawToken>) -> String {
    unescape_string(lex.slice())
}

fn lex_single_quoted_string(lex: &mut logos::Lexer<RawToken>) -> String {
    unescape_string(lex.slice())
}

fn lex_named_param(lex: &mut logos::Lexer<RawToken>) -> String {
    lex.slice()[1..].to_string()
}

fn lex_numeric_param(lex: &mut logos::Lexer<RawToken>) -> String {
    lex.slice()[1..].to_string()
}

fn lex_braced_param(lex: &mut logos::Lexer<RawToken>) -> String {
    let s = lex.slice();
    s[2..s.len() - 1].to_string()
}

fn lex_backtick_ident(lex: &mut logos::Lexer<RawToken>) -> String {
    let s = lex.slice();
    s[1..s.len() - 1].to_string()
}

pub struct Lexer<'src> {
    inner: logos::Lexer<'src, RawToken>,
    source: &'src str,
    peeked: Option<Option<(Token, Span)>>,
}

impl<'src> Lexer<'src> {
    pub fn new(input: &'src str) -> Self {
        Lexer {
            inner: RawToken::lexer(input),
            source: input,
            peeked: Option::None,
        }
    }

    pub fn next_token(&mut self) -> Option<(Token, Span)> {
        if let Some(peeked) = self.peeked.take() {
            return peeked;
        }
        self.advance()
    }

    pub fn peek(&mut self) -> Option<&Token> {
        if self.peeked.is_none() {
            self.peeked = Some(self.advance());
        }
        self.peeked.as_ref().unwrap().as_ref().map(|(t, _)| t)
    }

    fn advance(&mut self) -> Option<(Token, Span)> {
        loop {
            let raw = self.inner.next()?;
            let span = self.inner.span();
            let token = match raw {
                Ok(raw_tok) => self.convert(raw_tok, span.start, span.end),
                Err(()) => continue,
            };
            return Some((token, (span.start, span.end)));
        }
    }

    fn convert(&self, raw: RawToken, start: usize, end: usize) -> Token {
        match raw {
            RawToken::NotEq => Token::NotEq,
            RawToken::LtEq => Token::LtEq,
            RawToken::GtEq => Token::GtEq,
            RawToken::DotDot => Token::DotDot,
            RawToken::PlusEq => Token::PlusEq,
            RawToken::RegexMatch => Token::RegexMatch,
            RawToken::Plus => Token::Plus,
            RawToken::Minus => Token::Minus,
            RawToken::Star => Token::Star,
            RawToken::Slash => Token::Slash,
            RawToken::Percent => Token::Percent,
            RawToken::Eq => Token::Eq,
            RawToken::Lt => Token::Lt,
            RawToken::Gt => Token::Gt,
            RawToken::LParen => Token::LParen,
            RawToken::RParen => Token::RParen,
            RawToken::LBracket => Token::LBracket,
            RawToken::RBracket => Token::RBracket,
            RawToken::LBrace => Token::LBrace,
            RawToken::RBrace => Token::RBrace,
            RawToken::Dot => Token::Dot,
            RawToken::Comma => Token::Comma,
            RawToken::Colon => Token::Colon,
            RawToken::Semicolon => Token::Semicolon,
            RawToken::Pipe => Token::Pipe,
            RawToken::HexInteger(n) | RawToken::Integer(n) => Token::Integer(n),
            RawToken::Float(n) => Token::Float(n),
            RawToken::DoubleQuotedString(s) | RawToken::SingleQuotedString(s) => {
                Token::StringLit(s)
            }
            RawToken::NamedParam(s) | RawToken::NumericParam(s) | RawToken::BracedParam(s) => {
                Token::Parameter(s)
            }
            RawToken::BacktickIdent(s) => Token::BacktickIdent(s),
            RawToken::Ident => {
                let text = &self.source[start..end];
                match keyword_lookup(text) {
                    Some(kw) => kw,
                    Option::None => Token::Identifier(text.to_string()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keywords_are_case_insensitive() {
        let mut lex = Lexer::new("MATCH match Match");
        assert_eq!(lex.next_token().unwrap().0, Token::Match);
        assert_eq!(lex.next_token().unwrap().0, Token::Match);
        assert_eq!(lex.next_token().unwrap().0, Token::Match);
        assert!(lex.next_token().is_none());
    }

    #[test]
    fn identifiers() {
        let mut lex = Lexer::new("foo bar_123 _baz");
        assert_eq!(lex.next_token().unwrap().0, Token::Identifier("foo".into()));
        assert_eq!(
            lex.next_token().unwrap().0,
            Token::Identifier("bar_123".into())
        );
        assert_eq!(
            lex.next_token().unwrap().0,
            Token::Identifier("_baz".into())
        );
    }

    #[test]
    fn backtick_identifiers() {
        let mut lex = Lexer::new("`some identifier`");
        assert_eq!(
            lex.next_token().unwrap().0,
            Token::BacktickIdent("some identifier".into())
        );
    }

    #[test]
    fn integer_literals() {
        let mut lex = Lexer::new("42 0xFF 0");
        assert_eq!(lex.next_token().unwrap().0, Token::Integer(42));
        assert_eq!(lex.next_token().unwrap().0, Token::Integer(255));
        assert_eq!(lex.next_token().unwrap().0, Token::Integer(0));
    }

    #[test]
    fn float_literals() {
        let mut lex = Lexer::new("3.14 1.0e10");
        #[allow(clippy::approx_constant)]
        let expected = Token::Float(3.14);
        assert_eq!(lex.next_token().unwrap().0, expected);
        assert_eq!(lex.next_token().unwrap().0, Token::Float(1.0e10));
    }

    #[test]
    fn string_literals() {
        let mut lex = Lexer::new(r#""hello" 'world' "esc\nape""#);
        assert_eq!(
            lex.next_token().unwrap().0,
            Token::StringLit("hello".into())
        );
        assert_eq!(
            lex.next_token().unwrap().0,
            Token::StringLit("world".into())
        );
        assert_eq!(
            lex.next_token().unwrap().0,
            Token::StringLit("esc\nape".into())
        );
    }

    #[test]
    fn parameters() {
        let mut lex = Lexer::new("$name $0 ${braced}");
        assert_eq!(lex.next_token().unwrap().0, Token::Parameter("name".into()));
        assert_eq!(lex.next_token().unwrap().0, Token::Parameter("0".into()));
        assert_eq!(
            lex.next_token().unwrap().0,
            Token::Parameter("braced".into())
        );
    }

    #[test]
    fn operators() {
        let mut lex = Lexer::new("+ - * / % = <> < > <= >= =~ +=");
        assert_eq!(lex.next_token().unwrap().0, Token::Plus);
        assert_eq!(lex.next_token().unwrap().0, Token::Minus);
        assert_eq!(lex.next_token().unwrap().0, Token::Star);
        assert_eq!(lex.next_token().unwrap().0, Token::Slash);
        assert_eq!(lex.next_token().unwrap().0, Token::Percent);
        assert_eq!(lex.next_token().unwrap().0, Token::Eq);
        assert_eq!(lex.next_token().unwrap().0, Token::NotEq);
        assert_eq!(lex.next_token().unwrap().0, Token::Lt);
        assert_eq!(lex.next_token().unwrap().0, Token::Gt);
        assert_eq!(lex.next_token().unwrap().0, Token::LtEq);
        assert_eq!(lex.next_token().unwrap().0, Token::GtEq);
        assert_eq!(lex.next_token().unwrap().0, Token::RegexMatch);
        assert_eq!(lex.next_token().unwrap().0, Token::PlusEq);
    }

    #[test]
    fn punctuation() {
        let mut lex = Lexer::new("()[]{}..,;:|");
        assert_eq!(lex.next_token().unwrap().0, Token::LParen);
        assert_eq!(lex.next_token().unwrap().0, Token::RParen);
        assert_eq!(lex.next_token().unwrap().0, Token::LBracket);
        assert_eq!(lex.next_token().unwrap().0, Token::RBracket);
        assert_eq!(lex.next_token().unwrap().0, Token::LBrace);
        assert_eq!(lex.next_token().unwrap().0, Token::RBrace);
        assert_eq!(lex.next_token().unwrap().0, Token::DotDot);
        assert_eq!(lex.next_token().unwrap().0, Token::Comma);
        assert_eq!(lex.next_token().unwrap().0, Token::Semicolon);
        assert_eq!(lex.next_token().unwrap().0, Token::Colon);
        assert_eq!(lex.next_token().unwrap().0, Token::Pipe);
    }

    #[test]
    fn comments_are_skipped() {
        let mut lex = Lexer::new("MATCH // this is a comment\n(n)");
        assert_eq!(lex.next_token().unwrap().0, Token::Match);
        assert_eq!(lex.next_token().unwrap().0, Token::LParen);
        assert_eq!(lex.next_token().unwrap().0, Token::Identifier("n".into()));
        assert_eq!(lex.next_token().unwrap().0, Token::RParen);
    }

    #[test]
    fn block_comments_are_skipped() {
        let mut lex = Lexer::new("MATCH /* block comment */ (n)");
        assert_eq!(lex.next_token().unwrap().0, Token::Match);
        assert_eq!(lex.next_token().unwrap().0, Token::LParen);
        assert_eq!(lex.next_token().unwrap().0, Token::Identifier("n".into()));
        assert_eq!(lex.next_token().unwrap().0, Token::RParen);
    }

    #[test]
    fn span_tracking() {
        let mut lex = Lexer::new("MATCH (n)");
        let (tok, span) = lex.next_token().unwrap();
        assert_eq!(tok, Token::Match);
        assert_eq!(span, (0, 5));

        let (tok, span) = lex.next_token().unwrap();
        assert_eq!(tok, Token::LParen);
        assert_eq!(span, (6, 7));
    }

    #[test]
    fn peek_does_not_consume() {
        let mut lex = Lexer::new("MATCH RETURN");
        assert_eq!(lex.peek(), Some(&Token::Match));
        assert_eq!(lex.peek(), Some(&Token::Match));
        assert_eq!(lex.next_token().unwrap().0, Token::Match);
        assert_eq!(lex.next_token().unwrap().0, Token::Return);
    }

    #[test]
    fn full_gql_query() {
        let mut lex =
            Lexer::new("MATCH (n:Person {name: 'Alice'})-[r:KNOWS]->(m) RETURN n.name, m.name");
        let tokens: Vec<Token> = std::iter::from_fn(|| lex.next_token().map(|(t, _)| t)).collect();
        assert_eq!(tokens[0], Token::Match);
        assert_eq!(tokens[1], Token::LParen);
        assert_eq!(tokens[2], Token::Identifier("n".into()));
        assert_eq!(tokens[3], Token::Colon);
        assert_eq!(tokens[4], Token::Identifier("Person".into()));
        assert_eq!(tokens[5], Token::LBrace);
        assert_eq!(tokens[6], Token::Identifier("name".into()));
        assert_eq!(tokens[7], Token::Colon);
        assert_eq!(tokens[8], Token::StringLit("Alice".into()));
        assert_eq!(tokens[9], Token::RBrace);
        assert_eq!(tokens[10], Token::RParen);
        assert_eq!(tokens[11], Token::Minus);
        assert_eq!(tokens[12], Token::LBracket);
        assert_eq!(tokens[13], Token::Identifier("r".into()));
        assert_eq!(tokens[14], Token::Colon);
        assert_eq!(tokens[15], Token::Identifier("KNOWS".into()));
        assert_eq!(tokens[16], Token::RBracket);
        assert_eq!(tokens[17], Token::Minus);
        assert_eq!(tokens[18], Token::Gt);
        assert_eq!(tokens[19], Token::LParen);
        assert_eq!(tokens[20], Token::Identifier("m".into()));
        assert_eq!(tokens[21], Token::RParen);
        assert_eq!(tokens[22], Token::Return);
        assert_eq!(tokens[23], Token::Identifier("n".into()));
        assert_eq!(tokens[24], Token::Dot);
        assert_eq!(tokens[25], Token::Identifier("name".into()));
        assert_eq!(tokens[26], Token::Comma);
        assert_eq!(tokens[27], Token::Identifier("m".into()));
        assert_eq!(tokens[28], Token::Dot);
        assert_eq!(tokens[29], Token::Identifier("name".into()));
    }

    #[test]
    fn unicode_escape_in_string() {
        let mut lex = Lexer::new(r#""\u0041""#);
        assert_eq!(lex.next_token().unwrap().0, Token::StringLit("A".into()));
    }

    #[test]
    fn not_eq_via_exclamation() {
        let mut lex = Lexer::new("!=");
        assert_eq!(lex.next_token().unwrap().0, Token::NotEq);
    }
}
