use crate::ast::*;
use crate::lexer::Token;

#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
    pub position: usize,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "parse error at position {}: {}",
            self.position, self.message
        )
    }
}

impl std::error::Error for ParseError {}

pub struct Parser {
    tokens: Vec<(Token, Span)>,
    pos: usize,
}

pub fn parse(input: &str) -> Result<QueryOrUnion, ParseError> {
    let tokens = tokenize(input)?;
    let mut parser = Parser { tokens, pos: 0 };
    let result = parser.parse_stmt()?;
    if !parser.at_end() && !parser.check(&Token::Semicolon) {
        return Err(parser.error("unexpected token after query"));
    }
    Ok(result)
}

fn tokenize(input: &str) -> Result<Vec<(Token, Span)>, ParseError> {
    use crate::lexer::Lexer;
    let mut lexer = Lexer::new(input);
    let mut tokens = Vec::new();
    while let Some((tok, span)) = lexer.next_token() {
        tokens.push((tok, span));
    }
    Ok(tokens)
}

impl Parser {
    fn at_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos).map(|(t, _)| t)
    }

    fn current_pos(&self) -> usize {
        self.tokens.get(self.pos).map(|(_, s)| s.0).unwrap_or(0)
    }

    fn advance(&mut self) -> (Token, Span) {
        let tok = self.tokens[self.pos].clone();
        self.pos += 1;
        tok
    }

    fn check(&self, expected: &Token) -> bool {
        match self.peek() {
            Some(tok) => std::mem::discriminant(tok) == std::mem::discriminant(expected),
            None => false,
        }
    }

    fn check_keyword(&self, expected: &Token) -> bool {
        self.peek() == Some(expected)
    }

    fn eat(&mut self, expected: &Token) -> bool {
        if self.check(expected) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, expected: &Token) -> Result<(Token, Span), ParseError> {
        if self.check(expected) {
            Ok(self.advance())
        } else {
            Err(self.error(&format!("expected {:?}, got {:?}", expected, self.peek())))
        }
    }

    fn token_to_ident_string(tok: &Token) -> Option<String> {
        match tok {
            Token::Identifier(s) => Some(s.clone()),
            Token::BacktickIdent(s) => Some(s.clone()),
            Token::End => Some("end".to_string()),
            Token::Asc => Some("asc".to_string()),
            Token::Ascending => Some("ascending".to_string()),
            Token::Desc => Some("desc".to_string()),
            Token::Descending => Some("descending".to_string()),
            Token::Order => Some("order".to_string()),
            Token::By => Some("by".to_string()),
            Token::Count => Some("count".to_string()),
            Token::Coalesce => Some("coalesce".to_string()),
            Token::Contains => Some("contains".to_string()),
            Token::Starts => Some("starts".to_string()),
            Token::Ends => Some("ends".to_string()),
            Token::Exists => Some("exists".to_string()),
            Token::Pattern => Some("pattern".to_string()),
            Token::All => Some("all".to_string()),
            Token::Any => Some("any".to_string()),
            Token::None => Some("none".to_string()),
            Token::Single => Some("single".to_string()),
            Token::Reduce => Some("reduce".to_string()),
            Token::Call => Some("call".to_string()),
            Token::Yield => Some("yield".to_string()),
            Token::Case => Some("case".to_string()),
            Token::On => Some("on".to_string()),
            Token::From => Some("from".to_string()),
            Token::For => Some("for".to_string()),
            _ => Option::None,
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        match self.peek().and_then(Self::token_to_ident_string) {
            Some(s) => {
                self.advance();
                Ok(s)
            }
            _ => Err(self.error(&format!("expected identifier, got {:?}", self.peek()))),
        }
    }

    fn is_ident(&self) -> bool {
        self.peek()
            .map(|tok| Self::token_to_ident_string(tok).is_some())
            .unwrap_or(false)
    }

    fn token_is_ident(tok: &Token) -> bool {
        Self::token_to_ident_string(tok).is_some()
    }

    fn error(&self, msg: &str) -> ParseError {
        ParseError {
            message: msg.to_string(),
            position: self.current_pos(),
        }
    }

    // --- Top-level ---

    fn parse_stmt(&mut self) -> Result<QueryOrUnion, ParseError> {
        let explain = self.eat(&Token::Explain);

        let mut result = self.parse_single_query()?;
        if explain {
            if let QueryOrUnion::Query(ref mut q) = result {
                q.explain = true;
            }
        }

        while self.check_keyword(&Token::Union) {
            self.advance();
            let all = self.eat(&Token::All);
            let right_query = match self.parse_single_query()? {
                QueryOrUnion::Query(q) => q,
                QueryOrUnion::Union(_) => return Err(self.error("unexpected UNION nesting")),
            };
            if explain {
                // propagate explain to the union wrapper
            }
            let union = Union {
                left: Box::new(result),
                right: Box::new(right_query),
                all,
            };
            result = QueryOrUnion::Union(union);
        }

        self.eat(&Token::Semicolon);
        Ok(result)
    }

    fn parse_single_query(&mut self) -> Result<QueryOrUnion, ParseError> {
        let mut clauses = Vec::new();
        loop {
            if self.at_end() || self.check(&Token::Semicolon) || self.check_keyword(&Token::Union) {
                break;
            }
            let clause = self.parse_clause()?;
            clauses.push(clause);
        }
        if clauses.is_empty() {
            return Err(self.error("query must have at least one clause"));
        }
        Ok(QueryOrUnion::Query(Query {
            clauses,
            explain: false,
        }))
    }

    fn parse_clause(&mut self) -> Result<Clause, ParseError> {
        match self.peek() {
            Some(Token::Match) => self.parse_match(false),
            Some(Token::Optional) => {
                self.advance();
                self.expect(&Token::Match)?;
                self.parse_match_inner(true)
            }
            Some(Token::Return) => self.parse_return(),
            Some(Token::Create) => self.parse_create(),
            Some(Token::Set) => self.parse_set(),
            Some(Token::Delete) => self.parse_delete(false),
            Some(Token::Detach) => {
                self.advance();
                self.parse_delete(true)
            }
            Some(Token::With) => self.parse_with(),
            Some(Token::For) => self.parse_for(),
            _ => Err(self.error(&format!("expected clause keyword, got {:?}", self.peek()))),
        }
    }

    // --- MATCH ---

    fn parse_match(&mut self, optional: bool) -> Result<Clause, ParseError> {
        self.expect(&Token::Match)?;
        self.parse_match_inner(optional)
    }

    fn parse_match_inner(&mut self, optional: bool) -> Result<Clause, ParseError> {
        let pattern = self.parse_pattern_list()?;

        let from_graph = if self.check_keyword(&Token::From) {
            self.advance();
            Some(self.expect_ident()?)
        } else {
            Option::None
        };

        let where_expr = self.parse_where_opt()?;

        Ok(Clause::Match(MatchClause {
            pattern,
            where_expr,
            optional,
            from_graph,
        }))
    }

    // --- RETURN ---

    fn parse_return(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::Return)?;
        let distinct = self.eat(&Token::Distinct);
        let items = self.parse_return_items()?;
        let order_by = self.parse_order_by_opt()?;
        let skip = self.parse_skip_opt()?;
        let limit = self.parse_limit_opt()?;

        Ok(Clause::Return(ReturnClause {
            distinct,
            items,
            order_by,
            skip,
            limit,
        }))
    }

    // --- WITH ---

    fn parse_with(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::With)?;

        // WITH HEADERS is part of LOAD CSV, not WITH clause
        // But WITH as a clause has DISTINCT
        let distinct = self.eat(&Token::Distinct);
        let items = self.parse_return_items()?;
        let order_by = self.parse_order_by_opt()?;
        let skip = self.parse_skip_opt()?;
        let limit = self.parse_limit_opt()?;
        let where_expr = self.parse_where_opt()?;

        Ok(Clause::With(WithClause {
            distinct,
            items,
            order_by,
            skip,
            limit,
            where_expr,
        }))
    }

    // --- FOR (GQL: FOR variable IN expr) ---

    fn parse_for(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::For)?;
        let variable = self.expect_ident()?;
        self.expect(&Token::In)?;
        let list_expr = self.parse_expr(0)?;
        Ok(Clause::For(ForClause {
            variable,
            list_expr: Box::new(list_expr),
        }))
    }

    // --- CREATE ---

    fn parse_create(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::Create)?;
        let pattern = self.parse_pattern_list()?;
        Ok(Clause::Create(CreateClause { pattern }))
    }

    // --- SET ---

    fn parse_set(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::Set)?;
        let items = self.parse_set_items()?;
        Ok(Clause::Set(SetClause { items }))
    }

    fn parse_set_items(&mut self) -> Result<Vec<SetItem>, ParseError> {
        let mut items = vec![self.parse_set_item()?];
        while self.eat(&Token::Comma) {
            items.push(self.parse_set_item()?);
        }
        Ok(items)
    }

    fn parse_set_item(&mut self) -> Result<SetItem, ParseError> {
        // SET n:Label | SET n.prop = expr | SET n += expr
        // We need lookahead: if ident : ident, it's a label set
        if self.is_ident()
            && self
                .peek_at(1)
                .map(|t| matches!(t, Token::Colon))
                .unwrap_or(false)
        {
            // Check if this is ident:ident (label set) vs ident:something-else
            if let Some(tok2) = self.peek_at(2) {
                if Self::token_is_ident(tok2) {
                    let var_name = self.expect_ident()?;
                    self.expect(&Token::Colon)?;
                    let label = self.expect_ident()?;
                    let label_expr = Expr::LabelExpr {
                        expr: Box::new(Expr::Identifier(var_name)),
                        label,
                    };
                    return Ok(SetItem {
                        property: Box::new(label_expr.clone()),
                        expr: Box::new(Expr::Literal(Literal::Null)),
                        is_merge: false,
                    });
                }
            }
        }

        // Parse at precedence 7 so '=' (prec 6) is not consumed as comparison
        let target = self.parse_expr(7)?;

        if self.eat(&Token::PlusEq) {
            let expr = self.parse_expr(0)?;
            Ok(SetItem {
                property: Box::new(target),
                expr: Box::new(expr),
                is_merge: true,
            })
        } else {
            self.expect(&Token::Eq)?;
            let expr = self.parse_expr(0)?;
            Ok(SetItem {
                property: Box::new(target),
                expr: Box::new(expr),
                is_merge: false,
            })
        }
    }

    // --- DELETE ---

    fn parse_delete(&mut self, detach: bool) -> Result<Clause, ParseError> {
        self.expect(&Token::Delete)?;
        let mut items = vec![self.expect_ident()?];
        while self.eat(&Token::Comma) {
            items.push(self.expect_ident()?);
        }
        Ok(Clause::Delete(DeleteClause { items, detach }))
    }

    // --- Return items, order by, skip, limit, where ---

    fn parse_return_items(&mut self) -> Result<Vec<ReturnItem>, ParseError> {
        // Handle RETURN *
        if self.check(&Token::Star) {
            self.advance();
            return Ok(vec![ReturnItem {
                expr: Expr::Identifier("*".to_string()),
                alias: Option::None,
            }]);
        }

        let mut items = vec![self.parse_return_item()?];
        while self.eat(&Token::Comma) {
            items.push(self.parse_return_item()?);
        }
        Ok(items)
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, ParseError> {
        let expr = self.parse_expr(0)?;
        let alias = if self.eat(&Token::As) {
            Some(self.expect_ident()?)
        } else {
            Option::None
        };
        Ok(ReturnItem { expr, alias })
    }

    fn parse_order_by_opt(&mut self) -> Result<Vec<OrderByItem>, ParseError> {
        if !self.check_keyword(&Token::Order) {
            return Ok(Vec::new());
        }
        self.advance(); // ORDER
        self.expect(&Token::By)?;

        let mut items = vec![self.parse_order_by_item()?];
        while self.eat(&Token::Comma) {
            items.push(self.parse_order_by_item()?);
        }
        Ok(items)
    }

    fn parse_order_by_item(&mut self) -> Result<OrderByItem, ParseError> {
        let expr = self.parse_expr(0)?;
        let descending =
            if self.check_keyword(&Token::Desc) || self.check_keyword(&Token::Descending) {
                self.advance();
                true
            } else {
                if self.check_keyword(&Token::Asc) || self.check_keyword(&Token::Ascending) {
                    self.advance();
                }
                false
            };
        Ok(OrderByItem { expr, descending })
    }

    fn parse_skip_opt(&mut self) -> Result<Option<Box<Expr>>, ParseError> {
        if !self.check_keyword(&Token::Skip) {
            return Ok(Option::None);
        }
        self.advance();
        let expr = self.parse_expr(0)?;
        Ok(Some(Box::new(expr)))
    }

    fn parse_limit_opt(&mut self) -> Result<Option<Box<Expr>>, ParseError> {
        if !self.check_keyword(&Token::Limit) {
            return Ok(Option::None);
        }
        self.advance();
        let expr = self.parse_expr(0)?;
        Ok(Some(Box::new(expr)))
    }

    fn parse_where_opt(&mut self) -> Result<Option<Box<Expr>>, ParseError> {
        if !self.check_keyword(&Token::Where) {
            return Ok(Option::None);
        }
        self.advance();
        let expr = self.parse_expr(0)?;
        Ok(Some(Box::new(expr)))
    }

    // --- Pattern parsing ---

    fn parse_pattern_list(&mut self) -> Result<Vec<Path>, ParseError> {
        let mut paths = vec![self.parse_path()?];
        while self.eat(&Token::Comma) {
            paths.push(self.parse_path()?);
        }
        Ok(paths)
    }

    fn parse_path(&mut self) -> Result<Path, ParseError> {
        // Check for: ident = path, shortestPath(...), allShortestPaths(...)
        // or just simple_path

        if self.check_keyword(&Token::ShortestPath) {
            return self.parse_shortest_path(Option::None, PathType::Shortest);
        }
        if self.check_keyword(&Token::AllShortestPaths) {
            return self.parse_shortest_path(Option::None, PathType::AllShortest);
        }

        // ident = path OR ident = shortestPath(...)
        if self.is_ident() && self.peek_at(1) == Some(&Token::Eq) {
            let var_name = self.expect_ident()?;
            self.expect(&Token::Eq)?;

            if self.check_keyword(&Token::ShortestPath) {
                return self.parse_shortest_path(Some(var_name), PathType::Shortest);
            }
            if self.check_keyword(&Token::AllShortestPaths) {
                return self.parse_shortest_path(Some(var_name), PathType::AllShortest);
            }

            // regular path with variable assignment
            let mut path = self.parse_simple_path()?;
            path.variable = Some(var_name);
            return Ok(path);
        }

        self.parse_simple_path()
    }

    fn parse_shortest_path(
        &mut self,
        variable: Option<String>,
        path_type: PathType,
    ) -> Result<Path, ParseError> {
        self.advance(); // shortestPath or allShortestPaths
        self.expect(&Token::LParen)?;
        let mut path = self.parse_simple_path()?;
        self.expect(&Token::RParen)?;
        path.variable = variable;
        path.path_type = path_type;
        Ok(path)
    }

    fn parse_simple_path(&mut self) -> Result<Path, ParseError> {
        let mut elements = Vec::new();
        let node = self.parse_node_pattern()?;
        elements.push(PathElement::Node(node));

        while self.is_rel_start() {
            let rel = self.parse_rel_pattern()?;
            elements.push(PathElement::Rel(rel));
            let node = self.parse_node_pattern()?;
            elements.push(PathElement::Node(node));
        }

        Ok(Path {
            elements,
            variable: Option::None,
            path_type: PathType::Normal,
        })
    }

    fn parse_node_pattern(&mut self) -> Result<NodePattern, ParseError> {
        self.expect(&Token::LParen)?;

        let variable = if self.is_ident() && !self.check(&Token::Colon) {
            Some(self.expect_ident()?)
        } else {
            Option::None
        };

        let labels = self.parse_labels()?;
        let properties = self.parse_properties_opt()?;

        let where_expr = if self.check_keyword(&Token::Where) {
            self.advance();
            Some(Box::new(self.parse_expr(0)?))
        } else {
            Option::None
        };

        self.expect(&Token::RParen)?;

        Ok(NodePattern {
            variable,
            labels,
            properties,
            where_expr,
        })
    }

    fn parse_labels(&mut self) -> Result<Vec<String>, ParseError> {
        let mut labels = Vec::new();
        while self.check(&Token::Colon) {
            self.advance(); // ':'
            labels.push(self.expect_ident()?);
        }
        Ok(labels)
    }

    fn is_rel_start(&self) -> bool {
        match self.peek() {
            Some(Token::Minus) => true,
            Some(Token::Lt) => {
                // <-[...]-
                self.peek_at(1) == Some(&Token::Minus)
            }
            _ => false,
        }
    }

    fn parse_rel_pattern(&mut self) -> Result<RelPattern, ParseError> {
        // Determine direction by looking at pattern:
        // -[...]->  outgoing (Right)
        // <-[...]-  incoming (Left)
        // -[...]-   undirected (None/Both)

        let incoming_start = if self.check(&Token::Lt) {
            self.advance(); // '<'
            true
        } else {
            false
        };

        self.expect(&Token::Minus)?;
        self.expect(&Token::LBracket)?;

        let variable = if self.is_ident() && !self.check(&Token::Colon) && !self.check(&Token::Star)
        {
            Some(self.expect_ident()?)
        } else {
            Option::None
        };

        let rel_types = if self.eat(&Token::Colon) {
            self.parse_rel_types()?
        } else {
            Vec::new()
        };

        let varlen = self.parse_varlen_range()?;
        let properties = self.parse_properties_opt()?;

        let where_expr = if self.check_keyword(&Token::Where) {
            self.advance();
            Some(Box::new(self.parse_expr(0)?))
        } else {
            Option::None
        };

        self.expect(&Token::RBracket)?;
        self.expect(&Token::Minus)?;

        let outgoing_end = self.eat(&Token::Gt);

        let direction = if incoming_start && !outgoing_end {
            Direction::Left
        } else if !incoming_start && outgoing_end {
            Direction::Right
        } else if incoming_start && outgoing_end {
            Direction::Both
        } else {
            Direction::None
        };

        Ok(RelPattern {
            variable,
            rel_types,
            properties,
            where_expr,
            direction,
            varlen,
        })
    }

    fn parse_rel_types(&mut self) -> Result<Vec<String>, ParseError> {
        let mut types = vec![self.expect_ident()?];
        while self.eat(&Token::Pipe) {
            // optional colon before type name: [:TYPE1|:TYPE2]
            self.eat(&Token::Colon);
            types.push(self.expect_ident()?);
        }
        Ok(types)
    }

    fn parse_varlen_range(&mut self) -> Result<Option<VarLenRange>, ParseError> {
        if !self.eat(&Token::Star) {
            return Ok(Option::None);
        }

        // *  => unbounded (1..inf)
        // *N => exact
        // *N..M => range
        // *N.. => min only
        // *..M => max only

        let has_int = matches!(self.peek(), Some(Token::Integer(_)));
        let has_dotdot = self.check(&Token::DotDot);

        if !has_int && !has_dotdot {
            return Ok(Some(VarLenRange {
                min_hops: Some(1),
                max_hops: Option::None,
            }));
        }

        if has_dotdot && !has_int {
            // *..M
            self.advance(); // DotDot
            let max = self.expect_integer()?;
            return Ok(Some(VarLenRange {
                min_hops: Some(1),
                max_hops: Some(max as u32),
            }));
        }

        let min = self.expect_integer()?;
        if self.eat(&Token::DotDot) {
            if matches!(self.peek(), Some(Token::Integer(_))) {
                let max = self.expect_integer()?;
                Ok(Some(VarLenRange {
                    min_hops: Some(min as u32),
                    max_hops: Some(max as u32),
                }))
            } else {
                Ok(Some(VarLenRange {
                    min_hops: Some(min as u32),
                    max_hops: Option::None,
                }))
            }
        } else {
            Ok(Some(VarLenRange {
                min_hops: Some(min as u32),
                max_hops: Some(min as u32),
            }))
        }
    }

    fn expect_integer(&mut self) -> Result<i64, ParseError> {
        match self.advance().0 {
            Token::Integer(n) => Ok(n),
            other => Err(self.error(&format!("expected integer, got {other:?}"))),
        }
    }

    fn parse_properties_opt(&mut self) -> Result<Option<Box<Expr>>, ParseError> {
        if !self.check(&Token::LBrace) {
            return Ok(Option::None);
        }
        let map = self.parse_map_literal()?;
        match map {
            Expr::Map(ref pairs) if pairs.is_empty() => Ok(Option::None),
            _ => Ok(Some(Box::new(map))),
        }
    }

    // --- Expression parsing (Pratt parser) ---

    fn parse_expr(&mut self, min_prec: u8) -> Result<Expr, ParseError> {
        let mut left = self.parse_prefix()?;

        loop {
            if self.at_end() {
                break;
            }

            // Postfix: IS NULL, IS NOT NULL
            if self.check_keyword(&Token::Is) {
                let prec = 4; // IN/IS precedence
                if prec < min_prec {
                    break;
                }
                self.advance(); // IS
                let is_not_null = if self.check_keyword(&Token::Not) {
                    self.advance();
                    self.expect(&Token::Null)?;
                    true
                } else {
                    self.expect(&Token::Null)?;
                    false
                };
                left = Expr::NullCheck {
                    expr: Box::new(left),
                    is_not_null,
                };
                continue;
            }

            // IN operator
            if self.check_keyword(&Token::In) {
                let prec = 4;
                if prec < min_prec {
                    break;
                }
                self.advance();
                let right = self.parse_expr(prec + 1)?;
                left = Expr::BinaryOp {
                    op: BinaryOp::In,
                    left: Box::new(left),
                    right: Box::new(right),
                };
                continue;
            }

            // STARTS WITH, ENDS WITH, CONTAINS
            if self.check_keyword(&Token::Starts) {
                let prec = 6;
                if prec < min_prec {
                    break;
                }
                self.advance(); // STARTS
                self.expect(&Token::With)?;
                let right = self.parse_expr(prec + 1)?;
                left = Expr::BinaryOp {
                    op: BinaryOp::StartsWith,
                    left: Box::new(left),
                    right: Box::new(right),
                };
                continue;
            }
            if self.check_keyword(&Token::Ends) {
                let prec = 6;
                if prec < min_prec {
                    break;
                }
                self.advance(); // ENDS
                self.expect(&Token::With)?;
                let right = self.parse_expr(prec + 1)?;
                left = Expr::BinaryOp {
                    op: BinaryOp::EndsWith,
                    left: Box::new(left),
                    right: Box::new(right),
                };
                continue;
            }
            if self.check_keyword(&Token::Contains) {
                let prec = 6;
                if prec < min_prec {
                    break;
                }
                self.advance();
                let right = self.parse_expr(prec + 1)?;
                left = Expr::BinaryOp {
                    op: BinaryOp::Contains,
                    left: Box::new(left),
                    right: Box::new(right),
                };
                continue;
            }

            // Label check: expr:Label (e.g., n:Person in WHERE)
            if self.check(&Token::Colon) {
                let prec = 11;
                if prec < min_prec {
                    break;
                }
                self.advance(); // ':'
                let label = self.expect_ident()?;
                left = Expr::LabelExpr {
                    expr: Box::new(left),
                    label,
                };
                continue;
            }

            // Property access: expr.prop
            if self.check(&Token::Dot) {
                let prec = 12;
                if prec < min_prec {
                    break;
                }
                self.advance();
                let name = self.expect_ident()?;
                left = Expr::Property {
                    expr: Box::new(left),
                    name,
                };
                continue;
            }

            // Subscript: expr[index]
            if self.check(&Token::LBracket) {
                let prec = 12;
                if prec < min_prec {
                    break;
                }
                self.advance(); // '['
                let index = self.parse_expr(0)?;
                self.expect(&Token::RBracket)?;
                left = Expr::Subscript {
                    expr: Box::new(left),
                    index: Box::new(index),
                };
                continue;
            }

            // Binary operators
            if let Some((op, prec, right_assoc)) = self.peek_binop() {
                if prec < min_prec {
                    break;
                }
                self.advance_binop(&op);
                let next_prec = if right_assoc { prec } else { prec + 1 };
                let right = self.parse_expr(next_prec)?;
                left = Expr::BinaryOp {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                };
                continue;
            }

            break;
        }

        Ok(left)
    }

    fn peek_binop(&self) -> Option<(BinaryOp, u8, bool)> {
        match self.peek()? {
            Token::Or => Some((BinaryOp::Or, 1, false)),
            Token::Xor => Some((BinaryOp::Xor, 2, false)),
            Token::And => Some((BinaryOp::And, 3, false)),
            // Comparison operators (prec 6)
            Token::Eq => Some((BinaryOp::Eq, 6, false)),
            Token::NotEq => Some((BinaryOp::Neq, 6, false)),
            Token::Lt => {
                // Make sure this isn't the start of <-[...]- (incoming rel)
                // In expression context, '<' is always less-than
                Some((BinaryOp::Lt, 6, false))
            }
            Token::Gt => Some((BinaryOp::Gt, 6, false)),
            Token::LtEq => Some((BinaryOp::Lte, 6, false)),
            Token::GtEq => Some((BinaryOp::Gte, 6, false)),
            Token::RegexMatch => Some((BinaryOp::RegexMatch, 6, false)),
            // Additive (prec 8)
            Token::Plus => Some((BinaryOp::Add, 8, false)),
            Token::Minus => Some((BinaryOp::Sub, 8, false)),
            // Multiplicative (prec 9)
            Token::Star => Some((BinaryOp::Mul, 9, false)),
            Token::Slash => Some((BinaryOp::Div, 9, false)),
            Token::Percent => Some((BinaryOp::Mod, 9, false)),
            _ => Option::None,
        }
    }

    fn advance_binop(&mut self, _op: &BinaryOp) {
        self.advance();
    }

    fn parse_prefix(&mut self) -> Result<Expr, ParseError> {
        match self.peek().cloned() {
            Some(Token::Not) => {
                self.advance();
                let expr = self.parse_expr(5)?; // NOT precedence
                Ok(Expr::Not(Box::new(expr)))
            }
            Some(Token::Minus) => {
                self.advance();
                let expr = self.parse_expr(10)?; // Unary minus precedence
                match expr {
                    Expr::Literal(Literal::Integer(n)) => Ok(Expr::Literal(Literal::Integer(-n))),
                    Expr::Literal(Literal::Float(f)) => Ok(Expr::Literal(Literal::Float(-f))),
                    other => Ok(Expr::BinaryOp {
                        op: BinaryOp::Sub,
                        left: Box::new(Expr::Literal(Literal::Integer(0))),
                        right: Box::new(other),
                    }),
                }
            }
            Some(Token::Plus) => {
                self.advance();
                self.parse_expr(10)
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        match self.peek().cloned() {
            Some(Token::Integer(n)) => {
                self.advance();
                Ok(Expr::Literal(Literal::Integer(n)))
            }
            Some(Token::Float(f)) => {
                self.advance();
                Ok(Expr::Literal(Literal::Float(f)))
            }
            Some(Token::StringLit(s)) => {
                self.advance();
                Ok(Expr::Literal(Literal::String(s)))
            }
            Some(Token::True) => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(true)))
            }
            Some(Token::False) => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(false)))
            }
            Some(Token::Null) => {
                self.advance();
                Ok(Expr::Literal(Literal::Null))
            }
            Some(Token::Parameter(name)) => {
                self.advance();
                Ok(Expr::Parameter(name))
            }
            Some(Token::LParen) => {
                self.advance();
                let expr = self.parse_expr(0)?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Some(Token::LBracket) => self.parse_list_or_comprehension(),
            Some(Token::LBrace) => self.parse_map_literal(),
            Some(Token::Case) => self.parse_case(),
            Some(Token::Exists) => self.parse_exists(),
            Some(Token::All) => self.parse_list_predicate(ListPredicateType::All),
            Some(Token::Any) => self.parse_list_predicate(ListPredicateType::Any),
            Some(Token::None) => self.parse_list_predicate(ListPredicateType::None),
            Some(Token::Single) => self.parse_list_predicate(ListPredicateType::Single),
            Some(Token::Reduce) => self.parse_reduce(),
            Some(Token::Count) => {
                self.advance();
                if self.check(&Token::LParen) {
                    self.parse_function_call("count".to_string())
                } else {
                    Ok(Expr::Identifier("count".to_string()))
                }
            }
            Some(Token::Coalesce) => {
                self.advance();
                if self.check(&Token::LParen) {
                    self.parse_function_call("coalesce".to_string())
                } else {
                    Ok(Expr::Identifier("coalesce".to_string()))
                }
            }
            Some(Token::Identifier(_)) | Some(Token::BacktickIdent(_)) | Some(Token::End) => {
                let name = self.expect_ident()?;
                // Check for function call or map projection
                if self.check(&Token::LParen) {
                    self.parse_function_call(name)
                } else if self.check(&Token::LBrace) {
                    self.parse_map_projection(name)
                } else {
                    Ok(Expr::Identifier(name))
                }
            }
            _ => Err(self.error(&format!(
                "unexpected token in expression: {:?}",
                self.peek()
            ))),
        }
    }

    fn parse_function_call(&mut self, name: String) -> Result<Expr, ParseError> {
        self.expect(&Token::LParen)?;

        // func()
        if self.eat(&Token::RParen) {
            return Ok(Expr::FunctionCall {
                name,
                args: Vec::new(),
                distinct: false,
            });
        }

        // func(*)
        if self.check(&Token::Star) {
            self.advance();
            self.expect(&Token::RParen)?;
            return Ok(Expr::FunctionCall {
                name,
                args: Vec::new(), // empty args signals count(*)
                distinct: false,
            });
        }

        // func(DISTINCT expr)
        if self.check_keyword(&Token::Distinct) {
            self.advance();
            let expr = self.parse_expr(0)?;
            self.expect(&Token::RParen)?;
            return Ok(Expr::FunctionCall {
                name,
                args: vec![expr],
                distinct: true,
            });
        }

        // func(expr, expr, ...)
        let mut args = vec![self.parse_expr(0)?];
        while self.eat(&Token::Comma) {
            args.push(self.parse_expr(0)?);
        }
        self.expect(&Token::RParen)?;

        Ok(Expr::FunctionCall {
            name,
            args,
            distinct: false,
        })
    }

    fn parse_exists(&mut self) -> Result<Expr, ParseError> {
        self.advance(); // EXISTS
        self.expect(&Token::LParen)?;

        // EXISTS(n.property) - try property access first
        // EXISTS((pattern)) - pattern
        // We need to determine which. If next is ident followed by '.', it's property.
        // If next is '(' it's a pattern (node pattern start).

        if self.is_ident() {
            let save = self.pos;
            let name = self.expect_ident()?;
            if self.eat(&Token::Dot) {
                let prop = self.expect_ident()?;
                self.expect(&Token::RParen)?;
                return Ok(Expr::Exists(ExistsExpr::Property(Box::new(
                    Expr::Property {
                        expr: Box::new(Expr::Identifier(name)),
                        name: prop,
                    },
                ))));
            }
            // Not a property - backtrack and parse as pattern
            self.pos = save;
        }

        // Parse pattern list
        let pattern = self.parse_pattern_list()?;
        self.expect(&Token::RParen)?;
        Ok(Expr::Exists(ExistsExpr::Pattern(pattern)))
    }

    fn parse_list_predicate(&mut self, pred_type: ListPredicateType) -> Result<Expr, ParseError> {
        self.advance(); // ALL/ANY/NONE/SINGLE
        self.expect(&Token::LParen)?;
        let variable = self.expect_ident()?;
        self.expect(&Token::In)?;
        let list_expr = self.parse_expr(0)?;
        self.expect(&Token::Where)?;
        let predicate = self.parse_expr(0)?;
        self.expect(&Token::RParen)?;
        Ok(Expr::ListPredicate {
            pred_type,
            variable,
            list_expr: Box::new(list_expr),
            predicate: Box::new(predicate),
        })
    }

    fn parse_reduce(&mut self) -> Result<Expr, ParseError> {
        self.advance(); // REDUCE
        self.expect(&Token::LParen)?;
        let accumulator = self.expect_ident()?;
        self.expect(&Token::Eq)?;
        let initial_value = self.parse_expr(0)?;
        self.expect(&Token::Comma)?;
        let variable = self.expect_ident()?;
        self.expect(&Token::In)?;
        let list_expr = self.parse_expr(0)?;
        self.expect(&Token::Pipe)?;
        let expression = self.parse_expr(0)?;
        self.expect(&Token::RParen)?;
        Ok(Expr::Reduce {
            accumulator,
            initial_value: Box::new(initial_value),
            variable,
            list_expr: Box::new(list_expr),
            expression: Box::new(expression),
        })
    }

    fn parse_list_or_comprehension(&mut self) -> Result<Expr, ParseError> {
        self.advance(); // '['

        // Empty list
        if self.eat(&Token::RBracket) {
            return Ok(Expr::List(Vec::new()));
        }

        // Try to detect list comprehension: [ident IN expr ...]
        // or pattern comprehension: [(node)-[rel]->(node) | expr]
        if self.check(&Token::LParen) {
            // Could be pattern comprehension or just a parenthesized expr in a list
            return self.parse_possible_pattern_comprehension();
        }

        if self.is_ident() {
            // Check for list comprehension: [x IN expr ...]
            let save = self.pos;
            let var = self.expect_ident()?;
            if self.check_keyword(&Token::In) {
                self.advance(); // IN
                let list_expr = self.parse_expr(0)?;

                if self.eat(&Token::RBracket) {
                    return Ok(Expr::ListComprehension {
                        variable: var,
                        list_expr: Box::new(list_expr),
                        where_expr: Option::None,
                        transform_expr: Option::None,
                    });
                }

                if self.check_keyword(&Token::Where) {
                    self.advance();
                    let where_expr = self.parse_expr(0)?;
                    if self.eat(&Token::Pipe) {
                        let transform = self.parse_expr(0)?;
                        self.expect(&Token::RBracket)?;
                        return Ok(Expr::ListComprehension {
                            variable: var,
                            list_expr: Box::new(list_expr),
                            where_expr: Some(Box::new(where_expr)),
                            transform_expr: Some(Box::new(transform)),
                        });
                    }
                    self.expect(&Token::RBracket)?;
                    return Ok(Expr::ListComprehension {
                        variable: var,
                        list_expr: Box::new(list_expr),
                        where_expr: Some(Box::new(where_expr)),
                        transform_expr: Option::None,
                    });
                }

                if self.eat(&Token::Pipe) {
                    let transform = self.parse_expr(0)?;
                    self.expect(&Token::RBracket)?;
                    return Ok(Expr::ListComprehension {
                        variable: var,
                        list_expr: Box::new(list_expr),
                        where_expr: Option::None,
                        transform_expr: Some(Box::new(transform)),
                    });
                }

                // Not a comprehension after all - shouldn't happen in valid Cypher
                return Err(self.error("expected WHERE, |, or ] in list comprehension"));
            }

            // Not a list comprehension, backtrack
            self.pos = save;
        }

        // Plain list literal
        let mut items = vec![self.parse_expr(0)?];
        while self.eat(&Token::Comma) {
            items.push(self.parse_expr(0)?);
        }
        self.expect(&Token::RBracket)?;
        Ok(Expr::List(items))
    }

    fn parse_possible_pattern_comprehension(&mut self) -> Result<Expr, ParseError> {
        // We're inside '[' and next is '('
        // Could be pattern comprehension: [(n)-[r]->(m) | expr]
        // or list of parenthesized expressions: [(1+2), 3]
        // Try pattern comprehension first, backtrack on failure
        let save = self.pos;

        if let Ok(result) = self.try_pattern_comprehension() {
            return Ok(result);
        }

        // Backtrack and parse as plain list
        self.pos = save;
        let mut items = vec![self.parse_expr(0)?];
        while self.eat(&Token::Comma) {
            items.push(self.parse_expr(0)?);
        }
        self.expect(&Token::RBracket)?;
        Ok(Expr::List(items))
    }

    fn try_pattern_comprehension(&mut self) -> Result<Expr, ParseError> {
        // Parse: (node) rel_pattern node_pattern [WHERE expr] | expr ]
        let first_node = self.parse_node_pattern()?;
        if !self.is_rel_start() {
            return Err(self.error("expected relationship pattern in pattern comprehension"));
        }
        let rel = self.parse_rel_pattern()?;
        let second_node = self.parse_node_pattern()?;

        let mut elements = vec![
            PathElement::Node(first_node),
            PathElement::Rel(rel),
            PathElement::Node(second_node),
        ];

        // Handle additional rel-node pairs
        while self.is_rel_start() {
            let rel = self.parse_rel_pattern()?;
            elements.push(PathElement::Rel(rel));
            let node = self.parse_node_pattern()?;
            elements.push(PathElement::Node(node));
        }

        let path = Path {
            elements,
            variable: Option::None,
            path_type: PathType::Normal,
        };
        let pattern = vec![path];

        let where_expr = if self.check_keyword(&Token::Where) {
            self.advance();
            Some(Box::new(self.parse_expr(0)?))
        } else {
            Option::None
        };

        self.expect(&Token::Pipe)?;
        let collect_expr = self.parse_expr(0)?;
        self.expect(&Token::RBracket)?;

        Ok(Expr::PatternComprehension {
            pattern,
            where_expr,
            collect_expr: Some(Box::new(collect_expr)),
        })
    }

    fn parse_map_literal(&mut self) -> Result<Expr, ParseError> {
        self.expect(&Token::LBrace)?;
        if self.eat(&Token::RBrace) {
            return Ok(Expr::Map(Vec::new()));
        }

        let mut pairs = vec![self.parse_map_pair()?];
        while self.eat(&Token::Comma) {
            pairs.push(self.parse_map_pair()?);
        }
        self.expect(&Token::RBrace)?;
        Ok(Expr::Map(pairs))
    }

    fn parse_map_pair(&mut self) -> Result<MapPair, ParseError> {
        let key = match self.peek().cloned() {
            Some(Token::StringLit(s)) => {
                self.advance();
                s
            }
            _ => self.expect_ident()?,
        };
        self.expect(&Token::Colon)?;
        let value = self.parse_expr(0)?;
        Ok(MapPair { key, value })
    }

    fn parse_map_projection(&mut self, base_name: String) -> Result<Expr, ParseError> {
        self.expect(&Token::LBrace)?;
        let mut items = vec![self.parse_map_projection_item()?];
        while self.eat(&Token::Comma) {
            items.push(self.parse_map_projection_item()?);
        }
        self.expect(&Token::RBrace)?;
        Ok(Expr::MapProjection {
            base_expr: Box::new(Expr::Identifier(base_name)),
            items,
        })
    }

    fn parse_map_projection_item(&mut self) -> Result<MapProjectionItem, ParseError> {
        if self.eat(&Token::Dot) {
            // .prop or .*
            if self.eat(&Token::Star) {
                return Ok(MapProjectionItem {
                    key: Option::None,
                    property: Some("*".to_string()),
                    expr: Option::None,
                });
            }
            let prop = self.expect_ident()?;
            Ok(MapProjectionItem {
                key: Some(prop.clone()),
                property: Some(prop),
                expr: Option::None,
            })
        } else {
            // alias: .prop or alias: expr
            let alias = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            if self.eat(&Token::Dot) {
                let prop = self.expect_ident()?;
                Ok(MapProjectionItem {
                    key: Some(alias),
                    property: Some(prop),
                    expr: Option::None,
                })
            } else {
                let expr = self.parse_expr(0)?;
                Ok(MapProjectionItem {
                    key: Some(alias),
                    property: Option::None,
                    expr: Some(expr),
                })
            }
        }
    }

    fn parse_case(&mut self) -> Result<Expr, ParseError> {
        self.advance(); // CASE

        // Determine if simple or searched CASE
        // Simple: CASE expr WHEN ... END
        // Searched: CASE WHEN ... END
        let operand = if !self.check_keyword(&Token::When) {
            Some(Box::new(self.parse_expr(0)?))
        } else {
            Option::None
        };

        let mut when_clauses = Vec::new();
        while self.check_keyword(&Token::When) {
            self.advance(); // WHEN
            let condition = self.parse_expr(0)?;
            self.expect(&Token::Then)?;
            let result = self.parse_expr(0)?;
            when_clauses.push(WhenClause { condition, result });
        }

        let else_expr = if self.check_keyword(&Token::Else) {
            self.advance();
            Some(Box::new(self.parse_expr(0)?))
        } else {
            Option::None
        };

        self.expect(&Token::End)?;

        Ok(Expr::Case {
            operand,
            when_clauses,
            else_expr,
        })
    }

    fn peek_at(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset).map(|(t, _)| t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_query(input: &str) -> QueryOrUnion {
        parse(input).unwrap_or_else(|e| panic!("Parse error for '{input}': {e}"))
    }

    fn unwrap_query(qou: QueryOrUnion) -> Query {
        match qou {
            QueryOrUnion::Query(q) => q,
            _ => panic!("expected Query, got Union"),
        }
    }

    #[test]
    fn test_simple_match_return() {
        let q = unwrap_query(parse_query("MATCH (n:Person) RETURN n"));
        assert_eq!(q.clauses.len(), 2);
        assert!(matches!(&q.clauses[0], Clause::Match(_)));
        assert!(matches!(&q.clauses[1], Clause::Return(_)));
    }

    #[test]
    fn test_match_with_properties() {
        let q = unwrap_query(parse_query("MATCH (n:Person {name: 'Alice'}) RETURN n"));
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_match_relationship() {
        let q = unwrap_query(parse_query("MATCH (a)-[r:KNOWS]->(b) RETURN a, b"));
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_where_clause() {
        let q = unwrap_query(parse_query("MATCH (n) WHERE n.age > 30 RETURN n"));
        if let Clause::Match(m) = &q.clauses[0] {
            assert!(m.where_expr.is_some());
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_create() {
        let q = unwrap_query(parse_query("CREATE (n:Person {name: 'Bob', age: 25})"));
        assert!(matches!(&q.clauses[0], Clause::Create(_)));
    }

    #[test]
    fn test_set_property() {
        let q = unwrap_query(parse_query("MATCH (n) SET n.age = 30"));
        assert!(matches!(&q.clauses[1], Clause::Set(_)));
    }

    #[test]
    fn test_set_label() {
        let q = unwrap_query(parse_query("MATCH (n) SET n:Admin"));
        assert!(matches!(&q.clauses[1], Clause::Set(_)));
    }

    #[test]
    fn test_detach_delete() {
        let q = unwrap_query(parse_query("MATCH (n) DETACH DELETE n"));
        if let Clause::Delete(d) = &q.clauses[1] {
            assert!(d.detach);
            assert_eq!(d.items, vec!["n".to_string()]);
        } else {
            panic!("expected Delete clause");
        }
    }

    #[test]
    fn test_return_order_by() {
        let q = unwrap_query(parse_query(
            "MATCH (a)-[r:KNOWS]->(b) WHERE a.age > 30 RETURN a.name, b.name ORDER BY a.name",
        ));
        if let Clause::Return(r) = &q.clauses[1] {
            assert_eq!(r.items.len(), 2);
            assert_eq!(r.order_by.len(), 1);
        } else {
            panic!("expected Return clause");
        }
    }

    #[test]
    fn test_count_distinct() {
        let q = unwrap_query(parse_query(
            "MATCH (n) RETURN count(DISTINCT n.label) AS cnt",
        ));
        if let Clause::Return(r) = &q.clauses[1] {
            assert_eq!(r.items.len(), 1);
            assert_eq!(r.items[0].alias, Some("cnt".to_string()));
            if let Expr::FunctionCall { name, distinct, .. } = &r.items[0].expr {
                assert_eq!(name, "count");
                assert!(distinct);
            } else {
                panic!("expected FunctionCall");
            }
        } else {
            panic!("expected Return clause");
        }
    }

    #[test]
    fn test_starts_with_and_in() {
        let q = unwrap_query(parse_query(
            "MATCH (n) WHERE n.name STARTS WITH 'A' AND n.age IN [20, 30, 40] RETURN n",
        ));
        if let Clause::Match(m) = &q.clauses[0] {
            assert!(m.where_expr.is_some());
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_explain() {
        let q = unwrap_query(parse_query("EXPLAIN MATCH (n) RETURN n"));
        assert!(q.explain);
    }

    #[test]
    fn test_union() {
        let result = parse_query("MATCH (n:A) RETURN n UNION MATCH (n:B) RETURN n");
        assert!(matches!(result, QueryOrUnion::Union(_)));
    }

    #[test]
    fn test_union_all() {
        let result = parse_query("MATCH (n:A) RETURN n UNION ALL MATCH (n:B) RETURN n");
        if let QueryOrUnion::Union(u) = result {
            assert!(u.all);
        } else {
            panic!("expected Union");
        }
    }

    #[test]
    fn test_with_clause() {
        let q = unwrap_query(parse_query(
            "MATCH (n) WITH n.name AS name WHERE name STARTS WITH 'A' RETURN name",
        ));
        assert!(matches!(&q.clauses[1], Clause::With(_)));
    }

    #[test]
    fn test_for() {
        let q = unwrap_query(parse_query("FOR x IN [1, 2, 3] RETURN x"));
        assert!(matches!(&q.clauses[0], Clause::For(_)));
    }

    #[test]
    fn test_optional_match() {
        let q = unwrap_query(parse_query("OPTIONAL MATCH (n) RETURN n"));
        if let Clause::Match(m) = &q.clauses[0] {
            assert!(m.optional);
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_case_expression() {
        let q = unwrap_query(parse_query(
            "MATCH (n) RETURN CASE WHEN n.age > 30 THEN 'old' ELSE 'young' END",
        ));
        if let Clause::Return(r) = &q.clauses[1] {
            assert!(matches!(&r.items[0].expr, Expr::Case { .. }));
        } else {
            panic!("expected Return clause");
        }
    }

    #[test]
    fn test_variable_length_rel() {
        let q = unwrap_query(parse_query("MATCH (a)-[*1..3]->(b) RETURN a, b"));
        if let Clause::Match(m) = &q.clauses[0] {
            if let PathElement::Rel(r) = &m.pattern[0].elements[1] {
                let vl = r.varlen.as_ref().unwrap();
                assert_eq!(vl.min_hops, Some(1));
                assert_eq!(vl.max_hops, Some(3));
            } else {
                panic!("expected Rel");
            }
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn test_semicolon() {
        let q = unwrap_query(parse_query("MATCH (n) RETURN n;"));
        assert_eq!(q.clauses.len(), 2);
    }

    #[test]
    fn test_keyword_as_property_name() {
        let q = unwrap_query(parse_query("MATCH (n) WHERE n.count > 5 RETURN n"));
        assert_eq!(q.clauses.len(), 2);
        if let Clause::Match(m) = &q.clauses[0] {
            assert!(m.where_expr.is_some());
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_keyword_as_label() {
        let q = unwrap_query(parse_query("MATCH (n:count) RETURN n"));
        if let Clause::Match(m) = &q.clauses[0] {
            if let PathElement::Node(node) = &m.pattern[0].elements[0] {
                assert_eq!(node.labels, vec!["count".to_string()]);
            } else {
                panic!("expected Node");
            }
        } else {
            panic!("expected Match clause");
        }
    }

    #[test]
    fn test_various_keywords_as_identifiers() {
        // Keywords used as property names
        parse_query("MATCH (n) WHERE n.exists = true RETURN n");
        parse_query("MATCH (n) WHERE n.all > 0 RETURN n");
        parse_query("MATCH (n) WHERE n.any = 'test' RETURN n");
        parse_query("MATCH (n) WHERE n.none = false RETURN n");
        parse_query("MATCH (n) WHERE n.single = 1 RETURN n");
        parse_query("MATCH (n) RETURN n.count, n.exists, n.case");
        // Keywords used as labels
        parse_query("MATCH (n:exists) RETURN n");
        parse_query("MATCH (n:all) RETURN n");
        parse_query("CREATE (n:count {value: 1})");
    }
}
