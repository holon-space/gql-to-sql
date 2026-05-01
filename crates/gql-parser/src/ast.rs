pub type Span = (usize, usize);

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub clauses: Vec<Clause>,
    pub explain: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Union {
    pub left: Box<QueryOrUnion>,
    pub right: Box<Query>,
    pub all: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryOrUnion {
    Query(Query),
    Union(Union),
}

// --- Clauses ---

#[derive(Debug, Clone, PartialEq)]
pub enum Clause {
    Match(MatchClause),
    Return(ReturnClause),
    Create(CreateClause),
    Set(SetClause),
    Delete(DeleteClause),
    With(WithClause),
    For(ForClause),
}

#[derive(Debug, Clone, PartialEq)]
pub struct MatchClause {
    pub pattern: Vec<Path>,
    pub where_expr: Option<Box<Expr>>,
    pub optional: bool,
    pub from_graph: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub order_by: Vec<OrderByItem>,
    pub skip: Option<Box<Expr>>,
    pub limit: Option<Box<Expr>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub order_by: Vec<OrderByItem>,
    pub skip: Option<Box<Expr>>,
    pub limit: Option<Box<Expr>>,
    pub where_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateClause {
    pub pattern: Vec<Path>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetClause {
    pub items: Vec<SetItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetItem {
    pub property: Box<Expr>,
    pub expr: Box<Expr>,
    pub is_merge: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteClause {
    pub items: Vec<String>,
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ForClause {
    pub variable: String,
    pub list_expr: Box<Expr>,
}

// --- Return / Order By ---

#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub descending: bool,
}

// --- Patterns ---

#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Option<Box<Expr>>,
    pub where_expr: Option<Box<Expr>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Direction {
    Left,
    Right,
    Both,
    None,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelPattern {
    pub variable: Option<String>,
    pub rel_types: Vec<String>,
    pub properties: Option<Box<Expr>>,
    pub where_expr: Option<Box<Expr>>,
    pub direction: Direction,
    pub varlen: Option<VarLenRange>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VarLenRange {
    pub min_hops: Option<u32>,
    pub max_hops: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathType {
    Normal,
    Shortest,
    AllShortest,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PathElement {
    Node(NodePattern),
    Rel(RelPattern),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    pub elements: Vec<PathElement>,
    pub variable: Option<String>,
    pub path_type: PathType,
}

// --- Expressions ---

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Literal),
    Identifier(String),
    Parameter(String),
    Property {
        expr: Box<Expr>,
        name: String,
    },
    LabelExpr {
        expr: Box<Expr>,
        label: String,
    },
    Not(Box<Expr>),
    NullCheck {
        expr: Box<Expr>,
        is_not_null: bool,
    },
    BinaryOp {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    Exists(ExistsExpr),
    List(Vec<Expr>),
    ListComprehension {
        variable: String,
        list_expr: Box<Expr>,
        where_expr: Option<Box<Expr>>,
        transform_expr: Option<Box<Expr>>,
    },
    PatternComprehension {
        pattern: Vec<Path>,
        where_expr: Option<Box<Expr>>,
        collect_expr: Option<Box<Expr>>,
    },
    Map(Vec<MapPair>),
    MapProjection {
        base_expr: Box<Expr>,
        items: Vec<MapProjectionItem>,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<WhenClause>,
        else_expr: Option<Box<Expr>>,
    },
    ListPredicate {
        pred_type: ListPredicateType,
        variable: String,
        list_expr: Box<Expr>,
        predicate: Box<Expr>,
    },
    Reduce {
        accumulator: String,
        initial_value: Box<Expr>,
        variable: String,
        list_expr: Box<Expr>,
        expression: Box<Expr>,
    },
    Subscript {
        expr: Box<Expr>,
        index: Box<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOp {
    And,
    Or,
    Xor,
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    RegexMatch,
    In,
    StartsWith,
    EndsWith,
    Contains,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExistsExpr {
    /// `EXISTS { <pattern-list> [WHERE <expr>] }` — correlated subquery.
    /// `where_expr` is `None` for the legacy parenthesised form.
    Pattern {
        paths: Vec<Path>,
        where_expr: Option<Box<Expr>>,
    },
    Property(Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ListPredicateType {
    All,
    Any,
    None,
    Single,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MapPair {
    pub key: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MapProjectionItem {
    pub key: Option<String>,
    pub property: Option<String>,
    pub expr: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
}
