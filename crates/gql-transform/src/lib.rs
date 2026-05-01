pub mod plan;
pub mod resolver;
pub mod sql_builder;
pub mod transform_match;

use gql_parser::Query;
use resolver::GraphSchema;

#[derive(Debug)]
pub enum TransformError {
    UnsupportedClause(String),
    UnsupportedExpr(String),
    Internal(String),
    UnknownProperty { entity: String, property: String },
}

impl std::fmt::Display for TransformError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransformError::UnsupportedClause(s) => write!(f, "unsupported clause: {s}"),
            TransformError::UnsupportedExpr(s) => write!(f, "unsupported expression: {s}"),
            TransformError::Internal(s) => write!(f, "internal error: {s}"),
            TransformError::UnknownProperty { entity, property } => write!(
                f,
                "unknown property '{property}' on entity '{entity}': no column mapping or extension column registered"
            ),
        }
    }
}

impl std::error::Error for TransformError {}

pub fn transform(query: &Query, schema: &GraphSchema) -> Result<String, TransformError> {
    transform_match::transform_query(query, schema)
}

pub fn transform_default(query: &Query) -> Result<String, TransformError> {
    transform(query, &GraphSchema::default())
}
