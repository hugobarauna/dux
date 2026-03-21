use thiserror::Error;

#[derive(Error, Debug)]
pub enum DuxError {
    #[error("DuckDB error: {0}")]
    DuckDB(#[from] duckdb::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("{0}")]
    Other(String),
}
