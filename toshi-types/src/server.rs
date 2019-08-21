use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tantivy::schema::Schema;

#[derive(Debug, Serialize, Deserialize)]
pub struct DocsAffected {
    pub docs_affected: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexOptions {
    #[serde(default)]
    pub commit: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddDocument<D> {
    pub options: Option<IndexOptions>,
    pub document: D,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SchemaBody(pub Schema);

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteDoc {
    pub options: Option<IndexOptions>,
    pub terms: HashMap<String, String>,
}
