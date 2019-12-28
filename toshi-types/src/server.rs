use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use std::fmt::Formatter;
use tantivy::schema::Schema;

/// In a delete query, this is returned indicating the number of documents that were removed
/// by the delete.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DocsAffected {
    /// The number of documents removed by the query
    pub docs_affected: u64,
}

/// Indicates whether or not a commit should be done at the end of a document insert, the default
/// is false
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndexOptions {
    /// Whether to commit after insertion
    #[serde(default)]
    pub commit: bool,
}

/// The request body for adding a single document to an index
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddDocument<D> {
    /// Options surrounding the insert [`IndexOptions`]
    pub options: Option<IndexOptions>,
    /// The actual document to insert
    pub document: D,
}

/// A wrapper around Tantivy's schema for when an index is created. [`tantivy::schema::Schema`]
#[derive(Serialize, Deserialize, Clone)]
pub struct SchemaBody(pub Schema);

impl std::fmt::Debug for SchemaBody {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str("Schema {\n")?;
        for field in self.0.fields() {
            f.write_fmt(format_args!("{:2}{:15}: {:?},\n", " ", field.1.name(), field.1.field_type()))?;
        }
        f.write_str("};")?;
        Ok(())
    }
}

/// The request body for performing a delete request to an index
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteDoc {
    /// Options surrounding the delete [`IndexOptions`]
    pub options: Option<IndexOptions>,
    /// The term pairs to delete, since this could be any number of term pairs this does not use
    /// KeyValue like a lot of other queries do that only accept a single term pair at a time
    pub terms: HashMap<String, String>,
}
