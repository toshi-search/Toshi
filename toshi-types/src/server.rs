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

impl<D> AddDocument<D> {
    /// Convenience method for Raft Implementation
    pub fn new(document: D, options: Option<IndexOptions>) -> Self {
        Self { options, document }
    }
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

#[cfg(test)]
mod tests {

    use crate::SchemaBody;
    use tantivy::schema::*;

    #[test]
    fn test_debug() {
        let mut builder = SchemaBuilder::new();
        builder.add_text_field("test_text", STORED | TEXT);
        builder.add_i64_field("test_i64", STORED | INDEXED | FAST);
        builder.add_u64_field("test_u64", STORED | INDEXED);
        builder.add_text_field("test_unindex", STORED);
        builder.add_facet_field("test_facet");
        builder.add_date_field("test_date", INDEXED | FAST);
        let schema = SchemaBody(builder.build());

        println!("{:?}", schema);
    }
}
