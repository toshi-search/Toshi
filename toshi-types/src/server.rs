use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use std::fmt::Formatter;
use tantivy::schema::Schema;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DocsAffected {
    pub docs_affected: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndexOptions {
    #[serde(default)]
    pub commit: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddDocument<D> {
    pub options: Option<IndexOptions>,
    pub document: D,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SchemaBody(pub Schema);

impl std::fmt::Debug for SchemaBody {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        f.write_str("Schema {\n")?;
        for field in self.0.fields() {
            f.write_fmt(format_args!("{:2}{:15}: {:?},\n", " ", field.1.name(), field.1.field_type()))?;
        }
        f.write_str("};")?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeleteDoc {
    pub options: Option<IndexOptions>,
    pub terms: HashMap<String, String>,
}
