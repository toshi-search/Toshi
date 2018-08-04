use std::time::SystemTime;
use tantivy::Document;

#[derive(Debug, Serialize, Deserialize)]
pub struct Transaction {
    timestamp: SystemTime,
    opscode:   i64,
    document:  Document,
}

impl Transaction {
    #[allow(dead_code)]
    pub fn new(opscode: i64, document: Document) -> Self {
        Transaction {
            timestamp: SystemTime::now(),
            opscode,
            document,
        }
    }
}
