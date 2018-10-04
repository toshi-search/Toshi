use tantivy::schema::NamedFieldDocument;

#[derive(Serialize)]
pub struct SearchResults {
    hits: usize,
    docs: Vec<ScoredDoc>,
}

impl SearchResults {
    pub fn new(docs: Vec<ScoredDoc>) -> Self { SearchResults { hits: docs.len(), docs } }
}

#[derive(Serialize)]
pub struct ScoredDoc {
    score: f32,
    doc:   NamedFieldDocument,
}

impl ScoredDoc {
    pub fn new(score: f32, doc: NamedFieldDocument) -> Self { ScoredDoc { score, doc } }
}
