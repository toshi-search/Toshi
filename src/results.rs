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
    #[serde(skip_serializing_if = "Option::is_none")]
    score: Option<f32>,
    #[serde(flatten)]
    doc: NamedFieldDocument,
}

impl ScoredDoc {
    pub fn new(score: Option<f32>, doc: NamedFieldDocument) -> Self { ScoredDoc { score, doc } }
}
