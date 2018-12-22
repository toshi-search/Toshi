use query::SummaryDoc;
use tantivy::schema::NamedFieldDocument;

#[derive(Response, Serialize)]
pub struct SearchResults {
    pub hits: usize,
    pub docs: Vec<ScoredDoc>,
    pub aggregate: Option<Vec<SummaryDoc>>,
}

impl SearchResults {
    pub fn new(docs: Vec<ScoredDoc>) -> Self {
        Self {
            hits: docs.len(),
            docs,
            aggregate: None,
        }
    }

    pub fn with_aggregates(docs: Vec<ScoredDoc>, aggregate: Vec<SummaryDoc>) -> Self {
        Self {
            hits: docs.len(),
            docs,
            aggregate: Some(aggregate),
        }
    }
}

#[derive(Serialize)]
pub struct ScoredDoc {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
    #[serde(flatten)]
    pub doc: NamedFieldDocument,
}

impl ScoredDoc {
    pub fn new(score: Option<f32>, doc: NamedFieldDocument) -> Self {
        ScoredDoc { score, doc }
    }
}
