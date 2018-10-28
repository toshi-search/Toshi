use super::*;
use tantivy::collector::*;
use tantivy::schema::*;
use tantivy::Document;
use tantivy::*;

pub fn summary_schema() -> Schema {
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.add_u64_field("value", IntOptions::default());
    schema_builder.build()
}

#[derive(Serialize)]
pub struct SummaryDoc {
    field: Field,
    value: u64,
}

impl Into<Document> for SummaryDoc {
    fn into(self) -> Document {
        let mut doc = Document::new();
        doc.add_u64(self.field, self.value);
        info!("Doc: {:?}", doc);
        doc
    }
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Metrics {
    SumAgg { field: String },
}

pub struct SumCollector<'a> {
    field:     Field,
    collector: TopScoreCollector,
    searcher:  &'a Searcher,
}

impl<'a> SumCollector<'a> {
    pub fn new(field: Field, searcher: &'a Searcher, collector: TopCollector) -> Self {
        Self {
            field,
            searcher,
            collector,
        }
    }

    pub fn result(&self) -> SummaryDoc {
        let result: u64 = self
            .collector
            .docs()
            .into_iter()
            .map(|d| {
                // At this point docs have already passed through the collector, if we are in map it means we have
                // something
                let doc = self.searcher.doc(d).unwrap();
                doc.get_first(self.field)
                    .into_iter()
                    .map(|v| match v {
                        Value::I64(i) => (*i) as u64,
                        Value::U64(u) => *u,
                        // Should we even have these or only numerics?
                        Value::Str(s) => (*s).len() as u64,
                        Value::Bytes(b) => (*b).len() as u64,
                        _ => panic!("Value is not numeric"),
                    })
                    .sum::<u64>()
            })
            .sum();
        SummaryDoc {
            field: Field(0),
            value: result,
        }
    }
}

impl<'a> Collector for SumCollector<'a> {
    fn set_segment(&mut self, segment_local_id: u32, segment: &SegmentReader) -> tantivy::Result<()> {
        self.collector.set_segment(segment_local_id, segment)
    }

    fn collect(&mut self, doc: u32, score: f32) { self.collector.collect(doc, score); }

    fn requires_scoring(&self) -> bool { self.collector.requires_scoring() }
}
