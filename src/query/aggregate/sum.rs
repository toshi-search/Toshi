use super::super::{Error, Result};

use super::AggregateQuery;

use tantivy::collector::{Collector, TopCollector};
use tantivy::schema::Value;
use tantivy::{Searcher, SegmentReader};
use tantivy::schema::Field;

#[derive(Serialize, Debug)]
pub struct SummaryDoc {
    field: Field,
    value: u64,
}

pub struct SumCollector<'a> {
    field: String,
    collector: TopCollector,
    searcher: &'a Searcher,
}

impl<'a> SumCollector<'a> {
    pub fn new(field: String, searcher: &'a Searcher, collector: TopCollector) -> Self {
        Self {
            field,
            searcher,
            collector,
        }
    }
}

impl<'a> AggregateQuery<SummaryDoc> for SumCollector<'a> {
    fn result(&self) -> Result<SummaryDoc> {
        let field = self
            .searcher
            .schema()
            .get_field(&self.field)
            .ok_or_else(|| Error::QueryError(format!("Field {} does not exist", self.field)))?;
        let result: u64 = self
            .collector
            .docs()
            .into_iter()
            .map(move |d| {
                // At this point docs have already passed through the collector, if we are in map it means we have
                // something
                let doc = self.searcher.doc(d).unwrap();
                doc.get_first(field)
                    .into_iter()
                    .map(|v| match v {
                        Value::I64(i) => (*i) as u64,
                        Value::U64(u) => *u,
                        // Should we even have these or only numerics?
                        Value::Str(s) => (*s).len() as u64,
                        Value::Bytes(b) => (*b).len() as u64,
                        _ => panic!("Value is not numeric"),
                    }).sum::<u64>()
            }).sum();

        Ok(SummaryDoc {
            field: Field(0),
            value: result,
        })
    }
}

impl<'a> Collector for SumCollector<'a> {
    fn set_segment(&mut self, segment_local_id: u32, segment: &SegmentReader) -> tantivy::Result<()> {
        self.collector.set_segment(segment_local_id, segment)
    }

    fn collect(&mut self, doc: u32, score: f32) {
        self.collector.collect(doc, score);
    }

    fn requires_scoring(&self) -> bool {
        self.collector.requires_scoring()
    }
}
