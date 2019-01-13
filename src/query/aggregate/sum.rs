use serde::{Deserialize, Serialize};
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::fastfield::FastFieldReader;
use tantivy::schema::Field;
use tantivy::{Result as TantivyResult, SegmentReader};

#[derive(Serialize, Debug, Deserialize)]
pub struct SummaryDoc {
    field: Field,
    value: u64,
}

#[derive(Default)]
pub struct Stats {
    count: usize,
    sum: f64,
}

impl Stats {
    pub fn count(&self) -> usize {
        self.count
    }

    pub fn mean(&self) -> f64 {
        self.sum / (self.count as f64)
    }

    fn non_zero_count(self) -> Option<Stats> {
        if self.count == 0 {
            None
        } else {
            Some(self)
        }
    }
}

pub struct SumCollector {
    field: Field,
}

impl SumCollector {
    fn with_field(field: Field) -> SumCollector {
        SumCollector { field }
    }
}

impl Collector for SumCollector {
    type Fruit = Option<Stats>;
    type Child = SumSegmentCollector;

    fn for_segment(&self, _segment_local_id: u32, segment: &SegmentReader) -> TantivyResult<SumSegmentCollector> {
        let fast_field_reader = segment.fast_field_reader(self.field)?;
        Ok(SumSegmentCollector {
            fast_field_reader,
            stats: Stats::default(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_stats: Vec<Option<Stats>>) -> TantivyResult<Option<Stats>> {
        let mut stats = Stats::default();
        for segment_stats_opt in segment_stats {
            if let Some(segment_stats) = segment_stats_opt {
                stats.count += segment_stats.count;
                stats.sum += segment_stats.sum;
            }
        }
        Ok(stats.non_zero_count())
    }
}

pub struct SumSegmentCollector {
    fast_field_reader: FastFieldReader<u64>,
    stats: Stats,
}

impl SegmentCollector for SumSegmentCollector {
    type Fruit = Option<Stats>;

    fn collect(&mut self, doc: u32, _score: f32) {
        let value = self.fast_field_reader.get(doc) as f64;
        self.stats.count += 1;
        self.stats.sum += value;
    }

    fn harvest(self) -> Self::Fruit {
        self.stats.non_zero_count()
    }
}
