use super::*;
use std::ops::Bound;
use tantivy::query::RangeQuery as TantivyRangeQuery;

macro_rules! type_range {
    ($($n:ident $t:ty),*) => {
        #[derive(Deserialize, Debug, PartialEq, Clone, Copy)]
        #[serde(untagged)]
        pub enum Ranges {
            $($n { gte: Option<$t>, lte: Option<$t>, lt: Option<$t>, gt: Option<$t>, boost: Option<f32> },)*
        }
    };
}

type_range!(U64Range u64, I64Range i64);

#[derive(Deserialize, Debug, PartialEq)]
pub struct RangeQuery {
    range: HashMap<String, Ranges>,
}

impl RangeQuery {
    pub fn new(range: HashMap<String, Ranges>) -> Self { Self { range } }
}

impl CreateQuery for RangeQuery {
    fn create_query(self, schema: &Schema) -> Box<TantivyQuery> {
        if self.range.keys().len() > 1 {
            warn!("More than 1 range field specified, only using the first.");
        }
        let mut iter = self.range.into_iter().take(1);
        let (k, v): (String, Ranges) = iter.next().unwrap();
        let query: Box<TantivyQuery> = create_range_query(schema, &k, &v);
        query
    }
}

#[inline]
fn create_ranges<T>(gte: Option<T>, lte: Option<T>, lt: Option<T>, gt: Option<T>) -> (Bound<T>, Bound<T>) {
    let lower = if let Some(b) = gt {
        Bound::Excluded(b)
    } else if let Some(b) = gte {
        Bound::Included(b)
    } else {
        panic!("No lower bound specified ");
    };
    let upper = if let Some(b) = lt {
        Bound::Excluded(b)
    } else if let Some(b) = lte {
        Bound::Included(b)
    } else {
        panic!("No lower bound specified ");
    };
    (upper, lower)
}

pub fn create_range_query(schema: &Schema, field: &str, r: &Ranges) -> Box<TantivyQuery> {
    match r {
        Ranges::U64Range {
            gte,
            lte,
            lt,
            gt,
            boost: _,
        } => {
            let field = schema.get_field(field).unwrap_or_else(|| panic!("Field: {} does not exist", field));
            let (upper, lower) = create_ranges::<u64>(*gte, *lte, *lt, *gt);
            let query: Box<TantivyQuery> = Box::new(TantivyRangeQuery::new_u64_bounds(field, lower, upper));
            query
        }
        Ranges::I64Range {
            gte,
            lte,
            lt,
            gt,
            boost: _,
        } => {
            let field = schema.get_field(field).unwrap_or_else(|| panic!("Field: {} does not exist", field));
            let (upper, lower) = create_ranges::<i64>(*gte, *lte, *lt, *gt);
            let query: Box<TantivyQuery> = Box::new(TantivyRangeQuery::new_i64_bounds(field, lower, upper));
            query
        }
    }
}
