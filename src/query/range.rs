use super::{CreateQuery, Error, Result};

use log::warn;

use std::collections::HashMap;
use std::ops::Bound;

use serde::de::DeserializeOwned;
use serde_json::Value;

use tantivy::query::{Query, RangeQuery as TantivyRangeQuery};
use tantivy::schema::{FieldType, Schema};

#[derive(Deserialize, Debug, PartialEq, Clone)]
#[serde(untagged)]
pub enum Ranges {
    ValueRange {
        gte:   Option<Value>,
        lte:   Option<Value>,
        lt:    Option<Value>,
        gt:    Option<Value>,
        boost: Option<f32>,
    },
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct RangeQuery {
    range: HashMap<String, Ranges>,
}

impl CreateQuery for RangeQuery {
    fn create_query(self, schema: &Schema) -> Result<Box<Query>> {
        if self.range.keys().len() > 1 {
            warn!("More than 1 range field specified, only using the first.");
        }
        if let Some((k, v)) = self.range.into_iter().take(1).next() {
            create_range_query(schema, &k, &v)
        } else {
            Err(Error::QueryError("Query generation failed".into()))
        }
    }
}

#[inline]
fn create_ranges<T>(gte: &Option<Value>, lte: &Option<Value>, lt: &Option<Value>, gt: &Option<Value>) -> Result<(Bound<T>, Bound<T>)>
where T: DeserializeOwned {
    let lower = if let Some(b) = gt {
        let value = serde_json::from_value(b.clone()).map_err(Error::from)?;
        Bound::Excluded(value)
    } else if let Some(b) = gte {
        let value = serde_json::from_value(b.clone()).map_err(Error::from)?;
        Bound::Included(value)
    } else {
        return Err(Error::QueryError("No lower bound specified".into()));
    };
    let upper = if let Some(b) = lt {
        let value = serde_json::from_value(b.clone()).map_err(Error::from)?;
        Bound::Excluded(value)
    } else if let Some(b) = lte {
        let value = serde_json::from_value(b.clone()).map_err(Error::from)?;
        Bound::Included(value)
    } else {
        return Err(Error::QueryError("No upper bound specified".into()));
    };
    Ok((upper, lower))
}

pub fn create_range_query(schema: &Schema, field: &str, r: &Ranges) -> Result<Box<Query>> {
    match r {
        Ranges::ValueRange { gte, lte, lt, gt, .. } => {
            let field = schema
                .get_field(field)
                .ok_or_else(|| Error::IOError(format!("Field {} does not exist", field)))?;
            let field_type = schema.get_field_entry(field).field_type();
            match field_type {
                &FieldType::I64(_) => {
                    let (upper, lower) = create_ranges::<i64>(&gte, &lte, &lt, &gt)?;
                    Ok(Box::new(TantivyRangeQuery::new_i64_bounds(field, lower, upper)))
                }
                &FieldType::U64(_) => {
                    let (upper, lower) = create_ranges::<u64>(&gte, &lte, &lt, &gt)?;
                    Ok(Box::new(TantivyRangeQuery::new_u64_bounds(field, lower, upper)))
                }
                ref ft => Err(Error::QueryError(format!("Invalid field type: {:?} for range query", ft))),
            }
        }
    }
}
