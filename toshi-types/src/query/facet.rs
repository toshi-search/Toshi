use std::borrow::Cow;

use serde::{Deserialize, Serialize};

use crate::query::KeyValue;

#[derive(Serialize, Deserialize, Debug)]
pub struct FacetQuery<'a>(#[serde(borrow = "'a")] KeyValue<Cow<'a, str>, Vec<Cow<'a, str>>>);
