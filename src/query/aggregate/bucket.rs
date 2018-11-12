#[derive(Deserialize, Debug)]
pub struct Range {
    from: Option<u64>,
    to: Option<u64>,
}

#[derive(Serialize)]
pub struct RangeResult {
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    from: Option<String>,
    num_docs: u64,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Buckets {
    RangeAggregate { field: String, ranges: Vec<Range> },
}
