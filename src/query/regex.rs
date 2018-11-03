#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RegexQuery {
    pattern: String,
}
