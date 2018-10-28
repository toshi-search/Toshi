use log::info;
use tantivy::query::Query as TantivyQuery;
use tantivy::schema::Schema;

pub mod aggregate;
pub mod bool;
pub mod bucket;

pub trait CreateQuery {
    fn create_query(&self, schema: &Schema) -> Box<TantivyQuery>;
}
