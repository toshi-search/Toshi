/// Toshi-test is a modified version of Gotham's Test server, credit goes to them
/// https://github.com/gotham-rs/gotham/blob/master/gotham/src/test.rs
/// This was created to create a more integration test oriented way to test Toshi services
/// instead of magically bypassing some setup to call services.
pub use crate::server::*;
pub use crate::test::*;
use std::net::SocketAddr;
use tantivy::schema::*;
use tantivy::{doc, Index};

pub mod server;
pub mod test;

pub type Result<T> = std::result::Result<T, failure::Error>;
pub static CONTENT_TYPE: &str = "application/json";

pub fn get_localhost() -> SocketAddr {
    "127.0.0.1:8080".parse::<SocketAddr>().unwrap()
}

pub fn create_test_index() -> Index {
    let mut builder = SchemaBuilder::new();
    let test_text = builder.add_text_field("test_text", STORED | TEXT);
    let test_int = builder.add_i64_field("test_i64", STORED | INDEXED);
    let test_unsign = builder.add_u64_field("test_u64", STORED | INDEXED);
    let test_unindexed = builder.add_text_field("test_unindex", STORED);
    let test_facet = builder.add_facet_field("test_facet");

    let schema = builder.build();
    let idx = Index::create_in_ram(schema);
    let mut writer = idx.writer(30_000_000).unwrap();

    writer.add_document(doc! { test_text => "Test Document 1", test_int => 2014i64,  test_unsign => 10u64, test_unindexed => "no", test_facet => Facet::from("/cat/cat2") });
    writer.add_document(doc! { test_text => "Test Dockument 2", test_int => -2015i64, test_unsign => 11u64, test_unindexed => "yes", test_facet => Facet::from("/cat/cat2") });
    writer.add_document(doc! { test_text => "Test Duckiment 3", test_int => 2016i64,  test_unsign => 12u64, test_unindexed => "noo", test_facet => Facet::from("/cat/cat3") });
    writer.add_document(doc! { test_text => "Test Document 4", test_int => -2017i64, test_unsign => 13u64, test_unindexed => "yess", test_facet => Facet::from("/cat/cat4") });
    writer.add_document(doc! { test_text => "Test Document 5", test_int => 2018i64,  test_unsign => 14u64, test_unindexed => "nooo", test_facet => Facet::from("/dog/cat2") });
    writer.commit().unwrap();

    idx
}
