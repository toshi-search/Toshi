use failure::_core::marker::PhantomData;
use failure::_core::ops::AddAssign;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::fastfield::{FastFieldReader, FastValue};
use tantivy::schema::Field;
use tantivy::{SegmentReader, TantivyError};

#[allow(dead_code)]
struct FnCollector<C, V>
where
    C: SegmentCollector + Send + Sync,
{
    f: fn(Vec<V>) -> V,
    fields: Field,
    __c: PhantomData<C>,
}

impl<C> Collector for FnCollector<C, u64>
where
    C: SegmentCollector<Fruit = u64> + Send + Sync,
{
    type Fruit = u64;
    type Child = FnSegmentCollector<u64>;

    fn for_segment(&self, _segment_local_id: u32, segment: &SegmentReader) -> Result<Self::Child, TantivyError> {
        let fast_field_reader = segment.fast_fields().u64(self.fields).ok_or_else(|| {
            let field_name = segment.schema().get_field_name(self.fields);
            TantivyError::SchemaError(format!("Field {:?} is not a u64 fast field.", field_name))
        })?;
        Ok(sum_collector(fast_field_reader))
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> Result<Self::Fruit, TantivyError> {
        Ok((self.f)(segment_fruits))
    }
}

struct FnSegmentCollector<V>
where
    V: FastValue,
{
    reader: FastFieldReader<V>,
    value: V,
    func: fn(V, V, f32) -> V,
}

impl<V> SegmentCollector for FnSegmentCollector<V>
where
    V: FastValue + AddAssign + 'static,
{
    type Fruit = V;

    fn collect(&mut self, doc: u32, score: f32) {
        let value = self.reader.get(doc) as V;
        self.value += (self.func)(self.value, value, score);
    }

    fn harvest(self) -> Self::Fruit {
        self.value
    }
}

fn sum_collector(reader: FastFieldReader<u64>) -> FnSegmentCollector<u64> {
    FnSegmentCollector {
        reader,
        value: 0,
        func: |o: u64, v: u64, _: f32| -> u64 { o + v },
    }
}

#[cfg(test)]
mod tests {

    use crate::query::agg::{FnCollector, FnSegmentCollector};
    use tantivy::query::QueryParser;
    use tantivy::schema::*;
    use tantivy::{doc, Index};

    #[test]
    fn test_collector() -> tantivy::Result<()> {
        let mut schema_builder = Schema::builder();
        let product_name = schema_builder.add_text_field("name", TEXT);
        let product_description = schema_builder.add_text_field("description", TEXT);
        let price = schema_builder.add_u64_field("price", INDEXED | FAST);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema.clone());

        let mut index_writer = index.writer(50_000_000)?;
        index_writer.add_document(doc!(
            product_name => "Super Broom 2000",
            product_description => "While it is ok for short distance travel, this broom \
            was designed quiditch. It will up your game.",
            price => 1u64
        ));
        index_writer.add_document(doc!(
            product_name => "Turbulobroom",
            product_description => "You might have heard of this broom before : it is the sponsor of the Wales team.\
                You'll enjoy its sharp turns, and rapid acceleration",
            price => 1u64
        ));
        index_writer.add_document(doc!(
            product_name => "Broomio",
            product_description => "Great value for the price. This broom is a market favorite",
            price => 1u64
        ));
        index_writer.add_document(doc!(
            product_name => "Whack a Mole",
            product_description => "Prime quality bat.",
            price => 1u64
        ));
        index_writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![product_name, product_description]);
        let query = query_parser.parse_query("broom")?;
        let coll = FnCollector::<FnSegmentCollector<u64>, u64> {
            f: |nums: Vec<u64>| -> u64 {
                let mut total = 0u64;
                let mut len = 0u64;
                for n in nums {
                    if n != 0 {
                        total += n;
                        len += 1;
                    }
                }
                total / len
            },
            fields: price,
            __c: Default::default(),
        };
        let result = searcher.search(&query, &coll)?;
        println!("Result: {}", result);
        Ok(())
    }
}
