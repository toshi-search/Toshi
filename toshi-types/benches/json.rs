use criterion::{criterion_group, criterion_main, Benchmark, Criterion, Throughput};

const JSON: &'static [u8] = br#"{"query": { "range" : { "test_u64" : { "gte" : 10, "lte" : 1 } } } }"#;
fn bench_base(c: &mut Criterion) {
    c.bench(
        "json",
        Benchmark::new("New Types", move |b| {
            b.iter(|| serde_json::from_slice::<toshi_types::query::Search>(JSON));

            //            b.iter(|| serde_json::from_slice::<toshi::query::Search>(JSON))
        })
        .with_function("Old Types", move |b| {
            b.iter(|| serde_json::from_slice::<toshi::query::Search>(JSON));

            //            b.iter(|| serde_json::from_slice::<toshi_types::query::Search>(JSON))
        })
        .throughput(Throughput::Elements(1)),
    );
}

criterion_group!(benches, bench_base);
criterion_main!(benches);
