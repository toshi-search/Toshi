use index::IndexCatalog;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::prelude::*;
use tokio::runtime::{Builder as RtBuilder, Runtime};
use tokio::timer::Interval;
use tokio_threadpool::Builder as TpBuilder;

pub struct IndexWatcher {
    catalog: Arc<RwLock<IndexCatalog>>,
    runtime: Runtime,
}

impl IndexWatcher {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>) -> Self {
        let mut pool = TpBuilder::new();
        pool.name_prefix("toshi-index-committer");
        let runtime = RtBuilder::new().threadpool_builder(pool).build().unwrap();
        IndexWatcher { catalog, runtime }
    }

    pub fn start(mut self) {
        let cat = Arc::clone(&self.catalog);

        let task = Interval::new(Instant::now(), Duration::from_secs(10))
            .for_each(move |i| {
                info!("Hit the interval int={:?}", i);
                if let Ok(cat) = cat.read() {
                    cat.get_collection().into_iter().for_each(|(k, i)| {
                        match i.writer(3000000) {
                            Ok(mut w) => w.commit().unwrap(),
                            Err(e) => panic!("Unable to obtain lock for {}, err={:?}", k, e)
                        };
                    });
                }
                Ok(())
            }).map_err(|e| panic!("Error in timer={:?}", e));

        self.runtime.spawn(future::lazy(|| task));
        self.runtime.shutdown_on_idle();
    }

    pub fn shutdown(self) { self.runtime.shutdown_now(); }
}
