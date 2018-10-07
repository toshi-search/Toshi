use index::IndexCatalog;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::prelude::*;
use tokio::runtime::{Builder as RtBuilder, Runtime};
use tokio::timer::Interval;
use tokio_threadpool::Builder as TpBuilder;
use settings::SETTINGS;

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

        let task = Interval::new(Instant::now(), Duration::from_secs(SETTINGS.auto_commit_duration))
            .for_each(move |_| {
                if let Ok(mut cat) = cat.write() {
                    cat.get_mut_collection().iter_mut().for_each(|(key, index)| {
                        let current_ops = index.get_opstamp();
                        let writer = index.get_writer();
                        match writer.lock() {
                            Ok(mut w) => {
                                if current_ops == w.commit_opstamp() {
                                    info!("No update to index={}, opstamp={}", key, current_ops);
                                } else {
                                    let new_ops = w.commit().unwrap();
                                    index.set_opstamp(new_ops);
                                }
                            }
                            Err(e) => panic!("Unable to obtain lock for {}, err={:?}", key, e),
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
