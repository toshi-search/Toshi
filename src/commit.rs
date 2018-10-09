use index::IndexCatalog;
use settings::SETTINGS;

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use tokio::prelude::*;
use tokio::runtime::{Builder as RtBuilder, Runtime};
use tokio::timer::Interval;

pub struct IndexWatcher {
    catalog: Arc<RwLock<IndexCatalog>>,
    runtime: Runtime,
}

impl IndexWatcher {
    pub fn new(catalog: Arc<RwLock<IndexCatalog>>) -> Self {
        let runtime = RtBuilder::new()
            .core_threads(2)
            .name_prefix("toshi-index-committer")
            .build()
            .unwrap();
        IndexWatcher { catalog, runtime }
    }

    pub fn start(mut self) {
        let catalog = Arc::clone(&self.catalog);

        let task = Interval::new(Instant::now(), Duration::from_secs(SETTINGS.auto_commit_duration))
            .for_each(move |_| {
                if let Ok(mut cat) = catalog.write() {
                    cat.get_mut_collection().iter_mut().for_each(|(key, index)| {
                        let writer = index.get_writer();
                        match writer.lock() {
                            Ok(mut w) => {
                                let current_ops = index.get_opstamp();
                                if current_ops == 0 {
                                    info!("No update to index={}, opstamp={}", key, current_ops);
                                } else {
                                    w.commit().unwrap();
                                    index.set_opstamp(0);
                                }
                            }
                            Err(_) => (),
                        };
                    });
                }
                Ok(())
            })
            .map_err(|e| panic!("Error in commit-watcher={:?}", e));

        self.runtime.spawn(future::lazy(|| task));
        self.runtime.shutdown_on_idle();
    }

    pub fn shutdown(self) { self.runtime.shutdown_now(); }
}
