use std::fs::{create_dir_all, File};
use std::io::{Error, Write};
use std::path::PathBuf;

use serde_json::to_vec;
use tantivy::Document;

use capnp::message::HeapAllocator;
use capnp::traits::*;
use capnp::{message, serialize};

use wal_capnp::{transaction, Action};

#[allow(unused)]
pub struct Transaction {
    timestamp: u64,
    opscode:   u32,
    document:  Vec<u8>,
    action:    u16,
}

impl<'a> Into<Transaction> for transaction::Reader<'a> {
    fn into(self) -> Transaction {
        Transaction {
            timestamp: self.get_timestamp(),
            opscode:   self.get_opscode(),
            document:  self.get_document().unwrap().to_vec(),
            action:    self.get_action().unwrap().to_u16(),
        }
    }
}

pub struct TransactionConfig {
    buffer_size: usize,
}
#[allow(unused)]
impl TransactionConfig {
    pub fn new(buffer_size: usize) -> Self { TransactionConfig { buffer_size } }
}
#[allow(unused)]
pub struct TransactionLog {
    index_name:    String,
    opscode:       u32,
    path:          PathBuf,
    config:        TransactionConfig,
    buf_size:      usize,
    total_written: usize,
    buf:           File,
}

#[allow(dead_code)]
impl TransactionLog {
    pub fn from_config(index_name: &str, opscode: u32, path: &PathBuf, config: TransactionConfig) -> Result<Self, Error> {
        let mut base_path = path.clone();
        base_path.push(index_name);
        if !base_path.exists() {
            match create_dir_all(base_path.clone()) {
                Ok(_) => {}
                Err(e) => return Err(e),
            };
        }
        let segment_path = TransactionLog::create_filename(index_name, &base_path, opscode)?;
        let file = File::create(segment_path)?;
        Ok(TransactionLog {
            index_name: index_name.to_string(),
            opscode,
            buf: file,
            buf_size: 0,
            total_written: 0,
            path: base_path,
            config,
        })
    }

    fn read_transactions(mut file: File) -> Vec<Transaction> {
        let mut results = Vec::new();
        while let Ok(msg) = serialize::read_message(&mut file, message::ReaderOptions::new()) {
            let m: Transaction = msg.get_root::<transaction::Reader>().unwrap().into();
            results.push(m);
        }
        results
    }

    fn create_filename(index_name: &str, path: &PathBuf, opscode: u32) -> Result<PathBuf, Error> {
        let mut segment_path = path.clone();
        segment_path.push(format!("{}.{}", index_name, opscode));
        Ok(segment_path)
    }

    fn rotate_file(&mut self, opscode: u32) -> Result<(), Error> {
        self.flush()?;
        let segment_path = TransactionLog::create_filename(&self.index_name, &self.path, opscode)?;
        info!("Rotating to {:#?}", segment_path);
        self.buf_size = 0;
        self.buf = match File::create(segment_path) {
            Ok(f) => f,
            Err(e) => return Err(e),
        };
        Ok(())
    }

    fn build_transaction(timestamp: u64, opscode: u32, action: Action, doc: &Document) -> Result<message::Builder<HeapAllocator>, Error> {
        let mut message = message::Builder::new_default();
        {
            let mut trans = message.init_root::<transaction::Builder>();
            trans.set_action(action);
            trans.set_timestamp(timestamp);
            trans.set_opscode(opscode);
            let doc_bytes = to_vec(doc)?;
            let mut doc_builder = trans.init_document(doc_bytes.len() as u32);
            doc_builder.write_all(&doc_bytes)?;
        }
        Ok(message)
    }

    pub fn add_transaction(&mut self, timestamp: u64, opscode: u32, action: Action, doc: &Document) -> Result<(), Error> {
        let message = TransactionLog::build_transaction(timestamp, opscode, action, doc)?;
        let msg_size = serialize::compute_serialized_size_in_words(&message) * 8;
        if (self.buf_size + msg_size) > self.config.buffer_size {
            self.rotate_file(opscode)?
        }
        self.buf_size += msg_size;
        self.total_written += msg_size;
        serialize::write_message(&mut self.buf, &message)?;
        Ok(())
    }

    pub fn current_size(&self) -> usize { self.buf_size }

    pub fn total_size(&self) -> usize { self.total_written }

    pub fn flush(&mut self) -> Result<(), Error> {
        self.buf.flush()?;
        self.buf.sync_all()
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod tests {

    use super::*;
    use std::fs::create_dir;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tantivy::schema::*;

    fn now() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() }

    #[test]
    fn test_transaction_log() {
        let mut builder = SchemaBuilder::new();
        let text = builder.add_text_field("test_text", STORED);

        let cfg = TransactionConfig::new(400);
        let index_name = "test_index";
        let data_path = PathBuf::from("logs");
        if !data_path.exists() {
            create_dir(&data_path).unwrap();
        }

        let mut trans = TransactionLog::from_config(index_name, 0, &data_path, cfg).unwrap();

        trans.add_transaction(now(), 1, Action::Create, &doc!(text => "asdf"));
        trans.add_transaction(now(), 2, Action::Update, &doc!(text => "as123411111df"));
        trans.add_transaction(now(), 2, Action::Delete, &doc!(text => "as1234d123sf"));
        trans.add_transaction(now(), 3, Action::Create, &doc!(text => "as12asd34df"));
        assert_eq!(trans.current_size(), trans.total_size());

        trans.add_transaction(now(), 3, Action::Update, &doc!(text => "as1234w1123df"));
        assert_ne!(trans.current_size(), trans.total_size());
        assert_eq!(96, trans.current_size());

        let mut zero = data_path.clone();
        zero.push("test_index");
        zero.push("test_index.0");

        let f = File::open(zero).unwrap();
        let trans = TransactionLog::read_transactions(f);

        assert_eq!(trans.len(), 4);
    }
}
