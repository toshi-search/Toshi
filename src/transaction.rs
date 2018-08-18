use std::io::{Error, Write};
use std::path::PathBuf;
use std::time::Duration;

use tantivy::Document;

use capnp::{message, serialize};
use capnp::message::HeapAllocator;

use serde_json::to_vec;
use std::fs::File;
use std::io::BufWriter;
use wal_capnp::transaction;
use wal_capnp::Action;
use wal_capnp::transaction::Builder;

#[allow(dead_code)]
pub struct TransactionLog {
    index_name:      String,
    current_opscode: u32,
    path:            PathBuf,
    segment_size:    u32, // Make all this configurable
    buffer_size:     u64,
    age_of_buffer:   Duration,
    buf:             BufWriter<File>,
}

impl TransactionLog {
    #[allow(dead_code)]
    pub fn from_config(
        index_name: String,
        opscode: u32,
        path: PathBuf,
        segment_size: u32,
        buffer_size: u64,
        age_of_buffer: Duration,
    ) -> Result<Self, Error>
    {
        let mut segment_path = path.clone();
        segment_path.push(format!("{}.{}", index_name, opscode));
        println!("{:?}", segment_path);

        let file = File::create(segment_path)?;
        let buf_file = BufWriter::new(file);
        Ok(TransactionLog {
            index_name,
            current_opscode: opscode,
            buf: buf_file,
            path,
            segment_size,
            buffer_size,
            age_of_buffer,
        })
    }

    #[allow(dead_code)]
    fn rotate_file(&mut self, opscode: u32) -> Result<Self, Error> {
        self.flush()?;

        TransactionLog::from_config(
            self.index_name.clone(),
            opscode,
            self.path.clone(),
            self.segment_size,
            self.buffer_size,
            self.age_of_buffer,
        )
    }

    fn build_transaction<'a>(timestamp: u64, opscode: u32, action: Action, doc: Document) -> Result<message::Builder<HeapAllocator>, Error> {
        let mut message = message::Builder::new_default();
        {
            let mut trans = message.init_root::<transaction::Builder>();
            trans.set_action(action);
            trans.set_timestamp(timestamp);
            trans.set_opscode(opscode);
            let doc_bytes = to_vec(&doc)?;
            let mut doc_builder = trans.init_document(doc_bytes.len() as u32);
            doc_builder.write(&doc_bytes)?;
        }
        Ok(message)
    }

    #[allow(dead_code)]
    pub fn add_transaction(&mut self, timestamp: u64, opscode: u32, action: Action, doc: Document) -> Result<(), Error> {
        let message = TransactionLog::build_transaction(timestamp, opscode, action, doc)?;
        serialize::write_message(&mut self.buf, &message)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), Error> { self.buf.flush() }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use tantivy::schema::*;

    #[test]
    fn test_transaction_log() {
        let mut builder = SchemaBuilder::new();
        let text = builder.add_text_field("test_text", STORED);

        let mut trans = TransactionLog::from_config(
            "test_index".to_string(),
            0,
            PathBuf::from("data"),
            10_000_000,
            2_000,
            Duration::from_secs(180),
        ).unwrap();

        fn now() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() }
        trans.add_transaction(now(), 1, Action::Create, doc!(text => "asdf")).unwrap();
        trans
            .add_transaction(now(), 2, Action::Update, doc!(text => "as123411111df"))
            .unwrap();
        trans
            .add_transaction(now(), 2, Action::Delete, doc!(text => "as1234d123sf"))
            .unwrap();
        trans
            .add_transaction(now(), 3, Action::Create, doc!(text => "as12asd34df"))
            .unwrap();
        trans
            .add_transaction(now(), 3, Action::Update, doc!(text => "as1234w1123df"))
            .unwrap();
        trans.flush();

        use std::str::from_utf8;
        let mut f = File::open(PathBuf::from("data\\test_index.0")).unwrap();
        loop {
            let msg = match serialize::read_message(&mut f, message::ReaderOptions::new()) {
                Ok(m) => m,
                Err(_) => break,
            };
            let m = msg.get_root::<transaction::Reader>().unwrap();

            println!("  {:?}", from_utf8(m.get_document().unwrap()).unwrap());
        }
    }

}
