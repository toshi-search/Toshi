use std::fs::OpenOptions;
use std::io::{Error, Write};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use bincode::serialize;
use memmap::MmapMut;
use tantivy::Document;

#[allow(dead_code)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Action {
    Create,
    Update,
    Delete,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Transaction {
    timestamp: SystemTime,
    opscode:   u64,
    action:    Action,
    document:  Document,
}

impl Transaction {
    #[allow(dead_code)]
    pub fn new(action: Action, opscode: u64, document: Document) -> Self {
        Transaction {
            action,
            timestamp: SystemTime::now(),
            opscode,
            document,
        }
    }
}

#[allow(dead_code)]
pub struct TransactionLog {
    index_name:      String,
    current_opscode: u32,
    path:            PathBuf,
    segment_size:    u32, // Make all this configurable
    buffer_size:     u64,
    buf_pos:         usize,
    age_of_buffer:   Duration,
    mmap:            MmapMut,
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

        let file = OpenOptions::new().create(true).read(true).write(true).open(&segment_path).unwrap();
        file.set_len(buffer_size)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(TransactionLog {
            index_name,
            current_opscode: opscode,
            mmap,
            path,
            segment_size,
            buffer_size,
            buf_pos: 0,
            age_of_buffer,
        })
    }

    #[allow(dead_code)]
    fn rotate_file(&self, opscode: u32) -> Result<Self, Error> {
        TransactionLog::from_config(
            self.index_name.clone(),
            opscode,
            self.path.clone(),
            self.segment_size,
            self.buffer_size,
            self.age_of_buffer,
        )
    }

    #[allow(dead_code)]
    pub fn add_transaction(&mut self, transaction: Transaction) -> Result<(), Error> {
        println!("{}", self.buf_pos);
        let mut trans_bytes = serialize(&transaction).unwrap();
        trans_bytes.push(b'\n');
        let num_bytes = trans_bytes.len();

        (&mut self.mmap[self.buf_pos..(self.buf_pos + num_bytes)]).write(&trans_bytes)?;
        self.buf_pos += num_bytes;

        Ok(())
    }

    #[allow(dead_code)]
    pub fn flush(&self) -> Result<(), Error> { self.mmap.flush_async() }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tantivy::schema::*;
    use std::io::{BufRead, BufReader};
    use std::fs::File;
    use bincode::*;
    use std::io::Read;

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

        let test_transaction = Transaction::new(Action::Create, 0, doc!(text => "Some text for a test"));
        let tt2 = Transaction::new(Action::Update, 0, doc!(text => "Some More text here..."));

        trans.add_transaction(test_transaction).unwrap();
        //trans.add_transaction(tt2).unwrap();

        trans.flush().unwrap();

        let mut read = PathBuf::from("data");
        read.push("test_index.0");
        let mut bytes = File::open(read).unwrap();
        let mut readz = vec![];
        bytes.read_to_end(&mut readz).unwrap();
        let b: Transaction = deserialize(&readz).unwrap();
        println!("{:?}", b);
    }

}
