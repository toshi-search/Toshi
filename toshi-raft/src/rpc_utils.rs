use std::fs;
use std::path::PathBuf;

use http::Uri;
use slog::Logger;
use tantivy::directory::MmapDirectory;
use tantivy::schema::Schema;
use tantivy::Index;
use tonic::{transport, Code, Response, Status};

use toshi_proto::cluster_rpc::*;
use toshi_types::{Error, Search};

pub fn create_from_managed(mut base_path: PathBuf, index_path: &str, schema: Schema) -> Result<Index, Error> {
    base_path.push(index_path);
    if !base_path.exists() {
        fs::create_dir(&base_path)?;
    }
    let dir: MmapDirectory = MmapDirectory::open(base_path)?;
    Index::open_or_create(dir, schema).map_err(Into::into)
}

pub async fn create_client(uri: Uri, logger: Option<Logger>) -> Result<client::IndexServiceClient<transport::Channel>, transport::Error> {
    if let Some(log) = logger {
        slog::info!(log, "Creating Client to: {:?}", uri);
    }
    client::IndexServiceClient::connect(uri).await.map_err(Into::into)
}

pub fn ok_result() -> ResultReply {
    create_result(0, "".into())
}

pub fn create_result(code: i32, message: String) -> ResultReply {
    ResultReply { code, message }
}

pub fn create_search_reply(result: Option<ResultReply>, doc: Vec<u8>) -> SearchReply {
    SearchReply { result, doc }
}

pub fn error_response<T>(code: Code, msg: String) -> Result<Response<T>, Status> {
    let status = Status::new(code, msg);
    Err(status)
}

pub fn query_or_all(b: &[u8]) -> Result<Search, Error> {
    let deser: Search = serde_json::from_slice(b)?;
    if deser.query.is_none() {
        return Ok(Search::all_docs());
    }
    Ok(deser)
}
