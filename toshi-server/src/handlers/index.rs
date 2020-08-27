use hyper::body::to_bytes;
use hyper::{Body, Response, StatusCode};
use log::info;
use rand::random;
use tantivy::schema::*;
use tantivy::Index;
use tokio::sync::mpsc::Sender;

use toshi_proto::cluster_rpc::*;
use toshi_raft::rpc_server::RpcClient;
use toshi_types::{Catalog, IndexHandle};
use toshi_types::{DeleteDoc, DocsAffected, Error, SchemaBody};

use crate::handlers::ResponseFuture;
use crate::index::{IndexCatalog, SharedCatalog};
use crate::utils::{empty_with_code, error_response, with_body};
use crate::AddDocument;

async fn delete_terms(catalog: SharedCatalog, body: DeleteDoc, index: &str) -> Result<DocsAffected, Error> {
    let index_handle = catalog.get_index(index)?;
    index_handle.delete_term(body).await
}

async fn create_remote_index(nodes: &[String], index: String, schema: Schema) -> Result<Vec<RpcClient>, Error> {
    let mut clients = Vec::with_capacity(nodes.len());
    for n in nodes {
        let mut client = IndexCatalog::create_client(n.clone()).await?;
        let schema_bytes = serde_json::to_vec(&schema)?;
        let request = tonic::Request::new(PlaceRequest {
            index: index.clone(),
            schema: schema_bytes,
        });
        client.place_index(request).await?;
        clients.push(client);
    }

    Ok(clients)
}

pub async fn delete_term(catalog: SharedCatalog, body: Body, index: &str) -> ResponseFuture {
    let cat = catalog;
    let b = to_bytes(body).await?;
    let req = match serde_json::from_slice::<DeleteDoc>(&b) {
        Ok(v) => v,
        Err(_e) => return Ok(empty_with_code(hyper::StatusCode::BAD_REQUEST)),
    };
    let docs_affected = match delete_terms(cat, req, index).await {
        Ok(v) => with_body(v),
        Err(e) => return Ok(Response::from(e)),
    };

    Ok(docs_affected)
}

macro_rules! try_response {
    ($S: expr) => {
        match $S {
            Ok(v) => v,
            Err(e) => return Ok(Response::from(e)),
        }
    };
}

pub async fn create_index(catalog: SharedCatalog, body: Body, index: &str) -> ResponseFuture {
    let req = to_bytes(body)
        .await
        .map(|body_bytes| serde_json::from_slice::<SchemaBody>(&body_bytes));

    match req {
        Ok(Ok(req)) => {
            let base_path = catalog.base_path();
            let new_index: Index = try_response!(IndexCatalog::create_from_managed(base_path.into(), &index, req.0.clone()));
            try_response!(catalog.add_index(&index, new_index));

            let expir = catalog.settings.experimental;
            if expir {
                let nodes = &catalog.settings.get_nodes();
                match create_remote_index(&nodes, index.to_string(), req.0).await {
                    Ok(clients) => match catalog.add_multi_remote_index(index.to_string(), clients).await {
                        Ok(_) => Ok(empty_with_code(StatusCode::CREATED)),
                        Err(e) => Ok(Response::from(e)),
                    },
                    Err(e) => Ok(Response::from(e)),
                }
            } else {
                Ok(empty_with_code(StatusCode::CREATED))
            }
        }
        Ok(Err(e)) => Ok(error_response(StatusCode::BAD_REQUEST, Error::IOError(e.to_string()))),
        Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, Error::IOError(e.to_string()))),
    }
}

pub async fn add_document(catalog: SharedCatalog, body: Body, index: &str, raft_sender: Option<Sender<Message>>) -> ResponseFuture {
    let b = to_bytes(body).await?;
    let req = serde_json::from_slice::<AddDocument>(&b).unwrap();
    let location: bool = random();
    info!("LOCATION = {}", location);
    if location && catalog.remote_exists(index).await {
        info!("Pushing to remote...");
        let add = catalog.add_remote_document(index, req).await;

        add.map(|_| empty_with_code(StatusCode::CREATED))
            .or_else(|e| Ok(error_response(StatusCode::BAD_REQUEST, e)))
    } else {
        info!("Pushing to local...");
        if let Some(mut sender) = raft_sender {
            let mut msg = Message::default();
            msg.set_msg_type(MessageType::MsgPropose);
            msg.from = catalog.raft_id();
            let mut entry = Entry::default();
            entry.context = index.into();
            entry.data = match serde_json::to_vec(&req.document) {
                Ok(v) => v,
                Err(e) => return Ok(error_response(StatusCode::BAD_REQUEST, e.into())),
            };
            msg.entries = vec![entry].into();
            sender.send(msg).await.unwrap();
        }
        let add = catalog.add_local_document(index, req).await;

        add.map(|_| empty_with_code(StatusCode::CREATED))
            .or_else(|e| Ok(error_response(StatusCode::BAD_REQUEST, e)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use toshi_test::wait_json;
    use toshi_types::IndexOptions;

    use crate::handlers::all_docs;
    use crate::index::create_test_catalog;

    use super::*;

    fn test_index() -> String {
        String::from("test_index")
    }

    #[tokio::test]
    async fn test_create_index() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let shared_cat = create_test_catalog("test_index");
        let schema = r#"[
            { "name": "test_text", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_unindex", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_i64", "type": "i64", "options": { "indexed": true, "stored": true } },
            { "name": "test_u64", "type": "u64", "options": { "indexed": true, "stored": true } }
         ]"#;

        create_index(Arc::clone(&shared_cat), Body::from(schema), "new_index").await?;
        let resp = all_docs(Arc::clone(&shared_cat), "new_index").await?;
        let b = wait_json::<crate::SearchResults>(resp).await;
        assert_eq!(b.hits, 0);
        remove_dir_all::remove_dir_all("new_index").map_err(Into::into)
    }

    #[tokio::test]
    async fn test_doc_create() {
        let shared_cat = create_test_catalog("test_index");
        let q = r#" {"options": {"commit": true }, "document": {"test_text": "Babbaboo!", "test_u64": 10, "test_i64": -10} }"#;
        let req = add_document(Arc::clone(&shared_cat), Body::from(q), &test_index(), None).await;
        assert_eq!(req.is_ok(), true);
    }

    #[tokio::test]
    async fn test_doc_channel() {
        let shared_cat = create_test_catalog("test_index");
        let q = r#" {"options": {"commit": true }, "document": {"test_text": "Babbaboo!", "test_u64": 10, "test_i64": -10} }"#;
        let (snd, mut rcv) = tokio::sync::mpsc::channel(1024);
        let req = add_document(Arc::clone(&shared_cat), Body::from(q), &test_index(), Some(snd)).await;

        rcv.recv().await;
        assert_eq!(req.is_ok(), true);
    }

    #[tokio::test]
    async fn test_doc_delete() {
        let shared_cat = create_test_catalog("test_index");

        let mut terms = HashMap::new();
        terms.insert(test_index(), "document".to_string());
        let delete = DeleteDoc {
            options: Some(IndexOptions { commit: true }),
            terms,
        };
        let body_bytes = serde_json::to_vec(&delete).unwrap();
        let del = delete_term(Arc::clone(&shared_cat), Body::from(body_bytes), &test_index()).await;
        assert_eq!(del.is_ok(), true);
    }

    #[tokio::test]
    async fn test_bad_json() {
        let shared_cat = create_test_catalog("test_index");

        let bad_json: serde_json::Value = serde_json::Value::String("".into());
        let add_doc = AddDocument {
            document: bad_json,
            options: None,
        };
        let body_bytes = serde_json::to_vec(&add_doc).unwrap();
        let req = add_document(Arc::clone(&shared_cat), Body::from(body_bytes), &test_index(), None)
            .await
            .unwrap()
            .into_body();
        let buf = hyper::body::to_bytes(req).await.unwrap();
        let str_buf = std::str::from_utf8(&buf).unwrap();
        assert_eq!(str_buf, "{\"message\":\"IO Error: Document: '\\\"\\\"' is not valid JSON\"}")
    }
}
