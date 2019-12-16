use std::sync::Arc;

use bytes::Buf;
use futures::StreamExt;
use hyper::body::aggregate;
use hyper::{Body, Response, StatusCode};
use rand::random;
use tantivy::schema::*;
use tantivy::Index;

use toshi_types::error::Error;
use toshi_types::server::{DeleteDoc, DocsAffected, SchemaBody};

use crate::cluster::rpc_server::RpcClient;
use crate::handle::IndexHandle;
use crate::handlers::ResponseFuture;
use crate::index::{IndexCatalog, SharedCatalog};
use crate::utils::{empty_with_code, error_response, with_body};
use crate::AddDocument;

#[inline]
async fn add_index(catalog: SharedCatalog, name: String, index: Index) -> Result<(), Error> {
    catalog.lock().await.add_index(name, index)
}

#[inline]
async fn add_remote_index(catalog: SharedCatalog, name: String, clients: Vec<RpcClient>) -> Result<(), Error> {
    catalog.lock().await.add_multi_remote_index(name, clients)
}

async fn delete_terms(catalog: SharedCatalog, body: DeleteDoc, index: &str) -> Result<DocsAffected, Error> {
    let index_lock = catalog.lock().await;
    let index_handle = index_lock.get_index(index)?;
    index_handle.delete_term(body).await
}

async fn create_remote_index(_nodes: &[String], _index: String, _schema: Schema) -> Result<Vec<RpcClient>, tonic::transport::Error> {
    //    let futs = nodes
    //        .iter()
    //        .map(move |n| {
    //            async {
    //                let mut client = IndexCatalog::create_client(n.clone()).await.expect("Cannot creat client.");
    //                let index = index.clone();
    //                let schema = schema.clone();
    //
    //                let client_clone = client.clone();
    //                let schema_bytes = serde_json::to_vec(&schema).unwrap();
    //                let request = tonic::Request::new(PlaceRequest {
    //                    index,
    //                    schema: schema_bytes,
    //                });
    //                client.place_index(request).map(move |_| vec![client_clone]).await
    //            }
    //        })
    //        .collect::<FuturesUnordered<_>>();
    //    Ok(futs.concat().await)
    Ok(vec![])
}

pub async fn delete_term(catalog: SharedCatalog, body: Body, index: String) -> ResponseFuture {
    let cat = catalog;
    let agg_body = aggregate(body).await?;
    let b = agg_body.bytes();
    let req = match serde_json::from_slice::<DeleteDoc>(&b) {
        Ok(v) => v,
        Err(_e) => return Ok(empty_with_code(hyper::StatusCode::BAD_REQUEST)),
    };
    let docs_affected = match delete_terms(cat, req, &index).await {
        Ok(v) => with_body(v),
        Err(e) => return Ok(Response::from(e)),
    };

    Ok(docs_affected)
}

pub async fn create_index(catalog: SharedCatalog, mut body: Body, index: String) -> ResponseFuture {
    let cat = catalog;
    let mut b = vec![];
    while let Some(Ok(chunk)) = body.next().await {
        b.extend(chunk);
    }
    let req = serde_json::from_slice::<SchemaBody>(&b).unwrap();
    {
        let base_path = cat.lock().await.base_path().clone();
        let new_index: Index = match IndexCatalog::create_from_managed(base_path, &index, req.0.clone()) {
            Ok(v) => v,
            Err(e) => return Ok(Response::from(e)),
        };
        match add_index(Arc::clone(&cat), index.clone(), new_index).await {
            Ok(_) => (),
            Err(e) => return Ok(Response::from(e)),
        };
    }

    let expir = cat.lock().await.settings.experimental;
    if expir {
        let nodes = &cat.lock().await.settings.get_nodes();
        let clients = create_remote_index(&nodes, index.clone(), req.0).await.unwrap();
        add_remote_index(cat, index, clients).await.expect("Could not create index.");
        Ok(empty_with_code(StatusCode::CREATED))
    } else {
        Ok(empty_with_code(StatusCode::CREATED))
    }
}

pub async fn add_document(catalog: SharedCatalog, mut body: Body, index: String) -> ResponseFuture {
    let cat_clone = catalog;
    let mut b = vec![];
    while let Some(Ok(chunk)) = body.next().await {
        b.extend(chunk);
    }
    let req = serde_json::from_slice::<AddDocument>(&b).unwrap();
    let cat = cat_clone.lock().await;
    let location: bool = random();
    if location && cat.remote_exists(&index) {
        let add = cat.add_remote_document(&index, req).await;

        add.map(|_| empty_with_code(StatusCode::CREATED))
            .or_else(|e| Ok(error_response(StatusCode::BAD_REQUEST, e)))
    } else {
        let add = cat.add_local_document(&index, req).await;

        add.map(|_| empty_with_code(StatusCode::CREATED))
            .or_else(|e| Ok(error_response(StatusCode::BAD_REQUEST, e)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Buf;
    use pretty_assertions::assert_eq;

    use toshi_types::server::IndexOptions;

    use super::*;
    use crate::handlers::all_docs;
    use crate::handlers::search::tests::wait_json;
    use crate::index::tests::*;
    use std::convert::Infallible;
    use tokio::runtime::Runtime;

    fn test_index() -> String {
        String::from("test_index")
    }

    #[test]
    fn test_create_index() {
        let shared_cat = create_test_catalog("test_index");
        let schema = r#"[
            { "name": "test_text", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_unindex", "type": "text", "options": { "indexing": { "record": "position", "tokenizer": "default" }, "stored": true } },
            { "name": "test_i64", "type": "i64", "options": { "indexed": true, "stored": true } },
            { "name": "test_u64", "type": "u64", "options": { "indexed": true, "stored": true } }
         ]"#;
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(create_index(Arc::clone(&shared_cat), Body::from(schema), "new_index".into()))
            .unwrap();

        let docs = async {
            let resp = all_docs(Arc::clone(&shared_cat), "new_index".into()).await.unwrap();
            let b = wait_json::<crate::SearchResults>(resp).await;
            assert_eq!(b.hits, 0);
            Ok::<_, Infallible>(())
        };
        rt.block_on(docs).unwrap();
        remove_dir_all::remove_dir_all("new_index").unwrap();
    }

    #[test]
    fn test_doc_create() {
        let shared_cat = create_test_catalog("test_index");
        let body = async {
            let q = r#" {"options": {"commit": true }, "document": {"test_text": "Babbaboo!", "test_u64": 10, "test_i64": -10} }"#;
            let req = add_document(Arc::clone(&shared_cat), Body::from(q), test_index()).await;

            assert_eq!(req.is_ok(), true);
        };
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(body);
    }

    #[test]
    fn test_doc_delete() {
        let shared_cat = create_test_catalog("test_index");
        let req = async {
            let mut terms = HashMap::new();
            terms.insert(test_index(), "document".to_string());
            let delete = DeleteDoc {
                options: Some(IndexOptions { commit: true }),
                terms,
            };
            let body_bytes = serde_json::to_vec(&delete).unwrap();
            let del = delete_term(Arc::clone(&shared_cat), Body::from(body_bytes), test_index()).await;
            assert_eq!(del.is_ok(), true);
        };
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(req);
    }

    #[test]
    fn test_bad_json() {
        let shared_cat = create_test_catalog("test_index");
        let bad = async {
            let bad_json: serde_json::Value = serde_json::Value::String("".into());
            let add_doc = AddDocument {
                document: bad_json,
                options: None,
            };
            let body_bytes = serde_json::to_vec(&add_doc).unwrap();
            let req = add_document(Arc::clone(&shared_cat), Body::from(body_bytes), test_index())
                .await
                .unwrap()
                .into_body();
            let req_body = hyper::body::aggregate(req).await.unwrap();
            let buf = req_body.bytes();
            println!("{}", std::str::from_utf8(&buf).unwrap());
        };
        let mut rt: Runtime = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(bad);
    }
}
