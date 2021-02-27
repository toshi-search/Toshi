use hyper::body::to_bytes;
use hyper::{Body, Response, StatusCode};

use toshi_types::{Catalog, IndexHandle};
use toshi_types::{DeleteDoc, Error, SchemaBody};

use crate::handlers::ResponseFuture;
use crate::utils::{empty_with_code, error_response, with_body};
use crate::AddDocument;
use std::sync::Arc;

pub async fn delete_term<C: Catalog>(catalog: Arc<C>, body: Body, index: &str) -> ResponseFuture {
    if !catalog.exists(index) {
        return Ok(error_response(StatusCode::BAD_REQUEST, Error::UnknownIndex(index.to_string())));
    }
    let agg_body = to_bytes(body).await?;
    match serde_json::from_slice::<DeleteDoc>(&agg_body) {
        Ok(dd) => match catalog.get_index(index) {
            Ok(c) => c
                .delete_term(dd)
                .await
                .map(with_body)
                .or_else(|e| Ok(error_response(StatusCode::BAD_REQUEST, e))),
            Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, e)),
        },
        Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, e.into())),
    }
}

pub async fn create_index<C: Catalog>(catalog: Arc<C>, body: Body, index: &str) -> ResponseFuture {
    if catalog.exists(index) {
        return Ok(error_response(StatusCode::BAD_REQUEST, Error::AlreadyExists(index.to_string())));
    }
    let req = to_bytes(body).await?;
    match serde_json::from_slice::<SchemaBody>(&req) {
        Ok(schema_body) => match catalog.add_index(index, schema_body.0).await {
            Ok(_) => Ok(empty_with_code(StatusCode::CREATED)),
            Err(e) => Ok(Response::from(e)),
        },
        Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, e.into())),
    }
}

pub async fn add_document<C: Catalog>(catalog: Arc<C>, body: Body, index: &str) -> ResponseFuture {
    if !catalog.exists(index) {
        return Ok(error_response(StatusCode::BAD_REQUEST, Error::UnknownIndex(index.to_string())));
    }
    let full_body = to_bytes(body).await?;
    match serde_json::from_slice::<AddDocument>(&full_body) {
        Ok(v) => match catalog.get_index(index) {
            Ok(c) => c
                .add_document(v)
                .await
                .map(|_| empty_with_code(StatusCode::CREATED))
                .or_else(|e| Ok(error_response(StatusCode::BAD_REQUEST, e))),
            Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, e)),
        },
        Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, e.into())),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use toshi_types::IndexOptions;

    use crate::handlers::all_docs;
    use crate::index::create_test_catalog;

    use super::*;
    use crate::commit::tests::wait_json;

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
        remove_dir_all::remove_dir_all("new_index")?;
        Ok(())
    }

    #[tokio::test]
    async fn test_doc_create() {
        let shared_cat = create_test_catalog("test_index");
        let q = r#" {"options": {"commit": true }, "document": {"test_text": "Babbaboo!", "test_u64": 10, "test_i64": -10} }"#;
        let req = add_document(Arc::clone(&shared_cat), Body::from(q), &test_index()).await;
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
        let req = add_document(Arc::clone(&shared_cat), Body::from(body_bytes), &test_index())
            .await
            .unwrap()
            .into_body();
        let buf = hyper::body::to_bytes(req).await.unwrap();
        let str_buf = std::str::from_utf8(&buf).unwrap();
        assert_eq!(
            str_buf,
            "{\"message\":\"Error in Index: \'The provided string is not valid JSON\'\"}"
        )
    }
}
