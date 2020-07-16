use bytes::{Buf, Bytes};
use hyper::body::aggregate;
use hyper::{Body, Response, StatusCode};

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

pub async fn delete_term(catalog: SharedCatalog, body: Body, index: &str) -> ResponseFuture {
    if !catalog.exists(index) {
        return Ok(empty_with_code(StatusCode::NOT_FOUND));
    }
    let agg_body = aggregate(body).await?;
    let b = agg_body.bytes();
    if let Ok(dd) = serde_json::from_slice::<DeleteDoc>(&b) {
        match delete_terms(catalog, dd, index).await {
            Ok(v) => Ok(with_body(v)),
            Err(_e) => Ok(empty_with_code(StatusCode::BAD_REQUEST)),
        }
    } else {
        Ok(empty_with_code(StatusCode::BAD_REQUEST))
    }
}

pub async fn create_index(catalog: SharedCatalog, body: Body, index: &str) -> ResponseFuture {
    let req = aggregate(body)
        .await
        .map(|body| serde_json::from_slice::<SchemaBody>(&body.bytes()));

    match req {
        Ok(Ok(req)) => {
            let base_path = catalog.base_path();
            match IndexCatalog::create_from_managed(base_path.into(), &index, req.0) {
                Ok(v) => match catalog.add_index(&index, v) {
                    Ok(_) => Ok(empty_with_code(StatusCode::CREATED)),
                    Err(e) => Ok(Response::from(e)),
                },
                Err(e) => Ok(Response::from(e)),
            }
        }
        Ok(Err(e)) => Ok(error_response(StatusCode::BAD_REQUEST, e.into())),
        Err(e) => Ok(error_response(StatusCode::BAD_REQUEST, e.into())),
    }
}

pub async fn add_document(catalog: SharedCatalog, body: Body, index: &str) -> ResponseFuture {
    let full_body: Bytes = aggregate(body).await?.to_bytes();
    let req = match serde_json::from_slice::<AddDocument>(&full_body) {
        Ok(v) => v,
        Err(err) => return Ok(error_response(StatusCode::BAD_REQUEST, err.into())),
    };
    let add = catalog.get_index(index).unwrap().add_document(req).await;

    add.map(|_| empty_with_code(StatusCode::CREATED))
        .or_else(|e| Ok(error_response(StatusCode::BAD_REQUEST, e)))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Buf;
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
        remove_dir_all::remove_dir_all("new_index").unwrap();
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
        let req_body = hyper::body::aggregate(req).await.unwrap();
        let buf = req_body.bytes();
        let str_buf = std::str::from_utf8(&buf).unwrap();
        assert_eq!(
            str_buf,
            "{\"message\":\"Error in Index: \'The provided string is not valid JSON\'\"}"
        )
    }
}
