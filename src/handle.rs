use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tantivy::schema::*;
use tantivy::space_usage::SearcherSpaceUsage;
use tantivy::{Document, Index, IndexReader, IndexWriter, ReloadPolicy, Term};
use tokio::prelude::*;
use tracing::*;

use crate::handlers::index::{AddDocument, DeleteDoc, DocsAffected};
use crate::query::Search;
use crate::results::SearchResults;
use crate::settings::Settings;
use crate::{error::Error, Result};

use toshi_query::search::search_index;

pub enum IndexLocation {
    LOCAL,
    REMOTE,
}

pub trait IndexHandle {
    type SearchResponse: IntoFuture;
    type DeleteResponse: IntoFuture;
    type AddResponse: IntoFuture;

    fn get_name(&self) -> String;
    fn index_location(&self) -> IndexLocation;
    fn search_index(&self, search: Search) -> Self::SearchResponse;
    fn add_document(&self, doc: AddDocument) -> Self::AddResponse;
    fn delete_term(&self, term: DeleteDoc) -> Self::DeleteResponse;
}

/// Index handle that operates on an Index local to the node, a remote index handle
/// will eventually call to wherever the local index is stored, so at some level the relevant
/// local handle will always get called through rpc
pub struct LocalIndex {
    index: Index,
    writer: Arc<RwLock<IndexWriter>>,
    reader: IndexReader,
    current_opstamp: Arc<AtomicUsize>,
    deleted_docs: Arc<AtomicU64>,
    settings: Settings,
    name: String,
}

impl Clone for LocalIndex {
    fn clone(&self) -> Self {
        Self {
            index: self.index.clone(),
            writer: Arc::clone(&self.writer),
            reader: self.reader.clone(),
            current_opstamp: Arc::clone(&self.current_opstamp),
            deleted_docs: Arc::clone(&self.deleted_docs),
            settings: self.settings.clone(),
            name: self.name.clone(),
        }
    }
}

impl PartialEq for LocalIndex {
    fn eq(&self, other: &LocalIndex) -> bool {
        self.name == *other.name
    }
}

impl Eq for LocalIndex {}

impl Hash for LocalIndex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.name.as_bytes());
    }
}

impl IndexHandle for LocalIndex {
    type SearchResponse = Result<SearchResults>;
    type DeleteResponse = Result<DocsAffected>;
    type AddResponse = Result<()>;

    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn index_location(&self) -> IndexLocation {
        IndexLocation::LOCAL
    }

    fn search_index(&self, search: Search) -> Self::SearchResponse {
        let searcher = self.reader.searcher();
        let results = search_index(&self.index, &searcher, search);
        results.map_err(|e| e.into())
    }

    fn add_document(&self, add_doc: AddDocument) -> Self::AddResponse {
        let index_schema = self.index.schema();
        let writer_lock = self.get_writer();
        {
            let index_writer = writer_lock.read();
            let doc: Document = LocalIndex::parse_doc(&index_schema, &add_doc.document.to_string())?;
            index_writer.add_document(doc);
        }
        if let Some(opts) = add_doc.options {
            if opts.commit {
                let mut commit_writer = writer_lock.write();
                commit_writer.commit()?;
                self.set_opstamp(0);
            } else {
                self.set_opstamp(self.get_opstamp() + 1);
            }
        } else {
            self.set_opstamp(self.get_opstamp() + 1);
        }
        Ok(())
    }

    fn delete_term(&self, term: DeleteDoc) -> Self::DeleteResponse {
        let index_schema = self.index.schema();
        let writer_lock = self.get_writer();
        let before: u64;
        {
            let index_writer = writer_lock.read();
            before = self.reader.searcher().num_docs();

            for (field, value) in term.terms {
                if let Some(f) = index_schema.get_field(&field) {
                    let term = Term::from_field_text(f, &value);
                    index_writer.delete_term(term);
                }
            }
        }
        if let Some(opts) = term.options {
            if opts.commit {
                let mut commit_writer = writer_lock.write();
                commit_writer.commit()?;
                self.set_opstamp(0);
            }
        }
        let docs_affected = before - self.reader.searcher().num_docs();
        let current = self.deleted_docs.load(Ordering::SeqCst);
        self.deleted_docs.store(current + docs_affected, Ordering::SeqCst);
        Ok(DocsAffected { docs_affected })
    }
}

impl LocalIndex {
    pub fn new(index: Index, settings: Settings, name: &str) -> Result<Self> {
        let i = index.writer(settings.writer_memory)?;
        i.set_merge_policy(settings.get_merge_policy());
        let current_opstamp = Arc::new(AtomicUsize::new(0));
        let writer = Arc::new(RwLock::new(i));
        let reader = index.reader_builder().reload_policy(ReloadPolicy::OnCommit).try_into()?;
        Ok(Self {
            index,
            reader,
            writer,
            current_opstamp,
            deleted_docs: Arc::new(AtomicU64::new(0)),
            settings,
            name: name.into(),
        })
    }

    fn parse_doc(schema: &Schema, bytes: &str) -> Result<Document> {
        schema.parse_document(bytes).map_err(Into::into)
    }

    pub fn get_space(&self) -> SearcherSpaceUsage {
        self.reader.searcher().space_usage()
    }

    pub fn get_index(&self) -> &Index {
        &self.index
    }

    pub fn recreate_writer(self) -> Result<Self> {
        LocalIndex::new(self.index, self.settings.clone(), &self.name)
    }

    pub fn get_writer(&self) -> Arc<RwLock<IndexWriter>> {
        Arc::clone(&self.writer)
    }

    pub fn get_opstamp(&self) -> usize {
        trace!("Got the opstamp");
        self.current_opstamp.load(Ordering::SeqCst)
    }

    pub fn set_opstamp(&self, opstamp: usize) {
        trace!("Setting stamp to {}", opstamp);
        self.current_opstamp.store(opstamp, Ordering::SeqCst)
    }
}
