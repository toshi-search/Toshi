use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use log::debug;
use tantivy::collector::TopCollector;
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::*;
use tantivy::{Document, Index, IndexWriter, Term};

use crate::handlers::index::{AddDocument, DeleteDoc, DocsAffected};
use crate::query::{CreateQuery, Query, Request};
use crate::results::{ScoredDoc, SearchResults};
use crate::settings::Settings;
use crate::Result;

pub enum IndexLocation {
    LOCAL,
    REMOTE,
}

pub trait IndexHandle {
    fn get_name(&self) -> String;
    fn index_location(&self) -> IndexLocation;
    fn search_index(&self, search: Request) -> Result<SearchResults>;
    fn add_document(&self, doc: AddDocument) -> Result<()>;
    fn delete_term(&self, term: DeleteDoc) -> Result<DocsAffected>;
}

/// Index handle that operates on an Index local to the node, a remote index handle
/// will eventually call to wherever the local index is stored, so at some level the relevant
/// local handle will always get called through rpc
pub struct LocalIndexHandle {
    index: Index,
    writer: Arc<Mutex<IndexWriter>>,
    current_opstamp: AtomicUsize,
    settings: Settings,
    name: String,
}

impl IndexHandle for LocalIndexHandle {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn index_location(&self) -> IndexLocation {
        IndexLocation::LOCAL
    }

    fn search_index(&self, search: Request) -> Result<SearchResults> {
        self.index.load_searchers()?;
        let searcher = self.index.searcher();
        let schema = self.index.schema();
        let mut collector = TopCollector::with_limit(search.limit);
        if let Some(query) = search.query {
            match query {
                Query::Regex(regex) => {
                    let regex_query = regex.create_query(&schema)?;
                    searcher.search(&*regex_query, &mut collector)?
                }
                Query::Phrase(phrase) => {
                    let phrase_query = phrase.create_query(&schema)?;
                    searcher.search(&*phrase_query, &mut collector)?
                }
                Query::Fuzzy(fuzzy) => {
                    let fuzzy_query = fuzzy.create_query(&schema)?;
                    searcher.search(&*fuzzy_query, &mut collector)?
                }
                Query::Exact(term) => {
                    let exact_query = term.create_query(&schema)?;
                    searcher.search(&*exact_query, &mut collector)?
                }
                Query::Boolean { bool } => {
                    let bool_query = bool.create_query(&schema)?;
                    searcher.search(&*bool_query, &mut collector)?
                }
                Query::Range(range) => {
                    debug!("{:#?}", range);
                    let range_query = range.create_query(&schema)?;
                    debug!("{:?}", range_query);
                    searcher.search(&*range_query, &mut collector)?
                }
                Query::Raw { raw } => {
                    let fields: Vec<Field> = schema.fields().iter().filter_map(|e| schema.get_field(e.name())).collect();
                    let query_parser = QueryParser::for_index(&self.index, fields);
                    let query = query_parser.parse_query(&raw)?;
                    debug!("{:#?}", query);
                    searcher.search(&*query, &mut collector)?
                }
                Query::All => searcher.search(&AllQuery, &mut collector)?,
            }
        }

        let scored_docs: Vec<ScoredDoc> = collector
            .top_docs()
            .into_iter()
            .map(|(score, doc)| {
                let d = searcher.doc(doc).expect("Doc not found in segment");
                ScoredDoc::new(Some(score), schema.to_named_doc(&d))
            })
            .collect();

        Ok(SearchResults::new(scored_docs))
    }

    fn add_document(&self, add_doc: AddDocument) -> Result<()> {
        let index_schema = self.index.schema();
        let writer_lock = self.get_writer();
        let mut index_writer = writer_lock.lock()?;
        let doc: Document = LocalIndexHandle::parse_doc(&index_schema, &add_doc.document.to_string())?;
        index_writer.add_document(doc);
        if let Some(opts) = add_doc.options {
            if opts.commit {
                index_writer.commit().unwrap();
                self.set_opstamp(0);
            }
        } else {
            self.set_opstamp(self.get_opstamp() + 1);
        }
        Ok(())
    }

    fn delete_term(&self, term: DeleteDoc) -> Result<DocsAffected> {
        let index_schema = self.index.schema();
        let writer_lock = self.get_writer();
        let mut index_writer = writer_lock.lock()?;

        for (field, value) in term.terms {
            let f = index_schema.get_field(&field).unwrap();
            let term = Term::from_field_text(f, &value);
            index_writer.delete_term(term);
        }
        if let Some(opts) = term.options {
            if opts.commit {
                index_writer.commit().unwrap();
                self.set_opstamp(0);
            }
        }
        let docs_affected = self
            .index
            .load_metas()
            .map(|meta| meta.segments.iter().map(|seg| seg.num_deleted_docs()).sum())
            .unwrap_or(0);

        Ok(DocsAffected { docs_affected })
    }
}

impl LocalIndexHandle {
    pub fn new(index: Index, settings: Settings, name: &str) -> Result<Self> {
        let i = index.writer(settings.writer_memory)?;
        i.set_merge_policy(settings.get_merge_policy());
        let current_opstamp = AtomicUsize::new(0);
        let writer = Arc::new(Mutex::new(i));
        Ok(Self {
            index,
            writer,
            current_opstamp,
            settings,
            name: name.into(),
        })
    }

    fn parse_doc(schema: &Schema, bytes: &str) -> Result<Document> {
        schema.parse_document(bytes).map_err(|e| e.into())
    }

    pub fn get_index(&self) -> &Index {
        &self.index
    }

    pub fn recreate_writer(self) -> Result<Self> {
        LocalIndexHandle::new(self.index, self.settings.clone(), &self.name)
    }

    pub fn get_writer(&self) -> Arc<Mutex<IndexWriter>> {
        Arc::clone(&self.writer)
    }

    pub fn get_opstamp(&self) -> usize {
        self.current_opstamp.load(Ordering::Relaxed)
    }

    pub fn set_opstamp(&self, opstamp: usize) {
        self.current_opstamp.store(opstamp, Ordering::Relaxed)
    }
}
