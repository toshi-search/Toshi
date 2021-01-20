use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use log::*;
use tantivy::collector::{FacetCollector, MultiCollector, TopDocs};
use tantivy::directory::MmapDirectory;
use tantivy::merge_policy::MergePolicy;
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::*;
use tantivy::space_usage::SearcherSpaceUsage;
use tantivy::{Document, Index, IndexReader, IndexWriter, ReloadPolicy, Term};
use tokio::sync::*;

use toshi_types::*;

use crate::settings::{Settings, DEFAULT_WRITER_MEMORY};
use crate::Result;
use crate::{AddDocument, SearchResults};

/// Index handle that operates on an Index local to the node, a remote index handle
/// will eventually call to wherever the local index is stored, so at some level the relevant
/// local handle will always get called through rpc
#[derive(Clone)]
pub struct LocalIndex {
    index: Index,
    writer: Arc<Mutex<IndexWriter>>,
    reader: IndexReader,
    current_opstamp: Arc<AtomicUsize>,
    deleted_docs: Arc<AtomicU64>,
    name: String,
}

// impl Clone for LocalIndex {
//     fn clone(&self) -> Self {
//         Self {
//             index: self.index.clone(),
//             writer: Arc::clone(&self.writer),
//             reader: self.reader.clone(),
//             current_opstamp: Arc::clone(&self.current_opstamp),
//             deleted_docs: Arc::clone(&self.deleted_docs),
//             name: self.name.clone(),
//         }
//     }
// }

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

#[async_trait]
impl IndexHandle for LocalIndex {
    fn get_name(&self) -> String {
        self.name.clone()
    }

    fn index_location(&self) -> IndexLocation {
        IndexLocation::LOCAL
    }

    fn get_index(&self) -> Index {
        self.index.clone()
    }

    fn get_writer(&self) -> Arc<Mutex<IndexWriter>> {
        Arc::clone(&self.writer)
    }

    fn get_space(&self) -> SearcherSpaceUsage {
        self.reader.searcher().space_usage().unwrap()
    }

    fn get_opstamp(&self) -> usize {
        trace!("Got the opstamp");
        self.current_opstamp.load(Ordering::SeqCst)
    }

    fn set_opstamp(&self, opstamp: usize) {
        trace!("Setting stamp to {}", opstamp);
        self.current_opstamp.store(opstamp, Ordering::SeqCst)
    }

    async fn commit(&self) -> Result<u64> {
        let mut lock = self.writer.lock().await;
        Ok(lock.commit()?)
    }

    async fn search_index(&self, search: Search) -> Result<SearchResults> {
        let searcher = self.reader.searcher();
        let schema = self.index.schema();
        let mut multi_collector = MultiCollector::new();

        let sorted_top_handle = search.sort_by.clone().and_then(|sort_by| {
            info!("Sorting with: {}", sort_by);
            if let Some(f) = schema.get_field(&sort_by) {
                let entry = schema.get_field_entry(f);
                if entry.is_fast() && entry.is_stored() {
                    let c = TopDocs::with_limit(search.limit).order_by_u64_field(f);
                    return Some(multi_collector.add_collector(c));
                }
            }
            None
        });

        let top_handle = multi_collector.add_collector(TopDocs::with_limit(search.limit));
        let facet_handle = search.facets.clone().and_then(|f| {
            if let Some(field) = schema.get_field(&f.get_facets_fields()) {
                let mut col = FacetCollector::for_field(field);
                for term in f.get_facets_values() {
                    col.add_facet(&term);
                }
                Some(multi_collector.add_collector(col))
            } else {
                None
            }
        });

        if let Some(query) = search.query {
            let gen_query = match query {
                Query::Regex(regex) => regex.create_query(&schema)?,
                Query::Phrase(phrase) => phrase.create_query(&schema)?,
                Query::Fuzzy(fuzzy) => fuzzy.create_query(&schema)?,
                Query::Exact(term) => term.create_query(&schema)?,
                Query::Range(range) => range.create_query(&schema)?,
                Query::Boolean { bool } => bool.create_query(&schema)?,
                Query::Raw { raw } => {
                    let fields: Vec<Field> = schema.fields().filter_map(|f| schema.get_field(f.1.name())).collect();
                    let query_parser = QueryParser::for_index(&self.index, fields);
                    query_parser.parse_query(&raw)?
                }
                Query::All => Box::new(AllQuery),
            };

            trace!("{:?}", gen_query);
            let mut scored_docs = searcher.search(&*gen_query, &multi_collector)?;

            // FruitHandle isn't a public type which leads to some duplicate code like this.
            let docs: Vec<ScoredDoc<FlatNamedDocument>> = if let Some(h) = sorted_top_handle {
                h.extract(&mut scored_docs)
                    .into_iter()
                    .map(|(score, doc)| {
                        let d = searcher.doc(doc).expect("Doc not found in segment");
                        ScoredDoc::<FlatNamedDocument>::new(Some(score as f32), schema.to_named_doc(&d).into())
                    })
                    .collect()
            } else {
                top_handle
                    .extract(&mut scored_docs)
                    .into_iter()
                    .map(|(score, doc)| {
                        let d = searcher.doc(doc).expect("Doc not found in segment");
                        ScoredDoc::<FlatNamedDocument>::new(Some(score), schema.to_named_doc(&d).into())
                    })
                    .collect()
            };

            if let Some(facets) = facet_handle {
                if let Some(t) = &search.facets {
                    let facet_counts = facets
                        .extract(&mut scored_docs)
                        .get(&t.get_facets_values()[0])
                        .map(|(f, c)| KeyValue::new(f.to_string(), c))
                        .collect();
                    return Ok(SearchResults::with_facets(docs, facet_counts));
                }
            }
            Ok(SearchResults::new(docs))
        } else {
            Err(Error::QueryError("Empty Query Provided".into()))
        }
    }

    async fn add_document(&self, add_doc: AddDocument) -> Result<()> {
        let index_schema = self.index.schema();
        let writer_lock = self.get_writer();
        {
            let index_writer = writer_lock.lock().await;
            let doc: Document = LocalIndex::parse_doc(&index_schema, &add_doc.document.to_string())?;
            index_writer.add_document(doc);
        }
        if let Some(opts) = add_doc.options {
            if opts.commit {
                let mut commit_writer = writer_lock.lock().await;
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

    async fn delete_term(&self, term: DeleteDoc) -> Result<DocsAffected> {
        let index_schema = self.index.schema();
        let writer_lock = self.get_writer();
        let before: u64;
        {
            let index_writer = writer_lock.lock().await;
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
                let mut commit_writer = writer_lock.lock().await;
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
    pub fn new(
        mut base_path: PathBuf,
        index_name: &str,
        schema: Schema,
        writer_memory: usize,
        merge_policy: Box<dyn MergePolicy>,
    ) -> Result<Self> {
        base_path.push(index_name);
        if !base_path.exists() {
            fs::create_dir(&base_path)?;
        }
        let dir = MmapDirectory::open(base_path)?;
        let index = Index::open_or_create(dir, schema)?;
        let i = index.writer(writer_memory)?;
        i.set_merge_policy(merge_policy);
        let current_opstamp = Arc::new(AtomicUsize::new(0));
        let writer = Arc::new(Mutex::new(i));
        let reader = index.reader_builder().reload_policy(ReloadPolicy::OnCommit).try_into()?;
        Ok(Self {
            index,
            reader,
            writer,
            current_opstamp,
            deleted_docs: Arc::new(AtomicU64::new(0)),
            name: index_name.into(),
        })
    }

    pub(crate) fn with_existing(name: String, index: Index) -> Result<Self> {
        let i = index.writer(DEFAULT_WRITER_MEMORY)?;
        i.set_merge_policy(Settings::default().get_merge_policy());
        let current_opstamp = Arc::new(AtomicUsize::new(0));
        let writer = Arc::new(Mutex::new(i));
        let reader = index.reader_builder().reload_policy(ReloadPolicy::OnCommit).try_into()?;
        Ok(Self {
            index,
            reader,
            writer,
            current_opstamp,
            deleted_docs: Arc::new(AtomicU64::new(0)),
            name,
        })
    }

    fn parse_doc(schema: &Schema, bytes: &str) -> Result<Document> {
        schema.parse_document(bytes).map_err(Into::into)
    }
}
