use crate::query::{CreateQuery, KeyValue, Query, Search};
use crate::results::{ScoredDoc, SearchResults};
use crate::{Error, Result};
use tantivy::collector::{FacetCollector, MultiCollector, TopDocs};
use tantivy::query::{AllQuery, QueryParser};
use tantivy::schema::*;
use tantivy::{Index, IndexWriter, ReloadPolicy, Searcher, Term};

use log::debug;

pub fn search_index(index: &Index, searcher: &Searcher, search: Search) -> Result<SearchResults> {
    // let searcher = self.reader.searcher();
    // let schema = self.index.schema();
    let schema = index.schema();
    let collector = TopDocs::with_limit(search.limit);
    let mut multi_collector = MultiCollector::new();

    let top_handle = multi_collector.add_collector(collector);
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
        let mut scored_docs = match query {
            Query::Regex(regex) => {
                let regex_query = regex.create_query(&schema)?;
                debug!("{:?}", regex_query);
                searcher.search(&*regex_query, &multi_collector)?
            }
            Query::Phrase(phrase) => {
                let phrase_query = phrase.create_query(&schema)?;
                debug!("{:?}", phrase_query);
                searcher.search(&*phrase_query, &multi_collector)?
            }
            Query::Fuzzy(fuzzy) => {
                let fuzzy_query = fuzzy.create_query(&schema)?;
                debug!("{:?}", fuzzy_query);
                searcher.search(&*fuzzy_query, &multi_collector)?
            }
            Query::Exact(term) => {
                let exact_query = term.create_query(&schema)?;
                debug!("{:?}", exact_query);
                searcher.search(&*exact_query, &multi_collector)?
            }
            Query::Boolean { bool } => {
                let bool_query = bool.create_query(&schema)?;
                debug!("{:?}", bool_query);
                searcher.search(&*bool_query, &multi_collector)?
            }
            Query::Range(range) => {
                let range_query = range.create_query(&schema)?;
                debug!("{:?}", range_query);
                searcher.search(&*range_query, &multi_collector)?
            }
            Query::Raw { raw } => {
                let fields: Vec<Field> = schema.fields().iter().filter_map(|e| schema.get_field(e.name())).collect();
                let query_parser = QueryParser::for_index(&index, fields);
                let query = query_parser.parse_query(&raw)?;
                debug!("{:?}", query);
                searcher.search(&*query, &multi_collector)?
            }
            Query::All => searcher.search(&AllQuery, &multi_collector)?,
        };

        let docs = top_handle
            .extract(&mut scored_docs)
            .into_iter()
            .map(|(score, doc)| {
                let d = searcher.doc(doc).expect("Doc not found in segment");
                ScoredDoc::new(Some(score), schema.to_named_doc(&d))
            })
            .collect();

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
