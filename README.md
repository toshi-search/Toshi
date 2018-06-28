# Toshi
##### A Full Text Search Engine in Rust Based on [Tantivy](https://github.com/tantivy-search/tantivy)

#### Description
Tohsi is meant to be a full text search engine similar to ElasticSearch. Ideally, if what Tantivy is to Lucene, Toshi strives
to be that for ElasticSearch. 

#### Build Requirements
Toshi will always target stable rust and will never make any use of unsafe. While underlying libraries may make some 
use of unsafe, Toshi will make a concerted effort to vet these libraries in an effort to be completely free
of unsafe Rust usage.

#### Configuration

Files somewhere?

#### Building

`cargo build --release`

####Road Map
- 1.0 Single Node Parity with Elastic
- 2.0 Full Implementation of Elastic Search DSL
- 3.0 Cluster Distribution based on Raft

#### What is a Toshi?

