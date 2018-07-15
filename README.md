# Toshi
##### A Full Text Search Engine in Rust Based on [Tantivy](https://github.com/tantivy-search/tantivy)

[![dependency status](https://deps.rs/repo/github/hntd187/toshi/status.svg)](https://deps.rs/repo/github/hntd187/toshi) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![Build Status](https://travis-ci.org/hntd187/toshi.svg?branch=master)](https://travis-ci.org/hntd187/toshi) [![codecov](https://codecov.io/gh/hntd187/toshi/branch/master/graph/badge.svg)](https://codecov.io/gh/hntd187/toshi) [![Coverage Status](https://coveralls.io/repos/github/hntd187/toshi/badge.svg?branch=master)](https://coveralls.io/github/hntd187/toshi?branch=master)

#### Description
Tohsi is meant to be a full text search engine similar to ElasticSearch. Ideally, if what Tantivy is to Lucene, Toshi strives
to be that for ElasticSearch. 

#### Motivations
Toshi will always target stable rust and will try our best to never make any use of unsafe. While underlying libraries may make some 
use of unsafe, Toshi will make a concerted effort to vet these libraries in an effort to be completely free of unsafe Rust usage. The
reason I chose this was because I felt that for this to actually become an attractive option for people to consider it would have to have
be safe, stable and consistent. This was why stable rust was chosen because of the guarentees and safety it provides. Since Toshi is not 
meant to be a library I'm perfectly fine with having this requirement because people who would want to use this more than likely will 
take it off the shelf and not modify it. So my motivation was to cater to that usecase when building Toshi.

#### Build Requirements
At this current time Toshi should build and work fine on Windows, OSX and Linux. From dependency requirements you are going to be Rust >= 1.27 and cargo installed to build.

#### Configuration

Files somewhere?

#### Building

`cargo build --release`

#### Road Map
- 1.0 Single Node Parity with Elastic
- 2.0 Full Implementation of Elastic Search DSL
- 3.0 Cluster Distribution based on Raft

#### What is a Toshi?

Toshi is a three year old Shiba Inu. He is a very good boy and is the official mascot of this project.
