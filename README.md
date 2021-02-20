# Toshi
##### A Full-Text Search Engine in Rust

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) 
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/4751c082efd74f849b5274d74c284c87)](https://app.codacy.com/app/shcarman/Toshi?utm_source=github.com&utm_medium=referral&utm_content=toshi-search/Toshi&utm_campaign=Badge_Grade_Settings)
[![Actions Status](https://github.com/toshi-search/toshi/workflows/toshi-push/badge.svg)](https://github.com/toshi-search/toshi/actions)
[![codecov](https://codecov.io/gh/toshi-search/Toshi/branch/master/graph/badge.svg)](https://codecov.io/gh/toshi-search/Toshi) 
[![Join the chat at https://gitter.im/toshi-search/Toshi](https://badges.gitter.im/toshi-search/Toshi.svg)](https://gitter.im/toshi-search/Toshi?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![dependency status](https://deps.rs/repo/github/toshi-search/toshi/status.svg)](https://deps.rs/repo/github/toshi-search/toshi)

> *Please note that this is far from production ready, also Toshi is still under active development, I'm just slow.*

#### Description
Toshi is meant to be a full-text search engine similar to Elasticsearch. Toshi strives
to be to Elasticsearch what [Tantivy](https://github.com/tantivy-search/tantivy) is to Lucene. 

#### Motivations
Toshi will always target stable Rust and will try our best to never make any use of unsafe Rust. While underlying libraries may make some 
use of unsafe, Toshi will make a concerted effort to vet these libraries in an effort to be completely free of unsafe Rust usage. The
reason I chose this was because I felt that for this to actually become an attractive option for people to consider it would have to have
be safe, stable and consistent. This was why stable Rust was chosen because of the guarantees and safety it provides. I did not want to go down the rabbit hole of using nightly features to then have issues with their stability later on. Since Toshi is not 
meant to be a library, I'm perfectly fine with having this requirement because people who would want to use this more than likely will 
take it off the shelf and not modify it. My motivation was to cater to that use case when building Toshi.

#### Build Requirements
At this current time Toshi should build and work fine on Windows, Mac OS X, and Linux. From dependency requirements you are going to need 1.39.0 and Cargo installed in order to build. You can get rust easily from
[rustup](https://rustup.rs).

#### Configuration

There is a default configuration file in config/config.toml:

```toml
host = "127.0.0.1"
port = 8080
path = "data2/"
writer_memory = 200000000
log_level = "info"
json_parsing_threads = 4
bulk_buffer_size = 10000
auto_commit_duration = 10
experimental = false

[experimental_features]
master = true
nodes = [
    "127.0.0.1:8081"
]

[merge_policy]
kind = "log"
min_merge_size = 8
min_layer_size = 10_000
level_log_size = 0.75
```

##### Host
`host = "localhost"`

The hostname Toshi will bind to upon start.

##### Port
`port = 8080`

The port Toshi will bind to upon start.

##### Path
`path = "data/"`

The data path where Toshi will store its data and indices.

##### Writer Memory
`writer_memory = 200000000`

The amount of memory (in bytes) Toshi should allocate to commits for new documents.

##### Log Level
`log_level = "info"`

The detail level to use for Toshi's logging.

##### Json Parsing
`json_parsing_threads = 4`

When Toshi does a bulk ingest of documents it will spin up a number of threads to parse the document's json as it's
received. This controls the number of threads spawned to handle this job.

##### Bulk Buffer
`bulk_buffer_size = 10000`

This will control the buffer size for parsing documents into an index. It will control the amount of memory a bulk ingest will
take up by blocking when the message buffer is filled. If you want to go totally off the rails you can set this to 0 in order to make the buffer unbounded.

##### Auto Commit Duration
`auto_commit_duration = 10`

This controls how often an index will automatically commit documents if there are docs to be committed. Set this to 0 to disable this feature, but you will have to do commits yourself when you submit documents. 

##### Merge Policy
```toml
[merge_policy]
kind = "log"
```

Tantivy will merge index segments according to the configuration outlined here. There are 2 options for this. "log" which is the default 
segment merge behavior. Log has 3 additional values to it as well. Any of these 3 values can be omitted to use Tantivy's default value.
The default values are listed below.

```toml
min_merge_size = 8
min_layer_size = 10_000
level_log_size = 0.75
```

In addition there is the "nomerge" option, in which Tantivy will do no merging of segments.

##### Experimental Settings
```toml
experimental = false

[experimental_features]
master = true
nodes = [
    "127.0.0.1:8081"
]
```

In general these settings aren't ready for usage yet as they are very unstable or flat out broken. Right now the distribution of Toshi
is behind this flag, so if experimental is set to false then all these settings are ignored.


#### Building and Running
Toshi can be built using `cargo build --release`. Once Toshi is built you can run `./target/release/toshi` from the top level directory to start Toshi according to the configuration in config/config.toml

You should get a startup message like this.

```bash
  ______         __   _   ____                 __
 /_  __/__  ___ / /  (_) / __/__ ___ _________/ /
  / / / _ \(_-</ _ \/ / _\ \/ -_) _ `/ __/ __/ _ \
 /_/  \___/___/_//_/_/ /___/\__/\_,_/_/  \__/_//_/
 Such Relevance, Much Index, Many Search, Wow
 
 INFO  toshi::index > Indexes: []
```

You can verify Toshi is running with:

```bash
curl -X GET http://localhost:8080/
```

which should return:

```json
{
  "name": "Toshi Search",
  "version": "0.1.1"
}
```
Once toshi is running it's best to check the `requests.http` file in the root of this project to see some more examples of usage.

#### Example Queries
##### Term Query
```json
{ "query": {"term": {"test_text": "document" } }, "limit": 10 }
```
##### Fuzzy Term Query
```json
{ "query": {"fuzzy": {"test_text": {"value": "document", "distance": 0, "transposition": false } } }, "limit": 10 }
```
##### Phrase Query
```json
{ "query": {"phrase": {"test_text": {"terms": ["test","document"] } } }, "limit": 10 }
```
##### Range Query
```json
{ "query": {"range": { "test_i64": { "gte": 2012, "lte": 2015 } } }, "limit": 10 }
```
##### Regex Query
```json
{ "query": {"regex": { "test_text": "d[ou]{1}c[k]?ument" } }, "limit": 10 }
```
##### Boolean Query
```json
{ "query": {"bool": {"must": [ { "term": { "test_text": "document" } } ], "must_not": [ {"range": {"test_i64": { "gt": 2017 } } } ] } }, "limit": 10 }
```

##### Usage
To try any of the above queries you can use the above example
```bash
curl -X POST http://localhost:8080/test_index -H 'Content-Type: application/json' -d '{ "query": {"term": {"test_text": "document" } }, "limit": 10 }'
```
Also, to note, limit is optional, 10 is the default value. It's only included here for completeness.

#### Running Tests

`cargo test`

#### What is a Toshi?

Toshi is a three year old Shiba Inu. He is a very good boy and is the official mascot of this project. Toshi personally reviews all code before it is committed to this repository and is dedicated to only accepting the highest quality contributions from his human. He will, though, accept treats for easier code reviews. 
