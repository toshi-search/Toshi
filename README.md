# Toshi
##### A Full Text Search Engine in Rust Based on [Tantivy](https://github.com/tantivy-search/tantivy)

[![dependency status](https://deps.rs/repo/github/hntd187/toshi/status.svg)](https://deps.rs/repo/github/hntd187/toshi) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![Build Status](https://travis-ci.org/hntd187/Toshi.svg?branch=master)](https://travis-ci.org/hntd187/Toshi) [![codecov](https://codecov.io/gh/hntd187/Toshi/branch/master/graph/badge.svg)](https://codecov.io/gh/hntd187/Toshi) [![Coverage Status](https://coveralls.io/repos/github/hntd187/Toshi/badge.svg?branch=master)](https://coveralls.io/github/hntd187/Toshi?branch=master)

#### Description
Toshi is meant to be a full text search engine similar to ElasticSearch. Ideally, if what Tantivy is to Lucene, Toshi strives
to be that for ElasticSearch. 

#### Motivations
Toshi will always target stable rust and will try our best to never make any use of unsafe. While underlying libraries may make some 
use of unsafe, Toshi will make a concerted effort to vet these libraries in an effort to be completely free of unsafe Rust usage. The
reason I chose this was because I felt that for this to actually become an attractive option for people to consider it would have to have
be safe, stable and consistent. This was why stable rust was chosen because of the guarentees and safety it provides. I did not want to go down the rabbit hole of using nightly features to then have issues with their stability later on. Since Toshi is not 
meant to be a library I'm perfectly fine with having this requirement because people who would want to use this more than likely will 
take it off the shelf and not modify it. So my motivation was to cater to that usecase when building Toshi.

#### Build Requirements
At this current time Toshi should build and work fine on Windows, OSX and Linux. From dependency requirements you are going to be Rust >= 1.27 and cargo installed to build.

#### Configuration

There is a default config in config/config.toml

##### Host
`host = "localhost"`

The local hostname toshi will bind on upon start.

##### Port
`port = 8080`

The port toshi will bind to upon start.

##### Path
`path = "data/"`

This is the data path where Toshi will store it's data and indexes.

##### Writer Memory
`writer_memory = 200000000`

This is the amount of memory Toshi should allocate to commits for new documents in bytes.

##### Log Level
`log_level = "info"`

The informational level to use for Toshi's logging.

##### Json Parsing
`json_parsing_threads = 4`

When Toshi does a bulk ingest of documents it will spin up a number of threads to parse the document JSON as it's
received. This controls the number of threads spawned to handle this job.

##### Bulk Buffer
`bulk_buffer_size = 10000`

This will control the buffer size for parsing documents into an index. It will control the amount of memory a bulk ingest will
take up by blocking when the message buffer is filled. If you want to go totally off the rails you can set this to 0 in order to make
the buffer unbounded.

##### Merge Policy
```
[merge_policy]
kind = "log"
```

Tantivy will merge index segments according to the configuration outlined here. There are 2 options for this. "log" which is the default 
segment merge behavior. Log has 3 additional values to it as well. Any of these 3 values can be ommitted to use Tantivy's default value.
The default values are listed below.

```
min_merge_size = 8
min_layer_size = 10_000
level_log_size = 0.75
```

In addition there is the "nomerge" option, in which Tantivy will do no merging of segments.

#### Building and Running
Toshi can be build using `cargo build --release` once Toshi is built from the top level directory you can run `target/release/toshi` to
start toshi according to the configuration in config/config.toml

You should get a startup message like this.

```
  ______         __   _   ____                 __
 /_  __/__  ___ / /  (_) / __/__ ___ _________/ /
  / / / _ \(_-</ _ \/ / _\ \/ -_) _ `/ __/ __/ _ \
 /_/  \___/___/_//_/_/ /___/\__/\_,_/_/  \__/_//_/
 Such Relevance, Much Index, Many Search, Wow
 
 INFO  toshi::index > Indexes: []
 INFO  gotham::start >  Gotham listening on http://[::1]:8080 with 12 threads
```

You can verify Toshi is running with

```
curl -X GET http://localhost:8080/
```

Which should return

```
Toshi Search, Version: 0.1.0
```

Once Toshi is up and running we can create an Index. Toshi uses Tantivy so creating an index requires a Tantivy Schema. Let's create a 
simple one seen below.

```
curl -X PUT \
  http://localhost:8080/test_index \
  -H 'Content-Type: application/json' \
  -d '[
    {
      "name": "test_text",
      "type": "text",
      "options": {
        "indexing": {
          "record": "position",
          "tokenizer": "default"
        },
        "stored": true
      }
    },
    {
      "name": "test_i64",
      "type": "i64",
      "options": {
        "indexed": true,
        "stored": true
      }
    },
    {
      "name": "test_u64",
      "type": "u64",
      "options": {
        "indexed": true,
        "stored": true
      }
    }
  ]'
  ```
  
If everything succeeded we should receive a `201 CREATED` from this request and if you look in the data directory you configured you
should now see a directory for the test_index you just created.

Now we can add some documents to our Index.

```
curl -X PUT \
  http://localhost:8080/test_index \
  -H 'Content-Type: application/json' \
  -d '{
        "fields": [
          {"field": "test_text", "value": "Babbaboo!" },
          {"field": "test_u64",  "value": 10 },
          {"field": "test_i64",  "value": -10 }
        ]
    }'
```

And finally we can retreive all the documents in an index with a simple get call

```
curl -X GET http://localhost:8080/test_index -H 'Content-Type: application/json'
```

#### Running Tests

`cargo test`

#### Road Map
- 1.0 Single Node Parity with Elastic
- 2.0 Full Implementation of Elastic Search DSL
- 3.0 Cluster Distribution based on Raft

#### What is a Toshi?

Toshi is a three year old Shiba Inu. He is a very good boy and is the official mascot of this project. Toshi personally reviews all code before it is commited to this repository and is dedicated to only accepting the highest quality contributions from his human. He will though accept treats for easier code reviews. 
