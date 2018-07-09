### Road Map
So I am attempting to do this in phases of functionality. Mostly to potentially give something to test as an entire project
before moving on to some of the more complex aspects of this project.

#### 1.0 - Basic Single Node
The idea here is to be a "mostly" complete single server index machine. 
  - Index Catalog
  - Search Indexes (Range, Terms)
  - Add Documents to an Index
  - Create a new Index from a Tantivy Schema
  - Try my best to be on par with elastic's way of doing things
  
#### 2.0 - Elastic DSL
I want to tackle second the idea of the things that elastic does on top of lucene that don't come with lucene by default.
  - Aggregates
  - Metrics
  - Some of the more complex queries (Boosting, Scoring)
  - Shard Replication (Still Single Instance)
  
#### 3.0 - Clustering
So last, but not least, add in the multi-node based setup. I don't really see a reason to use whatever election and consensus
algorithm elastic uses when there exists so much information and good implementations of raft. With that in mind I think
raft will be how we do cluster coordination. 
  - Clustering (We're far off from here, so leaving this nebulous) 