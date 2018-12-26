#![allow(dead_code, unused_variables)]

use crate::cluster::cluster_rpc::client;
use crate::handle::{IndexHandle, IndexLocation};
use crate::handlers::index::{AddDocument, DeleteDoc, DocsAffected};
use crate::query::Request;
use crate::Result;
use crate::results::SearchResults;
use crate::settings::Settings;

/// A reference to an index stored somewhere else on the cluster, this operates via calling
/// the remote host and full filling the request via rpc
pub struct RemoteIndexHandle<C> {
  index_client: client::IndexService<C>,
  settings: Settings,
  name: String
}

impl<C> IndexHandle for RemoteIndexHandle<C> {
  fn get_name(&self) -> String {
    self.name.clone()
  }

  fn index_location(&self) -> IndexLocation {
    IndexLocation::REMOTE
  }

  fn search_index(&self, search: Request) -> Result<SearchResults> {
    unimplemented!()
  }

  fn add_document(&self, doc: AddDocument) -> Result<()> {
    unimplemented!()
  }

  fn delete_term(&self, term: DeleteDoc) -> Result<DocsAffected> {
    unimplemented!()
  }
}