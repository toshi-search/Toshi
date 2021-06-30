use std::sync::Arc;

use toshi_types::Catalog;

use crate::handlers::ResponseFuture;
use crate::utils::with_body;

pub async fn list_indexes<C: Catalog>(catalog: Arc<C>) -> ResponseFuture {
    Ok(with_body(catalog.list_indexes().await))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::tests::read_body;
    use crate::index::create_test_catalog;

    #[tokio::test]
    async fn test_list() -> Result<(), Box<dyn std::error::Error>> {
        let catalog = create_test_catalog("test_index");
        let req = list_indexes(catalog).await?;
        let body = read_body(req).await?;
        assert_eq!(body, "[\"test_index\"]");
        Ok(())
    }
}
