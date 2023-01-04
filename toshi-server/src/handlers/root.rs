use hyper::{Body, Response};

use crate::handlers::ResponseFuture;

pub fn toshi_info() -> String {
    format!("{{\"name\":\"Toshi Search\",\"version\":\"{}\"}}", "0.0.1")
}

pub async fn root() -> ResponseFuture {
    Ok(Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Body::from(toshi_info()))
        .unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::tests::read_body;

    #[tokio::test]
    async fn test_root() -> Result<(), Box<dyn std::error::Error>> {
        let req: Response<Body> = root().await?;
        let body = read_body(req).await?;
        assert_eq!(body, toshi_info());
        Ok(())
    }
}
