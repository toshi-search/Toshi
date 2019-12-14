use hyper::{Body, Response};

use crate::handlers::ResponseFuture;

#[inline]
fn toshi_info() -> String {
    format!("{{\"name\":\"Toshi Search\",\"version\":\"{}\"}}", clap::crate_version!())
}

pub async fn root() -> ResponseFuture {
    let resp = Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Body::from(toshi_info()))
        .unwrap();

    Ok(resp)
}

#[cfg(test)]
mod tests {

    use super::*;
    use bytes::Buf;
    use tokio::runtime::Runtime;

    #[test]
    fn test_root() -> Result<(), hyper::Error> {
        let mut runtime = Runtime::new().unwrap();

        let req = async {
            let req: Response<Body> = root().await?;
            hyper::body::aggregate(req.into_body()).await
        };
        let result = runtime.block_on(req)?;
        let result_bytes = result.bytes();

        let str_result = std::str::from_utf8(result_bytes).unwrap();
        assert_eq!(str_result, toshi_info());
        Ok(())
    }
}
