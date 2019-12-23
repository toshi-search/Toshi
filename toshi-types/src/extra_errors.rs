use crate::error::{Error, ErrorResponse};
use tonic::{Code, Status};

impl From<Error> for http::Response<hyper::Body> {
    fn from(err: Error) -> Self {
        let body = ErrorResponse::new(err);
        let bytes = serde_json::to_vec(&body).unwrap();
        http::Response::new(hyper::Body::from(bytes))
    }
}

impl From<Error> for tonic::Status {
    fn from(e: Error) -> Self {
        Status::new(Code::Internal, format!("{:?}", e))
    }
}

impl From<tonic::Status> for Error {
    fn from(s: tonic::Status) -> Self {
        Error::RPCError(s.to_string())
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(e: tonic::transport::Error) -> Self {
        Error::RPCError(e.to_string())
    }
}
