use serde::Serialize;
use tower_web::*;

#[derive(Clone, Debug)]
pub struct RootHandler(ToshiInfo);

#[derive(Debug, Clone, Response)]
struct ToshiInfo {
    name: String,
    version: String,
}

impl RootHandler {
    pub fn new(version: &str) -> Self {
        RootHandler(ToshiInfo {
            version: version.into(),
            name: "Toshi Search".into(),
        })
    }
}

impl_web!(
    impl RootHandler {
        #[get("/")]
        #[content_type("application/json")]
        fn root(&self) -> Result<ToshiInfo, ()> {
            Ok(self.0.clone())
        }
    }
);

#[cfg(test)]
mod tests {

    use super::*;
    use crate::settings::VERSION;

    #[test]
    fn test_root() {
        let handler = RootHandler::new(VERSION);
        assert_eq!(handler.root().unwrap().version, VERSION)
    }
}
