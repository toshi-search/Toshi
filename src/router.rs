use gotham::router::builder::*;
use gotham::router::Router;
use handlers::*;

pub fn router() -> Router {
    build_simple_router(|route| {
        route.associate("/", |r| {
            r.put().to(index_handler);
            r.post().to(search_handler);
        });
    })
}
