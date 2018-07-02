use gotham::handler::{Handler, NewHandler};
use gotham::router::builder::*;
use gotham::router::Router;
use gotham::state::State;
use handlers::*;
use index::get_index;
use settings::SETTINGS;

pub fn router() -> Router {
    let handle = IndexHandler::new(get_index(&SETTINGS.path, None).unwrap());

    build_simple_router(|route| {
        route.associate("/", |r| {
            r.get().to_new_handler(handle);
            r.put().to(index_handler);
            r.post().to(search_handler);
        });
    })
}
