use futures::{Future, Stream};
use tokio::sync::oneshot;
use tracing::*;

#[cfg_attr(tarpaulin, skip)]
#[cfg(unix)]
pub fn shutdown(signal: oneshot::Sender<()>) -> impl Future<Item = (), Error = ()> {
    use tokio_signal::unix::{Signal, SIGINT, SIGTERM};

    let sigint = Signal::new(SIGINT).flatten_stream().map(|_| String::from("SIGINT"));
    let sigterm = Signal::new(SIGTERM).flatten_stream().map(|_| String::from("SIGTERM"));

    handle_shutdown(signal, sigint.select(sigterm))
}

#[cfg_attr(tarpaulin, skip)]
#[cfg(not(unix))]
pub fn shutdown(signal: oneshot::Sender<()>) -> impl Future<Item = (), Error = ()> {
    let stream = tokio_signal::ctrl_c().flatten_stream().map(|_| String::from("ctrl-c"));
    handle_shutdown(signal, stream)
}

#[cfg_attr(tarpaulin, skip)]
pub fn handle_shutdown<S>(signal: oneshot::Sender<()>, stream: S) -> impl Future<Item = (), Error = ()>
where
    S: Stream<Item = String, Error = std::io::Error>,
{
    stream
        .take(1)
        .into_future()
        .and_then(move |(sig, _)| {
            if let Some(s) = sig {
                info!("Received signal: {}", s);
            }
            info!("Gracefully shutting down...");
            Ok(signal.send(()))
        })
        .map(|_| ())
        .map_err(|_| unreachable!("Signal handling should never error out"))
}
