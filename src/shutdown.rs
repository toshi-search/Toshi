use futures::{Future, FutureExt};
use tokio::sync::oneshot;
use tracing::*;

#[cfg_attr(tarpaulin, skip)]
#[cfg(unix)]
pub async fn shutdown(signal: oneshot::Sender<()>) -> Result<(), ()> {
    use tokio_signal::unix::{Signal, SIGINT, SIGTERM};

    let sigint = Signal::new(SIGINT).flatten_stream().map(|_| String::from("SIGINT"));
    let sigterm = Signal::new(SIGTERM).flatten_stream().map(|_| String::from("SIGTERM"));

    handle_shutdown(signal, sigint.select(sigterm))
}

#[cfg_attr(tarpaulin, skip)]
#[cfg(not(unix))]
pub async fn shutdown(signal: oneshot::Sender<()>) -> Result<(), ()> {
    let stream = tokio::signal::ctrl_c().map(|_| String::from("ctrl-c"));
    handle_shutdown(signal, stream).await
}

#[cfg_attr(tarpaulin, skip)]
pub async fn handle_shutdown<S>(signal: oneshot::Sender<()>, stream: S) -> Result<(), ()>
where
    S: Future<Output = String>,
{
    let s = stream.await;
    info!("Received signal: {}", s);

    info!("Gracefully shutting down...");
    signal.send(())
}
