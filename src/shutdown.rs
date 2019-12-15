use futures::{Future, FutureExt};
use tokio::sync::oneshot;
use tracing::*;

#[cfg_attr(tarpaulin, skip)]
#[cfg(unix)]
pub async fn shutdown(s: oneshot::Sender<()>) -> Result<(), ()> {
    use tokio::signal::unix::{signal, SignalKind};

    let sigint = signal(SignalKind::interrupt()).map(|_| String::from("SIGINT"));
    let sigterm = signal(SignalKind::terminate()).map(|_| String::from("SIGTERM"));

    handle_shutdown(s, sigint.select(sigterm)).await
}

#[cfg_attr(tarpaulin, skip)]
#[cfg(not(unix))]
pub fn shutdown(signal: oneshot::Sender<()>) -> impl Future<Output = Result<(), ()>> + Unpin + Send {
    let stream = tokio::signal::ctrl_c().map(|_| String::from("ctrl-c"));
    Box::pin(handle_shutdown(signal, Box::pin(stream)))
}

#[cfg_attr(tarpaulin, skip)]
pub async fn handle_shutdown<S>(signal: oneshot::Sender<()>, stream: S) -> Result<(), ()>
where
    S: Future<Output = String> + Unpin,
{
    let s = stream.await;
    info!("Received signal: {}", s);
    info!("Gracefully shutting down...");
    signal.send(())
}
