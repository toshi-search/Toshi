use futures::{Future, FutureExt};
use tokio::sync::oneshot;
use tracing::*;

#[cfg_attr(tarpaulin, skip)]
#[cfg(unix)]
pub fn shutdown(s: oneshot::Sender<()>) -> impl Future<Output = Result<(), ()>> + Unpin + Send {
    use futures::{future, Future, FutureExt, TryFuture, TryFutureExt};
    use tokio::signal::unix::{signal, SignalKind};

    let sigint = async {
        signal(SignalKind::interrupt()).unwrap().recv().await;
        String::from("sigint")
    };
    let sigterm = async {
        signal(SignalKind::terminate()).unwrap().recv().await;
        String::from("sigterm")
    };
    let sig = future::select(Box::pin(sigint), Box::pin(sigterm)).map(|_| String::from("Signal"));
    Box::pin(handle_shutdown(s, Box::pin(sig)))
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
