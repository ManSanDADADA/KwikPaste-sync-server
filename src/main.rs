use anyhow::Context;
use kwikpaste_sync_server::config::{self, TokenSource};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let (config, listen, token_source) = config::from_env()?;
    let listener = TcpListener::bind(&listen)
        .await
        .with_context(|| format!("cannot listen on {listen}"))?;
    info!(
        "KwikPaste sync server {} ({:?} mode) listening on {listen}, data in {}",
        env!("CARGO_PKG_VERSION"),
        config.mode,
        config.data_dir.display()
    );
    match (&token_source, &config.server_token) {
        (Some(TokenSource::File(path)), Some(token)) => info!(
            "Server token: {token}  (saved in {}; enter it in KwikPaste together with this server's address)",
            path.display()
        ),
        (Some(TokenSource::Env), _) => info!("Server token: taken from KP_SYNC_TOKEN"),
        _ => {}
    }

    kwikpaste_sync_server::serve(config, listener, shutdown_signal()).await
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                term.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    info!("shutting down");
}
