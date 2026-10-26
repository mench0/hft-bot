//! HFT-бот на Rust. Сейчас — каркас: состояние и админ-API в формате Java-бота (`hft-trader`), чтобы к нему
//! подключалась та же админка (`hft-admin`). Подключений к биржам и торговли пока нет.
//!
//! Переменные окружения: `ADMIN_PORT` (8080), `ADMIN_BIND` (127.0.0.1), `ADMIN_TOKEN`, `ADMIN_CORS_ORIGINS`
//! (через запятую), `STATE_FILE` (state.json), `RUST_LOG` (info).

mod account;
mod api;
mod aster_api;
mod book;
mod bybit_api;
mod data;
mod evm;
mod feed;
mod leadlag;
mod live;
mod market;
mod okx_api;
mod paper;
mod perparb;
mod rest;
mod screener;
mod signed;
mod spotarb;
mod spread;
mod statarb;
mod state;
mod transfer;
mod util;

use std::net::SocketAddr;
use std::sync::Arc;

fn env(key: &str, def: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| def.to_owned())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default(); // TLS для wss:// бирж
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let bind = env("ADMIN_BIND", "127.0.0.1");
    let port: u16 = env("ADMIN_PORT", "8080").parse()?;
    let cfg = api::AdminConfig {
        token: env("ADMIN_TOKEN", ""),
        cors_origins: env("ADMIN_CORS_ORIGINS", "")
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect(),
    };
    if cfg.token.is_empty() {
        tracing::warn!(
            "ADMIN_TOKEN не задан — админка без токена; открывайте порт только через SSH-туннель"
        );
    }
    let bot = Arc::new(state::Bot::load(env("STATE_FILE", "state.json").into()));
    bot.spawn_screener();
    let addr: SocketAddr = format!("{bind}:{port}").parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("Админка: http://{addr}/control/status (заголовок X-Admin-Token)");
    axum::serve(listener, api::router(bot, cfg))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("Остановка");
        })
        .await?;
    Ok(())
}
