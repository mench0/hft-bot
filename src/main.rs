//! HFT-бот на Rust: стаканы и ордера бирж (WebSocket в первую очередь, REST — запасной путь), бумажный и живой счёт,
//! стратегии, подбор тикеров; админ-API в формате Java-бота (`hft-trader`) для той же админки (`hft-admin`).
//! Код по папкам: server, exchange, market, trading, strategy, discovery, record, infra (см. README).
//!
//! Переменные окружения: `ADMIN_PORT` (8080), `ADMIN_BIND` (127.0.0.1), `ADMIN_TOKEN`, `ADMIN_CORS_ORIGINS`
//! (через запятую), `STATE_DB` (data/state.db, SQLite), `STATE_FILE` (JSON-файл вместо SQLite), `RUST_LOG` (info).
//! `hft-bot record-cat <файл>...` — вывести файл записи событий (record) JSON-строками; `hft-bot export` — запись
//! за день в таблицы CSV; `hft-bot fetch` — бесплатная история Binance и Bybit в те же таблицы (README, «Исследования»).

mod discovery;
mod exchange;
mod infra;
mod market;
mod record;
mod server;
mod strategy;
mod trading;

use infra::{metrics, store, util};
use server::{api, state};
use std::net::SocketAddr;
use std::sync::Arc;

fn env(key: &str, def: &str) -> String {
    util::env(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| def.to_owned())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // hft-bot record-cat <файл>... — события файла записи JSON-строками
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("record-cat") => return record::reader::cat(&args[1..]).map_err(Into::into),
        Some("export") => return record::export::cli(&args[1..]).map_err(Into::into),
        Some("report") => return record::report::cli(&args[1..]).map_err(Into::into),
        Some("fetch") => return record::fetch::cli(&args[1..]).await.map_err(Into::into),
        _ => {}
    }
    let _ = rustls::crypto::ring::default_provider().install_default(); // TLS для wss:// бирж
    metrics::mark_start();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let bind = env("ADMIN_BIND", "127.0.0.1");
    let port: u16 = env("ADMIN_PORT", "8080").parse()?;
    let cfg = api::AdminConfig {
        token: env("ADMIN_TOKEN", ""),
        metrics_auth: env("METRICS_AUTH", "").eq_ignore_ascii_case("true"),
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
    // состояние: STATE_DB (SQLite, по умолчанию data/state.db, как в Java); STATE_FILE без STATE_DB — прежний JSON-файл
    let state_path: std::path::PathBuf = match (
        util::env("STATE_DB").ok().filter(|v| !v.trim().is_empty()),
        util::env("STATE_FILE").ok(),
    ) {
        (Some(db), _) => db.into(),
        (None, Some(f)) if !f.trim().is_empty() => f.into(),
        _ => {
            let db = std::path::PathBuf::from("data/state.db");
            store::migrate_json(&db, std::path::Path::new("state.json"));
            db
        }
    };
    let bot = Arc::new(state::Bot::load(state_path));
    bot.spawn_screener();
    bot.spawn_signals();
    let addr: SocketAddr = format!("{bind}:{port}").parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("Админка: http://{addr}/control/status (заголовок X-Admin-Token)");
    axum::serve(
        listener,
        api::router(bot, cfg).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Остановка");
    })
    .await?;
    Ok(())
}
