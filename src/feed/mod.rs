//! Фиды рыночных данных по WebSocket: подключение, переподключение с нарастающей паузой, разбор сообщений биржи.

pub mod binance;

use crate::market::Market;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Что запустить для биржи: фид или причина, почему фида нет.
pub enum Plan {
    Run(JoinHandle<()>),
    Unsupported(String),
}

/// Запустить фид биржи по её параметрам (`market`, `testnet`, `wsUrl`, `wsBookTicker`).
pub fn start(
    market: Arc<Market>,
    ex: &str,
    symbols: Vec<String>,
    params: &serde_json::Map<String, serde_json::Value>,
) -> Plan {
    let perp = params.get("market").and_then(|v| v.as_str()) == Some("perp");
    let testnet = params
        .get("testnet")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let ws_url = params
        .get("wsUrl")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    let bbo = params
        .get("wsBookTicker")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    match ex {
        "binance" => {
            let base = ws_url.unwrap_or_else(|| binance::default_url(perp, testnet).to_owned());
            let url = binance::stream_url(&base, &symbols, bbo);
            let rest_url = params
                .get("restUrl")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| crate::rest::default_url(perp, testnet).to_owned());
            market.with_feed(ex, |f| {
                f.symbols = symbols.clone();
                f.unsupported.clear();
                f.bbo_enabled = bbo;
            });
            let (m2, ex2, syms) = (market.clone(), ex.to_owned(), symbols.clone());
            Plan::Run(tokio::spawn(async move {
                tokio::join!(
                    binance::run(market, ex2.clone(), url),
                    rest_loop(m2, ex2, rest_url, perp, syms)
                );
            }))
        }
        _ => Plan::Unsupported(format!(
            "{ex}: фида в Rust-бэкенде ещё нет — данные идут только у binance"
        )),
    }
}

/// Правила пар — при подключении (повтор раз в 10 с, пока не получены); оборот минутных свечей — раз в минуту.
async fn rest_loop(
    market: Arc<Market>,
    ex: String,
    base: String,
    perp: bool,
    symbols: Vec<String>,
) {
    loop {
        match crate::rest::filters(&base, perp, &symbols).await {
            Ok(f) => {
                tracing::info!("[{ex}] правила пар получены: {}", f.len());
                market.set_filters(&ex, f);
                break;
            }
            Err(e) => {
                market.with_feed(&ex, |s| s.rest_error = format!("правила пар: {e}"));
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
    loop {
        let mut err = String::new();
        for s in &symbols {
            match crate::rest::minute_volumes(&base, perp, s, 5).await {
                Ok(v) => market.set_minutes(&ex, s, v),
                Err(e) => err = format!("свечи {s}: {e}"),
            }
        }
        market.with_feed(&ex, |s| s.rest_error = err);
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

/// Пауза перед переподключением: 0.5 с, удваивается до 30 с; после успешного подключения — снова с начала.
pub struct Backoff(Duration);

impl Default for Backoff {
    fn default() -> Self {
        Backoff(Duration::from_millis(500))
    }
}

impl Backoff {
    pub fn next(&mut self) -> Duration {
        let d = self.0;
        self.0 = (self.0 * 2).min(Duration::from_secs(30));
        d
    }
    pub fn reset(&mut self) {
        *self = Backoff::default();
    }
}
