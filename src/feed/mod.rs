//! Фиды рыночных данных по WebSocket: общий цикл (подключение, подписка, пинги, переподключение с нарастающей
//! паузой) и «диалекты» бирж — адрес, сообщения подписки, пинг и разбор сообщений в обновления стакана.

pub mod binance;
pub mod bingx;
pub mod bybit;
pub mod gate;
pub mod gate_futures;
pub mod hyperliquid;
pub mod kucoin;
pub mod lbank;
pub mod mexc;
pub mod okx;
pub mod upbit;

use crate::market::Market;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Map, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

/// Обновление стакана из сообщения биржи.
#[derive(Debug, Clone, PartialEq)]
pub enum Update {
    /// Снимок N лучших уровней.
    Snapshot {
        sym: String,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
        id: u64,
    },
    /// Изменения уровней после снимка (объём 0 — убрать).
    Delta {
        sym: String,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
        id: u64,
    },
    /// Лучшие цены.
    Best {
        sym: String,
        bid: (f64, f64),
        ask: (f64, f64),
        id: u64,
    },
}

/// Разбор сообщения: обновления, служебное (пусто) или ошибка биржи (переподключиться).
pub type Parsed = Result<Vec<Update>, String>;

/// Особенности биржи.
pub trait Dialect: Send + Sync + 'static {
    /// Адрес подключения.
    fn url(&self) -> String;
    /// Сообщения подписки после подключения.
    fn subscribe(&self) -> Vec<String> {
        Vec::new()
    }
    /// Пинг приложения и как часто его слать (None — хватает пингов WebSocket).
    fn ping(&self) -> Option<(String, Duration)> {
        None
    }
    /// Разобрать текстовое сообщение.
    fn parse(&self, text: &str) -> Parsed;
    /// Ответ на пинг сервера (LBank, BingX); None — это не пинг.
    fn reply(&self, _text: &str) -> Option<String> {
        None
    }
    /// Бинарный кадр в текст (Upbit — JSON в UTF-8, BingX — gzip); None — пропустить.
    fn binary(&self, _data: &[u8]) -> Option<String> {
        None
    }
    /// Разбор бинарного кадра прямо в обновления (MEXC — protobuf); None — кадр текстовый, см. binary.
    fn parse_binary(&self, _data: &[u8]) -> Option<Parsed> {
        None
    }
    /// Адрес подключения, если для него нужен запрос (KuCoin — токен bullet-public).
    fn connect_url(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send + '_>>
    {
        Box::pin(async move { Ok(self.url()) })
    }
}

/// Что запустить для биржи: фид или причина, почему фида нет.
pub enum Plan {
    Run(JoinHandle<()>),
    Unsupported(String),
}

/// Уровни ["цена", "объём", ...] (строки или числа) → (цена, объём).
pub fn levels(v: Option<&Value>) -> Vec<(f64, f64)> {
    let num = |x: &Value| {
        x.as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| x.as_f64())
    };
    v.and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|l| {
                    let l = l.as_array()?;
                    Some((num(l.first()?)?, num(l.get(1)?)?))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Запустить фид биржи по её параметрам (`market`, `testnet`, `wsUrl`, `restUrl`, `wsBookTicker`).
pub fn start(
    market: Arc<Market>,
    ex: &str,
    symbols: Vec<String>,
    params: &Map<String, Value>,
) -> Plan {
    let perp = params.get("market").and_then(Value::as_str) == Some("perp");
    let testnet = params
        .get("testnet")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let ws_url = params
        .get("wsUrl")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned);
    let bbo = params
        .get("wsBookTicker")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    // перпы OKX и Gate: размеры контрактов (объёмы стакана — в контрактах)
    let sizes: okx::ContractSizes = Default::default();
    let dialect: Arc<dyn Dialect> = match ex {
        "binance" => {
            let base = ws_url.unwrap_or_else(|| binance::default_url(perp, testnet).to_owned());
            Arc::new(binance::Binance {
                url: binance::stream_url(&base, &symbols, bbo),
            })
        }
        "bybit" => Arc::new(bybit::Bybit::new(
            ws_url.unwrap_or_else(|| bybit::default_url(perp, testnet)),
            &symbols,
            bbo,
        )),
        "okx" if !perp => Arc::new(okx::Okx::new(
            ws_url.unwrap_or_else(|| okx::default_url(testnet).into()),
            &symbols,
            bbo,
        )),
        "gate" if !perp => Arc::new(gate::Gate::new(
            ws_url.unwrap_or_else(|| gate::default_url(testnet).into()),
            &symbols,
            bbo,
        )),
        "okx" => Arc::new(okx::Okx::new_swap(
            ws_url.unwrap_or_else(|| okx::default_url(testnet).into()),
            &symbols,
            bbo,
            sizes.clone(),
        )),
        "gate" => Arc::new(gate_futures::GateFutures::new(
            ws_url.unwrap_or_else(|| gate_futures::default_url(testnet).into()),
            &symbols,
            bbo,
            sizes.clone(),
        )),
        // Aster — сообщения как у Binance
        "aster" => {
            let base = ws_url.unwrap_or_else(|| {
                match (perp, testnet) {
                    (true, true) => "wss://fstream.asterdex-testnet.com/stream",
                    (true, false) => "wss://fstream.asterdex.com/stream",
                    (false, true) => "wss://sstream.asterdex-testnet.com/stream",
                    (false, false) => "wss://sstream.asterdex.com/stream",
                }
                .to_owned()
            });
            Arc::new(binance::Binance {
                url: binance::stream_url(&base, &symbols, bbo),
            })
        }
        "hyperliquid" if perp => Arc::new(hyperliquid::Hyperliquid::new(
            ws_url.unwrap_or_else(|| hyperliquid::default_url(testnet).into()),
            &symbols,
        )),
        "bingx" => Arc::new(bingx::Bingx::new(
            ws_url.unwrap_or_else(|| bingx::default_url(perp, testnet).into()),
            &symbols,
            perp,
        )),
        "kucoin" => Arc::new(kucoin::Kucoin::new(
            params
                .get("restUrl")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(kucoin::rest_url(perp))
                .to_owned(),
            ws_url,
            &symbols,
            perp,
        )),
        "mexc" if perp => Arc::new(mexc::MexcFutures::new(
            params
                .get("restUrl")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(mexc::FUTURES_REST)
                .to_owned(),
            ws_url.unwrap_or_else(|| mexc::FUTURES_URL.into()),
            &symbols,
        )),
        "mexc" => Arc::new(mexc::MexcSpot::new(
            ws_url.unwrap_or_else(|| mexc::SPOT_URL.into()),
            &symbols,
        )),
        "lbank" if !perp => Arc::new(lbank::Lbank::new(
            ws_url.unwrap_or_else(|| lbank::URL.into()),
            &symbols,
        )),
        "upbit" if !perp => Arc::new(upbit::Upbit::new(
            ws_url.unwrap_or_else(|| upbit::URL.into()),
            &symbols,
        )),
        "hyperliquid" | "lbank" | "upbit" => {
            return Plan::Unsupported(format!(
                "{ex}: {} в Rust-бэкенде ещё нет",
                if perp { "перпов" } else { "спота" }
            ));
        }
        _ => {
            return Plan::Unsupported(format!(
                "{ex}: фида в Rust-бэкенде ещё нет (Uniswap — нода Ethereum)"
            ));
        }
    };
    market.with_feed(ex, |f| {
        f.symbols = symbols.clone();
        f.unsupported.clear();
        f.bbo_enabled = bbo;
    });
    let (ex, m) = (ex.to_owned(), market.clone());
    let rest_url = params
        .get("restUrl")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            match ex.as_str() {
                "bybit" => crate::bybit_api::rest_url(testnet),
                "okx" => "https://www.okx.com",
                "gate" if testnet => "https://fx-api-testnet.gateio.ws",
                "gate" => "https://api.gateio.ws",
                "aster" if perp => "https://fapi.asterdex.com",
                "aster" => "https://sapi.asterdex.com",
                "bingx" => "https://open-api.bingx.com",
                "mexc" if perp => mexc::FUTURES_REST,
                "kucoin" => kucoin::rest_url(perp),
                "hyperliquid" if testnet => "https://api.hyperliquid-testnet.xyz",
                "hyperliquid" => "https://api.hyperliquid.xyz",
                _ => crate::rest::default_url(perp, testnet),
            }
            .to_owned()
        });
    let rest = (ex == "binance").then(|| {
        rest_loop(
            market.clone(),
            ex.clone(),
            rest_url.clone(),
            perp,
            symbols.clone(),
        )
    });
    let contracts = (perp && matches!(ex.as_str(), "okx" | "gate"))
        .then(|| contract_sizes_loop(market.clone(), ex.clone(), rest_url.clone(), sizes));
    let funding = perp.then(|| funding_loop(market, ex.clone(), rest_url, symbols));
    Plan::Run(tokio::spawn(async move {
        tokio::join!(
            run(m, ex, dialect),
            async {
                if let Some(c) = contracts {
                    c.await
                }
            },
            async {
                if let Some(r) = rest {
                    r.await
                }
            },
            async {
                if let Some(f) = funding {
                    f.await
                }
            }
        );
    }))
}

/// Применить обновление к стакану.
pub fn apply(market: &Market, ex: &str, u: Update) {
    match u {
        Update::Snapshot {
            sym,
            bids,
            asks,
            id,
        } => {
            market.with_book(ex, &sym, |b| b.apply_snapshot(bids, asks, id));
        }
        Update::Delta {
            sym,
            bids,
            asks,
            id,
        } => {
            market.with_book(ex, &sym, |b| b.apply_delta(&bids, &asks, id));
        }
        Update::Best { sym, bid, ask, id } => {
            let fresh =
                market.with_book(ex, &sym, |b| b.apply_best(bid.0, bid.1, ask.0, ask.1, id));
            market.with_feed(ex, |f| {
                if fresh {
                    f.bbo_updates += 1
                } else {
                    f.stale += 1
                }
            });
        }
    }
}

/// Держать соединение: подписаться, читать сообщения, слать пинги; при обрыве или ошибке биржи — переподключиться.
/// Задача снимается abort() при остановке.
pub async fn run(market: Arc<Market>, ex: String, d: Arc<dyn Dialect>) {
    let mut backoff = Backoff::default();
    loop {
        let url = match d.connect_url().await {
            Ok(u) => u,
            Err(e) => {
                market.with_feed(&ex, |f| f.last_error = e.clone());
                tokio::time::sleep(backoff.next()).await;
                continue;
            }
        };
        let err = match tokio_tungstenite::connect_async(url).await {
            Ok((mut ws, _)) => {
                tracing::info!("[{ex}] WebSocket подключён");
                market.with_feed(&ex, |f| {
                    f.connected = true;
                    f.last_error.clear();
                });
                backoff.reset();
                let mut err = String::new();
                for m in d.subscribe() {
                    if let Err(e) = ws.send(Message::text(m)).await {
                        err = e.to_string();
                    }
                }
                let (ping_msg, every) = d
                    .ping()
                    .unwrap_or((String::new(), Duration::from_secs(3600)));
                let mut ping = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
                while err.is_empty() {
                    tokio::select! {
                        msg = ws.next() => match msg {
                            Some(Ok(m @ (Message::Text(_) | Message::Binary(_)))) => {
                                let t = match m {
                                    Message::Text(t) => Some(t.to_string()),
                                    // protobuf (MEXC) — сразу в обновления
                                    Message::Binary(b) => match d.parse_binary(&b) {
                                        Some(r) => {
                                            market.with_feed(&ex, |f| f.messages += 1);
                                            match r {
                                                Ok(ups) => for u in ups { apply(&market, &ex, u); },
                                                Err(e) => err = e,
                                            }
                                            None
                                        }
                                        None => d.binary(&b),
                                    },
                                    _ => None,
                                };
                                if let Some(t) = t {
                                    market.with_feed(&ex, |f| f.messages += 1);
                                    if let Some(r) = d.reply(&t) {
                                        if let Err(e) = ws.send(Message::text(r)).await { err = e.to_string(); }
                                    } else {
                                        match d.parse(&t) {
                                            Ok(ups) => for u in ups { apply(&market, &ex, u); },
                                            Err(e) => err = e,
                                        }
                                    }
                                }
                            }
                            Some(Ok(Message::Ping(p))) => { let _ = ws.send(Message::Pong(p)).await; }
                            Some(Ok(Message::Close(c))) => err = format!("закрыто биржей: {c:?}"),
                            Some(Ok(_)) => {}
                            Some(Err(e)) => err = e.to_string(),
                            None => err = "соединение закрыто".into(),
                        },
                        _ = ping.tick(), if !ping_msg.is_empty() => {
                            if let Err(e) = ws.send(Message::text(ping_msg.clone())).await { err = e.to_string(); }
                        }
                    }
                }
                err
            }
            Err(e) => e.to_string(),
        };
        let pause = backoff.next();
        market.with_feed(&ex, |f| {
            f.connected = false;
            f.reconnects += 1;
            f.last_error = err.clone();
        });
        tracing::warn!(
            "[{ex}] WebSocket: {err} — переподключение через {} мс",
            pause.as_millis()
        );
        tokio::time::sleep(pause).await;
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

/// Размеры контрактов перпов OKX и Gate: при старте (повтор раз в 10 с, пока не получены), затем раз в час.
async fn contract_sizes_loop(
    market: Arc<Market>,
    ex: String,
    base: String,
    sizes: okx::ContractSizes,
) {
    loop {
        match crate::rest::contract_sizes(&ex, &base).await {
            Ok(m) if !m.is_empty() => {
                tracing::info!("[{ex}] размеры контрактов получены: {}", m.len());
                sizes.lock().unwrap().extend(m);
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
            Ok(_) => tokio::time::sleep(Duration::from_secs(10)).await,
            Err(e) => {
                market.with_feed(&ex, |s| s.rest_error = format!("размеры контрактов: {e}"));
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

/// Ставки funding перпов — раз в 30 с.
async fn funding_loop(market: Arc<Market>, ex: String, base: String, symbols: Vec<String>) {
    loop {
        for s in &symbols {
            match crate::rest::funding(&ex, &base, s).await {
                Ok(f) => market.set_funding(&ex, s, f),
                Err(e) => market.with_feed(&ex, |st| st.rest_error = format!("funding {s}: {e}")),
            }
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
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

#[cfg(test)]
pub mod testkit {
    //! Локальный WS-сервер для проверок фидов: запоминает полученные сообщения и шлёт заданные.
    use super::*;
    use std::sync::Mutex;
    use tokio::net::TcpListener;

    /// Запустить сервер: на подключение — отправить `send` по очереди; вернуть адрес и полученные сообщения.
    pub async fn server(send: Vec<String>) -> (String, Arc<Mutex<Vec<String>>>) {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("ws://{}", l.local_addr().unwrap());
        let got = Arc::new(Mutex::new(Vec::new()));
        let g = got.clone();
        tokio::spawn(async move {
            let (tcp, _) = l.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            // сначала дождаться подписки (если её шлют), потом данные
            if let Ok(Some(Ok(Message::Text(t)))) =
                tokio::time::timeout(Duration::from_millis(300), ws.next()).await
            {
                g.lock().unwrap().push(t.to_string());
            }
            for m in send {
                ws.send(Message::text(m)).await.unwrap();
            }
            while let Some(Ok(m)) = ws.next().await {
                if let Message::Text(t) = m {
                    g.lock().unwrap().push(t.to_string());
                }
            }
        });
        (addr, got)
    }

    /// Ждать, пока стакан символа не станет готов.
    pub async fn wait_ready(m: &Market, ex: &str, sym: &str) -> crate::book::Book {
        let until = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(b) = m.book(ex, sym).filter(|b| b.is_ready()) {
                return b;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "стакан {ex} {sym} не готов: {:?}",
                m.feed(ex)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
