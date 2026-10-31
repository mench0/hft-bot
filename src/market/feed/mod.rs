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
pub mod uniswap;
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

/// Настройки устойчивости фида (параметры биржи, как в Java).
#[derive(Clone, Debug)]
pub struct Tune {
    /// Тишина в WebSocket до переподключения (wsStaleMs; 0 — по умолчанию 30 с, у Uniswap — 120 с: пулы бывают тихими).
    pub stale: Duration,
    /// Начальная пауза переподключения (wsReconnectBaseMs, 500 мс), дальше — вдвое до 30 с.
    pub reconnect_base: Duration,
    /// Неудач подряд, после которых источник сдаётся (feedMaxFailures, 15).
    pub max_failures: u32,
    /// Ошибок разбора подряд до сброса соединения (wsMaxParseErrors, 5).
    pub max_parse_errors: u32,
    /// Перекрещённых стаканов подряд (bid ≥ ask) по символу до переподключения (wsMaxCrossedBooks, 200).
    pub max_crossed: u32,
}

impl Default for Tune {
    fn default() -> Self {
        Tune::from("", &Map::new())
    }
}

impl Tune {
    pub fn from(ex: &str, p: &Map<String, Value>) -> Tune {
        let n = |k: &str, d: f64| {
            p.get(k)
                .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse().ok()))
                .unwrap_or(d)
        };
        let ms = |x: f64| Duration::from_millis(x.max(0.0) as u64);
        let stale = n("wsStaleMs", 0.0);
        Tune {
            stale: if stale > 0.0 {
                ms(stale)
            } else if ex == "uniswapv2" {
                Duration::from_secs(120)
            } else {
                Duration::from_secs(30)
            },
            reconnect_base: ms(n("wsReconnectBaseMs", 500.0).max(50.0)),
            max_failures: n("feedMaxFailures", 15.0).max(1.0) as u32,
            max_parse_errors: n("wsMaxParseErrors", 5.0).max(1.0) as u32,
            max_crossed: n("wsMaxCrossedBooks", 200.0).max(1.0) as u32,
        }
    }
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
        "hyperliquid" => Arc::new(hyperliquid::Hyperliquid::new_spot(
            ws_url.unwrap_or_else(|| hyperliquid::default_url(testnet).into()),
            params
                .get("restUrl")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(if testnet {
                    "https://api.hyperliquid-testnet.xyz"
                } else {
                    "https://api.hyperliquid.xyz"
                })
                .to_owned(),
            &symbols,
        )),
        "lbank" | "upbit" => {
            return Plan::Unsupported(format!(
                "{ex}: {} в Rust-бэкенде ещё нет",
                if perp { "перпов" } else { "спота" }
            ));
        }
        "uniswapv2" if !perp => {
            let pools = match uniswap::parse_pools(
                params.get("uniPools").and_then(Value::as_str).unwrap_or(""),
            ) {
                Ok(p) if !p.is_empty() => p,
                Ok(_) => {
                    return Plan::Unsupported(
                        "uniswapv2: задайте пулы в параметре uniPools".into(),
                    );
                }
                Err(e) => return Plan::Unsupported(e),
            };
            // нода: wsUrl, иначе restUrl с ws://, иначе публичная
            let url = ws_url.unwrap_or_else(|| {
                params
                    .get("restUrl")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(|s| s.replacen("http", "ws", 1))
                    .unwrap_or_else(|| uniswap::DEFAULT_URL.into())
            });
            Arc::new(uniswap::Uniswap::new(url, pools, &symbols))
        }
        _ => {
            return Plan::Unsupported(format!(
                "{ex}: у этой биржи нет {}",
                if perp { "перпов" } else { "фида" }
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
                "bybit" => crate::exchange::bybit::rest_url(testnet),
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
                _ => crate::exchange::rest::default_url(perp, testnet),
            }
            .to_owned()
        });
    let tune = Tune::from(&ex, params);
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
            run(m, ex, dialect, tune),
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
pub fn apply(market: &Market, ex: &str, u: Update) -> String {
    match u {
        Update::Snapshot {
            sym,
            bids,
            asks,
            id,
        } => {
            market.with_book(ex, &sym, |b| {
                if b.apply_snapshot(bids, asks, id) {
                    crate::record::book(ex, &sym, b)
                }
            });
            sym
        }
        Update::Delta {
            sym,
            bids,
            asks,
            id,
        } => {
            market.with_book(ex, &sym, |b| {
                if b.apply_delta(&bids, &asks, id) {
                    crate::record::book(ex, &sym, b)
                }
            });
            sym
        }
        Update::Best { sym, bid, ask, id } => {
            let fresh = market.with_book(ex, &sym, |b| {
                let fresh = b.apply_best(bid.0, bid.1, ask.0, ask.1, id);
                if fresh {
                    crate::record::book(ex, &sym, b)
                }
                fresh
            });
            market.with_feed(ex, |f| {
                if fresh {
                    f.bbo_updates += 1
                } else {
                    f.stale += 1
                }
            });
            sym
        }
    }
}

/// Держать соединение: подписаться, читать сообщения, слать пинги; при обрыве или ошибке биржи — переподключиться.
/// Тишина дольше tune.stale, tune.max_parse_errors ошибок разбора подряд или tune.max_crossed перекрещённых стаканов
/// подряд по символу — переподключение; tune.max_failures неудач подряд — источник сдаётся (gave_up: торговля на бирже
/// на паузе), но попытки продолжаются и при успехе флаг снимается. Задача снимается abort() при остановке.
pub async fn run(market: Arc<Market>, ex: String, d: Arc<dyn Dialect>, tune: Tune) {
    let mut backoff = Backoff::with_base(tune.reconnect_base);
    market.with_feed(&ex, |f| {
        f.down_since.get_or_insert_with(std::time::Instant::now);
    });
    loop {
        let url = match d.connect_url().await {
            Ok(u) => u,
            Err(e) => {
                fail(&market, &ex, &e, &tune);
                tokio::time::sleep(backoff.next()).await;
                continue;
            }
        };
        // зависшее рукопожатие не держит фид вечно
        let _ = crate::infra::ratelimit::take(&ex, "ws_connect").await;
        let conn = match tokio::time::timeout(
            Duration::from_secs(10),
            tokio_tungstenite::connect_async(url),
        )
        .await
        {
            Ok(c) => c,
            Err(_) => Err(tokio_tungstenite::tungstenite::Error::Io(
                std::io::Error::other("подключение не ответило за 10 с"),
            )),
        };
        // соединение считается удачным, когда пришло первое сообщение
        let mut got = false;
        let err = match conn {
            Ok((mut ws, _)) => {
                tracing::info!("[{ex}] WebSocket подключён");
                market.with_feed(&ex, |f| {
                    f.connected = true;
                    f.last_error.clear();
                });
                let mut err = String::new();
                for m in d.subscribe() {
                    let _ = crate::infra::ratelimit::take(&ex, "ws_send").await;
                    if let Err(e) = ws.send(Message::text(m)).await {
                        err = e.to_string();
                    }
                }
                let (ping_msg, every) = d
                    .ping()
                    .unwrap_or((String::new(), Duration::from_secs(3600)));
                let mut ping = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
                let mut watch = tokio::time::interval(Duration::from_millis(500));
                let mut last = std::time::Instant::now();
                let (mut parse_errors, mut crossed) =
                    (0u32, std::collections::HashMap::<String, u32>::new());
                while err.is_empty() {
                    let mut ups: Vec<Update> = Vec::new();
                    let mut parsed: Option<Result<(), String>> = None;
                    tokio::select! {
                        msg = ws.next() => match msg {
                            Some(Ok(m @ (Message::Text(_) | Message::Binary(_)))) => {
                                last = std::time::Instant::now();
                                if !got {
                                    got = true;
                                    backoff.reset();
                                    market.with_feed(&ex, |f| {
                                        f.failures = 0;
                                        f.down_since = None;
                                        if f.gave_up {
                                            tracing::warn!("[{ex}] источник данных вернулся — торговля снова разрешена");
                                        }
                                        f.gave_up = false;
                                    });
                                }
                                let t = match m {
                                    Message::Text(t) => Some(t.to_string()),
                                    // protobuf (MEXC) — сразу в обновления
                                    Message::Binary(b) => match d.parse_binary(&b) {
                                        Some(r) => {
                                            market.with_feed(&ex, |f| f.messages += 1); crate::infra::metrics::inbound(&ex, "market");
                                            parsed = Some(r.map(|u| ups = u));
                                            None
                                        }
                                        None => d.binary(&b),
                                    },
                                    _ => None,
                                };
                                if let Some(t) = t {
                                    market.with_feed(&ex, |f| f.messages += 1); crate::infra::metrics::inbound(&ex, "market");
                                    if let Some(r) = d.reply(&t) {
                                        let _ = crate::infra::ratelimit::take(&ex, "ws_send").await; if let Err(e) = ws.send(Message::text(r)).await { err = e.to_string(); }
                                    } else {
                                        parsed = Some(d.parse(&t).map(|u| ups = u));
                                    }
                                }
                            }
                            Some(Ok(Message::Ping(p))) => { last = std::time::Instant::now(); let _ = ws.send(Message::Pong(p)).await; }
                            Some(Ok(Message::Close(c))) => err = format!("закрыто биржей: {c:?}"),
                            Some(Ok(_)) => last = std::time::Instant::now(),
                            Some(Err(e)) => err = e.to_string(),
                            None => err = "соединение закрыто".into(),
                        },
                        _ = ping.tick(), if !ping_msg.is_empty() => {
                            let _ = crate::infra::ratelimit::take(&ex, "ws_send").await; if let Err(e) = ws.send(Message::text(ping_msg.clone())).await { err = e.to_string(); }
                        }
                        _ = watch.tick() => {
                            if last.elapsed() >= tune.stale {
                                market.with_feed(&ex, |f| f.silence_reconnects += 1);
                                err = format!("тишина {} с", last.elapsed().as_secs());
                            }
                        }
                    }
                    match parsed {
                        Some(Err(e)) => {
                            parse_errors += 1;
                            market.with_feed(&ex, |f| f.parse_errors += 1);
                            if parse_errors >= tune.max_parse_errors {
                                err = format!("{parse_errors} ошибок разбора подряд: {e}");
                            } else {
                                tracing::warn!(
                                    "[{ex}] ошибка в потоке ({parse_errors} подряд): {e}"
                                );
                                market.with_feed(&ex, |f| f.last_error = e);
                            }
                        }
                        Some(Ok(())) => parse_errors = 0,
                        None => {}
                    }
                    for u in ups {
                        let sym = apply(&market, &ex, u);
                        let bad = market.with_book(&ex, &sym, |b| matches!((b.best_bid(), b.best_ask()), (Some(x), Some(y)) if x.0 >= y.0));
                        let n = crossed.entry(sym.clone()).or_default();
                        *n = if bad { *n + 1 } else { 0 };
                        if *n >= tune.max_crossed {
                            market.with_feed(&ex, |f| f.crossed_resets += 1);
                            err = format!("стакан {sym} перекрещён {} обновлений подряд", *n);
                        }
                    }
                }
                err
            }
            Err(e) => e.to_string(),
        };
        if !got {
            fail(&market, &ex, &err, &tune);
        }
        let pause = backoff.next();
        market.with_feed(&ex, |f| {
            f.connected = false;
            f.reconnects += 1;
            f.last_error = err.clone();
            f.down_since.get_or_insert_with(std::time::Instant::now);
        });
        tracing::warn!(
            "[{ex}] WebSocket: {err} — переподключение через {} мс",
            pause.as_millis()
        );
        tokio::time::sleep(pause).await;
    }
}

/// Неудачное подключение: счёт подряд; на tune.max_failures — источник сдаётся.
fn fail(market: &Market, ex: &str, e: &str, tune: &Tune) {
    crate::record::push(crate::record::event::Event::FeedError {
        ex: crate::record::event::Name::new(ex),
    });
    market.with_feed(ex, |f| {
        f.last_error = e.to_owned();
        f.failures += 1;
        if f.failures >= tune.max_failures && !f.gave_up {
            f.gave_up = true;
            tracing::error!(
                "[{ex}] источник данных сдался после {} неудач подряд ({e}) — заявки сняты, торговля на паузе до его возвращения",
                f.failures
            );
        }
    });
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
        let _ = crate::infra::ratelimit::take(&ex, "reference").await;
        match crate::exchange::rest::filters(&base, perp, &symbols).await {
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
            let _ = crate::infra::ratelimit::take(&ex, "public").await;
            match crate::exchange::rest::minute_volumes(&base, perp, s, 5).await {
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
        let _ = crate::infra::ratelimit::take(&ex, "reference").await;
        match crate::exchange::rest::contract_sizes(&ex, &base).await {
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
            let _ = crate::infra::ratelimit::take(&ex, "funding").await;
            match crate::exchange::rest::funding(&ex, &base, s).await {
                Ok(f) => market.set_funding(&ex, s, f),
                Err(e) => market.with_feed(&ex, |st| st.rest_error = format!("funding {s}: {e}")),
            }
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

/// Пауза перед переподключением: 0.5 с, удваивается до 30 с; после успешного подключения — снова с начала.
pub struct Backoff(Duration, Duration);

impl Default for Backoff {
    fn default() -> Self {
        Backoff::with_base(Duration::from_millis(500))
    }
}

impl Backoff {
    pub fn with_base(base: Duration) -> Backoff {
        Backoff(base, base)
    }
    pub fn next(&mut self) -> Duration {
        let d = self.0;
        self.0 = (self.0 * 2).min(Duration::from_secs(30)).max(self.1);
        d
    }
    pub fn reset(&mut self) {
        self.0 = self.1;
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
    pub async fn wait_ready(m: &Market, ex: &str, sym: &str) -> crate::market::book::Book {
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
