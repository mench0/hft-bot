//! Подбор тикеров (скринер), как DiscoveryService в Java, в формате его ответа `/discovery`.
//!
//! Сводки спота по выбранным биржам (`discoveryExchanges`) с лучшими ценами: Binance, MEXC, Bybit, OKX, Gate,
//! Upbit (в сводке Upbit нет бид/аск — они берутся из стаканов `/v1/orderbook` пачками по 50 рынков) и LBank
//! (лучшие цены всех пар — `/v2/supplement/ticker/bookTicker.do`; не отдаёт — спред LBank неизвестен).
//!
//! Профили:
//! - «Сбор спреда» — спред после двух комиссий мейкера, оборот в коридоре; у лучших кандидатов биржи проверяется
//!   стакан (проскальзывание на размер сделки, уровни, шаг цены). Стакан не получен — «под вопросом».
//! - «Спот – спот на разных биржах» — монета дешевле на одной бирже и дороже на другой больше двух комиссий тейкера.
//! - «Lead-lag» — ведущая биржа (leadLagLeader) и ведомые с меньшим оборотом и узким спредом.
//! - «Перп – перп» — сводки перпов (Binance, MEXC, Bybit, OKX, Gate, KuCoin, BingX, Hyperliquid): после 4 комиссий.
//! - «Статистический арбитраж» — 6 самых ликвидных монет биржи попарно: корреляция минутных доходностей и бэктест.
//!
//! Сбор спреда проверяет и минутные свечи (оборот за час, в тихий час, минуты без оборота). Пороги — с фильтрами
//! подбора по бирже и стратегии (discoveryFilters).

use crate::trading::paper::split;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

/// Сводка тикера.
#[derive(Clone, Debug, PartialEq)]
pub struct Ticker {
    pub exchange: String,
    /// BTCUSDT.
    pub symbol: String,
    pub base: String,
    pub quote: String,
    pub last: f64,
    pub bid: f64,
    pub ask: f64,
    /// Оборот за 24 ч в котируемой валюте.
    pub volume: f64,
    pub change_pct: f64,
    /// Перп (бессрочный фьючерс), а не спот.
    pub perp: bool,
    /// Ставка funding за 8 ч, доля (NaN — нет).
    pub funding8h: f64,
    /// Символ на бирже (BTC-USDT, XBTUSDTM, BTC — у Hyperliquid) — для свечей и стакана.
    pub venue: String,
}

impl Ticker {
    pub fn spread_pct(&self) -> f64 {
        if self.bid > 0.0 && self.ask >= self.bid {
            (self.ask - self.bid) / ((self.ask + self.bid) / 2.0) * 100.0
        } else {
            f64::NAN
        }
    }
}

/// Биржи со сводками в скринере.
pub const SUPPORTED: &[&str] = &[
    "binance",
    "mexc",
    "bybit",
    "okx",
    "gate",
    "upbit",
    "lbank",
    "kucoin",
    "bingx",
    "hyperliquid",
];

/// REST-адрес сводок биржи.
pub fn base_url(ex: &str) -> Option<&'static str> {
    Some(match ex {
        "binance" => "https://api.binance.com",
        "mexc" => "https://api.mexc.com",
        "bybit" => "https://api.bybit.com",
        "okx" => "https://www.okx.com",
        "gate" => "https://api.gateio.ws",
        "upbit" => "https://api.upbit.com",
        "lbank" => "https://api.lbkex.com",
        "kucoin" => "https://api.kucoin.com",
        "bingx" => "https://open-api.bingx.com",
        "hyperliquid" => "https://api.hyperliquid.xyz",
        _ => return None,
    })
}

/// REST-адрес сводки перпов биржи; None — перпов в подборе нет (Upbit, LBank — только спот).
pub fn perp_url(ex: &str) -> Option<&'static str> {
    Some(match ex {
        "binance" => "https://fapi.binance.com",
        "mexc" => "https://contract.mexc.com",
        "bybit" => "https://api.bybit.com",
        "okx" => "https://www.okx.com",
        "gate" => "https://api.gateio.ws",
        "kucoin" => "https://api-futures.kucoin.com",
        "bingx" => "https://open-api.bingx.com",
        "hyperliquid" => "https://api.hyperliquid.xyz",
        _ => return None,
    })
}

/// Перп-тикер из сводки фьючерсов (как perp в Java): BTC-USDT-SWAP, XBTUSDTM, BTC_USDT → BTCUSDT.
#[allow(clippy::too_many_arguments)]
fn perp_ticker(
    ex: &str,
    venue: &str,
    last: f64,
    bid: f64,
    ask: f64,
    volume: f64,
    change_pct: f64,
    funding8h: f64,
) -> Option<Ticker> {
    let mut v = venue.to_uppercase().replace("-SWAP", "");
    if ex == "kucoin" {
        v = v.trim_end_matches('M').to_owned();
        if let Some(r) = v.strip_prefix("XBT") {
            v = format!("BTC{r}");
        }
    }
    let mut t = ticker(ex, &v, false, last, bid, ask, volume, change_pct)?;
    t.perp = true;
    t.funding8h = funding8h;
    t.venue = venue.to_owned();
    Some(t)
}

/// Разбор сводки перпов. Binance — три ответа: 24hr, bookTicker, premiumIndex; BingX — ticker и premiumIndex.
pub fn parse_perp(ex: &str, v: &Value, book: &Value, fund: &Value) -> Vec<Ticker> {
    let arr = |x: &Value| x.as_array().cloned().unwrap_or_default();
    let by = |x: &Value| -> HashMap<String, Value> {
        arr(x)
            .into_iter()
            .filter_map(|e| Some((e["symbol"].as_str()?.to_owned(), e)))
            .collect()
    };
    match ex {
        "binance" => {
            let (b, f) = (by(book), by(fund));
            arr(v)
                .iter()
                .filter_map(|t| {
                    let s = t["symbol"].as_str()?;
                    let (bk, fu) = (b.get(s), f.get(s));
                    perp_ticker(
                        ex,
                        s,
                        num(&t["lastPrice"]),
                        bk.map_or(f64::NAN, |x| num(&x["bidPrice"])),
                        bk.map_or(f64::NAN, |x| num(&x["askPrice"])),
                        num(&t["quoteVolume"]),
                        num(&t["priceChangePercent"]),
                        fu.map_or(f64::NAN, |x| num(&x["lastFundingRate"])),
                    )
                })
                .collect()
        }
        "mexc" => arr(&v["data"])
            .iter()
            .filter_map(|t| {
                perp_ticker(
                    ex,
                    t["symbol"].as_str()?,
                    num(&t["lastPrice"]),
                    num(&t["bid1"]),
                    num(&t["ask1"]),
                    num(&t["amount24"]),
                    num(&t["riseFallRate"]) * 100.0,
                    num(&t["fundingRate"]),
                )
            })
            .collect(),
        "bybit" => arr(&v["result"]["list"])
            .iter()
            .filter_map(|t| {
                perp_ticker(
                    ex,
                    t["symbol"].as_str()?,
                    num(&t["lastPrice"]),
                    num(&t["bid1Price"]),
                    num(&t["ask1Price"]),
                    num(&t["turnover24h"]),
                    num(&t["price24hPcnt"]) * 100.0,
                    num(&t["fundingRate"]),
                )
            })
            .collect(),
        // у SWAP volCcy24h — в монетах: оборот = × цену; ставки в сводке нет
        "okx" => arr(&v["data"])
            .iter()
            .filter_map(|t| {
                let (last, open) = (num(&t["last"]), num(&t["sodUtc0"]));
                perp_ticker(
                    ex,
                    t["instId"].as_str()?,
                    last,
                    num(&t["bidPx"]),
                    num(&t["askPx"]),
                    num(&t["volCcy24h"]) * last,
                    (last / open - 1.0) * 100.0,
                    f64::NAN,
                )
            })
            .collect(),
        "gate" => arr(v)
            .iter()
            .filter_map(|t| {
                perp_ticker(
                    ex,
                    t["contract"].as_str()?,
                    num(&t["last"]),
                    num(&t["highest_bid"]),
                    num(&t["lowest_ask"]),
                    num(&t["volume_24h_quote"]),
                    num(&t["change_percentage"]),
                    num(&t["funding_rate"]),
                )
            })
            .collect(),
        // bid/ask в сводке KuCoin нет
        "kucoin" => arr(&v["data"])
            .iter()
            .filter_map(|t| {
                perp_ticker(
                    ex,
                    t["symbol"].as_str()?,
                    num(&t["lastTradePrice"]),
                    f64::NAN,
                    f64::NAN,
                    num(&t["turnoverOf24h"]),
                    num(&t["priceChgPct"]) * 100.0,
                    num(&t["fundingFeeRate"]),
                )
            })
            .collect(),
        "bingx" => {
            let f = by(&fund["data"]);
            arr(&v["data"])
                .iter()
                .filter_map(|t| {
                    let s = t["symbol"].as_str()?;
                    perp_ticker(
                        ex,
                        s,
                        num(&t["lastPrice"]),
                        num(&t["bidPrice"]),
                        num(&t["askPrice"]),
                        num(&t["quoteVolume"]),
                        num(&t["priceChangePercent"]),
                        f.get(s).map_or(f64::NAN, |x| num(&x["lastFundingRate"])),
                    )
                })
                .collect()
        }
        // metaAndAssetCtxs: [{universe}, [ctx]]; bid/ask — цены удара (impactPxs), ставка за час → за 8 ч
        "hyperliquid" => {
            let (u, c) = (arr(&v[0]["universe"]), arr(&v[1]));
            u.iter()
                .zip(c.iter())
                .filter_map(|(m, x)| {
                    let coin = m["name"].as_str()?;
                    let mid = num(&x["midPx"]);
                    let last = if mid.is_nan() { num(&x["markPx"]) } else { mid };
                    let prev = num(&x["prevDayPx"]);
                    let mut t = Ticker {
                        exchange: ex.into(),
                        symbol: format!("{coin}USDC"),
                        base: coin.into(),
                        quote: "USDC".into(),
                        last,
                        bid: num(&x["impactPxs"][0]),
                        ask: num(&x["impactPxs"][1]),
                        volume: num(&x["dayNtlVlm"]),
                        change_pct: (last / prev - 1.0) * 100.0,
                        perp: true,
                        funding8h: num(&x["funding"]) * 8.0,
                        venue: coin.into(),
                    };
                    if !(t.bid > 0.0 && t.ask >= t.bid) {
                        (t.bid, t.ask) = (f64::NAN, f64::NAN);
                    }
                    Some(t)
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

fn num(v: &Value) -> f64 {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(f64::NAN)
}

fn first(v: &Value, keys: &[&str]) -> f64 {
    keys.iter()
        .map(|k| num(&v[*k]))
        .find(|x| !x.is_nan())
        .unwrap_or(f64::NAN)
}

#[allow(clippy::too_many_arguments)]
/// Символ биржи (BTC-USDT, btc_usdt, KRW-BTC у Upbit — котировка первой) в тикер.
fn ticker(
    ex: &str,
    venue: &str,
    quote_first: bool,
    last: f64,
    bid: f64,
    ask: f64,
    volume: f64,
    change_pct: f64,
) -> Option<Ticker> {
    let v = venue.to_uppercase();
    let (base, quote) = match v.split_once(['-', '_']) {
        Some((a, b)) if quote_first => (b.to_owned(), a.to_owned()),
        Some((a, b)) => (a.to_owned(), b.to_owned()),
        None => split(&v)?,
    };
    Some(Ticker {
        exchange: ex.to_owned(),
        symbol: format!("{base}{quote}"),
        base,
        quote,
        last,
        bid,
        ask,
        volume,
        change_pct,
        perp: false,
        funding8h: f64::NAN,
        venue: venue.to_owned(),
    })
}

/// Разбор сводок биржи (ответ основного запроса).
pub fn parse(ex: &str, v: &Value) -> Vec<Ticker> {
    let arr = |x: &Value| x.as_array().cloned().unwrap_or_default();
    match ex {
        "binance" | "mexc" => arr(v)
            .iter()
            .filter_map(|t| {
                let last = num(&t["lastPrice"]);
                ticker(
                    ex,
                    t["symbol"].as_str()?,
                    false,
                    last,
                    num(&t["bidPrice"]),
                    num(&t["askPrice"]),
                    num(&t["quoteVolume"]),
                    num(&t["priceChangePercent"]),
                )
            })
            .collect(),
        "bybit" => arr(&v["result"]["list"])
            .iter()
            .filter_map(|t| {
                ticker(
                    ex,
                    t["symbol"].as_str()?,
                    false,
                    num(&t["lastPrice"]),
                    num(&t["bid1Price"]),
                    num(&t["ask1Price"]),
                    num(&t["turnover24h"]),
                    num(&t["price24hPcnt"]) * 100.0,
                )
            })
            .collect(),
        "okx" => arr(&v["data"])
            .iter()
            .filter_map(|t| {
                let (last, open) = (num(&t["last"]), num(&t["sodUtc0"]));
                // volCcy24h у спота — оборот в котируемой валюте
                ticker(
                    ex,
                    t["instId"].as_str()?,
                    false,
                    last,
                    num(&t["bidPx"]),
                    num(&t["askPx"]),
                    num(&t["volCcy24h"]),
                    (last / open - 1.0) * 100.0,
                )
            })
            .collect(),
        "gate" => arr(v)
            .iter()
            .filter_map(|t| {
                ticker(
                    ex,
                    t["currency_pair"].as_str()?,
                    false,
                    num(&t["last"]),
                    num(&t["highest_bid"]),
                    num(&t["lowest_ask"]),
                    num(&t["quote_volume"]),
                    num(&t["change_percentage"]),
                )
            })
            .collect(),
        "upbit" => arr(v)
            .iter()
            .filter_map(|t| {
                ticker(
                    ex,
                    t["market"].as_str()?,
                    true,
                    num(&t["trade_price"]),
                    f64::NAN,
                    f64::NAN,
                    num(&t["acc_trade_price_24h"]),
                    num(&t["signed_change_rate"]) * 100.0,
                )
            })
            .collect(),
        // KuCoin: data.ticker[{symbol BTC-USDT, buy, sell, last, changeRate, volValue}]
        "kucoin" => arr(&v["data"]["ticker"])
            .iter()
            .filter_map(|t| {
                ticker(
                    ex,
                    t["symbol"].as_str()?,
                    false,
                    num(&t["last"]),
                    num(&t["buy"]),
                    num(&t["sell"]),
                    num(&t["volValue"]),
                    num(&t["changeRate"]) * 100.0,
                )
            })
            .collect(),
        // BingX спот: {code, data:[{symbol BTC-USDT, lastPrice, bidPrice, askPrice, quoteVolume, priceChangePercent "1.2%"}]}
        "bingx" => arr(&v["data"])
            .iter()
            .filter_map(|t| {
                let ch = t["priceChangePercent"].as_str().map_or_else(
                    || num(&t["priceChangePercent"]),
                    |s| s.trim_end_matches('%').parse().unwrap_or(f64::NAN),
                );
                ticker(
                    ex,
                    t["symbol"].as_str()?,
                    false,
                    num(&t["lastPrice"]),
                    num(&t["bidPrice"]),
                    num(&t["askPrice"]),
                    num(&t["quoteVolume"]),
                    ch,
                )
            })
            .collect(),
        "lbank" => arr(&v["data"])
            .iter()
            .filter_map(|t| {
                let k = &t["ticker"];
                ticker(
                    ex,
                    t["symbol"].as_str()?,
                    false,
                    num(&k["latest"]),
                    f64::NAN,
                    f64::NAN,
                    num(&k["turnover"]),
                    num(&k["change"]),
                )
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Лучшие цены Upbit из ответа /v1/orderbook: рынок (KRW-BTC) → (бид, аск).
pub fn upbit_top(v: &Value) -> HashMap<String, (f64, f64)> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|b| {
            let u = &b["orderbook_units"][0];
            Some((
                b["market"].as_str()?.to_owned(),
                (num(&u["bid_price"]), num(&u["ask_price"])),
            ))
        })
        .collect()
}

/// Лучшие цены LBank из bookTicker: массив или объект, поля bidPrice/askPrice или bid_price/ask_price.
pub fn lbank_top(v: &Value) -> HashMap<String, (f64, f64)> {
    let d = &v["data"];
    let list = match d.as_array() {
        Some(a) => a.clone(),
        None => vec![d.clone()],
    };
    list.iter()
        .filter_map(|x| {
            let s = x["symbol"].as_str()?.to_lowercase();
            let (b, a) = (
                first(x, &["bidPrice", "bid_price", "bid"]),
                first(x, &["askPrice", "ask_price", "ask"]),
            );
            (b > 0.0 && a > 0.0).then_some((s, (b, a)))
        })
        .collect()
}

/// Строки свечей из ответа биржи: (время, закрытие, оборот в котировке).
type RowsOf = fn(&Value) -> Vec<(f64, f64, f64)>;

/// Минутные свечи: цены закрытия и оборот за минуту в котируемой валюте (NaN — биржа не отдала).
#[derive(Clone, Debug, Default)]
pub struct Candles {
    pub closes: Vec<f64>,
    pub vols: Vec<f64>,
}

impl Candles {
    pub fn has_volume(&self) -> bool {
        !self.vols.is_empty() && self.vols.iter().all(|v| !v.is_nan())
    }
    /// Оборот за последние n минут.
    pub fn last_volume(&self, n: usize) -> f64 {
        self.vols.iter().rev().take(n).sum()
    }
    /// Оборот в самый тихий полный час; часов — сколько.
    pub fn min_hourly(&self) -> (f64, usize) {
        let hours: Vec<f64> = self
            .vols
            .rchunks_exact(60)
            .map(|c| c.iter().sum())
            .collect();
        (
            hours.iter().copied().fold(f64::INFINITY, f64::min),
            hours.len(),
        )
    }
    /// Доля минут без оборота, %.
    pub fn zero_minutes_pct(&self) -> f64 {
        if !self.has_volume() {
            return f64::NAN;
        }
        self.vols.iter().filter(|v| **v <= 0.0).count() as f64 * 100.0 / self.vols.len() as f64
    }
}

/// Стакан: (уровни бидов, уровни асков) [(цена, кол-во)].
pub type Levels = (Vec<(f64, f64)>, Vec<(f64, f64)>);

/// Разбор стакана биржи.
pub fn parse_book(ex: &str, v: &Value) -> Option<Levels> {
    let lv = |x: &Value| -> Vec<(f64, f64)> {
        x.as_array()
            .into_iter()
            .flatten()
            .filter_map(|l| Some((num(l.get(0)?), num(l.get(1)?))))
            .filter(|l| l.0 > 0.0)
            .collect()
    };
    let (b, a) = match ex {
        "bybit" => (lv(&v["result"]["b"]), lv(&v["result"]["a"])),
        "okx" => (lv(&v["data"][0]["bids"]), lv(&v["data"][0]["asks"])),
        "lbank" | "kucoin" | "bingx" => (lv(&v["data"]["bids"]), lv(&v["data"]["asks"])),
        "upbit" => {
            let u = v[0]["orderbook_units"].as_array()?;
            (
                u.iter()
                    .map(|x| (num(&x["bid_price"]), num(&x["bid_size"])))
                    .collect(),
                u.iter()
                    .map(|x| (num(&x["ask_price"]), num(&x["ask_size"])))
                    .collect(),
            )
        }
        _ => (lv(&v["bids"]), lv(&v["asks"])),
    };
    (!b.is_empty() && !a.is_empty()).then_some((b, a))
}

fn book_url(ex: &str, base: &str, t: &Ticker) -> String {
    match ex {
        "bybit" => format!(
            "{base}/v5/market/orderbook?category=spot&symbol={}&limit=50",
            t.symbol
        ),
        "okx" => format!(
            "{base}/api/v5/market/books?instId={}-{}&sz=50",
            t.base, t.quote
        ),
        "gate" => format!(
            "{base}/api/v4/spot/order_book?currency_pair={}_{}&limit=50",
            t.base, t.quote
        ),
        "upbit" => format!("{base}/v1/orderbook?markets={}-{}", t.quote, t.base),
        "lbank" => format!(
            "{base}/v2/depth.do?symbol={}_{}&size=50",
            t.base.to_lowercase(),
            t.quote.to_lowercase()
        ),
        "kucoin" => format!(
            "{base}/api/v1/market/orderbook/level2_100?symbol={}-{}",
            t.base, t.quote
        ),
        "bingx" => format!(
            "{base}/openApi/spot/v1/market/depth?symbol={}-{}&limit=100",
            t.base, t.quote
        ),
        _ => format!("{base}/api/v3/depth?symbol={}&limit=50", t.symbol),
    }
}

/// Средняя цена рыночной покупки на сумму quote против лучшего аска, %; бесконечность — стакана не хватает.
pub fn impact_pct(asks: &[(f64, f64)], quote: f64) -> f64 {
    let (mut left, mut qty, mut cost) = (quote, 0.0, 0.0);
    for (p, q) in asks {
        let take = left.min(p * q);
        cost += take;
        qty += take / p;
        left -= take;
        if left <= 1e-9 {
            break;
        }
    }
    if left > 1e-9 || qty <= 0.0 {
        return f64::INFINITY;
    }
    (cost / qty / asks[0].0 - 1.0) * 100.0
}

/// Источник данных (адреса по биржам — для проверок свои).
#[derive(Clone)]
pub struct Sources {
    pub urls: HashMap<String, String>,
    /// Адреса сводок перпов (для проверок свои).
    pub perp_urls: HashMap<String, String>,
    http: reqwest::Client,
}

impl Default for Sources {
    fn default() -> Self {
        Sources {
            urls: SUPPORTED
                .iter()
                .filter_map(|e| Some((e.to_string(), base_url(e)?.to_owned())))
                .collect(),
            perp_urls: SUPPORTED
                .iter()
                .filter_map(|e| Some((e.to_string(), perp_url(e)?.to_owned())))
                .collect(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .expect("HTTP-клиент"),
        }
    }
}

impl Sources {
    async fn get(&self, url: &str) -> Result<Value, String> {
        let r = self.http.get(url).send().await.map_err(|e| e.to_string())?;
        let st = r.status();
        let v: Value = r.json().await.map_err(|e| format!("HTTP {st}: {e}"))?;
        if !st.is_success() {
            return Err(format!("HTTP {st}: {v}"));
        }
        Ok(v)
    }

    async fn post(&self, url: &str, body: Value) -> Result<Value, String> {
        let r = self
            .http
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let st = r.status();
        let v: Value = r.json().await.map_err(|e| format!("HTTP {st}: {e}"))?;
        if !st.is_success() {
            return Err(format!("HTTP {st}: {v}"));
        }
        Ok(v)
    }

    /// Сводка перпов биржи (со ставками funding, где биржа их отдаёт); пусто — перпов в подборе нет.
    pub async fn perp_tickers(&self, ex: &str) -> Result<Vec<Ticker>, String> {
        let Some(base) = self.perp_urls.get(ex) else {
            return Ok(Vec::new());
        };
        let _ = crate::infra::ratelimit::take(ex, "discovery").await;
        let n = Value::Null;
        Ok(match ex {
            "binance" => {
                let (u1, u2, u3) = (
                    format!("{base}/fapi/v1/ticker/24hr"),
                    format!("{base}/fapi/v1/ticker/bookTicker"),
                    format!("{base}/fapi/v1/premiumIndex"),
                );
                let (v, b, f) = futures_util::join!(self.get(&u1), self.get(&u2), self.get(&u3));
                parse_perp(ex, &v?, &b.unwrap_or_default(), &f.unwrap_or_default())
            }
            "mexc" => parse_perp(
                ex,
                &self.get(&format!("{base}/api/v1/contract/ticker")).await?,
                &n,
                &n,
            ),
            "bybit" => parse_perp(
                ex,
                &self
                    .get(&format!("{base}/v5/market/tickers?category=linear"))
                    .await?,
                &n,
                &n,
            ),
            "okx" => parse_perp(
                ex,
                &self
                    .get(&format!("{base}/api/v5/market/tickers?instType=SWAP"))
                    .await?,
                &n,
                &n,
            ),
            "gate" => parse_perp(
                ex,
                &self
                    .get(&format!("{base}/api/v4/futures/usdt/tickers"))
                    .await?,
                &n,
                &n,
            ),
            "kucoin" => parse_perp(
                ex,
                &self.get(&format!("{base}/api/v1/contracts/active")).await?,
                &n,
                &n,
            ),
            "bingx" => {
                let (u1, u2) = (
                    format!("{base}/openApi/swap/v2/quote/ticker"),
                    format!("{base}/openApi/swap/v2/quote/premiumIndex"),
                );
                let (v, f) = futures_util::join!(self.get(&u1), self.get(&u2));
                parse_perp(ex, &v?, &n, &f.unwrap_or_default())
            }
            "hyperliquid" => parse_perp(
                ex,
                &self
                    .post(&format!("{base}/info"), json!({"type": "metaAndAssetCtxs"}))
                    .await?,
                &n,
                &n,
            ),
            _ => Vec::new(),
        })
    }

    /// Минутные свечи (не больше limit): цены закрытия и оборот за минуту в котируемой валюте, по возрастанию времени.
    pub async fn candles(&self, t: &Ticker, limit: usize) -> Result<Candles, String> {
        let base = if t.perp {
            self.perp_urls.get(&t.exchange)
        } else {
            self.urls.get(&t.exchange)
        }
        .ok_or("нет источника")?;
        let _ = crate::infra::ratelimit::take(&t.exchange, "discovery").await;
        let now = crate::infra::util::now_ms();
        let (v, rows): (Value, RowsOf) = match (t.exchange.as_str(), t.perp) {
            ("hyperliquid", _) => (
                self.post(
                    &format!("{base}/info"),
                    json!({"type": "candleSnapshot", "req": {"coin": t.venue, "interval": "1m",
                    "startTime": now - limit as u64 * 60_000, "endTime": now}}),
                )
                .await?,
                |v| {
                    v.as_array()
                        .into_iter()
                        .flatten()
                        .map(|k| (num(&k["t"]), num(&k["c"]), num(&k["v"]) * num(&k["c"])))
                        .collect()
                },
            ),
            (_, true) => return Err("свечи перпов в подборе — только у Hyperliquid".into()),
            ("binance" | "mexc", _) => (
                self.get(&format!(
                    "{base}/api/v3/klines?symbol={}&interval=1m&limit={}",
                    t.symbol,
                    limit.min(1000)
                ))
                .await?,
                |v| {
                    v.as_array()
                        .into_iter()
                        .flatten()
                        .map(|k| (num(&k[0]), num(&k[4]), num(&k[7])))
                        .collect()
                },
            ),
            ("bybit", _) => (
                self.get(&format!(
                    "{base}/v5/market/kline?category=spot&symbol={}&interval=1&limit={}",
                    t.symbol,
                    limit.min(1000)
                ))
                .await?,
                |v| {
                    v["result"]["list"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|k| (num(&k[0]), num(&k[4]), num(&k[6])))
                        .collect()
                },
            ),
            ("okx", _) => (
                self.get(&format!(
                    "{base}/api/v5/market/candles?instId={}-{}&bar=1m&limit={}",
                    t.base,
                    t.quote,
                    limit.min(300)
                ))
                .await?,
                |v| {
                    v["data"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|k| (num(&k[0]), num(&k[4]), num(&k[7])))
                        .collect()
                },
            ),
            ("gate", _) => (
                self.get(&format!(
                    "{base}/api/v4/spot/candlesticks?currency_pair={}_{}&interval=1m&limit={}",
                    t.base,
                    t.quote,
                    limit.min(1000)
                ))
                .await?,
                |v| {
                    v.as_array()
                        .into_iter()
                        .flatten()
                        .map(|k| (num(&k[0]), num(&k[2]), num(&k[1])))
                        .collect()
                },
            ),
            ("kucoin", _) => (
                self.get(&format!(
                    "{base}/api/v1/market/candles?type=1min&symbol={}-{}&startAt={}&endAt={}",
                    t.base,
                    t.quote,
                    now / 1000 - limit as u64 * 60,
                    now / 1000
                ))
                .await?,
                |v| {
                    v["data"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|k| (num(&k[0]), num(&k[2]), num(&k[6])))
                        .collect()
                },
            ),
            ("bingx", _) => (
                self.get(&format!(
                    "{base}/openApi/spot/v1/market/kline?symbol={}-{}&interval=1m&limit={}",
                    t.base,
                    t.quote,
                    limit.min(1000)
                ))
                .await?,
                |v| {
                    v["data"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|k| (num(&k[0]), num(&k[4]), num(&k[7])))
                        .collect()
                },
            ),
            ("upbit", _) => (
                self.get(&format!(
                    "{base}/v1/candles/minutes/1?market={}-{}&count={}",
                    t.quote,
                    t.base,
                    limit.min(200)
                ))
                .await?,
                |v| {
                    v.as_array()
                        .into_iter()
                        .flatten()
                        .map(|k| {
                            (
                                num(&k["timestamp"]),
                                num(&k["trade_price"]),
                                num(&k["candle_acc_trade_price"]),
                            )
                        })
                        .collect()
                },
            ),
            ("lbank", _) => (
                self.get(&format!(
                    "{base}/v2/kline.do?symbol={}_{}&size={}&type=minute1&time={}",
                    t.base.to_lowercase(),
                    t.quote.to_lowercase(),
                    limit.min(2000),
                    now / 1000 - limit as u64 * 60
                ))
                .await?,
                |v| {
                    v["data"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|k| (num(&k[0]), num(&k[4]), num(&k[5]) * num(&k[4])))
                        .collect()
                },
            ),
            _ => return Err("свечей нет".into()),
        };
        let mut r = rows(&v);
        r.sort_by(|a, b| a.0.total_cmp(&b.0));
        Ok(Candles {
            closes: r.iter().map(|x| x.1).collect(),
            vols: r.iter().map(|x| x.2).collect(),
        })
    }

    /// Список пар спота биржи для листингов (без лучших цен): у Upbit — рынки KRW, BTC и USDT.
    pub async fn listing_tickers(&self, ex: &str) -> Result<Vec<Ticker>, String> {
        match (ex, self.urls.get(ex)) {
            ("upbit", Some(base)) => {
                let _ = crate::infra::ratelimit::take(ex, "discovery").await;
                Ok(parse(
                    ex,
                    &self
                        .get(&format!(
                            "{base}/v1/ticker/all?quote_currencies=KRW,BTC,USDT"
                        ))
                        .await?,
                ))
            }
            ("lbank", Some(base)) => {
                let _ = crate::infra::ratelimit::take(ex, "discovery").await;
                Ok(parse(
                    ex,
                    &self
                        .get(&format!("{base}/v2/ticker/24hr.do?symbol=all"))
                        .await?,
                ))
            }
            _ => self.tickers(ex).await,
        }
    }

    /// Адрес списка пар биржи по рынку (для админки: «откуда узнали о листинге»).
    pub fn listing_source(&self, ex: &str, perp: bool) -> Option<String> {
        let (b, p) = (self.urls.get(ex), self.perp_urls.get(ex));
        Some(match (ex, perp) {
            ("binance" | "mexc", false) => format!("{}/api/v3/ticker/24hr", b?),
            ("binance", true) => format!("{}/fapi/v1/ticker/24hr", p?),
            ("mexc", true) => format!("{}/api/v1/contract/ticker", p?),
            ("bybit", false) => format!("{}/v5/market/tickers?category=spot", b?),
            ("bybit", true) => format!("{}/v5/market/tickers?category=linear", p?),
            ("okx", false) => format!("{}/api/v5/market/tickers?instType=SPOT", b?),
            ("okx", true) => format!("{}/api/v5/market/tickers?instType=SWAP", p?),
            ("gate", false) => format!("{}/api/v4/spot/tickers", b?),
            ("gate", true) => format!("{}/api/v4/futures/usdt/tickers", p?),
            ("upbit", false) => format!("{}/v1/ticker/all", b?),
            ("lbank", false) => format!("{}/v2/ticker/24hr.do", b?),
            ("kucoin", false) => format!("{}/api/v1/market/allTickers", b?),
            ("kucoin", true) => format!("{}/api/v1/contracts/active", p?),
            ("bingx", false) => format!("{}/openApi/spot/v1/ticker/24hr", b?),
            ("bingx", true) => format!("{}/openApi/swap/v2/quote/ticker", p?),
            ("hyperliquid", true) => format!("{}/info (metaAndAssetCtxs)", p?),
            _ => return None,
        })
    }

    /// GET JSON (для анонсов).
    pub async fn get_json(&self, url: &str) -> Result<Value, String> {
        self.get(url).await
    }

    /// Сводки биржи с лучшими ценами.
    pub async fn tickers(&self, ex: &str) -> Result<Vec<Ticker>, String> {
        let base = self
            .urls
            .get(ex)
            .ok_or_else(|| format!("{ex}: сводок в Rust-скринере нет"))?;
        let url = match ex {
            "binance" | "mexc" => format!("{base}/api/v3/ticker/24hr"),
            "bybit" => format!("{base}/v5/market/tickers?category=spot"),
            "okx" => format!("{base}/api/v5/market/tickers?instType=SPOT"),
            "gate" => format!("{base}/api/v4/spot/tickers"),
            // торговля на Upbit — за USDT (KRW нужен только для поиска листингов)
            "upbit" => format!("{base}/v1/ticker/all?quote_currencies=USDT"),
            "lbank" => format!("{base}/v2/ticker/24hr.do?symbol=all"),
            "kucoin" => format!("{base}/api/v1/market/allTickers"),
            "bingx" => format!(
                "{base}/openApi/spot/v1/ticker/24hr?timestamp={}",
                crate::infra::util::now_ms()
            ),
            // Hyperliquid в подборе — только перпы
            "hyperliquid" => return Ok(Vec::new()),
            _ => return Err(format!("{ex}: сводок в Rust-скринере нет")),
        };
        let _ = crate::infra::ratelimit::take(ex, "discovery").await;
        let mut ts = parse(ex, &self.get(&url).await?);
        match ex {
            "upbit" => {
                let markets: Vec<String> = ts
                    .iter()
                    .map(|t| format!("{}-{}", t.quote, t.base))
                    .collect();
                let mut top = HashMap::new();
                for chunk in markets.chunks(50) {
                    // ошибка пачки — её рынки без цен, не сбой сводки
                    if let Ok(v) = self
                        .get(&format!("{base}/v1/orderbook?markets={}", chunk.join(",")))
                        .await
                    {
                        top.extend(upbit_top(&v));
                    }
                }
                for t in &mut ts {
                    if let Some((b, a)) = top.get(&format!("{}-{}", t.quote, t.base)) {
                        (t.bid, t.ask) = (*b, *a);
                    }
                }
            }
            "lbank" => {
                let top = match self
                    .get(&format!("{base}/v2/supplement/ticker/bookTicker.do"))
                    .await
                {
                    Ok(v) => lbank_top(&v),
                    Err(_) => HashMap::new(),
                };
                if top.len() <= 1 {
                    tracing::info!(
                        "[screener] lbank: лучшие цены всех пар одним запросом недоступны — спред LBank неизвестен"
                    );
                }
                for t in &mut ts {
                    if let Some((b, a)) = top.get(&format!("{}_{}", t.base, t.quote).to_lowercase())
                    {
                        (t.bid, t.ask) = (*b, *a);
                    }
                }
            }
            _ => {}
        }
        Ok(ts)
    }

    pub async fn book(&self, t: &Ticker) -> Result<Levels, String> {
        let base = self.urls.get(&t.exchange).ok_or("нет источника")?;
        parse_book(&t.exchange, &{
            let _ = crate::infra::ratelimit::take(&t.exchange, "discovery").await;
            self.get(&book_url(&t.exchange, base, t)).await?
        })
        .ok_or_else(|| "пустой стакан".into())
    }
}

/// Пороги подбора из настроек.
#[derive(Clone, Debug)]
pub struct Params {
    pub quotes: Vec<String>,
    pub exclude_leveraged: bool,
    pub exclude_stable: bool,
    pub spread_min_volume: f64,
    pub spread_max_volume: f64,
    pub spread_min_edge: f64,
    pub spread_max: f64,
    pub max_impact: f64,
    pub min_book_levels: usize,
    pub depth_band: f64,
    pub max_tick: f64,
    pub books_per_exchange: usize,
    pub order_quote: f64,
    pub arb_min_volume: f64,
    pub arb_max_gross: f64,
    /// Стат-арб: оборот самых ликвидных монет не меньше; корреляция минутных доходностей не меньше.
    pub min_volume: f64,
    pub statarb_min_corr: f64,
    /// Свечи: оборот за последний час, в самый тихий час, доля минут без оборота (100 — не проверять).
    pub min_volume_1h: f64,
    pub min_hourly_volume: f64,
    pub max_zero_minutes: f64,
    /// Lead-lag: ведущая биржа, движение для входа, %.
    pub lead_leader: String,
    pub lead_min_move: f64,
    pub lead_min_profit: f64,
    /// Перп – перп: прибыль после 4 комиссий не меньше, %.
    pub perp_min_profit: f64,
}

impl Params {
    pub fn from(p: &Map<String, Value>) -> Params {
        let f = |k: &str, d: f64| p.get(k).map(num).filter(|x| !x.is_nan()).unwrap_or(d);
        let s = |k: &str, d: &str| p.get(k).and_then(Value::as_str).unwrap_or(d).to_uppercase();
        let ex = s("discoveryExclude", "LEVERAGED,STABLE");
        Params {
            quotes: s("discoveryQuotes", "USDT,USDC,USD")
                .split(',')
                .map(|x| x.trim().to_owned())
                .filter(|x| !x.is_empty())
                .collect(),
            exclude_leveraged: ex.contains("LEVERAGED"),
            exclude_stable: ex.contains("STABLE"),
            spread_min_volume: f("discoverySpreadMinVolume", 50_000.0),
            spread_max_volume: f("discoverySpreadMaxVolume", 5_000_000.0),
            spread_min_edge: f("discoverySpreadMinEdgePercent", 0.1),
            spread_max: f("discoverySpreadMaxPercent", 3.0),
            max_impact: f("discoveryMaxImpactPercent", 1.0),
            min_book_levels: f("discoveryMinBookLevels", 3.0) as usize,
            depth_band: f("discoveryDepthBandPercent", 0.5),
            max_tick: f("discoveryMaxTickPercent", 0.5),
            books_per_exchange: f("discoveryBacktestPerExchange", 8.0) as usize,
            order_quote: f("scOrderQuote", 20.0),
            arb_min_volume: f("discoveryArbMinVolume", 50_000.0),
            arb_max_gross: f("discoveryArbMaxGrossPercent", 5.0),
            min_volume: f("discoveryMinVolume", 1_000_000.0),
            statarb_min_corr: f("statArbMinCorrelation", 0.6),
            min_volume_1h: f("discoveryMinVolume1h", 50_000.0),
            min_hourly_volume: f("discoveryMinHourlyVolume", 10_000.0),
            max_zero_minutes: f("discoveryMaxZeroVolumeMinutesPercent", 50.0),
            lead_leader: p
                .get("leadLagLeader")
                .and_then(Value::as_str)
                .unwrap_or("binance")
                .to_lowercase(),
            lead_min_move: f("leadLagMinMovePercent", 0.15),
            lead_min_profit: f("leadLagMinProfitPercent", 0.05),
            perp_min_profit: f("perpArbMinProfitPercent", 0.05),
        }
    }

    /// Пороги с фильтрами подбора биржи и стратегии (discoveryFilters: {биржа: {стратегия: {параметр: значение}}}).
    pub fn at(settings: &Map<String, Value>, ex: &str, strategy: &str) -> Params {
        let f = filters_of(settings, ex, strategy);
        if f.is_empty() {
            return Params::from(settings);
        }
        let mut m = settings.clone();
        m.extend(f);
        Params::from(&m)
    }
}

/// Фильтры подбора биржи и стратегии из discoveryFilters (строка JSON или объект).
pub fn filters_of(settings: &Map<String, Value>, ex: &str, strategy: &str) -> Map<String, Value> {
    let raw = settings
        .get("discoveryFilters")
        .cloned()
        .unwrap_or_default();
    let v = match raw {
        Value::String(s) => serde_json::from_str(&s).unwrap_or_default(),
        v => v,
    };
    v[ex.to_lowercase()][strategy]
        .as_object()
        .cloned()
        .unwrap_or_default()
}

const STABLES: &[&str] = &[
    "USDT", "USDC", "USD", "DAI", "TUSD", "FDUSD", "BUSD", "USDE", "PYUSD", "USDD", "EUR", "EURC",
    "USD1",
];

/// Почему тикер исключён (токены с плечом, стейблкоины, котировка не из списка); None — подходит.
fn excluded(t: &Ticker, p: &Params) -> Option<&'static str> {
    if !p.quotes.contains(&t.quote) {
        return Some("котировка не из discoveryQuotes");
    }
    let b = t.base.as_str();
    if p.exclude_leveraged
        && ["3L", "3S", "5L", "5S", "UP", "DOWN", "BULL", "BEAR"]
            .iter()
            .any(|s| b.ends_with(s) && b.len() > s.len())
    {
        return Some("токен с плечом");
    }
    if p.exclude_stable && STABLES.contains(&b) {
        return Some("стейблкоин");
    }
    None
}

fn fee(ex: &str, key: &str) -> f64 {
    crate::infra::data::catalog()["exchanges"]
        .as_array()
        .and_then(|a| a.iter().find(|e| e["id"] == ex))
        .and_then(|e| e[key].as_f64())
        .unwrap_or(0.1)
}

fn pct(x: f64) -> String {
    format!("{x:.3}%")
}

fn money(x: f64) -> String {
    if x >= 1e6 {
        format!("{:.1} млн", x / 1e6)
    } else if x >= 1e3 {
        format!("{:.0} тыс", x / 1e3)
    } else {
        format!("{x:.0}")
    }
}

fn candidate(
    ex: &str,
    sym: &str,
    verdict: &str,
    score: f64,
    reasons: Vec<String>,
    metrics: Value,
    profit: Option<Value>,
) -> Value {
    json!({"exchange": ex, "symbol": sym, "suitable": verdict == "подходит", "verdict": verdict, "score": score,
        "reasons": reasons, "metrics": metrics, "backtest": null, "profit": profit})
}

/// Что выбрать: рынок и монеты по биржам — в порядке ног (первая — покупка), как pick в Java.
fn pick(market: &str, legs: &[(&str, &str)]) -> Map<String, Value> {
    let mut syms = Map::new();
    for (e, s) in legs {
        if let Some(Value::Array(a)) = syms.get_mut(*e) {
            a.push(json!(s));
        } else {
            syms.insert((*e).to_owned(), json!([s]));
        }
    }
    let mut m = Map::new();
    m.insert("market".into(), json!(market));
    m.insert("symbols".into(), json!(syms));
    m
}

/// Сбор спреда: кандидаты по сводкам; books — стаканы лучших кандидатов (нет — «под вопросом»).
pub fn spread_capture(
    tickers: &[Ticker],
    books: &HashMap<String, Result<Levels, String>>,
    candles: &HashMap<String, Result<Candles, String>>,
    pf: &dyn Fn(&str) -> Params,
) -> Value {
    let mut out = Vec::new();
    for t in tickers.iter().filter(|t| !t.perp) {
        let p = &pf(&t.exchange);
        let (sp, vol) = (t.spread_pct(), t.volume);
        if sp.is_nan()
            || vol.is_nan()
            || vol < p.spread_min_volume
            || vol > p.spread_max_volume
            || excluded(t, p).is_some()
        {
            continue;
        }
        let maker = fee(&t.exchange, "makerFeePct");
        let edge = sp - 2.0 * maker;
        if edge < p.spread_min_edge || sp > p.spread_max {
            continue;
        }
        let mut why = vec![
            format!(
                "спред {}, после двух maker-комиссий остаётся {}",
                pct(sp),
                pct(edge)
            ),
            format!("оборот {}", money(vol)),
        ];
        let key = format!("{}:{}", t.exchange, t.symbol);
        // свечи: монета живая сейчас и торгуется равномерно
        let dead = match candles.get(&key) {
            Some(Ok(c)) => illiquid(c, p, &mut why),
            Some(Err(e)) => {
                why.push(format!("свечи не получены: {e}"));
                None
            }
            None => None,
        };
        let verdict = if let Some(d) = dead {
            why.push(d);
            "не подходит"
        } else {
            match books.get(&key) {
                None => {
                    why.push("стакан не проверен: не вошёл в число лучших кандидатов биржи".into());
                    "под вопросом"
                }
                Some(Err(e)) => {
                    why.push(format!("стакан не проверен: {e}"));
                    "под вопросом"
                }
                Some(Ok((bids, asks))) => match bad_book(bids, asks, p) {
                    Some(bad) => {
                        why.push(bad);
                        "не подходит"
                    }
                    None => {
                        why.push(format!(
                            "проскальзывание на {} {}",
                            money(p.order_quote),
                            pct(impact_pct(asks, p.order_quote))
                        ));
                        "подходит"
                    }
                },
            }
        };
        let mut m = pick("spot", &[(&t.exchange, &t.symbol)]);
        m.insert("spreadPct".into(), json!(sp));
        m.insert("edgePct".into(), json!(edge));
        m.insert("quoteVolume24h".into(), json!(vol));
        m.insert("makerFeePct".into(), json!(maker));
        let profit = json!({"text": format!("{} с круга", pct(edge)), "note": "спред минус две комиссии мейкера"});
        let score = edge
            * vol.max(10.0).log10()
            * if verdict == "подходит" {
                1.0
            } else if verdict == "под вопросом" {
                0.01
            } else {
                0.001
            };
        out.push(candidate(
            &t.exchange,
            &t.symbol,
            verdict,
            score,
            why,
            Value::Object(m),
            Some(profit),
        ));
    }
    sort(&mut out);
    let p = pf("");
    json!({"id": crate::strategy::spread::ID, "title": "Сбор широкого спреда", "executable": true,
        "description": "Лимитная покупка по биду и продажа по аску; нужны широкий спред и ровная торговля.",
        "criteria": [format!("спред − 2 maker ≥ {}%", p.spread_min_edge), format!("спред ≤ {}%", p.spread_max),
            format!("оборот {}–{}", money(p.spread_min_volume), money(p.spread_max_volume)),
            format!("по минутным свечам: оборот за час ≥ {}, в самый тихий час ≥ {}, минут без оборота ≤ {}%",
                money(p.min_volume_1h), money(p.min_hourly_volume), p.max_zero_minutes)],
        "candidates": out})
}

/// Неликвидность по минутным свечам (как illiquid в Java); удачные проверки — в why.
fn illiquid(c: &Candles, p: &Params, why: &mut Vec<String>) -> Option<String> {
    if !c.has_volume() {
        why.push("оборот по свечам биржа не отдала".into());
        return None;
    }
    let (v1h, (min_h, hours)) = (c.last_volume(60), c.min_hourly());
    if p.min_volume_1h > 0.0 && v1h < p.min_volume_1h {
        return Some(format!(
            "оборот за последний час {} < {}",
            money(v1h),
            money(p.min_volume_1h)
        ));
    }
    if p.min_hourly_volume > 0.0 && hours > 0 && min_h < p.min_hourly_volume {
        return Some(format!(
            "в самый тихий час из {hours} оборот {} < {}",
            money(min_h),
            money(p.min_hourly_volume)
        ));
    }
    why.push(format!(
        "оборот за час {}{}",
        money(v1h),
        if hours > 0 {
            format!(", в самый тихий час {}", money(min_h))
        } else {
            String::new()
        }
    ));
    let zero = c.zero_minutes_pct();
    if p.max_zero_minutes < 100.0 && zero > p.max_zero_minutes {
        return Some(format!(
            "минут без оборота {zero:.0}% (порог {:.0}%)",
            p.max_zero_minutes
        ));
    }
    why.push(format!("минут без оборота {zero:.0}%"));
    None
}

/// Lead-lag (как LeadLag в Java): ведущая биржа (leadLagLeader) и ведомые — тот же рынок, у ведущей оборот больше,
/// спред ведомой не шире движения для входа. «В выбор» — монета на обеих биржах.
pub fn lead_lag(tickers: &[Ticker], pf: &dyn Fn(&str) -> Params) -> Value {
    let p0 = pf("");
    let leader = p0.lead_leader.clone();
    let key = |t: &Ticker| format!("{}{}", t.symbol, if t.perp { ":perp" } else { "" });
    let lead: HashMap<String, &Ticker> = tickers
        .iter()
        .filter(|t| t.exchange == leader && t.bid > 0.0 && t.ask > 0.0)
        .map(|t| (key(t), t))
        .collect();
    let mut out = Vec::new();
    for f in tickers
        .iter()
        .filter(|t| t.exchange != leader && t.bid > 0.0 && t.ask > 0.0)
    {
        let p = pf(&f.exchange);
        if excluded(f, &p).is_some() || f.volume < p.arb_min_volume {
            continue;
        }
        let Some(l) = lead.get(&key(f)) else { continue };
        let (spread, ratio) = (f.spread_pct(), l.volume / f.volume);
        let mut why = vec![
            format!(
                "ведущая {leader}: оборот {}, ведомая {}: {}",
                money(l.volume),
                f.exchange,
                money(f.volume)
            ),
            format!("спред ведомой {}", pct(spread)),
        ];
        let bad = if ratio.is_nan() || ratio <= 1.0 {
            Some("на ведомой оборот не меньше, чем на ведущей — она вряд ли отстаёт".to_owned())
        } else if spread > p.lead_min_move {
            Some(format!(
                "спред ведомой {} шире движения для входа {}%",
                pct(spread),
                p.lead_min_move
            ))
        } else {
            None
        };
        let ok = bad.is_none();
        why.extend(bad);
        let mut m = pick(
            if f.perp { "perp" } else { "spot" },
            &[(&leader, &l.symbol), (&f.exchange, &f.symbol)],
        );
        m.insert("leader".into(), json!(leader));
        m.insert("follower".into(), json!(f.exchange));
        m.insert("volumeRatio".into(), json!(ratio));
        m.insert("followerSpreadPct".into(), json!(spread));
        m.insert("minProfitPct".into(), json!(p.lead_min_profit));
        m.insert("perp".into(), json!(f.perp));
        let score = if ratio > 0.0 { ratio.log10() } else { -1.0 } - spread;
        out.push(candidate(
            &format!("{leader}→{}", f.exchange),
            &f.symbol,
            if ok {
                "подходит"
            } else {
                "не подходит"
            },
            if ok { score } else { score - 10.0 },
            why,
            Value::Object(m),
            None,
        ));
    }
    sort(&mut out);
    json!({"id": "lead-lag", "title": "Lead-lag: ведущая биржа – ведомая", "executable": true,
        "description": format!("Цену монеты первой меняет ведущая биржа ({leader}), ведомые повторяют с задержкой. Бот следит за ведущей и торгует только на ведомой. «В выбор» добавляет монету на обеих биржах."),
        "criteria": [format!("ведущая биржа ({leader}) — среди бирж подбора; рынок тот же (спот со спотом, перп с перпом)"),
            format!("оборот на ведомой ≥ {}, на ведущей — больше", money(p0.arb_min_volume)),
            format!("спред ведомой не шире движения для входа ({}%)", p0.lead_min_move)],
        "candidates": out})
}

/// Перп – перп на разных биржах (как PerpPriceArb в Java): лонг там, где дешевле, шорт — где дороже; после 4 комиссий.
pub fn perp_arb(tickers: &[Ticker], pf: &dyn Fn(&str) -> Params) -> Value {
    let p0 = pf("");
    let mut by: BTreeMap<String, Vec<&Ticker>> = BTreeMap::new();
    for t in tickers
        .iter()
        .filter(|t| t.perp && t.bid > 0.0 && t.ask > 0.0)
    {
        by.entry(t.symbol.clone()).or_default().push(t);
    }
    let mut out = Vec::new();
    for (coin, ts) in by {
        let (Some(buy), Some(sell)) = (
            ts.iter().min_by(|a, b| a.ask.total_cmp(&b.ask)),
            ts.iter().max_by(|a, b| a.bid.total_cmp(&b.bid)),
        ) else {
            continue;
        };
        if buy.exchange == sell.exchange {
            continue;
        }
        let gross = (sell.bid - buy.ask) / buy.ask * 100.0;
        if gross.abs()
            > pf(&buy.exchange)
                .arb_max_gross
                .min(pf(&sell.exchange).arb_max_gross)
        {
            continue; // скорее всего, разные контракты
        }
        let net = gross
            - 2.0 * fee(&buy.exchange, "perpTakerFeePct")
            - 2.0 * fee(&sell.exchange, "perpTakerFeePct");
        if net < -0.3 {
            continue;
        }
        let ok = net >= p0.perp_min_profit;
        let why = vec![
            format!(
                "лонг {} {} → шорт {} {}",
                buy.exchange, buy.ask, sell.exchange, sell.bid
            ),
            format!("разница {}, после 4 комиссий {}", pct(gross), pct(net)),
            format!("оборот за сутки от {}", money(buy.volume.min(sell.volume))),
        ];
        let mut m = pick(
            "perp",
            &[(&buy.exchange, &buy.symbol), (&sell.exchange, &sell.symbol)],
        );
        m.insert("netPct".into(), json!(net));
        out.push(candidate(
            &format!("{}→{}", buy.exchange, sell.exchange),
            &coin,
            if ok {
                "подходит"
            } else {
                "близко"
            },
            if ok { net } else { net - 1.0 },
            why,
            Value::Object(m),
            None,
        ));
    }
    sort(&mut out);
    json!({"id": "perp-arb", "title": "Перп – перп на разных биржах", "executable": true,
        "description": "Шорт перпа там, где он дороже, лонг — где дешевле; закрытие, когда цены сходятся. Сводка — снимок с разницей в секунды: подсказка, какие пары смотреть.",
        "criteria": ["монета — перп минимум на двух биржах, есть bid/ask".to_owned(),
            format!("после 4 комиссий тейкера остаётся ≥ {}% (показываем и близкие)", p0.perp_min_profit),
            format!("разница > {}% отбрасывается: скорее всего, разные контракты", p0.arb_max_gross)],
        "candidates": out})
}

/// Сколько самых ликвидных монет биржи проверять попарно (стат-арб).
const STATARB_TOP: usize = 6;

/// Монеты для стат-арба по биржам (самые ликвидные с одной котировкой; перпы — только Hyperliquid, у них свечи перпов).
pub fn statarb_pool(tickers: &[Ticker], pf: &dyn Fn(&str) -> Params) -> Vec<Vec<Ticker>> {
    let mut by: BTreeMap<(String, String), Vec<&Ticker>> = BTreeMap::new();
    for t in tickers {
        let p = pf(&t.exchange);
        if (t.perp && t.exchange != "hyperliquid")
            || t.volume < p.min_volume
            || excluded(t, &p).is_some()
        {
            continue;
        }
        by.entry((t.exchange.clone(), t.quote.clone()))
            .or_default()
            .push(t);
    }
    by.into_values()
        .filter(|l| l.len() >= 2)
        .map(|mut l| {
            l.sort_by(|a, b| b.volume.total_cmp(&a.volume));
            l.into_iter().take(STATARB_TOP).cloned().collect()
        })
        .collect()
}

fn returns(c: &[f64]) -> Vec<f64> {
    c.windows(2)
        .map(|w| {
            if w[0] > 0.0 && w[1] > 0.0 {
                (w[1] / w[0]).ln()
            } else {
                0.0
            }
        })
        .collect()
}

/// Корреляция Пирсона по последним общим точкам; NaN — нет разброса.
pub fn correlation(a: &[f64], b: &[f64]) -> f64 {
    let n = a.len().min(b.len());
    let (a, b) = (&a[a.len() - n..], &b[b.len() - n..]);
    if n < 2 {
        return f64::NAN;
    }
    let (ma, mb) = (
        a.iter().sum::<f64>() / n as f64,
        b.iter().sum::<f64>() / n as f64,
    );
    let (mut cov, mut va, mut vb) = (0.0, 0.0, 0.0);
    for i in 0..n {
        cov += (a[i] - ma) * (b[i] - mb);
        va += (a[i] - ma).powi(2);
        vb += (b[i] - mb).powi(2);
    }
    if va > 0.0 && vb > 0.0 {
        cov / (va * vb).sqrt()
    } else {
        f64::NAN
    }
}

/// Бэктест пары (как в Java): спред ln(A/B), z-score за 60 мин; вход при |z| ≥ 2, выход при возврате z к 0 или через
/// 120 мин; итог в % на ногу после комиссий круга. Возвращает (итог %, сделок, минут).
pub fn pair_backtest(a: &[f64], b: &[f64], fees_pct: f64) -> (f64, usize, usize) {
    let n = a.len().min(b.len());
    let (a, b) = (&a[a.len() - n..], &b[b.len() - n..]);
    let w = 60;
    let sp: Vec<f64> = (0..n)
        .map(|i| {
            if a[i] > 0.0 && b[i] > 0.0 {
                (a[i] / b[i]).ln()
            } else {
                f64::NAN
            }
        })
        .collect();
    let (mut net, mut trades, mut dir, mut opened, mut entry) = (0.0, 0usize, 0i32, 0usize, 0.0);
    for i in w..n {
        let win: Vec<f64> = sp[i - w..i]
            .iter()
            .copied()
            .filter(|x| !x.is_nan())
            .collect();
        if win.len() < w / 2 || sp[i].is_nan() {
            continue;
        }
        let mean = win.iter().sum::<f64>() / win.len() as f64;
        let sd = (win.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / win.len() as f64).sqrt();
        if sd <= 0.0 {
            continue;
        }
        let z = (sp[i] - mean) / sd;
        if dir == 0 && z.abs() >= 2.0 {
            dir = if z > 0.0 { -1 } else { 1 };
            entry = sp[i];
            opened = i;
        } else if dir != 0 && (dir as f64 * z >= 0.0 || i - opened >= 120) {
            net += dir as f64 * (sp[i] - entry) * 100.0 - fees_pct;
            trades += 1;
            dir = 0;
        }
    }
    (net, trades, n)
}

/// Статистический арбитраж (как StatArb в Java): пары самых ликвидных монет биржи, корреляция минутных доходностей и бэктест.
pub fn stat_arb(
    pools: &[Vec<Ticker>],
    closes: &HashMap<String, Vec<f64>>,
    pf: &dyn Fn(&str) -> Params,
) -> Value {
    let p0 = pf("");
    let mut out = Vec::new();
    for top in pools {
        let ex = &top[0].exchange;
        let p = pf(ex);
        for i in 0..top.len() {
            for j in i + 1..top.len() {
                let (a, b) = (&top[i], &top[j]);
                let mut m = pick(
                    if a.perp { "perp" } else { "spot" },
                    &[(ex, &a.symbol), (ex, &b.symbol)],
                );
                let name = format!("{}/{}", a.symbol, b.symbol);
                let (Some(ca), Some(cb)) = (
                    closes.get(&format!("{ex}:{}", a.symbol)),
                    closes.get(&format!("{ex}:{}", b.symbol)),
                ) else {
                    out.push(candidate(
                        ex,
                        &name,
                        "под вопросом",
                        -0.5,
                        vec!["нет минутных свечей (лимит запросов или ошибка)".into()],
                        Value::Object(m),
                        None,
                    ));
                    continue;
                };
                if ca.len() <= 60 || cb.len() <= 60 {
                    out.push(candidate(
                        ex,
                        &name,
                        "под вопросом",
                        -0.5,
                        vec!["мало минутных свечей".into()],
                        Value::Object(m),
                        None,
                    ));
                    continue;
                }
                let (ra, rb) = (returns(ca), returns(cb));
                let corr = correlation(&ra, &rb);
                let (net, trades, bars) = pair_backtest(ca, cb, 4.0 * fee(ex, "takerFeePct"));
                m.insert("correlation".into(), json!(corr));
                let ok = corr >= p.statarb_min_corr && trades >= 2 && net > 0.0;
                let why = vec![
                    format!(
                        "корреляция {corr:.2} (нужно ≥ {:.2}) по {} минутам",
                        p.statarb_min_corr,
                        ra.len().min(rb.len())
                    ),
                    format!(
                        "бэктест {bars} мин: сделок {trades}, итог {} после 4 комиссий",
                        pct(net)
                    ),
                    format!("оборот {} и {}", money(a.volume), money(b.volume)),
                ];
                let mut c = candidate(
                    ex,
                    &name,
                    if ok {
                        "подходит"
                    } else {
                        "не подходит"
                    },
                    if ok { net } else { corr - 1.0 },
                    why,
                    Value::Object(m),
                    None,
                );
                c["backtest"] = json!({"bars": bars, "trades": trades, "netPct": net});
                out.push(c);
            }
        }
    }
    sort(&mut out);
    json!({"id": crate::strategy::statarb::ID, "title": "Статистический арбитраж", "executable": true,
        "description": "Пара монет, которые обычно ходят вместе: когда спред между ними отклоняется, одна покупается, другая продаётся, закрытие — при возврате. Подбор считает корреляцию минутных доходностей самых ликвидных монет биржи.",
        "criteria": [format!("{STATARB_TOP} самых ликвидных монет биржи с одной котировкой (оборот ≥ {})", money(p0.min_volume)),
            format!("корреляция минутных доходностей ≥ {}", p0.statarb_min_corr), "бэктест за ~5 часов: ≥ 2 сделок и плюс после 4 комиссий".to_owned()],
        "candidates": out})
}

/// Причина отказа по стакану или None.
fn bad_book(bids: &[(f64, f64)], asks: &[(f64, f64)], p: &Params) -> Option<String> {
    let imp = impact_pct(asks, p.order_quote);
    if p.max_impact > 0.0 && imp > p.max_impact {
        return Some(if imp.is_infinite() {
            format!("стакана не хватает на сделку {}", money(p.order_quote))
        } else {
            format!("проскальзывание {} > {}%", pct(imp), p.max_impact)
        });
    }
    let (bb, ba) = (bids[0].0, asks[0].0);
    let nb = bids
        .iter()
        .filter(|l| l.0 >= bb * (1.0 - p.depth_band / 100.0))
        .count();
    let na = asks
        .iter()
        .filter(|l| l.0 <= ba * (1.0 + p.depth_band / 100.0))
        .count();
    if p.min_book_levels > 0 && nb.min(na) < p.min_book_levels {
        return Some(format!(
            "уровней в стакане ±{}%: {} < {} (дыры в стакане)",
            p.depth_band,
            nb.min(na),
            p.min_book_levels
        ));
    }
    let tick = asks
        .windows(2)
        .map(|w| (w[1].0 - w[0].0).abs())
        .filter(|d| *d > 0.0)
        .fold(f64::INFINITY, f64::min);
    if p.max_tick > 0.0 && tick.is_finite() && tick / ba * 100.0 > p.max_tick {
        return Some(format!(
            "шаг цены {} > {}% от цены",
            pct(tick / ba * 100.0),
            p.max_tick
        ));
    }
    None
}

fn sort(v: &mut [Value]) {
    v.sort_by(|a, b| num(&b["score"]).total_cmp(&num(&a["score"])));
}

/// Итог пересчёта по бирже.
#[derive(Clone, Debug, Default)]
pub struct ExStatus {
    pub ok: bool,
    pub tickers: usize,
    /// Перпов в сводке; ошибка сводки перпов.
    pub perps: usize,
    pub perp_error: String,
    pub with_prices: usize,
    pub wait_ms: u64,
    pub error: String,
    pub depth_error: String,
}

/// Состояние скринера.
#[derive(Default)]
pub struct Screener {
    pub running: bool,
    pub updated_at: u64,
    pub duration_ms: u64,
    pub next_at: Option<u64>,
    pub exchanges: BTreeMap<String, ExStatus>,
    pub strategies: Vec<Value>,
}

/// Пересчёт (как DiscoveryService в Java): сводки спота и перпов всех бирж параллельно, стаканы и минутные свечи лучших
/// кандидатов сбора спреда, свечи самых ликвидных монет для стат-арба, профили. Пороги — из настроек с фильтрами
/// подбора по бирже и стратегии (discoveryFilters).
pub async fn run(
    src: &Sources,
    exchanges: &[String],
    settings: &Map<String, Value>,
) -> (BTreeMap<String, ExStatus>, Vec<Value>) {
    let jobs = exchanges.iter().map(|ex| async move {
        let t0 = Instant::now();
        let (spot, perp) = futures_util::join!(src.tickers(ex), src.perp_tickers(ex));
        (ex.clone(), spot, perp, t0.elapsed().as_millis() as u64)
    });
    let mut status = BTreeMap::new();
    let mut all = Vec::new();
    for (ex, spot, perp, ms) in futures_util::future::join_all(jobs).await {
        let mut st = ExStatus {
            wait_ms: ms,
            ..Default::default()
        };
        match spot {
            Ok(ts) => {
                st.ok = true;
                st.tickers = ts.len();
                st.with_prices = ts.iter().filter(|t| t.bid > 0.0).count();
                all.extend(ts);
            }
            Err(e) => st.error = e,
        }
        match perp {
            Ok(ts) => {
                st.ok = st.ok || !ts.is_empty();
                st.perps = ts.len();
                all.extend(ts.into_iter().filter(|t| excluded_quote(t, settings)));
            }
            Err(e) => st.perp_error = e,
        }
        status.insert(ex, st);
    }
    let sc = |ex: &str| Params::at(settings, ex, crate::strategy::spread::ID);
    // стаканы и свечи: лучшие по спреду кандидаты каждой биржи
    let pre = spread_capture(&all, &HashMap::new(), &HashMap::new(), &sc);
    let mut per: HashMap<String, usize> = HashMap::new();
    let wanted: Vec<Ticker> = pre["candidates"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| {
            let ex = c["exchange"].as_str()?;
            let n = per.entry(ex.to_owned()).or_default();
            *n += 1;
            (*n <= sc(ex).books_per_exchange)
                .then(|| {
                    all.iter()
                        .find(|t| !t.perp && t.exchange == ex && t.symbol == c["symbol"])
                        .cloned()
                })
                .flatten()
        })
        .collect();
    let key = |t: &Ticker| format!("{}:{}", t.exchange, t.symbol);
    let (books, candles) = futures_util::join!(
        futures_util::future::join_all(
            wanted
                .iter()
                .map(|t| async move { (key(t), src.book(t).await) })
        ),
        futures_util::future::join_all(
            wanted
                .iter()
                .map(|t| async move { (key(t), src.candles(t, 300).await) })
        )
    );
    let books: HashMap<String, Result<Levels, String>> = books.into_iter().collect();
    let candles: HashMap<String, Result<Candles, String>> = candles.into_iter().collect();
    for (k, r) in &books {
        if let (Err(e), Some((ex, _))) = (r, k.split_once(':'))
            && let Some(s) = status.get_mut(ex)
        {
            s.depth_error = e.clone();
        }
    }
    // стат-арб: минутные цены самых ликвидных монет
    let sa = |ex: &str| Params::at(settings, ex, crate::strategy::statarb::ID);
    let pools = statarb_pool(&all, &sa);
    let need: Vec<&Ticker> = pools.iter().flatten().collect();
    let got = &candles;
    let closes: HashMap<String, Vec<f64>> =
        futures_util::future::join_all(need.iter().map(|t| async move {
            let k = key(t);
            match got.get(&k) {
                Some(Ok(c)) => (k, Some(c.closes.clone())),
                _ => (k, src.candles(t, 300).await.ok().map(|c| c.closes)),
            }
        }))
        .await
        .into_iter()
        .filter_map(|(k, c)| Some((k, c?)))
        .collect();
    let mut out = vec![
        spread_capture(&all, &books, &candles, &sc),
        lead_lag(&all, &|ex| Params::at(settings, ex, "lead-lag")),
        perp_arb(&all, &|ex| Params::at(settings, ex, "perp-arb")),
        stat_arb(&pools, &closes, &sa),
    ];
    // биржа далеко от сервера бота (serverRegion) — стратегии «не подходит», как в Java
    crate::exchange::regions::mark(&mut out, crate::exchange::regions::Region::server(settings));
    (status, out)
}

/// Перп с котировкой из discoveryQuotes.
fn excluded_quote(t: &Ticker, settings: &Map<String, Value>) -> bool {
    Params::from(settings).quotes.contains(&t.quote)
}

impl Screener {
    /// Ответ /discovery в формате Java.
    pub fn json(
        &self,
        chosen: &[String],
        refresh_sec: u64,
        server: Option<crate::exchange::regions::Region>,
    ) -> Value {
        let exs: Map<String, Value> = self
            .exchanges
            .iter()
            .map(|(ex, s)| {
                (ex.clone(), json!({"network": "основная", "ok": s.ok, "tickers": s.tickers, "withBidAsk": s.with_prices,
                    "waitMs": s.wait_ms, "error": if s.error.is_empty() { Value::Null } else { json!(s.error) },
                    "perps": s.perps, "perpError": if s.perp_error.is_empty() { Value::Null } else { json!(s.perp_error) },
                    "depthError": if s.depth_error.is_empty() { Value::Null } else { json!(s.depth_error) }}))
            })
            .collect();
        let unsupported: Vec<&String> = chosen
            .iter()
            .filter(|e| !SUPPORTED.contains(&e.as_str()))
            .collect();
        json!({
            "running": self.running, "updatedAt": self.updated_at, "durationMs": self.duration_ms,
            "refreshSeconds": refresh_sec, "nextRefreshAt": self.next_at,
            "chosenExchanges": chosen, "availableExchanges": SUPPORTED,
            "exchanges": exs, "strategies": self.strategies, "location": crate::exchange::regions::location(server),
            "note": if unsupported.is_empty() { String::new() } else {
                format!("Нет сводок в Rust-скринере: {}", unsupported.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")) },
            "status": if chosen.is_empty() { "Выберите биржи для подбора" } else { "" },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> Params {
        Params::from(&Map::new())
    }

    #[test]
    fn parse_sources_and_top_of_book() {
        let up = parse(
            "upbit",
            &json!([{"market": "USDT-BTC", "trade_price": 60000, "acc_trade_price_24h": 1e6, "signed_change_rate": 0.01},
            {"market": "KRW-ETH", "trade_price": 4e6, "acc_trade_price_24h": 1e9, "signed_change_rate": -0.02}]),
        );
        assert_eq!(
            (
                up[0].symbol.as_str(),
                up[0].base.as_str(),
                up[0].quote.as_str()
            ),
            ("BTCUSDT", "BTC", "USDT")
        );
        assert!(up[0].bid.is_nan() && (up[0].change_pct - 1.0).abs() < 1e-9);
        let top = upbit_top(
            &json!([{"market": "USDT-BTC", "orderbook_units": [{"bid_price": 59990, "ask_price": 60010, "bid_size": 1, "ask_size": 1}]}]),
        );
        assert_eq!(top["USDT-BTC"], (59990.0, 60010.0));
        let lb = parse(
            "lbank",
            &json!({"result": "true", "data": [{"symbol": "eth_usdt", "ticker": {"latest": 2000, "turnover": 5e6, "change": 1.5}}]}),
        );
        assert_eq!((lb[0].symbol.as_str(), lb[0].volume), ("ETHUSDT", 5e6));
        assert_eq!(
            lbank_top(
                &json!({"data": [{"symbol": "eth_usdt", "bidPrice": "1999", "askPrice": "2001"}]})
            )["eth_usdt"],
            (1999.0, 2001.0)
        );
        assert_eq!(
            lbank_top(&json!({"data": {"symbol": "ETH_USDT", "bid_price": 1, "ask_price": 2}}))
                .len(),
            1
        );
        let bn = parse(
            "binance",
            &json!([{"symbol": "BTCUSDT", "lastPrice": "1", "bidPrice": "0.99", "askPrice": "1.01", "quoteVolume": "100", "priceChangePercent": "2"}]),
        );
        assert!((bn[0].spread_pct() - 2.0).abs() < 1e-9);
        let gt = parse(
            "gate",
            &json!([{"currency_pair": "BTC_USDT", "last": "1", "highest_bid": "0.9", "lowest_ask": "1.1", "quote_volume": "5", "change_percentage": "1"}]),
        );
        assert_eq!(gt[0].symbol, "BTCUSDT");
        let ok = parse(
            "okx",
            &json!({"data": [{"instId": "BTC-USDT", "last": "110", "sodUtc0": "100", "bidPx": "109", "askPx": "111", "volCcy24h": "1000"}]}),
        );
        assert!((ok[0].change_pct - 10.0).abs() < 1e-9);
        let by = parse(
            "bybit",
            &json!({"result": {"list": [{"symbol": "BTCUSDT", "lastPrice": "1", "bid1Price": "1", "ask1Price": "1.1", "turnover24h": "9", "price24hPcnt": "0.05"}]}}),
        );
        assert!((by[0].change_pct - 5.0).abs() < 1e-9);
    }

    fn t(ex: &str, sym: &str, bid: f64, ask: f64, vol: f64) -> Ticker {
        let (b, q) = split(sym).unwrap();
        Ticker {
            exchange: ex.into(),
            symbol: sym.into(),
            base: b,
            quote: q,
            last: bid,
            bid,
            ask,
            volume: vol,
            change_pct: 0.0,
            perp: false,
            funding8h: f64::NAN,
            venue: sym.into(),
        }
    }

    #[test]
    fn perp_summaries() {
        let n = Value::Null;
        let b = parse_perp(
            "binance",
            &json!([{"symbol": "BTCUSDT", "lastPrice": "100", "quoteVolume": "5e6", "priceChangePercent": "1"}]),
            &json!([{"symbol": "BTCUSDT", "bidPrice": "99.9", "askPrice": "100.1"}]),
            &json!([{"symbol": "BTCUSDT", "lastFundingRate": "0.0001"}]),
        );
        assert_eq!(
            (b[0].symbol.as_str(), b[0].perp, b[0].bid, b[0].funding8h),
            ("BTCUSDT", true, 99.9, 0.0001)
        );
        let o = parse_perp(
            "okx",
            &json!({"data": [{"instId": "ETH-USDT-SWAP", "last": "2000", "sodUtc0": "1000", "bidPx": "1999", "askPx": "2001", "volCcy24h": "10"}]}),
            &n,
            &n,
        );
        assert_eq!(
            (o[0].symbol.as_str(), o[0].volume, o[0].venue.as_str()),
            ("ETHUSDT", 20_000.0, "ETH-USDT-SWAP")
        );
        let k = parse_perp(
            "kucoin",
            &json!({"data": [{"symbol": "XBTUSDTM", "lastTradePrice": 60000, "turnoverOf24h": 1e7, "fundingFeeRate": 0.0001}]}),
            &n,
            &n,
        );
        assert_eq!((k[0].symbol.as_str(), k[0].bid.is_nan()), ("BTCUSDT", true));
        let h = parse_perp(
            "hyperliquid",
            &json!([{"universe": [{"name": "SOL"}]}, [{"midPx": "150", "prevDayPx": "100", "dayNtlVlm": "1e8",
            "funding": "0.00001", "impactPxs": ["149.9", "150.1"]}]]),
            &n,
            &n,
        );
        assert_eq!(
            (
                h[0].symbol.as_str(),
                h[0].venue.as_str(),
                h[0].ask,
                h[0].funding8h
            ),
            ("SOLUSDC", "SOL", 150.1, 0.00008)
        );
        let g = parse_perp(
            "gate",
            &json!([{"contract": "BTC_USDT", "last": "1", "highest_bid": "0.9", "lowest_ask": "1.1", "volume_24h_quote": "7", "funding_rate": "0.0002"}]),
            &n,
            &n,
        );
        assert_eq!(g[0].symbol, "BTCUSDT");
        let x = parse_perp(
            "bingx",
            &json!({"data": [{"symbol": "BTC-USDT", "lastPrice": "1", "bidPrice": "1", "askPrice": "1.1", "quoteVolume": "3"}]}),
            &n,
            &json!({"data": [{"symbol": "BTC-USDT", "lastFundingRate": "0.0003"}]}),
        );
        assert_eq!((x[0].symbol.as_str(), x[0].funding8h), ("BTCUSDT", 0.0003));
        let ks = parse(
            "kucoin",
            &json!({"data": {"ticker": [{"symbol": "ETH-USDT", "buy": "1", "sell": "1.1", "last": "1", "volValue": "9", "changeRate": "0.01"}]}}),
        );
        assert_eq!(
            (ks[0].symbol.as_str(), ks[0].ask, ks[0].perp),
            ("ETHUSDT", 1.1, false)
        );
        let bs = parse(
            "bingx",
            &json!({"data": [{"symbol": "ETH-USDT", "lastPrice": "1", "bidPrice": "1", "askPrice": "1.1", "quoteVolume": "9", "priceChangePercent": "2.5%"}]}),
        );
        assert_eq!(bs[0].change_pct, 2.5);
    }

    fn perp(ex: &str, sym: &str, bid: f64, ask: f64, vol: f64) -> Ticker {
        Ticker {
            perp: true,
            ..t(ex, sym, bid, ask, vol)
        }
    }

    #[test]
    fn lead_lag_and_perp_arb() {
        let ts = vec![
            t("binance", "XUSDT", 10.0, 10.001, 1e8),
            t("gate", "XUSDT", 10.0, 10.005, 1e6), // спред 0.05% ≤ 0.15 — подходит
            t("mexc", "XUSDT", 10.0, 10.1, 1e6),   // спред 1% — шире движения
            t("bybit", "XUSDT", 10.0, 10.001, 1e9), // оборот больше, чем у ведущей
            perp("okx", "XUSDT", 10.0, 10.001, 1e6), // перп — у ведущей перпа нет
        ];
        let v = lead_lag(&ts, &|_| params());
        let c = v["candidates"].as_array().unwrap();
        let verdict = |ex: &str| {
            c.iter()
                .find(|x| x["exchange"] == format!("binance→{ex}"))
                .map(|x| x["verdict"].clone())
        };
        assert_eq!(verdict("gate"), Some(json!("подходит")), "{v}");
        assert_eq!(verdict("mexc"), Some(json!("не подходит")));
        assert_eq!(verdict("bybit"), Some(json!("не подходит")));
        assert_eq!(verdict("okx"), None);
        assert_eq!(
            c[0]["metrics"]["symbols"],
            json!({"binance": ["XUSDT"], "gate": ["XUSDT"]})
        );
        // перпы: лонг на дешёвой, шорт на дорогой; 1% − 4 × 0.05 = 0.8%
        let ps = vec![
            perp("okx", "YUSDT", 9.99, 10.0, 1e6),
            perp("gate", "YUSDT", 10.1, 10.11, 1e6),
            t("binance", "YUSDT", 20.0, 20.1, 1e6),
        ];
        let v = perp_arb(&ps, &|_| params());
        let c = &v["candidates"][0];
        assert_eq!(
            (c["exchange"].clone(), c["verdict"].clone()),
            (json!("okx→gate"), json!("подходит")),
            "{v}"
        );
        assert_eq!(c["metrics"]["market"], "perp");
        assert!(
            (c["metrics"]["netPct"].as_f64().unwrap()
                - (1.0
                    - 2.0 * fee("okx", "perpTakerFeePct")
                    - 2.0 * fee("gate", "perpTakerFeePct")))
            .abs()
                < 1e-9
        );
    }

    #[test]
    fn stat_arb_correlation_and_backtest() {
        // A и B ходят вместе; у B — отклонения, которые возвращаются (есть сделки)
        let a: Vec<f64> = (0..300)
            .map(|i| 100.0 + (i as f64 / 7.0).sin() * 2.0 + i as f64 * 0.01)
            .collect();
        let b: Vec<f64> = a
            .iter()
            .enumerate()
            .map(|(i, x)| x * 0.5 * if i % 50 == 25 { 1.01 } else { 1.0 })
            .collect();
        assert!(correlation(&returns(&a), &returns(&b)) > 0.5);
        let (_, trades, bars) = pair_backtest(&a, &b, 0.0);
        assert!(trades >= 2 && bars == 300, "{trades}");
        let ts = vec![
            t("binance", "AUSDT", 1.0, 1.001, 5e6),
            t("binance", "BUSDT", 1.0, 1.001, 4e6),
            t("binance", "CUSDT", 1.0, 1.001, 10.0),
        ];
        let pools = statarb_pool(&ts, &|_| params());
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].len(), 2, "мало оборота — не в паре");
        let closes = HashMap::from([
            ("binance:AUSDT".to_string(), a),
            ("binance:BUSDT".to_string(), b),
        ]);
        let v = stat_arb(&pools, &closes, &|_| params());
        let c = &v["candidates"][0];
        assert_eq!(c["symbol"], "AUSDT/BUSDT");
        assert!(
            c["metrics"]["correlation"].as_f64().unwrap() > 0.5
                && c["backtest"]["trades"].as_u64().unwrap() >= 2,
            "{v}"
        );
    }

    #[test]
    fn candle_checks_and_filters() {
        // 3 часа: последний час живой, второй — тихий
        let mut vols = vec![500.0; 60];
        vols.extend(vec![10.0; 60]);
        vols.extend(vec![2000.0; 60]);
        let c = Candles {
            closes: vec![1.0; 180],
            vols,
        };
        let p = params();
        let mut why = Vec::new();
        let bad = illiquid(&c, &p, &mut why).unwrap();
        assert!(bad.contains("в самый тихий час из 3"), "{bad}");
        // фильтр биржи для стратегии: порог тихого часа 0 — проходит
        let settings: Map<String, Value> = serde_json::from_value(json!({"discoveryFilters": r#"{"gate": {"spread-capture": {"discoveryMinHourlyVolume": 0, "discoveryMinVolume1h": 0}}}"#})).unwrap();
        let pg = Params::at(&settings, "gate", "spread-capture");
        assert_eq!(
            (
                pg.min_hourly_volume,
                Params::at(&settings, "mexc", "spread-capture").min_hourly_volume
            ),
            (0.0, 10_000.0)
        );
        let mut why = Vec::new();
        assert_eq!(illiquid(&c, &pg, &mut why), None);
        let dead = Candles {
            closes: vec![1.0; 60],
            vols: [vec![0.0; 40], vec![1e6; 20]].concat(),
        };
        assert!(
            illiquid(&dead, &pg, &mut Vec::new())
                .unwrap()
                .contains("минут без оборота 67%")
        );
    }

    #[test]
    fn spread_capture_verdicts() {
        let ts = vec![
            t("upbit", "AAAUSDT", 1.0, 1.01, 200_000.0), // спред ~1% — кандидат, стакан хороший
            t("upbit", "BBBUSDT", 1.0, 1.01, 200_000.0), // стакан тонкий
            t("upbit", "CCCUSDT", 1.0, 1.0001, 200_000.0), // узкий спред — не кандидат
            t("upbit", "DDDUSDT", 1.0, 1.01, 100.0),     // мало оборота
            t("upbit", "USDCUSDT", 1.0, 1.01, 200_000.0), // стейблкоин
            t("lbank", "EEEUSDT", f64::NAN, f64::NAN, 200_000.0), // без цен — не оценить
            t("lbank", "FFFUSDT", 1.0, 1.01, 200_000.0), // стакан не получен
        ];
        let deep: Vec<(f64, f64)> = (0..10)
            .map(|i| (1.01 + i as f64 * 0.0005, 1000.0))
            .collect();
        let bids: Vec<(f64, f64)> = (0..10).map(|i| (1.0 - i as f64 * 0.0005, 1000.0)).collect();
        let books = HashMap::from([
            ("upbit:AAAUSDT".to_string(), Ok((bids.clone(), deep))),
            ("upbit:BBBUSDT".to_string(), Ok((bids, vec![(1.01, 1.0)]))),
            ("lbank:FFFUSDT".to_string(), Err("HTTP 500".to_string())),
        ]);
        let v = spread_capture(&ts, &books, &HashMap::new(), &|_| params());
        let c = v["candidates"].as_array().unwrap();
        let by = |s: &str| {
            c.iter()
                .find(|x| x["symbol"] == s)
                .map(|x| x["verdict"].as_str().unwrap().to_owned())
        };
        assert_eq!(by("AAAUSDT").as_deref(), Some("подходит"), "{v}");
        assert_eq!(by("BBBUSDT").as_deref(), Some("не подходит"));
        assert_eq!(by("FFFUSDT").as_deref(), Some("под вопросом"));
        assert!(
            by("CCCUSDT").is_none()
                && by("DDDUSDT").is_none()
                && by("USDCUSDT").is_none()
                && by("EEEUSDT").is_none()
        );
        assert_eq!(c[0]["symbol"], "AAAUSDT", "подходящие — сверху");
        assert_eq!(c[0]["metrics"]["symbols"]["upbit"], json!(["AAAUSDT"]));
    }

    #[test]
    fn impact() {
        let asks = vec![(10.0, 1.0), (11.0, 10.0)];
        assert_eq!(impact_pct(&asks, 5.0), 0.0);
        assert!(
            (impact_pct(&asks, 21.0) - ((21.0 / (1.0 + 1.0)) / 10.0 - 1.0) * 100.0).abs() < 1e-9
        );
        assert!(impact_pct(&asks, 1000.0).is_infinite());
    }

    /// Полный пересчёт против макетов: Upbit — 120 рынков, лучшие цены пачками по 50 (3 запроса), стакан кандидата;
    /// LBank — bookTicker всех пар.
    #[tokio::test]
    async fn run_against_mock_upbit_and_lbank() {
        use axum::{Json, Router, extract::RawQuery, routing::get};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let batches = Arc::new(AtomicUsize::new(0));
        let b2 = batches.clone();
        let all: Vec<Value> = (0..120)
            .map(|i| json!({"market": format!("USDT-C{i}"), "trade_price": 1.0, "acc_trade_price_24h": 200000.0, "signed_change_rate": 0.0}))
            .collect();
        let up = Router::new()
            .route("/v1/ticker/all", get(move || async move { Json(json!(all)) }))
            .route("/v1/orderbook", get(move |RawQuery(q): RawQuery| async move {
                let q = q.unwrap_or_default();
                let ms: Vec<String> = q.trim_start_matches("markets=").split(',').map(str::to_owned).collect();
                if ms.len() > 1 {
                    b2.fetch_add(1, Ordering::SeqCst);
                }
                // спред 1%, глубокий стакан
                Json(json!(ms.iter().map(|m| json!({"market": m, "orderbook_units": (0..10).map(|k| json!({
                    "bid_price": 1.0 - k as f64 * 0.0005, "ask_price": 1.01 + k as f64 * 0.0005, "bid_size": 1000.0, "ask_size": 1000.0})).collect::<Vec<_>>()})).collect::<Vec<_>>()))
            }));
        let lb = Router::new()
            .route("/v2/ticker/24hr.do", get(|| async { Json(json!({"result": "true", "data": [
                {"symbol": "c1_usdt", "ticker": {"latest": 1.05, "turnover": 300000.0, "change": 0}},
                {"symbol": "zz_usdt", "ticker": {"latest": 5.0, "turnover": 300000.0, "change": 0}}]})) }))
            .route("/v2/supplement/ticker/bookTicker.do", get(|| async { Json(json!({"result": "true", "data": [
                {"symbol": "c1_usdt", "bidPrice": "1.05", "askPrice": "1.051"}, {"symbol": "zz_usdt", "bidPrice": "5", "askPrice": "5.001"}]})) }));
        let mut src = Sources::default();
        for (ex, r) in [("upbit", up), ("lbank", lb)] {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            src.urls
                .insert(ex.into(), format!("http://{}", l.local_addr().unwrap()));
            tokio::spawn(async move { axum::serve(l, r).await.unwrap() });
        }
        let (st, strategies) = run(&src, &["upbit".into(), "lbank".into()], &Map::new()).await;
        assert_eq!(
            batches.load(Ordering::SeqCst),
            3,
            "лучшие цены Upbit пачками по 50"
        );
        assert_eq!((st["upbit"].tickers, st["upbit"].with_prices), (120, 120));
        assert_eq!(st["lbank"].with_prices, 2);
        let sc = &strategies[0]["candidates"];
        assert_eq!(
            sc.as_array()
                .unwrap()
                .iter()
                .filter(|c| c["verdict"] == "подходит")
                .count(),
            8,
            "стакан проверен у 8 лучших"
        );
    }
}
