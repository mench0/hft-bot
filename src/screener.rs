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

use crate::paper::split;
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
pub const SUPPORTED: &[&str] = &["binance", "mexc", "bybit", "okx", "gate", "upbit", "lbank"];

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
        _ => return None,
    })
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
        "lbank" => (lv(&v["data"]["bids"]), lv(&v["data"]["asks"])),
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
    http: reqwest::Client,
}

impl Default for Sources {
    fn default() -> Self {
        Sources {
            urls: SUPPORTED
                .iter()
                .filter_map(|e| Some((e.to_string(), base_url(e)?.to_owned())))
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
            _ => return Err(format!("{ex}: сводок в Rust-скринере нет")),
        };
        crate::metrics::out(ex, "discovery");
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
            crate::metrics::out(&t.exchange, "discovery");
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
    pub arb_min_profit: f64,
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
            arb_min_profit: f("spotArbMinProfitPercent", 0.05),
        }
    }
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
    crate::data::catalog()["exchanges"]
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
    p: &Params,
) -> Value {
    let mut out = Vec::new();
    for t in tickers {
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
        let verdict = match books.get(&key) {
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
    json!({"id": crate::spread::ID, "title": "Сбор широкого спреда", "executable": true,
        "description": "Лимитная покупка по биду и продажа по аску; нужны широкий спред и ровная торговля.",
        "criteria": [format!("спред − 2 maker ≥ {}%", p.spread_min_edge), format!("спред ≤ {}%", p.spread_max),
            format!("оборот {}–{}", money(p.spread_min_volume), money(p.spread_max_volume))],
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

/// Спот – спот на разных биржах: по каждой монете дешевле всего купить и дороже всего продать.
pub fn cross_arb(tickers: &[Ticker], p: &Params) -> Value {
    let mut by: BTreeMap<String, Vec<&Ticker>> = BTreeMap::new();
    for t in tickers.iter().filter(|t| {
        excluded(t, p).is_none() && t.bid > 0.0 && t.ask > 0.0 && t.volume >= p.arb_min_volume
    }) {
        by.entry(t.symbol.clone()).or_default().push(t);
    }
    let mut out = Vec::new();
    for (sym, ts) in by {
        let (Some(buy), Some(sell)) = (
            ts.iter().min_by(|a, b| a.ask.total_cmp(&b.ask)),
            ts.iter().max_by(|a, b| a.bid.total_cmp(&b.bid)),
        ) else {
            continue;
        };
        if buy.exchange == sell.exchange || sell.bid <= buy.ask {
            continue;
        }
        let gross = (sell.bid - buy.ask) / buy.ask * 100.0;
        if gross > p.arb_max_gross {
            continue; // скорее ошибка данных или разные монеты под одним тикером
        }
        let fees = fee(&buy.exchange, "takerFeePct") + fee(&sell.exchange, "takerFeePct");
        let net = gross - fees;
        if net <= 0.0 {
            continue;
        }
        let ok = net >= p.arb_min_profit;
        let mut m = pick(
            "spot",
            &[(&buy.exchange, &buy.symbol), (&sell.exchange, &sell.symbol)],
        );
        m.insert("grossPct".into(), json!(gross));
        m.insert("netPct".into(), json!(net));
        m.insert("buyAsk".into(), json!(buy.ask));
        m.insert("sellBid".into(), json!(sell.bid));
        let why = vec![
            format!(
                "{} {} → {} {}",
                buy.exchange, buy.ask, sell.exchange, sell.bid
            ),
            format!(
                "разница {}, после двух комиссий тейкера {}",
                pct(gross),
                pct(net)
            ),
            format!("оборот {} / {}", money(buy.volume), money(sell.volume)),
            "без запаса монеты на бирже продажи — перевод (комиссия сети не учтена)".into(),
        ];
        let verdict = if ok {
            "подходит"
        } else {
            "не подходит"
        };
        out.push(candidate(&format!("{}→{}", buy.exchange, sell.exchange), &sym, verdict, net, why, Value::Object(m),
            Some(json!({"text": format!("{} с круга", pct(net)), "note": "разница цен минус две комиссии тейкера"}))));
    }
    sort(&mut out);
    json!({"id": "cross-exchange-arb", "title": "Спот – спот на разных биржах", "executable": true,
        "description": "Купить там, где дешевле, продать там, где дороже: сразу при запасе монеты или после перевода.",
        "criteria": [format!("после комиссий ≥ {}%", p.arb_min_profit), format!("разница ≤ {}%", p.arb_max_gross), format!("оборот ≥ {}", money(p.arb_min_volume))],
        "candidates": out})
}

fn sort(v: &mut [Value]) {
    v.sort_by(|a, b| num(&b["score"]).total_cmp(&num(&a["score"])));
}

/// Итог пересчёта по бирже.
#[derive(Clone, Debug, Default)]
pub struct ExStatus {
    pub ok: bool,
    pub tickers: usize,
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

/// Пересчёт: сводки всех бирж параллельно, стаканы лучших кандидатов сбора спреда, профили.
pub async fn run(
    src: &Sources,
    exchanges: &[String],
    p: &Params,
) -> (BTreeMap<String, ExStatus>, Vec<Value>) {
    let jobs = exchanges.iter().map(|ex| async move {
        let t0 = Instant::now();
        (
            ex.clone(),
            src.tickers(ex).await,
            t0.elapsed().as_millis() as u64,
        )
    });
    let mut status = BTreeMap::new();
    let mut all = Vec::new();
    for (ex, r, ms) in futures_util::future::join_all(jobs).await {
        let st = match r {
            Ok(ts) => {
                let s = ExStatus {
                    ok: true,
                    tickers: ts.len(),
                    with_prices: ts.iter().filter(|t| t.bid > 0.0).count(),
                    wait_ms: ms,
                    ..Default::default()
                };
                all.extend(ts);
                s
            }
            Err(e) => ExStatus {
                error: e,
                wait_ms: ms,
                ..Default::default()
            },
        };
        status.insert(ex, st);
    }
    // стаканы: лучшие по спреду кандидаты каждой биржи
    let pre = spread_capture(&all, &HashMap::new(), p);
    let mut per: HashMap<String, usize> = HashMap::new();
    let wanted: Vec<Ticker> = pre["candidates"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| {
            let ex = c["exchange"].as_str()?;
            let n = per.entry(ex.to_owned()).or_default();
            *n += 1;
            (*n <= p.books_per_exchange)
                .then(|| {
                    all.iter()
                        .find(|t| t.exchange == ex && t.symbol == c["symbol"])
                        .cloned()
                })
                .flatten()
        })
        .collect();
    let books: HashMap<String, Result<Levels, String>> = futures_util::future::join_all(
        wanted
            .iter()
            .map(|t| async move { (format!("{}:{}", t.exchange, t.symbol), src.book(t).await) }),
    )
    .await
    .into_iter()
    .collect();
    for (k, r) in &books {
        if let (Err(e), Some((ex, _))) = (r, k.split_once(':'))
            && let Some(s) = status.get_mut(ex)
        {
            s.depth_error = e.clone();
        }
    }
    (
        status,
        vec![spread_capture(&all, &books, p), cross_arb(&all, p)],
    )
}

impl Screener {
    /// Ответ /discovery в формате Java.
    pub fn json(&self, chosen: &[String], refresh_sec: u64) -> Value {
        let exs: Map<String, Value> = self
            .exchanges
            .iter()
            .map(|(ex, s)| {
                (ex.clone(), json!({"network": "основная", "ok": s.ok, "tickers": s.tickers, "withBidAsk": s.with_prices,
                    "waitMs": s.wait_ms, "error": if s.error.is_empty() { Value::Null } else { json!(s.error) },
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
            "exchanges": exs, "strategies": self.strategies,
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
        }
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
        let v = spread_capture(&ts, &books, &params());
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
    fn cross_arb_finds_cheapest_and_dearest() {
        let ts = vec![
            t("upbit", "XUSDT", 9.9, 10.0, 1e6),
            t("lbank", "XUSDT", 10.2, 10.21, 1e6),
            t("binance", "XUSDT", 10.05, 10.06, 1e6),
            t("binance", "YUSDT", 1.0, 1.001, 1e6),
            t("lbank", "YUSDT", 2.0, 2.01, 1e6), // 100% — ошибка данных
        ];
        let v = cross_arb(&ts, &params());
        let c = v["candidates"].as_array().unwrap();
        assert_eq!(c.len(), 1, "{v}");
        assert_eq!(c[0]["exchange"], "upbit→lbank");
        assert_eq!(c[0]["verdict"], "подходит");
        assert_eq!(
            c[0]["metrics"]["symbols"],
            json!({"lbank": ["XUSDT"], "upbit": ["XUSDT"]})
        );
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
        let (st, strategies) = run(&src, &["upbit".into(), "lbank".into()], &params()).await;
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
        // C1: на Upbit аск 1.01, на LBank бид 1.05 — спот – спот
        let x = &strategies[1]["candidates"][0];
        assert_eq!(
            (x["exchange"].as_str(), x["symbol"].as_str()),
            (Some("upbit→lbank"), Some("C1USDT")),
            "{x}"
        );
    }
}
