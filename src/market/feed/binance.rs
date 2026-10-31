//! Binance (спот и USDⓈ-M перпы): комбинированные потоки `/stream?streams=` — частичный стакан `depth20@100ms`,
//! лучшие цены `bookTicker` и сделки `aggTrade` (время сделки `T`; у перпов и в стакане, и в лучших ценах есть `T`). Подписка — в адресе; номера обновлений общие, устаревшее не применяется.

use super::{Dialect, Parsed, Update};
use serde::Deserialize;

/// Адрес по умолчанию.
pub fn default_url(perp: bool, testnet: bool) -> &'static str {
    match (perp, testnet) {
        (false, false) => "wss://stream.binance.com:443/stream",
        (false, true) => "wss://stream.testnet.binance.vision/stream",
        (true, false) => "wss://fstream.binance.com/stream",
        (true, true) => "wss://fstream.binancefuture.com/stream",
    }
}

/// Адрес с подпиской на потоки символов.
pub fn stream_url(base: &str, symbols: &[String], book_ticker: bool) -> String {
    let mut streams = Vec::new();
    for s in symbols {
        let l = s.to_lowercase();
        streams.push(format!("{l}@depth20@100ms"));
        if book_ticker {
            streams.push(format!("{l}@bookTicker"));
        }
        streams.push(format!("{l}@aggTrade"));
    }
    format!(
        "{}?streams={}",
        base.trim_end_matches('/'),
        streams.join("/")
    )
}

/// Диалект Binance: адрес уже с подпиской.
pub struct Binance {
    pub url: String,
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    stream: &'a str,
    #[serde(borrow)]
    data: &'a serde_json::value::RawValue,
}

/// Частичный стакан: у спота `lastUpdateId`, у перпов `u`; уровни — строки ["цена", "объём"].
#[derive(Deserialize)]
struct Depth<'a> {
    #[serde(rename = "lastUpdateId", alias = "u", default)]
    id: u64,
    #[serde(borrow, alias = "b")]
    bids: Vec<[&'a str; 2]>,
    #[serde(borrow, alias = "a")]
    asks: Vec<[&'a str; 2]>,
    /// Время биржи (только перпы), мс.
    #[serde(rename = "T", default)]
    t: u64,
}

/// Сделка: m — покупатель мейкер (агрессор — продавец).
#[derive(Deserialize)]
struct AggTrade<'a> {
    #[serde(borrow)]
    p: &'a str,
    #[serde(borrow)]
    q: &'a str,
    m: bool,
    #[serde(rename = "T", default)]
    t: u64,
}

#[derive(Deserialize)]
struct Ticker<'a> {
    #[serde(default)]
    u: u64,
    #[serde(borrow)]
    b: &'a str,
    #[serde(borrow, rename = "B")]
    bq: &'a str,
    #[serde(borrow)]
    a: &'a str,
    #[serde(borrow, rename = "A")]
    aq: &'a str,
    #[serde(rename = "T", default)]
    t: u64,
}

/// Время биржи, если есть (0 — нет).
fn time(t: u64) -> Vec<Update> {
    if t > 0 { vec![Update::Time(t)] } else { vec![] }
}

fn levels(raw: &[[&str; 2]]) -> Vec<(f64, f64)> {
    raw.iter()
        .filter_map(|l| Some((l[0].parse().ok()?, l[1].parse().ok()?)))
        .collect()
}

impl Dialect for Binance {
    fn url(&self) -> String {
        self.url.clone()
    }

    fn parse(&self, text: &str) -> Parsed {
        let Ok(env) = serde_json::from_str::<Envelope>(text) else {
            return Ok(vec![]);
        }; // ответы на команды
        let Some((sym, kind)) = env.stream.split_once('@') else {
            return Ok(vec![]);
        };
        let sym = sym.to_uppercase();
        if kind.starts_with("depth") {
            let d: Depth =
                serde_json::from_str(env.data.get()).map_err(|e| format!("Binance depth: {e}"))?;
            let mut out = time(d.t);
            out.push(Update::Snapshot {
                sym,
                bids: levels(&d.bids),
                asks: levels(&d.asks),
                id: d.id,
            });
            Ok(out)
        } else if kind == "bookTicker" {
            let t: Ticker = serde_json::from_str(env.data.get())
                .map_err(|e| format!("Binance bookTicker: {e}"))?;
            let p = |s: &str| {
                s.parse::<f64>()
                    .map_err(|e| format!("Binance bookTicker: {e}"))
            };
            let mut out = time(t.t);
            out.push(Update::Best {
                sym,
                bid: (p(t.b)?, p(t.bq)?),
                ask: (p(t.a)?, p(t.aq)?),
                id: t.u,
            });
            Ok(out)
        } else if kind == "aggTrade" {
            let t: AggTrade = serde_json::from_str(env.data.get())
                .map_err(|e| format!("Binance aggTrade: {e}"))?;
            let p = |s: &str| {
                s.parse::<f64>()
                    .map_err(|e| format!("Binance aggTrade: {e}"))
            };
            Ok(vec![Update::Trade {
                sym,
                px: p(t.p)?,
                qty: p(t.q)?,
                buy: !t.m,
                ts: t.t,
            }])
        } else {
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{apply, run, testkit};
    use super::*;
    use crate::market::Market;
    use std::sync::Arc;

    #[test]
    fn url_and_parse() {
        let u = stream_url("wss://x/stream", &["BTCUSDT".into()], true);
        assert_eq!(
            u,
            "wss://x/stream?streams=btcusdt@depth20@100ms/btcusdt@bookTicker/btcusdt@aggTrade"
        );
        let d = Binance { url: u };
        let m = Market::default();
        for t in [
            r#"{"stream":"btcusdt@depth20@100ms","data":{"lastUpdateId":5,"bids":[["100.0","2"]],"asks":[["101.0","3"]]}}"#,
            r#"{"stream":"btcusdt@bookTicker","data":{"u":6,"s":"BTCUSDT","b":"100.5","B":"1","a":"100.9","A":"4"}}"#,
            r#"{"stream":"ethusdt@depth20@100ms","data":{"e":"depthUpdate","u":9,"b":[["10","1"]],"a":[["11","1"]]}}"#,
        ] {
            for u in d.parse(t).unwrap() {
                apply(&m, "binance", u);
            }
        }
        assert_eq!(
            d.parse(r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","s":"BTCUSDT","p":"100.7","q":"0.5","m":true,"T":1700000000001}}"#),
            Ok(vec![Update::Trade { sym: "BTCUSDT".into(), px: 100.7, qty: 0.5, buy: false, ts: 1_700_000_000_001 }])
        );
        assert_eq!(
            d.parse(r#"{"stream":"btcusdt@bookTicker","data":{"u":7,"T":1700000000002,"b":"1","B":"1","a":"2","A":"1"}}"#).unwrap()[0],
            Update::Time(1_700_000_000_002),
            "перпы: время биржи перед лучшими ценами"
        );
        let b = m.book("binance", "BTCUSDT").unwrap();
        assert_eq!(b.best_bid(), Some((100.5, 1.0)));
        assert_eq!(b.best_ask(), Some((100.9, 4.0)));
        assert!(
            m.book("binance", "ETHUSDT").unwrap().is_ready(),
            "перпы: поля b/a и номер u"
        );
        assert!(d.parse(r#"{"result":null,"id":1}"#).unwrap().is_empty());
    }

    /// Живое соединение: снимок доходит, после закрытия сервером — переподключение.
    #[tokio::test]
    async fn connects_and_reconnects() {
        let snap = r#"{"stream":"btcusdt@depth20@100ms","data":{"lastUpdateId":10,"bids":[["100","1"]],"asks":[["101","1"]]}}"#.to_owned();
        let (addr, _) = testkit::server(vec![snap.clone()]).await;
        let m = Arc::new(Market::default());
        let d = Arc::new(Binance {
            url: stream_url(&format!("{addr}/stream"), &["BTCUSDT".into()], true),
        });
        let task = tokio::spawn(run(m.clone(), "binance".into(), d, Default::default()));
        let b = testkit::wait_ready(&m, "binance", "BTCUSDT").await;
        assert_eq!(b.mid(), Some(100.5));
        task.abort();
    }

    /// Ошибки биржи в потоке ниже порога wsMaxParseErrors соединение не рвут.
    #[tokio::test]
    async fn parse_errors_below_limit_keep_connection() {
        let err = r#"{"channel":"error","data":"x"}"#.to_owned();
        let snap = r#"{"channel":"l2Book","data":{"coin":"BTC","time":5,"levels":[[{"px":"100","sz":"1","n":1}],[{"px":"101","sz":"1","n":1}]]}}"#.to_owned();
        let (addr, _) = testkit::server(vec![err.clone(), err.clone(), err, snap]).await;
        let m = Arc::new(Market::default());
        let d = Arc::new(crate::market::feed::hyperliquid::Hyperliquid::new(
            addr,
            &["BTCUSDC".into()],
        ));
        let task = tokio::spawn(run(m.clone(), "hyperliquid".into(), d, Default::default()));
        testkit::wait_ready(&m, "hyperliquid", "BTCUSDC").await;
        let f = m.feed("hyperliquid").unwrap();
        assert_eq!((f.parse_errors, f.reconnects), (3, 0), "{f:?}");
        task.abort();
    }

    /// Устойчивость: тишина — переподключение; сервер исчез —
    /// после feedMaxFailures неудач подряд источник сдаётся.
    #[tokio::test]
    async fn silence_parse_errors_and_give_up() {
        let snap = r#"{"stream":"btcusdt@depth20@100ms","data":{"lastUpdateId":10,"bids":[["100","1"]],"asks":[["101","1"]]}}"#.to_owned();
        let (addr, _) =
            testkit::server(vec![snap, "мусор".into(), "{не json".into(), "ещё".into()]).await;
        let m = Arc::new(Market::default());
        let d = Arc::new(Binance {
            url: stream_url(&format!("{addr}/stream"), &["BTCUSDT".into()], true),
        });
        let tune = super::super::Tune {
            stale: std::time::Duration::from_millis(400),
            reconnect_base: std::time::Duration::from_millis(50),
            max_failures: 2,
            ..Default::default()
        };
        let task = tokio::spawn(run(m.clone(), "binance".into(), d, tune));
        testkit::wait_ready(&m, "binance", "BTCUSDT").await;
        let until = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let f = m.feed("binance").unwrap();
            if f.gave_up {
                assert_eq!(f.silence_reconnects, 1, "соединение рвёт тишина: {f:?}");
                assert!(f.failures >= 2 && f.down_since.is_some(), "{f:?}");
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "источник не сдался: {f:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        task.abort();
    }
}
