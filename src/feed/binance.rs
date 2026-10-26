//! Binance (спот и USDⓈ-M перпы): комбинированные потоки `/stream?streams=` — частичный стакан `depth20@100ms`
//! и лучшие цены `bookTicker`. Подписка — в адресе; номера обновлений общие, устаревшее не применяется.

use super::{Dialect, Parsed, Update};
use serde::Deserialize;

/// Адрес по умолчанию.
pub fn default_url(perp: bool, testnet: bool) -> &'static str {
    match (perp, testnet) {
        (false, false) => "wss://stream.binance.com:9443/stream",
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
            Ok(vec![Update::Snapshot {
                sym,
                bids: levels(&d.bids),
                asks: levels(&d.asks),
                id: d.id,
            }])
        } else if kind == "bookTicker" {
            let t: Ticker = serde_json::from_str(env.data.get())
                .map_err(|e| format!("Binance bookTicker: {e}"))?;
            let p = |s: &str| {
                s.parse::<f64>()
                    .map_err(|e| format!("Binance bookTicker: {e}"))
            };
            Ok(vec![Update::Best {
                sym,
                bid: (p(t.b)?, p(t.bq)?),
                ask: (p(t.a)?, p(t.aq)?),
                id: t.u,
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
            "wss://x/stream?streams=btcusdt@depth20@100ms/btcusdt@bookTicker"
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
        let task = tokio::spawn(run(m.clone(), "binance".into(), d));
        let b = testkit::wait_ready(&m, "binance", "BTCUSDT").await;
        assert_eq!(b.mid(), Some(100.5));
        task.abort();
    }
}
