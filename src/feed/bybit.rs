//! Bybit v5 (спот и линейные перпы): `orderbook.50` — снимок, затем изменения; `orderbook.1` — лучшие цены.
//! Номер `seq` общий для всех глубин. Пинг `{"op":"ping"}` раз в 20 с.

use super::{Dialect, Parsed, Update, levels};
use serde_json::{Value, json};
use std::time::Duration;

/// Адрес по умолчанию.
pub fn default_url(perp: bool, testnet: bool) -> String {
    let host = if testnet {
        "wss://stream-testnet.bybit.com/v5/public/"
    } else {
        "wss://stream.bybit.com/v5/public/"
    };
    format!("{host}{}", if perp { "linear" } else { "spot" })
}

pub struct Bybit {
    url: String,
    topics: Vec<String>,
}

impl Bybit {
    pub fn new(url: String, symbols: &[String], bbo: bool) -> Bybit {
        let mut topics = Vec::new();
        for s in symbols {
            topics.push(format!("orderbook.50.{}", s.to_uppercase()));
            if bbo {
                topics.push(format!("orderbook.1.{}", s.to_uppercase()));
            }
        }
        Bybit { url, topics }
    }
}

impl Dialect for Bybit {
    fn url(&self) -> String {
        self.url.clone()
    }

    /// Не больше 10 тем в сообщении.
    fn subscribe(&self) -> Vec<String> {
        self.topics
            .chunks(10)
            .map(|c| json!({"op": "subscribe", "args": c}).to_string())
            .collect()
    }

    fn ping(&self) -> Option<(String, Duration)> {
        Some((r#"{"op":"ping"}"#.into(), Duration::from_secs(20)))
    }

    fn parse(&self, text: &str) -> Parsed {
        let v: Value = serde_json::from_str(text).map_err(|e| format!("Bybit: {e}"))?;
        if v.get("op").is_some() && v.get("data").is_none() {
            if v.get("success") == Some(&Value::Bool(false)) {
                return Err(format!("Bybit WS error: {}", v["ret_msg"]));
            }
            return Ok(vec![]); // ответ на подписку или pong
        }
        let topic = v["topic"].as_str().unwrap_or("");
        if !topic.starts_with("orderbook.") {
            return Ok(vec![]);
        }
        let d = &v["data"];
        let sym = d["s"].as_str().ok_or("Bybit: нет символа")?.to_owned();
        let id = d["seq"].as_u64().unwrap_or(0);
        let (bids, asks) = (levels(d.get("b")), levels(d.get("a")));
        if topic.starts_with("orderbook.1.") {
            return Ok(match (bids.first(), asks.first()) {
                (Some(b), Some(a)) => vec![Update::Best {
                    sym,
                    bid: *b,
                    ask: *a,
                    id,
                }],
                _ => vec![], // пустая сторона — не лучшие цены
            });
        }
        Ok(vec![if v["type"] == "snapshot" {
            Update::Snapshot {
                sym,
                bids,
                asks,
                id,
            }
        } else {
            Update::Delta {
                sym,
                bids,
                asks,
                id,
            }
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::super::{run, testkit};
    use super::*;
    use crate::market::Market;
    use std::sync::Arc;

    #[test]
    fn subscribe_and_parse() {
        let d = Bybit::new("wss://x".into(), &["BTCUSDT".into()], true);
        assert_eq!(
            d.subscribe(),
            vec![r#"{"op":"subscribe","args":["orderbook.50.BTCUSDT","orderbook.1.BTCUSDT"]}"#]
        );
        assert_eq!(
            d.parse(r#"{"success":true,"ret_msg":"","op":"subscribe"}"#),
            Ok(vec![])
        );
        assert!(
            d.parse(r#"{"success":false,"ret_msg":"bad","op":"subscribe"}"#)
                .is_err()
        );
        let s = d.parse(r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100","1"]],"a":[["101","2"]],"u":1,"seq":7}}"#).unwrap();
        assert_eq!(
            s,
            vec![Update::Snapshot {
                sym: "BTCUSDT".into(),
                bids: vec![(100.0, 1.0)],
                asks: vec![(101.0, 2.0)],
                id: 7
            }]
        );
        let b = d.parse(r#"{"topic":"orderbook.1.BTCUSDT","type":"delta","data":{"s":"BTCUSDT","b":[["100.5","1"]],"a":[["100.9","1"]],"seq":8}}"#).unwrap();
        assert!(matches!(b[0], Update::Best { id: 8, .. }));
    }

    #[tokio::test]
    async fn live_snapshot_then_delta() {
        let (addr, got) = testkit::server(vec![
            r#"{"success":true,"op":"subscribe"}"#.into(),
            r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","data":{"s":"BTCUSDT","b":[["100","1"],["99","1"]],"a":[["101","1"]],"seq":1}}"#.into(),
            r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","data":{"s":"BTCUSDT","b":[["100","0"]],"a":[["100.5","2"]],"seq":2}}"#.into(),
        ]).await;
        let m = Arc::new(Market::default());
        let task = tokio::spawn(run(
            m.clone(),
            "bybit".into(),
            Arc::new(Bybit::new(addr, &["BTCUSDT".into()], false)),
            Default::default(),
        ));
        testkit::wait_ready(&m, "bybit", "BTCUSDT").await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let b = m.book("bybit", "BTCUSDT").unwrap();
        assert_eq!(
            (b.best_bid(), b.best_ask()),
            (Some((99.0, 1.0)), Some((100.5, 2.0)))
        );
        assert!(
            got.lock().unwrap()[0].contains("orderbook.50.BTCUSDT"),
            "подписка отправлена"
        );
        task.abort();
    }
}
