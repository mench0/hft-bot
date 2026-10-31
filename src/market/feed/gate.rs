//! Gate v4 (спот): `spot.order_book` — снимки 20 уровней раз в 100 мс, `spot.book_ticker` — лучшие цены,
//! `spot.trades` — сделки; время биржи — `t` / `create_time_ms`;
//! пара BTC_USDT, номер обновления общий (`lastUpdateId` / `u`). Пинг `spot.ping` раз в 20 с.

use super::{Dialect, Parsed, Update, levels};
use crate::trading::paper::split;
use serde_json::{Value, json};
use std::time::Duration;

/// Адрес по умолчанию.
pub fn default_url(testnet: bool) -> &'static str {
    if testnet {
        "wss://ws-testnet.gate.com/v4/ws/spot"
    } else {
        "wss://api.gateio.ws/ws/v4/"
    }
}

/// BTCUSDT → BTC_USDT.
pub fn pair(s: &str) -> String {
    split(s).map_or_else(|| s.to_uppercase(), |(b, q)| format!("{b}_{q}"))
}

fn now_s() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub struct Gate {
    url: String,
    pairs: Vec<String>,
    bbo: bool,
}

impl Gate {
    pub fn new(url: String, symbols: &[String], bbo: bool) -> Gate {
        Gate {
            url,
            pairs: symbols.iter().map(|s| pair(s)).collect(),
            bbo,
        }
    }
}

impl Dialect for Gate {
    fn url(&self) -> String {
        self.url.clone()
    }

    fn subscribe(&self) -> Vec<String> {
        let mut out = Vec::new();
        for p in &self.pairs {
            out.push(json!({"time": now_s(), "channel": "spot.order_book", "event": "subscribe", "payload": [p, "20", "100ms"]}).to_string());
            if self.bbo {
                out.push(json!({"time": now_s(), "channel": "spot.book_ticker", "event": "subscribe", "payload": [p]}).to_string());
            }
            out.push(json!({"time": now_s(), "channel": "spot.trades", "event": "subscribe", "payload": [p]}).to_string());
        }
        out
    }

    fn ping(&self) -> Option<(String, Duration)> {
        Some((
            json!({"time": now_s(), "channel": "spot.ping"}).to_string(),
            Duration::from_secs(20),
        ))
    }

    fn parse(&self, text: &str) -> Parsed {
        let v: Value = serde_json::from_str(text).map_err(|e| format!("Gate: {e}"))?;
        if !v["error"].is_null() {
            return Err(format!(
                "Gate error {}: {}",
                v["error"]["code"], v["error"]["message"]
            ));
        }
        if v["event"] != "update" {
            return Ok(vec![]);
        } // ответы на подписку, pong
        let r = &v["result"];
        let num = |x: &Value| {
            x.as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| x.as_f64())
        };
        if v["channel"] == "spot.trades" {
            return Ok(
                match (
                    r["currency_pair"].as_str(),
                    num(&r["price"]),
                    num(&r["amount"]),
                ) {
                    (Some(p), Some(px), Some(qty)) => vec![Update::Trade {
                        sym: p.replace('_', ""),
                        px,
                        qty,
                        buy: r["side"] == "buy",
                        ts: num(&r["create_time_ms"]).unwrap_or(0.0) as u64,
                    }],
                    _ => vec![],
                },
            );
        }
        let sym = r["s"]
            .as_str()
            .ok_or("Gate: нет пары в обновлении")?
            .replace('_', "");
        let time: Vec<Update> = r["t"].as_u64().map(Update::Time).into_iter().collect();
        let id = r["lastUpdateId"]
            .as_u64()
            .or_else(|| r["u"].as_u64())
            .unwrap_or(0);
        match v["channel"].as_str() {
            Some("spot.order_book") => Ok([
                time,
                vec![Update::Snapshot {
                    sym,
                    bids: levels(r.get("bids")),
                    asks: levels(r.get("asks")),
                    id,
                }],
            ]
            .concat()),
            Some("spot.book_ticker") => {
                let n = |k: &str| r[k].as_str().and_then(|s| s.parse::<f64>().ok());
                match (n("b"), n("B"), n("a"), n("A")) {
                    (Some(b), Some(bq), Some(a), Some(aq)) if b > 0.0 && a > 0.0 => Ok([
                        time,
                        vec![Update::Best {
                            sym,
                            bid: (b, bq),
                            ask: (a, aq),
                            id,
                        }],
                    ]
                    .concat()),
                    _ => Err(format!("Gate: book_ticker без цен {text}")),
                }
            }
            _ => Ok(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_and_parse() {
        assert_eq!(pair("BTCUSDT"), "BTC_USDT");
        let d = Gate::new("wss://x".into(), &["BTCUSDT".into()], true);
        let s = d.subscribe();
        assert_eq!(s.len(), 3);
        assert!(s[2].contains(r#""channel":"spot.trades""#));
        assert!(s[0].contains(r#""payload":["BTC_USDT","20","100ms"]"#));
        assert_eq!(d.parse(r#"{"time":1,"channel":"spot.order_book","event":"subscribe","result":{"status":"success"}}"#), Ok(vec![]));
        assert!(d.parse(r#"{"channel":"spot.order_book","event":"subscribe","error":{"code":2,"message":"bad"}}"#).is_err());
        let u = d.parse(r#"{"channel":"spot.order_book","event":"update","result":{"t":1,"lastUpdateId":9,"s":"BTC_USDT","bids":[["100","1"]],"asks":[["101","1"]]}}"#).unwrap();
        assert_eq!(
            u,
            vec![
                Update::Time(1),
                Update::Snapshot {
                    sym: "BTCUSDT".into(),
                    bids: vec![(100.0, 1.0)],
                    asks: vec![(101.0, 1.0)],
                    id: 9
                }
            ]
        );
        let b = d.parse(r#"{"channel":"spot.book_ticker","event":"update","result":{"t":1,"u":10,"s":"BTC_USDT","b":"100.5","B":"1","a":"100.6","A":"2"}}"#).unwrap();
        assert!(matches!(b[1], Update::Best { id: 10, .. }));
        assert_eq!(
            d.parse(r#"{"time":1,"channel":"spot.trades","event":"update","result":{"id":1,"create_time":1,"create_time_ms":"1700000000004.123","side":"sell","currency_pair":"BTC_USDT","amount":"0.4","price":"100.52"}}"#),
            Ok(vec![Update::Trade { sym: "BTCUSDT".into(), px: 100.52, qty: 0.4, buy: false, ts: 1_700_000_000_004 }])
        );
    }
}
