//! Upbit (спот): одна подписка [{ticket}, {type: orderbook, codes: [KRW-BTC, …]}, {format: DEFAULT}] — каждое
//! сообщение полный снимок (до 15 уровней); кадры бинарные (JSON в UTF-8). Пинг — текст PING раз в 30 с.

use super::{Dialect, Parsed, Update};
use crate::trading::paper::split;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Duration;

pub const URL: &str = "wss://api.upbit.com/websocket/v1";

pub struct Upbit {
    url: String,
    /// Рынок (KRW-BTC) → символ бота (BTCKRW).
    markets: HashMap<String, String>,
}

impl Upbit {
    pub fn new(url: String, symbols: &[String]) -> Upbit {
        let markets = symbols
            .iter()
            .filter_map(|s| split(s).map(|(b, q)| (format!("{q}-{b}"), s.to_uppercase())))
            .collect();
        Upbit { url, markets }
    }
}

impl Dialect for Upbit {
    fn url(&self) -> String {
        self.url.clone()
    }

    fn subscribe(&self) -> Vec<String> {
        let mut codes: Vec<&String> = self.markets.keys().collect();
        codes.sort();
        let ticket = format!("hft-{}", std::process::id());
        vec![json!([{"ticket": ticket}, {"type": "orderbook", "codes": codes}, {"format": "DEFAULT"}]).to_string()]
    }

    fn ping(&self) -> Option<(String, Duration)> {
        Some(("PING".into(), Duration::from_secs(30)))
    }

    fn binary(&self, data: &[u8]) -> Option<String> {
        String::from_utf8(data.to_vec()).ok()
    }

    fn parse(&self, text: &str) -> Parsed {
        let v: Value = serde_json::from_str(text).map_err(|e| format!("Upbit: {e}"))?;
        if !v["error"].is_null() {
            return Err(format!("Upbit error: {}", v["error"]));
        }
        if v["type"] != "orderbook" {
            return Ok(vec![]); // {"status":"UP"} и прочее
        }
        let code = v["code"].as_str().ok_or("Upbit: нет рынка")?;
        let Some(sym) = self.markets.get(code) else {
            return Ok(vec![]);
        };
        let n = |x: &Value| {
            x.as_f64()
                .or_else(|| x.as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(0.0)
        };
        let (mut bids, mut asks) = (Vec::new(), Vec::new());
        for u in v["orderbook_units"].as_array().into_iter().flatten() {
            let (bp, bs, ap, a) = (
                n(&u["bid_price"]),
                n(&u["bid_size"]),
                n(&u["ask_price"]),
                n(&u["ask_size"]),
            );
            if bp > 0.0 && bs > 0.0 {
                bids.push((bp, bs));
            }
            if ap > 0.0 && a > 0.0 {
                asks.push((ap, a));
            }
        }
        Ok(vec![Update::Snapshot {
            sym: sym.clone(),
            bids,
            asks,
            id: v["timestamp"].as_u64().unwrap_or(0),
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_and_parse() {
        let d = Upbit::new(URL.into(), &["BTCKRW".into(), "ETHUSDT".into()]);
        let s = d.subscribe();
        assert!(s[0].contains(r#""codes":["KRW-BTC","USDT-ETH"]"#), "{s:?}");
        let m = br#"{"type":"orderbook","code":"KRW-BTC","timestamp":9,"orderbook_units":[{"ask_price":101,"bid_price":100,"ask_size":0.5,"bid_size":1}]}"#;
        let t = d.binary(m).unwrap();
        assert_eq!(
            d.parse(&t),
            Ok(vec![Update::Snapshot {
                sym: "BTCKRW".into(),
                bids: vec![(100.0, 1.0)],
                asks: vec![(101.0, 0.5)],
                id: 9
            }])
        );
        assert_eq!(d.parse(r#"{"status":"UP"}"#), Ok(vec![]));
        assert!(
            d.parse(r#"{"error":{"name":"INVALID","message":"x"}}"#)
                .is_err()
        );
    }
}
