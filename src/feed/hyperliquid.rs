//! Hyperliquid (перпы): `l2Book` по монете — каждое сообщение полный снимок (до 20 уровней с каждой стороны);
//! пинг {"method":"ping"} раз в 20 с. Монета — базовая валюта символа (BTCUSDC → BTC).

use super::{Dialect, Parsed, Update};
use crate::paper::split;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Duration;

pub fn default_url(testnet: bool) -> &'static str {
    if testnet {
        "wss://api.hyperliquid-testnet.xyz/ws"
    } else {
        "wss://api.hyperliquid.xyz/ws"
    }
}

pub struct Hyperliquid {
    url: String,
    /// Монета → символ бота.
    coins: HashMap<String, String>,
}

impl Hyperliquid {
    pub fn new(url: String, symbols: &[String]) -> Hyperliquid {
        let coins = symbols
            .iter()
            .map(|s| {
                (
                    split(s).map_or_else(|| s.to_uppercase(), |x| x.0),
                    s.to_uppercase(),
                )
            })
            .collect();
        Hyperliquid { url, coins }
    }
}

fn side(v: &Value) -> Vec<(f64, f64)> {
    let n = |x: &Value| {
        x.as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| x.as_f64())
    };
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|l| Some((n(&l["px"])?, n(&l["sz"])?)))
        .collect()
}

impl Dialect for Hyperliquid {
    fn url(&self) -> String {
        self.url.clone()
    }

    fn subscribe(&self) -> Vec<String> {
        let mut c: Vec<&String> = self.coins.keys().collect();
        c.sort();
        c.iter()
            .map(|coin| {
                json!({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}})
                    .to_string()
            })
            .collect()
    }

    fn ping(&self) -> Option<(String, Duration)> {
        Some((r#"{"method":"ping"}"#.into(), Duration::from_secs(20)))
    }

    fn parse(&self, text: &str) -> Parsed {
        let v: Value = serde_json::from_str(text).map_err(|e| format!("Hyperliquid: {e}"))?;
        match v["channel"].as_str() {
            Some("pong" | "subscriptionResponse") => Ok(vec![]),
            Some("error") => Err(format!("Hyperliquid error: {}", v["data"])),
            Some("l2Book") => {
                let d = &v["data"];
                let coin = d["coin"].as_str().ok_or("Hyperliquid: нет монеты")?;
                let Some(sym) = self.coins.get(coin) else {
                    return Ok(vec![]);
                };
                let lv = d["levels"]
                    .as_array()
                    .filter(|a| a.len() == 2)
                    .ok_or("Hyperliquid: levels не из двух сторон")?;
                Ok(vec![Update::Snapshot {
                    sym: sym.clone(),
                    bids: side(&lv[0]),
                    asks: side(&lv[1]),
                    id: d["time"].as_u64().unwrap_or(0),
                }])
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
        let d = Hyperliquid::new("wss://x".into(), &["BTCUSDC".into()]);
        assert_eq!(
            d.subscribe(),
            vec![
                r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}"#
                    .to_string()
            ]
        );
        let m = r#"{"channel":"l2Book","data":{"coin":"BTC","time":5,"levels":[[{"px":"100","sz":"1.5","n":2}],[{"px":"101","sz":"2","n":1}]]}}"#;
        assert_eq!(
            d.parse(m),
            Ok(vec![Update::Snapshot {
                sym: "BTCUSDC".into(),
                bids: vec![(100.0, 1.5)],
                asks: vec![(101.0, 2.0)],
                id: 5
            }])
        );
        assert_eq!(d.parse(r#"{"channel":"pong"}"#), Ok(vec![]));
        assert!(d.parse(r#"{"channel":"error","data":"bad"}"#).is_err());
    }
}
