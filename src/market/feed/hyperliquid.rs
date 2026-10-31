//! Hyperliquid (перпы и спот): `l2Book` по монете (спот — по имени пары из spotMeta: PURR/USDC или @107) — каждое сообщение полный снимок (до 20 уровней с каждой стороны);
//! `trades` — сделки (side B — агрессор покупатель); время биржи — `time`. Пинг {"method":"ping"} раз в 20 с. Монета — базовая валюта символа (BTCUSDC → BTC).

use super::{Dialect, Parsed, Update};
use crate::trading::paper::split;
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
    coins: std::sync::Mutex<HashMap<String, String>>,
    /// Спот: REST для spotMeta и символы, которые надо найти.
    spot: Option<(String, Vec<String>)>,
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
        Hyperliquid {
            url,
            coins: std::sync::Mutex::new(coins),
            spot: None,
        }
    }

    /// Спот: имена пар — из POST /info {"type":"spotMeta"} при подключении.
    pub fn new_spot(url: String, rest: String, symbols: &[String]) -> Hyperliquid {
        Hyperliquid {
            url,
            coins: Default::default(),
            spot: Some((rest.trim_end_matches('/').to_owned(), symbols.to_vec())),
        }
    }

    /// Пары спота из spotMeta: символ бота (PURRUSDC) → имя пары на бирже.
    pub fn map_spot(&self, meta: &Value) {
        let Some((_, symbols)) = &self.spot else {
            return;
        };
        let tokens: HashMap<u64, String> = meta["tokens"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|t| Some((t["index"].as_u64()?, t["name"].as_str()?.to_uppercase())))
            .collect();
        let mut m = self.coins.lock().unwrap();
        for u in meta["universe"].as_array().into_iter().flatten() {
            let (Some(name), Some(b), Some(q)) = (
                u["name"].as_str(),
                u["tokens"][0].as_u64(),
                u["tokens"][1].as_u64(),
            ) else {
                continue;
            };
            let (Some(b), Some(q)) = (tokens.get(&b), tokens.get(&q)) else {
                continue;
            };
            let sym = format!("{b}{q}");
            if symbols.iter().any(|s| s.eq_ignore_ascii_case(&sym)) {
                m.insert(name.to_owned(), sym);
            }
        }
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

    fn connect_url(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send + '_>>
    {
        Box::pin(async move {
            if let Some((rest, _)) = &self.spot
                && self.coins.lock().unwrap().is_empty()
            {
                let v: Value = reqwest::Client::new()
                    .post(format!("{rest}/info"))
                    .json(&json!({"type": "spotMeta"}))
                    .send()
                    .await
                    .map_err(|e| e.to_string())?
                    .json()
                    .await
                    .map_err(|e| e.to_string())?;
                self.map_spot(&v);
            }
            Ok(self.url.clone())
        })
    }

    fn subscribe(&self) -> Vec<String> {
        let coins = self.coins.lock().unwrap();
        let mut c: Vec<&String> = coins.keys().collect();
        c.sort();
        c.iter()
            .flat_map(|coin| {
                ["l2Book", "trades"].map(|t| {
                    json!({"method": "subscribe", "subscription": {"type": t, "coin": coin}})
                        .to_string()
                })
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
                let Some(sym) = self.coins.lock().unwrap().get(coin).cloned() else {
                    return Ok(vec![]);
                };
                let lv = d["levels"]
                    .as_array()
                    .filter(|a| a.len() == 2)
                    .ok_or("Hyperliquid: levels не из двух сторон")?;
                let ts = d["time"].as_u64().unwrap_or(0);
                Ok(vec![
                    Update::Time(ts),
                    Update::Snapshot {
                        sym,
                        bids: side(&lv[0]),
                        asks: side(&lv[1]),
                        id: ts,
                    },
                ])
            }
            // сделки: side "B" — агрессор покупатель, "A" — продавец
            Some("trades") => {
                let coins = self.coins.lock().unwrap();
                let n = |x: &Value| x.as_str().and_then(|s| s.parse::<f64>().ok());
                Ok(v["data"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|t| {
                        Some(Update::Trade {
                            sym: coins.get(t["coin"].as_str()?)?.clone(),
                            px: n(&t["px"])?,
                            qty: n(&t["sz"])?,
                            buy: t["side"] == "B",
                            ts: t["time"].as_u64().unwrap_or(0),
                        })
                    })
                    .collect())
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
                    .to_string(),
                r#"{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}"#
                    .to_string()
            ]
        );
        let m = r#"{"channel":"l2Book","data":{"coin":"BTC","time":5,"levels":[[{"px":"100","sz":"1.5","n":2}],[{"px":"101","sz":"2","n":1}]]}}"#;
        assert_eq!(
            d.parse(m),
            Ok(vec![
                Update::Time(5),
                Update::Snapshot {
                    sym: "BTCUSDC".into(),
                    bids: vec![(100.0, 1.5)],
                    asks: vec![(101.0, 2.0)],
                    id: 5
                }
            ])
        );
        assert_eq!(
            d.parse(r#"{"channel":"trades","data":[{"coin":"BTC","side":"A","px":"100.2","sz":"0.7","time":6,"hash":"0x","tid":1},{"coin":"ETH","side":"B","px":"1","sz":"1","time":6}]}"#),
            Ok(vec![Update::Trade { sym: "BTCUSDC".into(), px: 100.2, qty: 0.7, buy: false, ts: 6 }]),
            "чужая монета пропускается"
        );
        assert_eq!(d.parse(r#"{"channel":"pong"}"#), Ok(vec![]));
        assert!(d.parse(r#"{"channel":"error","data":"bad"}"#).is_err());
    }

    #[test]
    fn spot_pairs_from_spot_meta() {
        let d = Hyperliquid::new_spot(
            "wss://x".into(),
            "https://r".into(),
            &["PURRUSDC".into(), "HYPEUSDC".into()],
        );
        d.map_spot(&json!({"tokens": [{"name": "USDC", "index": 0}, {"name": "PURR", "index": 1}, {"name": "HYPE", "index": 150}],
            "universe": [{"name": "PURR/USDC", "tokens": [1, 0], "index": 0}, {"name": "@107", "tokens": [150, 0], "index": 107}]}));
        let subs = d.subscribe();
        assert!(
            subs.iter().any(|s| s.contains(r#""coin":"@107""#))
                && subs.iter().any(|s| s.contains("PURR/USDC")),
            "{subs:?}"
        );
        let m = r#"{"channel":"l2Book","data":{"coin":"@107","time":1,"levels":[[{"px":"30","sz":"2","n":1}],[{"px":"30.1","sz":"1","n":1}]]}}"#;
        assert_eq!(
            d.parse(m).unwrap()[1],
            Update::Snapshot {
                sym: "HYPEUSDC".into(),
                bids: vec![(30.0, 2.0)],
                asks: vec![(30.1, 1.0)],
                id: 1
            }
        );
    }
}
