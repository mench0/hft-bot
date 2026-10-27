//! LBank (спот): подписка `depth` по паре (btc_usdt, глубина 50) — каждое сообщение полный снимок.
//! Пинг сервера {"action":"ping","ping":x} → ответ {"action":"pong","pong":x}; свой пинг раз в 20 с.

use super::{Dialect, Parsed, Update, levels};
use crate::trading::paper::split;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Duration;

pub const URL: &str = "wss://www.lbkex.net/ws/V2/";

pub struct Lbank {
    url: String,
    /// Пара (btc_usdt) → символ бота.
    pairs: HashMap<String, String>,
}

impl Lbank {
    pub fn new(url: String, symbols: &[String]) -> Lbank {
        let pairs = symbols
            .iter()
            .map(|s| {
                (
                    split(s).map_or_else(
                        || s.to_lowercase(),
                        |(b, q)| format!("{b}_{q}").to_lowercase(),
                    ),
                    s.to_uppercase(),
                )
            })
            .collect();
        Lbank { url, pairs }
    }
}

impl Dialect for Lbank {
    fn url(&self) -> String {
        self.url.clone()
    }

    fn subscribe(&self) -> Vec<String> {
        let mut p: Vec<&String> = self.pairs.keys().collect();
        p.sort();
        p.iter()
            .map(|pair| {
                json!({"action": "subscribe", "subscribe": "depth", "depth": 50, "pair": pair})
                    .to_string()
            })
            .collect()
    }

    fn ping(&self) -> Option<(String, Duration)> {
        Some((
            json!({"action": "ping", "ping": "hft"}).to_string(),
            Duration::from_secs(20),
        ))
    }

    fn reply(&self, text: &str) -> Option<String> {
        let v: Value = serde_json::from_str(text).ok()?;
        (v["action"] == "ping").then(|| json!({"action": "pong", "pong": v["ping"]}).to_string())
    }

    fn parse(&self, text: &str) -> Parsed {
        let v: Value = serde_json::from_str(text).map_err(|e| format!("LBank: {e}"))?;
        if v["status"] == "error" {
            return Err(format!(
                "LBank error: {}",
                v["message"]
                    .as_str()
                    .or(v["msg"].as_str())
                    .or(v["error"].as_str())
                    .unwrap_or("")
            ));
        }
        if v["type"] != "depth" {
            return Ok(vec![]); // подтверждения подписки, pong
        }
        let pair = v["pair"].as_str().ok_or("LBank: нет пары в стакане")?;
        let Some(sym) = self.pairs.get(pair) else {
            return Ok(vec![]);
        };
        let d = &v["depth"];
        Ok(vec![Update::Snapshot {
            sym: sym.clone(),
            bids: levels(d.get("bids")),
            asks: levels(d.get("asks")),
            id: 0,
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_ping_and_parse() {
        let d = Lbank::new(URL.into(), &["ETHUSDT".into()]);
        assert_eq!(
            d.subscribe(),
            vec![
                r#"{"action":"subscribe","subscribe":"depth","depth":50,"pair":"eth_usdt"}"#
                    .to_string()
            ]
        );
        assert_eq!(
            d.reply(r#"{"action":"ping","ping":"abc"}"#).as_deref(),
            Some(r#"{"action":"pong","pong":"abc"}"#)
        );
        assert_eq!(d.reply(r#"{"type":"depth"}"#), None);
        let m = r#"{"depth":{"asks":[[2001,1.5]],"bids":[["1999","2"]]},"count":50,"type":"depth","pair":"eth_usdt","SERVER":"V2","TS":"2026"}"#;
        assert_eq!(
            d.parse(m),
            Ok(vec![Update::Snapshot {
                sym: "ETHUSDT".into(),
                bids: vec![(1999.0, 2.0)],
                asks: vec![(2001.0, 1.5)],
                id: 0
            }])
        );
        assert!(
            d.parse(r#"{"status":"error","message":"bad pair"}"#)
                .is_err()
        );
    }
}
