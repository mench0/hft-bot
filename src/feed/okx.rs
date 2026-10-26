//! OKX v5 (спот): канал `books` (снимок, затем изменения по `action`) и `bbo-tbt` (лучшие цены), номер `seqId` общий.
//! Инструмент BTC-USDT. Пинг — текст `ping` раз в 20 с (без сообщений 30 с OKX закрывает соединение).

use super::{Dialect, Parsed, Update, levels};
use crate::paper::split;
use serde_json::{Value, json};
use std::time::Duration;

/// Адрес по умолчанию.
pub fn default_url(testnet: bool) -> &'static str {
    if testnet {
        "wss://wspap.okx.com:8443/ws/v5/public"
    } else {
        "wss://ws.okx.com:8443/ws/v5/public"
    }
}

/// BTCUSDT → BTC-USDT.
pub fn inst_id(s: &str) -> String {
    split(s).map_or_else(|| s.to_uppercase(), |(b, q)| format!("{b}-{q}"))
}

pub struct Okx {
    url: String,
    args: Vec<Value>,
}

impl Okx {
    pub fn new(url: String, symbols: &[String], bbo: bool) -> Okx {
        let mut args = Vec::new();
        for s in symbols {
            args.push(json!({"channel": "books", "instId": inst_id(s)}));
            if bbo {
                args.push(json!({"channel": "bbo-tbt", "instId": inst_id(s)}));
            }
        }
        Okx { url, args }
    }
}

impl Dialect for Okx {
    fn url(&self) -> String {
        self.url.clone()
    }

    /// До 20 каналов в сообщении.
    fn subscribe(&self) -> Vec<String> {
        self.args
            .chunks(20)
            .map(|c| json!({"op": "subscribe", "args": c}).to_string())
            .collect()
    }

    fn ping(&self) -> Option<(String, Duration)> {
        Some(("ping".into(), Duration::from_secs(20)))
    }

    fn parse(&self, text: &str) -> Parsed {
        if text == "pong" {
            return Ok(vec![]);
        }
        let v: Value = serde_json::from_str(text).map_err(|e| format!("OKX: {e}"))?;
        if let Some(ev) = v["event"].as_str() {
            return if ev == "error" {
                Err(format!("OKX error {}: {}", v["code"], v["msg"]))
            } else {
                Ok(vec![])
            };
        }
        let ch = v["arg"]["channel"].as_str().unwrap_or("");
        let sym = v["arg"]["instId"].as_str().unwrap_or("").replace('-', "");
        let mut out = Vec::new();
        for d in v["data"].as_array().into_iter().flatten() {
            let id = d["seqId"].as_u64().unwrap_or(0);
            let (bids, asks) = (levels(d.get("bids")), levels(d.get("asks")));
            match ch {
                "bbo-tbt" => {
                    if let (Some(b), Some(a)) = (bids.first(), asks.first()) {
                        out.push(Update::Best {
                            sym: sym.clone(),
                            bid: *b,
                            ask: *a,
                            id,
                        });
                    }
                }
                "books" if v["action"] == "snapshot" => out.push(Update::Snapshot {
                    sym: sym.clone(),
                    bids,
                    asks,
                    id,
                }),
                "books" => out.push(Update::Delta {
                    sym: sym.clone(),
                    bids,
                    asks,
                    id,
                }),
                _ => {}
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_and_parse() {
        assert_eq!(inst_id("BTCUSDT"), "BTC-USDT");
        let d = Okx::new("wss://x".into(), &["BTCUSDT".into()], true);
        assert!(d.subscribe()[0].contains(r#"{"channel":"books","instId":"BTC-USDT"}"#));
        assert_eq!(d.parse("pong"), Ok(vec![]));
        assert_eq!(
            d.parse(r#"{"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"}}"#),
            Ok(vec![])
        );
        assert!(
            d.parse(r#"{"event":"error","code":"60012","msg":"bad"}"#)
                .is_err()
        );
        let s = d.parse(r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"bids":[["100","1","0","1"]],"asks":[["101","2","0","1"]],"ts":"1","seqId":5}]}"#).unwrap();
        assert_eq!(
            s,
            vec![Update::Snapshot {
                sym: "BTCUSDT".into(),
                bids: vec![(100.0, 1.0)],
                asks: vec![(101.0, 2.0)],
                id: 5
            }]
        );
        let u = d.parse(r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"bids":[["100","0","0","0"]],"asks":[],"seqId":6}]}"#).unwrap();
        assert!(matches!(&u[0], Update::Delta { id: 6, bids, .. } if bids == &vec![(100.0, 0.0)]));
        let b = d.parse(r#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"bids":[["100.5","1","0","1"]],"asks":[["100.6","1","0","1"]],"seqId":7}]}"#).unwrap();
        assert!(matches!(b[0], Update::Best { id: 7, .. }));
    }
}
