//! OKX v5 (спот и перпы SWAP — BTC-USDT-SWAP, объёмы в контрактах переводятся в монеты по ctVal): канал `books` (снимок, затем изменения по `action`), `bbo-tbt` (лучшие цены) и `trades` (сделки), номер `seqId`
//! общий, время биржи — `ts`.
//! Инструмент BTC-USDT. Пинг — текст `ping` раз в 20 с (без сообщений 30 с OKX закрывает соединение).

use super::{Dialect, Parsed, Update, levels};
use crate::trading::paper::split;
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

/// Размер контракта по символу (BTCUSDT → монет в контракте); общий для фида и загрузки при старте.
pub type ContractSizes = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, f64>>>;

pub struct Okx {
    url: String,
    args: Vec<Value>,
    /// Перпы: размеры контрактов; пока размер символа не получен, его обновления пропускаются.
    swap: Option<ContractSizes>,
}

impl Okx {
    pub fn new(url: String, symbols: &[String], bbo: bool) -> Okx {
        let mut args = Vec::new();
        for s in symbols {
            args.push(json!({"channel": "books", "instId": inst_id(s)}));
            if bbo {
                args.push(json!({"channel": "bbo-tbt", "instId": inst_id(s)}));
            }
            args.push(json!({"channel": "trades", "instId": inst_id(s)}));
        }
        Okx {
            url,
            args,
            swap: None,
        }
    }

    /// Перпы: инструменты BTC-USDT-SWAP, объёмы стакана × ctVal.
    pub fn new_swap(url: String, symbols: &[String], bbo: bool, sizes: ContractSizes) -> Okx {
        let mut args = Vec::new();
        for s in symbols {
            let id = format!("{}-SWAP", inst_id(s));
            args.push(json!({"channel": "books", "instId": id}));
            if bbo {
                args.push(json!({"channel": "bbo-tbt", "instId": id}));
            }
            args.push(json!({"channel": "trades", "instId": id}));
        }
        Okx {
            url,
            args,
            swap: Some(sizes),
        }
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
        let sym = v["arg"]["instId"]
            .as_str()
            .unwrap_or("")
            .trim_end_matches("-SWAP")
            .replace('-', "");
        let k = match &self.swap {
            None => 1.0,
            Some(sizes) => match sizes.lock().unwrap().get(&sym) {
                Some(k) => *k,
                None => return Ok(vec![]), // размер контракта ещё не получен
            },
        };
        let scale = |l: Vec<(f64, f64)>| l.into_iter().map(|(p, q)| (p, q * k)).collect::<Vec<_>>();
        let mut out = Vec::new();
        let num = |x: &Value| x.as_str().and_then(|s| s.parse::<f64>().ok());
        for d in v["data"].as_array().into_iter().flatten() {
            let ts = d["ts"]
                .as_str()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            if ch == "trades" {
                if let (Some(px), Some(sz)) = (num(&d["px"]), num(&d["sz"])) {
                    out.push(Update::Trade {
                        sym: sym.clone(),
                        px,
                        qty: sz * k,
                        buy: d["side"] == "buy",
                        ts,
                    });
                }
                continue;
            }
            if ts > 0 {
                out.push(Update::Time(ts));
            }
            let id = d["seqId"].as_u64().unwrap_or(0);
            let (bids, asks) = (scale(levels(d.get("bids"))), scale(levels(d.get("asks"))));
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
            vec![
                Update::Time(1),
                Update::Snapshot {
                    sym: "BTCUSDT".into(),
                    bids: vec![(100.0, 1.0)],
                    asks: vec![(101.0, 2.0)],
                    id: 5
                }
            ]
        );
        let u = d.parse(r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"bids":[["100","0","0","0"]],"asks":[],"seqId":6}]}"#).unwrap();
        assert!(matches!(&u[0], Update::Delta { id: 6, bids, .. } if bids == &vec![(100.0, 0.0)]));
        let b = d.parse(r#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT"},"data":[{"bids":[["100.5","1","0","1"]],"asks":[["100.6","1","0","1"]],"seqId":7}]}"#).unwrap();
        assert!(matches!(b[0], Update::Best { id: 7, .. }));
        assert_eq!(
            d.parse(r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"1","px":"100.55","sz":"0.3","side":"sell","ts":"1700000000003"}]}"#),
            Ok(vec![Update::Trade { sym: "BTCUSDT".into(), px: 100.55, qty: 0.3, buy: false, ts: 1_700_000_000_003 }])
        );
    }

    #[test]
    fn swap_scales_contracts() {
        let sizes: ContractSizes = Default::default();
        let d = Okx::new_swap("wss://x".into(), &["BTCUSDT".into()], false, sizes.clone());
        assert!(d.subscribe()[0].contains("BTC-USDT-SWAP"));
        let m = r#"{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"snapshot","data":[{"bids":[["100","5","0","1"]],"asks":[["101","3","0","1"]],"seqId":7}]}"#;
        assert_eq!(
            d.parse(m),
            Ok(vec![]),
            "размер контракта не известен — пропуск"
        );
        sizes.lock().unwrap().insert("BTCUSDT".into(), 0.01);
        assert_eq!(
            d.parse(m),
            Ok(vec![Update::Snapshot {
                sym: "BTCUSDT".into(),
                bids: vec![(100.0, 0.05)],
                asks: vec![(101.0, 0.03)],
                id: 7
            }])
        );
    }
}
