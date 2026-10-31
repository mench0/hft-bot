//! KuCoin: адрес и токен — POST /api/v1/bullet-public перед каждым подключением; стакан — 50 уровней снимками.
//! Спот — тема /spotMarket/level2Depth50:BTC-USDT,… (до 100 пар в теме); перпы — /contractMarket/level2Depth50:XBTUSDTM
//! (по теме на контракт, объём в лотах × multiplier из /api/v1/contracts/active). Сделки — /market/match: (спот) и
//! /contractMarket/execution: (перпы), время в наносекундах. Пинг {"id","type":"ping"}.

use super::{Dialect, Parsed, Update, levels};
use crate::trading::paper::split;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// REST по умолчанию.
pub fn rest_url(perp: bool) -> &'static str {
    if perp {
        "https://api-futures.kucoin.com"
    } else {
        "https://api.kucoin.com"
    }
}

pub struct Kucoin {
    rest: String,
    /// Свой адрес из параметров (без запроса токена).
    fixed_url: Option<String>,
    perp: bool,
    symbols: Vec<String>,
    /// Инструмент биржи → (символ бота, объём лота).
    inst: Mutex<HashMap<String, (String, f64)>>,
    ping_ms: AtomicU64,
}

/// Монета KuCoin Futures: BTC торгуется как XBT.
fn alias(c: &str) -> &str {
    if c == "BTC" { "XBT" } else { c }
}

impl Kucoin {
    pub fn new(rest: String, fixed_url: Option<String>, symbols: &[String], perp: bool) -> Kucoin {
        let mut inst = HashMap::new();
        if !perp {
            for s in symbols {
                if let Some((b, q)) = split(s) {
                    inst.insert(format!("{b}-{q}"), (s.to_uppercase(), 1.0));
                }
            }
        }
        Kucoin {
            rest: rest.trim_end_matches('/').to_owned(),
            fixed_url,
            perp,
            symbols: symbols.to_vec(),
            inst: Mutex::new(inst),
            ping_ms: AtomicU64::new(18_000),
        }
    }

    /// Контракты перпов: символ бота → инструмент и объём лота.
    pub fn map_contracts(&self, v: &Value) {
        let n = |x: &Value| {
            x.as_f64()
                .or_else(|| x.as_str().and_then(|s| s.parse().ok()))
        };
        let mut m = self.inst.lock().unwrap();
        for s in &self.symbols {
            let Some((b, q)) = split(s) else { continue };
            let found = v["data"].as_array().into_iter().flatten().find(|c| {
                c["baseCurrency"].as_str() == Some(alias(&b))
                    && c["quoteCurrency"].as_str() == Some(q.as_str())
                    && c["isInverse"] != true
            });
            if let Some(c) = found
                && let (Some(id), Some(k)) = (c["symbol"].as_str(), n(&c["multiplier"]))
            {
                m.insert(id.to_owned(), (s.to_uppercase(), k.abs()));
            }
        }
    }
}

impl Dialect for Kucoin {
    fn url(&self) -> String {
        self.fixed_url.clone().unwrap_or_default()
    }

    fn connect_url(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send + '_>>
    {
        Box::pin(async move {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|e| e.to_string())?;
            if self.perp && self.inst.lock().unwrap().is_empty() {
                let v: Value = http
                    .get(format!("{}/api/v1/contracts/active", self.rest))
                    .send()
                    .await
                    .map_err(|e| e.to_string())?
                    .json()
                    .await
                    .map_err(|e| e.to_string())?;
                self.map_contracts(&v);
            }
            if let Some(u) = &self.fixed_url {
                return Ok(u.clone());
            }
            let r: Value = http
                .post(format!("{}/api/v1/bullet-public", self.rest))
                .send()
                .await
                .map_err(|e| e.to_string())?
                .json()
                .await
                .map_err(|e| e.to_string())?;
            if r["code"] != "200000" {
                return Err(format!("KuCoin bullet-public: {r}"));
            }
            let srv = &r["data"]["instanceServers"][0];
            let interval = srv["pingInterval"].as_u64().unwrap_or(18_000);
            self.ping_ms
                .store(interval.saturating_sub(2_000).max(5_000), Ordering::Relaxed);
            Ok(format!(
                "{}?token={}&connectId=hft{}",
                srv["endpoint"].as_str().unwrap_or(""),
                r["data"]["token"].as_str().unwrap_or(""),
                std::process::id()
            ))
        })
    }

    fn subscribe(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.inst.lock().unwrap().keys().cloned().collect();
        ids.sort();
        // стакан и сделки
        let topics: &[&str] = if self.perp {
            &[
                "/contractMarket/level2Depth50:",
                "/contractMarket/execution:",
            ]
        } else {
            &["/spotMarket/level2Depth50:", "/market/match:"]
        };
        let batch = if self.perp { 1 } else { 100 };
        topics
            .iter()
            .flat_map(|t| ids.chunks(batch).map(move |c| format!("{t}{}", c.join(","))))
            .enumerate()
            .map(|(i, topic)| json!({"id": format!("sub{i}"), "type": "subscribe", "topic": topic, "privateChannel": false, "response": true}).to_string())
            .collect()
    }

    fn ping(&self) -> Option<(String, Duration)> {
        Some((
            json!({"id": "ping", "type": "ping"}).to_string(),
            Duration::from_millis(self.ping_ms.load(Ordering::Relaxed)),
        ))
    }

    fn parse(&self, text: &str) -> Parsed {
        let v: Value = serde_json::from_str(text).map_err(|e| format!("KuCoin: {e}"))?;
        match v["type"].as_str() {
            Some("error") => Err(format!("KuCoin error {}: {}", v["code"], v["data"])),
            Some("message") => {
                let topic = v["topic"].as_str().unwrap_or("");
                let Some((_, id)) = topic.split_once(':') else {
                    return Ok(vec![]);
                };
                let Some((sym, k)) = self.inst.lock().unwrap().get(id).cloned() else {
                    return Ok(vec![]);
                };
                let d = &v["data"];
                if topic.starts_with("/market/match:")
                    || topic.starts_with("/contractMarket/execution:")
                {
                    let n = |x: &Value| {
                        x.as_f64()
                            .or_else(|| x.as_str().and_then(|s| s.parse().ok()))
                    };
                    // время сделки — в наносекундах (спот: time, перпы: ts)
                    let ns = n(&d["time"]).or_else(|| n(&d["ts"])).unwrap_or(0.0);
                    return Ok(match (n(&d["price"]), n(&d["size"])) {
                        (Some(px), Some(q)) => vec![Update::Trade {
                            sym,
                            px,
                            qty: q * k,
                            buy: d["side"] == "buy",
                            ts: (ns / 1e6) as u64,
                        }],
                        _ => vec![],
                    });
                }
                let scale = |l: Vec<(f64, f64)>| l.into_iter().map(|(p, q)| (p, q * k)).collect();
                let ts = d["timestamp"].as_u64().unwrap_or(0);
                let mut out: Vec<Update> =
                    (ts > 0).then_some(Update::Time(ts)).into_iter().collect();
                out.push(Update::Snapshot {
                    sym,
                    bids: scale(levels(d.get("bids"))),
                    asks: scale(levels(d.get("asks"))),
                    id: ts,
                });
                Ok(out)
            }
            _ => Ok(vec![]), // welcome, ack, pong
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spot_and_futures() {
        let d = Kucoin::new(
            "https://x".into(),
            Some("wss://y".into()),
            &["BTCUSDT".into(), "ETHUSDT".into()],
            false,
        );
        assert_eq!(d.subscribe().len(), 2);
        assert!(d.subscribe()[0].contains("/spotMarket/level2Depth50:BTC-USDT,ETH-USDT"));
        assert!(d.subscribe()[1].contains("/market/match:BTC-USDT,ETH-USDT"));
        assert_eq!(
            d.parse(r#"{"type":"message","topic":"/market/match:BTC-USDT","subject":"trade.l3match","data":{"price":"100.3","size":"0.2","side":"sell","symbol":"BTC-USDT","time":"1700000000006000000"}}"#),
            Ok(vec![Update::Trade { sym: "BTCUSDT".into(), px: 100.3, qty: 0.2, buy: false, ts: 1_700_000_000_006 }])
        );
        let m = r#"{"type":"message","topic":"/spotMarket/level2Depth50:BTC-USDT","subject":"level2","data":{"asks":[["101","2"]],"bids":[["100","1"]],"timestamp":7}}"#;
        assert_eq!(
            d.parse(m),
            Ok(vec![
                Update::Time(7),
                Update::Snapshot {
                    sym: "BTCUSDT".into(),
                    bids: vec![(100.0, 1.0)],
                    asks: vec![(101.0, 2.0)],
                    id: 7
                }
            ])
        );
        assert!(
            d.parse(r#"{"type":"error","code":404,"data":"topic"}"#)
                .is_err()
        );
        let f = Kucoin::new("https://x".into(), None, &["BTCUSDT".into()], true);
        f.map_contracts(&json!({"data": [{"symbol": "XBTUSDTM", "baseCurrency": "XBT", "quoteCurrency": "USDT", "multiplier": 0.001, "isInverse": false},
            {"symbol": "XBTUSDM", "baseCurrency": "XBT", "quoteCurrency": "USD", "multiplier": -1, "isInverse": true}]}));
        assert!(f.subscribe()[0].contains("/contractMarket/level2Depth50:XBTUSDTM"));
        let m = r#"{"type":"message","topic":"/contractMarket/level2Depth50:XBTUSDTM","data":{"asks":[[101,2000]],"bids":[[100,1000]],"timestamp":8}}"#;
        assert_eq!(
            f.parse(m),
            Ok(vec![
                Update::Time(8),
                Update::Snapshot {
                    sym: "BTCUSDT".into(),
                    bids: vec![(100.0, 1.0)],
                    asks: vec![(101.0, 2.0)],
                    id: 8
                }
            ])
        );
        assert!(f.subscribe()[1].contains("/contractMarket/execution:XBTUSDTM"));
        assert_eq!(
            f.parse(r#"{"type":"message","topic":"/contractMarket/execution:XBTUSDTM","data":{"symbol":"XBTUSDTM","side":"buy","size":3,"price":"100.1","ts":1700000000007000000}}"#),
            Ok(vec![Update::Trade { sym: "BTCUSDT".into(), px: 100.1, qty: 0.003, buy: true, ts: 1_700_000_000_007 }])
        );
    }
}
