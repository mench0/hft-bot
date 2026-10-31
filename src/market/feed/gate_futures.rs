//! Gate v4 USDT-перпы: `futures.order_book` — снимки 20 уровней (`all`), `futures.book_ticker` — лучшие цены,
//! `futures.trades` — сделки (объём со знаком: минус — продажа); время биржи — `t` / `create_time_ms`;
//! контракт BTC_USDT, объёмы в контрактах переводятся в монеты по `quanto_multiplier`. Пинг `futures.ping` раз в 20 с.

use super::okx::ContractSizes;
use super::{Dialect, Parsed, Update};
use serde_json::{Value, json};
use std::time::Duration;

/// Адрес по умолчанию.
pub fn default_url(testnet: bool) -> &'static str {
    if testnet {
        "wss://ws-testnet.gate.com/v4/ws/futures/usdt"
    } else {
        "wss://fx-ws.gateio.ws/v4/ws/usdt"
    }
}

fn now_s() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub struct GateFutures {
    url: String,
    contracts: Vec<String>,
    bbo: bool,
    sizes: ContractSizes,
}

impl GateFutures {
    pub fn new(url: String, symbols: &[String], bbo: bool, sizes: ContractSizes) -> GateFutures {
        GateFutures {
            url,
            contracts: symbols.iter().map(|s| super::gate::pair(s)).collect(),
            bbo,
            sizes,
        }
    }
}

fn num(v: &Value) -> Option<f64> {
    v.as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| v.as_f64())
}

/// Уровни [{p, s}] → (цена, объём × k).
fn levels(v: &Value, k: f64) -> Vec<(f64, f64)> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|l| Some((num(&l["p"])?, num(&l["s"])? * k)))
        .collect()
}

impl Dialect for GateFutures {
    fn url(&self) -> String {
        self.url.clone()
    }

    fn subscribe(&self) -> Vec<String> {
        let mut out = Vec::new();
        for c in &self.contracts {
            out.push(json!({"time": now_s(), "channel": "futures.order_book", "event": "subscribe", "payload": [c, "20", "0"]}).to_string());
            if self.bbo {
                out.push(json!({"time": now_s(), "channel": "futures.book_ticker", "event": "subscribe", "payload": [c]}).to_string());
            }
            out.push(json!({"time": now_s(), "channel": "futures.trades", "event": "subscribe", "payload": [c]}).to_string());
        }
        out
    }

    fn ping(&self) -> Option<(String, Duration)> {
        Some((
            json!({"time": now_s(), "channel": "futures.ping"}).to_string(),
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
        let ev = v["event"].as_str().unwrap_or("");
        if ev != "all" && ev != "update" {
            return Ok(vec![]); // ответы на подписку, pong
        }
        let r = &v["result"];
        if v["channel"] == "futures.trades" {
            let sizes = self.sizes.lock().unwrap();
            return Ok(r
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|t| {
                    let sym = t["contract"].as_str()?.replace('_', "");
                    let k = *sizes.get(&sym)?;
                    let size = num(&t["size"])?;
                    Some(Update::Trade {
                        sym,
                        px: num(&t["price"])?,
                        qty: size.abs() * k,
                        buy: size > 0.0,
                        ts: num(&t["create_time_ms"]).unwrap_or(0.0) as u64,
                    })
                })
                .collect());
        }
        let c = r["contract"]
            .as_str()
            .or_else(|| r["s"].as_str())
            .ok_or("Gate: нет контракта в обновлении")?;
        let sym = c.replace('_', "");
        let Some(k) = self.sizes.lock().unwrap().get(&sym).copied() else {
            return Ok(vec![]); // размер контракта ещё не получен
        };
        let id = r["id"].as_u64().or_else(|| r["u"].as_u64()).unwrap_or(0);
        let time: Vec<Update> = r["t"].as_u64().map(Update::Time).into_iter().collect();
        match v["channel"].as_str() {
            Some("futures.order_book") if ev == "all" => Ok([
                time,
                vec![Update::Snapshot {
                    sym,
                    bids: levels(&r["bids"], k),
                    asks: levels(&r["asks"], k),
                    id,
                }],
            ]
            .concat()),
            Some("futures.book_ticker") => {
                match (num(&r["b"]), num(&r["B"]), num(&r["a"]), num(&r["A"])) {
                    (Some(b), Some(bq), Some(a), Some(aq)) if b > 0.0 && a > 0.0 => Ok([
                        time,
                        vec![Update::Best {
                            sym,
                            bid: (b, bq * k),
                            ask: (a, aq * k),
                            id,
                        }],
                    ]
                    .concat()),
                    _ => Ok(vec![]),
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
        let sizes: ContractSizes = Default::default();
        let d = GateFutures::new("wss://x".into(), &["BTCUSDT".into()], true, sizes.clone());
        assert!(d.subscribe()[0].contains(r#""payload":["BTC_USDT","20","0"]"#));
        let ob = r#"{"channel":"futures.order_book","event":"all","result":{"t":1,"id":5,"contract":"BTC_USDT","asks":[{"p":"101","s":3}],"bids":[{"p":"100","s":5}]}}"#;
        assert_eq!(d.parse(ob), Ok(vec![]));
        sizes.lock().unwrap().insert("BTCUSDT".into(), 0.5);
        assert_eq!(
            d.parse(ob),
            Ok(vec![
                Update::Time(1),
                Update::Snapshot {
                    sym: "BTCUSDT".into(),
                    bids: vec![(100.0, 2.5)],
                    asks: vec![(101.0, 1.5)],
                    id: 5
                }
            ])
        );
        let bt = r#"{"channel":"futures.book_ticker","event":"update","result":{"t":1,"u":6,"s":"BTC_USDT","b":"100.5","B":10,"a":"100.6","A":20}}"#;
        assert_eq!(
            d.parse(bt),
            Ok(vec![
                Update::Time(1),
                Update::Best {
                    sym: "BTCUSDT".into(),
                    bid: (100.5, 5.0),
                    ask: (100.6, 10.0),
                    id: 6
                }
            ])
        );
        assert_eq!(
            d.parse(r#"{"channel":"futures.trades","event":"update","result":[{"size":-4,"id":1,"create_time":1,"create_time_ms":1700000000005,"price":"100.4","contract":"BTC_USDT"}]}"#),
            Ok(vec![Update::Trade { sym: "BTCUSDT".into(), px: 100.4, qty: 2.0, buy: false, ts: 1_700_000_000_005 }])
        );
        assert!(d.parse(r#"{"channel":"futures.order_book","event":"subscribe","error":{"code":2,"message":"bad"}}"#).is_err());
    }
}
