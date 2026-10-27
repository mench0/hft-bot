//! Реальные ордера Upbit, спот за USDT (рынки USDT-BTC; KRW-рынки бот смотрит только для поиска листингов).
//! Как в Java UpbitRestClient: подпись — JWT HS256 {access_key, nonce, query_hash = SHA512 строки «k=v&…»,
//! query_hash_alg} в заголовке Authorization: Bearer; GET/DELETE — параметры в строке, POST — JSON.
//! Сторона bid — покупка, ask — продажа; рыночная покупка — на сумму (ord_type=price, price = объём × опорная × 1.005),
//! рыночная продажа — объёмом (ord_type=market); свой номер — identifier. Состояния: wait/watch — в работе,
//! done — исполнен, cancel — отменён. Приватный поток wss://api.upbit.com/websocket/v1/private (тот же JWT в заголовке):
//! myOrder и myAsset, на текст PING сервер отвечает {"status":"UP"}.

use crate::exchange::binance::{Keys, OrderState, dec};
use crate::infra::util::now_ms;
use crate::trading::live::{Live, StreamMsg};
use crate::trading::paper::{Side, split};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::{Digest, Sha256, Sha512};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub const REST: &str = "https://api.upbit.com";
const MARKET_BUY_SLIPPAGE: f64 = 0.005;

pub fn stream_url() -> String {
    crate::infra::util::env("UPBIT_STREAM_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "wss://api.upbit.com/websocket/v1/private".into())
}

/// BTCUSDT → USDT-BTC.
pub fn market(symbol: &str) -> String {
    split(symbol).map_or_else(|| symbol.to_uppercase(), |(b, q)| format!("{q}-{b}"))
}

fn b64url(b: &[u8]) -> String {
    crate::infra::util::base64(b)
        .trim_end_matches('=')
        .replace('+', "-")
        .replace('/', "_")
}

static NONCE: AtomicU64 = AtomicU64::new(0);

/// JWT HS256; query — строка параметров для query_hash (пустая — без него).
pub fn jwt(key: &str, secret: &str, query: &str) -> String {
    let nonce = format!("{}-{}", now_ms(), NONCE.fetch_add(1, Ordering::Relaxed));
    let mut p = json!({"access_key": key, "nonce": nonce});
    if !query.is_empty() {
        p["query_hash"] = json!(hex::encode(Sha512::digest(query.as_bytes())));
        p["query_hash_alg"] = json!("SHA512");
    }
    let head = b64url(br#"{"alg":"HS256","typ":"JWT"}"#);
    let body = b64url(p.to_string().as_bytes());
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC принимает ключ любой длины");
    mac.update(format!("{head}.{body}").as_bytes());
    format!("{head}.{body}.{}", b64url(&mac.finalize().into_bytes()))
}

fn num(v: &Value) -> f64 {
    v.as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| v.as_f64())
        .unwrap_or(0.0)
}

fn local_of(c: &str) -> Option<u64> {
    c.strip_prefix("hft")?.parse().ok()
}

/// Ордер Upbit (REST и myOrder): исполнено executed_volume, сумма executed_funds (или по сделкам, или × avg_price).
pub fn order_state(o: &Value) -> OrderState {
    let exec = num(&o["executed_volume"]);
    let mut funds = num(&o["executed_funds"]);
    if funds == 0.0 {
        funds = o["trades"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|t| num(&t["funds"]))
            .sum();
    }
    if funds == 0.0 {
        funds = exec * num(&o["avg_price"]);
    }
    let status = match o["state"].as_str().unwrap_or("wait") {
        "done" => "FILLED",
        "cancel" => "CANCELED",
        _ if exec > 0.0 => "PARTIALLY_FILLED",
        _ => "NEW",
    };
    OrderState {
        id: o["uuid"].as_str().unwrap_or("").into(),
        status: status.into(),
        executed: exec,
        quote: funds,
    }
}

pub struct Client {
    base: String,
    keys: Keys,
    symbols: Vec<String>,
    ticks: Mutex<HashMap<String, f64>>,
    http: reqwest::Client,
}

impl Client {
    pub fn new(base: &str, keys: Keys, symbols: &[String]) -> Client {
        Client {
            base: base.trim_end_matches('/').to_owned(),
            keys,
            symbols: symbols.to_vec(),
            ticks: Mutex::default(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP-клиент"),
        }
    }

    pub fn auth(&self, query: &str) -> String {
        format!("Bearer {}", jwt(&self.keys.key, &self.keys.secret, query))
    }

    async fn signed(
        &self,
        method: reqwest::Method,
        path: &str,
        p: &[(&str, String)],
    ) -> Result<Value, String> {
        let q: Vec<String> = p.iter().map(|(k, v)| format!("{k}={v}")).collect();
        let q = q.join("&");
        let mut r = if method == reqwest::Method::POST {
            let body: serde_json::Map<String, Value> =
                p.iter().map(|(k, v)| (k.to_string(), json!(v))).collect();
            self.http
                .post(format!("{}/v1{path}", self.base))
                .json(&body)
        } else {
            let url = if q.is_empty() {
                format!("{}/v1{path}", self.base)
            } else {
                format!("{}/v1{path}?{q}", self.base)
            };
            self.http.request(method, url)
        };
        r = r.header("Authorization", self.auth(&q));
        let r = r.send().await.map_err(|e| e.to_string())?;
        let st = r.status();
        let v: Value = r.json().await.map_err(|e| format!("HTTP {st}: {e}"))?;
        if !st.is_success() || v.get("error").is_some() {
            return Err(format!(
                "Upbit {}: {}",
                v["error"]["name"].as_str().unwrap_or(st.as_str()),
                v["error"]["message"].as_str().unwrap_or(&v.to_string())
            ));
        }
        Ok(v)
    }

    /// Шаг цены рынков: GET /v1/orderbook/instruments.
    pub async fn sync_time(&self) -> Result<(), String> {
        let m: Vec<String> = self.symbols.iter().map(|s| market(s)).collect();
        let v: Value = self
            .http
            .get(format!(
                "{}/v1/orderbook/instruments?markets={}",
                self.base,
                m.join(",")
            ))
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        let mut t = self.ticks.lock().unwrap();
        for x in v.as_array().into_iter().flatten() {
            if let Some(m) = x["market"].as_str() {
                t.insert(m.to_owned(), num(&x["tick_size"]));
            }
        }
        Ok(())
    }

    fn price(&self, m: &str, px: f64) -> String {
        match self
            .ticks
            .lock()
            .unwrap()
            .get(m)
            .copied()
            .filter(|t| *t > 0.0)
        {
            Some(t) => {
                let decs = (-t.log10()).ceil().max(0.0) as usize;
                format!("{:.*}", decs, (px / t).round() * t)
                    .parse::<f64>()
                    .map(dec)
                    .unwrap_or_else(|_| dec(px))
            }
            None => dec(px),
        }
    }

    pub async fn place(
        &self,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
        ref_px: f64,
    ) -> Result<OrderState, String> {
        let m = market(symbol);
        let vol = dec(((qty * 1e8) + 1e-6).floor() / 1e8);
        let mut p = vec![
            ("market", m.clone()),
            (
                "side",
                if side == Side::Buy { "bid" } else { "ask" }.to_owned(),
            ),
        ];
        match price {
            Some(px) => {
                p.push(("volume", vol));
                p.push(("price", self.price(&m, px)));
                p.push(("ord_type", "limit".into()));
            }
            None if side == Side::Buy => {
                if ref_px <= 0.0 {
                    return Err("Upbit: нет опорной цены для рыночной покупки".into());
                }
                let sum = qty * ref_px * (1.0 + MARKET_BUY_SLIPPAGE);
                p.push((
                    "price",
                    if m.starts_with("KRW-") {
                        format!("{:.0}", sum.ceil())
                    } else {
                        dec((sum * 1e8).round() / 1e8)
                    },
                ));
                p.push(("ord_type", "price".into()));
            }
            None => {
                p.push(("volume", vol));
                p.push(("ord_type", "market".into()));
            }
        }
        p.push(("identifier", format!("hft{local}")));
        let v = self.signed(reqwest::Method::POST, "/orders", &p).await?;
        let mut st = order_state(&v);
        if st.id.is_empty() {
            return Err(format!("Upbit: нет uuid в ответе: {v}"));
        }
        st.status = if st.status == "FILLED" {
            st.status
        } else {
            "NEW".into()
        };
        Ok(st)
    }

    pub async fn query(&self, _symbol: &str, id: &str) -> Result<OrderState, String> {
        Ok(order_state(
            &self
                .signed(reqwest::Method::GET, "/order", &[("uuid", id.to_owned())])
                .await?,
        ))
    }

    pub async fn cancel(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let _ = self
            .signed(
                reqwest::Method::DELETE,
                "/order",
                &[("uuid", id.to_owned())],
            )
            .await;
        self.query(symbol, id).await
    }

    pub async fn balances(&self) -> Result<BTreeMap<String, f64>, String> {
        let v = self.signed(reqwest::Method::GET, "/accounts", &[]).await?;
        Ok(v.as_array()
            .into_iter()
            .flatten()
            .filter_map(|b| Some((b["currency"].as_str()?.to_uppercase(), num(&b["balance"]))))
            .filter(|(_, f)| *f > 0.0)
            .collect())
    }

    pub fn hello(&self) -> Vec<String> {
        vec![json!([{"ticket": format!("hft-{}", now_ms())}, {"type": "myOrder"}, {"type": "myAsset"}, {"format": "DEFAULT"}]).to_string()]
    }

    pub fn stream_parse(&self, text: &str) -> StreamMsg {
        let Ok(v) = serde_json::from_str::<Value>(text) else {
            return StreamMsg::Ignore;
        };
        if v.get("error").is_some() {
            return StreamMsg::Fail(format!("Upbit: {text}"));
        }
        if v["status"] == "UP" {
            return StreamMsg::Ready;
        }
        match v["type"].as_str() {
            Some("myOrder") => {
                let Some(local) = v["identifier"].as_str().and_then(local_of) else {
                    return StreamMsg::Ignore;
                };
                // в потоке state «trade» — очередная сделка; executed_funds есть не всегда
                let st = order_state(&v);
                StreamMsg::Apply(Box::new(move |l: &mut Live| {
                    l.stream_events += 1;
                    l.on_state(local, &st);
                }))
            }
            Some("myAsset") => {
                let a: Vec<(String, f64)> = v["assets"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|x| {
                        Some((x["currency"].as_str()?.to_uppercase(), num(&x["balance"])))
                    })
                    .collect();
                StreamMsg::Apply(Box::new(move |l: &mut Live| {
                    l.stream_events += 1;
                    for (c, f) in &a {
                        l.ledger.set_balance(c, *f);
                    }
                }))
            }
            _ => StreamMsg::Ignore,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwt_and_orders() {
        let t = jwt("ak", "sk", "market=USDT-BTC&side=bid");
        let parts: Vec<&str> = t.split('.').collect();
        assert_eq!(parts.len(), 3);
        assert!(!t.contains('=') && !t.contains('+') && !t.contains('/'));
        let mut mac = Hmac::<Sha256>::new_from_slice(b"sk").unwrap();
        mac.update(format!("{}.{}", parts[0], parts[1]).as_bytes());
        assert_eq!(parts[2], b64url(&mac.finalize().into_bytes()));
        assert_eq!(market("BTCUSDT"), "USDT-BTC");
        let st = order_state(
            &json!({"uuid": "u", "state": "cancel", "executed_volume": "0.5", "trades": [{"funds": "20"}, {"funds": "30"}]}),
        );
        assert_eq!(
            (st.status.as_str(), st.executed, st.quote),
            ("CANCELED", 0.5, 50.0)
        );
        let c = Client::new(
            "http://x",
            Keys {
                key: "k".into(),
                secret: "s".into(),
            },
            &[],
        );
        c.ticks.lock().unwrap().insert("USDT-BTC".into(), 0.01);
        assert_eq!(c.price("USDT-BTC", 60123.456), "60123.46");
        assert!(matches!(
            c.stream_parse(r#"{"status":"UP"}"#),
            StreamMsg::Ready
        ));
    }
}
