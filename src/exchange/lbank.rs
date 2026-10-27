//! Реальные ордера LBank, спот (как в Java LbankRestClient). Подписанные запросы — POST формой: в теле параметры,
//! api_key и sign; timestamp (мс), signature_method=HmacSHA256 и echostr (35 букв и цифр) — ещё и в заголовках.
//! Подпись: параметры с api_key, timestamp, signature_method, echostr по алфавиту «k=v&…» → MD5 (hex, заглавные) →
//! HMAC-SHA256 секретом (hex). Пара — btc_usdt; рыночная покупка — на сумму в котировке (price = объём × опорная × 1.005).
//! Поток: ключ подписки (get_key.do, живёт 60 мин, продлевается refresh_key.do), wss://www.lbkex.net/ws/V2/,
//! подписки orderUpdate (свой номер — custom_id → customerID) и assetUpdate; на ping сервера — pong.

use crate::exchange::binance::{Keys, OrderState, dec};
use crate::infra::util::now_ms;
use crate::trading::live::{Live, StreamMsg};
use crate::trading::paper::{Side, split};
use md5::{Digest, Md5};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

pub const REST: &str = "https://api.lbkex.com";
const MARKET_BUY_SLIPPAGE: f64 = 0.005;

pub fn stream_url() -> String {
    crate::infra::util::env("LBANK_STREAM_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "wss://www.lbkex.net/ws/V2/".into())
}

pub fn pair(symbol: &str) -> String {
    split(symbol).map_or_else(
        || symbol.to_lowercase(),
        |(b, q)| format!("{}_{}", b.to_lowercase(), q.to_lowercase()),
    )
}

/// Подпись: параметры (вместе со служебными) по алфавиту → MD5 заглавными → HMAC-SHA256 (hex).
pub fn sign(secret: &str, params: &BTreeMap<String, String>) -> String {
    let s: Vec<String> = params.iter().map(|(k, v)| format!("{k}={v}")).collect();
    let md5 = hex::encode_upper(Md5::digest(s.join("&").as_bytes()));
    crate::exchange::binance::sign(secret, &md5)
}

fn echostr() -> String {
    const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut x = now_ms() ^ (std::process::id() as u64).rotate_left(32) ^ 0x9e37_79b9_7f4a_7c15;
    (0..35)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            A[(x % A.len() as u64) as usize] as char
        })
        .collect()
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

/// Ордер LBank: status −1 отменён, 0 в стакане, 1 частично, 2 исполнен, 3 частично и отменён, 4 отменяется.
pub fn order_state(d: &Value) -> OrderState {
    let exec = if d.get("executedQty").is_some() {
        num(&d["executedQty"])
    } else {
        num(&d["deal_amount"])
    };
    let quote = match num(&d["cummulativeQuoteQty"]) {
        q if q > 0.0 => q,
        _ => {
            exec * num(if d.get("avg_price").is_some() {
                &d["avg_price"]
            } else {
                &d["avgPrice"]
            })
        }
    };
    let status = match d["status"]
        .as_i64()
        .unwrap_or_else(|| num(&d["status"]) as i64)
    {
        -1 | 3 => "CANCELED",
        1 | 4 if exec > 0.0 => "PARTIALLY_FILLED",
        2 => "FILLED",
        _ => "NEW",
    };
    let id = d["orderId"]
        .as_str()
        .or(d["order_id"].as_str())
        .unwrap_or("")
        .to_owned();
    OrderState {
        id,
        status: status.into(),
        executed: exec,
        quote,
    }
}

pub struct Client {
    base: String,
    keys: Keys,
    offset: AtomicI64,
    /// Знаков объёма и цены по паре.
    acc: Mutex<HashMap<String, (u32, u32)>>,
    subscribe_key: Mutex<String>,
    http: reqwest::Client,
}

impl Client {
    /// Адрес лучших цен (свежая цена для выхода, когда сокет стакана лежит).
    pub fn top_url(&self, symbol: &str) -> String {
        format!("{}/v2/depth.do?symbol={}&size=1", self.base, pair(symbol))
    }

    pub fn new(base: &str, keys: Keys) -> Client {
        Client {
            base: base.trim_end_matches('/').to_owned(),
            keys,
            offset: AtomicI64::new(0),
            acc: Mutex::default(),
            subscribe_key: Mutex::default(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP-клиент"),
        }
    }

    async fn public(&self, path: &str) -> Result<Value, String> {
        self.http
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())
    }

    async fn signed(&self, path: &str, params: &[(&str, String)]) -> Result<Value, String> {
        let ts = (now_ms() as i64 + self.offset.load(Ordering::Relaxed)).to_string();
        let echo = echostr();
        let mut p: BTreeMap<String, String> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        p.insert("api_key".into(), self.keys.key.clone());
        let mut to_sign = p.clone();
        to_sign.insert("timestamp".into(), ts.clone());
        to_sign.insert("signature_method".into(), "HmacSHA256".into());
        to_sign.insert("echostr".into(), echo.clone());
        p.insert("sign".into(), sign(&self.keys.secret, &to_sign));
        let r = self
            .http
            .post(format!("{}{path}", self.base))
            .header("timestamp", ts)
            .header("signature_method", "HmacSHA256")
            .header("echostr", echo)
            .form(&p)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let st = r.status();
        let v: Value = r.json().await.map_err(|e| format!("HTTP {st}: {e}"))?;
        let code = v["error_code"].as_i64().unwrap_or(0);
        if !st.is_success() || v["result"] == false || v["result"] == "false" || code != 0 {
            return Err(format!(
                "LBank {code}: {}",
                v["msg"]
                    .as_str()
                    .or(v["message"].as_str())
                    .unwrap_or(&v.to_string())
            ));
        }
        Ok(v)
    }

    pub async fn sync_time(&self) -> Result<(), String> {
        let t0 = now_ms() as i64;
        let v = self.public("/v2/timestamp.do").await?;
        if let Some(s) = v["data"].as_i64() {
            self.offset
                .store(s - (t0 + now_ms() as i64) / 2, Ordering::Relaxed);
        }
        let a = self.public("/v2/accuracy.do").await?;
        let mut m = self.acc.lock().unwrap();
        for x in a["data"].as_array().into_iter().flatten() {
            if let Some(s) = x["symbol"].as_str() {
                m.insert(
                    s.to_owned(),
                    (
                        num(&x["quantityAccuracy"]) as u32,
                        num(&x["priceAccuracy"]) as u32,
                    ),
                );
            }
        }
        Ok(())
    }

    fn round(&self, p: &str, x: f64, price: bool) -> String {
        let (q, pr) = self.acc.lock().unwrap().get(p).copied().unwrap_or((8, 8));
        let m = 10f64.powi(if price { pr } else { q } as i32);
        dec(if price {
            (x * m).round() / m
        } else {
            ((x * m) + 1e-9).floor() / m
        })
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
        let p = pair(symbol);
        let s = if side == Side::Buy { "buy" } else { "sell" };
        let mut q = vec![("symbol", p.clone()), ("custom_id", format!("hft{local}"))];
        match price {
            Some(px) => {
                q.push(("type", s.to_owned()));
                q.push(("price", self.round(&p, px, true)));
                q.push(("amount", self.round(&p, qty, false)));
            }
            None if side == Side::Buy => {
                if ref_px <= 0.0 {
                    return Err("LBank: нет опорной цены для рыночной покупки".into());
                }
                q.push(("type", "buy_market".into()));
                q.push((
                    "price",
                    dec((qty * ref_px * (1.0 + MARKET_BUY_SLIPPAGE) * 1e8).round() / 1e8),
                ));
            }
            None => {
                q.push(("type", "sell_market".into()));
                q.push(("amount", self.round(&p, qty, false)));
            }
        }
        let v = self.signed("/v2/supplement/create_order.do", &q).await?;
        let d = &v["data"];
        let id = d["order_id"]
            .as_str()
            .or(d["orderId"].as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("LBank: нет order_id: {v}"))?;
        Ok(OrderState {
            id: id.into(),
            status: "NEW".into(),
            executed: 0.0,
            quote: 0.0,
        })
    }

    pub async fn query(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let v = self
            .signed(
                "/v2/supplement/orders_info.do",
                &[("symbol", pair(symbol)), ("orderId", id.to_owned())],
            )
            .await?;
        let mut d = v["data"].clone();
        if d.get("orders").is_some() {
            d = d["orders"].clone();
        }
        if d.is_array() {
            d = d[0].clone();
        }
        if d.is_null() {
            return Err(format!("LBank: ордер {id} не найден"));
        }
        let mut st = order_state(&d);
        st.id = id.into();
        Ok(st)
    }

    pub async fn cancel(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let _ = self
            .signed(
                "/v2/supplement/cancel_order.do",
                &[("symbol", pair(symbol)), ("orderId", id.to_owned())],
            )
            .await;
        self.query(symbol, id).await
    }

    pub async fn balances(&self) -> Result<BTreeMap<String, f64>, String> {
        let v = self
            .signed("/v2/supplement/user_info_account.do", &[])
            .await?;
        Ok(v["data"]["balances"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|b| Some((b["asset"].as_str()?.to_uppercase(), num(&b["free"]))))
            .filter(|(_, f)| *f > 0.0)
            .collect())
    }

    pub async fn stream_url(&self) -> Result<String, String> {
        let v = self.signed("/v2/subscribe/get_key.do", &[]).await?;
        let k = v["data"]
            .as_str()
            .filter(|k| !k.is_empty())
            .ok_or_else(|| format!("LBank: нет ключа подписки: {v}"))?;
        *self.subscribe_key.lock().unwrap() = k.to_owned();
        Ok(stream_url())
    }

    pub async fn keepalive(&self) -> Result<(), String> {
        let k = self.subscribe_key.lock().unwrap().clone();
        self.signed("/v2/subscribe/refresh_key.do", &[("subscribeKey", k)])
            .await
            .map(|_| ())
    }

    pub fn hello(&self) -> Vec<String> {
        let k = self.subscribe_key.lock().unwrap().clone();
        vec![
            json!({"action": "subscribe", "subscribe": "orderUpdate", "subscribeKey": k, "pair": "all"}).to_string(),
            json!({"action": "subscribe", "subscribe": "assetUpdate", "subscribeKey": k}).to_string(),
        ]
    }

    pub fn ping(&self) -> String {
        json!({"action": "ping", "ping": format!("hft{}", now_ms())}).to_string()
    }

    /// Ответ сервера (pong на наш пинг, подтверждение подписки) — поток готов; ping сервера — pong.
    pub fn stream_parse(&self, text: &str) -> StreamMsg {
        let Ok(v) = serde_json::from_str::<Value>(text) else {
            return StreamMsg::Ignore;
        };
        if v["status"] == "error" || v.get("error").is_some() {
            return StreamMsg::Fail(format!("LBank: {text}"));
        }
        match (v["action"].as_str(), v["type"].as_str()) {
            (Some("ping"), _) => StreamMsg::SendMany(vec![
                json!({"action": "pong", "pong": v["ping"]}).to_string(),
            ]),
            (Some("pong" | "subscribe"), _) => StreamMsg::Ready,
            (_, Some("orderUpdate")) => {
                let o = &v["orderUpdate"];
                let Some(local) = o["customerID"].as_str().and_then(local_of) else {
                    return StreamMsg::Ignore;
                };
                if o.get("accAmt").is_none() {
                    return StreamMsg::Ignore; // без исполненного объёма — итог дочитается опросом
                }
                let exec = num(&o["accAmt"]);
                let status = match num(&o["orderStatus"]) as i64 {
                    -1 | 3 => "CANCELED",
                    1 => "PARTIALLY_FILLED",
                    2 => "FILLED",
                    _ => "NEW",
                };
                let st = OrderState {
                    id: o["uuid"]
                        .as_str()
                        .or(o["orderId"].as_str())
                        .unwrap_or("")
                        .into(),
                    status: status.into(),
                    executed: exec,
                    quote: exec * num(&o["avgPrice"]),
                };
                StreamMsg::Apply(Box::new(move |l: &mut Live| {
                    l.stream_events += 1;
                    l.on_state(local, &st);
                }))
            }
            (_, Some("assetUpdate")) => {
                let d = &v["data"];
                let (a, f) = (
                    d["assetCode"].as_str().unwrap_or("").to_uppercase(),
                    num(&d["free"]),
                );
                if a.is_empty() {
                    return StreamMsg::Ignore;
                }
                StreamMsg::Apply(Box::new(move |l: &mut Live| {
                    l.stream_events += 1;
                    l.ledger.set_balance(&a, f);
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
    fn signature_and_orders() {
        let p: BTreeMap<String, String> = [
            ("api_key", "k"),
            ("symbol", "btc_usdt"),
            ("timestamp", "1"),
            ("echostr", "e"),
            ("signature_method", "HmacSHA256"),
        ]
        .iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect();
        // MD5("api_key=k&echostr=e&signature_method=HmacSHA256&symbol=btc_usdt&timestamp=1") заглавными → HMAC
        let md5 = hex::encode_upper(Md5::digest(
            b"api_key=k&echostr=e&signature_method=HmacSHA256&symbol=btc_usdt&timestamp=1",
        ));
        assert_eq!(sign("s", &p), crate::exchange::binance::sign("s", &md5));
        assert_eq!(md5.len(), 32);
        assert_eq!(pair("BTCUSDT"), "btc_usdt");
        assert_eq!(echostr().len(), 35);
        let st = order_state(
            &json!({"orderId": "x", "executedQty": "0.5", "cummulativeQuoteQty": "50", "status": 2}),
        );
        assert_eq!(
            (st.status.as_str(), st.executed, st.quote),
            ("FILLED", 0.5, 50.0)
        );
        assert_eq!(
            order_state(&json!({"executedQty": "0.2", "cummulativeQuoteQty": "20", "status": -1}))
                .status,
            "CANCELED"
        );
        let c = Client::new(
            "http://x",
            Keys {
                key: "k".into(),
                secret: "s".into(),
            },
        );
        assert!(
            matches!(c.stream_parse(r#"{"action":"ping","ping":"abc"}"#), StreamMsg::SendMany(m) if m[0].contains("abc"))
        );
    }
}
