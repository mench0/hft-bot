//! Подписанный REST BingX и приватный поток, как в Java BingxRestClient: спот — /openApi/spot/v1, перпы USDT-M —
//! /openApi/swap/v2 (односторонний режим positionSide=BOTH, объём в монетах). Ключ — заголовок X-BX-APIKEY, подпись —
//! hex(HMAC-SHA256) строки параметров по алфавиту с timestamp; параметры и signature — в строке запроса для любого
//! метода. Поток — по listenKey (кадры gzip): спот подписывается на spot.executionReport, у перпов
//! ORDER_TRADE_UPDATE и ACCOUNT_UPDATE приходят сами. Пинг сервера Ping → Pong.

use crate::live::{Live, StreamMsg};
use crate::paper::{Side, split};
use crate::signed::{Keys, OrderState, dec, sign};
use crate::util::now_ms;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

pub const REST: &str = "https://open-api.bingx.com";

/// Адрес потока без ключа; BINGX_STREAM_URL — свой.
pub fn stream_base(perp: bool, testnet: bool) -> String {
    crate::util::env("BINGX_STREAM_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| {
            match (perp, testnet) {
                (true, true) => "wss://vst-open-api-ws.bingx.com/swap-market",
                (true, false) => "wss://open-api-swap.bingx.com/swap-market",
                _ => "wss://open-api-ws.bingx.com/market",
            }
            .into()
        })
}

/// Статус BingX в общий: PENDING — NEW, FAILED — CANCELED.
pub fn status(s: &str) -> String {
    match s.to_uppercase().as_str() {
        "PARTIALLY_FILLED" => "PARTIALLY_FILLED",
        "FILLED" => "FILLED",
        "CANCELED" | "CANCELLED" | "FAILED" => "CANCELED",
        "REJECTED" => "REJECTED",
        "EXPIRED" => "EXPIRED",
        _ => "NEW",
    }
    .to_owned()
}

fn num(v: &Value) -> f64 {
    v.as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| v.as_f64())
        .unwrap_or(0.0)
}

fn id_of(v: &Value) -> String {
    v.as_str()
        .map(str::to_owned)
        .or_else(|| v.as_u64().map(|x| x.to_string()))
        .or_else(|| v.as_i64().map(|x| x.to_string()))
        .unwrap_or_default()
}

fn local_of(c: &Value) -> Option<u64> {
    c.as_str()?.strip_prefix("hft")?.parse().ok()
}

/// BTCUSDT → BTC-USDT.
pub fn sym(s: &str) -> String {
    split(s).map_or_else(|| s.to_uppercase(), |(b, q)| format!("{b}-{q}"))
}

/// Состояние ордера из ответа REST.
pub fn order_state(o: &Value) -> OrderState {
    let exec = num(&o["executedQty"]);
    let mut quote = num(&o["cummulativeQuoteQty"]).max(num(&o["cumQuote"]));
    if quote == 0.0 {
        quote = exec * num(&o["avgPrice"]);
    }
    OrderState {
        id: id_of(&o["orderId"]),
        status: status(o["status"].as_str().unwrap_or("")),
        executed: exec,
        quote,
    }
}

pub struct Client {
    base: String,
    pub perp: bool,
    testnet: bool,
    keys: Keys,
    offset: AtomicI64,
    key: Mutex<String>,
    http: reqwest::Client,
}

impl Client {
    pub fn new(base: &str, keys: Keys, perp: bool, testnet: bool) -> Client {
        Client {
            base: base.trim_end_matches('/').to_owned(),
            perp,
            testnet,
            keys,
            offset: AtomicI64::new(0),
            key: Mutex::default(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP-клиент"),
        }
    }

    fn api(&self) -> &'static str {
        if self.perp {
            "/openApi/swap/v2"
        } else {
            "/openApi/spot/v1"
        }
    }

    pub async fn sync_time(&self) -> Result<(), String> {
        let v: Value = self
            .http
            .get(format!("{}{}/server/time", self.base, self.api()))
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        if let Some(t) = v["data"]["serverTime"].as_i64() {
            self.offset.store(t - now_ms() as i64, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Строка параметров по алфавиту (с recvWindow и timestamp) — она же подписывается.
    pub fn params_query(params: &[(&str, String)], ts: i64) -> String {
        let mut p: BTreeMap<&str, String> = params.iter().cloned().collect();
        p.insert("recvWindow", "5000".into());
        p.insert("timestamp", ts.to_string());
        p.iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&")
    }

    async fn signed(
        &self,
        method: reqwest::Method,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Value, String> {
        let q = Self::params_query(
            params,
            now_ms() as i64 + self.offset.load(Ordering::Relaxed),
        );
        let url = format!(
            "{}{path}?{q}&signature={}",
            self.base,
            sign(&self.keys.secret, &q)
        );
        let v: Value = self
            .http
            .request(method, url)
            .header("X-BX-APIKEY", &self.keys.key)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        if v["code"].as_i64().unwrap_or(0) != 0 {
            return Err(format!(
                "BingX {}: {}",
                v["code"],
                v["msg"].as_str().unwrap_or("")
            ));
        }
        Ok(v["data"].clone())
    }

    fn unwrap_order(d: Value) -> Value {
        if d.get("order").is_some() {
            d["order"].clone()
        } else {
            d
        }
    }

    pub async fn place(
        &self,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
    ) -> Result<OrderState, String> {
        let mut p = vec![
            ("symbol", sym(symbol)),
            (
                "side",
                if side == Side::Buy { "BUY" } else { "SELL" }.to_owned(),
            ),
            ("quantity", dec(qty)),
        ];
        if self.perp {
            p.push(("positionSide", "BOTH".into()));
            p.push(("clientOrderID", format!("hft{local}")));
        } else {
            p.push(("newClientOrderId", format!("hft{local}")));
        }
        match price {
            Some(px) => {
                p.push(("type", "LIMIT".into()));
                p.push(("timeInForce", "GTC".into()));
                p.push(("price", dec(px)));
            }
            None => p.push(("type", "MARKET".into())),
        }
        let d = Self::unwrap_order(
            self.signed(
                reqwest::Method::POST,
                &format!("{}/trade/order", self.api()),
                &p,
            )
            .await?,
        );
        Ok(order_state(&d))
    }

    pub async fn query(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let p = [("symbol", sym(symbol)), ("orderId", id.to_owned())];
        let path = format!(
            "{}{}",
            self.api(),
            if self.perp {
                "/trade/order"
            } else {
                "/trade/query"
            }
        );
        Ok(order_state(&Self::unwrap_order(
            self.signed(reqwest::Method::GET, &path, &p).await?,
        )))
    }

    pub async fn cancel(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let p = [("symbol", sym(symbol)), ("orderId", id.to_owned())];
        let _ = if self.perp {
            self.signed(
                reqwest::Method::DELETE,
                &format!("{}/trade/order", self.api()),
                &p,
            )
            .await
        } else {
            self.signed(
                reqwest::Method::POST,
                &format!("{}/trade/cancel", self.api()),
                &p,
            )
            .await
        };
        self.query(symbol, id).await
    }

    pub async fn balances(&self) -> Result<BTreeMap<String, f64>, String> {
        if self.perp {
            let d = self
                .signed(
                    reqwest::Method::GET,
                    &format!("{}/user/balance", self.api()),
                    &[],
                )
                .await?;
            let list = if d.get("balance").is_some() {
                d["balance"].clone()
            } else {
                d
            };
            let items: Vec<Value> = match list {
                Value::Array(a) => a,
                other => vec![other],
            };
            return Ok(items
                .iter()
                .filter_map(|b| Some((b["asset"].as_str()?.to_owned(), num(&b["availableMargin"]))))
                .filter(|(_, f)| *f > 0.0)
                .collect());
        }
        let d = self
            .signed(
                reqwest::Method::GET,
                &format!("{}/account/balance", self.api()),
                &[],
            )
            .await?;
        Ok(d["balances"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|b| Some((b["asset"].as_str()?.to_owned(), num(&b["free"]))))
            .filter(|(_, f)| *f > 0.0)
            .collect())
    }

    /// Позиция: positionAmt бывает без знака — сторону даёт positionSide.
    pub fn position(p: &Value) -> Option<(String, f64, f64)> {
        let s = p["symbol"].as_str().or(p["s"].as_str())?.replace('-', "");
        let mut amt = num(if p.get("positionAmt").is_some() {
            &p["positionAmt"]
        } else {
            &p["pa"]
        });
        let side = p["positionSide"]
            .as_str()
            .or(p["ps"].as_str())
            .unwrap_or("");
        if side.eq_ignore_ascii_case("SHORT") && amt > 0.0 {
            amt = -amt;
        }
        let entry = num(if p.get("avgPrice").is_some() {
            &p["avgPrice"]
        } else {
            &p["ep"]
        });
        Some((s, amt, entry))
    }

    pub async fn positions(&self) -> Result<Vec<(String, f64, f64)>, String> {
        let d = self
            .signed(
                reqwest::Method::GET,
                &format!("{}/user/positions", self.api()),
                &[],
            )
            .await?;
        Ok(d.as_array()
            .into_iter()
            .flatten()
            .filter_map(Self::position)
            .collect())
    }

    pub async fn set_leverage(&self, symbol: &str, leverage: f64) -> Result<(), String> {
        let p = [
            ("symbol", sym(symbol)),
            ("side", "BOTH".to_owned()),
            ("leverage", format!("{}", leverage.round() as u64)),
        ];
        self.signed(
            reqwest::Method::POST,
            &format!("{}/trade/leverage", self.api()),
            &p,
        )
        .await
        .map(|_| ())
    }

    // ── приватный поток

    async fn listen_key(&self, method: reqwest::Method, query: &str) -> Result<Value, String> {
        self.http
            .request(
                method,
                format!("{}/openApi/user/auth/userDataStream{query}", self.base),
            )
            .header("X-BX-APIKEY", &self.keys.key)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn stream_url(&self) -> Result<String, String> {
        let r = self.listen_key(reqwest::Method::POST, "").await?;
        let k = r["listenKey"]
            .as_str()
            .or(r["data"]["listenKey"].as_str())
            .filter(|k| !k.is_empty())
            .ok_or_else(|| format!("нет listenKey: {r}"))?;
        *self.key.lock().unwrap() = k.to_owned();
        Ok(format!(
            "{}?listenKey={k}",
            stream_base(self.perp, self.testnet)
        ))
    }

    pub async fn keepalive(&self) -> Result<(), String> {
        let k = self.key.lock().unwrap().clone();
        self.listen_key(reqwest::Method::PUT, &format!("?listenKey={k}"))
            .await
            .map(|_| ())
    }

    pub fn subscription(&self) -> String {
        json!({"id": format!("sub{}", now_ms()), "reqType": "sub", "dataType": "spot.executionReport"}).to_string()
    }

    pub fn stream_parse(&self, text: &str) -> StreamMsg {
        if text.eq_ignore_ascii_case("ping") {
            return StreamMsg::Send("Pong".into());
        }
        let Ok(v) = serde_json::from_str::<Value>(text) else {
            return StreamMsg::Ignore;
        };
        if let Some(p) = v.get("ping") {
            return StreamMsg::Send(json!({"pong": p, "time": v["time"]}).to_string());
        }
        if v["dataType"]
            .as_str()
            .is_some_and(|d| d.ends_with("executionReport"))
            && v["data"].is_object()
        {
            let e = &v["data"];
            let Some(local) = local_of(&e["c"]) else {
                return StreamMsg::Ignore;
            };
            let st = OrderState {
                id: id_of(&e["i"]),
                status: status(e["X"].as_str().unwrap_or("")),
                executed: num(&e["z"]),
                quote: num(&e["Z"]),
            };
            return StreamMsg::Apply(Box::new(move |l: &mut Live| {
                l.stream_events += 1;
                l.on_state(local, &st);
            }));
        }
        match v["e"].as_str() {
            Some("ORDER_TRADE_UPDATE") => {
                let o = &v["o"];
                let Some(local) = local_of(&o["c"]) else {
                    return StreamMsg::Ignore;
                };
                let exec = num(&o["z"]);
                let st = OrderState {
                    id: id_of(&o["i"]),
                    status: status(o["X"].as_str().unwrap_or("")),
                    executed: exec,
                    quote: exec * num(&o["ap"]),
                };
                StreamMsg::Apply(Box::new(move |l: &mut Live| {
                    l.stream_events += 1;
                    l.on_state(local, &st);
                }))
            }
            Some("ACCOUNT_UPDATE") => {
                let ps: Vec<(String, f64, f64)> = v["a"]["P"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Self::position)
                    .collect();
                StreamMsg::Apply(Box::new(move |l: &mut Live| {
                    l.stream_events += 1;
                    for (s, q, e) in &ps {
                        l.ledger.set_position(s, *q, *e);
                    }
                }))
            }
            _ if v["code"].as_i64().unwrap_or(0) != 0 => {
                StreamMsg::Fail(format!("BingX {}: {}", v["code"], v["msg"]))
            }
            // подтверждение подписки спота
            _ if v["id"].as_str().is_some_and(|i| i.starts_with("sub")) => StreamMsg::Ready,
            _ => StreamMsg::Ignore,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_states_positions_stream() {
        assert_eq!(
            Client::params_query(&[("symbol", "BTC-USDT".into()), ("side", "BUY".into())], 5),
            "recvWindow=5000&side=BUY&symbol=BTC-USDT&timestamp=5"
        );
        let s = order_state(&json!({"orderId": 123, "status": "PENDING", "executedQty": "0"}));
        assert_eq!((s.id.as_str(), s.status.as_str()), ("123", "NEW"));
        let s = order_state(
            &json!({"orderId": "9", "status": "FILLED", "executedQty": "2", "avgPrice": "10"}),
        );
        assert_eq!(s.quote, 20.0);
        assert_eq!(
            Client::position(
                &json!({"symbol": "BTC-USDT", "positionSide": "SHORT", "positionAmt": "0.5", "avgPrice": "100"})
            ),
            Some(("BTCUSDT".into(), -0.5, 100.0))
        );
        let c = Client::new(
            "https://x",
            Keys {
                key: "k".into(),
                secret: "s".into(),
            },
            false,
            false,
        );
        assert!(matches!(c.stream_parse("Ping"), StreamMsg::Send(p) if p == "Pong"));
        assert!(
            matches!(c.stream_parse(r#"{"ping":"a","time":"t"}"#), StreamMsg::Send(p) if p.contains("pong"))
        );
        assert!(matches!(
            c.stream_parse(r#"{"id":"sub1","code":0,"msg":""}"#),
            StreamMsg::Ready
        ));
        assert!(matches!(c.stream_parse(r#"{"dataType":"spot.executionReport","data":{"e":"executionReport","c":"hft2","i":5,"X":"FILLED","z":"1","Z":"10"}}"#), StreamMsg::Apply(_)));
    }
}
