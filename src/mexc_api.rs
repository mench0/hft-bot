//! Подписанный REST MEXC и приватные потоки, как в Java MexcRestClient / MexcFuturesClient.
//! Спот v3 — почти Binance: ключ X-MEXC-APIKEY, подпись hex(HMAC-SHA256) строки запроса, все параметры в query;
//! поток по listenKey (wss://wbs-api.mexc.com/ws?listenKey=), каналы spot@private.orders.v3.api.pb и
//! spot@private.account.v3.api.pb — protobuf: PushDataV3ApiWrapper { symbol 3; privateOrders 304; privateAccount 307 },
//! PrivateOrdersV3Api { id 1; clientId 2; quantity 4; avgPrice 6; tradeType 8; cumulativeQuantity 13;
//! cumulativeAmount 14; status 15 (1 новый, 2 исполнен, 3 частично, 4 и 5 отменён) },
//! PrivateAccountV3Api { vcoinName 1; balanceAmount 3; frozenAmount 5 }.
//! Перпы (contract.mexc.com): BTC_USDT, vol — контракты (× contractSize), сторона задаёт и направление позиции
//! (1 — открыть лонг, 2 — закрыть шорт, 3 — открыть шорт, 4 — закрыть лонг), маржа кросс (openType 2), плечо в ордере;
//! подпись — заголовки ApiKey, Request-Time, Signature = hex(HMAC-SHA256(apiKey + время + параметры)), параметры —
//! тело JSON у POST и отсортированная строка запроса у GET; поток wss://contract.mexc.com/edge после login присылает
//! push.personal.order / position / asset. MEXC выдаёт доступ к фьючерсным ордерам через API отдельно.

use crate::feed::mexc::Pb;
use crate::live::{Live, StreamMsg};
use crate::paper::{Side, split};
use crate::signed::{Keys, OrderState, dec, sign};
use crate::util::now_ms;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

pub fn rest_url(perp: bool) -> &'static str {
    if perp {
        "https://contract.mexc.com"
    } else {
        "https://api.mexc.com"
    }
}

/// Адрес приватного потока перпов или спота без ключа; MEXC_STREAM_URL — свой.
pub fn stream_base(perp: bool) -> String {
    crate::util::env("MEXC_STREAM_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| {
            if perp {
                "wss://contract.mexc.com/edge".into()
            } else {
                "wss://wbs-api.mexc.com/ws".into()
            }
        })
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

fn s(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

/// Событие спота из protobuf: ордер (свой номер, состояние) и/или баланс (актив, свободно).
pub type PrivateEvent = (Option<(u64, OrderState)>, Option<(String, f64)>);

pub fn parse_private_pb(data: &[u8]) -> Result<PrivateEvent, String> {
    let mut w = Pb { b: data, pos: 0 };
    let (mut order, mut account) = (None, None);
    while !w.done() {
        match w.field()? {
            (304, 2, m, _) => order = Some(m),
            (307, 2, m, _) => account = Some(m),
            _ => {}
        }
    }
    let order = match order {
        None => None,
        Some(m) => {
            let mut r = Pb { b: m, pos: 0 };
            let (mut id, mut client, mut cum_q, mut cum_a, mut status) =
                (String::new(), String::new(), 0.0, 0.0, 0u64);
            while !r.done() {
                match r.field()? {
                    (1, 2, b, _) => id = s(b),
                    (2, 2, b, _) => client = s(b),
                    (13, 2, b, _) => cum_q = s(b).parse().unwrap_or(0.0),
                    (14, 2, b, _) => cum_a = s(b).parse().unwrap_or(0.0),
                    (15, 0, _, v) => status = v,
                    _ => {}
                }
            }
            let st = match status {
                1 => "NEW",
                2 => "FILLED",
                3 => "PARTIALLY_FILLED",
                4 | 5 => "CANCELED",
                _ => "NEW",
            };
            local_of(&client).map(|l| {
                (
                    l,
                    OrderState {
                        id,
                        status: st.into(),
                        executed: cum_q,
                        quote: cum_a,
                    },
                )
            })
        }
    };
    let account = match account {
        None => None,
        Some(m) => {
            let mut r = Pb { b: m, pos: 0 };
            let (mut asset, mut free) = (String::new(), 0.0);
            while !r.done() {
                match r.field()? {
                    (1, 2, b, _) => asset = s(b),
                    (3, 2, b, _) => free = s(b).parse().unwrap_or(0.0),
                    _ => {}
                }
            }
            (!asset.is_empty()).then_some((asset, free))
        }
    };
    Ok((order, account))
}

pub struct Client {
    base: String,
    pub perp: bool,
    keys: Keys,
    leverage: Mutex<f64>,
    /// Перпы: контракт → монет в контракте; позиции по ногам (лонг, шорт) — для выбора стороны ордера.
    sizes: Mutex<HashMap<String, f64>>,
    legs: Mutex<HashMap<String, [f64; 4]>>,
    listen_key: Mutex<String>,
    http: reqwest::Client,
}

impl Client {
    pub fn new(base: &str, keys: Keys, perp: bool) -> Client {
        Client {
            base: base.trim_end_matches('/').to_owned(),
            perp,
            keys,
            leverage: Mutex::new(1.0),
            sizes: Mutex::default(),
            legs: Mutex::default(),
            listen_key: Mutex::default(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP-клиент"),
        }
    }

    fn contract(symbol: &str) -> String {
        split(symbol).map_or_else(|| symbol.to_uppercase(), |(b, q)| format!("{b}_{q}"))
    }

    fn m(&self, c: &str) -> f64 {
        self.sizes.lock().unwrap().get(c).copied().unwrap_or(1.0)
    }

    pub fn map_sizes(&self, v: &Value) {
        let mut m = self.sizes.lock().unwrap();
        for c in v["data"].as_array().into_iter().flatten() {
            if let Some(n) = c["symbol"].as_str() {
                m.insert(n.to_owned(), num(&c["contractSize"]).max(1e-18));
            }
        }
    }

    pub async fn sync_time(&self) -> Result<(), String> {
        if self.perp {
            let v: Value = self
                .http
                .get(format!("{}/api/v1/contract/detail", self.base))
                .send()
                .await
                .map_err(|e| e.to_string())?
                .json()
                .await
                .map_err(|e| e.to_string())?;
            self.map_sizes(&v);
        }
        Ok(())
    }

    // ── спот

    async fn spot(
        &self,
        method: reqwest::Method,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Value, String> {
        let mut q: Vec<String> = params.iter().map(|(k, v)| format!("{k}={v}")).collect();
        q.push("recvWindow=5000".into());
        q.push(format!("timestamp={}", now_ms()));
        let q = q.join("&");
        let url = format!(
            "{}{path}?{q}&signature={}",
            self.base,
            sign(&self.keys.secret, &q)
        );
        let r = self
            .http
            .request(method, url)
            .header("X-MEXC-APIKEY", &self.keys.key)
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let st = r.status();
        let v: Value = r.json().await.map_err(|e| format!("HTTP {st}: {e}"))?;
        if !st.is_success() || v["code"].as_i64().is_some_and(|c| c != 0 && c != 200) {
            return Err(format!(
                "MEXC {}: {}",
                v["code"],
                v["msg"].as_str().unwrap_or("")
            ));
        }
        Ok(v)
    }

    fn spot_state(o: &Value) -> OrderState {
        let status = match o["status"].as_str().unwrap_or("") {
            "PARTIALLY_FILLED" => "PARTIALLY_FILLED",
            "FILLED" => "FILLED",
            "CANCELED" | "PARTIALLY_CANCELED" => "CANCELED",
            "REJECTED" => "REJECTED",
            _ => "NEW",
        };
        OrderState {
            id: o["orderId"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| o["orderId"].to_string()),
            status: status.into(),
            executed: num(&o["executedQty"]),
            quote: num(&o["cummulativeQuoteQty"]),
        }
    }

    // ── перпы

    async fn fut(
        &self,
        method: reqwest::Method,
        path: &str,
        query: &str,
        body: Option<Value>,
    ) -> Result<Value, String> {
        let ts = now_ms().to_string();
        let b = body.map(|b| b.to_string()).unwrap_or_default();
        let params = if method == reqwest::Method::GET {
            query.to_owned()
        } else {
            b.clone()
        };
        let sig = sign(&self.keys.secret, &format!("{}{ts}{params}", self.keys.key));
        let url = if query.is_empty() {
            format!("{}{path}", self.base)
        } else {
            format!("{}{path}?{query}", self.base)
        };
        let mut r = self
            .http
            .request(method, url)
            .header("ApiKey", &self.keys.key)
            .header("Request-Time", ts)
            .header("Signature", sig)
            .header("Content-Type", "application/json");
        if !b.is_empty() {
            r = r.body(b);
        }
        let v: Value = r
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        if v["success"] != true {
            return Err(format!(
                "MEXC futures {}: {}",
                v["code"],
                v["message"].as_str().or(v["msg"].as_str()).unwrap_or("")
            ));
        }
        Ok(v["data"].clone())
    }

    fn fut_state(&self, o: &Value) -> OrderState {
        let m = self.m(o["symbol"].as_str().unwrap_or(""));
        let deal = num(&o["dealVol"]) * m;
        let status = match o["state"].as_i64().unwrap_or(2) {
            3 => "FILLED",
            4 => "CANCELED",
            5 => "REJECTED",
            _ if deal > 0.0 => "PARTIALLY_FILLED",
            _ => "NEW",
        };
        OrderState {
            id: o["orderId"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| o["orderId"].to_string()),
            status: status.into(),
            executed: deal,
            quote: deal * num(&o["dealAvgPrice"]),
        }
    }

    /// Нога позиции (хедж-режим MEXC): positionType 1 — лонг, 2 — шорт; state 3 — закрыта. Возвращает итог по символу.
    fn leg(&self, p: &Value) -> Option<(String, f64, f64)> {
        let c = p["symbol"].as_str()?;
        let m = self.m(c);
        let vol = if p["state"].as_i64() == Some(3) {
            0.0
        } else {
            num(&p["holdVol"]) * m
        };
        let mut legs = self.legs.lock().unwrap();
        let l = legs.entry(c.to_owned()).or_default();
        if p["positionType"].as_i64() == Some(2) {
            (l[2], l[3]) = (vol, num(&p["holdAvgPrice"]))
        } else {
            (l[0], l[1]) = (vol, num(&p["holdAvgPrice"]))
        }
        let net = l[0] - l[2];
        let avg = if l[0] + l[2] > 0.0 {
            (l[0] * l[1] + l[2] * l[3]) / (l[0] + l[2])
        } else {
            0.0
        };
        Some((c.replace('_', ""), net, avg))
    }

    // ── общее

    pub async fn place(
        &self,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
    ) -> Result<OrderState, String> {
        if self.perp {
            let c = Self::contract(symbol);
            let vol = ((qty / self.m(&c)) + 1e-9).floor() as i64;
            if vol <= 0 {
                return Err(format!("MEXC: объём {qty} меньше одного контракта {c}"));
            }
            // встречная нога есть — закрываем её (2 / 4), иначе открываем (1 / 3)
            let l = self
                .legs
                .lock()
                .unwrap()
                .get(&c)
                .copied()
                .unwrap_or_default();
            let s = match side {
                Side::Buy if l[2] > 0.0 => 2,
                Side::Buy => 1,
                Side::Sell if l[0] > 0.0 => 4,
                Side::Sell => 3,
            };
            let mut b = json!({"symbol": c, "vol": vol, "side": s, "type": if price.is_some() { 1 } else { 5 }, "openType": 2,
                "leverage": (*self.leverage.lock().unwrap()).max(1.0).round() as u64, "externalOid": format!("hft{local}")});
            if let Some(px) = price {
                b["price"] = json!(px);
            }
            let d = self
                .fut(
                    reqwest::Method::POST,
                    "/api/v1/private/order/submit",
                    "",
                    Some(b),
                )
                .await?;
            let id = d
                .as_str()
                .map(str::to_owned)
                .or_else(|| d.as_i64().map(|x| x.to_string()))
                .or_else(|| d["orderId"].as_str().map(str::to_owned))
                .unwrap_or_default();
            return Ok(OrderState {
                id,
                status: "NEW".into(),
                executed: 0.0,
                quote: 0.0,
            });
        }
        let mut p = vec![
            ("symbol", symbol.to_uppercase()),
            (
                "side",
                if side == Side::Buy { "BUY" } else { "SELL" }.to_owned(),
            ),
            (
                "type",
                if price.is_some() { "LIMIT" } else { "MARKET" }.to_owned(),
            ),
            ("quantity", dec(qty)),
            ("newClientOrderId", format!("hft{local}")),
        ];
        if let Some(px) = price {
            p.push(("price", dec(px)));
        }
        let v = self
            .spot(reqwest::Method::POST, "/api/v3/order", &p)
            .await?;
        Ok(OrderState {
            id: v["orderId"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| v["orderId"].to_string()),
            status: "NEW".into(),
            executed: 0.0,
            quote: 0.0,
        })
    }

    pub async fn query(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        if self.perp {
            let d = self
                .fut(
                    reqwest::Method::GET,
                    &format!("/api/v1/private/order/get/{id}"),
                    "",
                    None,
                )
                .await?;
            return Ok(self.fut_state(&d));
        }
        Ok(Self::spot_state(
            &self
                .spot(
                    reqwest::Method::GET,
                    "/api/v3/order",
                    &[
                        ("symbol", symbol.to_uppercase()),
                        ("orderId", id.to_owned()),
                    ],
                )
                .await?,
        ))
    }

    pub async fn cancel(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let _ = if self.perp {
            self.fut(
                reqwest::Method::POST,
                "/api/v1/private/order/cancel",
                "",
                Some(json!([id])),
            )
            .await
        } else {
            self.spot(
                reqwest::Method::DELETE,
                "/api/v3/order",
                &[
                    ("symbol", symbol.to_uppercase()),
                    ("orderId", id.to_owned()),
                ],
            )
            .await
        };
        self.query(symbol, id).await
    }

    pub async fn balances(&self) -> Result<BTreeMap<String, f64>, String> {
        if self.perp {
            let d = self
                .fut(
                    reqwest::Method::GET,
                    "/api/v1/private/account/asset/USDT",
                    "",
                    None,
                )
                .await?;
            return Ok(BTreeMap::from([(
                "USDT".to_owned(),
                num(&d["availableBalance"]),
            )]));
        }
        let v = self
            .spot(reqwest::Method::GET, "/api/v3/account", &[])
            .await?;
        Ok(v["balances"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|b| Some((b["asset"].as_str()?.to_owned(), num(&b["free"]))))
            .filter(|(_, f)| *f > 0.0)
            .collect())
    }

    pub async fn positions(&self) -> Result<Vec<(String, f64, f64)>, String> {
        let d = self
            .fut(
                reqwest::Method::GET,
                "/api/v1/private/position/open_positions",
                "",
                None,
            )
            .await?;
        // ноги, которых нет в ответе, закрыты
        self.legs.lock().unwrap().clear();
        let mut out: HashMap<String, (f64, f64)> = HashMap::new();
        for p in d.as_array().into_iter().flatten() {
            if let Some((s, q, e)) = self.leg(p) {
                out.insert(s, (q, e));
            }
        }
        Ok(out.into_iter().map(|(s, (q, e))| (s, q, e)).collect())
    }

    pub async fn set_leverage(&self, symbol: &str, leverage: f64) -> Result<(), String> {
        *self.leverage.lock().unwrap() = leverage;
        for pt in [1, 2] {
            let b = json!({"symbol": Self::contract(symbol), "leverage": leverage.round() as u64, "openType": 2, "positionType": pt});
            self.fut(
                reqwest::Method::POST,
                "/api/v1/private/position/change_leverage",
                "",
                Some(b),
            )
            .await?;
        }
        Ok(())
    }

    // ── приватный поток

    pub async fn stream_url(&self) -> Result<String, String> {
        if self.perp {
            return Ok(stream_base(true));
        }
        let v = self
            .spot(reqwest::Method::POST, "/api/v3/userDataStream", &[])
            .await?;
        let k = v["listenKey"]
            .as_str()
            .filter(|k| !k.is_empty())
            .ok_or_else(|| format!("нет listenKey: {v}"))?;
        *self.listen_key.lock().unwrap() = k.to_owned();
        Ok(format!("{}?listenKey={k}", stream_base(false)))
    }

    pub async fn keepalive(&self) -> Result<(), String> {
        if self.perp {
            return Ok(());
        }
        let k = self.listen_key.lock().unwrap().clone();
        self.spot(
            reqwest::Method::PUT,
            "/api/v3/userDataStream",
            &[("listenKey", k)],
        )
        .await
        .map(|_| ())
    }

    pub fn hello(&self) -> Vec<String> {
        if self.perp {
            let ts = now_ms().to_string();
            vec![
                json!({"method": "login", "param": {"apiKey": self.keys.key, "reqTime": ts,
                "signature": sign(&self.keys.secret, &format!("{}{ts}", self.keys.key))}})
                .to_string(),
            ]
        } else {
            vec![r#"{"method":"SUBSCRIPTION","params":["spot@private.orders.v3.api.pb","spot@private.account.v3.api.pb"]}"#.into()]
        }
    }

    pub fn ping(&self) -> String {
        if self.perp {
            r#"{"method":"ping"}"#.into()
        } else {
            r#"{"method":"PING"}"#.into()
        }
    }

    /// Бинарный кадр спота (protobuf).
    pub fn stream_binary(&self, data: &[u8]) -> StreamMsg {
        match parse_private_pb(data) {
            Err(e) => StreamMsg::Fail(e),
            Ok((None, None)) => StreamMsg::Ignore,
            Ok((order, account)) => StreamMsg::Apply(Box::new(move |l: &mut Live| {
                l.stream_events += 1;
                if let Some((local, st)) = &order {
                    l.on_state(*local, st);
                }
                if let Some((a, f)) = &account {
                    l.ledger.set_balance(a, *f);
                }
            })),
        }
    }

    pub fn stream_parse(&self, text: &str) -> StreamMsg {
        if text.contains("Not Subscribed") || text.contains("Blocked") {
            return StreamMsg::Fail(format!("MEXC: подписка отклонена: {text}"));
        }
        let Ok(v) = serde_json::from_str::<Value>(text) else {
            return StreamMsg::Ignore;
        };
        if !self.perp {
            return match v["code"].as_i64() {
                Some(0)
                    if v["msg"]
                        .as_str()
                        .is_some_and(|m| m.contains("spot@private")) =>
                {
                    StreamMsg::Ready
                }
                Some(c) if c != 0 => StreamMsg::Fail(format!("MEXC: {text}")),
                _ => StreamMsg::Ignore,
            };
        }
        let d = v["data"].clone();
        match v["channel"].as_str().unwrap_or("") {
            "rs.login" if d == "success" => StreamMsg::Ready,
            "rs.login" | "rs.error" => StreamMsg::Fail(format!("MEXC futures: {d}")),
            "push.personal.order" => {
                let Some(local) = d["externalOid"].as_str().and_then(local_of) else {
                    return StreamMsg::Ignore;
                };
                let st = self.fut_state(&d);
                StreamMsg::Apply(Box::new(move |l: &mut Live| {
                    l.stream_events += 1;
                    l.on_state(local, &st);
                }))
            }
            "push.personal.position" => match self.leg(&d) {
                Some((s, q, e)) => StreamMsg::Apply(Box::new(move |l: &mut Live| {
                    l.stream_events += 1;
                    l.ledger.set_position(&s, q, e);
                })),
                None => StreamMsg::Ignore,
            },
            "push.personal.asset" => {
                let (c, a) = (
                    d["currency"].as_str().unwrap_or("USDT").to_owned(),
                    num(&d["availableBalance"]),
                );
                StreamMsg::Apply(Box::new(move |l: &mut Live| {
                    l.stream_events += 1;
                    l.ledger.set_balance(&c, a);
                }))
            }
            _ => StreamMsg::Ignore,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn varint(mut v: u64, out: &mut Vec<u8>) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                return;
            }
            out.push(b | 0x80);
        }
    }

    fn bytes(field: u64, data: &[u8], out: &mut Vec<u8>) {
        varint(field << 3 | 2, out);
        varint(data.len() as u64, out);
        out.extend_from_slice(data);
    }

    #[test]
    fn private_protobuf() {
        let mut order = Vec::new();
        bytes(1, b"C02__1", &mut order);
        bytes(2, b"hft5", &mut order);
        bytes(13, b"0.5", &mut order);
        bytes(14, b"50", &mut order);
        varint(15 << 3, &mut order);
        varint(2, &mut order);
        let mut acc = Vec::new();
        bytes(1, b"USDT", &mut acc);
        bytes(3, b"950", &mut acc);
        let mut msg = Vec::new();
        bytes(3, b"BTCUSDT", &mut msg);
        bytes(304, &order, &mut msg);
        bytes(307, &acc, &mut msg);
        let (o, a) = parse_private_pb(&msg).unwrap();
        assert_eq!(
            o,
            Some((
                5,
                OrderState {
                    id: "C02__1".into(),
                    status: "FILLED".into(),
                    executed: 0.5,
                    quote: 50.0
                }
            ))
        );
        assert_eq!(a, Some(("USDT".into(), 950.0)));
    }

    #[test]
    fn futures_legs_and_sides() {
        let c = Client::new(
            "https://x",
            Keys {
                key: "k".into(),
                secret: "s".into(),
            },
            true,
        );
        c.map_sizes(&json!({"data": [{"symbol": "BTC_USDT", "contractSize": 0.0001}]}));
        assert_eq!(c.leg(&json!({"symbol": "BTC_USDT", "holdVol": 5000, "positionType": 2, "holdAvgPrice": 100, "state": 1})), Some(("BTCUSDT".into(), -0.5, 100.0)));
        assert_eq!(c.leg(&json!({"symbol": "BTC_USDT", "holdVol": 2000, "positionType": 1, "holdAvgPrice": 110, "state": 1})).unwrap().1, -0.3);
        let st = c.fut_state(&json!({"orderId": "77", "symbol": "BTC_USDT", "state": 3, "dealVol": 5000, "dealAvgPrice": 100}));
        assert_eq!(
            (st.status.as_str(), st.executed, st.quote),
            ("FILLED", 0.5, 50.0)
        );
        assert!(matches!(
            c.stream_parse(r#"{"channel":"rs.login","data":"success"}"#),
            StreamMsg::Ready
        ));
    }
}
