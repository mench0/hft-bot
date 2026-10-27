//! Подписанный REST Gate v4 и приватный поток, как в Java GateRestClient / GateFuturesClient.
//! Подпись: hex(HMAC-SHA512(МЕТОД \n /api/v4/путь \n query \n hex(SHA512(тело)) \n секунды)), заголовки KEY, SIGN,
//! Timestamp. Спот — /spot/orders (text "t-hft<номер>"; рыночный ордер — IOC, покупка задаётся суммой в котируемой
//! валюте); перпы — /futures/usdt/orders (size — контракты со знаком, рыночный — price "0" + tif ioc).
//! Поток: подписки с подписью (spot.orders + spot.balances; перпы — futures.orders и futures.positions по номеру
//! пользователя из /futures/usdt/accounts), пинг spot.ping / futures.ping.

use crate::exchange::binance::{Keys, OrderState, dec};
use crate::infra::util::now_ms;
use crate::trading::live::{Live, StreamMsg};
use crate::trading::paper::{Side, split};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::{Digest, Sha512};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

pub fn rest_url(perp: bool, testnet: bool) -> &'static str {
    match (perp, testnet) {
        (_, false) => "https://api.gateio.ws",
        (true, true) => "https://fx-api-testnet.gateio.ws",
        (false, true) => "https://api-testnet.gateapi.io",
    }
}

/// Адрес приватного потока; GATE_WS_PRIVATE_URL — свой.
pub fn private_url(perp: bool, testnet: bool) -> String {
    crate::infra::util::env("GATE_WS_PRIVATE_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| {
            match (perp, testnet) {
                (true, false) => "wss://fx-ws.gateio.ws/v4/ws/usdt",
                (true, true) => "wss://ws-testnet.gate.com/v4/ws/futures/usdt",
                (false, false) => "wss://api.gateio.ws/ws/v4/",
                (false, true) => "wss://ws-testnet.gate.com/v4/ws/spot",
            }
            .into()
        })
}

pub fn hmac512(secret: &str, msg: &str) -> String {
    let mut m =
        Hmac::<Sha512>::new_from_slice(secret.as_bytes()).expect("HMAC принимает ключ любой длины");
    m.update(msg.as_bytes());
    hex::encode(m.finalize().into_bytes())
}

fn num(v: &Value) -> f64 {
    v.as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| v.as_f64())
        .unwrap_or(0.0)
}

/// BTCUSDT → BTC_USDT.
pub fn pair(s: &str) -> String {
    split(s).map_or_else(|| s.to_uppercase(), |(b, q)| format!("{b}_{q}"))
}

/// Свой номер из text "t-hft<номер>".
pub fn local_of(o: &Value) -> Option<u64> {
    o["text"].as_str()?.strip_prefix("t-hft")?.parse().ok()
}

pub struct Client {
    base: String,
    pub perp: bool,
    testnet: bool,
    keys: Keys,
    /// Перпы: контракт (BTC_USDT) → монет в контракте.
    sizes: Mutex<HashMap<String, f64>>,
    /// Перпы: номер пользователя (для подписок).
    user: Mutex<String>,
    /// Спот: пары подписки.
    symbols: Vec<String>,
    http: reqwest::Client,
}

impl Client {
    pub fn new(base: &str, keys: Keys, perp: bool, testnet: bool, symbols: &[String]) -> Client {
        Client {
            base: base.trim_end_matches('/').to_owned(),
            perp,
            testnet,
            keys,
            sizes: Mutex::default(),
            user: Mutex::default(),
            symbols: symbols.to_vec(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP-клиент"),
        }
    }

    pub fn sign(&self, method: &str, path: &str, query: &str, body: &str, ts: u64) -> String {
        let body_hash = hex::encode(Sha512::digest(body.as_bytes()));
        hmac512(
            &self.keys.secret,
            &format!("{method}\n/api/v4{path}\n{query}\n{body_hash}\n{ts}"),
        )
    }

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        query: &str,
        body: Option<Value>,
    ) -> Result<Value, String> {
        let ts = now_ms() / 1000;
        let b = body.map(|b| b.to_string()).unwrap_or_default();
        let sig = self.sign(method.as_str(), path, query, &b, ts);
        let url = if query.is_empty() {
            format!("{}/api/v4{path}", self.base)
        } else {
            format!("{}/api/v4{path}?{query}", self.base)
        };
        let mut r = self
            .http
            .request(method, url)
            .header("KEY", &self.keys.key)
            .header("SIGN", sig)
            .header("Timestamp", ts.to_string())
            .header("Content-Type", "application/json")
            .header("Accept", "application/json");
        if !b.is_empty() {
            r = r.body(b);
        }
        let resp = r.send().await.map_err(|e| e.to_string())?;
        crate::infra::ratelimit::sync_headers("gate", resp.headers());
        let st = resp.status();
        let v: Value = resp.json().await.map_err(|e| format!("HTTP {st}: {e}"))?;
        if !st.is_success() {
            return Err(format!(
                "Gate {}: {}",
                v["label"].as_str().unwrap_or(""),
                v["message"].as_str().unwrap_or("")
            ));
        }
        Ok(v)
    }

    fn m(&self, contract: &str) -> f64 {
        self.sizes
            .lock()
            .unwrap()
            .get(contract)
            .copied()
            .unwrap_or(1.0)
    }

    pub fn map_sizes(&self, v: &Value) {
        let mut m = self.sizes.lock().unwrap();
        for c in v.as_array().into_iter().flatten() {
            if let Some(n) = c["name"].as_str() {
                m.insert(n.to_owned(), num(&c["quanto_multiplier"]).max(1e-18));
            }
        }
    }

    /// Перпы: размеры контрактов и номер пользователя.
    pub async fn sync_time(&self) -> Result<(), String> {
        if !self.perp {
            return Ok(());
        }
        let v: Value = self
            .http
            .get(format!("{}/api/v4/futures/usdt/contracts", self.base))
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        self.map_sizes(&v);
        let a = self
            .call(reqwest::Method::GET, "/futures/usdt/accounts", "", None)
            .await?;
        *self.user.lock().unwrap() = a["user"]
            .as_u64()
            .map(|u| u.to_string())
            .or(a["user"].as_str().map(str::to_owned))
            .unwrap_or_default();
        Ok(())
    }

    /// Состояние ордера из ответа или события.
    pub fn order_state(&self, o: &Value) -> OrderState {
        let id = o["id"]
            .as_str()
            .map(str::to_owned)
            .or(o["id"].as_u64().map(|x| x.to_string()))
            .unwrap_or_default();
        if self.perp {
            let m = self.m(o["contract"].as_str().unwrap_or(""));
            let (size, left) = (num(&o["size"]).abs(), num(&o["left"]).abs());
            let exec = (size - left) * m;
            let finished = o["status"] == "finished"
                || o["finish_as"]
                    .as_str()
                    .is_some_and(|f| f != "_new" && f != "_update" && !f.is_empty());
            let status = if finished {
                if left == 0.0 { "FILLED" } else { "CANCELED" }
            } else if exec > 0.0 {
                "PARTIALLY_FILLED"
            } else {
                "NEW"
            };
            return OrderState {
                id,
                status: status.into(),
                executed: exec,
                quote: exec * num(&o["fill_price"]),
            };
        }
        let (amount, left, total) = (num(&o["amount"]), num(&o["left"]), num(&o["filled_total"]));
        let avg = num(&o["avg_deal_price"]);
        let market_buy = o["type"] == "market" && o["side"] == "buy";
        let exec = match o.get("filled_amount").map(num) {
            Some(x) if x > 0.0 => x,
            _ if market_buy => {
                if avg > 0.0 {
                    total / avg
                } else {
                    0.0
                }
            }
            _ => (amount - left).max(0.0),
        };
        let finish = o["finish_as"].as_str().unwrap_or("");
        let status = match (o["status"].as_str().unwrap_or(""), finish) {
            ("open", _) | ("", "open") => {
                if exec > 0.0 {
                    "PARTIALLY_FILLED"
                } else {
                    "NEW"
                }
            }
            (_, "filled") => "FILLED",
            ("closed", _) => {
                if exec > 0.0 {
                    "PARTIALLY_FILLED"
                } else {
                    "EXPIRED"
                }
            }
            _ => "CANCELED",
        };
        OrderState {
            id,
            status: status.into(),
            executed: exec,
            quote: total,
        }
    }

    fn order_body(
        &self,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
        ref_px: f64,
    ) -> Result<Value, String> {
        let text = format!("t-hft{local}");
        let b = if self.perp {
            let c = pair(symbol);
            let contracts = ((qty / self.m(&c)) + 1e-9).floor() as i64;
            if contracts <= 0 {
                return Err(format!("Gate: объём {qty} меньше одного контракта {c}"));
            }
            let size = if side == Side::Buy {
                contracts
            } else {
                -contracts
            };
            let mut b = json!({"contract": c, "size": size, "text": text});
            match price {
                Some(px) => {
                    b["price"] = json!(dec(px));
                    b["tif"] = json!("gtc");
                }
                None => {
                    b["price"] = json!("0");
                    b["tif"] = json!("ioc");
                }
            }
            b
        } else {
            let mut b = json!({"currency_pair": pair(symbol), "side": if side == Side::Buy { "buy" } else { "sell" }, "text": text, "account": "spot"});
            match price {
                Some(px) => {
                    b["type"] = json!("limit");
                    b["price"] = json!(dec(px));
                    b["amount"] = json!(dec(qty));
                    b["time_in_force"] = json!("gtc");
                }
                None => {
                    b["type"] = json!("market");
                    b["time_in_force"] = json!("ioc");
                    // рыночная покупка — сумма в котируемой валюте (с запасом 0.5% на движение цены)
                    let amount = if side == Side::Buy {
                        qty * ref_px * 1.005
                    } else {
                        qty
                    };
                    if amount <= 0.0 {
                        return Err("Gate: нет цены для рыночной покупки".into());
                    }
                    b["amount"] = json!(dec(amount));
                }
            }
            b
        };
        Ok(b)
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
        let b = self.order_body(local, symbol, side, qty, price, ref_px)?;
        let path = if self.perp {
            "/futures/usdt/orders"
        } else {
            "/spot/orders"
        };
        let r = self.call(reqwest::Method::POST, path, "", Some(b)).await?;
        Ok(self.order_state(&r))
    }

    pub async fn query(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let r = if self.perp {
            self.call(
                reqwest::Method::GET,
                &format!("/futures/usdt/orders/{id}"),
                "",
                None,
            )
            .await?
        } else {
            self.call(
                reqwest::Method::GET,
                &format!("/spot/orders/{id}"),
                &format!("currency_pair={}", pair(symbol)),
                None,
            )
            .await?
        };
        Ok(self.order_state(&r))
    }

    pub async fn cancel(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let _ = if self.perp {
            self.call(
                reqwest::Method::DELETE,
                &format!("/futures/usdt/orders/{id}"),
                "",
                None,
            )
            .await
        } else {
            self.call(
                reqwest::Method::DELETE,
                &format!("/spot/orders/{id}"),
                &format!("currency_pair={}", pair(symbol)),
                None,
            )
            .await
        };
        self.query(symbol, id).await
    }

    pub async fn balances(&self) -> Result<BTreeMap<String, f64>, String> {
        if self.perp {
            let a = self
                .call(reqwest::Method::GET, "/futures/usdt/accounts", "", None)
                .await?;
            let mut m = BTreeMap::new();
            m.insert(
                a["currency"].as_str().unwrap_or("USDT").to_owned(),
                num(&a["available"]),
            );
            return Ok(m);
        }
        let v = self
            .call(reqwest::Method::GET, "/spot/accounts", "", None)
            .await?;
        Ok(v.as_array()
            .into_iter()
            .flatten()
            .filter_map(|b| Some((b["currency"].as_str()?.to_owned(), num(&b["available"]))))
            .filter(|(_, f)| *f > 0.0)
            .collect())
    }

    pub fn position(&self, p: &Value) -> Option<(String, f64, f64)> {
        let c = p["contract"].as_str()?;
        Some((
            c.replace('_', ""),
            num(&p["size"]) * self.m(c),
            num(&p["entry_price"]),
        ))
    }

    pub async fn positions(&self) -> Result<Vec<(String, f64, f64)>, String> {
        let v = self
            .call(reqwest::Method::GET, "/futures/usdt/positions", "", None)
            .await?;
        Ok(v.as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| self.position(p))
            .collect())
    }

    pub async fn set_leverage(&self, symbol: &str, leverage: f64) -> Result<(), String> {
        let path = format!("/futures/usdt/positions/{}/leverage", pair(symbol));
        self.call(
            reqwest::Method::POST,
            &path,
            &format!("leverage={}", leverage.round() as u64),
            None,
        )
        .await
        .map(|_| ())
    }

    // ── приватный поток

    pub async fn stream_url(&self) -> Result<String, String> {
        Ok(private_url(self.perp, self.testnet))
    }

    fn sub(&self, channel: &str, payload: Vec<String>) -> String {
        let ts = now_ms() / 1000;
        json!({"time": ts, "channel": channel, "event": "subscribe", "payload": payload,
            "auth": {"method": "api_key", "KEY": self.keys.key, "SIGN": hmac512(&self.keys.secret, &format!("channel={channel}&event=subscribe&time={ts}"))}})
        .to_string()
    }

    pub fn subscriptions(&self) -> Vec<String> {
        if self.perp {
            let u = self.user.lock().unwrap().clone();
            vec![
                self.sub("futures.orders", vec![u.clone(), "!all".into()]),
                self.sub("futures.positions", vec![u, "!all".into()]),
            ]
        } else {
            let pairs: Vec<String> = self.symbols.iter().map(|s| pair(s)).collect();
            vec![
                self.sub("spot.orders", pairs),
                self.sub("spot.balances", vec![]),
            ]
        }
    }

    fn prefix(&self) -> &'static str {
        if self.perp { "futures" } else { "spot" }
    }

    /// Вход в WS API (spot.login / futures.login) — ордера по тому же сокету, что события счёта.
    pub fn login(&self) -> String {
        let ts = now_ms() / 1000;
        let ch = format!("{}.login", self.prefix());
        json!({"time": ts, "channel": ch, "event": "api", "payload": {"api_key": self.keys.key,
            "signature": hmac512(&self.keys.secret, &format!("api\n{ch}\n\n{ts}")), "timestamp": ts.to_string(), "req_id": "login"}})
        .to_string()
    }

    fn api(&self, id: &str, op: &str, param: Value) -> String {
        json!({"time": now_ms() / 1000, "channel": format!("{}.{op}", self.prefix()), "event": "api",
            "payload": {"req_id": id, "req_param": param}}).to_string()
    }

    /// Ордер по сокету (spot.order_place / futures.order_place).
    #[allow(clippy::too_many_arguments)]
    pub fn ws_place(
        &self,
        id: &str,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
        ref_px: f64,
    ) -> Result<String, String> {
        Ok(self.api(
            id,
            "order_place",
            self.order_body(local, symbol, side, qty, price, ref_px)?,
        ))
    }

    pub fn ws_cancel(&self, id: &str, symbol: &str, order_id: &str) -> String {
        let p = if self.perp {
            json!({"order_id": order_id})
        } else {
            json!({"order_id": order_id, "currency_pair": pair(symbol)})
        };
        self.api(id, "order_cancel", p)
    }

    /// Ордер по своему номеру text (исход запроса по сокету неизвестен).
    pub async fn query_client(&self, symbol: &str, local: u64) -> Result<OrderState, String> {
        let r = if self.perp {
            self.call(
                reqwest::Method::GET,
                &format!("/futures/usdt/orders/t-hft{local}"),
                "",
                None,
            )
            .await?
        } else {
            self.call(
                reqwest::Method::GET,
                &format!("/spot/orders/t-hft{local}"),
                &format!("currency_pair={}", pair(symbol)),
                None,
            )
            .await?
        };
        Ok(self.order_state(&r))
    }

    pub fn ping(&self) -> String {
        json!({"time": now_ms() / 1000, "channel": if self.perp { "futures.ping" } else { "spot.ping" }}).to_string()
    }

    pub fn stream_parse(&self, v: &Value) -> StreamMsg {
        // ответы WS API (вход, подтверждения): ордера разбирает ws_reply
        if v.get("header").is_some() {
            let h = &v["header"];
            if h["channel"].as_str().is_some_and(|c| c.ends_with(".login")) && h["status"] != "200"
            {
                return StreamMsg::Fail(format!(
                    "Gate: вход в WS API отклонён: {}",
                    v["data"]["errs"]
                ));
            }
            return StreamMsg::Ignore;
        }
        if !v["error"].is_null() {
            return StreamMsg::Fail(format!(
                "Gate {}: {}",
                v["error"]["code"], v["error"]["message"]
            ));
        }
        let ch = v["channel"].as_str().unwrap_or("");
        match v["event"].as_str() {
            Some("subscribe") if ch.ends_with(".orders") => StreamMsg::Ready,
            Some("update") => {
                let data: Vec<Value> = v["result"].as_array().cloned().unwrap_or_default();
                match ch {
                    "spot.orders" | "futures.orders" => {
                        let states: Vec<(u64, OrderState)> = data
                            .iter()
                            .filter_map(|o| Some((local_of(o)?, self.order_state(o))))
                            .collect();
                        StreamMsg::Apply(Box::new(move |l: &mut Live| {
                            l.stream_events += 1;
                            for (local, st) in &states {
                                l.on_state(*local, st);
                            }
                        }))
                    }
                    "spot.balances" => {
                        let b: Vec<(String, f64)> = data
                            .iter()
                            .filter_map(|b| {
                                Some((b["currency"].as_str()?.to_owned(), num(&b["available"])))
                            })
                            .collect();
                        StreamMsg::Apply(Box::new(move |l: &mut Live| {
                            l.stream_events += 1;
                            for (a, f) in &b {
                                l.ledger.set_balance(a, *f);
                            }
                        }))
                    }
                    "futures.positions" => {
                        let ps: Vec<(String, f64, f64)> =
                            data.iter().filter_map(|p| self.position(p)).collect();
                        StreamMsg::Apply(Box::new(move |l: &mut Live| {
                            l.stream_events += 1;
                            for (s, q, e) in &ps {
                                l.ledger.set_position(s, *q, *e);
                            }
                        }))
                    }
                    _ => StreamMsg::Ignore,
                }
            }
            _ => StreamMsg::Ignore,
        }
    }
}

/// Стоп на бирже (перпы): reduceOnly-стоп по цене маркировки на всю позицию; None — биржа или рынок не поддерживает.
impl Client {
    /// Ценовой триггер /price_orders по маркировке: рыночное закрытие всей позиции; лонг — при цене ≤ стопа (rule 2).
    pub async fn place_stop(
        &self,
        symbol: &str,
        side: crate::trading::paper::Side,
        price: f64,
    ) -> Result<Option<String>, String> {
        if !self.perp {
            return Ok(None);
        }
        let b = json!({"initial": {"contract": pair(symbol), "size": 0, "price": "0", "tif": "ioc", "close": true, "reduce_only": true},
            "trigger": {"strategy_type": 0, "price_type": 1, "price": dec(price), "rule": if side == crate::trading::paper::Side::Sell { 2 } else { 1 }}});
        let v = self
            .call(
                reqwest::Method::POST,
                "/futures/usdt/price_orders",
                "",
                Some(b),
            )
            .await?;
        let id = v["id"]
            .as_i64()
            .map(|x| x.to_string())
            .or_else(|| v["id"].as_str().map(str::to_owned))
            .ok_or_else(|| format!("Gate: нет id у стопа: {v}"))?;
        Ok(Some(id))
    }

    pub async fn cancel_stop(&self, _symbol: &str, id: &str) -> Result<(), String> {
        self.call(
            reqwest::Method::DELETE,
            &format!("/futures/usdt/price_orders/{id}"),
            "",
            None,
        )
        .await
        .map(|_| ())
    }
}

impl Client {
    /// Сети монеты: открыт ли вывод и депозит (публичный запрос) и комиссии по сетям.
    pub async fn networks(
        &self,
        coin: &str,
    ) -> Result<Vec<crate::exchange::transfer::Net>, String> {
        let q = format!("currency={coin}");
        let chains = self
            .call(reqwest::Method::GET, "/wallet/currency_chains", &q, None)
            .await?;
        let status = self
            .call(reqwest::Method::GET, "/wallet/withdraw_status", &q, None)
            .await?;
        Ok(crate::exchange::transfer::gate_nets(&chains, &status))
    }

    /// Адрес депозита монеты в сети (адрес, тег).
    pub async fn deposit_address(
        &self,
        coin: &str,
        chain: &str,
    ) -> Result<(String, String), String> {
        let r = self
            .call(
                reqwest::Method::GET,
                "/wallet/deposit_address",
                &format!("currency={coin}"),
                None,
            )
            .await?;
        let a = r["multichain_addresses"]
            .as_array()
            .and_then(|a| {
                a.iter()
                    .find(|x| x["chain"] == chain && x["obtain_failed"] != 1)
            })
            .ok_or_else(|| format!("Gate: нет адреса {coin} в сети {chain}"))?;
        Ok((
            a["address"].as_str().unwrap_or("").to_owned(),
            a["payment_id"].as_str().unwrap_or("").to_owned(),
        ))
    }

    /// Вывод со спот-счёта; номер вывода.
    pub async fn withdraw(
        &self,
        coin: &str,
        chain: &str,
        address: &str,
        tag: &str,
        amount: f64,
    ) -> Result<String, String> {
        let mut b =
            json!({"currency": coin, "address": address, "amount": dec(amount), "chain": chain});
        if !tag.is_empty() {
            b["memo"] = json!(tag);
        }
        let r = self
            .call(reqwest::Method::POST, "/withdrawals", "", Some(b))
            .await?;
        Ok(r["id"]
            .as_str()
            .map_or_else(|| r["id"].to_string(), str::to_owned))
    }
}

/// Ответ WS API на ордер или отмену: (req_id, result или ошибка «Gate метка: текст»); подтверждения ack — None.
pub fn ws_reply(v: &Value) -> Option<(String, Result<Value, String>)> {
    if v.get("header").is_none() || v["ack"] == true {
        return None;
    }
    let id = v["request_id"].as_str()?.to_owned();
    let st = v["header"]["status"].as_str().unwrap_or("200");
    Some((
        id,
        if st == "200" {
            Ok(v["data"]["result"].clone())
        } else {
            let e = &v["data"]["errs"];
            Err(format!(
                "Gate {}: {}",
                e["label"].as_str().unwrap_or(st),
                e["message"].as_str().unwrap_or("")
            ))
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(perp: bool) -> Client {
        Client::new(
            "https://x",
            Keys {
                key: "k".into(),
                secret: "s".into(),
            },
            perp,
            false,
            &["BTCUSDT".into()],
        )
    }

    #[test]
    fn spot_states() {
        let c = client(false);
        let s = c.order_state(&json!({"id": "1", "status": "closed", "finish_as": "filled", "type": "market", "side": "buy", "amount": "100", "left": "0", "filled_total": "99", "avg_deal_price": "10"}));
        assert_eq!(
            s,
            OrderState {
                id: "1".into(),
                status: "FILLED".into(),
                executed: 9.9,
                quote: 99.0
            }
        );
        let s = c.order_state(&json!({"id": "2", "status": "open", "type": "limit", "side": "sell", "amount": "5", "left": "3", "filled_total": "20"}));
        assert_eq!((s.status.as_str(), s.executed), ("PARTIALLY_FILLED", 2.0));
        let s = c.order_state(&json!({"id": "3", "status": "cancelled", "type": "limit", "side": "buy", "amount": "5", "left": "5", "filled_total": "0"}));
        assert_eq!(s.status, "CANCELED");
        assert_eq!(local_of(&json!({"text": "t-hft7"})), Some(7));
        // подпись: длина hex SHA512 и детерминированность
        assert_eq!(c.sign("GET", "/spot/accounts", "", "", 1).len(), 128);
        assert_eq!(
            c.sign("GET", "/spot/accounts", "", "", 1),
            c.sign("GET", "/spot/accounts", "", "", 1)
        );
    }

    #[test]
    fn futures_states_and_positions() {
        let c = client(true);
        c.map_sizes(&json!([{"name": "BTC_USDT", "quanto_multiplier": "0.0001"}]));
        let s = c.order_state(&json!({"id": 9, "contract": "BTC_USDT", "size": -50, "left": 0, "status": "finished", "fill_price": "100"}));
        assert_eq!(
            s,
            OrderState {
                id: "9".into(),
                status: "FILLED".into(),
                executed: 0.005,
                quote: 0.5
            }
        );
        assert_eq!(
            c.position(&json!({"contract": "BTC_USDT", "size": -50, "entry_price": "100"})),
            Some(("BTCUSDT".into(), -0.005, 100.0))
        );
        assert!(matches!(c.stream_parse(&json!({"channel": "futures.orders", "event": "subscribe", "result": {"status": "success"}})), StreamMsg::Ready));
        assert!(matches!(c.stream_parse(&json!({"channel": "futures.orders", "event": "subscribe", "error": {"code": 2, "message": "auth"}})), StreamMsg::Fail(_)));
    }
}
