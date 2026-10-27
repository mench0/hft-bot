//! Подписанный REST KuCoin и приватный поток, как в Java KucoinRestClient / KucoinFuturesClient.
//! Подпись: KC-API-SIGN = base64(HMAC-SHA256(время_мс + МЕТОД + путь?query + тело)), KC-API-PASSPHRASE — тоже HMAC
//! (ключ версии 2), нужен KUCOIN_PASSPHRASE. Спот — api.kucoin.com, BTC-USDT; перпы — api-futures.kucoin.com,
//! XBTUSDTM, объём — целые лоты (× multiplier — монеты), маржа CROSS, плечо в каждом ордере.
//! Поток: POST /api/v1/bullet-private → адрес с токеном; после welcome — подписки (спот: /spotMarket/tradeOrdersV2,
//! /account/balance; перпы: /contractMarket/tradeOrders, /contract/positionAll, /contractAccount/wallet).

use crate::live::{Live, StreamMsg};
use crate::paper::{Side, split};
use crate::signed::{Keys, OrderState, dec};
use crate::util::{hmac_b64, now_ms};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

pub fn rest_url(perp: bool) -> &'static str {
    crate::feed::kucoin::rest_url(perp)
}

fn num(v: &Value) -> f64 {
    v.as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| v.as_f64())
        .unwrap_or(0.0)
}

/// Монета KuCoin Futures: BTC торгуется как XBT.
fn alias(c: &str) -> &str {
    if c == "BTC" { "XBT" } else { c }
}

pub struct Client {
    base: String,
    pub perp: bool,
    keys: Keys,
    passphrase: String,
    leverage: Mutex<f64>,
    /// Перпы: контракт → (символ бота, монет в лоте); символ бота → контракт.
    contracts: Mutex<HashMap<String, (String, f64)>>,
    /// Исполнения из потока по номеру ордера: (объём, сумма) — для средней цены.
    fills: Mutex<HashMap<String, (f64, f64)>>,
    http: reqwest::Client,
}

impl Client {
    pub fn new(base: &str, keys: Keys, passphrase: String, perp: bool) -> Client {
        Client {
            base: base.trim_end_matches('/').to_owned(),
            perp,
            keys,
            passphrase,
            leverage: Mutex::new(1.0),
            contracts: Mutex::default(),
            fills: Mutex::default(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP-клиент"),
        }
    }

    pub fn instrument(&self, symbol: &str) -> String {
        let (b, q) = split(symbol).unwrap_or((symbol.to_uppercase(), String::new()));
        if !self.perp {
            return format!("{b}-{q}");
        }
        self.contracts
            .lock()
            .unwrap()
            .iter()
            .find(|(_, v)| v.0 == symbol.to_uppercase())
            .map(|(k, _)| k.clone())
            .unwrap_or_else(|| format!("{}{q}M", alias(&b)))
    }

    fn lot(&self, inst: &str) -> f64 {
        if self.perp {
            self.contracts
                .lock()
                .unwrap()
                .get(inst)
                .map_or(1.0, |c| c.1)
        } else {
            1.0
        }
    }

    pub fn symbol_of(&self, inst: &str) -> String {
        if self.perp
            && let Some(c) = self.contracts.lock().unwrap().get(inst)
        {
            return c.0.clone();
        }
        inst.replace('-', "")
    }

    /// Контракты перпов (/api/v1/contracts/active): все линейные — контракт → (символ бота, multiplier).
    pub fn map_contracts(&self, v: &Value) {
        let mut m = self.contracts.lock().unwrap();
        for c in v["data"].as_array().into_iter().flatten() {
            if c["isInverse"] == true {
                continue;
            }
            let (Some(id), Some(b), Some(q)) = (
                c["symbol"].as_str(),
                c["baseCurrency"].as_str(),
                c["quoteCurrency"].as_str(),
            ) else {
                continue;
            };
            let base = if b == "XBT" { "BTC" } else { b };
            m.insert(
                id.to_owned(),
                (format!("{base}{q}"), num(&c["multiplier"]).abs().max(1e-18)),
            );
        }
    }

    pub fn sign(&self, ts: &str, method: &str, path: &str, body: &str) -> String {
        hmac_b64(&self.keys.secret, &format!("{ts}{method}{path}{body}"))
    }

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, String> {
        let ts = now_ms().to_string();
        let b = body.map(|b| b.to_string()).unwrap_or_default();
        let mut r = self
            .http
            .request(method.clone(), format!("{}{path}", self.base))
            .header("KC-API-KEY", &self.keys.key)
            .header("KC-API-SIGN", self.sign(&ts, method.as_str(), path, &b))
            .header("KC-API-TIMESTAMP", ts)
            .header(
                "KC-API-PASSPHRASE",
                hmac_b64(&self.keys.secret, &self.passphrase),
            )
            .header("KC-API-KEY-VERSION", "2")
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
        if v["code"] != "200000" {
            return Err(format!(
                "KuCoin {}: {}",
                v["code"],
                v["msg"].as_str().unwrap_or("")
            ));
        }
        Ok(v["data"].clone())
    }

    pub async fn sync_time(&self) -> Result<(), String> {
        if self.perp {
            let v: Value = self
                .http
                .get(format!("{}/api/v1/contracts/active", self.base))
                .send()
                .await
                .map_err(|e| e.to_string())?
                .json()
                .await
                .map_err(|e| e.to_string())?;
            self.map_contracts(&v);
        }
        Ok(())
    }

    /// Состояние ордера из REST: спот — dealSize/dealFunds, перпы — filledSize (лоты)/filledValue.
    pub fn order_state(&self, o: &Value) -> OrderState {
        let id = o["id"]
            .as_str()
            .or(o["orderId"].as_str())
            .unwrap_or("")
            .to_owned();
        let active = o["isActive"] == true;
        if self.perp {
            let m = self.lot(o["symbol"].as_str().unwrap_or(""));
            let (filled, size) = (num(&o["filledSize"]) * m, num(&o["size"]) * m);
            let status = if active {
                if filled > 0.0 {
                    "PARTIALLY_FILLED"
                } else {
                    "NEW"
                }
            } else if filled >= size - 1e-12 && filled > 0.0 {
                "FILLED"
            } else {
                "CANCELED"
            };
            return OrderState {
                id,
                status: status.into(),
                executed: filled,
                quote: num(&o["filledValue"]),
            };
        }
        let (exec, funds) = (num(&o["dealSize"]), num(&o["dealFunds"]));
        let status = if active {
            if exec > 0.0 {
                "PARTIALLY_FILLED"
            } else {
                "NEW"
            }
        } else if o["cancelExist"] == true {
            "CANCELED"
        } else {
            "FILLED"
        };
        OrderState {
            id,
            status: status.into(),
            executed: exec,
            quote: funds,
        }
    }

    fn order_body(
        &self,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
    ) -> Result<Value, String> {
        let inst = self.instrument(symbol);
        let size = if self.perp {
            let lots = ((qty / self.lot(&inst)) + 1e-9).floor();
            if lots < 1.0 {
                return Err(format!("KuCoin: объём {qty} меньше одного лота {inst}"));
            }
            json!(lots as i64)
        } else {
            json!(dec(qty))
        };
        let mut b = json!({"clientOid": format!("hft{local}"), "side": if side == Side::Buy { "buy" } else { "sell" }, "symbol": inst,
            "type": if price.is_some() { "limit" } else { "market" }, "size": size});
        if let Some(px) = price {
            b["price"] = json!(dec(px));
        }
        if self.perp {
            b["leverage"] = json!((*self.leverage.lock().unwrap()).max(1.0).round() as u64);
            b["marginMode"] = json!("CROSS");
        }
        Ok(b)
    }

    pub async fn place(
        &self,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
    ) -> Result<OrderState, String> {
        let b = self.order_body(local, symbol, side, qty, price)?;
        let d = self
            .call(reqwest::Method::POST, "/api/v1/orders", Some(b))
            .await?;
        Ok(OrderState {
            id: d["orderId"].as_str().unwrap_or("").to_owned(),
            status: "NEW".into(),
            executed: 0.0,
            quote: 0.0,
        })
    }

    /// Ордер по торговому сокету (Pro WS API): spot.order / futures.order.
    pub fn ws_place(
        &self,
        id: &str,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
    ) -> Result<String, String> {
        let op = if self.perp {
            "futures.order"
        } else {
            "spot.order"
        };
        Ok(
            json!({"id": id, "op": op, "args": self.order_body(local, symbol, side, qty, price)?})
                .to_string(),
        )
    }

    pub fn ws_cancel(&self, id: &str, symbol: &str, order_id: &str) -> String {
        let op = if self.perp {
            "futures.cancel"
        } else {
            "spot.cancel"
        };
        json!({"id": id, "op": op, "args": {"orderId": order_id, "symbol": self.instrument(symbol)}}).to_string()
    }

    /// Адрес торгового сокета с подписью в параметрах; KUCOIN_WS_TRADE_URL — свой адрес.
    pub fn trade_url(&self) -> String {
        let base = crate::util::env("KUCOIN_WS_TRADE_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "wss://wsapi.kucoin.com/v1/private".into());
        let ts = now_ms().to_string();
        let enc = |x: &str| {
            x.bytes()
                .map(|c| match c {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                        (c as char).to_string()
                    }
                    _ => format!("%{c:02X}"),
                })
                .collect::<String>()
        };
        format!(
            "{base}{}apikey={}&timestamp={ts}&sign={}&passphrase={}",
            if base.contains('?') { '&' } else { '?' },
            enc(&self.keys.key),
            enc(&hmac_b64(
                &self.keys.secret,
                &format!("{}{ts}", self.keys.key)
            )),
            enc(&hmac_b64(&self.keys.secret, &self.passphrase))
        )
    }

    /// Ответ на приветствие торгового сокета: base64(HMAC(secret, приветствие)).
    pub fn welcome_answer(&self, text: &str) -> String {
        hmac_b64(&self.keys.secret, text)
    }

    /// Ордер по своему номеру (исход запроса по сокету неизвестен).
    pub async fn query_client(&self, _symbol: &str, local: u64) -> Result<OrderState, String> {
        let path = if self.perp {
            format!("/api/v1/orders/byClientOid?clientOid=hft{local}")
        } else {
            format!("/api/v1/order/client-order/hft{local}")
        };
        let d = self.call(reqwest::Method::GET, &path, None).await?;
        if d.is_null() {
            return Err("KuCoin: ордер не найден".into());
        }
        Ok(self.order_state(&d))
    }

    pub async fn query(&self, _symbol: &str, id: &str) -> Result<OrderState, String> {
        let d = self
            .call(reqwest::Method::GET, &format!("/api/v1/orders/{id}"), None)
            .await?;
        Ok(self.order_state(&d))
    }

    pub async fn cancel(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let _ = self
            .call(
                reqwest::Method::DELETE,
                &format!("/api/v1/orders/{id}"),
                None,
            )
            .await;
        self.query(symbol, id).await
    }

    pub async fn balances(&self) -> Result<BTreeMap<String, f64>, String> {
        if self.perp {
            let d = self
                .call(
                    reqwest::Method::GET,
                    "/api/v1/account-overview?currency=USDT",
                    None,
                )
                .await?;
            return Ok(BTreeMap::from([(
                "USDT".to_owned(),
                num(&d["availableBalance"]),
            )]));
        }
        let d = self
            .call(reqwest::Method::GET, "/api/v1/accounts?type=trade", None)
            .await?;
        Ok(d.as_array()
            .into_iter()
            .flatten()
            .filter_map(|a| Some((a["currency"].as_str()?.to_owned(), num(&a["available"]))))
            .filter(|(_, f)| *f > 0.0)
            .collect())
    }

    pub fn position(&self, p: &Value) -> Option<(String, f64, f64)> {
        let inst = p["symbol"].as_str()?;
        p.get("currentQty")?;
        Some((
            self.symbol_of(inst),
            num(&p["currentQty"]) * self.lot(inst),
            num(&p["avgEntryPrice"]),
        ))
    }

    pub async fn positions(&self) -> Result<Vec<(String, f64, f64)>, String> {
        let d = self
            .call(reqwest::Method::GET, "/api/v1/positions", None)
            .await?;
        Ok(d.as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| self.position(p))
            .collect())
    }

    /// Плечо кросс-маржи; запоминается и уходит в каждом ордере.
    pub async fn set_leverage(&self, symbol: &str, leverage: f64) -> Result<(), String> {
        *self.leverage.lock().unwrap() = leverage;
        let b = json!({"symbol": self.instrument(symbol), "leverage": format!("{}", leverage.round() as u64)});
        self.call(
            reqwest::Method::POST,
            "/api/v2/changeCrossUserLeverage",
            Some(b),
        )
        .await
        .map(|_| ())
    }

    /// Адрес приватного потока с токеном; KUCOIN_WS_PRIVATE_URL — свой (без запроса токена).
    pub async fn stream_url(&self) -> Result<String, String> {
        if let Ok(u) = crate::util::env("KUCOIN_WS_PRIVATE_URL")
            && !u.trim().is_empty()
        {
            return Ok(u);
        }
        let d = self
            .call(reqwest::Method::POST, "/api/v1/bullet-private", None)
            .await?;
        let srv = &d["instanceServers"][0];
        // адрес без пути (ws://host:port) — добавить «/», иначе запрос ?token=… некорректен
        let mut endpoint = srv["endpoint"].as_str().unwrap_or("").to_owned();
        if endpoint
            .split_once("://")
            .is_some_and(|(_, rest)| !rest.contains('/'))
        {
            endpoint.push('/');
        }
        Ok(format!(
            "{endpoint}?token={}&connectId=hft{}",
            d["token"].as_str().unwrap_or(""),
            now_ms()
        ))
    }

    pub fn subscriptions(&self) -> Vec<String> {
        let topics: &[&str] = if self.perp {
            &[
                "/contractMarket/tradeOrders",
                "/contract/positionAll",
                "/contractAccount/wallet",
            ]
        } else {
            &["/spotMarket/tradeOrdersV2", "/account/balance"]
        };
        topics
            .iter()
            .enumerate()
            .map(|(i, t)| json!({"id": format!("sub{i}"), "type": "subscribe", "topic": t, "privateChannel": true, "response": true}).to_string())
            .collect()
    }

    pub fn ping(&self) -> String {
        json!({"id": format!("p{}", now_ms()), "type": "ping"}).to_string()
    }

    pub fn stream_parse(&self, v: &Value) -> StreamMsg {
        match v["type"].as_str() {
            Some("welcome") => StreamMsg::SendMany(self.subscriptions()),
            Some("ack") if v["id"] == "sub0" => StreamMsg::Ready,
            Some("error") => StreamMsg::Fail(format!("KuCoin {}: {}", v["code"], v["data"])),
            Some("message") => {
                let topic = v["topic"].as_str().unwrap_or("");
                let d = &v["data"];
                if topic.starts_with("/spotMarket/tradeOrders")
                    || topic.starts_with("/contractMarket/tradeOrders")
                {
                    let Some(local) = d["clientOid"]
                        .as_str()
                        .and_then(|c| c.strip_prefix("hft"))
                        .and_then(|x| x.parse::<u64>().ok())
                    else {
                        return StreamMsg::Ignore;
                    };
                    let id = d["orderId"].as_str().unwrap_or("").to_owned();
                    let m = self.lot(d["symbol"].as_str().unwrap_or(""));
                    // средняя цена — по сделкам match (в событии её нет)
                    let (fq, fv) = {
                        let mut f = self.fills.lock().unwrap();
                        let e = f.entry(id.clone()).or_default();
                        if d["type"] == "match" {
                            let q = num(&d["matchSize"]) * m;
                            e.0 += q;
                            e.1 += q * num(&d["matchPrice"]);
                        }
                        *e
                    };
                    let (exec, size) = (num(&d["filledSize"]) * m, num(&d["size"]) * m);
                    let avg = if fq > 0.0 { fv / fq } else { 0.0 };
                    let status = match d["type"].as_str() {
                        Some("filled") => "FILLED",
                        Some("canceled") => "CANCELED",
                        _ if d["status"] == "done" => {
                            if exec > 0.0 && exec >= size - 1e-12 {
                                "FILLED"
                            } else {
                                "CANCELED"
                            }
                        }
                        _ => {
                            if exec > 0.0 {
                                "PARTIALLY_FILLED"
                            } else {
                                "NEW"
                            }
                        }
                    };
                    let st = OrderState {
                        id,
                        status: status.into(),
                        executed: exec,
                        quote: exec * avg,
                    };
                    StreamMsg::Apply(Box::new(move |l: &mut Live| {
                        l.stream_events += 1;
                        l.on_state(local, &st);
                    }))
                } else if topic.starts_with("/account/balance") {
                    // только торговый счёт
                    if !d["relationEvent"]
                        .as_str()
                        .unwrap_or("trade.")
                        .starts_with("trade")
                    {
                        return StreamMsg::Ignore;
                    }
                    let (c, a) = (
                        d["currency"].as_str().unwrap_or("").to_owned(),
                        num(&d["available"]),
                    );
                    StreamMsg::Apply(Box::new(move |l: &mut Live| {
                        l.stream_events += 1;
                        l.ledger.set_balance(&c, a);
                    }))
                } else if topic.starts_with("/contract/position") {
                    match self.position(d) {
                        Some((s, q, e)) => StreamMsg::Apply(Box::new(move |l: &mut Live| {
                            l.stream_events += 1;
                            l.ledger.set_position(&s, q, e);
                        })),
                        None => StreamMsg::Ignore,
                    }
                } else if topic.starts_with("/contractAccount/wallet")
                    && d.get("availableBalance").is_some()
                {
                    let a = num(&d["availableBalance"]);
                    StreamMsg::Apply(Box::new(move |l: &mut Live| {
                        l.stream_events += 1;
                        l.ledger.set_balance("USDT", a);
                    }))
                } else {
                    StreamMsg::Ignore
                }
            }
            _ => StreamMsg::Ignore,
        }
    }
}

/// Стоп на бирже (перпы): reduceOnly-стоп по цене маркировки на всю позицию; None — биржа или рынок не поддерживает.
impl Client {
    /// Стоп-ордер closeOrder по маркировке (stopPriceType MP): лонг — stop=down, шорт — stop=up.
    pub async fn place_stop(
        &self,
        symbol: &str,
        side: crate::paper::Side,
        price: f64,
    ) -> Result<Option<String>, String> {
        if !self.perp {
            return Ok(None);
        }
        let sell = side == crate::paper::Side::Sell;
        let b = json!({"clientOid": format!("sl{}", now_ms()), "side": if sell { "sell" } else { "buy" }, "symbol": self.instrument(symbol),
            "type": "market", "stop": if sell { "down" } else { "up" }, "stopPriceType": "MP", "stopPrice": dec(price),
            "closeOrder": true, "marginMode": "CROSS"});
        let v = self
            .call(reqwest::Method::POST, "/api/v1/orders", Some(b))
            .await?;
        let id = v["orderId"]
            .as_str()
            .or(v["data"]["orderId"].as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("KuCoin: нет orderId у стопа: {v}"))?;
        Ok(Some(id.to_owned()))
    }

    pub async fn cancel_stop(&self, _symbol: &str, id: &str) -> Result<(), String> {
        self.call(
            reqwest::Method::DELETE,
            &format!("/api/v1/orders/{id}"),
            None,
        )
        .await
        .map(|_| ())
    }
}

/// Ответ торгового сокета: (id, data или ошибка «KuCoin код: текст»); пинги и служебные — None.
pub fn ws_reply(v: &Value) -> Option<(String, Result<Value, String>)> {
    let id = v["id"]
        .as_str()
        .filter(|i| !i.is_empty() && !i.starts_with("ping"))?
        .to_owned();
    let code = v["code"].as_str().unwrap_or("");
    Some((
        id,
        if code == "200000" || code == "0" {
            Ok(v["data"].clone())
        } else {
            Err(format!(
                "KuCoin {code}: {}",
                v["msg"].as_str().unwrap_or("")
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
            "p".into(),
            perp,
        )
    }

    #[test]
    fn spot_states_and_events() {
        let c = client(false);
        assert_eq!(c.instrument("BTCUSDT"), "BTC-USDT");
        let s = c.order_state(&json!({"id": "a1", "isActive": false, "cancelExist": false, "dealSize": "2", "dealFunds": "20"}));
        assert_eq!(
            s,
            OrderState {
                id: "a1".into(),
                status: "FILLED".into(),
                executed: 2.0,
                quote: 20.0
            }
        );
        assert!(
            matches!(c.stream_parse(&json!({"type": "welcome"})), StreamMsg::SendMany(v) if v.len() == 2)
        );
        assert!(matches!(
            c.stream_parse(&json!({"type": "ack", "id": "sub0"})),
            StreamMsg::Ready
        ));
        assert!(matches!(c.stream_parse(&json!({"type": "message", "topic": "/spotMarket/tradeOrdersV2",
            "data": {"orderId": "a1", "clientOid": "hft3", "type": "match", "status": "match", "matchSize": "1", "matchPrice": "10", "filledSize": "1", "size": "2"}})), StreamMsg::Apply(_)));
        assert_eq!(c.fills.lock().unwrap()["a1"], (1.0, 10.0));
    }

    #[test]
    fn futures_lots() {
        let c = client(true);
        c.map_contracts(&json!({"data": [{"symbol": "XBTUSDTM", "baseCurrency": "XBT", "quoteCurrency": "USDT", "multiplier": 0.001, "isInverse": false}]}));
        assert_eq!(c.instrument("BTCUSDT"), "XBTUSDTM");
        let s = c.order_state(&json!({"id": "f1", "symbol": "XBTUSDTM", "isActive": false, "filledSize": 500, "size": 500, "filledValue": "50"}));
        assert_eq!(
            (s.status.as_str(), s.executed, s.quote),
            ("FILLED", 0.5, 50.0)
        );
        assert_eq!(
            c.position(&json!({"symbol": "XBTUSDTM", "currentQty": -200, "avgEntryPrice": "100"})),
            Some(("BTCUSDT".into(), -0.2, 100.0))
        );
    }
}
