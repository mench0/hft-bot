//! Подписанный REST OKX v5 и приватный поток, как в Java OkxRestClient: подпись — Base64(HMAC-SHA256(время ISO +
//! МЕТОД + путь_с_query + тело)) в заголовках OK-ACCESS-*, нужен passphrase (OKX_PASSPHRASE, у демо-ключей —
//! OKX_TESTNET_PASSPHRASE); testnet — демо-торговля заголовком x-simulated-trading. Спот — tdMode=cash, перпы —
//! инструменты -SWAP, tdMode=cross, объём в контрактах (× ctVal — в монеты). Поток: login, каналы orders, account,
//! positions (перпы); пинг — текст ping.

use crate::paper::{Side, split};
use crate::signed::{Keys, OrderState, dec};
use crate::util::{hmac_b64, iso_ms, now_ms};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

pub const REST: &str = "https://www.okx.com";

/// Адрес приватного потока; OKX_WS_PRIVATE_URL — свой.
pub fn private_url(testnet: bool) -> String {
    crate::util::env("OKX_WS_PRIVATE_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| {
            if testnet {
                "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999".into()
            } else {
                "wss://ws.okx.com:8443/ws/v5/private".into()
            }
        })
}

/// Статус OKX в общий.
pub fn status(s: &str) -> String {
    match s {
        "live" => "NEW",
        "partially_filled" => "PARTIALLY_FILLED",
        "filled" => "FILLED",
        "canceled" | "mmp_canceled" => "CANCELED",
        other => return other.to_uppercase(),
    }
    .to_owned()
}

fn num(v: &Value) -> f64 {
    v.as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| v.as_f64())
        .unwrap_or(0.0)
}

/// Инструмент → символ бота: BTC-USDT-SWAP → BTCUSDT.
pub fn symbol_of(inst: &str) -> String {
    inst.trim_end_matches("-SWAP").replace('-', "")
}

pub struct Client {
    base: String,
    pub perp: bool,
    testnet: bool,
    keys: Keys,
    passphrase: String,
    /// Перпы: instId → (ctVal, lotSz в контрактах).
    contracts: Mutex<HashMap<String, (f64, f64)>>,
    http: reqwest::Client,
}

impl Client {
    pub fn new(base: &str, keys: Keys, passphrase: String, perp: bool, testnet: bool) -> Client {
        Client {
            base: base.trim_end_matches('/').to_owned(),
            perp,
            testnet,
            keys,
            passphrase,
            contracts: Mutex::default(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP-клиент"),
        }
    }

    pub fn inst_id(&self, symbol: &str) -> String {
        let id = split(symbol).map_or_else(|| symbol.to_uppercase(), |(b, q)| format!("{b}-{q}"));
        if self.perp { format!("{id}-SWAP") } else { id }
    }

    /// Монет в контракте (спот — 1).
    pub fn ct(&self, inst: &str) -> f64 {
        if inst.ends_with("-SWAP") {
            self.contracts
                .lock()
                .unwrap()
                .get(inst)
                .map_or(1.0, |c| c.0)
        } else {
            1.0
        }
    }

    pub fn instrument_sizes(&self, v: &Value) {
        let mut m = self.contracts.lock().unwrap();
        for i in v["data"].as_array().into_iter().flatten() {
            if let Some(id) = i["instId"].as_str() {
                m.insert(
                    id.to_owned(),
                    (num(&i["ctVal"]).max(1e-18), num(&i["lotSz"])),
                );
            }
        }
    }

    /// Перпы: размеры контрактов (/api/v5/public/instruments).
    pub async fn sync_time(&self) -> Result<(), String> {
        if !self.perp {
            return Ok(());
        }
        let v: Value = self
            .http
            .get(format!(
                "{}/api/v5/public/instruments?instType=SWAP",
                self.base
            ))
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        self.instrument_sizes(&v);
        Ok(())
    }

    /// Подпись запроса.
    pub fn sign(&self, ts: &str, method: &str, path: &str, body: &str) -> String {
        hmac_b64(&self.keys.secret, &format!("{ts}{method}{path}{body}"))
    }

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, String> {
        let ts = iso_ms(now_ms());
        let b = body.map(|b| b.to_string()).unwrap_or_default();
        let sig = self.sign(&ts, method.as_str(), path, &b);
        let mut r = self
            .http
            .request(method, format!("{}{path}", self.base))
            .header("OK-ACCESS-KEY", &self.keys.key)
            .header("OK-ACCESS-SIGN", sig)
            .header("OK-ACCESS-TIMESTAMP", ts)
            .header("OK-ACCESS-PASSPHRASE", &self.passphrase)
            .header("Content-Type", "application/json");
        if self.testnet {
            r = r.header("x-simulated-trading", "1");
        }
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
        if v["code"] != "0" {
            let s = v["data"][0]["sMsg"]
                .as_str()
                .filter(|s| !s.is_empty())
                .or(v["msg"].as_str())
                .unwrap_or("");
            return Err(format!("OKX {}: {s}", v["code"]));
        }
        Ok(v)
    }

    /// Состояние ордера из ответа или события (объёмы — в монетах).
    pub fn order_state(&self, o: &Value) -> OrderState {
        let k = self.ct(o["instId"].as_str().unwrap_or(""));
        let exec = num(&o["accFillSz"]) * k;
        OrderState {
            id: o["ordId"].as_str().unwrap_or("").to_owned(),
            status: status(o["state"].as_str().unwrap_or("")),
            executed: exec,
            quote: exec * num(&o["avgPx"]),
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
        let inst = self.inst_id(symbol);
        let sz = if self.perp {
            // монеты → контракты, вниз до шага лота
            let (k, lot) = self
                .contracts
                .lock()
                .unwrap()
                .get(&inst)
                .copied()
                .ok_or_else(|| format!("OKX: нет размера контракта {inst}"))?;
            let c = qty / k;
            if lot > 0.0 {
                ((c / lot) + 1e-9).floor() * lot
            } else {
                c
            }
        } else {
            qty
        };
        let mut b = json!({"instId": inst, "tdMode": if self.perp { "cross" } else { "cash" }, "side": if side == Side::Buy { "buy" } else { "sell" },
            "clOrdId": format!("hft{local}"), "sz": dec(sz)});
        match price {
            Some(px) => {
                b["ordType"] = json!("limit");
                b["px"] = json!(dec(px));
            }
            None => {
                b["ordType"] = json!("market");
                if !self.perp {
                    b["tgtCcy"] = json!("base_ccy"); // рыночная покупка спота — объём в монете
                }
            }
        }
        let r = self
            .call(reqwest::Method::POST, "/api/v5/trade/order", Some(b))
            .await?;
        Ok(OrderState {
            id: r["data"][0]["ordId"].as_str().unwrap_or("").to_owned(),
            status: "NEW".into(),
            executed: 0.0,
            quote: 0.0,
        })
    }

    pub async fn query(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let r = self
            .call(
                reqwest::Method::GET,
                &format!(
                    "/api/v5/trade/order?instId={}&ordId={id}",
                    self.inst_id(symbol)
                ),
                None,
            )
            .await?;
        Ok(self.order_state(&r["data"][0]))
    }

    pub async fn cancel(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let _ = self
            .call(
                reqwest::Method::POST,
                "/api/v5/trade/cancel-order",
                Some(json!({"instId": self.inst_id(symbol), "ordId": id})),
            )
            .await;
        self.query(symbol, id).await
    }

    pub async fn balances(&self) -> Result<BTreeMap<String, f64>, String> {
        let r = self
            .call(reqwest::Method::GET, "/api/v5/account/balance", None)
            .await?;
        Ok(parse_balances(&r["data"][0], self.perp))
    }

    pub async fn positions(&self) -> Result<Vec<(String, f64, f64)>, String> {
        let r = self
            .call(
                reqwest::Method::GET,
                "/api/v5/account/positions?instType=SWAP",
                None,
            )
            .await?;
        Ok(r["data"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| self.position(p))
            .collect())
    }

    pub fn position(&self, p: &Value) -> Option<(String, f64, f64)> {
        let inst = p["instId"].as_str()?;
        (inst.ends_with("-SWAP") && p["posSide"].as_str().unwrap_or("net") == "net").then(|| {
            (
                symbol_of(inst),
                num(&p["pos"]) * self.ct(inst),
                num(&p["avgPx"]),
            )
        })
    }

    pub async fn set_leverage(&self, symbol: &str, leverage: f64) -> Result<(), String> {
        let b = json!({"instId": self.inst_id(symbol), "lever": format!("{}", leverage.round() as u64), "mgnMode": "cross"});
        self.call(
            reqwest::Method::POST,
            "/api/v5/account/set-leverage",
            Some(b),
        )
        .await
        .map(|_| ())
    }

    /// Вход в приватный поток.
    pub fn login(&self) -> String {
        let ts = (now_ms() / 1000).to_string();
        json!({"op": "login", "args": [{"apiKey": self.keys.key, "passphrase": self.passphrase, "timestamp": ts,
            "sign": hmac_b64(&self.keys.secret, &format!("{ts}GET/users/self/verify"))}]})
        .to_string()
    }

    pub fn subscribe(&self) -> String {
        let mut args = vec![
            json!({"channel": "orders", "instType": if self.perp { "SWAP" } else { "SPOT" }}),
            json!({"channel": "account"}),
        ];
        if self.perp {
            args.push(json!({"channel": "positions", "instType": "SWAP"}));
        }
        json!({"op": "subscribe", "args": args}).to_string()
    }
}

/// Свободные балансы из data[0].details (перпы — availEq, если есть).
pub fn parse_balances(d: &Value, perp: bool) -> BTreeMap<String, f64> {
    d["details"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|b| {
            let eq = b["availEq"].as_str().filter(|s| !s.is_empty());
            let free = if perp && eq.is_some() {
                num(&b["availEq"])
            } else {
                num(&b["availBal"])
            };
            Some((b["ccy"].as_str()?.to_owned(), free))
        })
        .filter(|(_, f)| *f > 0.0)
        .collect()
}

/// Стоп на бирже (перпы): reduceOnly-стоп по цене маркировки на всю позицию; None — биржа или рынок не поддерживает.
impl Client {
    /// Алгоритмический стоп conditional: вся позиция (closeFraction 1), по маркировке, рыночный (slOrdPx −1).
    pub async fn place_stop(
        &self,
        symbol: &str,
        side: crate::paper::Side,
        price: f64,
    ) -> Result<Option<String>, String> {
        if !self.perp {
            return Ok(None);
        }
        let b = json!({"instId": self.inst_id(symbol), "tdMode": "cross", "side": if side == crate::paper::Side::Buy { "buy" } else { "sell" },
            "ordType": "conditional", "closeFraction": "1", "reduceOnly": true, "slTriggerPx": crate::signed::dec(price),
            "slTriggerPxType": "mark", "slOrdPx": "-1"});
        let d = self
            .call(reqwest::Method::POST, "/api/v5/trade/order-algo", Some(b))
            .await?;
        let id = d[0]["algoId"]
            .as_str()
            .or(d["data"][0]["algoId"].as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("OKX: нет algoId у стопа: {d}"))?;
        Ok(Some(id.to_owned()))
    }

    pub async fn cancel_stop(&self, symbol: &str, id: &str) -> Result<(), String> {
        self.call(
            reqwest::Method::POST,
            "/api/v5/trade/cancel-algos",
            Some(json!([{"instId": self.inst_id(symbol), "algoId": id}])),
        )
        .await
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn states_sizes_and_signature() {
        let c = Client::new(
            "https://x",
            Keys {
                key: "k".into(),
                secret: "s".into(),
            },
            "p".into(),
            true,
            false,
        );
        c.instrument_sizes(
            &json!({"data": [{"instId": "BTC-USDT-SWAP", "ctVal": "0.01", "lotSz": "1"}]}),
        );
        assert_eq!(c.inst_id("BTCUSDT"), "BTC-USDT-SWAP");
        let st = c.order_state(&json!({"instId": "BTC-USDT-SWAP", "ordId": "123", "state": "partially_filled", "accFillSz": "3", "avgPx": "100"}));
        assert_eq!(
            st,
            OrderState {
                id: "123".into(),
                status: "PARTIALLY_FILLED".into(),
                executed: 0.03,
                quote: 3.0
            }
        );
        assert_eq!(
            c.position(
                &json!({"instId": "BTC-USDT-SWAP", "pos": "-5", "avgPx": "100", "posSide": "net"})
            ),
            Some(("BTCUSDT".into(), -0.05, 100.0))
        );
        // пример подписи из документации OKX: секрет, время, метод, путь, тело
        assert_eq!(
            c.sign(
                "2020-12-08T09:08:57.715Z",
                "GET",
                "/api/v5/account/balance?ccy=BTC",
                ""
            )
            .len(),
            44
        );
        let b = parse_balances(
            &json!({"details": [{"ccy": "USDT", "availBal": "10", "availEq": "12"}, {"ccy": "BTC", "availBal": "0"}]}),
            true,
        );
        assert_eq!(b.get("USDT"), Some(&12.0));
        assert!(!b.contains_key("BTC"));
    }
}
