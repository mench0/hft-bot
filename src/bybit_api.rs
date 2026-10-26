//! Подписанный REST Bybit v5 (спот) и приватный поток `order` / `wallet`, как в Java (BybitRestClient, BybitWs).
//! Подпись: X-BAPI-SIGN = hex(HMAC-SHA256(время + ключ + recvWindow + запрос)), где запрос — строка параметров для GET
//! и тело JSON для POST. Поток: op "auth" с [ключ, срок, hex(HMAC("GET/realtime" + срок))], пинг {"op":"ping"} раз в 20 с.

use crate::paper::Side;
use crate::signed::{Keys, OrderState, dec, sign};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Адрес REST по умолчанию.
pub fn rest_url(testnet: bool) -> &'static str {
    if testnet {
        "https://api-testnet.bybit.com"
    } else {
        "https://api.bybit.com"
    }
}

/// Адрес приватного потока; BYBIT_WS_PRIVATE_URL — свой адрес (проверки, прокси).
pub fn private_url(testnet: bool) -> String {
    crate::util::env("BYBIT_WS_PRIVATE_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| {
            if testnet {
                "wss://stream-testnet.bybit.com/v5/private".into()
            } else {
                "wss://stream.bybit.com/v5/private".into()
            }
        })
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Статус Bybit в общий (как в Java BybitWs.status).
pub fn status(s: &str) -> String {
    match s {
        "New" | "Untriggered" => "NEW",
        "PartiallyFilled" => "PARTIALLY_FILLED",
        "Filled" => "FILLED",
        "Cancelled" | "PartiallyFilledCanceled" | "Deactivated" => "CANCELED",
        "Rejected" => "REJECTED",
        other => return other.to_uppercase(),
    }
    .to_owned()
}

/// Состояние ордера из ответа или события Bybit.
pub fn order_state(o: &Value) -> Result<OrderState, String> {
    let n = |k: &str| {
        o[k].as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    Ok(OrderState {
        id: o["orderId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("нет orderId: {o}"))?
            .to_owned(),
        status: status(o["orderStatus"].as_str().unwrap_or("")),
        executed: n("cumExecQty"),
        quote: n("cumExecValue"),
    })
}

/// Свободный баланс монеты: walletBalance включает занятое в ордерах (locked).
pub fn free_of(c: &Value) -> f64 {
    let n = |k: &str| {
        c[k].as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    (n("walletBalance") - n("locked")).max(0.0)
}

/// Позиция из ответа или события: символ, количество со знаком (Sell — шорт), цена входа.
pub fn position(p: &Value) -> Option<(String, f64, f64)> {
    let n = |k: &str| {
        p[k].as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    let size = n("size");
    let qty = if p["side"] == "Sell" { -size } else { size };
    let entry = if n("avgPrice") > 0.0 {
        n("avgPrice")
    } else {
        n("entryPrice")
    };
    Some((p["symbol"].as_str()?.to_owned(), qty, entry))
}

pub struct Client {
    base: String,
    keys: Keys,
    http: reqwest::Client,
    offset_ms: AtomicI64,
    recv_window: u64,
    /// "spot" или "linear" (USDT-перпы).
    pub category: &'static str,
}

impl Client {
    pub fn new(base: &str, keys: Keys) -> Client {
        Client {
            base: base.trim_end_matches('/').to_owned(),
            keys,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP-клиент"),
            offset_ms: AtomicI64::new(0),
            recv_window: 5000,
            category: "spot",
        }
    }

    pub async fn sync_time(&self) -> Result<(), String> {
        let v: Value = self
            .http
            .get(format!("{}/v5/market/time", self.base))
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        let t = v["time"].as_i64().ok_or("нет time")?;
        self.offset_ms.store(t - now_ms(), Ordering::Relaxed);
        Ok(())
    }

    async fn call(&self, post: bool, path: &str, payload: String) -> Result<Value, String> {
        let ts = now_ms() + self.offset_ms.load(Ordering::Relaxed);
        let sig = sign(
            &self.keys.secret,
            &format!("{ts}{}{}{payload}", self.keys.key, self.recv_window),
        );
        let req = if post {
            self.http
                .post(format!("{}{path}", self.base))
                .header("Content-Type", "application/json")
                .body(payload)
        } else {
            self.http.get(format!("{}{path}?{payload}", self.base))
        };
        let v: Value = req
            .header("X-BAPI-API-KEY", &self.keys.key)
            .header("X-BAPI-TIMESTAMP", ts.to_string())
            .header("X-BAPI-RECV-WINDOW", self.recv_window.to_string())
            .header("X-BAPI-SIGN", sig)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        if v["retCode"].as_i64() != Some(0) {
            return Err(format!(
                "Bybit {}: {}",
                v["retCode"],
                v["retMsg"].as_str().unwrap_or("")
            ));
        }
        Ok(v["result"].clone())
    }

    /// Новый ордер; ответ — только номер, состояние — по запросу или из потока.
    pub async fn place(
        &self,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
    ) -> Result<OrderState, String> {
        let mut b = json!({"category": self.category, "symbol": symbol, "side": if side == Side::Buy { "Buy" } else { "Sell" },
            "orderType": if price.is_some() { "Limit" } else { "Market" }, "qty": dec(qty), "orderLinkId": format!("hft{local}")});
        match price {
            Some(px) => {
                b["price"] = Value::from(dec(px));
                b["timeInForce"] = Value::from("GTC");
            }
            // рыночная покупка на споте по умолчанию в котируемой валюте — единицу указываем явно
            None if self.category == "spot" => b["marketUnit"] = Value::from("baseCoin"),
            None => {}
        }
        let r = self.call(true, "/v5/order/create", b.to_string()).await?;
        Ok(OrderState {
            id: r["orderId"].as_str().unwrap_or("").to_owned(),
            status: "NEW".into(),
            executed: 0.0,
            quote: 0.0,
        })
    }

    /// Состояние ордера: открытые и недавние — /v5/order/realtime, иначе история.
    pub async fn query(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let q = format!("category={}&orderId={id}&symbol={symbol}", self.category);
        let r = self.call(false, "/v5/order/realtime", q.clone()).await?;
        let o = match r["list"].get(0) {
            Some(o) => o.clone(),
            None => self.call(false, "/v5/order/history", q).await?["list"]
                .get(0)
                .cloned()
                .ok_or("ордер не найден")?,
        };
        order_state(&o)
    }

    /// Снять ордер и узнать итог (уже исполнен или снят — тоже итог).
    pub async fn cancel(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let _ = self
            .call(
                true,
                "/v5/order/cancel",
                json!({"category": self.category, "symbol": symbol, "orderId": id}).to_string(),
            )
            .await;
        self.query(symbol, id).await
    }

    /// Свободные балансы единого счёта.
    pub async fn balances(&self) -> Result<BTreeMap<String, f64>, String> {
        let r = self
            .call(
                false,
                "/v5/account/wallet-balance",
                "accountType=UNIFIED".into(),
            )
            .await?;
        Ok(r["list"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|a| a["coin"].as_array().cloned().unwrap_or_default())
            .filter_map(|c| Some((c["coin"].as_str()?.to_owned(), free_of(&c))))
            .filter(|(_, f)| *f > 0.0)
            .collect())
    }

    /// Позиции USDT-перпов: символ, количество со знаком, цена входа.
    pub async fn positions(&self) -> Result<Vec<(String, f64, f64)>, String> {
        let r = self
            .call(
                false,
                "/v5/position/list",
                "category=linear&settleCoin=USDT".into(),
            )
            .await?;
        Ok(r["list"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(position)
            .collect())
    }

    /// Плечо символа; «не изменено» (110043) — не ошибка.
    pub async fn set_leverage(&self, symbol: &str, leverage: f64) -> Result<(), String> {
        let l = format!("{}", leverage.round() as u64);
        let b =
            json!({"category": "linear", "symbol": symbol, "buyLeverage": l, "sellLeverage": l});
        match self
            .call(true, "/v5/position/set-leverage", b.to_string())
            .await
        {
            Err(e) if e.contains("110043") => Ok(()),
            r => r.map(|_| ()),
        }
    }

    /// Сети вывода и депозита монеты.
    pub async fn networks(&self, coin: &str) -> Result<Vec<crate::transfer::Net>, String> {
        let r = self
            .call(false, "/v5/asset/coin/query-info", format!("coin={coin}"))
            .await?;
        Ok(crate::transfer::bybit_nets(&r))
    }

    /// Адрес депозита монеты в сети (адрес, тег).
    pub async fn deposit_address(
        &self,
        coin: &str,
        chain: &str,
    ) -> Result<(String, String), String> {
        let r = self
            .call(
                false,
                "/v5/asset/deposit/query-address",
                format!("chainType={chain}&coin={coin}"),
            )
            .await?;
        let c = r["chains"]
            .as_array()
            .and_then(|a| {
                a.iter()
                    .find(|c| c["chainType"] == chain || c["chain"] == chain)
                    .or(a.first())
            })
            .ok_or_else(|| format!("нет адреса: {r}"))?;
        let a = c["addressDeposit"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("нет адреса: {r}"))?;
        Ok((
            a.to_owned(),
            c["tagDeposit"].as_str().unwrap_or("").to_owned(),
        ))
    }

    /// Вывод с единого счёта; feeType 1 — комиссия из суммы (получатель получит amount − комиссия, как у Binance).
    pub async fn withdraw(
        &self,
        coin: &str,
        chain: &str,
        address: &str,
        tag: &str,
        amount: f64,
    ) -> Result<String, String> {
        let ts = now_ms() + self.offset_ms.load(Ordering::Relaxed);
        let mut b = json!({"coin": coin, "chain": chain, "address": address, "amount": dec(amount), "timestamp": ts,
            "forceChain": 1, "accountType": "UNIFIED", "feeType": 1});
        if !tag.is_empty() {
            b["tag"] = Value::from(tag);
        }
        let r = self
            .call(true, "/v5/asset/withdraw/create", b.to_string())
            .await?;
        Ok(r["id"].as_str().unwrap_or("").to_owned())
    }

    /// Вход в приватный поток.
    pub fn auth_message(&self) -> String {
        let expires = now_ms() + self.offset_ms.load(Ordering::Relaxed) + 10_000;
        json!({"op": "auth", "args": [self.keys.key, expires, sign(&self.keys.secret, &format!("GET/realtime{expires}"))]}).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_and_states() {
        assert_eq!(status("PartiallyFilledCanceled"), "CANCELED");
        assert_eq!(status("Filled"), "FILLED");
        let s = order_state(&json!({"orderId": "abc-1", "orderStatus": "PartiallyFilled", "cumExecQty": "0.5", "cumExecValue": "50"})).unwrap();
        assert_eq!(
            s,
            OrderState {
                id: "abc-1".into(),
                status: "PARTIALLY_FILLED".into(),
                executed: 0.5,
                quote: 50.0
            }
        );
        assert_eq!(
            free_of(&json!({"coin": "USDT", "walletBalance": "100", "locked": "30"})),
            70.0
        );
        assert_eq!(
            position(
                &json!({"symbol": "BTCUSDT", "side": "Sell", "size": "0.5", "avgPrice": "100"})
            ),
            Some(("BTCUSDT".into(), -0.5, 100.0))
        );
    }
}
