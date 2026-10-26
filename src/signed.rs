//! Подписанный REST Binance (спот): HMAC-SHA256 строки запроса, заголовок X-MBX-APIKEY, поправка на время сервера.

use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Ключи API.
#[derive(Clone)]
pub struct Keys {
    pub key: String,
    pub secret: String,
}

impl Keys {
    /// Ключи из окружения: BINANCE_API_KEY/_SECRET или (тестовая сеть) BINANCE_TESTNET_API_KEY/_SECRET.
    pub fn from_env(ex: &str, testnet: bool) -> Option<Keys> {
        let p = format!(
            "{}{}",
            ex.to_uppercase(),
            if testnet { "_TESTNET" } else { "" }
        );
        let get = |k: &str| {
            std::env::var(format!("{p}_{k}"))
                .ok()
                .map(|v| v.trim().to_owned())
                .filter(|v| !v.is_empty())
        };
        Some(Keys {
            key: get("API_KEY")?,
            secret: get("API_SECRET")?,
        })
    }
}

/// Подпись строки запроса.
pub fn sign(secret: &str, query: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC принимает ключ любой длины");
    mac.update(query.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Клиент.
pub struct Client {
    base: String,
    keys: Keys,
    http: reqwest::Client,
    offset_ms: AtomicI64,
    pub recv_window: u64,
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
        }
    }

    /// Сверить часы с сервером Binance (подпись с чужим временем отклоняется).
    pub async fn sync_time(&self) -> Result<(), String> {
        let v: Value = self
            .http
            .get(format!("{}/api/v3/time", self.base))
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        let server = v["serverTime"].as_i64().ok_or("нет serverTime")?;
        self.offset_ms.store(server - now_ms(), Ordering::Relaxed);
        Ok(())
    }

    /// Подписанный запрос; ошибка Binance — «Binance код: текст».
    pub async fn signed(
        &self,
        method: reqwest::Method,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Value, String> {
        let mut q: Vec<String> = params.iter().map(|(k, v)| format!("{k}={v}")).collect();
        q.push(format!("recvWindow={}", self.recv_window));
        q.push(format!(
            "timestamp={}",
            now_ms() + self.offset_ms.load(Ordering::Relaxed)
        ));
        let query = q.join("&");
        let url = format!(
            "{}{path}?{query}&signature={}",
            self.base,
            sign(&self.keys.secret, &query)
        );
        let r = self
            .http
            .request(method, url)
            .header("X-MBX-APIKEY", &self.keys.key)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = r.status();
        let v: Value = r.json().await.map_err(|e| format!("HTTP {status}: {e}"))?;
        if !status.is_success() {
            return Err(format!(
                "Binance {}: {}",
                v["code"],
                v["msg"].as_str().unwrap_or("")
            ));
        }
        Ok(v)
    }

    /// Свободные балансы больше нуля.
    pub async fn balances(&self) -> Result<std::collections::BTreeMap<String, f64>, String> {
        let v = self
            .signed(
                reqwest::Method::GET,
                "/api/v3/account",
                &[("omitZeroBalances", "true".into())],
            )
            .await?;
        Ok(v["balances"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|b| {
                let free: f64 = b["free"].as_str()?.parse().ok()?;
                let asset = b["asset"].as_str()?.to_owned();
                (free > 0.0).then_some((asset, free))
            })
            .collect())
    }
}

/// Состояние ордера на бирже: статус, исполнено, сумма исполненного в котируемой валюте.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderState {
    pub id: u64,
    pub status: String,
    pub executed: f64,
    pub quote: f64,
}

impl OrderState {
    pub fn from(v: &Value) -> Result<OrderState, String> {
        let n = |k: &str| {
            v[k].as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0)
        };
        Ok(OrderState {
            id: v["orderId"]
                .as_u64()
                .ok_or_else(|| format!("нет orderId: {v}"))?,
            status: v["status"].as_str().unwrap_or("").to_owned(),
            executed: n("executedQty"),
            quote: n("cummulativeQuoteQty"),
        })
    }

    /// Ордер больше не стоит на бирже.
    pub fn terminal(&self) -> bool {
        matches!(
            self.status.as_str(),
            "FILLED" | "CANCELED" | "REJECTED" | "EXPIRED" | "EXPIRED_IN_MATCH"
        )
    }
}

/// Число без лишних нулей и без экспоненты (Binance не принимает 1e-5).
pub fn dec(x: f64) -> String {
    let s = format!("{x:.10}");
    let s = s.trim_end_matches('0');
    s.trim_end_matches('.').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Пример из документации Binance (SIGNED Endpoint Examples).
    #[test]
    fn signature_matches_binance_docs() {
        let secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
        let q = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";
        assert_eq!(
            sign(secret, q),
            "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71"
        );
    }

    #[test]
    fn decimals() {
        assert_eq!(dec(0.00001), "0.00001");
        assert_eq!(dec(100.0), "100");
        assert_eq!(dec(1.5), "1.5");
    }

    #[test]
    fn order_state() {
        let v = serde_json::json!({"orderId": 7, "status": "PARTIALLY_FILLED", "executedQty": "0.5", "cummulativeQuoteQty": "50.25"});
        let s = OrderState::from(&v).unwrap();
        assert_eq!(
            s,
            OrderState {
                id: 7,
                status: "PARTIALLY_FILLED".into(),
                executed: 0.5,
                quote: 50.25
            }
        );
        assert!(!s.terminal());
    }
}
