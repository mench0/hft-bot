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
            crate::util::env(&format!("{p}_{k}"))
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
    /// USDⓈ-M перпы (/fapi) вместо спота (/api/v3).
    pub perp: bool,
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
            perp: false,
        }
    }

    /// Клиент USDⓈ-M перпов.
    pub fn new_perp(base: &str, keys: Keys) -> Client {
        Client {
            perp: true,
            ..Client::new(base, keys)
        }
    }

    fn order_path(&self) -> &'static str {
        if self.perp {
            "/fapi/v1/order"
        } else {
            "/api/v3/order"
        }
    }

    /// Сверить часы с сервером Binance (подпись с чужим временем отклоняется).
    pub async fn sync_time(&self) -> Result<(), String> {
        let v: Value = self
            .http
            .get(format!(
                "{}{}",
                self.base,
                if self.perp {
                    "/fapi/v1/time"
                } else {
                    "/api/v3/time"
                }
            ))
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

    /// Подписанный запрос WebSocket API (ws-api/v3): параметры по алфавиту вместе с apiKey и timestamp, подпись HMAC.
    pub fn ws_request(&self, id: &str, method: &str) -> String {
        let ts = now_ms() + self.offset_ms.load(Ordering::Relaxed);
        let query = format!("apiKey={}&timestamp={ts}", self.keys.key);
        serde_json::json!({"id": id, "method": method, "params": {
            "apiKey": self.keys.key, "timestamp": ts, "signature": sign(&self.keys.secret, &query)}}).to_string()
    }

    /// Свободные балансы больше нуля (перпы — доступная маржа).
    pub async fn balances(&self) -> Result<std::collections::BTreeMap<String, f64>, String> {
        if self.perp {
            let v = self
                .signed(reqwest::Method::GET, "/fapi/v2/balance", &[])
                .await?;
            return Ok(v
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|b| {
                    let free: f64 = b["availableBalance"].as_str()?.parse().ok()?;
                    (free > 0.0).then_some((b["asset"].as_str()?.to_owned(), free))
                })
                .collect());
        }
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

impl Client {
    /// Новый ордер (clientOrderId hft<номер>); ответ RESULT — состояние сразу.
    pub async fn place(
        &self,
        local: u64,
        symbol: &str,
        side: crate::paper::Side,
        qty: f64,
        price: Option<f64>,
    ) -> Result<OrderState, String> {
        let mut p = vec![
            ("symbol", symbol.to_owned()),
            (
                "side",
                if side == crate::paper::Side::Buy {
                    "BUY"
                } else {
                    "SELL"
                }
                .into(),
            ),
            (
                "type",
                if price.is_some() { "LIMIT" } else { "MARKET" }.into(),
            ),
            ("quantity", dec(qty)),
            ("newClientOrderId", format!("hft{local}")),
            ("newOrderRespType", "RESULT".into()),
        ];
        if let Some(px) = price {
            p.push(("price", dec(px)));
            p.push(("timeInForce", "GTC".into()));
        }
        OrderState::from(
            &self
                .signed(reqwest::Method::POST, self.order_path(), &p)
                .await?,
        )
    }

    /// Состояние ордера.
    pub async fn query(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let p = [("symbol", symbol.to_owned()), ("orderId", id.to_owned())];
        OrderState::from(
            &self
                .signed(reqwest::Method::GET, self.order_path(), &p)
                .await?,
        )
    }

    /// Снять ордер; уже исполнен или снят — узнать итог.
    pub async fn cancel(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let p = [("symbol", symbol.to_owned()), ("orderId", id.to_owned())];
        match self
            .signed(reqwest::Method::DELETE, self.order_path(), &p)
            .await
        {
            Ok(v) => OrderState::from(&v),
            Err(_) => self.query(symbol, id).await,
        }
    }
}

impl Client {
    /// Позиции перпов: символ, количество со знаком, цена входа.
    pub async fn positions(&self) -> Result<Vec<(String, f64, f64)>, String> {
        let v = self
            .signed(reqwest::Method::GET, "/fapi/v2/positionRisk", &[])
            .await?;
        Ok(v.as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| {
                let n = |k: &str| p[k].as_str().and_then(|s| s.parse::<f64>().ok());
                Some((
                    p["symbol"].as_str()?.to_owned(),
                    n("positionAmt")?,
                    n("entryPrice").unwrap_or(0.0),
                ))
            })
            .collect())
    }

    /// Плечо символа на бирже.
    pub async fn set_leverage(&self, symbol: &str, leverage: f64) -> Result<(), String> {
        let p = [
            ("symbol", symbol.to_owned()),
            ("leverage", format!("{}", leverage.round() as u64)),
        ];
        self.signed(reqwest::Method::POST, "/fapi/v1/leverage", &p)
            .await
            .map(|_| ())
    }

    /// Ключ потока событий счёта перпов (только X-MBX-APIKEY, без подписи); PUT — продлить.
    pub async fn listen_key(&self, method: reqwest::Method) -> Result<String, String> {
        let r = self
            .http
            .request(method, format!("{}/fapi/v1/listenKey", self.base))
            .header("X-MBX-APIKEY", &self.keys.key)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let v: Value = r.json().await.map_err(|e| e.to_string())?;
        v["listenKey"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| format!("нет listenKey: {v}"))
    }
}

impl Client {
    /// Сети вывода и депозита монеты.
    pub async fn networks(&self, coin: &str) -> Result<Vec<crate::transfer::Net>, String> {
        let v = self
            .signed(reqwest::Method::GET, "/sapi/v1/capital/config/getall", &[])
            .await?;
        Ok(crate::transfer::binance_nets(&v, coin))
    }

    /// Адрес депозита монеты в сети (адрес, тег).
    pub async fn deposit_address(
        &self,
        coin: &str,
        network: &str,
    ) -> Result<(String, String), String> {
        let p = [("coin", coin.to_owned()), ("network", network.to_owned())];
        let v = self
            .signed(reqwest::Method::GET, "/sapi/v1/capital/deposit/address", &p)
            .await?;
        let a = v["address"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("нет адреса: {v}"))?;
        Ok((a.to_owned(), v["tag"].as_str().unwrap_or("").to_owned()))
    }

    /// Вывод; получатель получит amount − комиссия сети. Ответ — номер вывода.
    pub async fn withdraw(
        &self,
        coin: &str,
        network: &str,
        address: &str,
        tag: &str,
        amount: f64,
    ) -> Result<String, String> {
        let mut p = vec![
            ("coin", coin.to_owned()),
            ("network", network.to_owned()),
            ("address", address.to_owned()),
            ("amount", dec(amount)),
        ];
        if !tag.is_empty() {
            p.push(("addressTag", tag.to_owned()));
        }
        let v = self
            .signed(reqwest::Method::POST, "/sapi/v1/capital/withdraw/apply", &p)
            .await?;
        Ok(v["id"].as_str().unwrap_or("").to_owned())
    }
}

/// Адрес потока событий счёта перпов (к нему добавляется /listenKey); BINANCE_FSTREAM_URL — свой адрес.
pub fn fstream_url(testnet: bool) -> String {
    crate::util::env("BINANCE_FSTREAM_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| {
            if testnet {
                "wss://stream.binancefuture.com/ws".into()
            } else {
                "wss://fstream.binance.com/ws".into()
            }
        })
}

/// Адрес WebSocket API Binance (события счёта); переменная BINANCE_WS_API_URL — свой адрес (проверки, прокси).
pub fn ws_api_url(testnet: bool) -> String {
    crate::util::env("BINANCE_WS_API_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| {
            if testnet {
                "wss://ws-api.testnet.binance.vision/ws-api/v3".into()
            } else {
                "wss://ws-api.binance.com:443/ws-api/v3".into()
            }
        })
}

/// Состояние ордера на бирже: статус, исполнено, сумма исполненного в котируемой валюте.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderState {
    /// Номер ордера на бирже (у Bybit — строка).
    pub id: String,
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
                .ok_or_else(|| format!("нет orderId: {v}"))?
                .to_string(),
            status: v["status"].as_str().unwrap_or("").to_owned(),
            executed: n("executedQty"),
            // спот — cummulativeQuoteQty, перпы — cumQuote
            quote: n("cummulativeQuoteQty").max(n("cumQuote")),
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

/// Стоп на бирже (перпы): reduceOnly-стоп по цене маркировки на всю позицию; None — биржа или рынок не поддерживает.
impl Client {
    /// STOP_MARKET closePosition через Algo Order API (id «a:…»), при ошибке — классический /fapi/v1/order («o:…»).
    pub async fn place_stop(
        &self,
        symbol: &str,
        side: crate::paper::Side,
        price: f64,
    ) -> Result<Option<String>, String> {
        if !self.perp {
            return Ok(None);
        }
        let s = if side == crate::paper::Side::Buy {
            "BUY"
        } else {
            "SELL"
        };
        let px = dec(price);
        let a = [
            ("algoType", "CONDITIONAL".to_owned()),
            ("symbol", symbol.to_uppercase()),
            ("side", s.to_owned()),
            ("type", "STOP_MARKET".to_owned()),
            ("triggerPrice", px.clone()),
            ("closePosition", "true".to_owned()),
            ("workingType", "MARK_PRICE".to_owned()),
        ];
        match self
            .signed(reqwest::Method::POST, "/fapi/v1/algoOrder", &a)
            .await
        {
            Ok(v) if !v["algoId"].is_null() => Ok(Some(format!(
                "a:{}",
                v["algoId"]
                    .as_str()
                    .map_or_else(|| v["algoId"].to_string(), str::to_owned)
            ))),
            r => {
                tracing::warn!("[binance] Algo Order API: {r:?} — стоп через /fapi/v1/order");
                let p = [
                    ("symbol", symbol.to_uppercase()),
                    ("side", s.to_owned()),
                    ("type", "STOP_MARKET".to_owned()),
                    ("stopPrice", px),
                    ("closePosition", "true".to_owned()),
                    ("workingType", "MARK_PRICE".to_owned()),
                ];
                let v = self
                    .signed(reqwest::Method::POST, "/fapi/v1/order", &p)
                    .await?;
                Ok(Some(format!("o:{}", v["orderId"])))
            }
        }
    }

    pub async fn cancel_stop(&self, symbol: &str, id: &str) -> Result<(), String> {
        let (path, k, v) = match id.split_once(':') {
            Some(("a", x)) => ("/fapi/v1/algoOrder", "algoId", x),
            Some((_, x)) => ("/fapi/v1/order", "orderId", x),
            None => ("/fapi/v1/order", "orderId", id),
        };
        self.signed(
            reqwest::Method::DELETE,
            path,
            &[("symbol", symbol.to_uppercase()), (k, v.to_owned())],
        )
        .await
        .map(|_| ())
    }
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
                id: "7".into(),
                status: "PARTIALLY_FILLED".into(),
                executed: 0.5,
                quote: 50.25
            }
        );
        assert!(!s.terminal());
    }
}
