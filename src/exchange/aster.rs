//! Подписанный REST Aster (API v3, как в Java AsterRestClient): формат запросов и ответов — как у Binance,
//! подпись — EIP-712 кошельком-агентом. К параметрам добавляются nonce (микросекунды, монотонно), user (адрес основного
//! кошелька, ASTER_API_KEY) и signer (адрес агента из ASTER_API_SECRET); строка параметров подписывается как
//! Message{string msg} с доменом AsterSignTransaction / "1" / chainId 1666 (testnet 714) / нулевой контракт;
//! подпись 0x r‖s‖v — параметр signature. Спот — /api/v3, перпы — /fapi/v3. События счёта — поток по listenKey.

use crate::exchange::binance::{Keys, OrderState, dec};
use crate::exchange::evm::{Wallet, domain, keccak, typed_digest};
use crate::trading::paper::Side;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn rest_url(perp: bool, testnet: bool) -> &'static str {
    match (perp, testnet) {
        (true, false) => "https://fapi.asterdex.com",
        (true, true) => "https://fapi.asterdex-testnet.com",
        (false, false) => "https://sapi.asterdex.com",
        (false, true) => "https://sapi.asterdex-testnet.com",
    }
}

/// Адрес потока событий счёта (к нему добавляется /listenKey); ASTER_STREAM_URL — свой адрес.
pub fn stream_url(perp: bool, testnet: bool) -> String {
    crate::infra::util::env("ASTER_STREAM_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| {
            match (perp, testnet) {
                (true, false) => "wss://fstream.asterdex.com/ws",
                (true, true) => "wss://fstream.asterdex-testnet.com/ws",
                (false, false) => "wss://sstream.asterdex.com/ws",
                (false, true) => "wss://sstream.asterdex-testnet.com/ws",
            }
            .into()
        })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub struct Client {
    base: String,
    pub perp: bool,
    user: String,
    wallet: Wallet,
    domain: [u8; 32],
    nonce: AtomicU64,
    http: reqwest::Client,
}

impl Client {
    pub fn new(base: &str, keys: Keys, perp: bool, testnet: bool) -> Result<Client, String> {
        Ok(Client {
            base: base.trim_end_matches('/').to_owned(),
            perp,
            user: keys.key,
            wallet: Wallet::from_hex(&keys.secret)?,
            domain: domain(
                "AsterSignTransaction",
                "1",
                if testnet { 714 } else { 1666 },
                [0; 20],
            ),
            nonce: AtomicU64::new(0),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP-клиент"),
        })
    }

    fn api(&self) -> &'static str {
        if self.perp { "/fapi/v3" } else { "/api/v3" }
    }

    /// Подпись строки параметров: EIP-712 Message{string msg}.
    pub fn sign(&self, msg: &str) -> String {
        let mut s = keccak(b"Message(string msg)").to_vec();
        s.extend(keccak(msg.as_bytes()));
        self.wallet
            .sign_hex(&typed_digest(&self.domain, &keccak(&s)))
    }

    /// Хеш, который подписывается (для проверок подписи).
    #[cfg(test)]
    pub fn digest(testnet: bool, msg: &str) -> [u8; 32] {
        let mut s = keccak(b"Message(string msg)").to_vec();
        s.extend(keccak(msg.as_bytes()));
        typed_digest(
            &domain(
                "AsterSignTransaction",
                "1",
                if testnet { 714 } else { 1666 },
                [0; 20],
            ),
            &keccak(&s),
        )
    }

    pub async fn signed(
        &self,
        method: reqwest::Method,
        path: &str,
        params: &[(&str, String)],
    ) -> Result<Value, String> {
        // микросекунды, строго возрастающие
        let micros = now_ms() * 1000;
        let prev = self
            .nonce
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |p| {
                Some((p + 1).max(micros))
            })
            .unwrap_or(0);
        let nonce = (prev + 1).max(micros);
        let mut q: Vec<String> = params.iter().map(|(k, v)| format!("{k}={v}")).collect();
        q.push("recvWindow=5000".into());
        q.push(format!("timestamp={}", now_ms()));
        q.push(format!("nonce={nonce}"));
        q.push(format!("user={}", self.user));
        q.push(format!("signer={}", self.wallet.address));
        let query = q.join("&");
        let url = format!(
            "{}{}{path}?{query}&signature={}",
            self.base,
            self.api(),
            self.sign(&query)
        );
        let r = self
            .http
            .request(method, url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .send()
            .await
            .map_err(|e| e.to_string())?;
        crate::infra::ratelimit::sync_headers("aster", r.headers());
        let st = r.status();
        let v: Value = r.json().await.map_err(|e| format!("HTTP {st}: {e}"))?;
        if !st.is_success() || v["code"].as_i64().is_some_and(|c| c < 0) {
            return Err(format!(
                "Aster {}: {}",
                v["code"],
                v["msg"].as_str().unwrap_or("")
            ));
        }
        Ok(v)
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
            ("symbol", symbol.to_owned()),
            (
                "side",
                if side == Side::Buy { "BUY" } else { "SELL" }.into(),
            ),
            ("newClientOrderId", format!("hft{local}")),
            (
                "type",
                if price.is_some() { "LIMIT" } else { "MARKET" }.into(),
            ),
            ("quantity", dec(qty)),
        ];
        if let Some(px) = price {
            p.push(("timeInForce", "GTC".into()));
            p.push(("price", dec(px)));
        }
        OrderState::from(&self.signed(reqwest::Method::POST, "/order", &p).await?)
    }

    pub async fn query(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        OrderState::from(
            &self
                .signed(
                    reqwest::Method::GET,
                    "/order",
                    &[("symbol", symbol.to_owned()), ("orderId", id.to_owned())],
                )
                .await?,
        )
    }

    pub async fn cancel(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let p = [("symbol", symbol.to_owned()), ("orderId", id.to_owned())];
        match self.signed(reqwest::Method::DELETE, "/order", &p).await {
            Ok(v) => OrderState::from(&v),
            Err(_) => self.query(symbol, id).await,
        }
    }

    /// Свободные балансы (перпы — доступная маржа).
    pub async fn balances(&self) -> Result<BTreeMap<String, f64>, String> {
        let n = |x: &Value| {
            x.as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0)
        };
        if self.perp {
            let v = self.signed(reqwest::Method::GET, "/balance", &[]).await?;
            return Ok(v
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|b| Some((b["asset"].as_str()?.to_owned(), n(&b["availableBalance"]))))
                .filter(|(_, f)| *f > 0.0)
                .collect());
        }
        let v = self.signed(reqwest::Method::GET, "/account", &[]).await?;
        Ok(v["balances"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|b| Some((b["asset"].as_str()?.to_owned(), n(&b["free"]))))
            .filter(|(_, f)| *f > 0.0)
            .collect())
    }

    pub async fn positions(&self) -> Result<Vec<(String, f64, f64)>, String> {
        let v = self
            .signed(reqwest::Method::GET, "/positionRisk", &[])
            .await?;
        let n = |x: &Value| x.as_str().and_then(|s| s.parse::<f64>().ok());
        Ok(v.as_array()
            .into_iter()
            .flatten()
            .filter_map(|p| {
                Some((
                    p["symbol"].as_str()?.to_owned(),
                    n(&p["positionAmt"])?,
                    n(&p["entryPrice"]).unwrap_or(0.0),
                ))
            })
            .collect())
    }

    pub async fn set_leverage(&self, symbol: &str, leverage: f64) -> Result<(), String> {
        let p = [
            ("symbol", symbol.to_owned()),
            ("leverage", format!("{}", leverage.round() as u64)),
        ];
        self.signed(reqwest::Method::POST, "/leverage", &p)
            .await
            .map(|_| ())
    }

    /// Ключ потока событий счёта; PUT — продлить.
    pub async fn listen_key(&self, method: reqwest::Method) -> Result<String, String> {
        let v = self.signed(method, "/listenKey", &[]).await?;
        v["listenKey"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| format!("нет listenKey: {v}"))
    }
}

/// Стоп на бирже (перпы): reduceOnly-стоп по цене маркировки на всю позицию; None — биржа или рынок не поддерживает.
impl Client {
    /// STOP_MARKET closePosition по маркировке, формат Binance.
    pub async fn place_stop(
        &self,
        symbol: &str,
        side: Side,
        price: f64,
    ) -> Result<Option<String>, String> {
        if !self.perp {
            return Ok(None);
        }
        let p = [
            ("symbol", symbol.to_uppercase()),
            (
                "side",
                if side == Side::Buy { "BUY" } else { "SELL" }.to_owned(),
            ),
            ("type", "STOP_MARKET".to_owned()),
            ("stopPrice", dec(price)),
            ("closePosition", "true".to_owned()),
            ("workingType", "MARK_PRICE".to_owned()),
        ];
        let v = self.signed(reqwest::Method::POST, "/order", &p).await?;
        Ok(Some(
            v["orderId"]
                .as_i64()
                .map(|x| x.to_string())
                .unwrap_or_else(|| v["orderId"].to_string()),
        ))
    }

    pub async fn cancel_stop(&self, symbol: &str, id: &str) -> Result<(), String> {
        self.signed(
            reqwest::Method::DELETE,
            "/order",
            &[
                ("symbol", symbol.to_uppercase()),
                ("orderId", id.to_owned()),
            ],
        )
        .await
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_recovers_agent() {
        let keys = Keys {
            key: "0xmain".into(),
            secret: "0x0000000000000000000000000000000000000000000000000000000000000002".into(),
        };
        let c = Client::new("https://x", keys, false, true).unwrap();
        let msg =
            "symbol=BTCUSDT&side=BUY&nonce=1&user=0xmain&signer=".to_owned() + &c.wallet.address;
        let sig = c.sign(&msg);
        assert_eq!(
            crate::exchange::evm::recover(&Client::digest(true, &msg), &sig).unwrap(),
            c.wallet.address
        );
        // другая сеть — другой домен
        assert_ne!(
            crate::exchange::evm::recover(&Client::digest(false, &msg), &sig).unwrap(),
            c.wallet.address
        );
    }
}
