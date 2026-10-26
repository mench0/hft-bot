//! MEXC. Спот (WebSocket v3, wss://wbs-api.mexc.com/ws): канал spot@public.limit.depth.v3.api.pb@SYMBOL@20 —
//! снимки 20 уровней в protobuf (бинарные кадры), служебные ответы — текст JSON; не больше 30 подписок на соединение.
//! Разбор protobuf свой, только нужные поля (схема mexcdevelop/websocket-proto):
//! PushDataV3ApiWrapper { string channel = 1; string symbol = 3; int64 createTime = 5; int64 sendTime = 6;
//! PublicLimitDepthsV3Api publicLimitDepths = 303 }, PublicLimitDepthsV3Api { repeated Item asks = 1; bids = 2 },
//! Item { string price = 1; string quantity = 2 }.
//! Перпы (wss://contract.mexc.com/edge): sub.depth.full по контракту BTC_USDT, JSON, объём в контрактах × contractSize
//! (/api/v1/contract/detail). Пинг {"method":"ping"}.

use super::{Dialect, Parsed, Update, levels};
use crate::paper::split;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

pub const SPOT_URL: &str = "wss://wbs-api.mexc.com/ws";
pub const FUTURES_URL: &str = "wss://contract.mexc.com/edge";
pub const FUTURES_REST: &str = "https://contract.mexc.com";
/// Подписок на одно соединение у MEXC.
pub const MAX_SUBSCRIPTIONS: usize = 30;

pub struct MexcSpot {
    url: String,
    symbols: Vec<String>,
}

impl MexcSpot {
    pub fn new(url: String, symbols: &[String]) -> MexcSpot {
        if symbols.len() > MAX_SUBSCRIPTIONS {
            tracing::warn!(
                "[mexc] не больше {MAX_SUBSCRIPTIONS} подписок на соединение — {} символов без стакана",
                symbols.len() - MAX_SUBSCRIPTIONS
            );
        }
        MexcSpot {
            url,
            symbols: symbols
                .iter()
                .take(MAX_SUBSCRIPTIONS)
                .map(|s| s.to_uppercase())
                .collect(),
        }
    }
}

/// Читатель protobuf: varint, поля по типу.
struct Pb<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Pb<'a> {
    fn varint(&mut self) -> Result<u64, String> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let x = *self.b.get(self.pos).ok_or("MEXC: обрезанный varint")?;
            self.pos += 1;
            v |= u64::from(x & 0x7f) << shift;
            if x & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err("MEXC: слишком длинный varint".into())
    }

    /// Следующее поле: (номер, тип, байты для типа 2, число для типа 0).
    fn field(&mut self) -> Result<(u64, u64, &'a [u8], u64), String> {
        let tag = self.varint()?;
        let (f, w) = (tag >> 3, tag & 7);
        match w {
            0 => Ok((f, w, &[], self.varint()?)),
            2 => {
                let n = self.varint()? as usize;
                let s = self
                    .b
                    .get(self.pos..self.pos + n)
                    .ok_or("MEXC: обрезанное сообщение")?;
                self.pos += n;
                Ok((f, w, s, 0))
            }
            1 => {
                self.pos += 8;
                Ok((f, w, &[], 0))
            }
            5 => {
                self.pos += 4;
                Ok((f, w, &[], 0))
            }
            _ => Err(format!("MEXC: неподдерживаемый тип поля protobuf {w}")),
        }
    }

    fn done(&self) -> bool {
        self.pos >= self.b.len()
    }
}

fn num(b: &[u8]) -> Result<f64, String> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "MEXC: не число".into())
}

/// PushDataV3ApiWrapper со снимком стакана → обновление; чужие каналы — пусто.
pub fn parse_pb(data: &[u8]) -> Parsed {
    let mut p = Pb { b: data, pos: 0 };
    let (mut sym, mut depth, mut ts) = (None, None, 0u64);
    while !p.done() {
        match p.field()? {
            (3, 2, s, _) => sym = Some(String::from_utf8_lossy(s).to_uppercase()),
            (303, 2, s, _) => depth = Some(s),
            (5, 0, _, v) if ts == 0 => ts = v,
            (6, 0, _, v) => ts = v,
            _ => {}
        }
    }
    let Some(depth) = depth else {
        return Ok(vec![]);
    };
    let sym = sym.ok_or("MEXC: нет symbol в сообщении стакана")?;
    let (mut bids, mut asks) = (Vec::new(), Vec::new());
    let mut d = Pb { b: depth, pos: 0 };
    while !d.done() {
        let (f, w, item, _) = d.field()?;
        if w != 2 || !(f == 1 || f == 2) {
            continue;
        }
        let mut it = Pb { b: item, pos: 0 };
        let (mut px, mut q) = (None, None);
        while !it.done() {
            match it.field()? {
                (1, 2, s, _) => px = Some(num(s)?),
                (2, 2, s, _) => q = Some(num(s)?),
                _ => {}
            }
        }
        let l = (
            px.ok_or("MEXC: уровень без цены")?,
            q.ok_or("MEXC: уровень без объёма")?,
        );
        if f == 2 { bids.push(l) } else { asks.push(l) }
    }
    Ok(vec![Update::Snapshot {
        sym,
        bids,
        asks,
        id: ts,
    }])
}

impl Dialect for MexcSpot {
    fn url(&self) -> String {
        self.url.clone()
    }

    fn subscribe(&self) -> Vec<String> {
        let params: Vec<String> = self
            .symbols
            .iter()
            .map(|s| format!("spot@public.limit.depth.v3.api.pb@{s}@20"))
            .collect();
        vec![json!({"method": "SUBSCRIPTION", "params": params}).to_string()]
    }

    fn ping(&self) -> Option<(String, Duration)> {
        Some((r#"{"method":"PING"}"#.into(), Duration::from_secs(20)))
    }

    fn parse_binary(&self, data: &[u8]) -> Option<Parsed> {
        Some(parse_pb(data))
    }

    /// Текст — только служебное: подтверждение подписки, PONG, ошибки.
    fn parse(&self, text: &str) -> Parsed {
        if text.contains("Not Subscribed") || text.contains("Blocked") {
            return Err(format!("MEXC: подписка отклонена: {text}"));
        }
        let v: Value = serde_json::from_str(text).unwrap_or(Value::Null);
        if v["code"].as_i64().is_some_and(|c| c != 0) {
            return Err(format!("MEXC: {text}"));
        }
        Ok(vec![])
    }
}

pub struct MexcFutures {
    url: String,
    rest: String,
    /// Контракт (BTC_USDT) → (символ бота, размер контракта).
    contracts: Mutex<HashMap<String, (String, f64)>>,
    symbols: Vec<String>,
}

impl MexcFutures {
    pub fn new(rest: String, url: String, symbols: &[String]) -> MexcFutures {
        MexcFutures {
            url,
            rest: rest.trim_end_matches('/').to_owned(),
            contracts: Mutex::default(),
            symbols: symbols.to_vec(),
        }
    }

    pub fn map_contracts(&self, v: &Value) {
        let mut m = self.contracts.lock().unwrap();
        for s in &self.symbols {
            let Some((b, q)) = split(s) else { continue };
            let id = format!("{b}_{q}");
            let size = v["data"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|c| c["symbol"] == id.as_str())
                .and_then(|c| c["contractSize"].as_f64());
            if let Some(k) = size {
                m.insert(id, (s.to_uppercase(), k));
            }
        }
    }
}

impl Dialect for MexcFutures {
    fn url(&self) -> String {
        self.url.clone()
    }

    fn connect_url(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send + '_>>
    {
        Box::pin(async move {
            if self.contracts.lock().unwrap().is_empty() {
                let v: Value = reqwest::get(format!("{}/api/v1/contract/detail", self.rest))
                    .await
                    .map_err(|e| e.to_string())?
                    .json()
                    .await
                    .map_err(|e| e.to_string())?;
                self.map_contracts(&v);
            }
            Ok(self.url.clone())
        })
    }

    fn subscribe(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.contracts.lock().unwrap().keys().cloned().collect();
        ids.sort();
        ids.iter()
            .map(|s| {
                json!({"method": "sub.depth.full", "param": {"symbol": s, "limit": 20}}).to_string()
            })
            .collect()
    }

    fn ping(&self) -> Option<(String, Duration)> {
        Some((r#"{"method":"ping"}"#.into(), Duration::from_secs(15)))
    }

    fn parse(&self, text: &str) -> Parsed {
        let v: Value = serde_json::from_str(text).map_err(|e| format!("MEXC futures: {e}"))?;
        let ch = v["channel"].as_str().unwrap_or("");
        if ch == "rs.error" {
            return Err(format!("MEXC futures error: {}", v["data"]));
        }
        if ch == "pong" || ch.starts_with("rs.") {
            return Ok(vec![]);
        }
        if ch != "push.depth.full" && ch != "push.depth" {
            return Err(format!(
                "MEXC futures: неожиданное сообщение {}",
                &text[..text.len().min(200)]
            ));
        }
        let id = v["symbol"].as_str().ok_or("MEXC futures: нет контракта")?;
        let Some((sym, k)) = self.contracts.lock().unwrap().get(id).cloned() else {
            return Ok(vec![]);
        };
        let d = &v["data"];
        let scale = |l: Vec<(f64, f64)>| l.into_iter().map(|(p, q)| (p, q * k)).collect();
        Ok(vec![Update::Snapshot {
            sym,
            bids: scale(levels(d.get("bids"))),
            asks: scale(levels(d.get("asks"))),
            id: v["ts"].as_u64().unwrap_or(0),
        }])
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

    fn item(px: &str, q: &str) -> Vec<u8> {
        let mut v = Vec::new();
        bytes(1, px.as_bytes(), &mut v);
        bytes(2, q.as_bytes(), &mut v);
        v
    }

    #[test]
    fn protobuf_depth() {
        let mut depth = Vec::new();
        bytes(1, &item("101.5", "2"), &mut depth);
        bytes(2, &item("100", "1.25"), &mut depth);
        bytes(3, b"spot@public.limit.depth.v3.api.pb", &mut depth);
        let mut msg = Vec::new();
        bytes(1, b"spot@public.limit.depth.v3.api.pb@BTCUSDT@20", &mut msg);
        bytes(3, b"BTCUSDT", &mut msg);
        varint(6 << 3, &mut msg);
        varint(1_700_000_000_123, &mut msg);
        bytes(303, &depth, &mut msg);
        let d = MexcSpot::new(SPOT_URL.into(), &["BTCUSDT".into()]);
        assert_eq!(
            d.parse_binary(&msg),
            Some(Ok(vec![Update::Snapshot {
                sym: "BTCUSDT".into(),
                bids: vec![(100.0, 1.25)],
                asks: vec![(101.5, 2.0)],
                id: 1_700_000_000_123
            }]))
        );
        // чужой канал без стакана — пусто; обрезанное — ошибка
        let mut other = Vec::new();
        bytes(3, b"BTCUSDT", &mut other);
        assert_eq!(parse_pb(&other), Ok(vec![]));
        assert!(parse_pb(&msg[..msg.len() - 3]).is_err());
        assert!(d.subscribe()[0].contains("spot@public.limit.depth.v3.api.pb@BTCUSDT@20"));
        assert!(
            d.parse(r#"{"id":0,"code":0,"msg":"spot@public..."}"#)
                .unwrap()
                .is_empty()
        );
        assert!(
            d.parse(r#"{"id":0,"code":0,"msg":"Not Subscribed successfully! [x]"}"#)
                .is_err()
        );
    }

    #[test]
    fn futures_json() {
        let d = MexcFutures::new(FUTURES_REST.into(), FUTURES_URL.into(), &["BTCUSDT".into()]);
        d.map_contracts(&json!({"data": [{"symbol": "BTC_USDT", "contractSize": 0.0001}]}));
        assert!(d.subscribe()[0].contains(r#""symbol":"BTC_USDT""#));
        let m = r#"{"channel":"push.depth.full","data":{"asks":[[101,20000,1]],"bids":[[100,10000,1]],"version":5},"symbol":"BTC_USDT","ts":9}"#;
        assert_eq!(
            d.parse(m),
            Ok(vec![Update::Snapshot {
                sym: "BTCUSDT".into(),
                bids: vec![(100.0, 1.0)],
                asks: vec![(101.0, 2.0)],
                id: 9
            }])
        );
        assert_eq!(d.parse(r#"{"channel":"pong","data":1}"#), Ok(vec![]));
        assert!(d.parse(r#"{"channel":"rs.error","data":"bad"}"#).is_err());
    }
}
