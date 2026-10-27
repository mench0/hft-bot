//! BingX: снимки depth20 (кадры сжаты gzip). Спот — BTC-USDT@depth20, пинг сервера {"ping","time"} → {"pong","time"};
//! перпы — BTC-USDT@depth20@500ms на swap-market, пинг сервера — текст Ping → Pong.

use super::{Dialect, Parsed, Update, levels};
use crate::trading::paper::split;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::Read;

pub fn default_url(perp: bool, testnet: bool) -> &'static str {
    match (perp, testnet) {
        (true, true) => "wss://vst-open-api-ws.bingx.com/swap-market",
        (true, false) => "wss://open-api-swap.bingx.com/swap-market",
        _ => "wss://open-api-ws.bingx.com/market", // у спота тестовой сети нет
    }
}

/// Кадр gzip в текст; несжатый — как есть.
pub fn gunzip(data: &[u8]) -> Option<String> {
    if data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b {
        let mut s = String::new();
        flate2::read::GzDecoder::new(data)
            .read_to_string(&mut s)
            .ok()?;
        Some(s)
    } else {
        String::from_utf8(data.to_vec()).ok()
    }
}

pub struct Bingx {
    url: String,
    perp: bool,
    /// BTC-USDT → символ бота.
    symbols: HashMap<String, String>,
}

impl Bingx {
    pub fn new(url: String, symbols: &[String], perp: bool) -> Bingx {
        let symbols = symbols
            .iter()
            .map(|s| {
                (
                    split(s).map_or_else(|| s.to_uppercase(), |(b, q)| format!("{b}-{q}")),
                    s.to_uppercase(),
                )
            })
            .collect();
        Bingx { url, perp, symbols }
    }
}

impl Dialect for Bingx {
    fn url(&self) -> String {
        self.url.clone()
    }

    fn subscribe(&self) -> Vec<String> {
        let mut v: Vec<&String> = self.symbols.keys().collect();
        v.sort();
        v.iter()
            .enumerate()
            .map(|(i, s)| {
                let dt = if self.perp {
                    format!("{s}@depth20@500ms")
                } else {
                    format!("{s}@depth20")
                };
                json!({"id": format!("hft{i}"), "reqType": "sub", "dataType": dt}).to_string()
            })
            .collect()
    }

    fn binary(&self, data: &[u8]) -> Option<String> {
        gunzip(data)
    }

    fn reply(&self, text: &str) -> Option<String> {
        if text.eq_ignore_ascii_case("ping") {
            return Some("Pong".into());
        }
        let v: Value = serde_json::from_str(text).ok()?;
        v.get("ping")
            .map(|p| json!({"pong": p, "time": v["time"]}).to_string())
    }

    fn parse(&self, text: &str) -> Parsed {
        if text.eq_ignore_ascii_case("pong") {
            return Ok(vec![]);
        }
        let v: Value = serde_json::from_str(text).map_err(|e| format!("BingX: {e}"))?;
        if v["code"].as_i64().is_some_and(|c| c != 0) {
            return Err(format!("BingX error {}: {}", v["code"], v["msg"]));
        }
        let d = &v["data"];
        if !d.is_object() {
            return Ok(vec![]); // ответ на подписку
        }
        let dt = v["dataType"].as_str().unwrap_or("");
        let Some((venue, kind)) = dt.split_once('@') else {
            return Ok(vec![]);
        };
        if !kind.starts_with("depth") {
            return Err(format!("BingX: не стакан {dt}"));
        }
        let Some(sym) = self.symbols.get(venue) else {
            return Ok(vec![]);
        };
        Ok(vec![Update::Snapshot {
            sym: sym.clone(),
            bids: levels(d.get("bids")),
            asks: levels(d.get("asks")),
            id: 0,
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn gzip_ping_and_parse() {
        let d = Bingx::new("wss://x".into(), &["BTCUSDT".into()], true);
        assert!(d.subscribe()[0].contains(r#""dataType":"BTC-USDT@depth20@500ms""#));
        let m = r#"{"code":0,"dataType":"BTC-USDT@depth20@500ms","data":{"bids":[["100","1"]],"asks":[["101","2"]]}}"#;
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(m.as_bytes()).unwrap();
        let t = d.binary(&gz.finish().unwrap()).unwrap();
        assert_eq!(
            d.parse(&t),
            Ok(vec![Update::Snapshot {
                sym: "BTCUSDT".into(),
                bids: vec![(100.0, 1.0)],
                asks: vec![(101.0, 2.0)],
                id: 0
            }])
        );
        assert_eq!(d.reply("Ping").as_deref(), Some("Pong"));
        assert_eq!(
            d.reply(r#"{"ping":"x1","time":"t"}"#).as_deref(),
            Some(r#"{"pong":"x1","time":"t"}"#)
        );
        assert_eq!(
            d.parse(r#"{"id":"hft0","code":0,"msg":"","dataType":"","data":null}"#),
            Ok(vec![])
        );
        assert!(d.parse(r#"{"code":100400,"msg":"bad"}"#).is_err());
    }
}
