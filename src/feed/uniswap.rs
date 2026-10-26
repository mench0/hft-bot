//! Uniswap V2 через ноду Ethereum по WebSocket, как в Java: подписка eth_subscribe("logs") на событие
//! Sync(uint112,uint112) нужных пар — каждый своп меняет резервы и сразу приходит; первый снимок — eth_call
//! getReserves() на пару; раз в 15 с — eth_blockNumber как сердцебиение. Стакан синтетический по формуле x·y=k
//! с комиссией пула 0.3% (10 ступеней по 0.2% резерва в каждую сторону).
//! Пулы — параметр uniPools: ТИКЕР=0xадрес_пары:база_это_token0:десятичные_базы:десятичные_котир;…

use super::{Dialect, Parsed, Update};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Duration;

/// keccak256("Sync(uint112,uint112)").
pub const SYNC_TOPIC: &str = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1";
pub const DEFAULT_URL: &str = "wss://ethereum-rpc.publicnode.com";
const FEE: f64 = 0.003;
const STEPS: usize = 10;

#[derive(Clone, Debug, PartialEq)]
pub struct Pool {
    pub symbol: String,
    pub pair: String,
    pub base_is_token0: bool,
    pub dec_base: i32,
    pub dec_quote: i32,
}

/// Разбор uniPools: ТИКЕР=0xпара:true:18:6;…
pub fn parse_pools(spec: &str) -> Result<Vec<Pool>, String> {
    spec.split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|item| {
            let (sym, rest) = item
                .split_once('=')
                .ok_or_else(|| format!("uniPools: нет «=» в {item}"))?;
            let f: Vec<&str> = rest.split(':').collect();
            if f.len() != 4 || !f[0].starts_with("0x") {
                return Err(format!(
                    "uniPools: ожидается 0xпара:база_это_token0:десятичные:десятичные в {item}"
                ));
            }
            Ok(Pool {
                symbol: sym.trim().to_uppercase(),
                pair: f[0].to_lowercase(),
                base_is_token0: f[1].eq_ignore_ascii_case("true"),
                dec_base: f[2]
                    .parse()
                    .map_err(|_| format!("uniPools: десятичные базы в {item}"))?,
                dec_quote: f[3]
                    .parse()
                    .map_err(|_| format!("uniPools: десятичные котировки в {item}"))?,
            })
        })
        .collect()
}

/// Синтетический стакан по резервам (x·y=k): (биды, аски).
pub fn book(p: &Pool, r0: u128, r1: u128) -> Result<crate::screener::Levels, String> {
    let (rb, rq) = if p.base_is_token0 { (r0, r1) } else { (r1, r0) };
    let base = rb as f64 / 10f64.powi(p.dec_base);
    let quote = rq as f64 / 10f64.powi(p.dec_quote);
    if base <= 0.0 || quote <= 0.0 {
        return Err("пустой пул".into());
    }
    let (mut asks, mut bids) = (Vec::new(), Vec::new());
    let (mut pb, mut pq) = (0.0, 0.0);
    for i in 1..=STEPS {
        // покупка базы за котировку
        let q_in = quote * 0.002 * i as f64;
        let out = base - base * quote / (quote + q_in * (1.0 - FEE));
        asks.push(((q_in - pq) / (out - pb), out - pb));
        (pb, pq) = (out, q_in);
    }
    (pb, pq) = (0.0, 0.0);
    for i in 1..=STEPS {
        // продажа базы за котировку
        let b_in = base * 0.002 * i as f64;
        let out = quote - base * quote / (base + b_in * (1.0 - FEE));
        bids.push(((out - pq) / (b_in - pb), b_in - pb));
        (pb, pq) = (b_in, out);
    }
    Ok((bids, asks))
}

/// Резервы из данных Sync / getReserves: два слова по 32 байта (uint112 помещается в u128).
fn reserves(hex: &str) -> Result<(u128, u128), String> {
    let h = hex.strip_prefix("0x").ok_or("Uniswap: данные без 0x")?;
    if h.len() < 128 {
        return Err(format!("Uniswap: короткие данные {h}"));
    }
    let word = |s: &str| u128::from_str_radix(&s[32..64], 16).map_err(|e| e.to_string());
    Ok((word(&h[0..64])?, word(&h[64..128])?))
}

pub struct Uniswap {
    url: String,
    /// Адрес пары (нижний регистр) → пул.
    pools: HashMap<String, Pool>,
}

impl Uniswap {
    pub fn new(url: String, pools: Vec<Pool>, symbols: &[String]) -> Uniswap {
        let pools = pools
            .into_iter()
            .filter(|p| symbols.contains(&p.symbol))
            .map(|p| (p.pair.clone(), p))
            .collect();
        Uniswap { url, pools }
    }

    fn fill(&self, addr: &str, data: &str) -> Parsed {
        let p = self
            .pools
            .get(&addr.to_lowercase())
            .ok_or_else(|| format!("Uniswap: неизвестная пара {addr}"))?;
        let (r0, r1) = reserves(data)?;
        let (bids, asks) = book(p, r0, r1)?;
        Ok(vec![Update::Snapshot {
            sym: p.symbol.clone(),
            bids,
            asks,
            id: 0,
        }])
    }
}

impl Dialect for Uniswap {
    fn url(&self) -> String {
        self.url.clone()
    }

    fn subscribe(&self) -> Vec<String> {
        let mut addrs: Vec<&String> = self.pools.keys().collect();
        addrs.sort();
        let mut out = vec![
            json!({"jsonrpc": "2.0", "id": "sub:logs", "method": "eth_subscribe",
            "params": ["logs", {"address": addrs, "topics": [SYNC_TOPIC]}]})
            .to_string(),
        ];
        for a in addrs {
            out.push(
                json!({"jsonrpc": "2.0", "id": format!("call:{a}"), "method": "eth_call",
                "params": [{"to": a, "data": "0x0902f1ac"}, "latest"]})
                .to_string(),
            );
        }
        out
    }

    fn ping(&self) -> Option<(String, Duration)> {
        Some((
            r#"{"jsonrpc":"2.0","id":"hb","method":"eth_blockNumber","params":[]}"#.into(),
            Duration::from_secs(15),
        ))
    }

    fn parse(&self, text: &str) -> Parsed {
        let v: Value = serde_json::from_str(text).map_err(|e| format!("Uniswap: {e}"))?;
        if v["method"] == "eth_subscription" {
            let r = &v["params"]["result"];
            if r["removed"] == true {
                return Ok(vec![]); // реорг: следующий Sync исправит
            }
            return self.fill(
                r["address"].as_str().unwrap_or(""),
                r["data"].as_str().unwrap_or(""),
            );
        }
        if !v["error"].is_null() {
            return Err(format!(
                "RPC error {}: {}",
                v["error"]["code"], v["error"]["message"]
            ));
        }
        match v["id"].as_str() {
            Some("hb" | "sub:logs") => Ok(vec![]),
            Some(id) if id.starts_with("call:") => {
                self.fill(&id[5..], v["result"].as_str().unwrap_or(""))
            }
            _ => Err(format!(
                "Uniswap WS: неожиданное сообщение {}",
                &text[..text.len().min(200)]
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pools_reserves_and_book() {
        let pools =
            parse_pools("WETHUSDC=0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc:false:18:6").unwrap();
        assert_eq!(pools[0].pair, "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc");
        assert!(parse_pools("X=bad").is_err());
        // token0 = USDC (2 000 000), token1 = WETH (1 000) — цена 2000
        let d = Uniswap::new(DEFAULT_URL.into(), pools, &["WETHUSDC".into()]);
        let hex = format!(
            "0x{:064x}{:064x}",
            2_000_000u128 * 1_000_000,
            1_000u128 * 10u128.pow(18)
        );
        let ups = d.parse(&format!(r#"{{"jsonrpc":"2.0","id":"call:0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc","result":"{hex}"}}"#)).unwrap();
        let Update::Snapshot {
            bids, asks, sym, ..
        } = &ups[0]
        else {
            panic!()
        };
        assert_eq!(sym, "WETHUSDC");
        assert!(bids[0].0 < 2000.0 && bids[0].0 > 1980.0, "{bids:?}");
        assert!(asks[0].0 > 2000.0 && asks[0].0 < 2020.0, "{asks:?}");
        assert!(
            asks[1].0 > asks[0].0 && bids[1].0 < bids[0].0,
            "ступени хуже"
        );
        let sync = format!(
            r#"{{"jsonrpc":"2.0","method":"eth_subscription","params":{{"result":{{"address":"0xB4E16D0168E52D35CACD2C6185B44281EC28C9DC","data":"{hex}","removed":false}}}}}}"#
        );
        assert_eq!(d.parse(&sync).unwrap().len(), 1);
        assert_eq!(
            d.parse(r#"{"jsonrpc":"2.0","id":"hb","result":"0x10"}"#),
            Ok(vec![])
        );
        assert!(
            d.parse(r#"{"jsonrpc":"2.0","id":"sub:logs","error":{"code":-32000,"message":"x"}}"#)
                .is_err()
        );
        assert!(d.subscribe()[0].contains(SYNC_TOPIC));
    }
}
