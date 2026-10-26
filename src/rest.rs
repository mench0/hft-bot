//! REST Binance (спот `/api/v3`, перпы `/fapi/v1`): правила торговли пар и минутные свечи; ставки funding Binance и Bybit.

use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

/// Правила торговли пары: шаг цены и объёма, минимальный объём и сумма ордера.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Filter {
    pub tick: f64,
    pub step: f64,
    pub min_qty: f64,
    pub min_notional: f64,
}

impl Filter {
    /// Объём вниз до шага.
    pub fn round_qty(&self, q: f64) -> f64 {
        if self.step > 0.0 {
            ((q / self.step) + 1e-9).floor() * self.step
        } else {
            q
        }
    }

    /// Цена к шагу: покупка — вниз, продажа — вверх (не хуже желаемой).
    pub fn round_price(&self, p: f64, up: bool) -> f64 {
        if self.tick <= 0.0 {
            return p;
        }
        let n = p / self.tick;
        (if up {
            (n - 1e-9).ceil()
        } else {
            (n + 1e-9).floor()
        }) * self.tick
    }
}

/// Адрес REST по умолчанию.
pub fn default_url(perp: bool, testnet: bool) -> &'static str {
    match (perp, testnet) {
        (false, false) => "https://api.binance.com",
        (false, true) => "https://testnet.binance.vision",
        (true, false) => "https://fapi.binance.com",
        (true, true) => "https://testnet.binancefuture.com",
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("HTTP-клиент")
}

#[derive(Deserialize)]
struct Info {
    symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
struct SymbolInfo {
    symbol: String,
    filters: Vec<serde_json::Value>,
}

fn num(v: &serde_json::Value, k: &str) -> f64 {
    v.get(k)
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0)
}

/// Правила пар (только запрошенных символов).
pub async fn filters(
    base: &str,
    perp: bool,
    symbols: &[String],
) -> Result<HashMap<String, Filter>, String> {
    let path = if perp {
        "/fapi/v1/exchangeInfo"
    } else {
        "/api/v3/exchangeInfo"
    };
    let url = format!("{}{path}", base.trim_end_matches('/'));
    let info: Info = client()
        .get(&url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    let mut out = HashMap::new();
    for s in info
        .symbols
        .into_iter()
        .filter(|s| symbols.contains(&s.symbol))
    {
        let mut f = Filter::default();
        for x in &s.filters {
            match x.get("filterType").and_then(|t| t.as_str()) {
                Some("PRICE_FILTER") => f.tick = num(x, "tickSize"),
                Some("LOT_SIZE") => {
                    f.step = num(x, "stepSize");
                    f.min_qty = num(x, "minQty");
                }
                Some("NOTIONAL") | Some("MIN_NOTIONAL") => {
                    f.min_notional = num(x, "minNotional").max(num(x, "notional"));
                }
                _ => {}
            }
        }
        out.insert(s.symbol, f);
    }
    Ok(out)
}

/// Оборот (в котируемой валюте) последних закрытых минутных свечей, по возрастанию времени.
pub async fn minute_volumes(
    base: &str,
    perp: bool,
    symbol: &str,
    minutes: usize,
) -> Result<Vec<f64>, String> {
    let path = if perp {
        "/fapi/v1/klines"
    } else {
        "/api/v3/klines"
    };
    let url = format!(
        "{}{path}?symbol={symbol}&interval=1m&limit={}",
        base.trim_end_matches('/'),
        minutes + 1
    );
    let rows: Vec<Vec<serde_json::Value>> = client()
        .get(&url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    Ok(rows
        .iter()
        .filter(|r| {
            r.get(6)
                .and_then(|c| c.as_u64())
                .is_some_and(|close| close < now)
        }) // только закрытые минуты
        .map(|r| {
            r.get(7)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0)
        })
        .collect())
}

/// Ставка funding перпа: Binance и Aster — /fapi/v1/premiumIndex, Bybit — /v5/market/tickers (linear),
/// OKX — /api/v5/public/funding-rate, Gate — /futures/usdt/contracts, BingX — /openApi/swap/v2/quote/premiumIndex,
/// KuCoin — /api/v1/funding-rate/{контракт}/current, Hyperliquid — POST /info metaAndAssetCtxs (раз в час).
pub async fn funding(ex: &str, base: &str, symbol: &str) -> Result<crate::market::Funding, String> {
    let base = base.trim_end_matches('/');
    let (b, q) = crate::paper::split(symbol).unwrap_or((symbol.to_owned(), String::new()));
    let get = |url: String| async move {
        client()
            .get(&url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| e.to_string())?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| e.to_string())
    };
    let v = match ex {
        "hyperliquid" => client()
            .post(format!("{base}/info"))
            .json(&serde_json::json!({"type": "metaAndAssetCtxs"}))
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| e.to_string())?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| e.to_string())?,
        "bybit" => {
            get(format!(
                "{base}/v5/market/tickers?category=linear&symbol={symbol}"
            ))
            .await?
        }
        "okx" => {
            get(format!(
                "{base}/api/v5/public/funding-rate?instId={b}-{q}-SWAP"
            ))
            .await?
        }
        "gate" => get(format!("{base}/api/v4/futures/usdt/contracts/{b}_{q}")).await?,
        "bingx" => {
            get(format!(
                "{base}/openApi/swap/v2/quote/premiumIndex?symbol={b}-{q}"
            ))
            .await?
        }
        "kucoin" => {
            let c = if b == "BTC" { "XBT" } else { b.as_str() };
            get(format!("{base}/api/v1/funding-rate/{c}{q}M/current")).await?
        }
        _ => get(format!("{base}/fapi/v1/premiumIndex?symbol={symbol}")).await?,
    };
    parse_funding(ex, &b, &v)
}

/// Разбор ответа о ставке funding.
pub fn parse_funding(
    ex: &str,
    coin: &str,
    v: &serde_json::Value,
) -> Result<crate::market::Funding, String> {
    let n = |x: &serde_json::Value| {
        x.as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| x.as_f64())
            .unwrap_or(0.0)
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let (rate, next, mark) = match ex {
        "hyperliquid" => {
            let i = v[0]["universe"]
                .as_array()
                .and_then(|u| u.iter().position(|a| a["name"] == coin))
                .ok_or("нет монеты в universe")?;
            let c = &v[1][i];
            // funding начисляется каждый час — следующий час
            (
                c["funding"].clone(),
                serde_json::json!((now_ms / 3_600_000 + 1) * 3_600_000),
                c["markPx"].clone(),
            )
        }
        "gate" => (
            v["funding_rate"].clone(),
            serde_json::json!(n(&v["funding_next_apply"]) * 1000.0),
            v["mark_price"].clone(),
        ),
        "bybit" => {
            let t = &v["result"]["list"][0];
            (
                t["fundingRate"].clone(),
                t["nextFundingTime"].clone(),
                t["markPrice"].clone(),
            )
        }
        "okx" => {
            let t = &v["data"][0];
            (
                t["fundingRate"].clone(),
                t["fundingTime"].clone(),
                serde_json::Value::Null,
            )
        }
        "bingx" => {
            let t = &v["data"];
            (
                t["lastFundingRate"].clone(),
                t["nextFundingTime"].clone(),
                t["markPrice"].clone(),
            )
        }
        "kucoin" => {
            let t = &v["data"];
            (
                t["value"].clone(),
                t["fundingTime"].clone(),
                serde_json::Value::Null,
            )
        }
        _ => (
            v["lastFundingRate"].clone(),
            v["nextFundingTime"].clone(),
            v["markPrice"].clone(),
        ),
    };
    if rate.is_null() {
        return Err(format!("нет ставки funding: {v}"));
    }
    Ok(crate::market::Funding {
        rate: n(&rate),
        next_ms: n(&next) as u64,
        mark: n(&mark),
    })
}

/// Размеры контрактов перпов: OKX — ctVal (/api/v5/public/instruments?instType=SWAP), Gate — quanto_multiplier
/// (/api/v4/futures/usdt/contracts). Ключ — символ бота (BTCUSDT).
pub async fn contract_sizes(ex: &str, base: &str) -> Result<HashMap<String, f64>, String> {
    let base = base.trim_end_matches('/');
    let url = if ex == "okx" {
        format!("{base}/api/v5/public/instruments?instType=SWAP")
    } else {
        format!("{base}/api/v4/futures/usdt/contracts")
    };
    let v: serde_json::Value = client()
        .get(&url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    Ok(parse_contract_sizes(ex, &v))
}

pub fn parse_contract_sizes(ex: &str, v: &serde_json::Value) -> HashMap<String, f64> {
    let n = |x: &serde_json::Value| {
        x.as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| x.as_f64())
    };
    let list = if ex == "okx" { &v["data"] } else { v };
    list.as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| {
            if ex == "okx" {
                // только линейные (USDT/USDC); у инверсных ctVal — в долларах
                if c["ctType"] != "linear" {
                    return None;
                }
                let id = c["instId"]
                    .as_str()?
                    .trim_end_matches("-SWAP")
                    .replace('-', "");
                Some((id, n(&c["ctVal"])?))
            } else {
                Some((
                    c["name"].as_str()?.replace('_', ""),
                    n(&c["quanto_multiplier"])?,
                ))
            }
        })
        .filter(|(_, k)| *k > 0.0)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::get};
    use serde_json::json;

    #[test]
    fn rounding() {
        let f = Filter {
            tick: 0.01,
            step: 0.001,
            min_qty: 0.001,
            min_notional: 5.0,
        };
        assert!((f.round_qty(1.23456) - 1.234).abs() < 1e-12);
        assert!((f.round_price(100.123, false) - 100.12).abs() < 1e-9);
        assert!((f.round_price(100.123, true) - 100.13).abs() < 1e-9);
        assert!((f.round_price(100.12, true) - 100.12).abs() < 1e-9);
    }

    #[tokio::test]
    async fn filters_and_klines_from_mock() {
        let app = Router::new()
            .route(
                "/api/v3/exchangeInfo",
                get(|| async {
                    Json(json!({"symbols": [
                {"symbol": "BTCUSDT", "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {"filterType": "LOT_SIZE", "stepSize": "0.00001", "minQty": "0.00001"},
                    {"filterType": "NOTIONAL", "minNotional": "5.0"}]},
                {"symbol": "OTHER", "filters": []}]}))
                }),
            )
            .route(
                "/api/v3/klines",
                get(|| async {
                    Json(json!([
                        [0, "1", "1", "1", "1", "10", 1000, "123.5", 3],
                        [0, "1", "1", "1", "1", "10", 9_999_999_999_999u64, "7", 1]
                    ]))
                }),
            );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        let f = filters(&base, false, &["BTCUSDT".into()]).await.unwrap();
        assert_eq!(f.len(), 1);
        assert_eq!(
            f["BTCUSDT"],
            Filter {
                tick: 0.01,
                step: 0.00001,
                min_qty: 0.00001,
                min_notional: 5.0
            }
        );
        assert_eq!(
            minute_volumes(&base, false, "BTCUSDT", 5).await.unwrap(),
            vec![123.5],
            "незакрытая минута не считается"
        );
    }

    #[test]
    fn contract_sizes_okx_and_gate() {
        let o = parse_contract_sizes(
            "okx",
            &serde_json::json!({"data": [
            {"instId": "BTC-USDT-SWAP", "ctType": "linear", "ctVal": "0.01"},
            {"instId": "BTC-USD-SWAP", "ctType": "inverse", "ctVal": "100"}]}),
        );
        assert_eq!(o.len(), 1);
        assert_eq!(o["BTCUSDT"], 0.01);
        let g = parse_contract_sizes(
            "gate",
            &serde_json::json!([{"name": "ETH_USDT", "quanto_multiplier": "0.01"}]),
        );
        assert_eq!(g["ETHUSDT"], 0.01);
    }

    #[test]
    fn funding_formats() {
        use serde_json::json;
        let h = parse_funding("hyperliquid", "BTC", &json!([{"universe": [{"name": "ETH"}, {"name": "BTC"}]}, [{"funding": "0.1"}, {"funding": "0.0000125", "markPx": "60000"}]])).unwrap();
        assert_eq!((h.rate, h.mark), (0.0000125, 60000.0));
        assert_eq!(h.next_ms % 3_600_000, 0);
        let g = parse_funding(
            "gate",
            "BTC",
            &json!({"funding_rate": "0.0001", "funding_next_apply": 1700000000, "mark_price": "1"}),
        )
        .unwrap();
        assert_eq!(g.next_ms, 1_700_000_000_000);
        let x = parse_funding("bingx", "BTC", &json!({"code": 0, "data": {"lastFundingRate": "0.0002", "nextFundingTime": 5, "markPrice": "2"}})).unwrap();
        assert_eq!((x.rate, x.next_ms), (0.0002, 5));
        let k = parse_funding(
            "kucoin",
            "BTC",
            &json!({"data": {"value": 0.0003, "fundingTime": 7}}),
        )
        .unwrap();
        assert_eq!((k.rate, k.next_ms), (0.0003, 7));
        let b = parse_funding(
            "binance",
            "BTC",
            &json!({"lastFundingRate": "0.0001", "nextFundingTime": 9, "markPrice": "3"}),
        )
        .unwrap();
        assert_eq!(b.next_ms, 9);
        assert!(parse_funding("okx", "BTC", &json!({"data": []})).is_err());
    }
}
