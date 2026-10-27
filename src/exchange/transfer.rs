//! Переводы монет между биржами: сети вывода, адрес депозита, вывод (Binance SAPI, Bybit v5 asset).

use serde_json::Value;

/// Сеть монеты на бирже.
#[derive(Clone, Debug, PartialEq)]
pub struct Net {
    /// Имя сети в общем виде (TRX, BSC, ETH, ARBITRUM…).
    pub name: String,
    /// Имя сети, как его ждёт биржа.
    pub raw: String,
    pub withdraw_fee: f64,
    pub withdraw_min: f64,
    pub withdraw: bool,
    pub deposit: bool,
}

/// Общее имя сети: у бирж разные названия одной сети.
pub fn norm(n: &str) -> String {
    let u = n.trim().to_uppercase();
    match u.as_str() {
        "TRC20" | "TRON" => "TRX",
        "BEP20" | "BSC(BEP20)" | "BNB SMART CHAIN" => "BSC",
        "ERC20" => "ETH",
        "ARBI" | "ARB" | "ARBITRUM ONE" => "ARBITRUM",
        "OP" | "OPTIMISM" => "OPTIMISM",
        "POLYGON" | "MATIC" | "POL" => "MATIC",
        "SOLANA" => "SOL",
        "CAVAX" | "AVAX C-CHAIN" | "AVAXC" => "AVAXC",
        _ => return u,
    }
    .to_owned()
}

/// Самая дешёвая сеть: вывод открыт у источника, депозит — у получателя; цена — комиссия вывода.
pub fn cheapest<'a>(from: &'a [Net], to: &'a [Net]) -> Option<(&'a Net, &'a str)> {
    from.iter()
        .filter(|n| n.withdraw)
        .filter_map(|n| {
            to.iter()
                .find(|d| d.deposit && d.name == n.name)
                .map(|d| (n, d.raw.as_str()))
        })
        .min_by(|a, b| a.0.withdraw_fee.total_cmp(&b.0.withdraw_fee))
}

fn num(v: &Value) -> f64 {
    v.as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| v.as_f64())
        .unwrap_or(0.0)
}

/// Сети Binance из /sapi/v1/capital/config/getall.
pub fn binance_nets(all: &Value, coin: &str) -> Vec<Net> {
    all.as_array()
        .into_iter()
        .flatten()
        .filter(|c| c["coin"] == coin)
        .flat_map(|c| c["networkList"].as_array().cloned().unwrap_or_default())
        .map(|n| Net {
            name: norm(n["network"].as_str().unwrap_or("")),
            raw: n["network"].as_str().unwrap_or("").to_owned(),
            withdraw_fee: num(&n["withdrawFee"]),
            withdraw_min: num(&n["withdrawMin"]),
            withdraw: n["withdrawEnable"] == true,
            deposit: n["depositEnable"] == true,
        })
        .collect()
}

/// Сети Bybit из /v5/asset/coin/query-info (result).
pub fn bybit_nets(r: &Value) -> Vec<Net> {
    r["rows"][0]["chains"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|n| Net {
            name: norm(n["chain"].as_str().unwrap_or("")),
            raw: n["chain"].as_str().unwrap_or("").to_owned(),
            withdraw_fee: num(&n["withdrawFee"]),
            withdraw_min: num(&n["withdrawMin"]),
            withdraw: n["chainWithdraw"] == "1",
            deposit: n["chainDeposit"] == "1",
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cheapest_common_network() {
        let b = binance_nets(
            &json!([{"coin": "USDT", "networkList": [
            {"network": "ETH", "withdrawFee": "4", "withdrawMin": "10", "withdrawEnable": true, "depositEnable": true},
            {"network": "TRX", "withdrawFee": "1", "withdrawMin": "10", "withdrawEnable": true, "depositEnable": true},
            {"network": "ARBITRUM", "withdrawFee": "0.5", "withdrawMin": "10", "withdrawEnable": false, "depositEnable": true}]}]),
            "USDT",
        );
        let y = bybit_nets(&json!({"rows": [{"coin": "USDT", "chains": [
            {"chain": "ETH", "withdrawFee": "3", "withdrawMin": "5", "chainWithdraw": "1", "chainDeposit": "1"},
            {"chain": "TRX", "withdrawFee": "1.5", "withdrawMin": "5", "chainWithdraw": "1", "chainDeposit": "0"},
            {"chain": "ARBI", "withdrawFee": "0.8", "withdrawMin": "5", "chainWithdraw": "1", "chainDeposit": "1"}]}]}));
        // Binance → Bybit: TRX дешевле, но депозит на Bybit по TRX закрыт; ARBITRUM закрыт на вывод — ETH
        let (n, raw) = cheapest(&b, &y).unwrap();
        assert_eq!((n.raw.as_str(), raw), ("ETH", "ETH"));
        // Bybit → Binance: ARBI (0.8) = ARBITRUM у Binance
        let (n, raw) = cheapest(&y, &b).unwrap();
        assert_eq!(
            (n.raw.as_str(), raw, n.withdraw_fee),
            ("ARBI", "ARBITRUM", 0.8)
        );
    }
}
