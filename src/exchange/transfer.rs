//! Переводы монет между биржами: сети вывода, адрес депозита, вывод (Binance SAPI, Bybit v5 asset, OKX asset,
//! Gate wallet, KuCoin v3). У OKX и KuCoin вывод — со счёта фондирования, депозит приходит туда же: перед выводом
//! монета переводится с торгового счёта, пришедшая — обратно на торговый (sweep).

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
        "ARBI" | "ARB" | "ARBITRUM ONE" | "ARBEVM" => "ARBITRUM",
        "OP" | "OPTIMISM" | "OPETH" => "OPTIMISM",
        "POLYGON" | "MATIC" | "POL" => "MATIC",
        "SOLANA" => "SOL",
        "CAVAX" | "AVAX C-CHAIN" | "AVAXC" | "AVAX_C" | "AVALANCHE C-CHAIN" | "AVAXC-CHAIN" => {
            "AVAXC"
        }
        "BSC" | "BNB" => "BSC",
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

/// Сети OKX из /api/v5/asset/currencies (data): chain «USDT-TRC20» — имя после первого «-».
pub fn okx_nets(data: &Value, coin: &str) -> Vec<Net> {
    data.as_array()
        .into_iter()
        .flatten()
        .filter(|c| c["ccy"] == coin)
        .map(|c| {
            let raw = c["chain"].as_str().unwrap_or("");
            Net {
                name: norm(raw.split_once('-').map_or(raw, |x| x.1)),
                raw: raw.to_owned(),
                withdraw_fee: num(&c["fee"]).max(num(&c["minFee"])),
                withdraw_min: num(&c["minWd"]),
                withdraw: c["canWd"] == true,
                deposit: c["canDep"] == true,
            }
        })
        .collect()
}

/// Сети Gate: /wallet/currency_chains (открыт ли вывод и депозит) и /wallet/withdraw_status (комиссия по сетям).
pub fn gate_nets(chains: &Value, status: &Value) -> Vec<Net> {
    let st = status
        .as_array()
        .and_then(|a| a.first())
        .cloned()
        .unwrap_or_default();
    chains
        .as_array()
        .into_iter()
        .flatten()
        .map(|c| {
            let raw = c["chain"].as_str().unwrap_or("");
            let off = |k: &str| c[k] == 1 || c[k] == "1" || c[k] == true;
            Net {
                name: norm(raw),
                raw: raw.to_owned(),
                withdraw_fee: num(&st["withdraw_fix_on_chains"][raw]),
                withdraw_min: num(&st["withdraw_amount_mini"]),
                withdraw: !off("is_disabled") && !off("is_withdraw_disabled"),
                deposit: !off("is_disabled") && !off("is_deposit_disabled"),
            }
        })
        .collect()
}

/// Сети KuCoin из /api/v3/currencies/{монета} (data): имя — chainName, для запросов — chainId.
pub fn kucoin_nets(d: &Value) -> Vec<Net> {
    d["chains"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|c| Net {
            name: norm(c["chainName"].as_str().unwrap_or("")),
            raw: c["chainId"]
                .as_str()
                .or(c["chain"].as_str())
                .unwrap_or("")
                .to_owned(),
            withdraw_fee: num(&c["withdrawalMinFee"]),
            withdraw_min: num(&c["withdrawalMinSize"]),
            withdraw: c["isWithdrawEnabled"] == true,
            deposit: c["isDepositEnabled"] == true,
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

    #[test]
    fn okx_gate_kucoin_networks() {
        let o = okx_nets(
            &json!([{"ccy": "USDT", "chain": "USDT-TRC20", "canDep": true, "canWd": true, "fee": "1", "minWd": "2"},
            {"ccy": "USDT", "chain": "USDT-Arbitrum One", "canDep": true, "canWd": false, "fee": "0.1", "minWd": "1"}]),
            "USDT",
        );
        assert_eq!(
            (o[0].name.as_str(), o[0].raw.as_str(), o[0].withdraw_fee),
            ("TRX", "USDT-TRC20", 1.0)
        );
        assert_eq!((o[1].name.as_str(), o[1].withdraw), ("ARBITRUM", false));
        let g = gate_nets(
            &json!([{"chain": "TRX", "is_disabled": 0, "is_deposit_disabled": 0, "is_withdraw_disabled": 0},
            {"chain": "ARBEVM", "is_disabled": 0, "is_deposit_disabled": 1, "is_withdraw_disabled": 0}]),
            &json!([{"currency": "USDT", "withdraw_amount_mini": "1.5", "withdraw_fix_on_chains": {"TRX": "1.2", "ARBEVM": "0.3"}}]),
        );
        assert_eq!(
            (g[0].name.as_str(), g[0].withdraw_fee, g[0].withdraw_min),
            ("TRX", 1.2, 1.5)
        );
        assert_eq!(
            (g[1].name.as_str(), g[1].deposit, g[1].withdraw),
            ("ARBITRUM", false, true)
        );
        let k = kucoin_nets(
            &json!({"currency": "USDT", "chains": [{"chainName": "TRC20", "chainId": "trx", "withdrawalMinFee": "1.5",
            "withdrawalMinSize": "10", "isWithdrawEnabled": true, "isDepositEnabled": true}]}),
        );
        assert_eq!((k[0].name.as_str(), k[0].raw.as_str()), ("TRX", "trx"));
        // Gate → OKX: Arbitrum дешевле (депозит на OKX открыт, вывод с Gate — тоже), у получателя — своё имя сети
        let (n, raw) = cheapest(&g, &o).unwrap();
        assert_eq!((n.raw.as_str(), raw), ("ARBEVM", "USDT-Arbitrum One"));
        // OKX → Gate: вывод по Arbitrum с OKX закрыт, депозит на Gate по нему тоже — TRX
        let (n, raw) = cheapest(&o, &g).unwrap();
        assert_eq!((n.raw.as_str(), raw), ("USDT-TRC20", "TRX"));
    }
}
