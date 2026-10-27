//! Справочные данные, общие с Java-ботом: каталог бирж, схемы параметров биржи и настроек процесса.
//! Файлы в `data/` — ответы Java-бота (`/exchanges/catalog`, `/exchange/params/schema`, `/settings`), чтобы формат совпадал.

use serde_json::{Map, Value};
use std::sync::OnceLock;

/// Биржи, у которых есть тестовая сеть (как `ExchangeInfo.hasTestnet` в Java).
pub const WITH_TESTNET: &[&str] = &["binance", "bybit", "okx", "gate", "hyperliquid", "aster"];

fn parse(text: &'static str) -> Value {
    serde_json::from_str(text).expect("встроенный JSON из data/")
}

/// Каталог бирж: `{"exchanges": [...]}`.
pub fn catalog() -> &'static Value {
    static V: OnceLock<Value> = OnceLock::new();
    V.get_or_init(|| parse(include_str!("../../data/catalog.json")))
}

/// Схема параметров биржи: имя → `{default, min?, max?, restart?, help, format?}`.
pub fn exchange_schema() -> &'static Map<String, Value> {
    static V: OnceLock<Value> = OnceLock::new();
    V.get_or_init(|| parse(include_str!("../../data/exchange_schema.json")))
        .as_object()
        .expect("схема параметров биржи — объект")
}

/// Схема настроек процесса и фильтры подбора по стратегиям: `{"описание": {...}, "фильтры_стратегий": {...}}`.
pub fn settings_schema() -> &'static Value {
    static V: OnceLock<Value> = OnceLock::new();
    V.get_or_init(|| parse(include_str!("../../data/settings_schema.json")))
}

/// Описания настроек процесса.
pub fn settings_specs() -> &'static Map<String, Value> {
    settings_schema()["описание"]
        .as_object()
        .expect("описание настроек — объект")
}

/// Id бирж каталога по порядку.
pub fn exchange_ids() -> Vec<String> {
    catalog()["exchanges"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|e| e["id"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// Значение параметра по умолчанию из схемы в типе, как его отдаёт Java: флаг, число или строка.
pub fn typed(raw: &str) -> Value {
    match raw {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => {
            if let Ok(i) = raw.parse::<i64>() {
                Value::from(i)
            } else if let Ok(f) = raw.parse::<f64>() {
                if f.is_finite() {
                    Value::from(f)
                } else {
                    Value::String(raw.to_owned())
                }
            } else {
                Value::String(raw.to_owned())
            }
        }
    }
}

/// Значения по умолчанию по схеме.
pub fn defaults(specs: &Map<String, Value>) -> Map<String, Value> {
    specs
        .iter()
        .map(|(k, s)| (k.clone(), typed(s["default"].as_str().unwrap_or(""))))
        .collect()
}

/// Проверить новое значение параметра по схеме и вернуть его в нужном типе; Err — понятная причина.
pub fn validate(specs: &Map<String, Value>, key: &str, raw: &str) -> Result<Value, String> {
    let spec = specs
        .get(key)
        .ok_or_else(|| format!("Неизвестный параметр: {key}"))?;
    let def = spec["default"].as_str().unwrap_or("");
    if def == "true" || def == "false" {
        return match raw {
            "true" | "false" => Ok(Value::Bool(raw == "true")),
            _ => Err(format!("{key}: ожидается true или false")),
        };
    }
    let numeric =
        def.parse::<f64>().is_ok() || spec.get("min").is_some() || spec.get("max").is_some();
    if numeric {
        let v: f64 = raw
            .trim()
            .parse()
            .map_err(|_| format!("{key}: ожидается число"))?;
        if let Some(min) = spec["min"].as_f64()
            && v < min
        {
            return Err(format!("{key}: не меньше {min}"));
        }
        if let Some(max) = spec["max"].as_f64()
            && v > max
        {
            return Err(format!("{key}: не больше {max}"));
        }
        return Ok(if def.contains('.') || raw.contains('.') {
            Value::from(v)
        } else {
            Value::from(v as i64)
        });
    }
    Ok(Value::String(raw.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_and_schemas_load() {
        assert!(exchange_ids().contains(&"binance".to_owned()));
        assert!(exchange_schema().contains_key("scJumpTicks"));
        assert!(settings_specs().contains_key("discoveryRefreshSec"));
    }

    #[test]
    fn typed_values() {
        assert_eq!(typed("true"), Value::Bool(true));
        assert_eq!(typed("12"), Value::from(12));
        assert_eq!(typed("0.5"), Value::from(0.5));
        assert_eq!(typed("spot"), Value::from("spot"));
    }

    #[test]
    fn validation() {
        let s = exchange_schema();
        assert_eq!(validate(s, "tradingEnabled", "true"), Ok(Value::Bool(true)));
        assert!(validate(s, "tradingEnabled", "yes").is_err());
        assert!(validate(s, "scJumpTicks", "1000").is_err());
        assert!(validate(s, "nope", "1").is_err());
    }
}
