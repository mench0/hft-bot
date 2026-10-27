//! Где стоят серверы бирж и насколько они далеко от сервера бота (настройка `serverRegion`), как Java
//! ExchangeRegions: подбор помечает «не подходит» стратегии, которым биржа слишком далеко, каталог показывает, что
//! подходит именно этому серверу.

use serde_json::{Value, json};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Region {
    Tokyo,
    Singapore,
    Hongkong,
    Asia,
    Europe,
    Us,
    Other,
    Chain,
}

impl Region {
    pub fn title(self) -> &'static str {
        match self {
            Region::Tokyo => "Токио",
            Region::Singapore => "Сингапур",
            Region::Hongkong => "Гонконг",
            Region::Asia => "Азия",
            Region::Europe => "Европа",
            Region::Us => "США",
            Region::Other => "другое",
            Region::Chain => "блокчейн",
        }
    }

    fn asia(self) -> bool {
        matches!(
            self,
            Region::Tokyo | Region::Singapore | Region::Hongkong | Region::Asia
        )
    }

    /// Регион по значению настройки serverRegion; пусто или неизвестно — None.
    pub fn of_setting(v: &str) -> Option<Region> {
        Some(match v.trim().to_lowercase().as_str() {
            "tokyo" => Region::Tokyo,
            "singapore" => Region::Singapore,
            "hongkong" => Region::Hongkong,
            "asia" => Region::Asia,
            "europe" => Region::Europe,
            "us" => Region::Us,
            "other" => Region::Other,
            "chain" => Region::Chain,
            _ => return None,
        })
    }

    /// Сервер бота из настроек процесса.
    pub fn server(settings: &serde_json::Map<String, Value>) -> Option<Region> {
        settings
            .get("serverRegion")
            .and_then(Value::as_str)
            .and_then(Region::of_setting)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Distance {
    Near,
    Neighbor,
    Far,
}

impl Distance {
    pub fn title(self) -> &'static str {
        match self {
            Distance::Near => "рядом",
            Distance::Neighbor => "соседний регион",
            Distance::Far => "далеко",
        }
    }
}

/// Стратегии (id) и названия — в порядке Java.
pub const STRATEGIES: [(&str, &str); 6] = [
    ("perp-arb", "Перп – перп на разных биржах"),
    ("stat-arb", "Статистический арбитраж"),
    ("mean-reversion", "Возврат к среднему"),
    (
        "cross-exchange-arb",
        "Спот – спот на разных биржах (сканер)",
    ),
    ("lead-lag", "Lead-lag: ведущая биржа – ведомая"),
    ("spread-capture", "Сбор спреда (сканер)"),
];
/// Соседний регион и блокчейн: без быстрых арбитражей и маркет-мейкинга.
const SLOW: [&str; 2] = ["stat-arb", "mean-reversion"];

pub fn region(id: &str) -> Region {
    match id {
        "binance" | "hyperliquid" | "gate" | "mexc" | "kucoin" => Region::Tokyo,
        "bybit" => Region::Singapore,
        "okx" | "lbank" => Region::Hongkong,
        "uniswapv2" | "uniswap" => Region::Chain,
        _ => Region::Asia,
    }
}

/// Расстояние от сервера бота до биржи.
pub fn distance(server: Region, ex: &str) -> Distance {
    let r = region(ex);
    if r == Region::Chain {
        return Distance::Neighbor; // зависит от RPC-ноды, а не от биржи
    }
    if server == r && r != Region::Asia {
        Distance::Near
    } else if server.asia() && r.asia() {
        Distance::Neighbor
    } else {
        Distance::Far
    }
}

/// Стратегии, которым подходит такое расстояние до биржи.
pub fn suitable(ex: &str, d: Distance) -> Vec<&'static str> {
    if region(ex) == Region::Chain {
        return SLOW.to_vec();
    }
    match d {
        Distance::Near => STRATEGIES.iter().map(|s| s.0).collect(),
        Distance::Neighbor => SLOW.to_vec(),
        Distance::Far => Vec::new(),
    }
}

/// Почему стратегия не подходит бирже; None — подходит или сервер не задан.
pub fn why_not(strategy: &str, server: Option<Region>, ex: &str) -> Option<String> {
    let server = server?;
    let d = distance(server, ex);
    if suitable(ex, d).contains(&strategy) {
        return None;
    }
    let title = STRATEGIES
        .iter()
        .find(|s| s.0 == strategy)
        .map_or(strategy, |s| s.1);
    Some(format!(
        "биржа {ex} ({}) — {} от сервера ({}): для «{title}» нужен сервер ближе к бирже",
        region(ex).title(),
        d.title(),
        server.title()
    ))
}

fn names(ids: &[&str]) -> Vec<&'static str> {
    ids.iter()
        .filter_map(|i| STRATEGIES.iter().find(|s| s.0 == *i).map(|s| s.1))
        .collect()
}

/// Рекомендация каталога: где держать сервер и что подходит; при заданном сервере — расстояние и что подходит вам.
pub fn recommendation(ex: &str, server: Option<Region>) -> Value {
    let r = region(ex);
    let chain = r == Region::Chain;
    let all: Vec<&str> = STRATEGIES.iter().map(|s| s.0).collect();
    let mut m = json!({
        "server": if chain { "рядом с RPC-нодой" } else if r == Region::Asia { "Азия (Токио или Сингапур)" } else { r.title() },
        "near": names(if chain { &SLOW } else { &all }), "neighbor": names(&SLOW), "far": names(if chain { &SLOW } else { &[] }),
    });
    if chain {
        m["note"] =
            json!("Скорость ограничена временем блока; быстрые арбитражи на AMM не подходят");
    }
    if let Some(s) = server {
        let d = distance(s, ex);
        m["yourServer"] = json!(s.title());
        m["distance"] = json!(d.title());
        m["forYou"] = json!(names(&suitable(ex, d)));
    }
    m
}

/// Пометить кандидатов подбора, которым биржа (или одна из бирж «a→b») далеко от сервера: «не подходит» + причина.
pub fn mark(strategies: &mut [Value], server: Option<Region>) {
    if server.is_none() {
        return;
    }
    for s in strategies {
        let id = s["id"].as_str().unwrap_or("").to_owned();
        let leader = s["leader"].as_str().map(str::to_owned);
        for c in s["candidates"].as_array_mut().into_iter().flatten() {
            let lead = c["metrics"]["leader"].as_str().map(str::to_owned);
            let exs: Vec<String> = c["exchange"]
                .as_str()
                .unwrap_or("")
                .split('→')
                .map(str::to_owned)
                .chain(leader.clone())
                .chain(lead)
                .collect();
            if let Some(why) = exs.iter().find_map(|e| why_not(&id, server, e)) {
                c["verdict"] = json!("не подходит");
                c["suitable"] = json!(false);
                if let Some(r) = c["reasons"].as_array_mut() {
                    r.push(json!(why));
                }
            }
        }
    }
}

/// Строка «location» ответа /discovery.
pub fn location(server: Option<Region>) -> String {
    match server {
        None => "Местоположение бирж не учитывается: задайте serverRegion в настройках (где стоит сервер бота)".into(),
        Some(s) => format!("Сервер бота: {}. Стратегии, которым биржа слишком далеко, помечены «не подходит»", s.title()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distances_and_marks_as_java() {
        assert_eq!(distance(Region::Tokyo, "binance"), Distance::Near);
        assert_eq!(distance(Region::Tokyo, "bybit"), Distance::Neighbor);
        assert_eq!(distance(Region::Europe, "okx"), Distance::Far);
        assert_eq!(distance(Region::Asia, "bingx"), Distance::Neighbor);
        assert_eq!(distance(Region::Us, "uniswapv2"), Distance::Neighbor);
        assert!(why_not("stat-arb", Some(Region::Singapore), "binance").is_none());
        assert!(
            why_not("spread-capture", Some(Region::Singapore), "binance")
                .unwrap()
                .contains("соседний регион")
        );
        assert!(why_not("spread-capture", None, "binance").is_none());
        let mut s = vec![json!({"id": "cross-exchange-arb", "candidates": [
            {"exchange": "binance→gate", "verdict": "подходит", "suitable": true, "reasons": []},
            {"exchange": "binance→bybit", "verdict": "подходит", "suitable": true, "reasons": []}]})];
        mark(&mut s, Some(Region::Tokyo));
        assert_eq!(s[0]["candidates"][0]["verdict"], "подходит");
        assert_eq!(s[0]["candidates"][1]["verdict"], "не подходит");
        assert!(
            s[0]["candidates"][1]["reasons"][0]
                .as_str()
                .unwrap()
                .contains("bybit")
        );
        let r = recommendation("okx", Some(Region::Hongkong));
        assert_eq!(
            (
                r["distance"].as_str(),
                r["forYou"].as_array().unwrap().len()
            ),
            (Some("рядом"), 6)
        );
    }
}
