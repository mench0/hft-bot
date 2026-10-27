//! Лимиты частоты запросов к биржам (как Java RateBudget и RateLimits): на биржу — набор вёдер жетонов по её
//! документации, у каждого ведра — доля rateLimitSafety (0.8) официальной ёмкости, полное восполнение за окно.
//! Запрос берёт по жетону из каждого ведра своего вида; не хватает — ждёт, дольше допустимого — отказ «очередь
//! запросов переполнена». Ответ биржи «слишком часто» (429/418/403 или код лимита в теле) — все запросы к бирже на
//! паузу: 10 с (418 — 2 мин, 403 Bybit — 10 мин), при повторах в течение 5 мин — вдвое, до 30 мин; если биржа сообщила
//! время снятия бана («banned until …», "retryAfter") — ровно до него. После паузы вёдра пустые.

use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Public,
    Private,
    Order,
    WsMessage,
    WsConnect,
}
use Kind::*;

const REST: &[Kind] = &[Public, Private, Order];

struct Bucket {
    name: &'static str,
    capacity: f64,
    window_ms: f64,
    kinds: &'static [Kind],
    /// Может уйти в минус: это уже занятые будущие жетоны.
    tokens: f64,
    last_ms: u64,
}

impl Bucket {
    fn refill(&mut self, now: u64) {
        if now > self.last_ms {
            self.tokens = (self.tokens
                + (now - self.last_ms) as f64 * self.capacity / self.window_ms)
                .min(self.capacity);
            self.last_ms = now;
        }
    }
}

#[derive(Default)]
struct Budget {
    buckets: Vec<Bucket>,
    blocked_until: u64,
    block_reason: String,
    /// 0 — своё притормаживание, 1 — биржа отказала (время неизвестно), 2 — бан до времени от биржи.
    block_kind: u8,
    last_hit: u64,
    hits: u32,
    rate_limited: u64,
    local_rejects: u64,
    waited_ms: u64,
}

static SAFETY: Mutex<f64> = Mutex::new(0.8);
static ALL: Mutex<Option<HashMap<String, Budget>>> = Mutex::new(None);

/// Доля официального лимита (настройка rateLimitSafety); действует на бюджеты, созданные после вызова.
pub fn set_safety(s: f64) {
    *SAFETY.lock().unwrap() = s.clamp(0.1, 1.0);
}

/// Вёдра биржи по документации (как в Java RateLimits): (имя, официальная ёмкость, окно мс, виды).
fn limits(ex: &str) -> Vec<(&'static str, f64, u64, &'static [Kind])> {
    match ex {
        // вес 6000/мин на IP, ордера 50/10 с и 160 000/сутки, сырые 61 000/5 мин; WS 5 сообщений/с, 300 подключений/5 мин
        "binance" => vec![
            ("weight", 6000.0, 60_000, REST),
            ("raw", 61_000.0, 300_000, REST),
            ("orders10s", 50.0, 10_000, &[Order]),
            ("orders1d", 160_000.0, 86_400_000, &[Order]),
            ("wsMessages", 5.0, 1_000, &[WsMessage]),
            ("wsConnects", 300.0, 300_000, &[WsConnect]),
        ],
        "aster" => vec![
            ("weight", 2400.0, 60_000, REST),
            ("orders1m", 1200.0, 60_000, &[Order]),
            ("orders10s", 100.0, 10_000, &[Order]),
            ("wsMessages", 10.0, 1_000, &[WsMessage]),
            ("wsConnects", 300.0, 300_000, &[WsConnect]),
        ],
        "bybit" => vec![
            ("ip", 600.0, 5_000, REST),
            ("orders", 20.0, 1_000, &[Order]),
            ("private", 10.0, 1_000, &[Private]),
            ("wsMessages", 10.0, 1_000, &[WsMessage]),
            ("wsConnects", 500.0, 300_000, &[WsConnect]),
        ],
        "okx" => vec![
            ("public", 40.0, 2_000, &[Public]),
            ("private", 10.0, 2_000, &[Private]),
            ("orders", 60.0, 2_000, &[Order]),
            ("wsMessages", 480.0, 3_600_000, &[WsMessage]),
            ("wsConnects", 3.0, 1_000, &[WsConnect]),
        ],
        "gate" => vec![
            ("public", 200.0, 10_000, &[Public]),
            ("private", 200.0, 10_000, &[Private]),
            ("orders", 10.0, 1_000, &[Order]),
            ("wsMessages", 50.0, 1_000, &[WsMessage]),
            ("wsConnects", 100.0, 60_000, &[WsConnect]),
        ],
        "mexc" => vec![
            ("public", 500.0, 10_000, &[Public]),
            ("private", 500.0, 10_000, &[Private]),
            ("orders", 5.0, 1_000, &[Order]),
            ("wsMessages", 100.0, 1_000, &[WsMessage]),
            ("wsConnects", 100.0, 60_000, &[WsConnect]),
        ],
        "hyperliquid" => vec![
            ("weight", 1200.0, 60_000, REST),
            ("wsMessages", 2000.0, 60_000, &[WsMessage]),
            ("wsConnects", 100.0, 60_000, &[WsConnect]),
        ],
        "kucoin" => vec![
            ("public", 2000.0, 30_000, &[Public]),
            ("spot", 4000.0, 30_000, &[Private, Order]),
            ("wsMessages", 100.0, 10_000, &[WsMessage]),
            ("wsConnects", 30.0, 60_000, &[WsConnect]),
        ],
        "lbank" => vec![
            ("ip", 200.0, 10_000, &[Public, Private]),
            ("orders", 500.0, 10_000, &[Order]),
            ("wsMessages", 10.0, 1_000, &[WsMessage]),
            ("wsConnects", 20.0, 60_000, &[WsConnect]),
        ],
        "upbit" => vec![
            ("public", 10.0, 1_000, &[Public]),
            ("private", 30.0, 1_000, &[Private]),
            ("orders", 8.0, 1_000, &[Order]),
            ("wsMessages", 5.0, 1_000, &[WsMessage]),
            ("wsConnects", 5.0, 1_000, &[WsConnect]),
        ],
        "bingx" => vec![
            ("public", 100.0, 10_000, &[Public]),
            ("private", 1000.0, 10_000, &[Private]),
            ("orders", 10.0, 1_000, &[Order]),
            ("wsMessages", 10.0, 1_000, &[WsMessage]),
            ("wsConnects", 60.0, 60_000, &[WsConnect]),
        ],
        "uniswapv2" => vec![
            ("rpc", 5.0, 1_000, REST),
            ("tx", 1.25, 1_000, &[Order]),
            ("wsMessages", 10.0, 1_000, &[WsMessage]),
            ("wsConnects", 10.0, 60_000, &[WsConnect]),
        ],
        // остальные — консервативный лимит из каталога (уже с запасом)
        _ => {
            let rps = crate::infra::data::catalog()["exchanges"]
                .as_array()
                .and_then(|a| a.iter().find(|e| e["id"] == ex))
                .and_then(|e| e["maxRequestsPerSec"].as_f64())
                .unwrap_or(2.0);
            let s = *SAFETY.lock().unwrap();
            vec![
                ("rest", rps / s, 1_000, REST),
                ("wsMessages", 5.0, 1_000, &[WsMessage]),
                ("wsConnects", 20.0, 60_000, &[WsConnect]),
            ]
        }
    }
}

fn now_ms() -> u64 {
    crate::infra::util::now_ms()
}

fn with<R>(ex: &str, f: impl FnOnce(&mut Budget) -> R) -> R {
    let mut all = ALL.lock().unwrap();
    let b = all
        .get_or_insert_with(HashMap::new)
        .entry(ex.to_owned())
        .or_insert_with(|| {
            let s = *SAFETY.lock().unwrap();
            let now = now_ms();
            Budget {
                buckets: limits(ex)
                    .into_iter()
                    .map(|(name, cap, w, kinds)| Bucket {
                        name,
                        capacity: cap * s,
                        window_ms: w as f64,
                        kinds,
                        tokens: cap * s,
                        last_ms: now,
                    })
                    .collect(),
                ..Default::default()
            }
        });
    f(b)
}

/// Вид запроса по источнику (метки вкладки «Запросы»).
pub fn kind_of(source: &str) -> Kind {
    match source {
        "orders" => Order,
        "account" => Private,
        "ws_connect" => WsConnect,
        "ws_send" => WsMessage,
        _ => Public,
    }
}

/// Дождаться разрешения на запрос (вес 1) и учесть его во вкладке «Запросы»; Err — ждать дольше допустимого
/// (ордера — 2 с, остальное — 30 с) или запросы к бирже приостановлены надолго.
pub async fn take(ex: &str, source: &'static str) -> Result<(), String> {
    let kind = kind_of(source);
    let max_wait = if kind == Order { 2_000 } else { 30_000 };
    let blocked = with(ex, |b| b.blocked_until.saturating_sub(now_ms()));
    if blocked > 0 {
        if blocked > max_wait {
            let reason = with(ex, |b| {
                b.local_rejects += 1;
                b.block_reason.clone()
            });
            return Err(format!(
                "{ex}: запросы приостановлены ещё на {blocked} мс ({reason})"
            ));
        }
        tokio::time::sleep(Duration::from_millis(blocked)).await;
    }
    let wait = with(ex, |b| {
        let now = now_ms();
        let mut wait = 0u64;
        for k in b.buckets.iter_mut().filter(|k| k.kinds.contains(&kind)) {
            k.refill(now);
            let deficit = 1f64.min(k.capacity) - k.tokens;
            if deficit > 0.0 {
                wait = wait.max((deficit * k.window_ms / k.capacity).ceil() as u64);
            }
        }
        if wait > max_wait {
            b.local_rejects += 1;
            return Err(format!(
                "{ex}: очередь запросов переполнена (ждать {wait} мс)"
            ));
        }
        for k in b.buckets.iter_mut().filter(|k| k.kinds.contains(&kind)) {
            k.tokens -= 1f64.min(k.capacity);
        }
        b.waited_ms += wait;
        Ok(wait)
    })?;
    if wait > 0 {
        tokio::time::sleep(Duration::from_millis(wait)).await;
    }
    crate::infra::metrics::out(ex, source);
    crate::infra::journal::request(ex, source);
    Ok(())
}

/// Ошибка запроса похожа на ответ «слишком часто».
pub fn is_limit_error(e: &str) -> bool {
    let l = e.to_lowercase();
    [
        "429",
        "418",
        "too many",
        "too frequent",
        "rate limit",
        "ratelimit",
        "banned until",
        "retryafter",
        "10006",
        "-1003",
    ]
    .iter()
    .any(|s| l.contains(s))
}

/// Момент окончания бана из текста ответа: «banned until 1700000000000» или "retryAfter":1700000000000.
pub fn ban_until(text: &str, now: u64) -> Option<u64> {
    let l = text.to_lowercase();
    for key in ["banned until", "retryafter"] {
        let mut rest = l.as_str();
        while let Some(i) = rest.find(key) {
            rest = &rest[i + key.len()..];
            let d: String = rest
                .trim_start_matches(|c: char| !c.is_ascii_digit() && !c.is_alphabetic())
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            if let Ok(t) = d.parse::<u64>()
                && d.len() == 13
                && t + 60_000 > now
                && t < now + 7 * 86_400_000
            {
                return Some(t);
            }
        }
    }
    None
}

/// Биржа ответила «слишком часто»: пауза всех запросов к ней, вёдра опустошаются.
pub fn hit(ex: &str, what: &str) -> u64 {
    let now = now_ms();
    let code = if what.contains("418") {
        418
    } else if what.contains("403") {
        403
    } else {
        429
    };
    with(ex, |b| {
        b.hits = if now.saturating_sub(b.last_hit) < 300_000 {
            b.hits + 1
        } else {
            1
        };
        b.last_hit = now;
        b.rate_limited += 1;
        let (pause, kind) = match ban_until(what, now) {
            Some(t) => (t + 1_000 - now, 2),
            None => {
                let base: u64 = match code {
                    418 => 120_000,
                    403 if ex.starts_with("bybit") => 600_000,
                    403 => 60_000,
                    _ => 10_000,
                };
                ((base << (b.hits - 1).min(6)).min(1_800_000), 1)
            }
        };
        let until = now + pause;
        if until > b.blocked_until {
            b.blocked_until = until;
            b.block_reason = what.chars().take(200).collect();
            b.block_kind = kind;
        }
        for k in b.buckets.iter_mut() {
            k.refill(now);
            k.tokens = k.tokens.min(0.0);
        }
        tracing::warn!(
            "[{ex}] превышен лимит запросов ({}) — все запросы к бирже приостановлены на {} с (повтор №{})",
            b.block_reason,
            pause / 1000,
            b.hits
        );
        pause
    })
}

/// Биржи с бюджетом.
pub fn exchanges() -> Vec<String> {
    let mut v: Vec<String> = ALL
        .lock()
        .unwrap()
        .as_ref()
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    v.sort();
    v
}

/// Состояние бюджета для /requests/live и метрик: вёдра, ответы «слишком часто», свои отказы, ожидание, пауза, бан.
pub fn snapshot(ex: &str) -> Value {
    if !ALL
        .lock()
        .unwrap()
        .as_ref()
        .is_some_and(|m| m.contains_key(ex))
    {
        return json!({"buckets": [], "rateLimited": 0, "localRejects": 0, "waitedMs": 0, "blockedMs": 0});
    }
    with(ex, |b| {
        let now = now_ms();
        let buckets: Vec<Value> = b
            .buckets
            .iter_mut()
            .map(|k| {
                k.refill(now);
                json!({"name": k.name, "capacity": k.capacity, "windowMs": k.window_ms as u64,
                    "usedRatio": (1.0 - k.tokens / k.capacity).clamp(0.0, 1.0)})
            })
            .collect();
        let blocked = b.blocked_until.saturating_sub(now);
        let mut v = json!({"buckets": buckets, "rateLimited": b.rate_limited, "localRejects": b.local_rejects,
            "waitedMs": b.waited_ms, "blockedMs": blocked});
        if blocked > 0 && b.block_kind > 0 {
            v["ban"] = json!({"untilMs": b.blocked_until, "exact": b.block_kind == 2, "remainingMs": blocked,
                "reason": b.block_reason, "repeat": b.hits});
        }
        v
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn buckets_wait_reject_and_pause() {
        // Upbit: ордера 8/с × 0.8 = 6.4 в секунду
        let ex = "upbit-test";
        with(ex, |b| {
            b.buckets = vec![Bucket {
                name: "orders",
                capacity: 2.0,
                window_ms: 1_000.0,
                kinds: &[Order],
                tokens: 2.0,
                last_ms: now_ms(),
            }]
        });
        take(ex, "orders").await.unwrap();
        take(ex, "orders").await.unwrap();
        let t = std::time::Instant::now();
        take(ex, "orders").await.unwrap(); // третий ждёт восполнения ~0.5 с
        assert!(
            t.elapsed() >= Duration::from_millis(400),
            "{:?}",
            t.elapsed()
        );
        take(ex, "account").await.unwrap(); // другого вида — без ограничений
        let s = snapshot(ex);
        assert!(s["buckets"][0]["usedRatio"].as_f64().unwrap() > 0.5, "{s}");
        // «слишком часто» — пауза 10 с, ордер отклонён сразу (ждать дольше 2 с нельзя)
        assert_eq!(hit(ex, "HTTP 429 Too Many Requests"), 10_000);
        assert_eq!(hit(ex, "HTTP 429"), 20_000, "повтор — вдвое");
        let e = take(ex, "orders").await.unwrap_err();
        assert!(e.contains("приостановлены"), "{e}");
        let s = snapshot(ex);
        assert_eq!(
            (
                s["rateLimited"].clone(),
                s["localRejects"].clone(),
                s["ban"]["exact"].clone()
            ),
            (json!(2), json!(1), json!(false))
        );
    }

    #[test]
    fn ban_time_from_body_and_limit_errors() {
        let now = 1_700_000_000_000;
        assert_eq!(
            ban_until(
                r#"{"code":-1003,"msg":"Way too many requests; IP banned until 1700000123456."}"#,
                now
            ),
            Some(1_700_000_123_456)
        );
        assert_eq!(
            ban_until(r#"{"data":{"retryAfter":1700000060000}}"#, now),
            Some(1_700_000_060_000)
        );
        assert_eq!(ban_until("banned until 12", now), None);
        assert!(is_limit_error("HTTP 429: слишком часто"));
        assert!(is_limit_error("Bybit 10006: Too many visits"));
        assert!(!is_limit_error("LBank 10022: Insufficient balance"));
        // бан до времени от биржи — пауза ровно до него (+1 с), без удвоения
        let ex = "binance-ban-test";
        let until = now_ms() + 3_600_000;
        let pause = hit(ex, &format!("IP banned until {until}"));
        assert!((3_600_000..=3_601_000).contains(&pause), "{pause}");
        assert_eq!(snapshot(ex)["ban"]["exact"], json!(true));
    }
}
