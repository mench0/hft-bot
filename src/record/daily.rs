//! Ежедневный отчёт по записи в Telegram: раз в сутки (reportDailyEnabled, час UTC — reportDailyHourUtc) — отчёт
//! report::day за вчера коротким текстом: задержки и обрывы бирж, markout своих исполнений и где «забирают»,
//! кто ведёт, поток сделок. Бот и чат — переменные окружения TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID (секрет не
//! хранится в настройках и не уходит в админку). Отправленный день помнится в recordDir/.daily-sent — после
//! перезапуска не повторяется. Одно сообщение в сутки: лимиты Telegram не задеваются.

use serde_json::{Map, Value};
use std::fmt::Write;
use std::path::Path;

/// Длина сообщения Telegram, не больше.
const MAX_LEN: usize = 4000;

fn f(v: &Value) -> String {
    match v.as_f64() {
        Some(x) if x > 0.0 => format!("+{x}"),
        Some(x) => format!("{x}"),
        None => "—".into(),
    }
}

/// Текст отчёта (обычный текст, без разметки).
pub fn text(r: &Value) -> String {
    let mut t = String::new();
    let _ = writeln!(
        t,
        "Отчёт HFT-бота за {} (UTC)\nСобытий {}, своих исполнений {}",
        r["day"].as_str().unwrap_or("?"),
        r["events"],
        r["fills"]
    );
    let _ = writeln!(t, "\nЗадержка данных, мс (p50 / p99) и обрывы:");
    for x in r["latency"].as_array().into_iter().flatten() {
        if x["samples"].as_u64().unwrap_or(0) > 0 {
            let _ = writeln!(
                t,
                "• {}: {} / {}, обрывов {}",
                x["exchange"].as_str().unwrap_or(""),
                x["p50Ms"],
                x["p99Ms"],
                x["feedErrors"]
            );
        } else {
            let _ = writeln!(
                t,
                "• {}: время биржи не приходит, обрывов {}",
                x["exchange"].as_str().unwrap_or(""),
                x["feedErrors"]
            );
        }
    }
    let mut mk: Vec<&Value> = r["markout"].as_array().into_iter().flatten().collect();
    if mk.is_empty() {
        let _ = writeln!(t, "\nMarkout: своих исполнений нет.");
    } else {
        // худшие сверху: здесь теряем
        mk.sort_by(|a, b| {
            a["markout1000msBps"]
                .as_f64()
                .unwrap_or(0.0)
                .total_cmp(&b["markout1000msBps"].as_f64().unwrap_or(0.0))
        });
        let _ = writeln!(
            t,
            "\nMarkout своих исполнений, б.п. (1 с / 5 с), худшие сверху:"
        );
        for x in mk.iter().take(8) {
            let _ = writeln!(
                t,
                "• {} {}: {} / {}, исполнений {}",
                x["exchange"].as_str().unwrap_or(""),
                x["symbol"].as_str().unwrap_or(""),
                f(&x["markout1000msBps"]),
                f(&x["markout5000msBps"]),
                x["fills"]
            );
        }
        let _ = writeln!(t, "\nПо причинам (markout 1 с):");
        for x in r["markoutByCause"].as_array().into_iter().flatten() {
            let _ = writeln!(
                t,
                "• {}: {} — {} ({} исп.)",
                x["factor"].as_str().unwrap_or(""),
                x["value"].as_str().unwrap_or(""),
                f(&x["markout1000msBps"]),
                x["fills"]
            );
        }
    }
    let mut ld: Vec<&Value> = r["leaders"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|x| x["lagMs"].as_u64().unwrap_or(0) > 0)
        .collect();
    ld.sort_by(|a, b| {
        b["corr"]
            .as_f64()
            .unwrap_or(0.0)
            .total_cmp(&a["corr"].as_f64().unwrap_or(0.0))
    });
    if !ld.is_empty() {
        let _ = writeln!(t, "\nКто ведёт (сильнейшие связи):");
        for x in ld.iter().take(5) {
            let _ = writeln!(
                t,
                "• {}: {} → {} на {} мс, корреляция {} (без сдвига {})",
                x["symbol"].as_str().unwrap_or(""),
                x["leader"].as_str().unwrap_or(""),
                x["follower"].as_str().unwrap_or(""),
                x["lagMs"],
                x["corr"],
                x["corrNoLag"]
            );
        }
    }
    let flow: Vec<&Value> = r["flow"].as_array().into_iter().flatten().collect();
    if !flow.is_empty() {
        let _ = writeln!(
            t,
            "\nСделок рынка: {} по {} монетам",
            flow.iter()
                .filter_map(|x| x["trades"].as_u64())
                .sum::<u64>(),
            flow.len()
        );
    }
    if t.len() > MAX_LEN {
        let mut cut = MAX_LEN;
        while !t.is_char_boundary(cut) {
            cut -= 1;
        }
        t.truncate(cut);
        t.push_str("\n…");
    }
    t
}

/// Отправить текст в Telegram (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID).
pub async fn send(text: &str) -> Result<(), String> {
    send_to(&telegram_base(), text).await
}

fn telegram_base() -> String {
    crate::infra::util::env("TELEGRAM_API_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "https://api.telegram.org".into())
}

async fn send_to(base: &str, text: &str) -> Result<(), String> {
    let get = |k: &str| {
        crate::infra::util::env(k)
            .ok()
            .filter(|v| !v.trim().is_empty())
    };
    let (Some(token), Some(chat)) = (get("TELEGRAM_BOT_TOKEN"), get("TELEGRAM_CHAT_ID")) else {
        return Err("не заданы TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID (вкладка «Окружение»)".into());
    };
    let r = reqwest::Client::new()
        .post(format!(
            "{}/bot{token}/sendMessage",
            base.trim_end_matches('/')
        ))
        .timeout(std::time::Duration::from_secs(15))
        .json(&serde_json::json!({"chat_id": chat, "text": text, "disable_web_page_preview": true}))
        .send()
        .await
        // текст ошибки reqwest содержит адрес с токеном — не показываем его
        .map_err(|e| format!("Telegram недоступен: {}", e.without_url()))?;
    let st = r.status();
    let v: Value = r.json().await.unwrap_or_default();
    if st.is_success() && v["ok"] == true {
        Ok(())
    } else {
        Err(format!(
            "Telegram ответил {st}: {}",
            v["description"].as_str().unwrap_or("")
        ))
    }
}

/// Отчёт за день и отправка (кнопка в админке и ежедневная задача).
pub async fn report_and_send(dir: &Path, day: &str) -> Result<String, String> {
    let (d, day2) = (dir.to_owned(), day.to_owned());
    let r = tokio::task::spawn_blocking(move || super::report::day(&d, &day2, None))
        .await
        .map_err(|e| e.to_string())??;
    let t = text(&r);
    send(&t).await?;
    Ok(t)
}

fn setting(s: &Map<String, Value>, k: &str) -> Option<String> {
    s.get(k).map(|v| {
        v.as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| v.to_string())
    })
}

/// Раз в минуту: пора ли слать отчёт за вчера.
pub fn spawn(settings: impl Fn() -> Option<Map<String, Value>> + Send + 'static) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tick.tick().await;
            let Some(s) = settings() else { return };
            if setting(&s, "reportDailyEnabled").as_deref() != Some("true") {
                continue;
            }
            let hour: u64 = setting(&s, "reportDailyHourUtc")
                .and_then(|h| h.parse().ok())
                .unwrap_or(1);
            let now = crate::infra::util::now_ms();
            if (now / 3_600_000) % 24 < hour {
                continue;
            }
            let day = crate::infra::util::iso_ms(now - 86_400_000)[..10].to_owned();
            let dir: std::path::PathBuf = setting(&s, "recordDir")
                .filter(|d| !d.trim().is_empty())
                .unwrap_or_else(|| "data/rec".into())
                .into();
            let mark = dir.join(".daily-sent");
            if std::fs::read_to_string(&mark).is_ok_and(|d| d.trim() == day) {
                continue;
            }
            match report_and_send(&dir, &day).await {
                Ok(_) => tracing::info!("Отчёт за {day} отправлен в Telegram"),
                Err(e) => tracing::warn!("Отчёт за {day} в Telegram не отправлен: {e}"),
            }
            // и при ошибке — не чаще раза в сутки (нет записи за вчера, нет токена)
            let _ = std::fs::create_dir_all(&dir);
            let _ = std::fs::write(&mark, &day);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::post};
    use std::sync::{Arc, Mutex};

    #[test]
    fn text_shows_worst_markout_first_and_fits() {
        let r = serde_json::json!({"day": "2026-10-30", "events": 10, "fills": 3,
            "latency": [{"exchange": "okx", "samples": 5, "p50Ms": 4, "p99Ms": 30, "feedErrors": 1}, {"exchange": "binance", "samples": 0, "feedErrors": 0}],
            "markout": [{"exchange": "gate", "symbol": "A", "fills": 2, "markout1000msBps": 3.0, "markout5000msBps": 1.0},
                        {"exchange": "gate", "symbol": "B", "fills": 1, "markout1000msBps": -9.5, "markout5000msBps": -12.0}],
            "markoutByCause": [{"factor": "время", "value": "остальное время", "fills": 3, "markout1000msBps": -1.0}],
            "leaders": [{"symbol": "A", "leader": "binance", "follower": "gate", "lagMs": 200, "corr": 0.8, "corrNoLag": 0.2}],
            "flow": [{"trades": 7}]});
        let t = text(&r);
        assert!(t.find("gate B").unwrap() < t.find("gate A").unwrap(), "{t}");
        assert!(
            t.contains("okx: 4 / 30, обрывов 1")
                && t.contains("binance → gate на 200 мс")
                && t.contains("Сделок рынка: 7"),
            "{t}"
        );
        let mut big = r.clone();
        big["markoutByCause"] =
            Value::Array((0..500).map(|_| r["markoutByCause"][0].clone()).collect());
        assert!(text(&big).chars().count() <= MAX_LEN + 2);
    }

    #[tokio::test]
    async fn sends_to_telegram_api() {
        let got: Arc<Mutex<Vec<(String, Value)>>> = Default::default();
        let g = got.clone();
        let app = Router::new().route(
            "/{bot}/sendMessage",
            post(
                move |axum::extract::Path(bot): axum::extract::Path<String>,
                      Json(v): Json<Value>| async move {
                    g.lock().unwrap().push((bot, v));
                    Json(serde_json::json!({"ok": true}))
                },
            ),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        crate::infra::util::set_env("TELEGRAM_BOT_TOKEN", "123:abc");
        crate::infra::util::set_env("TELEGRAM_CHAT_ID", "-100");
        send_to(&base, "привет").await.unwrap();
        let got = got.lock().unwrap();
        assert_eq!(got[0].0, "bot123:abc");
        assert_eq!(
            (got[0].1["chat_id"].as_str(), got[0].1["text"].as_str()),
            (Some("-100"), Some("привет"))
        );
    }
}
