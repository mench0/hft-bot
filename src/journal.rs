//! Журнал отладки (как Java TradeLog и RequestLog, настройка debugRequests — включается на лету):
//! `logs/actions.log` — действия в админке и в торговле (ордера, исполнения, отказы, отмены, ответы биржи), секреты скрыты;
//! `logs/requests.log` — каждый запрос к бирже (вид, биржа). Папка — LOG_DIR (по умолчанию logs).

use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

static ON: AtomicBool = AtomicBool::new(false);
static FILES: Mutex<()> = Mutex::new(());

pub fn set_on(on: bool) {
    if ON.swap(on, Ordering::Relaxed) != on {
        tracing::info!(
            "Режим отладки (debugRequests) {}",
            if on {
                "включён: logs/actions.log, logs/requests.log"
            } else {
                "выключен"
            }
        );
    }
}

pub fn on() -> bool {
    ON.load(Ordering::Relaxed)
}

fn dir() -> std::path::PathBuf {
    crate::util::env("LOG_DIR")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "logs".into())
        .into()
}

fn append(file: &str, line: &str) {
    let _g = FILES.lock().unwrap();
    let d = dir();
    let _ = std::fs::create_dir_all(&d);
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(d.join(file))
    {
        let _ = writeln!(f, "{} {line}", crate::util::iso_ms(crate::util::now_ms()));
    }
}

/// Действие: «[биржа] символ — текст».
pub fn action(ex: &str, symbol: Option<&str>, text: &str) {
    if !on() {
        return;
    }
    let line = format!(
        "[{ex}] {}{text}",
        symbol.map_or(String::new(), |s| format!("{s} — "))
    );
    tracing::info!(target: "hft.trade", "{line}");
    append("actions.log", &line);
}

/// Запрос к бирже.
pub fn request(ex: &str, source: &str) {
    if on() {
        append("requests.log", &format!("[{ex}] {source}"));
    }
}

/// Запрос к админке: метод, путь и параметры; значения переменных /env и токены скрыты.
pub fn admin(method: &str, path: &str, query: &str) {
    if !on() {
        return;
    }
    let q: Vec<String> = query
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| match kv.split_once('=') {
            Some((k, _))
                if path.starts_with("/env")
                    || k.to_lowercase().contains("token")
                    || k.to_lowercase().contains("secret") =>
            {
                format!("{k}=***")
            }
            _ => kv.to_owned(),
        })
        .collect();
    append(
        "actions.log",
        &format!(
            "[админка] {method} {path}{}",
            if q.is_empty() {
                String::new()
            } else {
                format!("?{}", q.join("&"))
            }
        ),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_only_when_on_and_hides_secrets() {
        let d = tempfile::tempdir().unwrap();
        crate::util::set_env("LOG_DIR", d.path().to_str().unwrap());
        set_on(true);
        action("binance", Some("BTCUSDT"), "ордер BUY 1");
        admin("POST", "/env", "BINANCE_API_SECRET=abc");
        admin("POST", "/order", "exchange=binance&symbol=BTCUSDT");
        request("binance", "orders");
        let a = std::fs::read_to_string(d.path().join("actions.log")).unwrap();
        assert!(a.contains("[binance] BTCUSDT — ордер BUY 1"), "{a}");
        assert!(
            a.contains("POST /env?BINANCE_API_SECRET=***") && !a.contains("abc"),
            "{a}"
        );
        assert!(a.contains("POST /order?exchange=binance&symbol=BTCUSDT"));
        assert!(
            std::fs::read_to_string(d.path().join("requests.log"))
                .unwrap()
                .contains("[binance] orders")
        );
        set_on(false);
    }
}
