//! Сигналы — только алерты, без торговли (как Java Signals, Discord, Announcements, ListingFeed).
//!
//! **Листинги**: раз в `listingPollSec` списки пар бирж (`listingExchanges`, * — все; спот и перпы) сверяются с прошлым
//! опросом; новая пара в валюте из `listingQuotes` (KRW — корейские Upbit, USDT…) — событие и алерт в Discord: где
//! появилась, между какими проверками и где монета уже торгуется. Первый опрос биржи только запоминает список.
//! **Анонсы**: раз в `announcePollSec` — официальные API анонсов листингов Bybit, OKX и KuCoin (заранее, до старта
//! торгов); у Upbit открытого API нет — push-фид по WebSocket (`LISTING_FEED_UPBIT`, по умолчанию
//! wss://kr.cryptolisting.ws, ключ — `LISTING_FEED_KEY`). Алерты — в вебхук `listingDiscordUrl`; одинаковый не чаще
//! раза в час (анонс — в сутки), текст обрезается до 2000 символов (лимит Discord).

use crate::screener::{SUPPORTED, Sources, Ticker};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Сколько последних событий хранить.
const KEEP: usize = 200;
/// Биржи с официальным API анонсов.
pub const ANNOUNCE: &[&str] = &["bybit", "okx", "kucoin"];
pub const UPBIT_FEED: &str = "wss://kr.cryptolisting.ws";

fn ann_path(ex: &str) -> &'static str {
    match ex {
        "bybit" => "/v5/announcements/index?locale=en-US&type=new_crypto&limit=20",
        "okx" => "/api/v5/support/announcements?annType=announcements-new-listings",
        _ => "/api/v3/announcements?annType=new-listings&lang=en_US&pageSize=20",
    }
}

// ───────────────────────── Discord ─────────────────────────

#[derive(Default)]
struct DiscordState {
    sent_keys: HashMap<String, u64>,
    sent: u64,
    failed: u64,
    last_error: String,
}

static DISCORD: Mutex<Option<DiscordState>> = Mutex::new(None);

fn discord<R>(f: impl FnOnce(&mut DiscordState) -> R) -> R {
    f(DISCORD.lock().unwrap().get_or_insert_with(Default::default))
}

/// POST {content} на вебхук; код ответа.
async fn post(url: &str, text: &str) -> Result<u16, String> {
    let content: String = if text.chars().count() > 2000 {
        text.chars().take(1997).collect::<String>() + "…"
    } else {
        text.to_owned()
    };
    let r = reqwest::Client::new()
        .post(url)
        .timeout(Duration::from_secs(10))
        .json(&json!({"content": content}))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(r.status().as_u16())
}

/// Алерт в вебхук; повтор с тем же ключом раньше cooldown — пропускается. Отправка в фоне, ошибка — в счётчик и лог.
pub fn alert(url: &str, key: &str, text: String, cooldown_ms: u64) -> bool {
    if url.trim().is_empty() {
        return false;
    }
    let now = crate::util::now_ms();
    let fresh = discord(|d| match d.sent_keys.get(key) {
        Some(t) if now.saturating_sub(*t) < cooldown_ms => false,
        _ => {
            d.sent_keys.insert(key.to_owned(), now);
            true
        }
    });
    if !fresh {
        return false;
    }
    let url = url.trim().to_owned();
    tokio::spawn(async move {
        let r = post(&url, &text).await;
        discord(|d| match r {
            Ok(c) if c < 300 => d.sent += 1,
            Ok(c) => {
                d.failed += 1;
                d.last_error = format!("HTTP {c}");
                tracing::warn!("Discord: HTTP {c}");
            }
            Err(e) => {
                d.failed += 1;
                tracing::warn!("Discord: не отправлено — {e}");
                d.last_error = e;
            }
        });
    });
    true
}

/// Проверочный алерт сразу; ошибка — Err.
pub async fn send_now(url: &str, text: &str) -> Result<(), String> {
    if url.trim().is_empty() {
        return Err("адрес вебхука Discord не задан".into());
    }
    match post(url.trim(), text).await? {
        c if c < 300 => {
            discord(|d| d.sent += 1);
            Ok(())
        }
        c => Err(format!("Discord ответил HTTP {c}")),
    }
}

pub fn discord_stats(url: &str) -> Value {
    let on = !url.trim().is_empty();
    discord(
        |d| json!({"enabled": on, "listing": on, "sent": d.sent, "failed": d.failed, "lastError": d.last_error}),
    )
}

// ───────────────────────── анонсы ─────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub struct Announcement {
    pub exchange: String,
    pub id: String,
    pub title: String,
    pub url: String,
    pub published_at: u64,
    pub starts_at: u64,
    pub coins: Vec<String>,
    pub source: String,
}

const NOT_COINS: &[&str] = &[
    "UTC", "USDT", "USDC", "USD", "KRW", "BTC", "ETH", "EUR", "API", "NEW", "VIP", "APR",
];
const PAIR_QUOTES: &[&str] = &["USDT", "USDC", "USD", "KRW", "BTC", "ETH", "EUR"];

fn coin_like(s: &str) -> bool {
    (2..=15).contains(&s.len())
        && s.chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}

/// Монеты из заголовка: «Name (XYZ)», «XYZUSDT», «XYZ/USDT», «XYZ-USDT».
pub fn coins(title: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut add = |c: &str| {
        if coin_like(c) && !NOT_COINS.contains(&c) && !out.iter().any(|x| x == c) {
            out.push(c.to_owned());
        }
    };
    for part in title.split('(').skip(1) {
        if let Some(inner) = part.split(')').next() {
            add(inner);
        }
    }
    let words: Vec<&str> = title
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '/' || c == '-'))
        .filter(|w| !w.is_empty())
        .collect();
    for w in words {
        if let Some((a, b)) = w.split_once(['/', '-']) {
            if PAIR_QUOTES.contains(&b) {
                add(a);
            }
            continue;
        }
        for q in PAIR_QUOTES {
            if let Some(a) = w.strip_suffix(q).filter(|a| a.len() >= 2) {
                add(a);
                break;
            }
        }
    }
    out
}

/// Дней от 1970-01-01 до даты (григорианский календарь).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Старт торгов из текста анонса, мс: «2026-10-22 10:00 (UTC+8)» или «Oct 22, 2026, 10:00 (UTC)»; 0 — не найден.
pub fn start_time(text: &str) -> u64 {
    let b = text.as_bytes();
    let utc_offset = |rest: &str| -> Option<i64> {
        let r = rest.trim_start().trim_start_matches('(').trim_start();
        let r = r.strip_prefix("UTC")?;
        let r = r.trim_start();
        let sign = match r.chars().next() {
            Some('+') => 1,
            Some('-') => -1,
            _ => return Some(0),
        };
        let n: String = r[1..]
            .trim_start()
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        Some(sign * n.parse::<i64>().unwrap_or(0))
    };
    let hm = |s: &str| -> Option<(i64, i64, usize)> {
        let (h, rest) = s.split_once(':')?;
        let h: i64 = h.trim().parse().ok()?;
        let m: String = rest.chars().take(2).collect();
        Some((h, m.parse().ok()?, h.to_string().len() + 3))
    };
    // ISO: YYYY-MM-DD[ T,]HH:MM
    // только по границам символов: в заголовках бывает не только латиница (корейский у Upbit)
    for (i, _) in text.char_indices().filter(|(i, _)| *i + 15 <= b.len()) {
        let w = &text[i..];
        if !w.is_char_boundary(10) {
            continue;
        }
        let d = &w[..10];
        let ok = d.as_bytes().iter().enumerate().all(|(k, c)| {
            if k == 4 || k == 7 {
                *c == b'-'
            } else {
                c.is_ascii_digit()
            }
        });
        if !ok {
            continue;
        }
        let rest = w[10..].trim_start_matches([' ', 'T', ',']);
        let Some((h, mi, used)) = hm(rest) else {
            continue;
        };
        let rest = &rest[used.min(rest.len())..];
        let rest = rest
            .strip_prefix(|c: char| c == ':')
            .map_or(rest, |r| &r[2.min(r.len())..]);
        let Some(off) = utc_offset(rest) else {
            continue;
        };
        let (y, m, dd) = (
            d[..4].parse().unwrap_or(0),
            d[5..7].parse().unwrap_or(0),
            d[8..10].parse().unwrap_or(0),
        );
        let secs = days_from_civil(y, m, dd) * 86_400 + h * 3600 + mi * 60 - off * 3600;
        return (secs.max(0) as u64) * 1000;
    }
    // «Oct 22, 2026, 10:00 (UTC+8)» / «October 22, 2026 at 10:00 UTC»
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let words: Vec<&str> = text.split_whitespace().collect();
    for i in 0..words.len().saturating_sub(3) {
        let Some(m) = MONTHS.iter().position(|mm| words[i].starts_with(mm)) else {
            continue;
        };
        let d: String = words[i + 1]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        let y: String = words[i + 2]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        let (Ok(d), Ok(y)) = (d.parse::<i64>(), y.parse::<i64>()) else {
            continue;
        };
        let mut k = i + 3;
        if words.get(k) == Some(&"at") {
            k += 1;
        }
        let Some(t) = words.get(k) else { continue };
        let Some((h, mi, _)) = hm(t) else { continue };
        let rest = words[k + 1..].join(" ");
        let Some(off) = utc_offset(&rest) else {
            continue;
        };
        let secs = days_from_civil(y, m as i64 + 1, d) * 86_400 + h * 3600 + mi * 60 - off * 3600;
        return (secs.max(0) as u64) * 1000;
    }
    0
}

fn s(v: &Value) -> String {
    match v {
        Value::String(x) => x.clone(),
        Value::Number(n) => n.to_string(),
        _ => String::new(),
    }
}

fn ms(v: &Value) -> u64 {
    v.as_u64().or_else(|| v.as_str()?.parse().ok()).unwrap_or(0)
}

/// Анонсы листингов из ответа официального API биржи.
pub fn parse_announcements(ex: &str, v: &Value, url: &str) -> Vec<Announcement> {
    let src = format!("официальный API анонсов {ex}: {url}");
    let mk = |id: String, title: String, link: String, published: u64, start: u64| Announcement {
        exchange: ex.to_owned(),
        id: if id.is_empty() {
            if link.is_empty() {
                title.clone()
            } else {
                link.clone()
            }
        } else {
            id
        },
        coins: coins(&title),
        title,
        url: link,
        published_at: published,
        starts_at: start,
        source: src.clone(),
    };
    let arr = |x: &Value| x.as_array().cloned().unwrap_or_default();
    match ex {
        "bybit" => arr(&v["result"]["list"])
            .iter()
            .map(|x| {
                let (title, link) = (s(&x["title"]), s(&x["url"]));
                let start = match ms(&x["startDateTimestamp"]) {
                    0 => start_time(&format!("{title} {}", s(&x["description"]))),
                    t => t,
                };
                let pubt = match ms(&x["publishTime"]) {
                    0 => ms(&x["dateTimestamp"]),
                    t => t,
                };
                mk(link.clone(), title, link, pubt, start)
            })
            .collect(),
        "okx" => arr(&v["data"])
            .iter()
            .flat_map(|page| arr(&page["details"]))
            .map(|x| {
                let (title, link) = (s(&x["title"]), s(&x["url"]));
                let start = start_time(&title);
                mk(link.clone(), title, link, ms(&x["pTime"]), start)
            })
            .collect(),
        "kucoin" => arr(&v["data"]["items"])
            .iter()
            .map(|x| {
                let title = s(&x["annTitle"]);
                let start = start_time(&format!("{title} {}", s(&x["annDesc"])));
                mk(
                    s(&x["annId"]),
                    title,
                    s(&x["annUrl"]),
                    ms(&x["cTime"]),
                    start,
                )
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Сообщение push-фида анонсов (формат сервиса не задокументирован — разбор терпимый, как в Java ListingFeed).
pub fn parse_feed(ex: &str, n: &Value) -> Vec<Announcement> {
    if let Some(a) = n.as_array() {
        return a.iter().flat_map(|x| parse_feed(ex, x)).collect();
    }
    let Some(o) = n.as_object() else {
        return Vec::new();
    };
    let text = |keys: &[&str]| {
        keys.iter()
            .find_map(|k| o.get(*k).map(s).filter(|v| !v.trim().is_empty()))
            .unwrap_or_default()
    };
    let title = text(&["title", "subject", "headline", "message", "text"]);
    if title.is_empty() {
        return ["data", "payload", "announcement", "notice", "result"]
            .iter()
            .find_map(|w| o.get(*w))
            .map_or_else(Vec::new, |x| parse_feed(ex, x));
    }
    let venue = text(&["exchange", "source", "venue", "market"]).to_lowercase();
    if !venue.is_empty() && !venue.contains(ex) {
        return Vec::new();
    }
    let kind = text(&["type", "category", "kind", "event"]).to_lowercase();
    if kind.contains("delist") || kind.contains("maint") {
        return Vec::new();
    }
    let link = text(&["url", "link", "href"]);
    let mut id = text(&["id", "noticeId", "notice_id", "uuid"]);
    let published = [
        "publishedAt",
        "published_at",
        "timestamp",
        "ts",
        "time",
        "created_at",
        "createdAt",
        "date",
    ]
    .iter()
    .find_map(|k| o.get(*k).and_then(Value::as_u64))
    .map_or(0, |x| if x < 10_000_000_000 { x * 1000 } else { x });
    let mut cs: Vec<String> = Vec::new();
    for k in ["coins", "symbols", "tickers", "symbol", "coin", "ticker"] {
        let items: Vec<String> = match o.get(k) {
            Some(Value::Array(a)) => a.iter().map(s).collect(),
            Some(v) => s(v).split([',', ' ']).map(str::to_owned).collect(),
            None => continue,
        };
        for c in items {
            let c = c.trim().to_uppercase();
            let c = ["KRW-", "USDT-", "BTC-"]
                .iter()
                .find_map(|p| c.strip_prefix(p))
                .unwrap_or(&c)
                .to_owned();
            if !c.is_empty() && !cs.contains(&c) {
                cs.push(c);
            }
        }
    }
    if cs.is_empty() {
        cs = coins(&title);
    }
    if id.is_empty() {
        id = if link.is_empty() {
            title.clone()
        } else {
            link.clone()
        };
    }
    let start = start_time(&format!("{title} {}", text(&["body", "content"])));
    vec![Announcement {
        exchange: ex.into(),
        id,
        title,
        url: link,
        published_at: published,
        starts_at: start,
        coins: cs,
        source: format!("фид анонсов {ex}"),
    }]
}

// ───────────────────────── состояние ─────────────────────────

#[derive(Default)]
struct State {
    /// Биржа → известные пары «символ:spot|perp»; когда проверялась.
    known: HashMap<String, HashSet<String>>,
    checked_at: HashMap<String, u64>,
    last: BTreeMap<String, Vec<Ticker>>,
    listings: VecDeque<Value>,
    announces: VecDeque<Value>,
    known_ann: HashMap<String, HashSet<String>>,
    errors: BTreeMap<String, String>,
    listing_checked_at: u64,
    announce_checked_at: u64,
    next_listing: u64,
    next_announce: u64,
    feed: Map<String, Value>,
}

/// Сигналы (листинги и анонсы).
#[derive(Default, Clone)]
pub struct Signals {
    st: Arc<Mutex<State>>,
    src: Sources,
}

/// Биржи из списка настроек: «*» — все, пусто — ни одной.
pub fn exchanges(list: &str) -> Vec<String> {
    if list.trim() == "*" {
        return SUPPORTED.iter().map(|s| s.to_string()).collect();
    }
    let mut v: Vec<String> = list
        .split(',')
        .map(|x| x.trim().to_lowercase())
        .filter(|x| SUPPORTED.contains(&x.as_str()))
        .collect();
    v.dedup();
    v
}

fn push(d: &mut VecDeque<Value>, v: Value) {
    d.push_front(v);
    d.truncate(KEEP);
}

fn setting(g: &Map<String, Value>, k: &str) -> String {
    match g.get(k) {
        Some(Value::String(x)) => x.clone(),
        Some(v) => v.to_string(),
        None => String::new(),
    }
}

impl Signals {
    pub fn with_sources(src: Sources) -> Signals {
        Signals {
            st: Arc::default(),
            src,
        }
    }

    /// Где ещё торгуется монета (кроме рынка события): «binance (спот, перп)».
    fn also_on(
        &self,
        st: &State,
        base: &str,
        ex: Option<&str>,
        perp_of_event: bool,
    ) -> Vec<String> {
        st.last
            .iter()
            .filter_map(|(e, l)| {
                let (mut spot, mut perp) = (false, false);
                for x in l.iter().filter(|x| {
                    x.base == base && !(Some(e.as_str()) == ex && x.perp == perp_of_event)
                }) {
                    if x.perp { perp = true } else { spot = true }
                }
                (spot || perp).then(|| {
                    format!(
                        "{e} ({})",
                        if spot && perp {
                            "спот, перп"
                        } else if spot {
                            "спот"
                        } else {
                            "перп"
                        }
                    )
                })
            })
            .collect()
    }

    /// Сравнить список пар биржи с прошлым; новые пары в валютах листингов — события и алерт. Первый раз — база.
    pub fn on_tickers(&self, ex: &str, all: Vec<Ticker>, quotes: &[String], hook: &str) {
        let mut st = self.st.lock().unwrap();
        let now_set: HashSet<String> = all
            .iter()
            .map(|t| format!("{}:{}", t.symbol, if t.perp { "perp" } else { "spot" }))
            .collect();
        let at = crate::util::now_ms();
        let prev_at = st.checked_at.insert(ex.to_owned(), at);
        st.last.insert(ex.to_owned(), all.clone());
        let before = st.known.insert(ex.to_owned(), now_set.clone());
        let Some(before) = before else { return };
        if now_set.is_empty() {
            return;
        }
        for t in &all {
            let key = format!("{}:{}", t.symbol, if t.perp { "perp" } else { "spot" });
            if before.contains(&key) || !quotes.contains(&t.quote) {
                continue;
            }
            let also = self.also_on(&st, &t.base, Some(ex), t.perp);
            let since = prev_at.unwrap_or(at);
            let source = format!(
                "официальный API {ex} — список пар ({}){}",
                if t.perp { "перпы" } else { "спот" },
                self.src
                    .listing_source(ex, t.perp)
                    .map_or(String::new(), |u| format!(": {u}"))
            );
            tracing::warn!(
                "[listing] {ex} {} {} ({})",
                t.symbol,
                if t.perp { "перп" } else { "спот" },
                if also.is_empty() {
                    "больше нигде".into()
                } else {
                    format!("уже на {also:?}")
                }
            );
            push(
                &mut st.listings,
                json!({"at": at, "since": since, "exchange": ex, "symbol": t.symbol, "base": t.base, "quote": t.quote,
                "perp": t.perp, "price": if t.last.is_finite() { json!(t.last) } else { Value::Null }, "alsoOn": also, "source": source}),
            );
            alert(
                hook,
                &format!("listing:{ex}:{key}"),
                format!(
                    "🆕 **Листинг**: {}/{} на **{ex}** ({}) · появился между <t:{}:T> и <t:{}:T> (<t:{}:R>){}{} · источник: {source}",
                    t.base,
                    t.quote,
                    if t.perp { "перп" } else { "спот" },
                    since / 1000,
                    at / 1000,
                    at / 1000,
                    if t.last.is_finite() && t.last > 0.0 {
                        format!(" · цена {}", t.last)
                    } else {
                        String::new()
                    },
                    if also.is_empty() {
                        " · больше нигде не торгуется".to_owned()
                    } else {
                        format!(" · уже торгуется на: {}", also.join(", "))
                    }
                ),
                3_600_000,
            );
        }
    }

    /// Новые анонсы биржи — события и алерт; первая проверка (не push) только запоминает.
    pub fn on_announcements(&self, ex: &str, list: Vec<Announcement>, pushed: bool, hook: &str) {
        let mut st = self.st.lock().unwrap();
        let first = !st.known_ann.contains_key(ex);
        let fresh: Vec<Announcement> = {
            let seen = st.known_ann.entry(ex.to_owned()).or_default();
            list.into_iter()
                .filter(|a| seen.insert(a.id.clone()))
                .collect()
        };
        if first && !pushed {
            return;
        }
        for a in fresh {
            let mut also: Vec<String> = Vec::new();
            for c in &a.coins {
                for w in self.also_on(&st, c, None, false) {
                    if !also.contains(&w) {
                        also.push(w);
                    }
                }
            }
            tracing::warn!("[announce] {ex}: {} ({})", a.title, a.url);
            push(
                &mut st.announces,
                json!({"at": crate::util::now_ms(), "exchange": ex, "title": a.title, "url": a.url, "publishedAt": a.published_at,
                "startsAt": a.starts_at, "coins": a.coins, "alsoOn": also, "source": a.source}),
            );
            alert(
                hook,
                &format!("announce:{ex}:{}", a.id),
                format!(
                    "🆕 **Листинг** на **{ex}**: {}{}{}{}{} · источник: анонс — {}{}",
                    a.title,
                    if a.coins.is_empty() {
                        String::new()
                    } else {
                        format!(" · монеты: {}", a.coins.join(", "))
                    },
                    if a.starts_at > 0 {
                        format!(
                            " · старт торгов <t:{}:f> (<t:{}:R>)",
                            a.starts_at / 1000,
                            a.starts_at / 1000
                        )
                    } else {
                        String::new()
                    },
                    if a.published_at > 0 {
                        format!(" · опубликован <t:{}:f>", a.published_at / 1000)
                    } else {
                        String::new()
                    },
                    if also.is_empty() {
                        String::new()
                    } else {
                        format!(" · уже торгуется на: {}", also.join(", "))
                    },
                    a.source,
                    if a.url.is_empty() {
                        String::new()
                    } else {
                        format!(" · ссылка: {}", a.url)
                    }
                ),
                86_400_000,
            );
        }
    }

    /// Один опрос списков пар выбранных бирж.
    pub async fn poll_listings(&self, g: &Map<String, Value>) {
        let quotes: Vec<String> = setting(g, "listingQuotes")
            .split(',')
            .map(|x| x.trim().to_uppercase())
            .filter(|x| !x.is_empty())
            .collect();
        let hook = setting(g, "listingDiscordUrl");
        for ex in exchanges(&setting(g, "listingExchanges")) {
            let (spot, perp) =
                futures_util::join!(self.src.listing_tickers(&ex), self.src.perp_tickers(&ex));
            let mut all = match spot {
                Ok(v) => {
                    self.st.lock().unwrap().errors.remove(&ex);
                    v
                }
                Err(e) => {
                    self.st.lock().unwrap().errors.insert(ex.clone(), e);
                    continue;
                }
            };
            match perp {
                Ok(v) => all.extend(v),
                Err(e) => {
                    self.st
                        .lock()
                        .unwrap()
                        .errors
                        .insert(format!("{ex}:perp"), e);
                }
            }
            self.on_tickers(&ex, all, &quotes, &hook);
        }
        self.st.lock().unwrap().listing_checked_at = crate::util::now_ms();
    }

    /// Один опрос официальных анонсов (Bybit, OKX, KuCoin) из выбранных бирж.
    pub async fn poll_announcements(&self, g: &Map<String, Value>) {
        let hook = setting(g, "listingDiscordUrl");
        for ex in exchanges(&setting(g, "listingExchanges"))
            .into_iter()
            .filter(|e| ANNOUNCE.contains(&e.as_str()))
        {
            let Some(base) = self.src.urls.get(&ex) else {
                continue;
            };
            let url = format!("{base}{}", ann_path(&ex));
            let _ = crate::ratelimit::take(&ex, "discovery").await;
            match self.src.get_json(&url).await {
                Ok(v) => {
                    self.st
                        .lock()
                        .unwrap()
                        .errors
                        .remove(&format!("{ex}:анонсы"));
                    self.on_announcements(&ex, parse_announcements(&ex, &v, &url), false, &hook);
                }
                Err(e) => {
                    self.st
                        .lock()
                        .unwrap()
                        .errors
                        .insert(format!("{ex}:анонсы"), e);
                }
            }
        }
        self.st.lock().unwrap().announce_checked_at = crate::util::now_ms();
    }

    /// Состояние и события для /signals (формат Java).
    pub fn json(&self, g: &Map<String, Value>) -> Value {
        let st = self.st.lock().unwrap();
        let sources: Map<String, Value> = SUPPORTED
            .iter()
            .map(|ex| {
                let mut m = Map::new();
                if let Some(u) = self.src.listing_source(ex, false) {
                    m.insert("спот".into(), json!(u));
                }
                if let Some(u) = self.src.listing_source(ex, true) {
                    m.insert("перпы".into(), json!(u));
                }
                (ex.to_string(), Value::Object(m))
            })
            .collect();
        let mut ann_src: Map<String, Value> = ANNOUNCE
            .iter()
            .filter_map(|ex| {
                Some((
                    ex.to_string(),
                    json!(format!("{}{}", self.src.urls.get(*ex)?, ann_path(ex))),
                ))
            })
            .collect();
        ann_src.insert("upbit".into(), json!(upbit_feed_url()));
        let mut ann_ex: Vec<&str> = ANNOUNCE.to_vec();
        ann_ex.push("upbit");
        json!({"listings": st.listings, "announcements": st.announces, "announceCheckedAt": st.announce_checked_at,
            "announceExchanges": ann_ex, "listingSources": sources, "announceSources": ann_src,
            "announceFeeds": {"upbit": st.feed}, "listingCheckedAt": st.listing_checked_at,
            "knownPairs": st.known.iter().map(|(k, v)| (k.clone(), json!(v.len()))).collect::<BTreeMap<_, _>>(),
            "errors": st.errors, "discord": discord_stats(&setting(g, "listingDiscordUrl")), "supported": SUPPORTED})
    }

    /// Фон: проверка раз в 5 с; опросы — по периодам из настроек (меняются на лету); push-фид Upbit — пока Upbit выбран.
    pub fn spawn(&self, settings: impl Fn() -> Option<Map<String, Value>> + Send + 'static) {
        let me = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            let mut feed: Option<tokio::task::JoinHandle<()>> = None;
            loop {
                tick.tick().await;
                let Some(g) = settings() else { return };
                let on = g
                    .get("listingEnabled")
                    .is_none_or(|v| *v == true || v == "true");
                let num = |k: &str, d: u64| {
                    g.get(k)
                        .and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok()))
                        .unwrap_or(d)
                };
                let now = crate::util::now_ms();
                let want_feed = on
                    && exchanges(&setting(&g, "listingExchanges"))
                        .iter()
                        .any(|e| e == "upbit");
                match (&feed, want_feed) {
                    (None, true) => {
                        feed = Some(tokio::spawn(upbit_feed(
                            me.clone(),
                            setting(&g, "listingDiscordUrl"),
                        )))
                    }
                    (Some(h), false) => {
                        h.abort();
                        feed = None;
                    }
                    _ => {}
                }
                if !on {
                    continue;
                }
                let (due_ann, due_list) = {
                    let mut st = me.st.lock().unwrap();
                    let a = now >= st.next_announce;
                    if a {
                        st.next_announce = now + num("announcePollSec", 60) * 1000;
                    }
                    let l = now >= st.next_listing;
                    if l {
                        st.next_listing = now + num("listingPollSec", 60) * 1000;
                    }
                    (a, l)
                };
                if due_ann {
                    me.poll_announcements(&g).await;
                }
                if due_list {
                    me.poll_listings(&g).await;
                }
            }
        });
    }
}

pub fn upbit_feed_url() -> String {
    crate::util::env("LISTING_FEED_UPBIT")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| UPBIT_FEED.into())
}

/// Push-фид анонсов Upbit: подключение с ключом LISTING_FEED_KEY (Authorization: Bearer и X-API-Key), переподключение
/// через 5 с (401/403 — через 60 с); каждый новый анонс — сразу событие и алерт.
async fn upbit_feed(sig: Signals, hook: String) {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let url = upbit_feed_url();
    let mut logged = 0;
    loop {
        let mut req = match url.as_str().into_client_request() {
            Ok(r) => r,
            Err(e) => {
                sig.st.lock().unwrap().feed =
                    json!({"url": url, "connected": false, "error": e.to_string()})
                        .as_object()
                        .cloned()
                        .unwrap_or_default();
                return;
            }
        };
        let key = crate::util::env("LISTING_FEED_KEY")
            .ok()
            .filter(|k| !k.trim().is_empty());
        if let Some(k) = &key
            && let (Ok(a), Ok(x)) = (format!("Bearer {}", k.trim()).parse(), k.trim().parse())
        {
            req.headers_mut().insert("Authorization", a);
            req.headers_mut().insert("X-API-Key", x);
        }
        let err = match tokio::time::timeout(
            Duration::from_secs(10),
            tokio_tungstenite::connect_async(req),
        )
        .await
        {
            Ok(Ok((mut ws, _))) => {
                tracing::info!("[listing-feed] upbit: подключено {url}");
                sig.st.lock().unwrap().feed = json!({"url": url, "connected": true, "connectedAt": crate::util::now_ms(), "error": ""}).as_object().cloned().unwrap_or_default();
                loop {
                    match ws.next().await {
                        Some(Ok(m)) if m.is_text() => {
                            let t = m.into_text().map(|t| t.to_string()).unwrap_or_default();
                            if logged < 5 {
                                logged += 1;
                                tracing::info!(
                                    "[listing-feed] upbit: сообщение {}",
                                    t.chars().take(2000).collect::<String>()
                                );
                            }
                            sig.st
                                .lock()
                                .unwrap()
                                .feed
                                .insert("lastMessageAt".into(), json!(crate::util::now_ms()));
                            if let Ok(v) = serde_json::from_str::<Value>(&t) {
                                sig.on_announcements("upbit", parse_feed("upbit", &v), true, &hook);
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => break e.to_string(),
                        None => break "соединение закрыто".into(),
                    }
                }
            }
            Ok(Err(e)) => e.to_string(),
            Err(_) => "подключение не ответило за 10 с".into(),
        };
        let auth = err.contains("401") || err.contains("403");
        let why = if auth {
            format!(
                "{err}{}",
                if key.is_none() {
                    " (сервису нужен ключ: задайте LISTING_FEED_KEY)"
                } else {
                    " (ключ LISTING_FEED_KEY не принят)"
                }
            )
        } else {
            err
        };
        let delay = if auth { 60 } else { 5 };
        tracing::warn!("[listing-feed] upbit: {why} — переподключение через {delay} с");
        {
            let mut st = sig.st.lock().unwrap();
            st.feed.insert("connected".into(), json!(false));
            st.feed.insert("url".into(), json!(url));
            st.feed.insert("error".into(), json!(why));
        }
        tokio::time::sleep(Duration::from_secs(delay)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(ex: &str, sym: &str, perp: bool) -> Ticker {
        let (b, q) = crate::paper::split(sym).unwrap();
        Ticker {
            exchange: ex.into(),
            symbol: sym.into(),
            base: b,
            quote: q,
            last: 1.5,
            bid: 1.0,
            ask: 2.0,
            volume: 1.0,
            change_pct: 0.0,
            perp,
            funding8h: f64::NAN,
            venue: sym.into(),
        }
    }

    #[test]
    fn coins_and_start_time() {
        assert_eq!(
            coins("Bybit Will List Foo (FOO) and BARUSDT Perpetual"),
            vec!["FOO", "BAR"]
        );
        assert_eq!(
            coins("OKX to list XYZ/USDT and ABC-USDC spot"),
            vec!["XYZ", "ABC"]
        );
        assert!(coins("New listing: (USDT) rewards").is_empty());
        // 2026-10-22 10:00 UTC = 1792663200
        assert_eq!(
            start_time("Trading starts 2026-10-22 10:00 (UTC)"),
            1_792_663_200_000
        );
        assert_eq!(
            start_time("Trading starts 2026-10-22 18:00 (UTC+8)"),
            1_792_663_200_000
        );
        assert_eq!(
            start_time("Spot trading: Oct 22, 2026, 10:00 (UTC)"),
            1_792_663_200_000
        );
        assert_eq!(
            start_time("October 22, 2026 at 10:00 UTC"),
            1_792_663_200_000
        );
        assert_eq!(start_time("no time here"), 0);
        assert_eq!(
            start_time("신규 상장: 2026-10-22 19:00 (UTC+9) 거래 시작"),
            1_792_663_200_000
        );
    }

    #[test]
    fn parse_official_and_feed() {
        let b = parse_announcements(
            "bybit",
            &json!({"result": {"list": [{"title": "New Listing: FOO (FOO)", "url": "https://x/1",
            "publishTime": 1700000000000u64, "startDateTimestamp": 1700000500000u64}]}}),
            "u",
        );
        assert_eq!(
            (b[0].id.as_str(), b[0].coins.clone(), b[0].starts_at),
            ("https://x/1", vec!["FOO".to_string()], 1_700_000_500_000)
        );
        let o = parse_announcements(
            "okx",
            &json!({"data": [{"details": [{"title": "OKX to list BAR (BAR)", "url": "https://o/2", "pTime": "1700000000000"}]}]}),
            "u",
        );
        assert_eq!(
            (o[0].published_at, o[0].coins[0].as_str()),
            (1_700_000_000_000, "BAR")
        );
        let k = parse_announcements(
            "kucoin",
            &json!({"data": {"items": [{"annId": 7, "annTitle": "BAZ (BAZ) Gets Listed", "annUrl": "https://k/7", "cTime": 1}]}}),
            "u",
        );
        assert_eq!(k[0].id, "7");
        let f = parse_feed(
            "upbit",
            &json!({"data": [{"title": "신규 상장 (QQQ)", "exchange": "Upbit", "symbol": "KRW-QQQ", "timestamp": 1700000000}]}),
        );
        assert_eq!(
            (f[0].coins.clone(), f[0].published_at),
            (vec!["QQQ".to_string()], 1_700_000_000_000)
        );
        assert!(parse_feed("upbit", &json!({"title": "x", "exchange": "bithumb"})).is_empty());
        assert!(parse_feed("upbit", &json!({"title": "x", "type": "delisting"})).is_empty());
    }

    #[tokio::test]
    async fn listings_and_announcements_events() {
        let s = Signals::default();
        let q = vec!["USDT".to_string(), "KRW".to_string()];
        s.on_tickers(
            "binance",
            vec![
                t("binance", "AAAUSDT", false),
                t("binance", "ZZZUSDT", true),
            ],
            &q,
            "",
        );
        assert!(
            s.st.lock().unwrap().listings.is_empty(),
            "первый опрос — база"
        );
        s.on_tickers(
            "binance",
            vec![
                t("binance", "AAAUSDT", false),
                t("binance", "ZZZUSDT", true),
                t("binance", "ZZZUSDT", false),
                t("binance", "BBBEUR", false),
            ],
            &q,
            "",
        );
        let st = s.st.lock().unwrap();
        assert_eq!(st.listings.len(), 1, "новая пара в EUR — не листинг");
        assert_eq!(
            (
                st.listings[0]["symbol"].clone(),
                st.listings[0]["perp"].clone(),
                st.listings[0]["alsoOn"].clone()
            ),
            (json!("ZZZUSDT"), json!(false), json!(["binance (перп)"]))
        );
        drop(st);
        let a = |id: &str| Announcement {
            exchange: "okx".into(),
            id: id.into(),
            title: "OKX to list ZZZ (ZZZ)".into(),
            url: String::new(),
            published_at: 0,
            starts_at: 0,
            coins: vec!["ZZZ".into()],
            source: "src".into(),
        };
        s.on_announcements("okx", vec![a("1")], false, "");
        s.on_announcements("okx", vec![a("1"), a("2")], false, "");
        let st = s.st.lock().unwrap();
        assert_eq!(st.announces.len(), 1);
        assert_eq!(st.announces[0]["alsoOn"], json!(["binance (спот, перп)"]));
        drop(st);
        assert_eq!(exchanges("*").len(), SUPPORTED.len());
        assert_eq!(exchanges("upbit, nope,okx"), vec!["upbit", "okx"]);
        // вебхук: одинаковый алерт не чаще cooldown, пустой адрес — не отправляется
        assert!(!alert("", "k", "x".into(), 1000));
    }

    /// Опрос списка пар биржи-заглушки и официальных анонсов: база, затем новая пара и новый анонс — события.
    #[tokio::test]
    async fn poll_against_mock() {
        use axum::{Json, Router, routing::get};
        let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let n2 = n.clone();
        let app = Router::new()
            .route("/api/v3/ticker/24hr", get(move || {
                let k = n2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    let mut v = vec![json!({"symbol": "AAAUSDT", "lastPrice": "1", "quoteVolume": "1"})];
                    if k > 0 {
                        v.push(json!({"symbol": "NEWUSDT", "lastPrice": "0.5", "quoteVolume": "1"}));
                    }
                    Json(Value::Array(v))
                }
            }))
            .route("/api/v5/support/announcements", get(|| async {
                Json(json!({"data": [{"details": [{"title": "OKX to list NEW (NEW)", "url": "https://o/1", "pTime": "1700000000000"}]}]}))
            }));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        let mut src = Sources::default();
        src.urls = HashMap::from([
            ("binance".into(), base.clone()),
            ("okx".into(), base.clone()),
        ]);
        src.perp_urls = HashMap::new();
        let s = Signals::with_sources(src);
        let g: Map<String, Value> =
            serde_json::from_value(json!({"listingExchanges": "binance", "listingQuotes": "USDT"}))
                .unwrap();
        s.poll_listings(&g).await;
        s.poll_listings(&g).await;
        let v = s.json(&g);
        assert_eq!(v["listings"][0]["symbol"], "NEWUSDT", "{v}");
        assert_eq!(v["knownPairs"]["binance"], 2);
        assert!(
            v["listings"][0]["source"]
                .as_str()
                .unwrap()
                .contains("/api/v3/ticker/24hr")
        );
        // анонсы OKX: первый раз — база, новых нет
        let g2: Map<String, Value> =
            serde_json::from_value(json!({"listingExchanges": "okx"})).unwrap();
        s.poll_announcements(&g2).await;
        assert_eq!(s.json(&g2)["announcements"], json!([]));
        assert!(s.json(&g2)["announceCheckedAt"].as_u64().unwrap() > 0);
    }

    /// Вебхук получает {content}; проверочный алерт и счётчики.
    #[tokio::test]
    async fn discord_webhook() {
        use axum::{Json, Router, routing::post as p};
        let got = Arc::new(Mutex::new(Vec::<Value>::new()));
        let g = got.clone();
        let app = Router::new().route(
            "/hook",
            p(move |Json(b): Json<Value>| async move {
                g.lock().unwrap().push(b);
                axum::http::StatusCode::NO_CONTENT
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/hook", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        send_now(&url, "проверка").await.unwrap();
        assert!(alert(&url, "same-key", "x".repeat(2100), 60_000));
        assert!(
            !alert(&url, "same-key", "y".into(), 60_000),
            "повтор раньше cooldown"
        );
        for _ in 0..50 {
            if got.lock().unwrap().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let g = got.lock().unwrap();
        assert_eq!(g[0]["content"], "проверка");
        assert_eq!(g[1]["content"].as_str().unwrap().chars().count(), 1998);
        assert!(discord_stats(&url)["sent"].as_u64().unwrap() >= 2);
    }
}
