//! Рыночные данные всех бирж: стаканы по символам и состояние фидов (подключение, сообщения, ошибки).

pub mod book;
pub mod feed;
pub mod tape;

use crate::exchange::rest::Filter;
use crate::market::book::Book;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

/// Состояние фида биржи.
#[derive(Debug, Clone, Default)]
pub struct FeedState {
    pub connected: bool,
    pub messages: u64,
    pub reconnects: u64,
    pub last_error: String,
    pub symbols: Vec<String>,
    /// Почему фида нет (биржа без адаптера в Rust); пусто — фид есть.
    pub unsupported: String,
    /// Поток лучших цен (bookTicker) включён; сколько обновлений применено и отброшено как устаревшие.
    pub bbo_enabled: bool,
    pub bbo_updates: u64,
    pub stale: u64,
    /// Сделок рынка получено (поток сделок).
    pub trades: u64,
    /// Ошибка REST (правила пар, свечи); пусто — в порядке.
    pub rest_error: String,
    /// Переподключений из-за тишины, ошибок разбора (сколько всего), перекрещённых стаканов.
    pub silence_reconnects: u64,
    pub parse_errors: u64,
    pub crossed_resets: u64,
    /// Неудачных подключений подряд; источник сдался (feedMaxFailures) — торговля на бирже на паузе, пока он не вернётся.
    pub failures: u32,
    pub gave_up: bool,
    /// С какого момента нет WebSocket стакана (None — подключён). Стакан — только по WebSocket: пока его нет, новые
    /// входы запрещены, выход идёт по последнему стакану.
    pub down_since: Option<std::time::Instant>,
}

/// Оборот последних закрытых минут и когда получен.
type Minutes = (std::time::Instant, Vec<f64>);

/// Стаканы и фиды.
#[derive(Default)]
pub struct Market {
    books: Mutex<HashMap<String, HashMap<String, Book>>>,
    feeds: Mutex<BTreeMap<String, FeedState>>,
    /// Правила торговли пар по биржам.
    filters: Mutex<HashMap<String, HashMap<String, Filter>>>,
    /// Оборот последних закрытых минут по (биржа, символ) и когда получен.
    minutes: Mutex<HashMap<(String, String), Minutes>>,
    /// Funding перпов по (биржа, символ).
    funding: Mutex<HashMap<(String, String), Funding>>,
}

/// Ставка funding: текущая (доля), время следующего списания (мс), цена маркировки.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Funding {
    pub rate: f64,
    pub next_ms: u64,
    pub mark: f64,
}

impl Market {
    /// Изменить стакан символа (создаётся при первом обращении).
    pub fn with_book<R>(&self, ex: &str, sym: &str, f: impl FnOnce(&mut Book) -> R) -> R {
        let mut all = self.books.lock().unwrap();
        f(all
            .entry(ex.to_owned())
            .or_default()
            .entry(sym.to_owned())
            .or_default())
    }

    /// Копия стакана символа.
    pub fn book(&self, ex: &str, sym: &str) -> Option<Book> {
        self.books
            .lock()
            .unwrap()
            .get(ex)
            .and_then(|m| m.get(sym))
            .cloned()
    }

    /// Изменить состояние фида биржи.
    pub fn with_feed<R>(&self, ex: &str, f: impl FnOnce(&mut FeedState) -> R) -> R {
        f(self.feeds.lock().unwrap().entry(ex.to_owned()).or_default())
    }

    /// Состояние фида биржи.
    pub fn feed(&self, ex: &str) -> Option<FeedState> {
        self.feeds.lock().unwrap().get(ex).cloned()
    }

    /// Забыть всё (соединения остановлены).
    pub fn clear(&self) {
        self.books.lock().unwrap().clear();
        self.feeds.lock().unwrap().clear();
        self.filters.lock().unwrap().clear();
        self.minutes.lock().unwrap().clear();
        self.funding.lock().unwrap().clear();
    }

    pub fn set_funding(&self, ex: &str, sym: &str, f: Funding) {
        self.funding
            .lock()
            .unwrap()
            .insert((ex.to_owned(), sym.to_owned()), f);
    }

    pub fn funding(&self, ex: &str, sym: &str) -> Option<Funding> {
        self.funding
            .lock()
            .unwrap()
            .get(&(ex.to_owned(), sym.to_owned()))
            .copied()
    }

    /// Запомнить правила пар биржи.
    pub fn set_filters(&self, ex: &str, f: HashMap<String, Filter>) {
        self.filters
            .lock()
            .unwrap()
            .entry(ex.to_owned())
            .or_default()
            .extend(f);
    }

    /// Правила пар биржи.
    pub fn filters(&self, ex: &str) -> HashMap<String, Filter> {
        self.filters
            .lock()
            .unwrap()
            .get(ex)
            .cloned()
            .unwrap_or_default()
    }

    /// Запомнить оборот последних минут.
    pub fn set_minutes(&self, ex: &str, sym: &str, v: Vec<f64>) {
        self.minutes.lock().unwrap().insert(
            (ex.to_owned(), sym.to_owned()),
            (std::time::Instant::now(), v),
        );
    }

    /// Оборот последних минут, если получен не раньше max_age назад.
    pub fn minutes(&self, ex: &str, sym: &str, max_age: std::time::Duration) -> Option<Vec<f64>> {
        self.minutes
            .lock()
            .unwrap()
            .get(&(ex.to_owned(), sym.to_owned()))
            .filter(|(t, _)| t.elapsed() <= max_age)
            .map(|(_, v)| v.clone())
    }

    /// Ответ /market?exchange= в формате Java-бота: символ → лучшие цены, середина, спред, дисбаланс, возраст.
    pub fn market_json(&self, ex: &str) -> Value {
        let syms = self.feed(ex).map(|f| f.symbols).unwrap_or_default();
        let mut root = Map::new();
        for s in syms {
            let fund = self.funding(ex, &s);
            let node = match self.book(ex, &s).filter(Book::is_ready) {
                Some(b) => json!({
                    "funding_проц": fund.map(|f| f.rate * 100.0),
                    "следующий_funding": fund.map(|f| f.next_ms),
                    "лучший_бид": b.best_bid().map(|x| x.0),
                    "лучший_аск": b.best_ask().map(|x| x.0),
                    "середина": b.mid(),
                    "спред_проц": b.spread_percent().map(|v| (v * 10_000.0).round() / 10_000.0),
                    "дисбаланс": (b.imbalance(5) * 10_000.0).round() / 10_000.0,
                    "возраст_мс": b.age_ms(),
                }),
                None => json!({"состояние": "стакан не готов"}),
            };
            root.insert(s, node);
        }
        Value::Object(root)
    }
}
