//! Рыночные данные всех бирж: стаканы по символам и состояние фидов (подключение, сообщения, ошибки).

use crate::book::Book;
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
}

/// Стаканы и фиды.
#[derive(Default)]
pub struct Market {
    books: Mutex<HashMap<String, HashMap<String, Book>>>,
    feeds: Mutex<BTreeMap<String, FeedState>>,
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
    }

    /// Ответ /market?exchange= в формате Java-бота: символ → лучшие цены, середина, спред, дисбаланс, возраст.
    pub fn market_json(&self, ex: &str) -> Value {
        let syms = self.feed(ex).map(|f| f.symbols).unwrap_or_default();
        let mut root = Map::new();
        for s in syms {
            let node = match self.book(ex, &s).filter(Book::is_ready) {
                Some(b) => json!({
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
