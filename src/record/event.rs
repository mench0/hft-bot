//! События записи: числа и короткие имена без выделения памяти (кроме уровней стакана) — горячий путь только копирует.

/// Короткое имя (биржа, символ) без выделения памяти: до 31 байта, длиннее — обрезается.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Name {
    len: u8,
    buf: [u8; 31],
}

impl Name {
    pub fn new(s: &str) -> Name {
        let mut n = s.len().min(31);
        while !s.is_char_boundary(n) {
            n -= 1;
        }
        let mut buf = [0; 31];
        buf[..n].copy_from_slice(&s.as_bytes()[..n]);
        Name { len: n as u8, buf }
    }
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.buf[..self.len as usize]).unwrap_or("")
    }
}

impl std::fmt::Debug for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.as_str())
    }
}

/// Сторона ордера: 1 — покупка, 2 — продажа.
pub const BUY: u8 = 1;
pub const SELL: u8 = 2;

/// Итог отправки ордера.
pub mod status {
    /// Принят биржей и стоит (лимитный).
    pub const NEW: u8 = 1;
    pub const PARTIAL: u8 = 2;
    pub const FILLED: u8 = 3;
    pub const REJECTED: u8 = 4;
    /// Отправлен, ответа ещё нет (живой счёт).
    pub const SENT: u8 = 5;
}

/// Вид ордера.
pub mod kind {
    pub const LIMIT: u8 = 1;
    pub const MARKET: u8 = 2;
    /// Рыночный выход (продажа спота, уменьшение позиции перпа).
    pub const EXIT: u8 = 3;
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// Лучшие цены после обновления стакана.
    Bbo {
        ex: Name,
        sym: Name,
        bid: f64,
        bid_qty: f64,
        ask: f64,
        ask_qty: f64,
    },
    /// Снимок N лучших уровней (recordDepthLevels > 0, не чаще recordDepthMs по символу).
    Depth {
        ex: Name,
        sym: Name,
        bids: Vec<(f64, f64)>,
        asks: Vec<(f64, f64)>,
    },
    /// Свой ордер: объём NaN — «доля» или «всё», цена NaN — рыночный.
    Order {
        ex: Name,
        sym: Name,
        id: u64,
        side: u8,
        kind: u8,
        qty: f64,
        price: f64,
        status: u8,
        executed: f64,
        avg: f64,
    },
    /// Своё исполнение (комиссия — в котируемой валюте).
    Fill {
        ex: Name,
        sym: Name,
        id: u64,
        side: u8,
        qty: f64,
        price: f64,
        fee: f64,
    },
    /// Снятие заявки (id 0 — все заявки символа или счёта, n — сколько снято).
    Cancel {
        ex: Name,
        sym: Name,
        id: u64,
        n: u32,
    },
    /// Ошибка или обрыв источника данных биржи.
    FeedError { ex: Name },
}

/// Событие с временем получения (наносекунды эпохи UTC).
#[derive(Debug, Clone, PartialEq)]
pub struct Rec {
    pub ts: u64,
    pub ev: Event,
}

pub fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
