//! Лента по символу для графиков: лучшие цены (при изменении), сделки рынка и свои исполнения за последние минуты
//! (вкладка «Рынок» в админке, GET /market/tape) и накопленные счётчики сделок для /metrics (дашборд Grafana «Рынок»).
//! Время — локальное время получения, мс. Хранится не дольше KEEP_MS и не больше CAP точек каждого вида.

use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

/// Сколько хранить, мс.
pub const KEEP_MS: u64 = 10 * 60_000;
/// Точек каждого вида на символ, не больше.
const CAP: usize = 20_000;

#[derive(Default)]
struct Tape {
    /// (время, бид, аск)
    bbo: VecDeque<(u64, f64, f64)>,
    /// (время, цена, объём, агрессор покупатель)
    trades: VecDeque<(u64, f64, f64, bool)>,
    /// (время, цена, объём, покупка)
    fills: VecDeque<(u64, f64, f64, bool)>,
    /// Накопленные с запуска: сделок и оборот в котируемой валюте — покупки и продажи; своих исполнений.
    buys: u64,
    sells: u64,
    buy_quote: f64,
    sell_quote: f64,
    own_buys: u64,
    own_sells: u64,
}

fn push<T>(q: &mut VecDeque<T>, v: T, at: impl Fn(&T) -> u64, now: u64) {
    q.push_back(v);
    while q.len() > CAP
        || q.front()
            .is_some_and(|x| now.saturating_sub(at(x)) > KEEP_MS)
    {
        q.pop_front();
    }
}

type Key = (String, String);

static TAPES: OnceLock<Mutex<HashMap<Key, Tape>>> = OnceLock::new();

fn with(ex: &str, sym: &str, f: impl FnOnce(&mut Tape)) {
    // бэктест не попадает в живую ленту
    if crate::infra::clock::simulated() {
        return;
    }
    let mut m = TAPES.get_or_init(Default::default).lock().unwrap();
    let t = match m.get_mut(&(ex.to_owned(), sym.to_owned())) {
        Some(t) => t,
        None => m.entry((ex.to_owned(), sym.to_owned())).or_default(),
    };
    f(t)
}

/// Лучшие цены изменились.
pub fn bbo(ex: &str, sym: &str, bid: f64, ask: f64) {
    let now = crate::infra::util::now_ms();
    with(ex, sym, |t| {
        if t.bbo.back().is_some_and(|x| x.1 == bid && x.2 == ask) {
            return;
        }
        push(&mut t.bbo, (now, bid, ask), |x| x.0, now);
    });
}

/// Сделка рынка.
pub fn trade(ex: &str, sym: &str, px: f64, qty: f64, buy: bool) {
    let now = crate::infra::util::now_ms();
    with(ex, sym, |t| {
        if buy {
            t.buys += 1;
            t.buy_quote += px * qty;
        } else {
            t.sells += 1;
            t.sell_quote += px * qty;
        }
        push(&mut t.trades, (now, px, qty, buy), |x| x.0, now);
    });
}

/// Своё исполнение.
pub fn fill(ex: &str, sym: &str, px: f64, qty: f64, buy: bool) {
    let now = crate::infra::util::now_ms();
    with(ex, sym, |t| {
        if buy {
            t.own_buys += 1
        } else {
            t.own_sells += 1
        }
        push(&mut t.fills, (now, px, qty, buy), |x| x.0, now);
    });
}

/// Лента символа с момента since (мс): {"bbo": [[t, бид, аск]], "trades": [[t, цена, объём, покупка]], "fills": [...]}.
/// Лучшие цены — с последней точкой до since, чтобы линия начиналась с левого края.
pub fn json(ex: &str, sym: &str, since: u64) -> Value {
    let m = TAPES.get_or_init(Default::default).lock().unwrap();
    let Some(t) = m.get(&(ex.to_owned(), sym.to_uppercase())) else {
        return json!({"bbo": [], "trades": [], "fills": [], "keepMs": KEEP_MS});
    };
    let first = t.bbo.partition_point(|x| x.0 < since).saturating_sub(1);
    let bbo: Vec<Value> = t
        .bbo
        .iter()
        .skip(first)
        .map(|x| json!([x.0, x.1, x.2]))
        .collect();
    let rows = |q: &VecDeque<(u64, f64, f64, bool)>| -> Vec<Value> {
        q.iter()
            .filter(|x| x.0 >= since)
            .map(|x| json!([x.0, x.1, x.2, x.3]))
            .collect()
    };
    json!({"bbo": bbo, "trades": rows(&t.trades), "fills": rows(&t.fills), "keepMs": KEEP_MS,
        "now": crate::infra::util::now_ms()})
}

/// Счётчики для /metrics: (биржа, символ, сделок покупок, продаж, оборот покупок, продаж, своих покупок, продаж).
pub fn counters() -> Vec<(String, String, [f64; 6])> {
    let m = TAPES.get_or_init(Default::default).lock().unwrap();
    let mut v: Vec<_> = m
        .iter()
        .map(|((ex, s), t)| {
            (
                ex.clone(),
                s.clone(),
                [
                    t.buys as f64,
                    t.sells as f64,
                    t.buy_quote,
                    t.sell_quote,
                    t.own_buys as f64,
                    t.own_sells as f64,
                ],
            )
        })
        .collect();
    v.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    v
}

#[cfg(test)]
mod tests {
    #[test]
    fn keeps_recent_points_and_counts() {
        super::bbo("tapetest", "XUSDT", 1.0, 2.0);
        super::bbo("tapetest", "XUSDT", 1.0, 2.0); // без изменений — не пишется
        super::bbo("tapetest", "XUSDT", 1.5, 2.0);
        super::trade("tapetest", "XUSDT", 2.0, 3.0, true);
        super::trade("tapetest", "XUSDT", 1.5, 1.0, false);
        super::fill("tapetest", "XUSDT", 1.5, 1.0, true);
        let j = super::json("tapetest", "xusdt", 0);
        assert_eq!(j["bbo"].as_array().unwrap().len(), 2);
        assert_eq!(j["trades"][0][3], true);
        assert_eq!(j["fills"].as_array().unwrap().len(), 1);
        let far = super::json("tapetest", "XUSDT", u64::MAX);
        assert_eq!(far["trades"].as_array().unwrap().len(), 0);
        assert_eq!(
            far["bbo"].as_array().unwrap().len(),
            1,
            "последние лучшие цены до начала окна"
        );
        let c = super::counters()
            .into_iter()
            .find(|x| x.0 == "tapetest")
            .unwrap();
        assert_eq!(c.2, [1.0, 1.0, 6.0, 1.5, 1.0, 0.0]);
    }
}
