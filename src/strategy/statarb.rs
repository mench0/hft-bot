//! Статистический арбитраж (торговля парами) внутри одной биржи, как StatArbStrategy в Java.
//!
//! Для пары A/B раз в `statArbSampleMs` берутся середины стаканов. На окне из `statArbWindow` отсчётов:
//! β = cov(ln A, ln B) / var(ln B), спред s = ln A − β·ln B, z = (s − среднее) / σ. В торговлю идут только пары
//! с корреляцией доходностей не ниже `statArbMinCorrelation`.
//!
//! - Вход при |z| ≥ `statArbEntryZ` (и ниже стопа), если ожидаемый возврат спреда (|z| − exit)·σ окупает комиссии:
//!   z < 0 — A дёшев, покупаем A; z > 0 — покупаем B. На перпах дорогая нога шортится на сумму × |β|
//!   (позиция рыночно-нейтральная); не встала одна нога — вторая закрывается.
//! - Выход: |z| вернулся к `statArbExitZ`, разошёлся до `statArbStopZ` (стоп) или истёк `statArbMaxHoldMs`.
//! - Выключили стратегию или торговлю, убрали монету — позиции пары закрываются рыночными ордерами.
//!
//! Ноги и результат считаются по исполнениям своих ордеров, поэтому одинаково работают бумажный и живой счёт
//! (у живого ордер исполняется позже, в фоне).

use crate::market::book::Book;
use crate::trading::account::Account;
use crate::trading::paper::{Amount, Exec, Limits, Side, split};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Имя стратегии (как OWNER в Java и id профиля подбора).
pub const ID: &str = "stat-arb";

/// Параметры стратегии из параметров биржи.
#[derive(Clone, Debug)]
pub struct Params {
    pub enabled: bool,
    pub pairs: String,
    pub max_auto_pairs: usize,
    pub window: usize,
    pub sample: Duration,
    pub entry_z: f64,
    pub exit_z: f64,
    pub stop_z: f64,
    pub min_correlation: f64,
    pub order_quote: f64,
    pub max_hold: Duration,
    pub max_data_age_ms: u64,
}

impl Params {
    pub fn from(p: &Map<String, Value>) -> Params {
        let f = |k: &str, d: f64| {
            p.get(k)
                .and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                })
                .unwrap_or(d)
        };
        Params {
            enabled: p
                .get("statArbEnabled")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            pairs: p
                .get("statArbPairs")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
            max_auto_pairs: f("statArbMaxAutoPairs", 15.0) as usize,
            window: (f("statArbWindow", 300.0) as usize).max(3),
            sample: Duration::from_millis(f("statArbSampleMs", 1000.0).max(10.0) as u64),
            entry_z: f("statArbEntryZ", 2.0),
            exit_z: f("statArbExitZ", 0.5),
            stop_z: f("statArbStopZ", 4.0),
            min_correlation: f("statArbMinCorrelation", 0.6),
            order_quote: f("statArbOrderQuote", 20.0),
            max_hold: Duration::from_millis(f("statArbMaxHoldMs", 3_600_000.0) as u64),
            max_data_age_ms: f("maxDataAgeMs", 5000.0) as u64,
        }
    }
}

/// Пары из параметра "A/B,C/D" или все пары символов с одной котируемой валютой (по алфавиту, не больше max).
pub fn pair_list(spec: &str, symbols: &[String], max: usize) -> Vec<(String, String)> {
    if !spec.trim().is_empty() {
        return spec
            .split(',')
            .filter_map(|p| {
                let (a, b) = p.trim().split_once('/')?;
                let (a, b) = (a.trim().to_uppercase(), b.trim().to_uppercase());
                (symbols.contains(&a) && symbols.contains(&b) && a != b).then_some((a, b))
            })
            .collect();
    }
    let mut syms = symbols.to_vec();
    syms.sort();
    let quote = |s: &str| split(s).map(|x| x.1).unwrap_or_else(|| format!("?{s}"));
    let mut out = Vec::new();
    for i in 0..syms.len() {
        for j in i + 1..syms.len() {
            if out.len() < max && quote(&syms[i]) == quote(&syms[j]) {
                out.push((syms[i].clone(), syms[j].clone()));
            }
        }
    }
    out
}

/// Окно отсчётов пары.
#[derive(Debug, Clone)]
struct Window {
    a: String,
    b: String,
    la: Vec<f64>,
    lb: Vec<f64>,
    n: usize,
    head: usize,
    z: f64,
    beta: f64,
    corr: f64,
    sd: f64,
}

impl Window {
    fn new(a: String, b: String, w: usize) -> Window {
        Window {
            a,
            b,
            la: vec![0.0; w],
            lb: vec![0.0; w],
            n: 0,
            head: 0,
            z: f64::NAN,
            beta: f64::NAN,
            corr: f64::NAN,
            sd: f64::NAN,
        }
    }

    fn name(&self) -> String {
        format!("{}/{}", self.a, self.b)
    }

    fn add(&mut self, pa: f64, pb: f64) {
        let w = self.la.len();
        self.la[self.head] = pa.ln();
        self.lb[self.head] = pb.ln();
        self.head = (self.head + 1) % w;
        self.n = (self.n + 1).min(w);
    }

    /// Пересчитать β, z и корреляцию доходностей; false — окно не заполнено или σ = 0.
    fn compute(&mut self) -> bool {
        let w = self.la.len();
        if self.n < w {
            return false;
        }
        let (ma, mb) = (
            self.la.iter().sum::<f64>() / w as f64,
            self.lb.iter().sum::<f64>() / w as f64,
        );
        let (mut cov, mut vb) = (0.0, 0.0);
        for i in 0..w {
            cov += (self.la[i] - ma) * (self.lb[i] - mb);
            vb += (self.lb[i] - mb).powi(2);
        }
        if vb <= 0.0 {
            return false;
        }
        let bt = cov / vb;
        let ms = (0..w).map(|i| self.la[i] - bt * self.lb[i]).sum::<f64>() / w as f64;
        let vs: f64 = (0..w)
            .map(|i| (self.la[i] - bt * self.lb[i] - ms).powi(2))
            .sum();
        let sd = (vs / (w - 1) as f64).sqrt();
        if sd <= 0.0 {
            return false;
        }
        let last = (self.head + w - 1) % w;
        // корреляция доходностей (а не уровней: у уровней корреляция бывает высокой и без связи)
        let (mut sra, mut srb, mut saa, mut sbb, mut sab) = (0.0, 0.0, 0.0, 0.0, 0.0);
        for k in 1..w {
            let (i, j) = ((self.head + k) % w, (self.head + k - 1) % w);
            let (ra, rb) = (self.la[i] - self.la[j], self.lb[i] - self.lb[j]);
            sra += ra;
            srb += rb;
            saa += ra * ra;
            sbb += rb * rb;
            sab += ra * rb;
        }
        let m = (w - 1) as f64;
        let (cva, cvb, cab) = (
            saa - sra * sra / m,
            sbb - srb * srb / m,
            sab - sra * srb / m,
        );
        self.corr = if cva > 0.0 && cvb > 0.0 {
            cab / (cva * cvb).sqrt()
        } else {
            0.0
        };
        self.sd = sd;
        self.beta = bt;
        self.z = (self.la[last] - bt * self.lb[last] - ms) / sd;
        true
    }
}

/// Позиция пары: ноги по исполнениям своих ордеров.
#[derive(Debug)]
struct Position {
    /// Купленная нога; на перпах — шорт второй (hedge).
    long: String,
    hedge: Option<String>,
    long_a: bool,
    /// Количество по символам со знаком (лонг > 0, шорт < 0).
    qty: HashMap<String, f64>,
    /// Денежный поток: продажи − покупки − комиссии; при закрытых ногах — результат.
    cash: f64,
    pending: Vec<u64>,
    opened: Instant,
    filled: bool,
    closing: bool,
}

impl Position {
    fn q(&self, s: &str) -> f64 {
        self.qty.get(s).copied().unwrap_or(0.0)
    }
    fn flat(&self) -> bool {
        self.qty.values().all(|q| q.abs() < 1e-12)
    }
    fn label(&self) -> String {
        match &self.hedge {
            Some(h) => format!("{} лонг / {h} шорт", self.long),
            None => self.long.clone(),
        }
    }
}

/// Стратегия одной биржи.
#[derive(Debug, Default)]
pub struct StatArb {
    windows: Vec<Window>,
    built: Option<(String, usize, Vec<String>)>,
    positions: HashMap<String, Position>,
    /// Свои ордера: номер → пара.
    own: HashMap<u64, String>,
    seen: u64,
    last_sample: Option<Instant>,
    pub trades: u64,
    pub stops: u64,
    pub total_pnl: f64,
}

impl StatArb {
    /// Пары и текущий z-score (без значения — пропускаются).
    pub fn zscores(&self) -> Vec<(String, f64)> {
        self.windows
            .iter()
            .filter(|w| w.z.is_finite())
            .map(|w| (w.name(), w.z))
            .collect()
    }

    pub fn position_count(&self) -> usize {
        self.positions.len()
    }

    /// Символы, занятые позициями пар (другим стратегиям их не трогать).
    pub fn held(&self) -> HashSet<String> {
        self.positions
            .values()
            .flat_map(|p| std::iter::once(p.long.clone()).chain(p.hedge.clone()))
            .collect()
    }

    #[cfg(test)]
    pub fn active(&self) -> bool {
        !self.positions.is_empty()
    }

    /// Учесть исполнения своих ордеров.
    fn absorb(&mut self, execs: &VecDeque<Exec>) {
        let from = self.seen;
        for e in execs.iter().filter(|e| e.seq > from) {
            self.seen = e.seq;
            let Some(pos) = self
                .own
                .get(&e.order_id)
                .and_then(|n| self.positions.get_mut(n))
            else {
                continue;
            };
            let sign = if e.side == Side::Buy { 1.0 } else { -1.0 };
            *pos.qty.entry(e.symbol.clone()).or_default() += sign * e.qty;
            pos.cash += -sign * e.qty * e.price - e.fee;
            pos.filled = true;
        }
    }

    fn rebuild(&mut self, p: &Params, symbols: &[String]) {
        let key = (p.pairs.clone(), p.window, symbols.to_vec());
        if self.built.as_ref() == Some(&key) {
            return;
        }
        // те же пары с тем же окном — с накопленной историей
        let mut old: HashMap<String, Window> = self
            .windows
            .drain(..)
            .filter(|w| w.la.len() == p.window)
            .map(|w| (w.name(), w))
            .collect();
        self.windows = pair_list(&p.pairs, symbols, p.max_auto_pairs)
            .into_iter()
            .map(|(a, b)| {
                old.remove(&format!("{a}/{b}"))
                    .unwrap_or_else(|| Window::new(a, b, p.window))
            })
            .collect();
        tracing::info!(
            "[{ID}] пары: {:?} (окно {} × {} мс)",
            self.windows.iter().map(Window::name).collect::<Vec<_>>(),
            p.window,
            p.sample.as_millis()
        );
        self.built = Some(key);
    }

    /// Шаг стратегии: отсчёт (раз в statArbSampleMs), входы и выходы. symbols — монеты, которые ей можно торговать;
    /// busy — монеты, занятые другими стратегиями.
    #[allow(clippy::too_many_arguments)]
    pub fn step(
        &mut self,
        books: &HashMap<String, Book>,
        acc: &mut Account,
        p: &Params,
        lim: &Limits,
        symbols: &[String],
        busy: &HashSet<String>,
        now: Instant,
    ) {
        self.absorb(acc.execs());
        for pos in self.positions.values_mut() {
            pos.pending.retain(|id| acc.is_open(*id));
        }
        self.rebuild(p, symbols);
        let on = p.enabled && lim.trading_enabled;
        // выключили или монету убрали — закрыть
        let names: Vec<String> = self.positions.keys().cloned().collect();
        for n in &names {
            let gone = !self.windows.iter().any(|w| &w.name() == n);
            if !on || gone {
                let why = if on {
                    "монета убрана из пар"
                } else {
                    "стратегия или торговля выключена"
                };
                self.close(n, books, acc, why);
            }
        }
        self.finish(acc);
        if !on
            || self
                .last_sample
                .is_some_and(|t| now.duration_since(t) < p.sample)
        {
            return;
        }
        self.last_sample = Some(now);
        let max_age = p.max_data_age_ms.max(2 * p.sample.as_millis() as u64);
        let mid = |s: &str| {
            books
                .get(s)
                .filter(|b| b.is_ready() && b.age_ms().is_some_and(|a| a <= max_age))
                .and_then(Book::mid)
        };
        for i in 0..self.windows.len() {
            let w = &mut self.windows[i];
            let (Some(ma), Some(mb)) = (mid(&w.a), mid(&w.b)) else {
                continue; // нет свежих данных — отсчёт пропускаем
            };
            w.add(ma, mb);
            if w.compute() {
                let w = w.clone();
                self.decide(&w, books, acc, p, lim, busy, now);
            }
        }
        self.finish(acc);
    }

    /// Пары без ног и без ордеров — результат в учёт, позиция забыта.
    fn finish(&mut self, acc: &Account) {
        let done: Vec<String> = self
            .positions
            .iter()
            .filter(|(_, p)| p.pending.is_empty() && p.flat() && (p.closing || !p.filled))
            .map(|(n, _)| n.clone())
            .collect();
        for n in done {
            let pos = self.positions.remove(&n).unwrap();
            self.own.retain(|_, v| *v != n);
            if pos.filled {
                self.trades += 1;
                self.total_pnl += pos.cash;
                tracing::info!("[{ID}] {n} закрыта, результат {:.4}", pos.cash);
            }
        }
        let _ = acc;
    }

    #[allow(clippy::too_many_arguments)]
    fn decide(
        &mut self,
        w: &Window,
        books: &HashMap<String, Book>,
        acc: &mut Account,
        p: &Params,
        lim: &Limits,
        busy: &HashSet<String>,
        now: Instant,
    ) {
        let name = w.name();
        let z = w.z;
        if let Some(pos) = self.positions.get(&name) {
            if !pos.pending.is_empty() || pos.closing {
                return;
            }
            // перп: одна нога не встала — без хеджа не держим
            let broken = pos
                .hedge
                .as_ref()
                .is_some_and(|h| (pos.q(&pos.long).abs() < 1e-12) != (pos.q(h).abs() < 1e-12));
            let reverted = if pos.long_a {
                z >= -p.exit_z
            } else {
                z <= p.exit_z
            };
            let stop = if pos.long_a {
                z <= -p.stop_z
            } else {
                z >= p.stop_z
            };
            let timeout = now.duration_since(pos.opened) > p.max_hold;
            let why = if broken {
                "вторая нога не исполнилась"
            } else if stop {
                "стоп (спред разошёлся)"
            } else if reverted {
                "спред вернулся"
            } else if timeout {
                "таймаут"
            } else {
                return;
            };
            if stop && !broken {
                self.stops += 1;
            }
            tracing::info!("[{ID}] выход {name} ({why}): z={z:.2}");
            self.close(&name, books, acc, why);
            return;
        }
        if w.corr < p.min_correlation || z.abs() < p.entry_z || z.abs() >= p.stop_z {
            return;
        }
        // ожидаемый возврат спреда должен окупать комиссии всех сделок: спот — 2, перпы — 4
        let perp = acc.is_perp();
        let expected = (z.abs() - p.exit_z) * w.sd * 100.0;
        let cost = 2.0 * acc.taker_fee() * 100.0 * if perp { 2.0 } else { 1.0 };
        if expected <= cost {
            return;
        }
        let long_a = z < 0.0;
        let (sym, other) = if long_a { (&w.a, &w.b) } else { (&w.b, &w.a) };
        let hedge = perp.then(|| other.clone());
        let held = self.held();
        let free = |s: &String| !busy.contains(s) && !held.contains(s);
        if !free(sym) || hedge.as_ref().is_some_and(|h| !free(h)) {
            return;
        }
        let Some(ask) = books.get(sym).and_then(|b| b.best_ask()).map(|x| x.0) else {
            return;
        };
        let hedge_bid = hedge
            .as_ref()
            .and_then(|h| books.get(h)?.best_bid())
            .map(|x| x.0);
        if hedge.is_some() && hedge_bid.is_none() {
            return;
        }
        let beta = w.beta.abs().clamp(0.2, 5.0); // защита от вырожденного β
        tracing::info!(
            "[{ID}] вход {name}: z={z:.2} β={:.2} корр={:.2} — покупаю {sym}{}",
            w.beta,
            w.corr,
            hedge
                .as_ref()
                .map(|h| format!(", шорт {h}"))
                .unwrap_or_default()
        );
        let mut pos = Position {
            long: sym.clone(),
            hedge: hedge.clone(),
            long_a,
            qty: HashMap::new(),
            cash: 0.0,
            pending: Vec::new(),
            opened: now,
            filled: false,
            closing: false,
        };
        match acc.order(
            sym,
            Side::Buy,
            Amount::Qty(p.order_quote / ask),
            None,
            books.get(sym),
            lim,
        ) {
            Ok(f) => pos.pending.push(f.order_id),
            Err(e) => {
                tracing::warn!("[{ID}] {name}: покупка {sym} отклонена: {e}");
                return;
            }
        }
        if let (Some(h), Some(bid)) = (&hedge, hedge_bid) {
            // спред ln A − β·ln B: на 1 у.е. в A — β у.е. в B. Лонг A — шорт B на β×сумма; лонг B — шорт A на сумма/β
            let hedge_quote = if long_a {
                p.order_quote * beta
            } else {
                p.order_quote / beta
            };
            match acc.order(
                h,
                Side::Sell,
                Amount::Qty(hedge_quote / bid),
                None,
                books.get(h),
                lim,
            ) {
                Ok(f) => pos.pending.push(f.order_id),
                // лонг закроется на следующем шаге как «вторая нога не исполнилась»
                Err(e) => tracing::warn!("[{ID}] {name}: шорт {h} отклонён: {e}"),
            }
        }
        for id in &pos.pending {
            self.own.insert(*id, name.clone());
        }
        self.positions.insert(name, pos);
        self.absorb(acc.execs());
    }

    /// Закрыть ноги пары рыночными выходами (без проверок торговли).
    fn close(&mut self, name: &str, books: &HashMap<String, Book>, acc: &mut Account, why: &str) {
        let Some(pos) = self.positions.get_mut(name) else {
            return;
        };
        if !pos.pending.is_empty() {
            return;
        }
        pos.closing = true;
        let legs: Vec<(String, f64)> = pos
            .qty
            .iter()
            .filter(|(_, q)| q.abs() > 1e-12)
            .map(|(s, q)| (s.clone(), *q))
            .collect();
        for (s, q) in legs {
            let side = if q > 0.0 { Side::Sell } else { Side::Buy };
            match acc.exit_market(&s, side, q.abs(), books.get(&s)) {
                Ok(f) => {
                    pos.pending.push(f.order_id);
                    self.own.insert(f.order_id, name.to_owned());
                }
                Err(e) => {
                    tracing::warn!("[{ID}] {name}: выход {s} ({why}) не удался: {e}");
                    // закрывать нечего (позиция закрыта мимо стратегии) — нога забыта
                    if e.starts_with("нечего закрывать") {
                        pos.qty.insert(s, 0.0);
                    }
                }
            }
        }
        self.absorb(acc.execs());
    }

    /// Состояние для /strategies.
    pub fn stats(&self, p: &Params, symbols: &[String]) -> Value {
        let pairs: Vec<Value> = if self.built.is_none() {
            pair_list(&p.pairs, symbols, p.max_auto_pairs)
                .into_iter()
                .map(|(a, b)| json!({"pair": format!("{a}/{b}"), "samples": 0}))
                .collect()
        } else {
            let num = |v: f64| if v.is_nan() { Value::Null } else { json!(v) };
            self.windows
                .iter()
                .map(|w| {
                    json!({"pair": w.name(), "samples": w.n, "z": num(w.z), "beta": num(w.beta), "correlation": num(w.corr),
                        "position": self.positions.get(&w.name()).map(Position::label)})
                })
                .collect()
        };
        json!({"enabled": p.enabled, "pairs": pairs, "trades": self.trades, "stops": self.stops, "totalPnl": self.total_pnl})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trading::paper::Paper;

    fn book(mid: f64) -> Book {
        let mut b = Book::default();
        b.apply_snapshot(vec![(mid - 0.01, 1000.0)], vec![(mid + 0.01, 1000.0)], 1);
        b
    }

    fn lim() -> Limits {
        Limits {
            trading_enabled: true,
            max_order_quote: 1000.0,
            max_daily_loss_quote: 1000.0,
            max_slippage_percent: 5.0,
            max_orders_per_minute: 1000,
            max_data_age_ms: 60_000,
        }
    }

    fn params() -> Params {
        let mut m = Map::new();
        m.insert("statArbWindow".into(), json!(20));
        m.insert("statArbSampleMs".into(), json!(10));
        m.insert("statArbMinCorrelation".into(), json!(0.0));
        Params::from(&m)
    }

    #[test]
    fn pairs_auto_and_explicit() {
        let s: Vec<String> = ["ETHUSDT", "BTCUSDT", "BTCETH", "SOLUSDT"]
            .iter()
            .map(|x| x.to_string())
            .collect();
        assert_eq!(
            pair_list("", &s, 15),
            vec![
                ("BTCUSDT".into(), "ETHUSDT".into()),
                ("BTCUSDT".into(), "SOLUSDT".into()),
                ("ETHUSDT".into(), "SOLUSDT".into())
            ]
        );
        assert_eq!(pair_list("eth/btcusdt, X/Y", &s, 15), vec![]);
        assert_eq!(
            pair_list("ETHUSDT/BTCUSDT", &s, 1),
            vec![("ETHUSDT".into(), "BTCUSDT".into())]
        );
    }

    #[test]
    fn window_math() {
        // B не двигается — β не определить
        let mut w = Window::new("A".into(), "B".into(), 4);
        for a in [100.0, 101.0, 99.0, 100.0] {
            w.add(a, 50.0);
        }
        assert!(!w.compute());
        let mut w = Window::new("A".into(), "B".into(), 5);
        for (a, b) in [
            (100.0, 50.0),
            (101.0, 50.4),
            (99.0, 49.6),
            (100.5, 50.1),
            (97.0, 50.0),
        ] {
            w.add(a, b);
        }
        assert!(w.compute());
        assert!(w.z < -1.0, "A резко подешевел относительно B: z={}", w.z);
        assert!(w.corr > 0.0);
    }

    /// Прогон: пара ходит вместе, затем A проседает — вход (на перпах ещё шорт B), A возвращается — выход с прибылью.
    fn run(perp: bool) -> (StatArb, Account) {
        let mut paper = Paper::new(&["AAAUSDT".into(), "BBBUSDT".into()], 1000.0, 0.0, 0.0);
        if perp {
            paper.set_perp(5.0);
        }
        let mut acc = Account::Paper(paper);
        let mut sa = StatArb::default();
        let p = Params {
            order_quote: 100.0,
            ..params()
        };
        let syms = vec!["AAAUSDT".to_string(), "BBBUSDT".to_string()];
        let t0 = Instant::now();
        let mut prices: Vec<(f64, f64)> = (0..30)
            .map(|i| {
                let x = (i as f64 * 0.7).sin();
                (100.0 + x, 50.0 + x / 2.0 + (i as f64 * 1.3).cos() * 0.05)
            })
            .collect();
        prices.extend([(96.0, 50.0), (96.0, 50.0)]); // A проседает — вход
        prices.extend((0..3).map(|_| (100.0, 50.0))); // вернулся — выход
        for (i, (a, b)) in prices.into_iter().enumerate() {
            let books = HashMap::from([
                ("AAAUSDT".to_string(), book(a)),
                ("BBBUSDT".to_string(), book(b)),
            ]);
            sa.step(
                &books,
                &mut acc,
                &p,
                &lim(),
                &syms,
                &HashSet::new(),
                t0 + Duration::from_millis(20 * i as u64),
            );
        }
        (sa, acc)
    }

    #[test]
    fn spot_buys_cheap_leg_and_exits_on_reversion() {
        let (sa, acc) = run(false);
        assert_eq!(sa.trades, 1, "{:?}", sa.stats(&params(), &[]));
        assert!(sa.total_pnl > 0.0, "{}", sa.total_pnl);
        assert!(!sa.active());
        assert!(!acc.balances().contains_key("AAAUSDT"));
    }

    /// z > 0 (A дорогая): лонг B на сумму, шорт A на сумму/β (β ≈ 2 — A движется вдвое сильнее B в логарифмах).
    #[test]
    fn perp_short_a_hedge_is_quote_over_beta() {
        let mut paper = Paper::new(&["AAAUSDT".into(), "BBBUSDT".into()], 10_000.0, 0.0, 0.0);
        paper.set_perp(5.0);
        let mut acc = Account::Paper(paper);
        let mut sa = StatArb::default();
        let p = Params {
            order_quote: 100.0,
            ..params()
        };
        let syms = vec!["AAAUSDT".to_string(), "BBBUSDT".to_string()];
        let t0 = Instant::now();
        let mut prices: Vec<(f64, f64)> = (0..30)
            .map(|i| {
                let x = (i as f64 * 0.7).sin();
                (100.0 + x, 50.0 + x / 4.0 + (i as f64 * 1.3).cos() * 0.005)
            })
            .collect();
        prices.push((100.1, 50.0)); // A подорожала — вход: лонг B, шорт A
        for (i, (a, b)) in prices.into_iter().enumerate() {
            let books = HashMap::from([
                ("AAAUSDT".to_string(), book(a)),
                ("BBBUSDT".to_string(), book(b)),
            ]);
            sa.step(
                &books,
                &mut acc,
                &p,
                &lim(),
                &syms,
                &HashSet::new(),
                t0 + Duration::from_millis(20 * i as u64),
            );
        }
        let Account::Paper(pp) = &acc else {
            unreachable!()
        };
        let buy_b: f64 = pp
            .execs
            .iter()
            .filter(|e| e.symbol == "BBBUSDT" && e.side == Side::Buy)
            .map(|e| e.qty * e.price)
            .sum();
        let short_a: f64 = pp
            .execs
            .iter()
            .filter(|e| e.symbol == "AAAUSDT" && e.side == Side::Sell)
            .map(|e| e.qty * e.price)
            .sum();
        let beta = sa.windows[0].beta.abs();
        assert!(beta > 1.5 && beta < 2.5, "β = {beta}");
        assert!(
            (buy_b - 100.0).abs() < 2.0,
            "лонг B на сумму: {buy_b}, z = {}, корр = {}",
            sa.windows[0].z,
            sa.windows[0].corr
        );
        assert!(
            (short_a - 100.0 / beta).abs() < 5.0,
            "шорт A на сумму/β = {}: {short_a}",
            100.0 / beta
        );
    }

    #[test]
    fn perp_hedges_with_short_and_closes_both_legs() {
        let (sa, acc) = run(true);
        assert_eq!(sa.trades, 1);
        let Account::Paper(p) = &acc else {
            unreachable!()
        };
        assert!(
            p.positions.is_empty(),
            "обе ноги закрыты: {:?}",
            p.positions
        );
        assert!(
            p.execs
                .iter()
                .any(|e| e.symbol == "BBBUSDT" && e.side == Side::Sell),
            "шорт B был"
        );
    }
}
