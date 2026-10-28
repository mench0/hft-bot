//! Lead-lag: ведущая биржа – ведомая, как LeadLagStrategy в Java.
//!
//! Цену монеты «открывает» ведущая биржа (`leadLagLeader`, обычно Binance), остальные повторяют с задержкой. Бот
//! следит за ведущей и торгует только на ведомой:
//! - базис — среднее отношение середин ведомой и ведущей за `leadLagBasisSec`; справедливая цена ведомой =
//!   середина ведущей × базис;
//! - вход: ведущая за `leadLagWindowMs` сдвинулась не меньше `leadLagMinMovePercent`, а ведомая отстала от справедливой
//!   больше двух комиссий тейкера плюс `leadLagMinProfitPercent`. Рост — покупка по аску ведомой, падение — продажа
//!   (только на перпах);
//! - выход: ведомая догнала (отставание ≤ `leadLagExitGapPercent`), стоп `leadLagStopLossPercent` или таймаут
//!   `leadLagMaxHoldMs`; выключение стратегии или торговли — тоже выход.

use crate::market::book::Book;
use crate::strategy::Venue;
use crate::trading::paper::{Amount, Side, split};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet, VecDeque};

/// Параметры из глобальных настроек.
#[derive(Clone, Debug)]
pub struct Params {
    pub enabled: bool,
    pub leader: String,
    pub check_ms: u64,
    pub window_ms: u64,
    pub min_move: f64,
    pub min_profit: f64,
    pub exit_gap: f64,
    pub stop_loss: f64,
    pub max_hold_ms: u64,
    pub order_quote: f64,
    pub max_positions: usize,
    pub basis_sec: f64,
    pub max_book_age_ms: u64,
    pub cooldown_ms: u64,
    pub symbols: String,
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
                .get("leadLagEnabled")
                .and_then(|v| {
                    v.as_bool()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                })
                .unwrap_or(true),
            leader: p
                .get("leadLagLeader")
                .and_then(Value::as_str)
                .unwrap_or("binance")
                .to_lowercase(),
            check_ms: f("leadLagCheckMs", 50.0) as u64,
            window_ms: f("leadLagWindowMs", 500.0) as u64,
            min_move: f("leadLagMinMovePercent", 0.15),
            min_profit: f("leadLagMinProfitPercent", 0.05),
            exit_gap: f("leadLagExitGapPercent", 0.02),
            stop_loss: f("leadLagStopLossPercent", 0.3),
            max_hold_ms: f("leadLagMaxHoldMs", 5000.0) as u64,
            order_quote: f("leadLagOrderQuote", 50.0),
            max_positions: f("leadLagMaxPositions", 3.0) as usize,
            basis_sec: f("leadLagBasisSec", 60.0),
            max_book_age_ms: f("leadLagMaxBookAgeMs", 500.0) as u64,
            cooldown_ms: f("leadLagCooldownMs", 2000.0) as u64,
            symbols: p
                .get("leadLagSymbols")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_uppercase(),
        }
    }
}

/// Монета символа (BTCUSDT → BTC).
fn coin(s: &str) -> String {
    split(s).map(|x| x.0).unwrap_or_else(|| s.to_uppercase())
}

#[derive(Debug, Clone)]
struct Opportunity {
    coin: String,
    leader: String,
    follower: String,
    mov: f64,
    gap: f64,
    fees: f64,
    expected: f64,
    basis: f64,
    can_short: bool,
}

#[derive(Debug, Clone)]
struct Position {
    coin: String,
    ex: String,
    sym: String,
    long: bool,
    /// Исполнено (из исполнений своих ордеров), средняя цена входа и денежный поток с комиссиями.
    qty: f64,
    entry: f64,
    cash: f64,
    fair_ratio: f64,
    entry_gap: f64,
    opened_ms: u64,
    pending: Option<u64>,
    closing: Option<String>,
}

/// Стратегия (одна на процесс).
#[derive(Debug, Default)]
pub struct LeadLag {
    /// Середины ведущей по монете: (время, середина).
    rings: HashMap<String, VecDeque<(u64, f64)>>,
    /// Базис «биржа:символ» ведомой → (отношение, время).
    basis: HashMap<String, (f64, u64)>,
    positions: HashMap<String, Position>,
    own: HashMap<(String, u64), String>,
    seen: HashMap<String, u64>,
    cooldown: HashMap<String, u64>,
    last_check: u64,
    opportunities: Vec<Opportunity>,
    leader_up: bool,
    pub opened: u64,
    pub closed: u64,
    pub wins: u64,
    pub failed: u64,
    pub pnl: f64,
}

fn mid(b: &Book, now_ms: u64, max_age: u64) -> Option<(f64, f64, f64, f64)> {
    let _ = now_ms;
    if !b.is_ready() || b.age_ms().is_none_or(|a| a > max_age) {
        return None;
    }
    let ((bid, bq), (ask, aq)) = (b.best_bid()?, b.best_ask()?);
    Some((bid, bq, ask, aq))
}

impl LeadLag {
    /// Символы ведомых бирж, занятые позициями («биржа:символ»).
    pub fn held(&self) -> HashSet<String> {
        self.positions.keys().cloned().collect()
    }

    fn absorb(&mut self, venues: &HashMap<String, Venue>) {
        for (ex, v) in venues {
            let from = self.seen.get(ex).copied().unwrap_or(0);
            let mut last = from;
            for e in v.account.execs().iter().filter(|e| e.seq > from) {
                last = e.seq;
                let Some(key) = self.own.get(&(ex.clone(), e.order_id)) else {
                    continue;
                };
                let Some(p) = self.positions.get_mut(key) else {
                    continue;
                };
                let opening = (e.side == Side::Buy) == p.long;
                if opening {
                    p.entry = (p.entry * p.qty + e.price * e.qty) / (p.qty + e.qty);
                    p.qty += e.qty;
                } else {
                    p.qty = (p.qty - e.qty).max(0.0);
                }
                p.cash += if e.side == Side::Buy {
                    -e.qty * e.price
                } else {
                    e.qty * e.price
                } - e.fee;
            }
            self.seen.insert(ex.clone(), last);
        }
    }

    /// Шаг: (раз в leadLagCheckMs) цены ведущей, базис, выходы и входы. busy — «биржа:символ», занятые другими
    /// стратегиями.
    pub fn step(
        &mut self,
        venues: &mut HashMap<String, Venue>,
        p: &Params,
        busy: &HashSet<String>,
        now_ms: u64,
    ) {
        self.absorb(venues);
        // ордера в полёте
        let keys: Vec<String> = self.positions.keys().cloned().collect();
        for k in &keys {
            let pos = self.positions.get_mut(k).unwrap();
            if let Some(id) = pos.pending
                && !venues.get(&pos.ex).is_some_and(|v| v.account.is_open(id))
            {
                pos.pending = None;
            }
        }
        self.finish(now_ms, p);
        if now_ms < self.last_check + p.check_ms.max(10) {
            return;
        }
        self.last_check = now_ms;
        self.leader_up = venues.contains_key(&p.leader);
        let on = p.enabled;
        for k in keys {
            self.manage(&k, venues, p, on, now_ms);
        }
        if !self.leader_up {
            self.opportunities.clear();
            return;
        }
        self.scan(venues, p, now_ms);
        if !on {
            return;
        }
        let ops = self.opportunities.clone();
        for o in ops {
            if o.expected < p.min_profit {
                break; // по убыванию
            }
            if o.mov.abs() < p.min_move || busy.contains(&o.follower) {
                continue;
            }
            if self.positions.len() >= p.max_positions {
                break;
            }
            self.open(&o, venues, p, now_ms);
        }
        self.absorb(venues);
    }

    /// Закрытые и несостоявшиеся позиции — в итог.
    fn finish(&mut self, now_ms: u64, p: &Params) {
        let done: Vec<String> = self
            .positions
            .iter()
            .filter(|(_, x)| x.pending.is_none() && x.qty <= 1e-12)
            .map(|(k, _)| k.clone())
            .collect();
        for k in done {
            let x = self.positions.remove(&k).unwrap();
            self.own.retain(|_, v| *v != k);
            match &x.closing {
                None => {
                    self.failed += 1;
                    tracing::warn!("[lead-lag] {k}: вход не исполнился");
                }
                Some(why) => {
                    self.closed += 1;
                    if x.cash > 0.0 {
                        self.wins += 1;
                    }
                    self.pnl += x.cash;
                    // тот же рывок ведущей не должен снова открыть позицию
                    self.cooldown
                        .insert(k.clone(), now_ms + p.cooldown_ms.max(p.window_ms));
                    tracing::info!("[lead-lag] {k}: выход — {why}, результат {:.4}", x.cash);
                }
            }
        }
    }

    fn scan(&mut self, venues: &HashMap<String, Venue>, p: &Params, now_ms: u64) {
        let filter: HashSet<String> = p
            .symbols
            .split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect();
        let mut leader_mid: HashMap<String, (f64, String)> = HashMap::new();
        if let Some(l) = venues.get(&p.leader) {
            for (s, b) in &l.books {
                let c = coin(s);
                if !filter.is_empty() && !filter.contains(s) && !filter.contains(&c) {
                    continue;
                }
                let Some((bid, _, ask, _)) = mid(b, now_ms, p.max_book_age_ms) else {
                    continue;
                };
                let m = (bid + ask) / 2.0;
                let ring = self.rings.entry(c.clone()).or_default();
                ring.push_back((now_ms, m));
                while ring.len() > 1024 {
                    ring.pop_front();
                }
                leader_mid.insert(c, (m, format!("{}:{s}", p.leader)));
            }
        }
        let mut out = Vec::new();
        for (ex, v) in venues {
            if ex == &p.leader {
                continue;
            }
            let perp = v.account.is_perp();
            for (s, b) in &v.books {
                let c = coin(s);
                let Some((lm, lname)) = leader_mid.get(&c) else {
                    continue;
                };
                let Some((bid, _, ask, _)) = mid(b, now_ms, p.max_book_age_ms) else {
                    continue;
                };
                let fm = (bid + ask) / 2.0;
                let key = format!("{ex}:{s}");
                let bs = self.basis.entry(key.clone()).or_insert((fm / lm, now_ms));
                let ratio = bs.0; // базис до этого отсчёта: рывок его почти не сдвигает
                let alpha = ((now_ms - bs.1) as f64 / (p.basis_sec * 1000.0)).min(1.0);
                *bs = (ratio + alpha * (fm / lm - ratio), now_ms);
                let Some(mov) = move_pct(&self.rings[&c], now_ms, p.window_ms) else {
                    continue;
                };
                let fair = lm * ratio;
                let fees = 2.0 * v.account.taker_fee() * 100.0;
                let gap = if mov >= 0.0 {
                    (fair - ask) / ask * 100.0
                } else {
                    (bid - fair) / bid * 100.0
                };
                out.push(Opportunity {
                    coin: c,
                    leader: lname.clone(),
                    follower: key,
                    mov,
                    gap,
                    fees,
                    expected: gap - fees,
                    basis: (ratio - 1.0) * 100.0,
                    can_short: perp,
                });
            }
        }
        out.sort_by(|a, b| b.expected.total_cmp(&a.expected));
        self.opportunities = out;
    }

    fn open(
        &mut self,
        o: &Opportunity,
        venues: &mut HashMap<String, Venue>,
        p: &Params,
        now_ms: u64,
    ) {
        if self.positions.contains_key(&o.follower)
            || self.cooldown.get(&o.follower).is_some_and(|t| *t > now_ms)
        {
            return;
        }
        let long = o.mov > 0.0;
        if !long && !o.can_short {
            return; // на споте продавать нечего
        }
        let Some((ex, sym)) = o.follower.split_once(':') else {
            return;
        };
        let Some(v) = venues.get_mut(ex) else { return };
        if !v.trading || (v.account.is_perp() && v.account.position_qty(sym).abs() > 1e-12) {
            return; // торговля выключена или позицией владеет кто-то ещё
        }
        let Some(b) = v.books.get(sym).cloned() else {
            return;
        };
        let Some((bid, bq, ask, aq)) = mid(&b, now_ms, p.max_book_age_ms) else {
            return;
        };
        let px = if long { ask } else { bid };
        let q = (p.order_quote / px).min(if long { aq } else { bq } * 0.5);
        self.cooldown
            .insert(o.follower.clone(), now_ms + p.cooldown_ms);
        let side = if long { Side::Buy } else { Side::Sell };
        match v
            .account
            .order(sym, side, Amount::Qty(q), None, Some(&b), &v.lim)
        {
            Ok(f) => {
                tracing::info!(
                    "[lead-lag] {}: {} {q} {} — ведущая {} сдвинулась на {:.4}%, ведомая отстала на {:.4}%",
                    o.coin,
                    if long {
                        "покупка"
                    } else {
                        "продажа"
                    },
                    o.follower,
                    o.leader,
                    o.mov,
                    o.gap
                );
                self.opened += 1;
                self.own
                    .insert((ex.to_owned(), f.order_id), o.follower.clone());
                let ratio = self.basis.get(&o.follower).map_or(1.0, |b| b.0);
                self.positions.insert(
                    o.follower.clone(),
                    Position {
                        coin: o.coin.clone(),
                        ex: ex.to_owned(),
                        sym: sym.to_owned(),
                        long,
                        qty: 0.0,
                        entry: 0.0,
                        cash: 0.0,
                        fair_ratio: ratio,
                        entry_gap: o.gap,
                        opened_ms: now_ms,
                        pending: Some(f.order_id),
                        closing: None,
                    },
                );
            }
            Err(e) => {
                self.failed += 1;
                tracing::warn!("[lead-lag] {}: вход отклонён: {e}", o.follower);
            }
        }
    }

    fn manage(
        &mut self,
        key: &str,
        venues: &mut HashMap<String, Venue>,
        p: &Params,
        on: bool,
        now_ms: u64,
    ) {
        let Some(pos) = self.positions.get(key).cloned() else {
            return;
        };
        if pos.pending.is_some() || pos.qty <= 1e-12 {
            return;
        }
        let Some(v) = venues.get_mut(&pos.ex) else {
            return;
        };
        if v.account.is_perp() && v.account.position_qty(&pos.sym).abs() < 1e-12 {
            tracing::warn!("[lead-lag] {key}: позиции на бирже уже нет — убираю из учёта");
            self.positions.remove(key);
            return;
        }
        let book = v.books.get(&pos.sym).cloned();
        let top = book.as_ref().and_then(|b| mid(b, now_ms, u64::MAX));
        let held = now_ms.saturating_sub(pos.opened_ms);
        let why = match top {
            None if held > p.max_hold_ms => Some("таймаут (нет стакана)".to_owned()),
            None => None,
            Some((bid, _, ask, _)) => {
                let exit = if pos.long { bid } else { ask };
                let pnl = if pos.long {
                    exit - pos.entry
                } else {
                    pos.entry - exit
                } / pos.entry
                    * 100.0;
                let lm = self
                    .rings
                    .get(&pos.coin)
                    .and_then(|r| r.back())
                    .map(|x| x.1);
                let gap = lm.map(|lm| {
                    let fair = lm * pos.fair_ratio;
                    if pos.long {
                        (fair - exit) / exit * 100.0
                    } else {
                        (exit - fair) / exit * 100.0
                    }
                });
                if !on {
                    Some("стратегия выключена".into())
                } else if pnl <= -p.stop_loss {
                    Some(format!("стоп {pnl:.4}%"))
                } else if held >= p.max_hold_ms {
                    Some("таймаут".into())
                } else {
                    gap.filter(|g| *g <= p.exit_gap)
                        .map(|g| format!("ведомая догнала (отставание {g:.4}%)"))
                }
            }
        };
        let Some(why) = why else { return };
        let side = if pos.long { Side::Sell } else { Side::Buy };
        match v
            .account
            .exit_market(&pos.sym, side, pos.qty, book.as_ref())
        {
            Ok(f) => {
                self.own
                    .insert((pos.ex.clone(), f.order_id), key.to_owned());
                let x = self.positions.get_mut(key).unwrap();
                x.pending = Some(f.order_id);
                x.closing = Some(why);
            }
            Err(e) => {
                tracing::warn!("[lead-lag] {key}: выход не исполнился ({why}): {e} — повторю")
            }
        }
    }

    /// Состояние для /arbitrage (leadLag), как в Java.
    pub fn stats(&self, p: &Params, now_ms: u64) -> Value {
        let ps: Vec<Value> = self
            .positions
            .iter()
            .map(|(k, x)| json!({"coin": x.coin, "follower": k, "side": if x.long { "лонг" } else { "шорт" }, "qty": x.qty,
                "entryPrice": x.entry, "entryGapPercent": x.entry_gap, "heldMs": now_ms.saturating_sub(x.opened_ms)}))
            .collect();
        let os: Vec<Value> = self
            .opportunities
            .iter()
            .take(30)
            .map(|o| json!({"coin": o.coin, "leader": o.leader, "follower": o.follower, "leaderMovePercent": o.mov,
                "gapPercent": o.gap, "roundTripFeePercent": o.fees, "expectedPercent": o.expected, "basisPercent": o.basis,
                "enough": o.mov.abs() >= p.min_move && o.expected >= p.min_profit && (o.mov > 0.0 || o.can_short)}))
            .collect();
        json!({"enabled": p.enabled, "leader": p.leader, "leaderRunning": self.leader_up, "opened": self.opened,
            "closed": self.closed, "wins": self.wins, "failed": self.failed, "pnl": self.pnl, "positions": ps, "opportunities": os})
    }
}

/// Движение ведущей за окно, %: от самого раннего отсчёта не старше окна до последнего.
fn move_pct(r: &VecDeque<(u64, f64)>, now_ms: u64, window_ms: u64) -> Option<f64> {
    if r.len() < 2 {
        return None;
    }
    let last = r.back()?.1;
    let from = r
        .iter()
        .find(|(t, _)| now_ms.saturating_sub(*t) <= window_ms)?
        .1;
    (from > 0.0).then(|| (last - from) / from * 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trading::account::Account;
    use crate::trading::paper::{Limits, Paper};

    fn book(bid: f64, ask: f64) -> Book {
        let mut b = Book::default();
        b.apply_snapshot(vec![(bid, 100.0)], vec![(ask, 100.0)], 1);
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
        m.insert("leadLagOrderQuote".into(), json!(100));
        m.insert("leadLagMaxBookAgeMs".into(), json!(60_000));
        m.insert("leadLagCheckMs".into(), json!(10));
        Params::from(&m)
    }

    fn step(
        ll: &mut LeadLag,
        acc: &mut HashMap<String, Account>,
        leader: (f64, f64),
        follower: (f64, f64),
        now: u64,
    ) {
        let mut venues: HashMap<String, Venue> = acc
            .iter_mut()
            .map(|(ex, a)| {
                let (b, k) = if ex == "binance" { leader } else { follower };
                (
                    ex.clone(),
                    Venue {
                        account: a,
                        trading: true,
                        lim: lim(),
                        books: HashMap::from([("XUSDT".to_string(), book(b, k))]),
                    },
                )
            })
            .collect();
        ll.step(&mut venues, &params(), &HashSet::new(), now);
    }

    fn accounts(perp: bool) -> HashMap<String, Account> {
        let mut f = Paper::new(&["XUSDT".into()], 1000.0, 0.02, 0.02);
        if perp {
            f.set_perp(5.0);
        }
        HashMap::from([
            (
                "binance".to_string(),
                Account::Paper(Paper::new(&["XUSDT".into()], 1000.0, 0.1, 0.1)),
            ),
            ("bybit".to_string(), Account::Paper(f)),
        ])
    }

    #[test]
    fn follows_leader_up_and_exits_when_caught_up() {
        let mut acc = accounts(false);
        let mut ll = LeadLag::default();
        // базис 1: обе биржи 100
        for t in 0..5u64 {
            step(&mut ll, &mut acc, (99.99, 100.01), (99.99, 100.01), t * 100);
        }
        // ведущая +1% за 100 мс, ведомая стоит — покупка на ведомой
        step(&mut ll, &mut acc, (100.99, 101.01), (99.99, 100.01), 500);
        assert_eq!(ll.opened, 1, "{}", ll.stats(&params(), 500));
        assert!(acc["bybit"].free("X") > 0.9);
        // ведомая догнала — выход с прибылью
        step(&mut ll, &mut acc, (100.99, 101.01), (100.99, 101.01), 600);
        step(&mut ll, &mut acc, (100.99, 101.01), (100.99, 101.01), 700);
        assert_eq!(ll.closed, 1, "{}", ll.stats(&params(), 700));
        assert!(ll.pnl > 0.8, "{}", ll.pnl);
        assert!(acc["bybit"].free("X") < 1e-9);
    }

    #[test]
    fn short_only_on_perp_and_stop_loss() {
        // спот: ведущая упала — не продаём
        let mut acc = accounts(false);
        let mut ll = LeadLag::default();
        for t in 0..5u64 {
            step(&mut ll, &mut acc, (99.99, 100.01), (99.99, 100.01), t * 100);
        }
        step(&mut ll, &mut acc, (98.99, 99.01), (99.99, 100.01), 500);
        assert_eq!(ll.opened, 0);
        // перп: шорт, затем ведомая растёт против — стоп
        let mut acc = accounts(true);
        let mut ll = LeadLag::default();
        for t in 0..5u64 {
            step(&mut ll, &mut acc, (99.99, 100.01), (99.99, 100.01), t * 100);
        }
        step(&mut ll, &mut acc, (98.99, 99.01), (99.99, 100.01), 500);
        assert_eq!(ll.opened, 1);
        let Account::Paper(f) = &acc["bybit"] else {
            unreachable!()
        };
        assert!(f.position("XUSDT").qty < 0.0, "шорт");
        step(&mut ll, &mut acc, (98.99, 99.01), (100.99, 101.01), 600);
        step(&mut ll, &mut acc, (98.99, 99.01), (100.99, 101.01), 700);
        assert_eq!(ll.closed, 1);
        assert!(ll.pnl < 0.0);
        let Account::Paper(f) = &acc["bybit"] else {
            unreachable!()
        };
        assert!(f.positions.is_empty());
    }
}
