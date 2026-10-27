//! Ценовой арбитраж перпов между биржами (только market=perp), как PerpPriceArbitrageStrategy в Java.
//!
//! Цена одного перпа на разных биржах ненадолго расходится. Лучший бид на A выше лучшего аска на B — бот одновременно
//! шортит на A и покупает на B; когда цены сошлись, обе позиции закрываются.
//!
//! - спред входа = (бид A − аск B) / середина, %; спред выхода = (аск A − бид B) / середина, %;
//! - вход: спред входа − `perpArbExitSpreadPercent` − 4 комиссии тейкера ≥ `perpArbMinProfitPercent`, оба стакана не
//!   старше `perpArbMaxBookAgeMs`, объём не больше `perpArbDepthUsage` лучших уровней; одна нога не встала — вторая
//!   закрывается, объёмы разошлись — лишнее закрывается;
//! - выход: спред выхода ≤ `perpArbExitSpreadPercent`, убыток по ценам больше `perpArbStopLossPercent` (стоп),
//!   таймаут `perpArbMaxHoldMinutes`, торговля на бирже выключена или стратегия выключена.
//!
//! Ноги и результат — по исполнениям своих ордеров (бумага и биржа одинаково). Монеты сопоставляются по базовой
//! валюте (USDT- и USDC-перпы — одна монета).

use crate::market::book::Book;
use crate::strategy::spotarb::Venue;
use crate::trading::paper::{Amount, Side, split};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct Params {
    pub enabled: bool,
    pub check_ms: u64,
    pub min_profit: f64,
    pub exit_spread: f64,
    pub stop_loss: f64,
    pub order_quote: f64,
    pub max_positions: usize,
    pub max_hold_ms: u64,
    pub max_book_age_ms: u64,
    pub depth_usage: f64,
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
                .get("perpArbEnabled")
                .and_then(|v| {
                    v.as_bool()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                })
                .unwrap_or(true),
            check_ms: f("perpArbCheckMs", 200.0) as u64,
            min_profit: f("perpArbMinProfitPercent", 0.05),
            exit_spread: f("perpArbExitSpreadPercent", 0.0),
            stop_loss: f("perpArbStopLossPercent", 0.5),
            order_quote: f("perpArbOrderQuote", 50.0),
            max_positions: f("perpArbMaxPositions", 3.0) as usize,
            max_hold_ms: (f("perpArbMaxHoldMinutes", 60.0) * 60_000.0) as u64,
            max_book_age_ms: f("perpArbMaxBookAgeMs", 1000.0) as u64,
            depth_usage: f("perpArbDepthUsage", 0.5),
            symbols: p
                .get("perpArbSymbols")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_uppercase(),
        }
    }
}

fn coin(s: &str) -> String {
    split(s).map(|x| x.0).unwrap_or_else(|| s.to_uppercase())
}

/// Нога: биржа и символ.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Leg {
    ex: String,
    sym: String,
}

impl Leg {
    fn name(&self) -> String {
        format!("{}:{}", self.ex, self.sym)
    }
}

/// Нога, верх стакана (бид, объём бида, аск, объём аска) и комиссия тейкера, %.
type LegTop = (Leg, (f64, f64, f64, f64), f64);

#[derive(Clone, Debug)]
struct Opportunity {
    coin: String,
    short: Leg,
    long: Leg,
    spread: f64,
    fees: f64,
    expected: f64,
    max_qty: f64,
    mid: f64,
}

#[derive(Clone, Debug, PartialEq)]
enum Stage {
    Opening,
    Open,
    Closing(String),
}

#[derive(Clone, Debug)]
struct Pair {
    coin: String,
    short: Leg,
    long: Leg,
    /// Позиция по ногам со знаком (по исполнениям) и сумма входа по ногам (для цен входа).
    held: HashMap<String, f64>,
    entry_notional: HashMap<String, f64>,
    entry_qty: HashMap<String, f64>,
    cash: f64,
    pending: Vec<(String, u64)>,
    stage: Stage,
    entry_spread: f64,
    /// Последний спред выхода (для админки).
    last_exit: Option<f64>,
    opened_ms: u64,
}

impl Pair {
    fn q(&self, ex: &str) -> f64 {
        self.held.get(ex).copied().unwrap_or(0.0)
    }
    fn entry(&self, ex: &str) -> f64 {
        self.entry_notional.get(ex).copied().unwrap_or(0.0)
            / self.entry_qty.get(ex).copied().unwrap_or(0.0).max(1e-18)
    }
}

/// Стратегия (одна на процесс).
#[derive(Debug, Default)]
pub struct PerpArb {
    pairs: BTreeMap<String, Pair>,
    own: HashMap<(String, u64), String>,
    seen: HashMap<String, u64>,
    last_check: u64,
    opportunities: Vec<Opportunity>,
    pub opened: u64,
    pub closed: u64,
    pub failed: u64,
    pub stops: u64,
    pub pnl: f64,
}

fn top(b: &Book) -> Option<(f64, f64, f64, f64)> {
    let ((bid, bq), (ask, aq)) = (b.best_bid()?, b.best_ask()?);
    Some((bid, bq, ask, aq))
}

impl PerpArb {
    /// «биржа:символ», занятые парами.
    pub fn held(&self) -> HashSet<String> {
        self.pairs
            .values()
            .flat_map(|p| [p.short.name(), p.long.name()])
            .collect()
    }

    fn absorb(&mut self, venues: &HashMap<String, Venue>) {
        for (ex, v) in venues {
            let from = self.seen.get(ex).copied().unwrap_or(0);
            let mut last = from;
            for e in v.account.execs().iter().filter(|e| e.seq > from) {
                last = e.seq;
                let Some(c) = self.own.get(&(ex.clone(), e.order_id)) else {
                    continue;
                };
                let Some(p) = self.pairs.get_mut(c) else {
                    continue;
                };
                let sign = if e.side == Side::Buy { 1.0 } else { -1.0 };
                let before = p.q(ex);
                *p.held.entry(ex.clone()).or_default() += sign * e.qty;
                if before * sign >= 0.0 {
                    // открывающее исполнение — в цену входа
                    *p.entry_notional.entry(ex.clone()).or_default() += e.qty * e.price;
                    *p.entry_qty.entry(ex.clone()).or_default() += e.qty;
                }
                p.cash += -sign * e.qty * e.price - e.fee;
            }
            self.seen.insert(ex.clone(), last);
        }
    }

    /// Спред выхода пары: (аск шорта − бид лонга) / середина, %.
    fn exit_spread(p: &Pair, venues: &HashMap<String, Venue>) -> Option<f64> {
        let a = top(venues.get(&p.short.ex)?.books.get(&p.short.sym)?)?;
        let b = top(venues.get(&p.long.ex)?.books.get(&p.long.sym)?)?;
        let mid = (a.0 + a.2 + b.0 + b.2) / 4.0;
        Some((a.2 - b.0) / mid * 100.0)
    }

    pub fn step(
        &mut self,
        venues: &mut HashMap<String, Venue>,
        p: &Params,
        busy: &HashSet<String>,
        now_ms: u64,
    ) {
        self.absorb(venues);
        for pair in self.pairs.values_mut() {
            pair.pending
                .retain(|(ex, id)| venues.get(ex).is_some_and(|v| v.account.is_open(*id)));
        }
        let coins: Vec<String> = self.pairs.keys().cloned().collect();
        for c in &coins {
            self.advance(c, venues, p, now_ms);
        }
        self.absorb(venues);
        if now_ms < self.last_check + p.check_ms.max(50) {
            return;
        }
        self.last_check = now_ms;
        self.scan(venues, p);
        if !p.enabled {
            return;
        }
        for o in self.opportunities.clone() {
            if self.pairs.len() >= p.max_positions {
                break;
            }
            if o.expected < p.min_profit || self.pairs.contains_key(&o.coin) {
                continue;
            }
            let ok = |l: &Leg| {
                venues
                    .get(&l.ex)
                    .is_some_and(|v| v.trading && v.account.position_qty(&l.sym).abs() < 1e-12)
                    && !busy.contains(&l.name())
            };
            if !ok(&o.short) || !ok(&o.long) {
                continue;
            }
            self.open(&o, venues, p, now_ms);
        }
        self.absorb(venues);
    }

    fn scan(&mut self, venues: &HashMap<String, Venue>, p: &Params) {
        let filter: HashSet<String> = p
            .symbols
            .split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect();
        let mut by: BTreeMap<String, Vec<LegTop>> = BTreeMap::new();
        for (ex, v) in venues {
            if !v.account.is_perp() {
                continue;
            }
            for (s, b) in &v.books {
                let c = coin(s);
                if !filter.is_empty() && !filter.contains(s) && !filter.contains(&c) {
                    continue;
                }
                if !b.is_ready() || b.age_ms().is_none_or(|a| a > p.max_book_age_ms) {
                    continue; // старый стакан — не сравниваем
                }
                if let Some(t) = top(b) {
                    by.entry(c).or_default().push((
                        Leg {
                            ex: ex.clone(),
                            sym: s.clone(),
                        },
                        t,
                        v.account.taker_fee() * 100.0,
                    ));
                }
            }
        }
        let mut out = Vec::new();
        for (c, vs) in by {
            let mut best: Option<Opportunity> = None;
            for (a, ta, fa) in &vs {
                for (b, tb, fb) in &vs {
                    if a.ex == b.ex {
                        continue;
                    }
                    let mid = (ta.0 + ta.2 + tb.0 + tb.2) / 4.0;
                    let spread = (ta.0 - tb.2) / mid * 100.0;
                    let fees = 2.0 * fa + 2.0 * fb;
                    let expected = spread - p.exit_spread - fees;
                    if best.as_ref().is_none_or(|x| expected > x.expected) {
                        best = Some(Opportunity {
                            coin: c.clone(),
                            short: a.clone(),
                            long: b.clone(),
                            spread,
                            fees,
                            expected,
                            max_qty: ta.1.min(tb.3) * p.depth_usage,
                            mid,
                        });
                    }
                }
            }
            out.extend(best);
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
        let mut qty = (p.order_quote / o.mid).min(o.max_qty);
        // общий шаг лота обеих бирж
        for _ in 0..4 {
            let qa = venues[&o.short.ex]
                .account
                .filter(&o.short.sym)
                .map_or(qty, |f| f.round_qty(qty));
            let qb = venues[&o.long.ex]
                .account
                .filter(&o.long.sym)
                .map_or(qa, |f| f.round_qty(qa));
            if (qb - qty).abs() < 1e-12 {
                break;
            }
            qty = qb;
        }
        if qty <= 0.0 {
            return;
        }
        tracing::info!(
            "[perp-arb] {}: продаю {} / покупаю {}, спред {:.4}%, ожидаемо {:.4}% после комиссий, объём {qty}",
            o.coin,
            o.short.name(),
            o.long.name(),
            o.spread,
            o.expected
        );
        let mut pair = Pair {
            coin: o.coin.clone(),
            short: o.short.clone(),
            long: o.long.clone(),
            held: HashMap::new(),
            entry_notional: HashMap::new(),
            entry_qty: HashMap::new(),
            cash: 0.0,
            pending: Vec::new(),
            stage: Stage::Opening,
            entry_spread: o.spread,
            last_exit: None,
            opened_ms: now_ms,
        };
        for (leg, side) in [(&o.short, Side::Sell), (&o.long, Side::Buy)] {
            let v = venues.get_mut(&leg.ex).unwrap();
            let book = v.books.get(&leg.sym).cloned();
            match v.account.order(
                &leg.sym,
                side,
                Amount::Qty(qty),
                None,
                book.as_ref(),
                &v.lim,
            ) {
                Ok(f) => {
                    pair.pending.push((leg.ex.clone(), f.order_id));
                    self.own
                        .insert((leg.ex.clone(), f.order_id), o.coin.clone());
                }
                Err(e) => {
                    tracing::warn!("[perp-arb] {}: нога {} отклонена: {e}", o.coin, leg.name())
                }
            }
        }
        self.pairs.insert(o.coin.clone(), pair);
        self.opened += 1;
    }

    /// Закрыть ноги пары: reduce-выходы не больше фактической позиции.
    fn close(&mut self, c: &str, venues: &mut HashMap<String, Venue>, why: String) {
        let Some(p) = self.pairs.get(c).cloned() else {
            return;
        };
        tracing::info!("[perp-arb] {c}: закрываю ({why})");
        let mut ids = Vec::new();
        for leg in [&p.short, &p.long] {
            let mine = p.q(&leg.ex);
            let Some(v) = venues.get_mut(&leg.ex) else {
                continue;
            };
            let actual = v.account.position_qty(&leg.sym);
            // закрываем не больше фактической позиции: ногу могли ликвидировать или закрыть вручную
            let q = if mine < 0.0 {
                (-mine).min((-actual).max(0.0))
            } else {
                mine.min(actual.max(0.0))
            };
            if q <= 1e-12 {
                if mine.abs() > 1e-12 {
                    self.pairs
                        .get_mut(c)
                        .unwrap()
                        .held
                        .insert(leg.ex.clone(), 0.0); // позиции уже нет
                }
                continue;
            }
            let side = if mine < 0.0 { Side::Buy } else { Side::Sell };
            let book = v.books.get(&leg.sym).cloned();
            match v.account.exit_market(&leg.sym, side, q, book.as_ref()) {
                Ok(f) => ids.push((leg.ex.clone(), f.order_id)),
                Err(e) => tracing::warn!(
                    "[perp-arb] {c}: выход {} не исполнился: {e} — повторю",
                    leg.name()
                ),
            }
        }
        for (ex, id) in &ids {
            self.own.insert((ex.clone(), *id), c.to_owned());
        }
        let x = self.pairs.get_mut(c).unwrap();
        x.pending.extend(ids);
        x.stage = Stage::Closing(why);
    }

    fn advance(&mut self, c: &str, venues: &mut HashMap<String, Venue>, p: &Params, now_ms: u64) {
        let Some(pair) = self.pairs.get(c).cloned() else {
            return;
        };
        if !pair.pending.is_empty() {
            return;
        }
        let (sq, lq) = (-pair.q(&pair.short.ex), pair.q(&pair.long.ex));
        let flat = sq.abs() < 1e-12 && lq.abs() < 1e-12;
        match pair.stage {
            Stage::Opening => {
                if sq <= 0.0 || lq <= 0.0 {
                    // одна нога не встала — без хеджа не держим
                    self.failed += 1;
                    if flat {
                        self.pairs.remove(c);
                    } else {
                        self.close(
                            c,
                            venues,
                            format!("нога не исполнилась (шорт {sq}, лонг {lq}) — откат"),
                        );
                    }
                    return;
                }
                let common = sq.min(lq);
                let x = self.pairs.get_mut(c).unwrap();
                let (se, le) = (x.entry(&x.short.ex), x.entry(&x.long.ex));
                x.entry_spread = (se - le) / ((se + le) / 2.0) * 100.0;
                x.stage = Stage::Open;
                // объёмы разошлись — лишнее закрыть
                for (leg, extra, side) in [
                    (pair.short.clone(), sq - common, Side::Buy),
                    (pair.long.clone(), lq - common, Side::Sell),
                ] {
                    if extra > 1e-12
                        && let Some(v) = venues.get_mut(&leg.ex)
                    {
                        let book = v.books.get(&leg.sym).cloned();
                        if let Ok(f) = v.account.exit_market(&leg.sym, side, extra, book.as_ref()) {
                            self.own.insert((leg.ex.clone(), f.order_id), c.to_owned());
                            self.pairs
                                .get_mut(c)
                                .unwrap()
                                .pending
                                .push((leg.ex.clone(), f.order_id));
                        }
                    }
                }
            }
            Stage::Closing(why) => {
                if flat {
                    let x = self.pairs.remove(c).unwrap();
                    self.own.retain(|_, v| v != c);
                    self.closed += 1;
                    self.pnl += x.cash;
                    tracing::info!(
                        "[perp-arb] {c}: пара закрыта ({why}), результат {:.4}",
                        x.cash
                    );
                } else {
                    self.close(c, venues, why); // не всё закрылось — повтор
                }
            }
            Stage::Open => {
                let gone = |l: &Leg| venues.get(&l.ex).is_none_or(|v| !v.trading);
                let why = if !p.enabled {
                    Some("стратегия выключена".to_owned())
                } else if gone(&pair.short) || gone(&pair.long) {
                    Some("торговля на бирже остановлена".to_owned())
                } else if now_ms.saturating_sub(pair.opened_ms) > p.max_hold_ms {
                    Some("таймаут".to_owned())
                } else {
                    let ex = Self::exit_spread(&pair, venues);
                    self.pairs.get_mut(c).unwrap().last_exit = ex;
                    match ex {
                        Some(e) if e <= p.exit_spread => {
                            Some(format!("цены сошлись (спред выхода {e:.4}%)"))
                        }
                        Some(e) if pair.entry_spread - e < -p.stop_loss => {
                            self.stops += 1;
                            Some(format!(
                                "стоп: расхождение выросло, {:.4}%",
                                pair.entry_spread - e
                            ))
                        }
                        _ => None,
                    }
                };
                if let Some(w) = why {
                    self.close(c, venues, w);
                }
            }
        }
    }

    /// Состояние для /arbitrage (perpPrice), как в Java.
    pub fn stats(&self, p: &Params, venues: Option<&HashMap<String, Venue>>) -> Value {
        let ps: Vec<Value> = self
            .pairs
            .values()
            .map(|x| {
                let e = venues.and_then(|v| Self::exit_spread(x, v)).or(x.last_exit);
                json!({"coin": x.coin, "short": x.short.name(), "long": x.long.name(), "qty": (-x.q(&x.short.ex)).min(x.q(&x.long.ex)),
                    "entrySpreadPercent": x.entry_spread, "exitSpreadPercent": e, "openedAtMs": x.opened_ms})
            })
            .collect();
        let os: Vec<Value> = self
            .opportunities
            .iter()
            .map(|o| json!({"coin": o.coin, "sell": o.short.name(), "buy": o.long.name(), "spreadPercent": o.spread,
                "roundTripFeePercent": o.fees, "expectedPercent": o.expected, "enough": o.expected >= p.min_profit}))
            .collect();
        json!({"enabled": p.enabled, "opened": self.opened, "closed": self.closed, "failed": self.failed, "stops": self.stops,
            "pnl": self.pnl, "pairs": ps, "opportunities": os})
    }
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
        m.insert("perpArbOrderQuote".into(), json!(100));
        m.insert("perpArbMaxBookAgeMs".into(), json!(60_000));
        Params::from(&m)
    }

    fn accounts() -> HashMap<String, Account> {
        let mk = || {
            let mut p = Paper::new(&["XUSDT".into()], 1000.0, 0.02, 0.05);
            p.set_perp(5.0);
            Account::Paper(p)
        };
        HashMap::from([("a".to_string(), mk()), ("b".to_string(), mk())])
    }

    fn step(
        pa: &mut PerpArb,
        acc: &mut HashMap<String, Account>,
        a: (f64, f64),
        b: (f64, f64),
        now: u64,
    ) {
        let mut venues: HashMap<String, Venue> = acc
            .iter_mut()
            .map(|(ex, x)| {
                let (bid, ask) = if ex == "a" { a } else { b };
                (
                    ex.clone(),
                    Venue {
                        account: x,
                        trading: true,
                        lim: lim(),
                        books: HashMap::from([("XUSDT".to_string(), book(bid, ask))]),
                    },
                )
            })
            .collect();
        pa.step(&mut venues, &params(), &HashSet::new(), now);
    }

    fn pos(acc: &HashMap<String, Account>, ex: &str) -> f64 {
        acc[ex].position_qty("XUSDT")
    }

    #[test]
    fn opens_short_long_and_closes_on_convergence() {
        let mut acc = accounts();
        let mut pa = PerpArb::default();
        // на a дороже на 1%: шорт a, лонг b
        step(&mut pa, &mut acc, (101.0, 101.01), (99.99, 100.0), 1000);
        step(&mut pa, &mut acc, (101.0, 101.01), (99.99, 100.0), 1100);
        assert_eq!(pa.opened, 1, "{}", pa.stats(&params(), None));
        assert!(pos(&acc, "a") < 0.0 && pos(&acc, "b") > 0.0);
        assert!((pa.pairs["X"].entry_spread - 1.0).abs() < 0.05);
        // сошлись — закрыть обе ноги
        step(&mut pa, &mut acc, (100.49, 100.5), (100.5, 100.51), 1300);
        step(&mut pa, &mut acc, (100.49, 100.5), (100.5, 100.51), 1500);
        assert_eq!(pa.closed, 1, "{}", pa.stats(&params(), None));
        assert!(pos(&acc, "a").abs() < 1e-12 && pos(&acc, "b").abs() < 1e-12);
        assert!(pa.pnl > 0.5, "{}", pa.pnl);
    }

    #[test]
    fn stop_when_spread_widens() {
        let mut acc = accounts();
        let mut pa = PerpArb::default();
        step(&mut pa, &mut acc, (101.0, 101.01), (99.99, 100.0), 1000);
        step(&mut pa, &mut acc, (101.0, 101.01), (99.99, 100.0), 1100);
        // расхождение выросло до ~3%
        step(&mut pa, &mut acc, (103.0, 103.01), (99.99, 100.0), 1300);
        step(&mut pa, &mut acc, (103.0, 103.01), (99.99, 100.0), 1500);
        assert_eq!(
            (pa.stops, pa.closed),
            (1, 1),
            "{}",
            pa.stats(&params(), None)
        );
        assert!(pa.pnl < 0.0);
    }
}
