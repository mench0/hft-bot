//! «Сбор спреда» (маркет-мейкинг на споте), как SpreadCaptureStrategy в Java: лимитная покупка по лучшему биду
//! и продажа купленного по лучшему аску. Купили по биду, продали по аску — заработали спред минус две комиссии мейкера.
//!
//! - Заявки стоят, только если спред минус две комиссии мейкера ≥ `scMinEdgePercent`; раз в `scRequoteMs` они
//!   сверяются с ценой и переставляются.
//! - Покупка — пока купленного меньше чем на `scMaxInventoryQuote`, стакан ликвиден (уровни и глубина в полосе),
//!   цена за 15 минут не упала больше `scMaxDropPercent` и лучшая цена меняется не чаще `scMaxTopChangesPerSec`.
//! - Продаётся только купленное самой стратегией, не дешевле средней покупки плюс две комиссии мейкера.
//! - Стоп: цена ниже средней покупки на `scStopLossPercent` или монета не продалась за `scMaxHoldMinutes` —
//!   продажа рыночным ордером. Выключили стратегию или торговлю — заявки снимаются, купленное продаётся.

use crate::account::Account;
use crate::book::Book;
#[cfg(test)]
use crate::paper::Paper;
use crate::paper::{Amount, Exec, Limits, Side};
use crate::rest::Filter;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// Имя стратегии (как OWNER в Java и id профиля подбора).
pub const ID: &str = "spread-capture";

/// Параметры стратегии из параметров биржи.
#[derive(Clone, Debug)]
pub struct Params {
    pub enabled: bool,
    pub order_quote: f64,
    pub min_edge_percent: f64,
    pub requote: Duration,
    pub max_inventory_quote: f64,
    pub stop_loss_percent: f64,
    pub max_hold: Duration,
    pub min_depth_multiple: f64,
    pub depth_band_percent: f64,
    pub min_book_levels: usize,
    pub max_drop_percent: f64,
    pub max_top_changes_per_sec: f64,
    pub min_trade_minutes: f64,
    pub min_minute_volume_multiple: f64,
    pub min_spread_presence_percent: f64,
    pub jump_ticks: f64,
    pub max_jumps_per_min: usize,
}

impl Params {
    pub fn from(p: &Map<String, Value>) -> Params {
        let f = |k: &str, d: f64| p.get(k).and_then(Value::as_f64).unwrap_or(d);
        Params {
            enabled: p
                .get("spreadCaptureEnabled")
                .and_then(Value::as_bool)
                .unwrap_or(true)
                && p.get("market").and_then(Value::as_str) != Some("perp"),
            order_quote: f("scOrderQuote", 20.0),
            min_edge_percent: f("scMinEdgePercent", 0.1),
            requote: Duration::from_millis(f("scRequoteMs", 3000.0) as u64),
            max_inventory_quote: f("scMaxInventoryQuote", 100.0),
            stop_loss_percent: f("scStopLossPercent", 2.0),
            max_hold: Duration::from_secs_f64(f("scMaxHoldMinutes", 60.0) * 60.0),
            min_depth_multiple: f("scMinDepthMultiple", 0.0),
            depth_band_percent: f("scDepthBandPercent", 1.0),
            min_book_levels: f("scMinBookLevels", 0.0) as usize,
            max_drop_percent: f("scMaxDropPercent", 0.0),
            max_top_changes_per_sec: f("scMaxTopChangesPerSec", 0.0),
            min_trade_minutes: f("scMinTradeMinutes", 0.0),
            min_minute_volume_multiple: f("scMinMinuteVolumeMultiple", 0.0),
            min_spread_presence_percent: f("scMinSpreadPresencePercent", 0.0),
            jump_ticks: f("scJumpTicks", 0.0),
            max_jumps_per_min: f("scMaxJumpsPerMin", 6.0) as usize,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Quote {
    id: u64,
    price: f64,
    qty: f64,
}

/// Своя заявка в стакане. На бумаге своих заявок в стакане биржи нет — весь объём уровня чужой.
#[derive(Clone, Debug, Default)]
struct Queue {
    status: Option<&'static str>,
    level: f64,
    others: f64,
    ahead: Option<f64>,
    outbid: u64,
}

#[derive(Debug, Default)]
struct State {
    bid: Option<Quote>,
    ask: Option<Quote>,
    inv: f64,
    avg: f64,
    inv_since: Option<Instant>,
    pnl: f64,
    trades: u64,
    last_step: Option<Instant>,
    /// Середина цены и «спред после комиссий был достаточным» по шагам (15 минут).
    mids: VecDeque<(Instant, f64, bool)>,
    /// Когда начали следить (первую минуту постоянство спреда не оценивается).
    since: Option<Instant>,
    flow: Option<String>,
    trade_minutes: Option<usize>,
    minute_volume: Option<f64>,
    presence: Option<f64>,
    jumps: u64,
    jump_ts: VecDeque<Instant>,
    jump_pause_until: Option<Instant>,
    last_top: (f64, f64),
    top_changes: u32,
    top_window: Option<Instant>,
    top_per_sec: f64,
    thin: Option<String>,
    market: Option<String>,
    decision: String,
    q_bid: Queue,
    q_ask: Queue,
}

/// Стратегия одной биржи.
#[derive(Debug)]
pub struct SpreadCapture {
    maker: f64,
    states: HashMap<String, State>,
    /// Свои заявки: номер → символ.
    own: HashMap<u64, String>,
    /// Последнее учтённое исполнение счёта.
    seen: u64,
}

impl SpreadCapture {
    /// maker_pct — комиссия мейкера биржи, %.
    pub fn new(maker_pct: f64) -> SpreadCapture {
        SpreadCapture {
            maker: maker_pct / 100.0,
            states: HashMap::new(),
            own: HashMap::new(),
            seen: 0,
        }
    }

    /// Учесть исполнения своих заявок: покупка — запас и средняя цена с комиссией, продажа — результат.
    pub fn absorb(&mut self, execs: &VecDeque<Exec>) {
        let from = self.seen;
        for e in execs.iter().filter(|e| e.seq > from) {
            self.seen = e.seq;
            if !self.own.contains_key(&e.order_id) {
                continue;
            }
            let s = self.states.entry(e.symbol.clone()).or_default();
            match e.side {
                Side::Buy => {
                    s.avg = (s.avg * s.inv + e.qty * e.price + e.fee) / (s.inv + e.qty);
                    s.inv += e.qty;
                    s.inv_since.get_or_insert_with(Instant::now);
                }
                Side::Sell => {
                    let q = e.qty.min(s.inv);
                    let r = q * e.price - e.fee - q * s.avg;
                    s.inv -= q;
                    s.pnl += r;
                    s.trades += 1;
                    if s.inv <= 1e-12 {
                        s.inv = 0.0;
                        s.avg = 0.0;
                        s.inv_since = None;
                    }
                    tracing::info!(
                        "[spread-capture] {}: продано {} @ {}, результат {r:.4}",
                        e.symbol,
                        e.qty,
                        e.price
                    );
                }
            }
        }
    }

    fn cancel(&mut self, paper: &mut Account, sym: &str, buy: bool) {
        let s = self.states.entry(sym.to_owned()).or_default();
        let q = if buy { s.bid.take() } else { s.ask.take() };
        if let Some(q) = q {
            paper.cancel(q.id);
        }
        if buy {
            s.q_bid = Queue::default()
        } else {
            s.q_ask = Queue::default()
        }
    }

    /// Снять заявки символа.
    pub fn cancel_quotes(&mut self, paper: &mut Account, sym: &str) {
        self.cancel(paper, sym, true);
        self.cancel(paper, sym, false);
    }

    /// Продать купленное рыночным ордером (стоп, таймаут, выключение).
    fn dump(&mut self, paper: &mut Account, sym: &str, book: Option<&Book>, why: &str) {
        let inv = self.states.get(sym).map_or(0.0, |s| s.inv);
        if inv <= 0.0 {
            return;
        }
        tracing::warn!("[spread-capture] {sym}: продаю купленное ({inv}) — {why}");
        match paper.sell_exit(sym, inv, book) {
            Ok(f) => {
                self.own.insert(f.order_id, sym.to_owned());
                self.absorb(&paper.execs().clone());
            }
            Err(e) => tracing::warn!("[spread-capture] {sym}: продать не удалось — {e}"),
        }
    }

    /// Выключить по символу: заявки снять, купленное продать.
    pub fn stop_symbol(&mut self, paper: &mut Account, sym: &str, book: Option<&Book>, why: &str) {
        self.cancel_quotes(paper, sym);
        self.dump(paper, sym, book, why);
    }

    /// Есть ли что снимать или продавать.
    pub fn active(&self) -> bool {
        self.states
            .values()
            .any(|s| s.bid.is_some() || s.ask.is_some() || s.inv > 0.0)
    }

    /// Символы, где стратегия что-то держит.
    pub fn symbols(&self) -> Vec<String> {
        self.states.keys().cloned().collect()
    }

    /// Один шаг по символу (вызывается часто: смены лучшей цены считаются на каждом шаге, заявки — раз в requote).
    /// `minutes` — оборот последних закрытых минут (None — свечей нет или устарели), `filter` — правила пары.
    #[allow(clippy::too_many_arguments)]
    pub fn step(
        &mut self,
        sym: &str,
        book: Option<&Book>,
        paper: &mut Account,
        p: &Params,
        lim: &Limits,
        now: Instant,
        minutes: Option<&[f64]>,
        filter: Option<Filter>,
    ) {
        self.absorb(&paper.execs().clone());
        let ready =
            book.filter(|b| b.is_ready() && b.age_ms().is_some_and(|a| a <= lim.max_data_age_ms));
        let Some(b) = ready else {
            self.cancel_quotes(paper, sym);
            return;
        };
        let (bid, bq) = b.best_bid().unwrap();
        let (ask, aq) = b.best_ask().unwrap();
        let mid = (bid + ask) / 2.0;
        let s = self.states.entry(sym.to_owned()).or_default();
        count_top(s, bid, ask, now);
        if s.last_step
            .is_some_and(|t| now.duration_since(t) < p.requote)
        {
            return;
        }
        s.last_step = Some(now);
        queue(&mut s.q_bid, s.bid, true, bid, bq);
        queue(&mut s.q_ask, s.ask, false, ask, aq);
        // стоп-лосс и таймаут удержания
        let stop = s.inv > 0.0 && mid < s.avg * (1.0 - p.stop_loss_percent / 100.0);
        let hold = s.inv > 0.0
            && s.inv_since
                .is_some_and(|t| now.duration_since(t) > p.max_hold);
        if stop || hold {
            self.stop_symbol(
                paper,
                sym,
                Some(b),
                if stop {
                    "стоп-лосс"
                } else {
                    "таймаут"
                },
            );
            return;
        }
        let edge = (ask - bid) / mid * 100.0 - 2.0 * self.maker * 100.0;
        let s = self.states.get_mut(sym).unwrap();
        s.since.get_or_insert(now);
        s.market = market_issue(s, p, mid, now, edge >= p.min_edge_percent);
        s.thin = thin_book(b, p, bid, ask);
        s.flow = flow_issue(s, p, minutes, now);
        if edge < p.min_edge_percent {
            s.decision = format!(
                "заявки сняты: спред после комиссий {edge:.3}% < {:.3}%",
                p.min_edge_percent
            );
            self.cancel_quotes(paper, sym);
            return;
        }
        let qty = filter.map_or(p.order_quote / mid, |f| f.round_qty(p.order_quote / mid));
        let quote = crate::paper::split(sym).map(|x| x.1).unwrap_or_default();
        let s = &self.states[sym];
        let held = s.bid.map_or(0.0, |q| q.qty * q.price);
        let want_bid = qty > 0.0
            && s.thin.is_none()
            && s.market.is_none()
            && s.flow.is_none()
            && s.inv * mid + qty * bid <= p.max_inventory_quote + 1e-9
            && paper.free(&quote) + held >= qty * bid * (1.0 + self.maker);
        let decision = if want_bid {
            "покупаем и продаём".to_owned()
        } else {
            format!(
                "только продажа: {}",
                s.thin
                    .clone()
                    .or_else(|| s.market.clone())
                    .or_else(|| s.flow.clone())
                    .unwrap_or_else(|| "лимит купленного или нет денег".into())
            )
        };
        let inv = s.inv;
        let floor = if inv > 0.0 {
            s.avg * (1.0 + 2.0 * self.maker)
        } else {
            0.0
        };
        self.states.get_mut(sym).unwrap().decision = decision;
        // перебить чужую лучшую цену на scJumpTicks шагов, если спред после этого ещё достаточный и нет ценовой войны
        let step = self.jump_step(sym, p, filter, bid, ask, mid, now);
        let (bid_px, ask_base) = (bid + step, ask - step);
        let cur_bid = self.states[sym].bid.map(|q| q.price);
        if step > 0.0 && want_bid && cur_bid != Some(bid_px) {
            self.count_jump(sym, p, now);
        }
        self.requote(paper, sym, Side::Buy, want_bid, bid_px, qty, b, lim);
        let ask_qty = if inv <= qty * 1.5 { inv } else { qty };
        let ask_px = ask_base.max(floor);
        let cur_ask = self.states[sym].ask.map(|q| q.price);
        if step > 0.0 && ask_qty > 0.0 && ask_px == ask_base && cur_ask != Some(ask_px) {
            self.count_jump(sym, p, now);
        }
        self.requote(
            paper,
            sym,
            Side::Sell,
            ask_qty > 0.0,
            ask_px,
            ask_qty,
            b,
            lim,
        );
    }

    /// На сколько сдвинуть цену от чужой лучшей: scJumpTicks шагов цены, если шаг известен, нет паузы после
    /// ценовой войны, заявки не сходятся и спред после сдвига минус две комиссии мейкера ≥ scMinEdgePercent; иначе 0.
    #[allow(clippy::too_many_arguments)]
    fn jump_step(
        &self,
        sym: &str,
        p: &Params,
        filter: Option<Filter>,
        bid: f64,
        ask: f64,
        mid: f64,
        now: Instant,
    ) -> f64 {
        let tick = filter.map_or(0.0, |f| f.tick);
        if p.jump_ticks <= 0.0
            || tick <= 0.0
            || self.states[sym].jump_pause_until.is_some_and(|t| now < t)
        {
            return 0.0;
        }
        let step = p.jump_ticks * tick;
        if bid + step >= ask - step {
            return 0.0;
        }
        let edge = (ask - bid - 2.0 * step) / mid * 100.0 - 2.0 * self.maker * 100.0;
        if edge >= p.min_edge_percent {
            step
        } else {
            0.0
        }
    }

    /// Учесть перебивание; больше scMaxJumpsPerMin за минуту — ценовая война: минуту не перебивать.
    fn count_jump(&mut self, sym: &str, p: &Params, now: Instant) {
        let s = self.states.get_mut(sym).unwrap();
        s.jumps += 1;
        s.jump_ts.push_back(now);
        while s
            .jump_ts
            .front()
            .is_some_and(|t| now.duration_since(*t) > Duration::from_secs(60))
        {
            s.jump_ts.pop_front();
        }
        if s.jump_ts.len() > p.max_jumps_per_min {
            s.jump_pause_until = Some(now + Duration::from_secs(60));
            s.jump_ts.clear();
            s.decision = format!(
                "ценовая война: перебиваний больше {} в минуту — минуту встаю в очередь",
                p.max_jumps_per_min
            );
            tracing::info!("[spread-capture] {sym}: {}", s.decision);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn requote(
        &mut self,
        paper: &mut Account,
        sym: &str,
        side: Side,
        want: bool,
        px: f64,
        qty: f64,
        b: &Book,
        lim: &Limits,
    ) {
        let buy = side == Side::Buy;
        let cur = {
            let s = &self.states[sym];
            if buy { s.bid } else { s.ask }
        };
        if !want {
            if cur.is_some() {
                self.cancel(paper, sym, buy);
            }
            return;
        }
        if let Some(q) = cur {
            if paper.is_open(q.id) && q.price == px && (q.qty - qty).abs() <= 1e-12 * qty.max(1.0) {
                return; // уже стоит где нужно
            }
            self.cancel(paper, sym, buy);
        }
        match paper.order(sym, side, Amount::Qty(qty), Some(px), Some(b), lim) {
            Ok(f) => {
                self.own.insert(f.order_id, sym.to_owned());
                let resting = f.status == "NEW" || f.status == "PARTIALLY_FILLED";
                let s = self.states.get_mut(sym).unwrap();
                let q = resting.then_some(Quote {
                    id: f.order_id,
                    price: px,
                    qty: qty - f.executed,
                });
                if buy {
                    s.bid = q;
                    s.q_bid.ahead = None;
                } else {
                    s.ask = q;
                    s.q_ask.ahead = None;
                }
                if f.executed > 0.0 {
                    self.absorb(&paper.execs().clone());
                }
            }
            Err(e) => {
                self.states.get_mut(sym).unwrap().decision = format!("заявка не поставлена: {e}")
            }
        }
    }

    /// Состояние для админки (как stats() стратегии в Java).
    pub fn stats(&self, enabled: bool) -> Value {
        let rows: Vec<Value> = self.states.iter().map(|(sym, s)| json!({
            "symbol": sym,
            "bid": s.bid.map(|q| q.price), "ask": s.ask.map(|q| q.price),
            "inventory": s.inv, "avgCost": (s.inv > 0.0).then_some(s.avg),
            "trades": s.trades, "pnl": s.pnl,
            "book": s.thin.as_ref().map_or("ликвидный".to_owned(), |t| format!("тонкий: {t} — только продажа")),
            "market": s.market.as_ref().map_or("можно покупать".to_owned(), |m| format!("{m} — только продажа")),
            "topChangesPerSec": (s.top_per_sec * 10.0).round() / 10.0,
            "decision": s.decision,
            "bidQueue": queue_json(&s.q_bid), "askQueue": queue_json(&s.q_ask), "jumps": s.jumps,
            "jumpPauseSec": s.jump_pause_until.filter(|t| *t > Instant::now()).map(|t| (t - Instant::now()).as_secs()),
            "flow": s.flow.as_ref().map_or("сделки и спред в порядке".to_owned(), |f| format!("{f} — только продажа")),
            "tradeMinutes": s.trade_minutes.map(|n| format!("{n} из 5")),
            "minuteVolume": s.minute_volume.map(f64::round),
            "spreadPresencePercent": s.presence.map(f64::round),
        })).collect();
        json!({
            "enabled": enabled, "makerFeePercent": self.maker * 100.0,
            "trades": self.states.values().map(|s| s.trades).sum::<u64>(),
            "pnl": self.states.values().map(|s| s.pnl).sum::<f64>(),
            "symbols": rows,
        })
    }
}

fn queue_json(q: &Queue) -> Value {
    match q.status {
        None => Value::Null,
        Some(st) => {
            json!({"status": st, "level": q.level, "mine": 0.0, "others": q.others, "ahead": q.ahead, "outbid": q.outbid})
        }
    }
}

/// Своя заявка относительно лучшей цены: перебили / есть чужие / только я; «впереди» только убывает.
fn queue(q: &mut Queue, mine: Option<Quote>, bids: bool, best: f64, best_qty: f64) {
    let Some(m) = mine else {
        *q = Queue {
            outbid: q.outbid,
            ..Queue::default()
        };
        return;
    };
    let beaten = if bids { best > m.price } else { best < m.price };
    let others = if best == m.price { best_qty } else { 0.0 };
    let st = if beaten {
        "перебили"
    } else if others > 0.0 {
        "есть чужие"
    } else {
        "только я"
    };
    if st == "перебили" && q.status != Some("перебили") {
        q.outbid += 1;
    }
    q.status = Some(st);
    q.level = others;
    q.others = others;
    q.ahead = Some(q.ahead.map_or(others, |a| a.min(others)));
}

/// Смены лучшей цены за окно 10 с — скорость в секунду (много смен — конкуренты-боты).
fn count_top(s: &mut State, bid: f64, ask: f64, now: Instant) {
    if (bid, ask) != s.last_top {
        s.top_changes += 1;
        s.last_top = (bid, ask);
    }
    let start = *s.top_window.get_or_insert(now);
    let el = now.duration_since(start);
    if el >= Duration::from_secs(10) {
        s.top_per_sec = s.top_changes as f64 / el.as_secs_f64();
        s.top_changes = 0;
        s.top_window = Some(now);
    }
}

/// Падение цены за 15 минут и конкуренция; None — можно покупать.
fn market_issue(
    s: &mut State,
    p: &Params,
    mid: f64,
    now: Instant,
    spread_ok: bool,
) -> Option<String> {
    s.mids.push_back((now, mid, spread_ok));
    while s
        .mids
        .front()
        .is_some_and(|(t, _, _)| now.duration_since(*t) > Duration::from_secs(900))
    {
        s.mids.pop_front();
    }
    if p.max_drop_percent > 0.0 {
        let max = s.mids.iter().map(|x| x.1).fold(0.0, f64::max);
        let drop = if max > 0.0 {
            (max - mid) / max * 100.0
        } else {
            0.0
        };
        if drop > p.max_drop_percent {
            return Some(format!(
                "цена упала на {drop:.2}% за 15 мин (порог {:.2}%)",
                p.max_drop_percent
            ));
        }
    }
    if p.max_top_changes_per_sec > 0.0 && s.top_per_sec > p.max_top_changes_per_sec {
        return Some(format!(
            "лучшая цена меняется {:.1} раз/с > {} — много конкурентов",
            s.top_per_sec, p.max_top_changes_per_sec
        ));
    }
    None
}

/// Сделки по минутным свечам (идут каждую минуту, оборот) и постоянство спреда за 5 минут; None — в порядке.
/// Нет свечей — сделки не проверяются (как в Java: проверка только при данных).
fn flow_issue(s: &mut State, p: &Params, minutes: Option<&[f64]>, now: Instant) -> Option<String> {
    let mut issue = None;
    match minutes.filter(|v| !v.is_empty()) {
        Some(v) => {
            let last = &v[v.len().saturating_sub(5)..];
            let with = last.iter().filter(|x| **x > 0.0).count();
            let avg = last.iter().map(|x| x.max(0.0)).sum::<f64>() / last.len() as f64;
            s.trade_minutes = Some(with);
            s.minute_volume = Some(avg);
            let need = (p.min_trade_minutes.round() as usize).min(last.len());
            if p.min_trade_minutes > 0.0 && with < need {
                issue = Some(format!(
                    "сделки идут не каждую минуту: {with} из {} мин (порог {need})",
                    last.len()
                ));
            } else if p.min_minute_volume_multiple > 0.0
                && avg < p.min_minute_volume_multiple * p.order_quote
            {
                issue = Some(format!(
                    "мало сделок: оборот {avg:.0} в минуту < {:.0} ({} заявки)",
                    p.min_minute_volume_multiple * p.order_quote,
                    p.min_minute_volume_multiple
                ));
            }
        }
        None => {
            s.trade_minutes = None;
            s.minute_volume = None;
        }
    }
    let recent: Vec<bool> = s
        .mids
        .iter()
        .filter(|x| now.duration_since(x.0) <= Duration::from_secs(300))
        .map(|x| x.2)
        .collect();
    s.presence = (!recent.is_empty())
        .then(|| recent.iter().filter(|x| **x).count() as f64 * 100.0 / recent.len() as f64);
    let warmed = s
        .since
        .is_some_and(|t| now.duration_since(t) >= Duration::from_secs(60));
    if issue.is_none()
        && warmed
        && p.min_spread_presence_percent > 0.0
        && let Some(pr) = s.presence.filter(|pr| *pr < p.min_spread_presence_percent)
    {
        issue = Some(format!(
            "спред непостоянный: достаточный {pr:.0}% времени за 5 мин (порог {:.0}%)",
            p.min_spread_presence_percent
        ));
    }
    issue
}

/// Ликвиден ли стакан: в полосе от лучших цен не меньше уровней и глубины; None — ликвиден.
fn thin_book(b: &Book, p: &Params, bid: f64, ask: f64) -> Option<String> {
    if p.min_depth_multiple <= 0.0 && p.min_book_levels == 0 {
        return None;
    }
    let band = p.depth_band_percent / 100.0;
    let (lo, hi) = (bid * (1.0 - band), ask * (1.0 + band));
    let side = |asks: bool| {
        b.levels(asks)
            .take_while(|l| if asks { l.0 <= hi } else { l.0 >= lo })
            .fold((0usize, 0.0), |(n, d), l| (n + 1, d + l.0 * l.1))
    };
    let ((nb, db), (na, da)) = (side(false), side(true));
    if p.min_book_levels > 0 && nb.min(na) < p.min_book_levels {
        return Some(format!(
            "уровней в стакане {} < {}",
            nb.min(na),
            p.min_book_levels
        ));
    }
    let need = p.min_depth_multiple * p.order_quote;
    if p.min_depth_multiple > 0.0 && db.min(da) < need {
        return Some(format!(
            "глубина стакана {:.0} < {need:.0} ({} заявок)",
            db.min(da),
            p.min_depth_multiple
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lim() -> Limits {
        Limits {
            trading_enabled: true,
            max_order_quote: 1000.0,
            max_daily_loss_quote: 50.0,
            max_slippage_percent: 50.0,
            max_orders_per_minute: 10_000,
            max_data_age_ms: 5000,
        }
    }

    fn params() -> Params {
        let mut p = Params::from(&Map::new());
        p.requote = Duration::ZERO;
        p.order_quote = 20.0;
        p.min_edge_percent = 0.1;
        p.max_inventory_quote = 100.0;
        p
    }

    fn book(bid: f64, ask: f64, id: u64) -> Book {
        let mut b = Book::default();
        b.apply_snapshot(vec![(bid, 1000.0)], vec![(ask, 1000.0)], id);
        b
    }

    #[test]
    fn quote_buy_sell_profit() {
        let mut paper = Account::Paper(Paper::new(&["XYZUSDT".into()], 1000.0, 0.1, 0.1));
        let mut sc = SpreadCapture::new(0.1);
        let p = params();
        let t = Instant::now();
        let b = book(99.0, 101.0, 1);
        sc.step("XYZUSDT", Some(&b), &mut paper, &p, &lim(), t, None, None);
        let s = &sc.states["XYZUSDT"];
        assert_eq!(s.bid.map(|q| q.price), Some(99.0), "{}", s.decision);
        assert!(s.ask.is_none(), "без купленного продажи нет");
        // рынок прошёл через бид — покупка исполнилась
        let b2 = book(97.0, 98.5, 2);
        paper.match_resting("XYZUSDT", &b2, &lim());
        sc.step(
            "XYZUSDT",
            Some(&b2),
            &mut paper,
            &p,
            &lim(),
            t + Duration::from_millis(1),
            None,
            None,
        );
        let s = &sc.states["XYZUSDT"];
        assert!((s.inv - 20.0 / 100.0).abs() < 1e-9 && (s.avg - 99.0 * 1.001).abs() < 1e-9);
        assert!(
            s.ask.unwrap().price >= 99.0 * 1.001 * 1.002 - 1e-9,
            "продажа не ниже покупки с комиссиями"
        );
        // цена ушла вверх через аск — продажа с прибылью
        let b3 = book(100.5, 101.0, 3);
        sc.step(
            "XYZUSDT",
            Some(&b3),
            &mut paper,
            &p,
            &lim(),
            t + Duration::from_millis(2),
            None,
            None,
        );
        let ask = sc.states["XYZUSDT"].ask.unwrap().price;
        paper.match_resting("XYZUSDT", &book(ask + 1.0, ask + 1.5, 4), &lim());
        sc.step(
            "XYZUSDT",
            Some(&book(ask + 1.0, ask + 1.5, 4)),
            &mut paper,
            &p,
            &lim(),
            t + Duration::from_millis(3),
            None,
            None,
        );
        let s = &sc.states["XYZUSDT"];
        assert_eq!(s.trades, 1);
        assert!(s.pnl > 0.0 && s.inv == 0.0, "{s:?}");
    }

    #[test]
    fn narrow_spread_pulls_quotes_and_stop_dumps() {
        let mut paper = Account::Paper(Paper::new(&["XYZUSDT".into()], 1000.0, 0.1, 0.1));
        let mut sc = SpreadCapture::new(0.1);
        let p = params();
        let t = Instant::now();
        sc.step(
            "XYZUSDT",
            Some(&book(99.0, 101.0, 1)),
            &mut paper,
            &p,
            &lim(),
            t,
            None,
            None,
        );
        assert_eq!(paper.open_orders(), 1);
        sc.step(
            "XYZUSDT",
            Some(&book(99.95, 100.0, 2)),
            &mut paper,
            &p,
            &lim(),
            t + Duration::from_millis(1),
            None,
            None,
        );
        assert_eq!(
            paper.open_orders(),
            0,
            "спред меньше двух комиссий — заявки сняты"
        );
        // купили, затем цена упала ниже стопа — продано рыночным
        sc.step(
            "XYZUSDT",
            Some(&book(99.0, 101.0, 3)),
            &mut paper,
            &p,
            &lim(),
            t + Duration::from_millis(2),
            None,
            None,
        );
        paper.match_resting("XYZUSDT", &book(97.0, 98.0, 4), &lim());
        sc.step(
            "XYZUSDT",
            Some(&book(90.0, 92.0, 5)),
            &mut paper,
            &p,
            &lim(),
            t + Duration::from_millis(3),
            None,
            None,
        );
        let s = &sc.states["XYZUSDT"];
        assert_eq!(s.inv, 0.0);
        assert!(s.pnl < 0.0 && s.trades == 1);
        assert_eq!(paper.free("XYZ"), 0.0);
    }

    #[test]
    fn thin_book_blocks_buys() {
        let mut paper = Account::Paper(Paper::new(&["XYZUSDT".into()], 1000.0, 0.1, 0.1));
        let mut sc = SpreadCapture::new(0.1);
        let mut p = params();
        p.min_book_levels = 3;
        sc.step(
            "XYZUSDT",
            Some(&book(99.0, 101.0, 1)),
            &mut paper,
            &p,
            &lim(),
            Instant::now(),
            None,
            None,
        );
        assert_eq!(paper.open_orders(), 0);
        assert!(sc.states["XYZUSDT"].decision.contains("уровней"));
    }

    fn filt() -> Option<Filter> {
        Some(Filter {
            tick: 0.01,
            step: 0.0001,
            min_qty: 0.0001,
            min_notional: 1.0,
        })
    }

    #[test]
    fn jump_one_tick_and_price_war_pause() {
        let mut paper = Account::Paper(Paper::new(&["XYZUSDT".into()], 1000.0, 0.1, 0.1));
        paper.set_filters(&HashMap::from([("XYZUSDT".to_owned(), filt().unwrap())]));
        let mut sc = SpreadCapture::new(0.1);
        let mut p = params();
        p.jump_ticks = 1.0;
        p.max_jumps_per_min = 3;
        let t = Instant::now();
        sc.step(
            "XYZUSDT",
            Some(&book(99.0, 101.0, 1)),
            &mut paper,
            &p,
            &lim(),
            t,
            None,
            filt(),
        );
        assert_eq!(
            sc.states["XYZUSDT"].bid.map(|q| q.price),
            Some(99.01),
            "покупка на тик выше чужой лучшей"
        );
        // чужая лучшая цена каждый раз выше нашей — перебиваем, пока не началась война
        for i in 0..5u64 {
            let our = sc.states["XYZUSDT"].bid.unwrap().price;
            sc.step(
                "XYZUSDT",
                Some(&book(our + 0.01, 101.0, 2 + i)),
                &mut paper,
                &p,
                &lim(),
                t + Duration::from_millis(1 + i),
                None,
                filt(),
            );
        }
        let s = &sc.states["XYZUSDT"];
        assert!(
            s.jump_pause_until.is_some_and(|u| u > t),
            "ценовая война — пауза"
        );
        let before = s.bid.unwrap().price;
        sc.step(
            "XYZUSDT",
            Some(&book(before + 0.01, 101.0, 20)),
            &mut paper,
            &p,
            &lim(),
            t + Duration::from_millis(10),
            None,
            filt(),
        );
        assert!(
            (sc.states["XYZUSDT"].bid.unwrap().price - (before + 0.01)).abs() < 1e-9,
            "в паузе встаёт в очередь, не перебивает"
        );
        // узкий спред — перебивать нельзя: встаёт по лучшей цене
        let mut sc2 = SpreadCapture::new(0.1);
        let mut paper2 = Account::Paper(Paper::new(&["XYZUSDT".into()], 1000.0, 0.1, 0.1));
        sc2.step(
            "XYZUSDT",
            Some(&book(100.0, 100.31, 1)),
            &mut paper2,
            &p,
            &lim(),
            t,
            None,
            filt(),
        );
        assert_eq!(sc2.states["XYZUSDT"].bid.map(|q| q.price), Some(100.0));
    }

    #[test]
    fn minute_trades_check() {
        let mut paper = Account::Paper(Paper::new(&["XYZUSDT".into()], 1000.0, 0.1, 0.1));
        let mut sc = SpreadCapture::new(0.1);
        let mut p = params();
        p.min_trade_minutes = 4.0;
        p.min_minute_volume_multiple = 2.0;
        let t = Instant::now();
        sc.step(
            "XYZUSDT",
            Some(&book(99.0, 101.0, 1)),
            &mut paper,
            &p,
            &lim(),
            t,
            Some(&[100.0, 0.0, 0.0, 100.0, 100.0]),
            None,
        );
        assert!(
            sc.states["XYZUSDT"].decision.contains("не каждую минуту"),
            "{}",
            sc.states["XYZUSDT"].decision
        );
        assert_eq!(paper.open_orders(), 0);
        sc.step(
            "XYZUSDT",
            Some(&book(99.0, 101.0, 2)),
            &mut paper,
            &p,
            &lim(),
            t + Duration::from_millis(1),
            Some(&[10.0; 5]),
            None,
        );
        assert!(sc.states["XYZUSDT"].decision.contains("мало сделок"));
        sc.step(
            "XYZUSDT",
            Some(&book(99.0, 101.0, 3)),
            &mut paper,
            &p,
            &lim(),
            t + Duration::from_millis(2),
            Some(&[100.0; 5]),
            None,
        );
        assert_eq!(sc.states["XYZUSDT"].decision, "покупаем и продаём");
        assert_eq!(sc.states["XYZUSDT"].trade_minutes, Some(5));
    }
}
