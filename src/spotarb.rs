//! Межбиржевой арбитраж на споте с переводом монет.
//!
//! Лучший бид монеты на бирже A выше лучшего аска на бирже B — круг выгоден, если разница окупает две комиссии
//! тейкера (и комиссию сети, если монету надо везти). Два режима:
//!
//! - **С запасом** (как в Java): монета уже есть на A — одновременно продаём на A и покупаем на B. Риска цены нет,
//!   монета просто «переехала» с A на B. Исполнилась одна нога — она откатывается на своей бирже.
//! - **С переводом** (`spotArbTransfer`): монеты на A нет — покупаем на B, выводим на A по самой дешёвой сети, общей
//!   для обеих бирж (вывод открыт на B, депозит — на A, минимум комиссии вывода), и продаём на A, когда монета пришла.
//!   Комиссия сети входит в расчёт выгоды. Пока монета в пути (минуты), цена может уйти — это риск режима.
//!
//! Бумага: перевод — со счёта на счёт через `spotArbPaperTransferMinutes`, комиссия — `spotArbPaperWithdrawFeeQuote`.
//! Биржа: реальный вывод только при `spotArbLiveWithdraw` (нужны ключи с правом вывода и адреса в белом списке биржи);
//! приход замечается по росту баланса монеты на A, не дольше `spotArbTransferTimeoutMinutes`.

use crate::account::Account;
use crate::book::Book;
use crate::live::Cmd;
use crate::paper::{Amount, Limits, Side, split};
use crate::transfer::Net;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// Параметры из глобальных настроек.
#[derive(Clone, Debug)]
pub struct Params {
    pub enabled: bool,
    pub check: Duration,
    pub min_profit_percent: f64,
    pub order_quote: f64,
    pub max_book_age_ms: u64,
    pub depth_usage: f64,
    pub cooldown: Duration,
    pub symbols: String,
    pub transfer: bool,
    pub paper_transfer: Duration,
    pub paper_fee_quote: f64,
    pub live_withdraw: bool,
    pub transfer_timeout: Duration,
    pub max_transfers: usize,
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
        let b = |k: &str, d: bool| {
            p.get(k)
                .and_then(|v| {
                    v.as_bool()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                })
                .unwrap_or(d)
        };
        Params {
            enabled: b("spotArbEnabled", true),
            check: Duration::from_millis(f("spotArbCheckMs", 200.0) as u64),
            min_profit_percent: f("spotArbMinProfitPercent", 0.05),
            order_quote: f("spotArbOrderQuote", 50.0),
            max_book_age_ms: f("spotArbMaxBookAgeMs", 1000.0) as u64,
            depth_usage: f("spotArbDepthUsage", 0.5),
            cooldown: Duration::from_millis(f("spotArbCooldownMs", 1000.0) as u64),
            symbols: p
                .get("spotArbSymbols")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_uppercase(),
            transfer: b("spotArbTransfer", true),
            paper_transfer: Duration::from_secs_f64(f("spotArbPaperTransferMinutes", 10.0) * 60.0),
            paper_fee_quote: f("spotArbPaperWithdrawFeeQuote", 1.0),
            live_withdraw: b("spotArbLiveWithdraw", false),
            transfer_timeout: Duration::from_secs_f64(
                f("spotArbTransferTimeoutMinutes", 120.0) * 60.0,
            ),
            max_transfers: f("spotArbMaxTransfers", 3.0) as usize,
        }
    }

    fn allows(&self, sym: &str, coin: &str) -> bool {
        self.symbols.trim().is_empty()
            || self
                .symbols
                .split(',')
                .any(|s| s.trim() == sym || s.trim() == coin)
    }
}

/// Биржа для стратегии: счёт, торговля включена, стаканы выбранных монет.
pub struct Venue<'a> {
    pub account: &'a mut Account,
    pub trading: bool,
    pub lim: Limits,
    pub books: HashMap<String, Book>,
}

/// Стадия круга.
#[derive(Debug, Clone, PartialEq)]
enum Stage {
    /// С запасом: продажа на A и покупка на B отправлены.
    Hedged,
    /// Откат одной исполнившейся ноги.
    Rollback,
    /// С переводом: покупка на B.
    Buying,
    /// Узнаём адрес депозита на A.
    Address,
    /// Вывод отправлен (живой счёт ждёт номер).
    Withdrawing {
        tid: u64,
    },
    /// Монета в пути; baseline — баланс монеты на A до вывода (живой счёт).
    Transit {
        since: Instant,
        eta: Option<Instant>,
        baseline: f64,
        avg: f64,
    },
    /// Продажа на A.
    Selling,
    Done,
    Failed(String),
}

#[derive(Debug, Clone)]
struct Round {
    id: u64,
    coin: String,
    symbol: String,
    sell_ex: String,
    buy_ex: String,
    stage: Stage,
    qty: f64,
    /// Сеть перевода (имя у источника, имя у получателя) и комиссия сети в монете.
    network: Option<(String, String, f64)>,
    /// Количество по биржам со знаком (из исполнений своих ордеров) и денежный поток.
    held: HashMap<String, f64>,
    cash: f64,
    pending: Vec<(String, u64)>,
    started: Instant,
    note: String,
}

impl Round {
    fn h(&self, ex: &str) -> f64 {
        self.held.get(ex).copied().unwrap_or(0.0)
    }
}

/// Возможность для админки.
#[derive(Debug, Clone)]
struct Opportunity {
    coin: String,
    sell: String,
    buy: String,
    spread: f64,
    fees: f64,
    network_fee: f64,
    network: String,
    expected: f64,
    coin_on_sell: f64,
    money_on_buy: f64,
    mode: &'static str,
}

/// Стратегия (одна на процесс).
#[derive(Debug, Default)]
pub struct SpotArb {
    rounds: Vec<Round>,
    next: u64,
    /// Свои ордера: (биржа, номер) → круг.
    own: HashMap<(String, u64), u64>,
    seen: HashMap<String, u64>,
    cooldown: HashMap<String, Instant>,
    last_check: Option<Instant>,
    opportunities: Vec<Opportunity>,
    pub completed: u64,
    pub failed: u64,
    pub pnl: f64,
}

fn pct(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

impl SpotArb {
    /// Учесть исполнения своих ордеров на всех биржах.
    fn absorb(&mut self, venues: &mut HashMap<String, Venue>) {
        for (ex, v) in venues.iter() {
            let from = self.seen.get(ex).copied().unwrap_or(0);
            let mut last = from;
            for e in v.account.execs().iter().filter(|e| e.seq > from) {
                last = e.seq;
                let Some(rid) = self.own.get(&(ex.clone(), e.order_id)) else {
                    continue;
                };
                let Some(r) = self.rounds.iter_mut().find(|r| r.id == *rid) else {
                    continue;
                };
                let sign = if e.side == Side::Buy { 1.0 } else { -1.0 };
                *r.held.entry(ex.clone()).or_default() += sign * e.qty;
                r.cash += -sign * e.qty * e.price - e.fee;
            }
            self.seen.insert(ex.clone(), last);
        }
    }

    fn track(&mut self, rid: u64, ex: &str, id: u64) {
        self.own.insert((ex.to_owned(), id), rid);
        if let Some(r) = self.rounds.iter_mut().find(|r| r.id == rid) {
            r.pending.push((ex.to_owned(), id));
        }
    }

    /// Сеть перевода монеты с from на to и комиссия в монете: бумага — условная, биржа — самая дешёвая общая
    /// (сети запрашиваются у бирж; пока не пришли — None).
    fn network(
        venues: &mut HashMap<String, Venue>,
        coin: &str,
        from: &str,
        to: &str,
        price: f64,
        p: &Params,
    ) -> Option<Net> {
        let paper = |v: &mut Venue| v.account.live().is_none();
        let (fp, tp) = (paper(venues.get_mut(from)?), paper(venues.get_mut(to)?));
        if fp && tp {
            let fee = if price > 0.0 {
                p.paper_fee_quote / price
            } else {
                0.0
            };
            return Some(Net {
                name: "PAPER".into(),
                raw: "PAPER".into(),
                withdraw_fee: fee,
                withdraw_min: 0.0,
                withdraw: true,
                deposit: true,
            });
        }
        if fp != tp || !p.live_withdraw {
            return None; // между бумагой и биржей не перевести; реальный вывод выключен
        }
        let mut nets = Vec::new();
        for ex in [from, to] {
            let l = venues.get_mut(ex)?.account.live()?;
            match l.networks.get(coin) {
                Some(Ok(n)) => nets.push(n.clone()),
                Some(Err(_)) => return None,
                None => {
                    l.networks.insert(coin.to_owned(), Err("запрошено".into()));
                    l.request(Cmd::Networks {
                        coin: coin.to_owned(),
                    });
                    return None;
                }
            }
        }
        let (n, dest) = crate::transfer::cheapest(&nets[0], &nets[1])?;
        Some(Net {
            raw: format!("{}>{dest}", n.raw),
            ..n.clone()
        })
    }

    /// Шаг: продвинуть начатые круги и (раз в spotArbCheckMs) искать новые.
    pub fn step(&mut self, venues: &mut HashMap<String, Venue>, p: &Params, now: Instant) {
        self.progress(venues, p, now);
        if self
            .last_check
            .is_some_and(|t| now.duration_since(t) < p.check)
        {
            return;
        }
        self.last_check = Some(now);
        let before = self.next;
        self.scan(venues, p, now);
        if self.next != before {
            self.progress(venues, p, now); // новый круг — сразу дальше (бумажная покупка уже исполнилась)
        }
    }

    /// Продвинуть круги и подвести итог закончившихся.
    fn progress(&mut self, venues: &mut HashMap<String, Venue>, p: &Params, now: Instant) {
        self.absorb(venues);
        let ids: Vec<u64> = self.rounds.iter().map(|r| r.id).collect();
        for id in ids {
            self.advance(id, venues, p, now);
        }
        self.absorb(venues);
        let (done, live): (Vec<Round>, Vec<Round>) =
            std::mem::take(&mut self.rounds).into_iter().partition(|r| {
                matches!(r.stage, Stage::Done | Stage::Failed(_)) && r.pending.is_empty()
            });
        self.rounds = live;
        for r in done {
            self.own.retain(|_, v| *v != r.id);
            match &r.stage {
                Stage::Failed(why) => {
                    self.failed += 1;
                    tracing::warn!(
                        "[spot-arb] круг {} {}→{} не удался: {why}",
                        r.coin,
                        r.buy_ex,
                        r.sell_ex
                    );
                }
                _ => {
                    self.completed += 1;
                    tracing::info!(
                        "[spot-arb] круг {} {}→{} закрыт, результат {:.4}",
                        r.coin,
                        r.buy_ex,
                        r.sell_ex,
                        r.cash
                    );
                }
            }
            self.pnl += r.cash;
        }
    }

    fn scan(&mut self, venues: &mut HashMap<String, Venue>, p: &Params, now: Instant) {
        self.opportunities.clear();
        let mut syms: HashMap<String, Vec<String>> = HashMap::new();
        for (ex, v) in venues.iter() {
            if v.account.is_perp() {
                continue;
            }
            for s in v.books.keys() {
                syms.entry(s.clone()).or_default().push(ex.clone());
            }
        }
        let busy: HashSet<String> = self.rounds.iter().map(|r| r.coin.clone()).collect();
        let mut syms: Vec<(String, Vec<String>)> =
            syms.into_iter().filter(|(_, e)| e.len() >= 2).collect();
        syms.sort();
        for (sym, exs) in syms {
            let Some((coin, quote)) = split(&sym) else {
                continue;
            };
            if !p.allows(&sym, &coin) {
                continue;
            }
            for sell in &exs {
                for buy in &exs {
                    if sell == buy {
                        continue;
                    }
                    let fresh = |v: &Venue| {
                        v.books
                            .get(&sym)
                            .filter(|b| {
                                b.is_ready() && b.age_ms().is_some_and(|a| a <= p.max_book_age_ms)
                            })
                            .cloned()
                    };
                    let (Some(ba), Some(bb)) = (fresh(&venues[sell]), fresh(&venues[buy])) else {
                        continue;
                    };
                    let (Some((bid, bid_q)), Some((ask, ask_q))) = (ba.best_bid(), bb.best_ask())
                    else {
                        continue;
                    };
                    if bid <= ask {
                        continue;
                    }
                    let mid = (bid + ask) / 2.0;
                    let spread = (bid - ask) / mid * 100.0;
                    let fees = (venues[sell].account.taker_fee() + venues[buy].account.taker_fee())
                        * 100.0;
                    let coin_on_sell = venues[sell].account.free(&coin);
                    let money_on_buy = venues[buy].account.free(&quote);
                    let mut qty = (p.order_quote / ask)
                        .min(p.depth_usage * bid_q.min(ask_q))
                        .min(money_on_buy / (ask * 1.01));
                    let hedged = coin_on_sell >= qty && qty > 0.0;
                    let net = if hedged || !p.transfer {
                        None
                    } else {
                        Self::network(venues, &coin, buy, sell, ask, p)
                    };
                    if hedged {
                        qty = qty.min(coin_on_sell);
                    }
                    let network_fee = net.as_ref().map_or(0.0, |n| {
                        n.withdraw_fee * ask / (qty * ask).max(1e-12) * 100.0
                    });
                    let expected = spread - fees - network_fee;
                    let mode = if hedged {
                        "с запасом"
                    } else if net.is_some() {
                        "с переводом"
                    } else {
                        "нет монеты на бирже продажи"
                    };
                    self.opportunities.push(Opportunity {
                        coin: coin.clone(),
                        sell: format!("{sell}:{sym}"),
                        buy: format!("{buy}:{sym}"),
                        spread: pct(spread),
                        fees: pct(fees),
                        network_fee: pct(network_fee),
                        network: net.as_ref().map(|n| n.raw.clone()).unwrap_or_default(),
                        expected: pct(expected),
                        coin_on_sell,
                        money_on_buy,
                        mode,
                    });
                    let ready = p.enabled
                        && venues[sell].trading
                        && venues[buy].trading
                        && expected >= p.min_profit_percent;
                    let cooled = self.cooldown.get(&coin).is_none_or(|t| now >= *t);
                    if !ready || !cooled || busy.contains(&coin) || qty <= 0.0 {
                        continue;
                    }
                    if !hedged
                        && (net.is_none()
                            || self.rounds.iter().filter(|r| r.network.is_some()).count()
                                >= p.max_transfers)
                    {
                        continue;
                    }
                    if let Some(n) = &net
                        && qty < n.withdraw_min + n.withdraw_fee
                    {
                        continue;
                    }
                    self.start(
                        &coin, &sym, sell, buy, qty, hedged, net, spread, venues, now,
                    );
                    self.cooldown.insert(coin.clone(), now + p.cooldown);
                    return; // один круг за проверку
                }
            }
        }
        self.opportunities
            .sort_by(|a, b| b.expected.total_cmp(&a.expected));
        self.opportunities.truncate(20);
    }

    #[allow(clippy::too_many_arguments)]
    fn start(
        &mut self,
        coin: &str,
        sym: &str,
        sell: &str,
        buy: &str,
        qty: f64,
        hedged: bool,
        net: Option<Net>,
        spread: f64,
        venues: &mut HashMap<String, Venue>,
        now: Instant,
    ) {
        self.next += 1;
        let id = self.next;
        let network = net.map(|n| {
            let (from, to) = n
                .raw
                .split_once('>')
                .map(|(a, b)| (a.to_owned(), b.to_owned()))
                .unwrap_or((n.raw.clone(), n.raw.clone()));
            (from, to, n.withdraw_fee)
        });
        tracing::info!(
            "[spot-arb] {coin}: разница {spread:.3}% — {} {qty} на {buy}, продажа на {sell}{}",
            "покупка",
            network
                .as_ref()
                .map(|n| format!(" после перевода по {} (комиссия {} {coin})", n.0, n.2))
                .unwrap_or(" сразу (запас есть)".into())
        );
        self.rounds.push(Round {
            id,
            coin: coin.to_owned(),
            symbol: sym.to_owned(),
            sell_ex: sell.to_owned(),
            buy_ex: buy.to_owned(),
            stage: if hedged { Stage::Hedged } else { Stage::Buying },
            qty,
            network,
            held: HashMap::new(),
            cash: 0.0,
            pending: Vec::new(),
            started: now,
            note: String::new(),
        });
        let v = venues.get_mut(buy).unwrap();
        let book = v.books.get(sym).cloned();
        match v.account.order(
            sym,
            Side::Buy,
            Amount::Qty(qty),
            None,
            book.as_ref(),
            &v.lim,
        ) {
            Ok(f) => self.track(id, buy, f.order_id),
            Err(e) => self.fail(id, format!("покупка на {buy}: {e}")),
        }
        if hedged {
            let v = venues.get_mut(sell).unwrap();
            let book = v.books.get(sym).cloned();
            match v.account.order(
                sym,
                Side::Sell,
                Amount::Qty(qty),
                None,
                book.as_ref(),
                &v.lim,
            ) {
                Ok(f) => self.track(id, sell, f.order_id),
                Err(e) => self.note(id, format!("продажа на {sell}: {e}")),
            }
        }
    }

    fn fail(&mut self, id: u64, why: String) {
        if let Some(r) = self.rounds.iter_mut().find(|r| r.id == id) {
            r.stage = Stage::Failed(why);
        }
    }

    fn note(&mut self, id: u64, why: String) {
        if let Some(r) = self.rounds.iter_mut().find(|r| r.id == id) {
            tracing::warn!("[spot-arb] {}: {why}", r.coin);
            r.note = why;
        }
    }

    fn advance(&mut self, id: u64, venues: &mut HashMap<String, Venue>, p: &Params, now: Instant) {
        let Some(r) = self.rounds.iter_mut().find(|r| r.id == id) else {
            return;
        };
        r.pending
            .retain(|(ex, oid)| venues.get(ex).is_some_and(|v| v.account.is_open(*oid)));
        if !r.pending.is_empty() {
            return;
        }
        let r = r.clone();
        let (buy, sell, sym, coin) = (
            r.buy_ex.clone(),
            r.sell_ex.clone(),
            r.symbol.clone(),
            r.coin.clone(),
        );
        let set = |s: &mut Self, st: Stage| {
            if let Some(x) = s.rounds.iter_mut().find(|x| x.id == id) {
                x.stage = st;
            }
        };
        match r.stage.clone() {
            Stage::Hedged => {
                let (b, s) = (r.h(&buy), -r.h(&sell));
                if (b - s).abs() <= r.qty * 1e-6 {
                    set(self, Stage::Done);
                    return;
                }
                // исполнилась одна нога (или не целиком) — откатить разницу на её бирже
                tracing::warn!(
                    "[spot-arb] {coin}: ноги разошлись (куплено {b}, продано {s}) — откат"
                );
                set(self, Stage::Rollback);
                let (ex, side, q) = if b > s {
                    (buy, Side::Sell, b - s)
                } else {
                    (sell, Side::Buy, s - b)
                };
                let v = venues.get_mut(&ex).unwrap();
                let book = v.books.get(&sym).cloned();
                let res = if side == Side::Sell {
                    v.account.exit_market(&sym, side, q, book.as_ref())
                } else {
                    v.account
                        .order(&sym, side, Amount::Qty(q), None, book.as_ref(), &v.lim)
                };
                match res {
                    Ok(f) => self.track(id, &ex, f.order_id),
                    Err(e) => set(self, Stage::Failed(format!("откат на {ex}: {e}"))),
                }
            }
            Stage::Rollback => set(self, Stage::Failed("ноги разошлись, откат выполнен".into())),
            Stage::Buying => {
                if r.h(&buy) <= 0.0 {
                    set(
                        self,
                        Stage::Failed(format!("покупка на {buy} не исполнилась")),
                    );
                } else {
                    set(self, Stage::Address);
                    self.advance(id, venues, p, now);
                }
            }
            Stage::Address => {
                let (from_net, to_net, fee) = r.network.clone().unwrap();
                let amount = r.h(&buy);
                if !venues[&buy].account.is_live() {
                    // бумага: со счёта на счёт через заданное время
                    let res = venues
                        .get_mut(&buy)
                        .unwrap()
                        .account
                        .paper()
                        .unwrap()
                        .withdraw(&coin, amount);
                    match res {
                        Ok(avg) => {
                            if let Some(x) = self.rounds.iter_mut().find(|x| x.id == id) {
                                *x.held.entry(buy.clone()).or_default() -= amount;
                                x.qty = (amount - fee).max(0.0);
                                x.stage = Stage::Transit {
                                    since: now,
                                    eta: Some(now + p.paper_transfer),
                                    baseline: 0.0,
                                    avg,
                                };
                            }
                        }
                        Err(e) => set(self, Stage::Failed(e)),
                    }
                    return;
                }
                let dest = venues.get_mut(&sell).unwrap().account.live().unwrap();
                let key = (coin.clone(), to_net.clone());
                match dest.addresses.get(&key).cloned() {
                    None => {
                        dest.addresses.insert(key, Err("запрошено".into()));
                        dest.request(Cmd::Address {
                            coin: coin.clone(),
                            network: to_net,
                        });
                    }
                    Some(Err(e)) if e == "запрошено" => {}
                    Some(Err(e)) => set(
                        self,
                        Stage::Failed(format!("адрес депозита на {sell}: {e}")),
                    ),
                    Some(Ok((addr, tag))) => {
                        let baseline = venues[&sell].account.free(&coin);
                        let tid = id;
                        let src = venues.get_mut(&buy).unwrap().account.live().unwrap();
                        src.withdrawals.remove(&tid);
                        src.request(Cmd::Withdraw {
                            tid,
                            coin: coin.clone(),
                            network: from_net,
                            address: addr,
                            tag,
                            amount,
                        });
                        if let Some(x) = self.rounds.iter_mut().find(|x| x.id == id) {
                            x.qty = (amount - fee).max(0.0);
                            x.note = format!("баланс {coin} на {sell} до перевода: {baseline}");
                            x.stage = Stage::Withdrawing { tid };
                            x.held.insert(format!("{sell}#baseline"), baseline);
                        }
                    }
                }
            }
            Stage::Withdrawing { tid } => {
                let src = venues.get_mut(&buy).unwrap().account.live().unwrap();
                match src.withdrawals.get(&tid).cloned() {
                    None => {}
                    Some(Err(e)) => set(
                        self,
                        Stage::Failed(format!("вывод с {buy}: {e} (монета осталась на {buy})")),
                    ),
                    Some(Ok(wid)) => {
                        let baseline = r.h(&format!("{sell}#baseline"));
                        if let Some(x) = self.rounds.iter_mut().find(|x| x.id == id) {
                            *x.held.entry(buy.clone()).or_default() -=
                                x.qty + x.network.as_ref().map_or(0.0, |n| n.2);
                            x.note = format!("вывод {wid}");
                            x.stage = Stage::Transit {
                                since: now,
                                eta: None,
                                baseline,
                                avg: 0.0,
                            };
                        }
                    }
                }
            }
            Stage::Transit {
                since,
                eta,
                baseline,
                avg,
            } => {
                let arrived = match eta {
                    Some(t) => now >= t,
                    None => venues[&sell].account.free(&coin) >= baseline + r.qty * 0.98,
                };
                if !arrived {
                    if eta.is_none() && now.duration_since(since) > p.transfer_timeout {
                        set(
                            self,
                            Stage::Failed(format!(
                                "перевод {coin} на {sell} не пришёл за {} мин — проверьте вывод на бирже",
                                p.transfer_timeout.as_secs() / 60
                            )),
                        );
                    }
                    return;
                }
                if let Some(paper) = venues.get_mut(&sell).unwrap().account.paper() {
                    paper.deposit(&coin, r.qty, avg);
                }
                if let Some(x) = self.rounds.iter_mut().find(|x| x.id == id) {
                    *x.held.entry(sell.clone()).or_default() += r.qty;
                    x.stage = Stage::Selling;
                }
                tracing::info!(
                    "[spot-arb] {coin}: перевод на {sell} пришёл ({}) — продаю",
                    r.qty
                );
                let v = venues.get_mut(&sell).unwrap();
                let book = v.books.get(&sym).cloned();
                match v
                    .account
                    .exit_market(&sym, Side::Sell, r.qty, book.as_ref())
                {
                    Ok(f) => self.track(id, &sell, f.order_id),
                    Err(e) => self.note(id, format!("продажа на {sell}: {e} — повтор")),
                }
            }
            Stage::Selling => {
                let left = r.h(&sell);
                if left <= r.qty * 1e-6 {
                    set(self, Stage::Done);
                    return;
                }
                // продажа не прошла или исполнилась не целиком — продать остаток
                let v = venues.get_mut(&sell).unwrap();
                let book = v.books.get(&sym).cloned();
                match v.account.exit_market(&sym, Side::Sell, left, book.as_ref()) {
                    Ok(f) => self.track(id, &sell, f.order_id),
                    Err(e) => set(
                        self,
                        Stage::Failed(format!(
                            "продажа {left} {coin} на {sell}: {e} — монета осталась на {sell}"
                        )),
                    ),
                }
            }
            Stage::Done | Stage::Failed(_) => {}
        }
    }

    /// Состояние для /arbitrage (spotPrice), как в Java, плюс переводы.
    pub fn stats(&self, p: &Params) -> Value {
        let now = Instant::now();
        let opp: Vec<Value> = self
            .opportunities
            .iter()
            .map(|o| {
                json!({"coin": o.coin, "sell": o.sell, "buy": o.buy, "spreadPercent": o.spread, "roundTripFeePercent": o.fees,
                    "networkFeePercent": o.network_fee, "network": o.network, "expectedPercent": o.expected,
                    "coinOnSellExchange": o.coin_on_sell, "moneyOnBuyExchange": o.money_on_buy, "mode": o.mode,
                    "enough": o.expected >= p.min_profit_percent})
            })
            .collect();
        let rounds: Vec<Value> = self
            .rounds
            .iter()
            .map(|r| {
                let stage = match &r.stage {
                    Stage::Hedged => "сделки на обеих биржах".to_owned(),
                    Stage::Rollback => "откат ноги".into(),
                    Stage::Buying => format!("покупка на {}", r.buy_ex),
                    Stage::Address => format!("адрес депозита на {}", r.sell_ex),
                    Stage::Withdrawing { .. } => format!("вывод с {}", r.buy_ex),
                    Stage::Transit { eta, .. } => match eta {
                        Some(t) => format!("в пути, ещё {} с", t.saturating_duration_since(now).as_secs()),
                        None => format!("в пути на {}", r.sell_ex),
                    },
                    Stage::Selling => format!("продажа на {}", r.sell_ex),
                    Stage::Done => "готово".into(),
                    Stage::Failed(w) => format!("ошибка: {w}"),
                };
                json!({"coin": r.coin, "buy": r.buy_ex, "sell": r.sell_ex, "qty": r.qty, "stage": stage,
                    "network": r.network.as_ref().map(|n| n.0.clone()), "networkFee": r.network.as_ref().map(|n| n.2),
                    "seconds": now.duration_since(r.started).as_secs(), "note": r.note})
            })
            .collect();
        json!({"enabled": p.enabled, "rounds": self.completed, "failed": self.failed, "pnl": self.pnl,
            "transferMode": p.transfer, "liveWithdraw": p.live_withdraw, "inProgress": rounds, "opportunities": opp})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paper::Paper;

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

    fn params(extra: &[(&str, Value)]) -> Params {
        let mut m = Map::new();
        m.insert("spotArbOrderQuote".into(), json!(100.0));
        m.insert("spotArbMaxBookAgeMs".into(), json!(60_000));
        m.insert("spotArbPaperTransferMinutes".into(), json!(1.0));
        for (k, v) in extra {
            m.insert((*k).into(), v.clone());
        }
        Params::from(&m)
    }

    fn run(
        accounts: &mut HashMap<String, Account>,
        books: &[(&str, f64, f64)],
        sa: &mut SpotArb,
        p: &Params,
        now: Instant,
    ) {
        let mut venues: HashMap<String, Venue> = accounts
            .iter_mut()
            .map(|(ex, a)| {
                let (b, k) = books
                    .iter()
                    .find(|x| x.0 == ex)
                    .map(|x| (x.1, x.2))
                    .unwrap();
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
        sa.step(&mut venues, p, now);
    }

    fn accounts(coin_on_a: f64) -> HashMap<String, Account> {
        let mut a = Paper::new(&["XUSDT".into()], 1000.0, 0.1, 0.1);
        if coin_on_a > 0.0 {
            a.set_balance("X", coin_on_a);
        }
        let b = Paper::new(&["XUSDT".into()], 1000.0, 0.1, 0.1);
        HashMap::from([
            ("a".to_string(), Account::Paper(a)),
            ("b".to_string(), Account::Paper(b)),
        ])
    }

    #[test]
    fn transfer_round_buy_move_sell() {
        let mut acc = accounts(0.0);
        let mut sa = SpotArb::default();
        let p = params(&[("spotArbCooldownMs", json!(600_000))]); // один круг
        let t0 = Instant::now();
        // на a дороже на 3%: монеты на a нет — покупка на b, перевод на a (1 мин), продажа на a
        run(
            &mut acc,
            &[("a", 10.3, 10.31), ("b", 9.99, 10.0)],
            &mut sa,
            &p,
            t0,
        );
        let st = sa.stats(&p);
        assert!(
            st["inProgress"][0]["stage"]
                .as_str()
                .unwrap()
                .starts_with("в пути"),
            "{st}"
        );
        assert_eq!(st["opportunities"][0]["mode"], json!("с переводом"));
        assert!(acc["b"].free("X") < 1e-9, "монета ушла с b");
        run(
            &mut acc,
            &[("a", 10.3, 10.31), ("b", 9.99, 10.0)],
            &mut sa,
            &p,
            t0 + Duration::from_secs(30),
        );
        assert_eq!(sa.completed, 0);
        run(
            &mut acc,
            &[("a", 10.3, 10.31), ("b", 9.99, 10.0)],
            &mut sa,
            &p,
            t0 + Duration::from_secs(61),
        );
        run(
            &mut acc,
            &[("a", 10.3, 10.31), ("b", 9.99, 10.0)],
            &mut sa,
            &p,
            t0 + Duration::from_secs(62),
        );
        assert_eq!(sa.completed, 1, "{}", sa.stats(&p));
        // 10 X × (10.3 − 10.0) = 3 − комиссии 0.1%×(100+~102) − сеть 1 USDT ≈ 1.8
        assert!(sa.pnl > 1.5 && sa.pnl < 2.0, "{}", sa.pnl);
        assert!(acc["a"].free("X") < 1e-9);
        assert!((acc["a"].free("USDT") + acc["b"].free("USDT") - 2000.0 - sa.pnl).abs() < 1e-6);
    }

    #[test]
    fn hedged_round_with_inventory_and_no_transfer_when_unprofitable() {
        let mut acc = accounts(20.0);
        let mut sa = SpotArb::default();
        let p = params(&[]);
        run(
            &mut acc,
            &[("a", 10.1, 10.11), ("b", 9.99, 10.0)],
            &mut sa,
            &p,
            Instant::now(),
        );
        assert_eq!(sa.completed, 1, "{}", sa.stats(&p));
        assert!(
            (acc["a"].free("X") - 10.0).abs() < 1e-9 && (acc["b"].free("X") - 10.0).abs() < 1e-9
        );
        // монеты на a нет, а разница 1% меньше 0.2% комиссий + 1% сети — не входим
        let mut acc = accounts(0.0);
        let mut sa = SpotArb::default();
        run(
            &mut acc,
            &[("a", 10.1, 10.11), ("b", 9.99, 10.0)],
            &mut sa,
            &p,
            Instant::now(),
        );
        assert_eq!(sa.rounds.len(), 0);
        assert!(sa.opportunities[0].expected < 0.0);
    }
}
