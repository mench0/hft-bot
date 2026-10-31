//! Бэктест межбиржевых стратегий — lead-lag и перп – перп — тем же кодом (strategy::leadlag, strategy::perparb):
//! несколько бирж из записи, у каждой свой бумажный счёт. Как и в сборе спреда, стратегия видит рынок на latencyMs
//! позже, а ордера (рыночные — сразу, лимитные — по модели fill) исполняются по настоящему рынку: иначе lead-lag
//! «зарабатывал» бы на собственной задержке. Funding перпов не начисляется — для долгих позиций перп – перп результат
//! оптимистичнее или пессимистичнее живого на сумму funding.

use super::{Fill, TICK_NS};
use crate::infra::clock::{self, Sim};
use crate::market::Market;
use crate::market::feed::{Update, apply};
use crate::record::event::{Event, Name, Rec};
use crate::strategy::Venue;
use crate::trading::account::Account;
use crate::trading::paper::{Limits, Paper};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Strategy {
    LeadLag,
    PerpArb,
}

impl Strategy {
    pub fn parse(s: &str) -> Option<Strategy> {
        match s {
            "lead-lag" | "leadlag" => Some(Strategy::LeadLag),
            "perp-arb" | "perparb" => Some(Strategy::PerpArb),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct CrossConfig {
    pub strategy: Strategy,
    pub exchanges: Vec<String>,
    pub symbols: Vec<String>,
    /// Настройки процесса (leadLag*, perpArb*).
    pub settings: Map<String, Value>,
    /// Параметры каждой биржи (рынок, плечо, лимиты, стартовый баланс).
    pub params: HashMap<String, Map<String, Value>>,
    pub latency_ms: u64,
    pub fill: Fill,
}

fn r4(x: f64) -> f64 {
    (x * 1e4).round() / 1e4
}

fn num(p: &Map<String, Value>, k: &str, d: f64) -> f64 {
    p.get(k)
        .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse().ok()))
        .unwrap_or(d)
}

pub fn run(recs: &[Rec], cfg: &CrossConfig) -> Value {
    let syms: Vec<String> = cfg.symbols.iter().map(|s| s.to_uppercase()).collect();
    let exs: Vec<String> = cfg.exchanges.iter().map(|e| e.to_lowercase()).collect();
    let mine = |e: &Name, s: &Name| {
        exs.iter().any(|x| x == e.as_str()) && syms.iter().any(|x| x == s.as_str())
    };
    let mut recs: Vec<Rec> = recs
        .iter()
        .filter(|r| match &r.ev {
            Event::Bbo { ex, sym, .. }
            | Event::Trade { ex, sym, .. }
            | Event::Depth { ex, sym, .. } => mine(ex, sym),
            _ => false,
        })
        .cloned()
        .collect();
    recs.sort_by_key(|r| r.ts);
    let (Some(first), Some(last)) = (recs.first().map(|r| r.ts), recs.last().map(|r| r.ts)) else {
        return json!({"error": format!("в записи нет цен {} по {}", exs.join(", "), syms.join(", "))});
    };
    let seen_ex: HashSet<&str> = recs
        .iter()
        .filter_map(|r| match &r.ev {
            Event::Bbo { ex, .. } => Some(ex.as_str()),
            _ => None,
        })
        .collect();
    let mut warnings: Vec<String> = exs
        .iter()
        .filter(|e| !seen_ex.contains(e.as_str()))
        .map(|e| format!("в записи нет цен биржи {e} по этим монетам"))
        .collect();
    let sim = Sim::start(first);
    let (truth, view) = (Market::default(), Market::default());
    let lat_ns = cfg.latency_ms * 1_000_000;
    let mut delayed: VecDeque<(u64, String, Update)> = VecDeque::new();
    let strict = cfg.fill != Fill::Touch;
    // счета и лимиты по биржам
    let mut accs: BTreeMap<String, Account> = BTreeMap::new();
    let mut lims: HashMap<String, Limits> = HashMap::new();
    for ex in &exs {
        let p = cfg.params.get(ex).cloned().unwrap_or_default();
        let (maker, taker) = super::fees(ex);
        let mut a = Account::Paper(Paper::new(
            &syms,
            num(&p, "paperStartBalance", 1000.0),
            maker,
            taker,
        ));
        a.set_venue(&format!("backtest-{ex}"));
        if p.get("market").and_then(Value::as_str) == Some("perp") {
            a.set_perp(num(&p, "leverage", 2.0).clamp(1.0, 125.0));
        }
        let mut lim = Limits::from_params(&p);
        lim.trading_enabled = true;
        lim.max_data_age_ms = u64::MAX / 4;
        accs.insert(ex.clone(), a);
        lims.insert(ex.clone(), lim);
    }
    if cfg.strategy == Strategy::PerpArb && accs.values().filter(|a| a.is_perp()).count() < 2 {
        warnings.push("перп – перп торгует только перпами: у бирж в параметрах market = perp должно быть минимум у двух".into());
    }
    let mut ll = crate::strategy::leadlag::LeadLag::default();
    let mut pa = crate::strategy::perparb::PerpArb::default();
    // возраст стакана не проверяется: запись хранит лучшие цены только при изменении
    let mut llp = crate::strategy::leadlag::Params::from(&cfg.settings);
    llp.enabled = true;
    llp.max_book_age_ms = u64::MAX / 4;
    let mut pap = crate::strategy::perparb::Params::from(&cfg.settings);
    pap.enabled = true;
    pap.max_book_age_ms = u64::MAX / 4;
    if cfg.strategy == Strategy::LeadLag && !exs.contains(&llp.leader) {
        warnings.push(format!(
            "ведущая биржа {} (leadLagLeader) не в списке бирж бэктеста",
            llp.leader
        ));
    }
    let mids = |m: &Market, ex: &str| -> HashMap<String, f64> {
        syms.iter()
            .filter_map(|s| {
                let b = m.book(ex, s)?;
                Some((s.clone(), (b.best_bid()?.0 + b.best_ask()?.0) / 2.0))
            })
            .collect()
    };
    let equity = |accs: &mut BTreeMap<String, Account>, m: &Market| -> f64 {
        accs.iter_mut()
            .filter_map(|(ex, a)| {
                let v = a.account_json(&mids(m, ex))?;
                Some(
                    v["byQuote"]
                        .as_object()?
                        .values()
                        .filter_map(|q| q["equity"].as_f64())
                        .sum::<f64>(),
                )
            })
            .sum()
    };
    let mut out_recs: Vec<Rec> = Vec::new();
    let mut seen: HashMap<String, u64> = HashMap::new();
    let collect = |accs: &BTreeMap<String, Account>,
                   ts: u64,
                   seen: &mut HashMap<String, u64>,
                   out: &mut Vec<Rec>| {
        for (ex, a) in accs {
            let from = seen.get(ex).copied().unwrap_or(0);
            for e in a.execs().iter().filter(|e| e.seq > from) {
                out.push(Rec {
                    ts,
                    ev: Event::Fill {
                        ex: Name::new(ex),
                        sym: Name::new(&e.symbol),
                        id: e.order_id,
                        side: e.side.code(),
                        qty: e.qty,
                        price: e.price,
                        fee: e.fee,
                    },
                });
                seen.insert(ex.clone(), e.seq);
            }
        }
    };
    let mut curve: Vec<(u64, f64)> = Vec::new();
    let (mut next_tick, mut next_curve) = (first + TICK_NS, first);
    let busy = HashSet::new();
    for r in recs {
        while next_tick <= r.ts {
            sim.set(next_tick);
            while delayed.front().is_some_and(|d| d.0 <= next_tick) {
                let (_, e, u) = delayed.pop_front().unwrap();
                apply(&view, &e, u);
            }
            // ордера исполняются по настоящему рынку, стратегия видит рынок с задержкой
            for (ex, a) in accs.iter_mut() {
                let real: HashMap<String, _> = syms
                    .iter()
                    .filter_map(|s| Some((s.clone(), truth.book(ex, s)?)))
                    .collect();
                a.set_exec_books(Some(real));
            }
            {
                let mut venues: HashMap<String, Venue> = accs
                    .iter_mut()
                    .map(|(ex, a)| {
                        let lim = lims[ex].clone();
                        let books = syms
                            .iter()
                            .filter_map(|s| Some((s.clone(), view.book(ex, s)?)))
                            .collect();
                        (
                            ex.clone(),
                            Venue {
                                account: a,
                                trading: true,
                                lim,
                                books,
                            },
                        )
                    })
                    .collect();
                let now_ms = clock::now_ms();
                match cfg.strategy {
                    Strategy::LeadLag => ll.step(&mut venues, &llp, &busy, now_ms),
                    Strategy::PerpArb => pa.step(&mut venues, &pap, &busy, now_ms),
                }
            }
            collect(&accs, next_tick, &mut seen, &mut out_recs);
            if next_tick >= next_curve {
                curve.push((next_tick / 1_000_000, equity(&mut accs, &truth)));
                next_curve = next_tick + 1_000_000_000;
            }
            next_tick += TICK_NS;
        }
        sim.set(r.ts);
        match r.ev {
            Event::Bbo {
                ex,
                sym,
                bid,
                bid_qty,
                ask,
                ask_qty,
                ..
            } => {
                let (e, s) = (ex.as_str().to_owned(), sym.as_str().to_owned());
                let u = Update::Best {
                    sym: s.clone(),
                    bid: (bid, bid_qty),
                    ask: (ask, ask_qty),
                    id: 0,
                };
                apply(&truth, &e, u.clone());
                delayed.push_back((r.ts + lat_ns, e.clone(), u));
                if let (Some(b), Some(a)) = (truth.book(&e, &s), accs.get_mut(&e)) {
                    a.match_book(&s, &b, &lims[&e], strict);
                }
                out_recs.push(Rec {
                    ts: r.ts,
                    ev: Event::Bbo {
                        ex,
                        sym,
                        ts_ex: 0,
                        bid,
                        bid_qty,
                        ask,
                        ask_qty,
                    },
                });
            }
            Event::Depth {
                ex,
                sym,
                bids,
                asks,
                ..
            } => {
                let u = Update::Snapshot {
                    sym: sym.as_str().to_owned(),
                    bids,
                    asks,
                    id: 0,
                };
                apply(&truth, ex.as_str(), u.clone());
                delayed.push_back((r.ts + lat_ns, ex.as_str().to_owned(), u));
            }
            Event::Trade {
                ex, sym, px, buy, ..
            } => {
                let (e, s) = (ex.as_str(), sym.as_str());
                if cfg.fill != Fill::Touch
                    && let Some(m) = mids(&truth, e).get(s)
                    && let Some(a) = accs.get_mut(e)
                {
                    a.match_trade(s, px, buy, *m, &lims[e], cfg.fill == Fill::Through);
                }
            }
            _ => {}
        }
        collect(&accs, r.ts, &mut seen, &mut out_recs);
    }
    let end = equity(&mut accs, &truth);
    let start: f64 = accs
        .iter_mut()
        .filter_map(|(ex, a)| {
            let v = a.account_json(&mids(&truth, ex))?;
            Some(
                v["byQuote"]
                    .as_object()?
                    .values()
                    .filter_map(|q| q["start"].as_f64())
                    .sum::<f64>(),
            )
        })
        .sum();
    let (mut peak, mut dd) = (f64::MIN, 0.0f64);
    for (_, e) in &curve {
        peak = peak.max(*e);
        dd = dd.max(peak - e);
    }
    let step = curve.len().div_ceil(500).max(1);
    let curve: Vec<Value> = curve
        .iter()
        .step_by(step)
        .map(|(t, e)| json!([t, r4(*e)]))
        .collect();
    let fills = out_recs
        .iter()
        .filter(|r| matches!(r.ev, Event::Fill { .. }))
        .count();
    let per_ex: Vec<Value> = accs
        .iter_mut()
        .map(|(ex, a)| {
            let m = mids(&truth, ex);
            let v = a.account_json(&m).unwrap_or_default();
            json!({"exchange": ex, "fees": v["fees"], "realizedPnl": v["realizedPnl"], "openPositions": v["openPositions"],
                "ordersAccepted": a.stats()["ордеров_принято"], "ordersRejected": a.stats()["ордеров_отклонено"],
                "lastReject": a.stats()["последний_отказ"]})
        })
        .collect();
    let now_ms = clock::now_ms();
    let strat = match cfg.strategy {
        Strategy::LeadLag => ll.stats(&llp, now_ms),
        Strategy::PerpArb => pa.stats(&pap, None),
    };
    let fees: f64 = per_ex.iter().filter_map(|x| x["fees"].as_f64()).sum();
    let closed = strat["closed"].as_f64().unwrap_or(0.0);
    let wins = strat["wins"].as_f64().unwrap_or(0.0);
    let mut report = crate::record::report::build(out_recs);
    json!({
        "strategy": match cfg.strategy { Strategy::LeadLag => "lead-lag", Strategy::PerpArb => "perp-arb" },
        "exchanges": exs, "symbols": syms, "fromMs": first / 1_000_000, "toMs": last / 1_000_000,
        "minutes": (last - first) / 60_000_000_000, "latencyMs": cfg.latency_ms, "fill": format!("{:?}", cfg.fill).to_lowercase(),
        "startEquity": r4(start), "endEquity": r4(end), "pnl": r4(end - start),
        "pnlPercent": if start > 0.0 { r4((end - start) / start * 100.0) } else { 0.0 },
        "maxDrawdown": r4(dd), "fills": fills, "fees": r4(fees), "closedTrades": closed,
        "winRatePercent": if closed > 0.0 { r4(wins / closed * 100.0) } else { 0.0 }, "byExchange": per_ex, "markout": report["markout"].take(),
        "strategyState": strat, "equity": curve, "warnings": warnings,
    })
}

/// Переопределения варианта: ключ из настроек процесса — в настройки, иначе — в параметры всех бирж.
pub fn with_overrides(base: &CrossConfig, over: &Map<String, Value>) -> CrossConfig {
    let mut c = base.clone();
    let specs = crate::infra::data::settings_specs();
    for (k, v) in over {
        if specs.contains_key(k) {
            c.settings.insert(k.clone(), v.clone());
        } else {
            for p in c.params.values_mut() {
                p.insert(k.clone(), v.clone());
            }
        }
    }
    c
}

/// Перебор межбиржевой стратегии.
pub fn sweep(
    recs: &[Rec],
    base: &CrossConfig,
    grid: &[(String, Vec<Value>)],
    threads: usize,
) -> Value {
    let mut v = super::sweep::sweep_with(grid, threads, |p| run(recs, &with_overrides(base, p)));
    v["exchange"] = json!(base.exchanges.join(" + "));
    v["symbols"] = json!(base.symbols);
    v["latencyMs"] = json!(base.latency_ms);
    v["fill"] = json!(format!("{:?}", base.fill).to_lowercase());
    v
}
