//! Бэктест сбора спреда на записи (record): тот же код стратегии (strategy::spread) и бумажного счёта, что в живом
//! боте, но время — из записи (infra::clock::Sim), а биржа — симуляция:
//!
//! - **задержка** (latencyMs): стратегия видит стакан с опозданием на задержку (данные доходят до бота и ордер — до
//!   биржи не мгновенно), а заявки исполняются по настоящему рынку — так в результат попадает «забрали раньше нас»;
//! - **исполнение** (fill): `touch` — как бумажный счёт: заявка исполнена, как только противоположная цена дошла до неё
//!   (оптимистично: будто мы первые в очереди); `trade` — сделка рынка по цене заявки или сквозь неё, либо стакан
//!   прошёл сквозь цену (по умолчанию); `through` — только сквозь цену (пессимистично: на уровне стоят раньше нас);
//! - шаг движка — 50 мс времени рынка, как в живом боте; оборот минут для фильтров — из сделок записи.
//!
//! Правил пар (шаг цены и объёма) в записи нет: перебивание на тики (scJumpTicks) без шага цены не работает.
//! Возраст стакана не проверяется: запись хранит лучшие цены только при изменении.

use crate::infra::clock::{self, Sim};
use crate::market::Market;
use crate::market::feed::{Update, apply};
use crate::record::event::{Event, Name, Rec};
use crate::strategy::spread::{self, SpreadCapture};
use crate::trading::account::Account;
use crate::trading::paper::{Limits, Paper};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap, VecDeque};

/// Шаг движка, нс.
const TICK_NS: u64 = 50_000_000;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Fill {
    Touch,
    Trade,
    Through,
}

impl Fill {
    pub fn parse(s: &str) -> Option<Fill> {
        match s {
            "touch" => Some(Fill::Touch),
            "trade" => Some(Fill::Trade),
            "through" => Some(Fill::Through),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub exchange: String,
    pub symbols: Vec<String>,
    /// Параметры биржи (как в живом боте: sc*, maxOrdersPerMinute…).
    pub params: Map<String, Value>,
    pub latency_ms: u64,
    pub fill: Fill,
    pub start_balance: f64,
    pub maker_pct: f64,
    pub taker_pct: f64,
}

/// Оборот сделок по минутам (для фильтров стратегии): последние 5 закрытых минут.
#[derive(Default)]
struct Minutes(BTreeMap<u64, f64>);

impl Minutes {
    fn add(&mut self, ms: u64, quote: f64) {
        *self.0.entry(ms / 60_000).or_default() += quote;
        while self.0.len() > 10 {
            self.0.pop_first();
        }
    }
    fn last(&self, now_ms: u64) -> Option<Vec<f64>> {
        let cur = now_ms / 60_000;
        let first = *self.0.keys().next()?;
        if first + 5 > cur {
            return None; // пяти закрытых минут ещё нет
        }
        Some(
            (cur - 5..cur)
                .map(|m| self.0.get(&m).copied().unwrap_or(0.0))
                .collect(),
        )
    }
}

fn r4(x: f64) -> f64 {
    (x * 1e4).round() / 1e4
}

/// Прогон по событиям записи (по порядку времени получения).
pub fn run(recs: &[Rec], cfg: &Config) -> Value {
    let ex = cfg.exchange.as_str();
    let syms: Vec<String> = cfg.symbols.iter().map(|s| s.to_uppercase()).collect();
    let mine = |e: &Name, s: &Name| e.as_str() == ex && syms.iter().any(|x| x == s.as_str());
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
        return json!({"error": format!("в записи нет лучших цен {ex} по {}", syms.join(", "))});
    };
    let sim = Sim::start(first);
    let (truth, view) = (Market::default(), Market::default());
    let mut delayed: VecDeque<(u64, Update)> = VecDeque::new();
    let lat_ns = cfg.latency_ms * 1_000_000;
    let mut acc = Account::Paper(Paper::new(
        &syms,
        cfg.start_balance,
        cfg.maker_pct,
        cfg.taker_pct,
    ));
    acc.set_venue(&format!("backtest-{ex}"));
    let perp = cfg.params.get("market").and_then(Value::as_str) == Some("perp");
    if perp {
        let lev = cfg
            .params
            .get("leverage")
            .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse().ok()));
        acc.set_perp(lev.unwrap_or(2.0).clamp(1.0, 125.0));
    }
    let mut lim = Limits::from_params(&cfg.params);
    lim.trading_enabled = true;
    // запись хранит лучшие цены только при изменении: тихий стакан в записи — не устаревший
    lim.max_data_age_ms = u64::MAX / 4;
    let mut sp = spread::Params::from(&cfg.params);
    sp.enabled = true;
    let mut warnings: Vec<String> = Vec::new();
    // без снимков стакана в записи (recordDepthLevels = 0) уровни и глубину не проверить — фильтры выключаются
    if !recs.iter().any(|r| matches!(r.ev, Event::Depth { .. }))
        && (sp.min_book_levels > 0 || sp.min_depth_multiple > 0.0)
    {
        sp.min_book_levels = 0;
        sp.min_depth_multiple = 0.0;
        warnings.push(
            "в записи нет снимков стакана (recordDepthLevels = 0): фильтры уровней и глубины (scMinBookLevels, \
             scMinDepthMultiple) выключены — результат оптимистичнее живого"
                .into(),
        );
    }
    let mut sc = SpreadCapture::new(cfg.maker_pct);
    let strict = cfg.fill != Fill::Touch;
    let mut minutes: HashMap<String, Minutes> = HashMap::new();
    let (mut trades_seen, mut events) = (0u64, 0u64);
    // своё исполнение и середины для markout (report::build), кривая капитала раз в секунду
    let mut out_recs: Vec<Rec> = Vec::new();
    let mut seen_exec = 0u64;
    let mut curve: Vec<(u64, f64)> = Vec::new();
    let mids = |m: &Market| -> HashMap<String, f64> {
        syms.iter()
            .filter_map(|s| {
                let b = m.book(ex, s)?;
                Some((s.clone(), (b.best_bid()?.0 + b.best_ask()?.0) / 2.0))
            })
            .collect()
    };
    let equity = |acc: &mut Account, m: &HashMap<String, f64>| -> Option<f64> {
        let v = acc.account_json(m)?;
        Some(
            v["byQuote"]
                .as_object()?
                .values()
                .filter_map(|q| q["equity"].as_f64())
                .sum(),
        )
    };
    let mut collect_fills = |acc: &Account, ts: u64, out: &mut Vec<Rec>| {
        let from = seen_exec;
        for e in acc.execs().iter().filter(|e| e.seq > from) {
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
            seen_exec = e.seq;
        }
    };
    let mut next_tick = first + TICK_NS;
    let mut next_curve = first;
    for r in recs {
        // шаги движка до этого события
        while next_tick <= r.ts {
            sim.set(next_tick);
            while delayed.front().is_some_and(|d| d.0 <= next_tick) {
                let (_, u) = delayed.pop_front().unwrap();
                apply(&view, ex, u);
            }
            let now_ms = clock::now_ms();
            for s in &syms {
                let book = view.book(ex, s);
                let mins = minutes.get(s).and_then(|m| m.last(now_ms));
                sc.step(
                    s,
                    book.as_ref(),
                    &mut acc,
                    &sp,
                    &lim,
                    clock::now(),
                    mins.as_deref(),
                    None,
                );
            }
            collect_fills(&acc, next_tick, &mut out_recs);
            if next_tick >= next_curve {
                let m = mids(&truth);
                if let Some(e) = equity(&mut acc, &m) {
                    curve.push((next_tick / 1_000_000, e));
                }
                next_curve = next_tick + 1_000_000_000;
            }
            next_tick += TICK_NS;
        }
        sim.set(r.ts);
        events += 1;
        match r.ev {
            Event::Bbo {
                sym,
                bid,
                bid_qty,
                ask,
                ask_qty,
                ..
            } => {
                let s = sym.as_str().to_owned();
                let u = Update::Best {
                    sym: s.clone(),
                    bid: (bid, bid_qty),
                    ask: (ask, ask_qty),
                    id: 0,
                };
                apply(&truth, ex, u.clone());
                delayed.push_back((r.ts + lat_ns, u));
                if let Some(b) = truth.book(ex, &s) {
                    acc.match_book(&s, &b, &lim, strict);
                }
                out_recs.push(Rec {
                    ts: r.ts,
                    ev: Event::Bbo {
                        ex: Name::new(ex),
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
                sym, bids, asks, ..
            } => {
                let u = Update::Snapshot {
                    sym: sym.as_str().to_owned(),
                    bids,
                    asks,
                    id: 0,
                };
                apply(&truth, ex, u.clone());
                delayed.push_back((r.ts + lat_ns, u));
            }
            Event::Trade {
                sym, px, qty, buy, ..
            } => {
                trades_seen += 1;
                let s = sym.as_str().to_owned();
                minutes
                    .entry(s.clone())
                    .or_default()
                    .add(r.ts / 1_000_000, px * qty);
                if cfg.fill != Fill::Touch
                    && let Some(m) = mids(&truth).get(&s)
                {
                    acc.match_trade(&s, px, buy, *m, &lim, cfg.fill == Fill::Through);
                }
            }
            _ => {}
        }
        collect_fills(&acc, r.ts, &mut out_recs);
    }
    let m = mids(&truth);
    let end_equity = equity(&mut acc, &m).unwrap_or(0.0);
    let v = acc.account_json(&m).unwrap_or_default();
    let start: f64 = v["byQuote"]
        .as_object()
        .map(|q| q.values().filter_map(|x| x["start"].as_f64()).sum())
        .unwrap_or(0.0);
    // просадка по кривой капитала
    let (mut peak, mut dd) = (f64::MIN, 0.0f64);
    for (_, e) in &curve {
        peak = peak.max(*e);
        dd = dd.max(peak - e);
    }
    // кривая не длиннее 500 точек
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
    let mut report = crate::record::report::build(out_recs);
    let markout = report["markout"].take();
    if cfg.fill != Fill::Touch && trades_seen == 0 {
        warnings.push("в записи нет сделок рынка по этим монетам: исполнение только когда стакан прошёл сквозь цену заявки (fill=touch — оптимистичнее)".into());
    }
    json!({
        "exchange": ex, "symbols": syms, "fromMs": first / 1_000_000, "toMs": last / 1_000_000,
        "minutes": (last - first) / 60_000_000_000, "events": events, "marketTrades": trades_seen,
        "latencyMs": cfg.latency_ms, "fill": format!("{:?}", cfg.fill).to_lowercase(),
        "startEquity": r4(start), "endEquity": r4(end_equity), "pnl": r4(end_equity - start),
        "pnlPercent": if start > 0.0 { r4((end_equity - start) / start * 100.0) } else { 0.0 },
        "maxDrawdown": r4(dd), "fills": fills,
        "realizedPnl": v["realizedPnl"], "fees": v["fees"], "closedTrades": v["closedTrades"], "winRatePercent": v["winRatePercent"],
        "ordersAccepted": acc.stats()["ордеров_принято"], "ordersRejected": acc.stats()["ордеров_отклонено"],
        "openPositions": v["openPositions"], "markout": markout, "strategy": sc.stats(true)["symbols"].clone(),
        "equity": curve, "warnings": warnings,
    })
}

/// Записи дня (часы from..=to) из папки записи.
pub fn load(
    dir: &std::path::Path,
    day: &str,
    hours: Option<(u32, u32)>,
) -> Result<Vec<Rec>, String> {
    let d = dir.join(day);
    let mut files: Vec<_> = std::fs::read_dir(&d)
        .map_err(|_| format!("за {day} записи нет (папка {})", d.display()))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            let name = p.file_name().unwrap_or_default().to_string_lossy();
            name.ends_with(".bin.zst")
                && hours
                    .is_none_or(|(a, b)| name[..2].parse::<u32>().is_ok_and(|h| h >= a && h <= b))
        })
        .collect();
    files.sort();
    if files.is_empty() {
        return Err(format!("за {day} файлов записи нет"));
    }
    let mut all = Vec::new();
    for p in &files {
        all.extend(crate::record::reader::read_file(p)?);
    }
    Ok(all)
}

/// Комиссии биржи из каталога, % (как у бумажного счёта бота).
pub fn fees(ex: &str) -> (f64, f64) {
    let e = crate::infra::data::catalog()["exchanges"]
        .as_array()
        .and_then(|a| a.iter().find(|e| e["id"] == ex).cloned())
        .unwrap_or_default();
    (
        e["makerFeePct"].as_f64().unwrap_or(0.1),
        e["takerFeePct"].as_f64().unwrap_or(0.1),
    )
}

/// `hft-bot backtest --day ГГГГ-ММ-ДД --exchange okx --symbols BTCUSDT [--hours 9-12] [--latency-ms 50]
/// [--fill trade|touch|through] [--set scMinEdgePercent=0.2,scOrderQuote=50] [--dir data/rec]
/// [--sweep "scMinEdgePercent=0.05:0.3:0.05;scOrderQuote=20|50" [--threads N]] [--validate] [--no-save]` — итог JSON,
/// прогон сохраняется в журнал (BACKTEST_DB).
pub fn cli(args: &[String]) -> Result<(), String> {
    let get = |k: &str| {
        args.iter()
            .position(|a| a == k)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let day = get("--day").ok_or("укажите --day ГГГГ-ММ-ДД")?;
    let ex = get("--exchange")
        .ok_or("укажите --exchange")?
        .to_lowercase();
    let symbols: Vec<String> = get("--symbols")
        .ok_or("укажите --symbols")?
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    let hours = get("--hours").and_then(|h| {
        let (a, b) = h.split_once('-').unwrap_or((&h, &h));
        Some((a.parse().ok()?, b.parse().ok()?))
    });
    let mut params = crate::infra::data::defaults(crate::infra::data::exchange_schema());
    for kv in get("--set")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
    {
        let (k, v) = kv
            .split_once('=')
            .ok_or(format!("--set: {kv} — нужно ключ=значение"))?;
        params.insert(k.trim().to_owned(), crate::infra::data::typed(v.trim()));
    }
    let (maker, taker) = fees(&ex);
    let cfg = Config {
        exchange: ex,
        symbols,
        start_balance: params
            .get("paperStartBalance")
            .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse().ok()))
            .unwrap_or(1000.0),
        params,
        latency_ms: get("--latency-ms")
            .and_then(|v| v.parse().ok())
            .unwrap_or(50),
        fill: Fill::parse(&get("--fill").unwrap_or_else(|| "trade".into()))
            .ok_or("--fill: touch, trade или through")?,
        maker_pct: maker,
        taker_pct: taker,
    };
    let dir = get("--dir").unwrap_or_else(|| "data/rec".into());
    let recs = load(std::path::Path::new(&dir), &day, hours)?;
    let (kind, mut v) = if let Some(spec) = get("--sweep") {
        let grid = sweep::parse(&spec)?;
        let threads = get("--threads")
            .and_then(|t| t.parse().ok())
            .unwrap_or_else(|| {
                std::thread::available_parallelism().map_or(1, |n| n.get().saturating_sub(1).max(1))
            });
        ("sweep", sweep::sweep(&recs, &cfg, &grid, threads))
    } else if args.iter().any(|a| a == "--validate") {
        ("validate", validate::validate(&recs, &cfg))
    } else {
        ("run", run(&recs, &cfg))
    };
    if !args.iter().any(|a| a == "--no-save") && v["error"].is_null() {
        v["runId"] = Value::from(runs::save(kind, &Value::from(args.join(" ")), &v));
    }
    println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
    Ok(())
}

pub mod runs;
pub mod sweep;
pub mod validate;

#[cfg(test)]
mod tests;
