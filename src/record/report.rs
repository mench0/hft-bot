//! Отчёты по записи за день (или часы дня): `hft-bot report --day …` и GET /research/report.
//!
//! - **markout** — куда ушла середина после своего исполнения через 100 мс, 1, 5, 30 с, в б.п. в пользу сделки
//!   (покупка: цена выросла — плюс). Отрицательный markout — исполнения «забирают» перед движением против нас.
//! - **latency** — задержка данных биржи: время получения минус время биржи (p50/p90/p99), обрывы фида.
//!   Учитывает и сдвиг часов: отрицательные значения — часы сервера отстают.
//! - **leaders** — кто двигается первым: корреляция приращений середины двух бирж по одной монете со сдвигом
//!   (сетка 50 мс, сдвиги до ±1 с); положительный сдвиг — первая биржа ведёт.
//! - **flow** — сделки рынка: сколько, оборот, доля покупок.

use super::event::{Event, Rec};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

/// Горизонты markout, мс.
pub const HORIZONS: [u64; 4] = [100, 1_000, 5_000, 30_000];
/// Шаг сетки для поиска ведущей биржи, мс, и сдвиги в шагах.
const GRID_MS: u64 = 50;
const MAX_LAG: i64 = 20;

type Key = (String, String);

#[derive(Default)]
struct Acc {
    /// Середина по времени получения: (мс, середина).
    mids: HashMap<Key, Vec<(u64, f64)>>,
    /// Свои исполнения: (биржа, символ, мс, цена, объём, покупка, комиссия).
    fills: Vec<(String, String, u64, f64, f64, bool, f64)>,
    /// Задержки биржи, мс (может быть отрицательной при сдвиге часов).
    lat: HashMap<String, Vec<i64>>,
    gaps: HashMap<String, u64>,
    /// Сделки: (кол-во, оборот, оборот покупок).
    flow: HashMap<Key, (u64, f64, f64)>,
    /// Сделки рынка по времени: (мс, покупатель-агрессор).
    trades: HashMap<Key, Vec<(u64, bool)>>,
}

impl Acc {
    fn add(&mut self, r: &Rec) {
        let ms = r.ts / 1_000_000;
        match &r.ev {
            Event::Bbo {
                ex,
                sym,
                ts_ex,
                bid,
                ask,
                ..
            } => {
                if *ts_ex > 0 {
                    self.lat
                        .entry(ex.as_str().to_owned())
                        .or_default()
                        .push(ms as i64 - *ts_ex as i64);
                }
                if *bid > 0.0 && *ask >= *bid {
                    self.mids
                        .entry((ex.as_str().to_owned(), sym.as_str().to_owned()))
                        .or_default()
                        .push((ms, (bid + ask) / 2.0));
                }
            }
            Event::Trade {
                ex,
                sym,
                ts_ex,
                px,
                qty,
                buy,
            } => {
                if *ts_ex > 0 {
                    self.lat
                        .entry(ex.as_str().to_owned())
                        .or_default()
                        .push(ms as i64 - *ts_ex as i64);
                }
                self.trades
                    .entry((ex.as_str().to_owned(), sym.as_str().to_owned()))
                    .or_default()
                    .push((ms, *buy));
                let f = self
                    .flow
                    .entry((ex.as_str().to_owned(), sym.as_str().to_owned()))
                    .or_default();
                f.0 += 1;
                f.1 += px * qty;
                if *buy {
                    f.2 += px * qty;
                }
            }
            Event::Fill {
                ex,
                sym,
                side,
                qty,
                price,
                fee,
                ..
            } => self.fills.push((
                ex.as_str().to_owned(),
                sym.as_str().to_owned(),
                ms,
                *price,
                *qty,
                *side == super::event::BUY,
                *fee,
            )),
            Event::FeedError { ex } => *self.gaps.entry(ex.as_str().to_owned()).or_default() += 1,
            _ => {}
        }
    }
}

/// Середина на момент t (последняя известная), None — до первой.
fn mid_at(v: &[(u64, f64)], t: u64) -> Option<f64> {
    let i = v.partition_point(|x| x.0 <= t);
    (i > 0).then(|| v[i - 1].1)
}

fn pct(v: &mut [i64], p: f64) -> i64 {
    v.sort_unstable();
    v[((v.len() - 1) as f64 * p).round() as usize]
}

fn r2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// Суммы markout по символу: исполнений, суммы и число по горизонтам, оборот, комиссии, край на входе.
type MarkoutSum = (u64, [f64; 4], [u64; 4], f64, f64, f64);

fn markout(a: &Acc) -> Value {
    // по (биржа, символ): число, сумма markout по горизонтам, сколько учтено, оборот, комиссии
    let mut by: BTreeMap<Key, MarkoutSum> = BTreeMap::new();
    for (ex, sym, t, px, qty, buy, fee) in &a.fills {
        let Some(m) = a.mids.get(&(ex.clone(), sym.clone())) else {
            continue;
        };
        let e = by.entry((ex.clone(), sym.clone())).or_default();
        e.0 += 1;
        e.3 += px * qty;
        e.4 += fee;
        let sign = if *buy { 1.0 } else { -1.0 };
        // край на входе: насколько цена исполнения лучше середины в момент сделки
        if let Some(m0) = mid_at(m, *t) {
            e.5 += sign * (m0 - px) / px * 1e4;
        }
        let last = m.last().map_or(0, |x| x.0);
        for (i, h) in HORIZONS.iter().enumerate() {
            if t + h <= last
                && let Some(mh) = mid_at(m, t + h)
            {
                e.1[i] += sign * (mh - px) / px * 1e4;
                e.2[i] += 1;
            }
        }
    }
    Value::Array(
        by.into_iter()
            .map(|((ex, sym), (n, sum, cnt, quote, fees, edge))| {
                let mut o = json!({"exchange": ex, "symbol": sym, "fills": n, "quote": r2(quote), "fees": r2(fees),
                    "edgeBps": r2(edge / n as f64)});
                for (i, h) in HORIZONS.iter().enumerate() {
                    o[format!("markout{h}msBps")] = if cnt[i] > 0 {
                        json!(r2(sum[i] / cnt[i] as f64))
                    } else {
                        Value::Null
                    };
                }
                o
            })
            .collect(),
    )
}

/// Окно «перед исполнением» для причин, мс, и порог движения другой биржи, б.п.
const CAUSE_WINDOW_MS: u64 = 500;
const CAUSE_MOVE_BPS: f64 = 3.0;

/// Причины исполнения: что было перед ним. Против нас — для покупки цена шла вниз (и наоборот).
fn causes(a: &Acc, ex: &str, sym: &str, t: u64, buy: bool) -> Vec<(&'static str, &'static str)> {
    let sign = if buy { 1.0 } else { -1.0 };
    // другая биржа по той же монете сдвинулась против нас за окно
    let mut moved = None;
    for ((e, s), m) in &a.mids {
        if s != sym || e == ex {
            continue;
        }
        if let (Some(a0), Some(a1)) = (mid_at(m, t.saturating_sub(CAUSE_WINDOW_MS)), mid_at(m, t)) {
            let bps = sign * (a1 - a0) / a0 * 1e4;
            moved = Some(moved.map_or(bps, |x: f64| x.min(bps)));
        }
    }
    let other = match moved {
        None => "нет данных другой биржи",
        Some(b) if b <= -CAUSE_MOVE_BPS => "другая биржа сдвинулась против нас",
        Some(b) if b >= CAUSE_MOVE_BPS => "другая биржа сдвинулась в нашу пользу",
        Some(_) => "другие биржи спокойны",
    };
    // серия сделок, бьющих в нашу сторону (для покупки — продавцы-агрессоры), за окно
    let run = match a.trades.get(&(ex.to_owned(), sym.to_owned())) {
        None => "нет сделок рынка",
        Some(v) => {
            let from = v.partition_point(|x| x.0 < t.saturating_sub(CAUSE_WINDOW_MS));
            let to = v.partition_point(|x| x.0 <= t);
            let w = &v[from..to];
            let against = w.iter().filter(|x| x.1 != buy).count();
            if w.len() >= 3 && against * 10 >= w.len() * 8 {
                "серия сделок в нашу заявку"
            } else {
                "без серии сделок"
            }
        }
    };
    // последние 3 минуты часа (по UTC)
    let minute = (t / 60_000) % 60;
    let hour = if minute >= 57 {
        "последние 3 минуты часа"
    } else {
        "остальное время"
    };
    vec![
        ("другая биржа за 500 мс", other),
        ("сделки за 500 мс", run),
        ("время", hour),
    ]
}

/// Суммы по причине: исполнений, сумма и число markout на 1 с, на 5 с.
type CauseSum = (u64, f64, u64, f64, u64);

/// Markout через 1 и 5 с по причинам исполнения: где именно нас «забирают».
fn markout_by_cause(a: &Acc) -> Value {
    // (фактор, значение) → исполнений, суммы и число на 1 с и 5 с
    let mut by: BTreeMap<(&str, &str), CauseSum> = BTreeMap::new();
    for (ex, sym, t, px, _, buy, _) in &a.fills {
        let Some(m) = a.mids.get(&(ex.clone(), sym.clone())) else {
            continue;
        };
        let sign = if *buy { 1.0 } else { -1.0 };
        let last = m.last().map_or(0, |x| x.0);
        let mk = |h: u64| {
            (t + h <= last)
                .then(|| mid_at(m, t + h))
                .flatten()
                .map(|mh| sign * (mh - px) / px * 1e4)
        };
        let (m1, m5) = (mk(1_000), mk(5_000));
        for c in causes(a, ex, sym, *t, *buy) {
            let e = by.entry(c).or_default();
            e.0 += 1;
            if let Some(v) = m1 {
                e.1 += v;
                e.2 += 1;
            }
            if let Some(v) = m5 {
                e.3 += v;
                e.4 += 1;
            }
        }
    }
    let avg = |s: f64, n: u64| {
        if n > 0 {
            json!(r2(s / n as f64))
        } else {
            Value::Null
        }
    };
    Value::Array(
        by.into_iter()
            .map(|((f, v), (n, s1, n1, s5, n5))| {
                json!({"factor": f, "value": v, "fills": n, "markout1000msBps": avg(s1, n1), "markout5000msBps": avg(s5, n5)})
            })
            .collect(),
    )
}

fn latency(a: &mut Acc) -> Value {
    let mut exs: Vec<String> = a.lat.keys().chain(a.gaps.keys()).cloned().collect();
    exs.sort();
    exs.dedup();
    Value::Array(
        exs.into_iter()
            .map(|ex| {
                let gaps = a.gaps.get(&ex).copied().unwrap_or(0);
                match a.lat.get_mut(&ex).filter(|v| !v.is_empty()) {
                    Some(v) => json!({"exchange": ex, "samples": v.len(), "p50Ms": pct(v, 0.5), "p90Ms": pct(v, 0.9),
                        "p99Ms": pct(v, 0.99), "minMs": v[0], "maxMs": v[v.len() - 1], "feedErrors": gaps}),
                    None => json!({"exchange": ex, "samples": 0, "feedErrors": gaps,
                        "note": "биржа не присылает время в данных"}),
                }
            })
            .collect(),
    )
}

/// Приращения середины на сетке GRID_MS в [t0, t1).
fn grid(v: &[(u64, f64)], t0: u64, t1: u64) -> Vec<f64> {
    let n = ((t1 - t0) / GRID_MS) as usize;
    let mut out = Vec::with_capacity(n);
    let mut prev = mid_at(v, t0);
    for k in 1..=n {
        let cur = mid_at(v, t0 + k as u64 * GRID_MS);
        out.push(match (prev, cur) {
            (Some(a), Some(b)) => (b / a).ln(),
            _ => 0.0,
        });
        prev = cur;
    }
    out
}

/// Корреляция x[t] и y[t + lag].
fn corr_lag(x: &[f64], y: &[f64], lag: i64) -> f64 {
    let n = x.len() as i64;
    let (mut sxy, mut sxx, mut syy) = (0.0, 0.0, 0.0);
    for t in 0.max(-lag)..n.min(n - lag) {
        let (a, b) = (x[t as usize], y[(t + lag) as usize]);
        sxy += a * b;
        sxx += a * a;
        syy += b * b;
    }
    if sxx > 0.0 && syy > 0.0 {
        sxy / (sxx * syy).sqrt()
    } else {
        0.0
    }
}

fn leaders(a: &Acc) -> Value {
    let mut by_sym: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (ex, sym) in a.mids.keys() {
        by_sym.entry(sym).or_default().push(ex);
    }
    let mut out = Vec::new();
    for (sym, mut exs) in by_sym {
        exs.sort();
        for i in 0..exs.len() {
            for j in i + 1..exs.len() {
                let (va, vb) = (
                    &a.mids[&(exs[i].to_owned(), sym.to_owned())],
                    &a.mids[&(exs[j].to_owned(), sym.to_owned())],
                );
                // общее окно, не длиннее суток
                let t0 = va[0].0.max(vb[0].0);
                let t1 = va[va.len() - 1].0.min(vb[vb.len() - 1].0);
                if t1 <= t0 + 60_000 {
                    continue;
                }
                let (x, y) = (grid(va, t0, t1), grid(vb, t0, t1));
                let (best, c) = (-MAX_LAG..=MAX_LAG)
                    .map(|l| (l, corr_lag(&x, &y, l)))
                    .max_by(|p, q| p.1.total_cmp(&q.1))
                    .unwrap_or((0, 0.0));
                let c0 = corr_lag(&x, &y, 0);
                let (leader, follower, lag) = if best >= 0 {
                    (exs[i], exs[j], best)
                } else {
                    (exs[j], exs[i], -best)
                };
                out.push(json!({"symbol": sym, "leader": if lag == 0 { "одновременно" } else { leader },
                    "follower": if lag == 0 { format!("{leader} / {follower}") } else { follower.to_owned() },
                    "lagMs": lag as u64 * GRID_MS, "corr": r2(c), "corrNoLag": r2(c0), "minutes": (t1 - t0) / 60_000}));
            }
        }
    }
    Value::Array(out)
}

fn flow(a: &Acc) -> Value {
    let mut v: Vec<_> = a.flow.iter().collect();
    v.sort_by(|x, y| x.0.cmp(y.0));
    Value::Array(
        v.into_iter()
            .map(|((ex, sym), (n, q, qb))| {
                json!({"exchange": ex, "symbol": sym, "trades": n, "quote": r2(*q),
                    "buyShare": if *q > 0.0 { r2(qb / q) } else { 0.0 }})
            })
            .collect(),
    )
}

/// Отчёт по событиям (уже по порядку времени получения внутри файла).
pub fn build(recs: impl IntoIterator<Item = Rec>) -> Value {
    let mut a = Acc::default();
    let mut n = 0u64;
    for r in recs {
        a.add(&r);
        n += 1;
    }
    for v in a.mids.values_mut() {
        v.sort_by_key(|x| x.0);
    }
    for v in a.trades.values_mut() {
        v.sort_by_key(|x| x.0);
    }
    json!({"events": n, "fills": a.fills.len(), "markout": markout(&a), "markoutByCause": markout_by_cause(&a),
        "latency": latency(&mut a),
        "leaders": leaders(&a), "flow": flow(&a), "horizonsMs": HORIZONS})
}

/// Файлы дня (часы from..=to) → отчёт.
pub fn day(dir: &Path, day: &str, hours: Option<(u32, u32)>) -> Result<Value, String> {
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
        all.extend(super::reader::read_file(p)?);
    }
    let mut v = build(all);
    v["day"] = json!(day);
    v["files"] = json!(files.len());
    Ok(v)
}

/// `hft-bot report --day ГГГГ-ММ-ДД [--hours 9-12] [--dir data/rec]` — отчёт JSON в stdout.
pub fn cli(args: &[String]) -> Result<(), String> {
    let get = |k: &str| {
        args.iter()
            .position(|a| a == k)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let d = get("--day").ok_or("укажите --day ГГГГ-ММ-ДД")?;
    let dir = get("--dir").unwrap_or_else(|| "data/rec".into());
    let hours = get("--hours").and_then(|h| {
        let (a, b) = h.split_once('-').unwrap_or((&h, &h));
        Some((a.parse().ok()?, b.parse().ok()?))
    });
    let v = day(Path::new(&dir), &d, hours)?;
    println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::event::{BUY, Event, Name, Rec, SELL};
    use serde_json::json;

    fn bbo(ms: u64, ex: &str, mid: f64, ts_ex: u64) -> Rec {
        Rec {
            ts: ms * 1_000_000,
            ev: Event::Bbo {
                ex: Name::new(ex),
                sym: Name::new("XUSDT"),
                ts_ex,
                bid: mid - 0.01,
                bid_qty: 1.0,
                ask: mid + 0.01,
                ask_qty: 1.0,
            },
        }
    }

    #[test]
    fn markout_latency_leader_flow() {
        let mut recs = Vec::new();
        // «binance» ведёт: его середина ступенькой каждые 2 с, «okx» повторяет через 200 мс
        let t0 = 1_000_000u64;
        let mut level = 100.0;
        for k in 0..60 {
            let t = t0 + k * 2_000;
            level += if k % 3 == 0 { 0.5 } else { -0.2 };
            recs.push(bbo(t, "binance", level, t - 5));
            recs.push(bbo(t + 200, "okx", level, 0));
        }
        // покупка на okx перед ростом и продажа перед ростом (плохая)
        let fill = |ms: u64, side: u8, px: f64| Rec {
            ts: ms * 1_000_000,
            ev: Event::Fill {
                ex: Name::new("okx"),
                sym: Name::new("XUSDT"),
                id: 1,
                side,
                qty: 1.0,
                price: px,
                fee: 0.01,
            },
        };
        recs.push(fill(t0 + 6_100, BUY, 100.6));
        recs.push(fill(t0 + 6_150, SELL, 100.6));
        recs.push(Rec {
            ts: (t0 + 10) * 1_000_000,
            ev: Event::Trade {
                ex: Name::new("okx"),
                sym: Name::new("XUSDT"),
                ts_ex: 0,
                px: 100.0,
                qty: 2.0,
                buy: true,
            },
        });
        recs.push(Rec {
            ts: t0 * 1_000_000,
            ev: Event::FeedError {
                ex: Name::new("okx"),
            },
        });
        recs.sort_by_key(|r| r.ts);
        let v = super::build(recs);
        let m = &v["markout"][0];
        assert_eq!(
            (m["exchange"].as_str(), m["fills"].as_u64()),
            (Some("okx"), Some(2))
        );
        assert_eq!(
            m["markout1000msBps"],
            json!(0.0),
            "покупка и продажа по одной цене — в сумме ноль"
        );
        let lat = v["latency"].as_array().unwrap();
        assert_eq!(lat[0]["exchange"], "binance");
        assert_eq!(lat[0]["p50Ms"], 5);
        assert_eq!(lat[1]["feedErrors"], 1);
        let l = &v["leaders"][0];
        assert_eq!(
            (l["leader"].as_str(), l["follower"].as_str()),
            (Some("binance"), Some("okx")),
            "{l}"
        );
        assert_eq!(l["lagMs"], 200);
        assert_eq!(v["flow"][0]["buyShare"], 1.0);
        let by = v["markoutByCause"].as_array().unwrap();
        assert!(
            by.iter().any(|r| r["factor"] == "время" && r["fills"] == 2),
            "{by:?}"
        );
        assert!(
            by.iter().any(|r| r["factor"] == "другая биржа за 500 мс"),
            "{by:?}"
        );
    }

    #[test]
    fn markout_sign() {
        let mut recs: Vec<Rec> = (0..40)
            .map(|k| bbo(k * 1_000, "gate", 100.0 + k as f64 * 0.1, 0))
            .collect();
        recs.push(Rec {
            ts: 5_000 * 1_000_000,
            ev: Event::Fill {
                ex: Name::new("gate"),
                sym: Name::new("XUSDT"),
                id: 1,
                side: SELL,
                qty: 1.0,
                price: 100.5,
                fee: 0.0,
            },
        });
        recs.sort_by_key(|r| r.ts);
        let v = super::build(recs);
        // продали, а цена росла — markout отрицательный: «забрали» перед движением против нас
        assert!(
            v["markout"][0]["markout5000msBps"].as_f64().unwrap() < -40.0,
            "{v}"
        );
    }
}

#[cfg(test)]
mod cause_tests {
    use super::super::event::{BUY, Event, Name, Rec};

    fn bbo(ms: u64, ex: &str, mid: f64) -> Rec {
        Rec {
            ts: ms * 1_000_000,
            ev: Event::Bbo {
                ex: Name::new(ex),
                sym: Name::new("XUSDT"),
                ts_ex: 0,
                bid: mid - 0.01,
                bid_qty: 1.0,
                ask: mid + 0.01,
                ask_qty: 1.0,
            },
        }
    }

    #[test]
    fn fills_after_leader_drop_are_marked_against_us() {
        let mut recs = vec![bbo(0, "binance", 100.0), bbo(0, "okx", 100.0)];
        // binance падает на 10 б.п. за 200 мс до нашей покупки на okx, okx догоняет через 300 мс
        recs.push(bbo(9_800, "binance", 99.9));
        recs.push(bbo(10_300, "okx", 99.9));
        recs.push(bbo(20_000, "binance", 99.9));
        recs.push(bbo(20_000, "okx", 99.9));
        // три продажи-агрессора перед исполнением
        for k in 0..3 {
            recs.push(Rec {
                ts: (9_900 + k * 10) * 1_000_000,
                ev: Event::Trade {
                    ex: Name::new("okx"),
                    sym: Name::new("XUSDT"),
                    ts_ex: 0,
                    px: 99.99,
                    qty: 1.0,
                    buy: false,
                },
            });
        }
        recs.push(Rec {
            ts: 10_000 * 1_000_000,
            ev: Event::Fill {
                ex: Name::new("okx"),
                sym: Name::new("XUSDT"),
                id: 1,
                side: BUY,
                qty: 1.0,
                price: 99.99,
                fee: 0.0,
            },
        });
        recs.sort_by_key(|r| r.ts);
        let v = super::build(recs);
        let by = v["markoutByCause"].as_array().unwrap();
        let row = |f: &str| by.iter().find(|r| r["factor"] == f).unwrap();
        assert_eq!(
            row("другая биржа за 500 мс")["value"],
            "другая биржа сдвинулась против нас"
        );
        assert_eq!(
            row("сделки за 500 мс")["value"],
            "серия сделок в нашу заявку"
        );
        assert!(row("время")["markout1000msBps"].as_f64().unwrap() < -5.0);
    }
}
