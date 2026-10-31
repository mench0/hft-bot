//! Перебор параметров: сетка значений (`ключ=от:до:шаг` или `ключ=a|b|c`, ключи через `;`), каждый вариант —
//! отдельный прогон бэктеста на тех же событиях, параллельно в нескольких потоках (часы симуляции — свои у потока).
//! Не больше MAX_COMBOS вариантов. Итог — таблица по убыванию результата.

use super::{Config, run};
use crate::record::event::Rec;
use serde_json::{Map, Value, json};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

pub const MAX_COMBOS: usize = 200;

/// Сетка: ключ → значения.
pub fn parse(spec: &str) -> Result<Vec<(String, Vec<Value>)>, String> {
    let mut out = Vec::new();
    for part in spec.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        let (k, v) = part.split_once('=').ok_or(format!(
            "перебор: {part} — нужно ключ=от:до:шаг или ключ=a|b|c"
        ))?;
        let vals: Vec<Value> = if v.contains(':') {
            let n: Vec<f64> = v.split(':').filter_map(|x| x.trim().parse().ok()).collect();
            let [a, b, step] = n[..] else {
                return Err(format!("перебор {k}: нужно от:до:шаг числами"));
            };
            if step <= 0.0 || b < a || (b - a) / step > MAX_COMBOS as f64 {
                return Err(format!(
                    "перебор {k}: шаг > 0, до ≥ от, не больше {MAX_COMBOS} значений"
                ));
            }
            let mut v = Vec::new();
            let mut i = 0.0;
            while a + i * step <= b + step * 1e-9 {
                // округление, чтобы 0.1+0.2 не превращалось в 0.30000000000000004
                v.push(json!(((a + i * step) * 1e9).round() / 1e9));
                i += 1.0;
            }
            v
        } else {
            v.split('|')
                .map(|x| crate::infra::data::typed(x.trim()))
                .collect()
        };
        if vals.is_empty() {
            return Err(format!("перебор {k}: нет значений"));
        }
        out.push((k.trim().to_owned(), vals));
    }
    let total: usize = out.iter().map(|x| x.1.len()).product();
    if out.is_empty() || total > MAX_COMBOS {
        return Err(format!("вариантов {total}: нужно от 1 до {MAX_COMBOS}"));
    }
    Ok(out)
}

/// Все сочетания значений.
fn combos(grid: &[(String, Vec<Value>)]) -> Vec<Map<String, Value>> {
    let mut out = vec![Map::new()];
    for (k, vals) in grid {
        out = out
            .into_iter()
            .flat_map(|m| {
                vals.iter().map(move |v| {
                    let mut m = m.clone();
                    m.insert(k.clone(), v.clone());
                    m
                })
            })
            .collect();
    }
    out
}

/// Средний markout через 1 с по всем монетам (с весом по исполнениям).
fn markout_1s(r: &Value) -> Value {
    let (mut sum, mut n) = (0.0, 0.0);
    for m in r["markout"].as_array().into_iter().flatten() {
        if let (Some(v), Some(f)) = (m["markout1000msBps"].as_f64(), m["fills"].as_f64()) {
            sum += v * f;
            n += f;
        }
    }
    if n > 0.0 {
        json!((sum / n * 100.0).round() / 100.0)
    } else {
        Value::Null
    }
}

/// Прогнать сетку в threads потоках (сбор спреда).
pub fn sweep(recs: &[Rec], base: &Config, grid: &[(String, Vec<Value>)], threads: usize) -> Value {
    let mut v = sweep_with(grid, threads, |p| {
        let mut cfg = base.clone();
        cfg.params.extend(p.clone());
        run(recs, &cfg)
    });
    v["exchange"] = json!(base.exchange);
    v["symbols"] = json!(base.symbols);
    v["latencyMs"] = json!(base.latency_ms);
    v["fill"] = json!(format!("{:?}", base.fill).to_lowercase());
    v
}

/// Перебор с любым прогоном: run получает переопределения варианта.
pub fn sweep_with(
    grid: &[(String, Vec<Value>)],
    threads: usize,
    run: impl Fn(&Map<String, Value>) -> Value + Sync,
) -> Value {
    let all = combos(grid);
    let next = AtomicUsize::new(0);
    let rows: Mutex<Vec<Value>> = Mutex::new(Vec::new());
    std::thread::scope(|s| {
        for _ in 0..threads.clamp(1, all.len()) {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::SeqCst);
                    let Some(p) = all.get(i) else { break };
                    let r = run(p);
                    rows.lock().unwrap().push(json!({"params": p, "pnl": r["pnl"], "pnlPercent": r["pnlPercent"],
                        "maxDrawdown": r["maxDrawdown"], "fills": r["fills"], "closedTrades": r["closedTrades"],
                        "winRatePercent": r["winRatePercent"], "fees": r["fees"], "markout1sBps": markout_1s(&r),
                        "error": r["error"]}));
                }
            });
        }
    });
    let mut rows = rows.into_inner().unwrap();
    rows.sort_by(|a, b| {
        b["pnl"]
            .as_f64()
            .unwrap_or(f64::MIN)
            .total_cmp(&a["pnl"].as_f64().unwrap_or(f64::MIN))
    });
    json!({"variants": rows.len(), "keys": grid.iter().map(|g| &g.0).collect::<Vec<_>>(), "rows": rows})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grid_parse_and_combos() {
        let g = parse("scMinEdgePercent=0.1:0.3:0.1; scOrderQuote=20|50").unwrap();
        assert_eq!(g[0].1, vec![json!(0.1), json!(0.2), json!(0.3)]);
        assert_eq!(g[1].1, vec![json!(20), json!(50)]);
        assert_eq!(combos(&g).len(), 6);
        assert!(parse("a=1:1000:0.001").is_err());
        assert!(parse("a=1|2;b=1:300:1").is_err(), "больше 200 вариантов");
        assert!(parse("").is_err());
    }
}
