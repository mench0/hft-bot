//! Проверка симулятора: бэктест дня, в который бот торговал вживую (или на бумаге), против записанных своих
//! исполнений того же дня — число исполнений, результат, комиссии, markout. Большое расхождение — бэктесту (его модели
//! исполнения и задержке) по этим монетам верить нельзя: подберите fill и latencyMs, при которых сходится.
//! Параметры — текущие параметры биржи в боте: если в тот день они были другими, сравнение неточное.

use super::{Config, run};
use crate::record::event::{BUY, Event, Rec};
use serde_json::{Value, json};
use std::collections::HashMap;

fn r2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// Результат реальных исполнений: деньги по сделкам минус комиссии плюс остаток по последней середине.
fn real(recs: &[Rec], cfg: &Config) -> Value {
    let (mut cash, mut fees, mut n, mut quote) = (0.0, 0.0, 0u64, 0.0);
    let mut held: HashMap<String, f64> = HashMap::new();
    let mut mid: HashMap<String, f64> = HashMap::new();
    let mut ours: Vec<Rec> = Vec::new();
    for r in recs {
        match &r.ev {
            Event::Fill {
                ex,
                sym,
                side,
                qty,
                price,
                fee,
                ..
            } if ex.as_str() == cfg.exchange && cfg.symbols.iter().any(|s| s == sym.as_str()) => {
                let s = if *side == BUY { 1.0 } else { -1.0 };
                cash -= s * qty * price;
                fees += fee;
                quote += qty * price;
                n += 1;
                *held.entry(sym.as_str().to_owned()).or_default() += s * qty;
                ours.push(r.clone());
            }
            Event::Bbo {
                ex, sym, bid, ask, ..
            } if ex.as_str() == cfg.exchange => {
                mid.insert(sym.as_str().to_owned(), (bid + ask) / 2.0);
                ours.push(r.clone());
            }
            _ => {}
        }
    }
    let open: f64 = held
        .iter()
        .map(|(s, q)| q * mid.get(s).copied().unwrap_or(0.0))
        .sum();
    let markout = crate::record::report::build(ours)["markout"].take();
    json!({"fills": n, "quote": r2(quote), "fees": r2(fees), "pnl": r2(cash - fees + open), "markout": markout})
}

pub fn validate(recs: &[Rec], cfg: &Config) -> Value {
    let real = real(recs, cfg);
    let sim = run(recs, cfg);
    let (rf, sf) = (
        real["fills"].as_f64().unwrap_or(0.0),
        sim["fills"].as_f64().unwrap_or(0.0),
    );
    let verdict = if rf == 0.0 {
        "своих исполнений в записи нет — сравнивать не с чем (бот в этот день не торговал этими монетами или запись была выключена)".to_owned()
    } else {
        let ratio = sf / rf;
        let fills = if (0.7..=1.3).contains(&ratio) {
            "число исполнений сходится (±30 %)".to_owned()
        } else if ratio > 1.3 {
            format!(
                "бэктест исполняет в {ratio:.1} раза чаще — модель оптимистична: попробуйте fill=through или большую задержку"
            )
        } else {
            format!(
                "бэктест исполняет в {:.1} раза реже — модель пессимистична: попробуйте fill=trade/touch или меньшую задержку",
                1.0 / ratio.max(1e-9)
            )
        };
        format!(
            "{fills}; результат: бэктест {} против реального {}",
            sim["pnl"], real["pnl"]
        )
    };
    let mut sim = sim;
    sim.as_object_mut().map(|o| o.remove("equity"));
    json!({"real": real, "sim": sim, "verdict": verdict, "fillsRatio": if rf > 0.0 { json!(r2(sf / rf)) } else { Value::Null }})
}
