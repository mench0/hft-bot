use super::*;
use crate::record::event::{Event, Name, Rec};

/// Широкий спред 99.5 / 100.5, сделки по очереди в бид и в аск каждые 500 мс, 10 минут.
fn market() -> Vec<Rec> {
    let mut v = Vec::new();
    let t0 = 1_761_900_000_000_000_000u64;
    for k in 0..1200u64 {
        let ts = t0 + k * 500_000_000;
        let tick = (k % 2) as f64 * 0.001;
        v.push(Rec {
            ts,
            ev: Event::Bbo {
                ex: Name::new("gate"),
                sym: Name::new("XUSDT"),
                ts_ex: 0,
                bid: 99.5 + tick,
                bid_qty: 50.0,
                ask: 100.5,
                ask_qty: 50.0,
            },
        });
        let buy = k % 4 < 2;
        v.push(Rec {
            ts: ts + 100_000_000,
            ev: Event::Trade {
                ex: Name::new("gate"),
                sym: Name::new("XUSDT"),
                ts_ex: 0,
                px: if buy { 100.5 } else { 99.5 },
                qty: 1.0,
                buy,
            },
        });
    }
    // чужая биржа и монета — не мешают
    v.push(Rec {
        ts: t0,
        ev: Event::Bbo {
            ex: Name::new("okx"),
            sym: Name::new("XUSDT"),
            ts_ex: 0,
            bid: 1.0,
            bid_qty: 1.0,
            ask: 2.0,
            ask_qty: 1.0,
        },
    });
    v
}

fn cfg(fill: Fill) -> Config {
    let mut params = crate::infra::data::defaults(crate::infra::data::exchange_schema());
    params.insert("scRequoteMs".into(), 500.into());
    params.insert("maxOrdersPerMinute".into(), 1000.into());
    Config {
        exchange: "gate".into(),
        symbols: vec!["XUSDT".into()],
        params,
        latency_ms: 50,
        fill,
        start_balance: 1000.0,
        maker_pct: 0.1,
        taker_pct: 0.1,
    }
}

#[test]
fn spread_capture_earns_on_wide_spread_with_trades() {
    let r = run(market(), &cfg(Fill::Trade));
    assert!(r["fills"].as_u64().unwrap() >= 4, "{r}");
    assert!(r["pnl"].as_f64().unwrap() > 0.0, "{r}");
    assert_eq!(r["minutes"], 9);
    assert!(r["equity"].as_array().unwrap().len() > 100);
    assert_eq!(r["markout"][0]["exchange"], "gate");
    // живая лента и запись бэктест не видят
    assert_eq!(
        crate::market::tape::json("gate", "XUSDT", 0)["trades"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    assert!(
        !crate::infra::clock::simulated(),
        "часы вернулись к реальному времени"
    );
}

#[test]
fn fill_models_differ() {
    // сделки только по цене заявки: «сквозь» не исполняет, «касание» стакана тоже (стакан не прошёл через цену)
    let n = |f| run(market(), &cfg(f))["fills"].as_u64().unwrap();
    let (trade, through, touch) = (n(Fill::Trade), n(Fill::Through), n(Fill::Touch));
    // «сквозь»: продажа по 100.5 не исполняется никогда (сделки ровно по цене заявки) — только покупки, пока не упрёмся в лимит запаса
    assert!(through * 10 < trade, "trade {trade}, through {through}");
    // «касание» стакана: стакан ни разу не прошёл через цену заявки — исполнений нет
    assert_eq!(touch, 0);
    let none = run(vec![], &cfg(Fill::Trade));
    assert!(none["error"].is_string());
}
