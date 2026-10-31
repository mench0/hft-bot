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
    let r = run(&market(), &cfg(Fill::Trade));
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
    let n = |f| run(&market(), &cfg(f))["fills"].as_u64().unwrap();
    let (trade, through, touch) = (n(Fill::Trade), n(Fill::Through), n(Fill::Touch));
    // «сквозь»: продажа по 100.5 не исполняется никогда (сделки ровно по цене заявки) — только покупки, пока не упрёмся в лимит запаса
    assert!(through * 10 < trade, "trade {trade}, through {through}");
    // «касание» стакана: стакан ни разу не прошёл через цену заявки — исполнений нет
    assert_eq!(touch, 0);
    let none = run(&[], &cfg(Fill::Trade));
    assert!(none["error"].is_string());
}

#[test]
fn sweep_ranks_variants_and_journal_keeps_runs() {
    let grid = super::sweep::parse("scMinEdgePercent=0.1|0.9;scOrderQuote=20|40").unwrap();
    let v = super::sweep::sweep(&market(), &cfg(Fill::Trade), &grid, 3);
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    assert!(
        rows[0]["pnl"].as_f64() >= rows[3]["pnl"].as_f64(),
        "по убыванию результата"
    );
    // запас 0.9 % при спреде 1 % и комиссиях 0.2 % — продать с прибылью нельзя, сделок не закрыть
    assert!(
        rows.iter()
            .filter(|r| r["params"]["scMinEdgePercent"] == 0.9)
            .all(|r| r["closedTrades"] == 0)
    );
    assert_eq!(
        rows[0]["params"]["scOrderQuote"], 40,
        "крупнее сделка — больше результат: {}",
        rows[0]
    );
    let dir = tempfile::tempdir().unwrap();
    crate::infra::util::set_env(
        "BACKTEST_DB",
        &dir.path().join("b.db").display().to_string(),
    );
    let id = super::runs::save("sweep", &serde_json::json!({"day": "x"}), &v).unwrap();
    let l = super::runs::list(10);
    assert_eq!(
        (l[0]["id"].as_i64(), l[0]["variants"].as_u64()),
        (Some(id), Some(4))
    );
    assert_eq!(
        super::runs::get(id).unwrap()["result"]["rows"]
            .as_array()
            .unwrap()
            .len(),
        4
    );
}

#[test]
fn validate_compares_with_recorded_fills() {
    let mut recs = market();
    // «реальные» исполнения того дня: купили по 99.5 и продали по 100.5
    for (k, side, px) in [
        (10u64, crate::record::event::BUY, 99.5),
        (20, crate::record::event::SELL, 100.5),
    ] {
        recs.push(Rec {
            ts: 1_761_900_000_000_000_000 + k * 1_000_000_000,
            ev: Event::Fill {
                ex: Name::new("gate"),
                sym: Name::new("XUSDT"),
                id: k,
                side,
                qty: 1.0,
                price: px,
                fee: 0.1,
            },
        });
    }
    let v = super::validate::validate(&recs, &cfg(Fill::Trade));
    assert_eq!(v["real"]["fills"], 2);
    assert_eq!(v["real"]["pnl"], 0.8, "1 − две комиссии по 0.1");
    assert!(v["fillsRatio"].as_f64().unwrap() > 1.3);
    assert!(
        v["verdict"].as_str().unwrap().contains("чаще"),
        "{}",
        v["verdict"]
    );
    assert!(v["sim"]["equity"].is_null());
}

#[test]
fn toxic_guard_pulls_bid_after_other_exchange_drops() {
    let mut recs = market();
    let t0 = 1_761_900_000_000_000_000u64;
    // binance: каждые 10 с падает на 0.2 % и через 2 с возвращается
    for k in 0..60u64 {
        let ts = t0 + k * 10_000_000_000;
        for (dt, mid) in [(0u64, 100.0), (7_900_000_000, 99.8), (9_900_000_000, 100.0)] {
            recs.push(Rec {
                ts: ts + dt,
                ev: Event::Bbo {
                    ex: Name::new("binance"),
                    sym: Name::new("XUSDT"),
                    ts_ex: 0,
                    bid: mid - 0.01,
                    bid_qty: 1.0,
                    ask: mid + 0.01,
                    ask_qty: 1.0,
                },
            });
        }
    }
    let off = run(&recs, &cfg(Fill::Trade));
    let mut c = cfg(Fill::Trade);
    c.params.insert("scToxicMovePercent".into(), 0.1.into());
    c.params.insert("scToxicPauseMs".into(), 1500.into());
    let on = run(&recs, &c);
    let pauses = on["strategy"][0]["toxicPauses"].as_u64().unwrap();
    assert!(pauses >= 50, "{pauses}");
    assert!(
        on["fills"].as_u64() < off["fills"].as_u64(),
        "on {} off {}",
        on["fills"],
        off["fills"]
    );
    assert_eq!(off["strategy"][0]["toxicPauses"], 0);
    assert!(
        on["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .all(|w| !w.as_str().unwrap().contains("токсичного"))
    );
}

/// Ведущая binance прыгает на +0.5 % каждые 20 с, ведомая gate догоняет через 1 с; через 10 с обе возвращаются.
fn lead_lag_market() -> Vec<Rec> {
    let mut v = Vec::new();
    let t0 = 1_761_900_000_000_000_000u64;
    let s = 1_000_000_000u64;
    let bbo = |ts: u64, ex: &str, mid: f64| Rec {
        ts,
        ev: Event::Bbo {
            ex: Name::new(ex),
            sym: Name::new("XUSDT"),
            ts_ex: 0,
            bid: mid - 0.005,
            bid_qty: 100.0,
            ask: mid + 0.005,
            ask_qty: 100.0,
        },
    };
    for k in 0..30u64 {
        let t = t0 + k * 20 * s;
        // спокойные цены для базиса
        for j in 0..10u64 {
            v.push(bbo(t + j * s / 2, "binance", 100.0 + j as f64 * 1e-4));
            v.push(bbo(t + j * s / 2 + 1, "gate", 100.0 + j as f64 * 1e-4));
        }
        v.push(bbo(t + 6 * s, "binance", 100.5));
        v.push(bbo(t + 7 * s, "gate", 100.5));
        v.push(bbo(t + 16 * s, "binance", 100.0));
        v.push(bbo(t + 16 * s + 1, "gate", 100.0));
    }
    v
}

fn cross_cfg(latency_ms: u64) -> super::cross::CrossConfig {
    let mut settings = crate::infra::data::defaults(crate::infra::data::settings_specs());
    for (k, v) in [
        ("leadLagLeader", serde_json::json!("binance")),
        ("leadLagMinMovePercent", 0.2.into()),
        ("leadLagWindowMs", 1000.into()),
        ("leadLagMinProfitPercent", 0.05.into()),
        ("leadLagBasisSec", 5.into()),
        ("leadLagMaxHoldMs", 5000.into()),
        ("leadLagOrderQuote", 100.into()),
    ] {
        settings.insert(k.into(), v);
    }
    let params = crate::infra::data::defaults(crate::infra::data::exchange_schema());
    super::cross::CrossConfig {
        strategy: super::cross::Strategy::LeadLag,
        exchanges: vec!["binance".into(), "gate".into()],
        symbols: vec!["XUSDT".into()],
        settings,
        params: [("binance".into(), params.clone()), ("gate".into(), params)].into(),
        latency_ms,
        fill: Fill::Trade,
    }
}

#[test]
fn lead_lag_earns_only_when_faster_than_follower() {
    let fast = super::cross::run(&lead_lag_market(), &cross_cfg(50));
    assert!(
        fast["fills"].as_u64().unwrap() >= 10,
        "{}",
        fast["strategyState"]
    );
    assert!(fast["pnl"].as_f64().unwrap() > 0.0, "{fast}");
    // задержка больше отставания ведомой: к моменту, когда бот видит движение, ведомая уже догнала
    let slow = super::cross::run(&lead_lag_market(), &cross_cfg(1500));
    assert!(
        slow["pnl"].as_f64().unwrap() < fast["pnl"].as_f64().unwrap() / 2.0,
        "fast {} slow {}",
        fast["pnl"],
        slow["pnl"]
    );
    assert_eq!(fast["byExchange"].as_array().unwrap().len(), 2);
}

#[test]
fn perp_arb_opens_and_closes_on_divergence() {
    // перпы: gate дороже binance на 1.5 % (круг комиссий 0.6 %) по 5 с каждые 20 с, потом цены сходятся
    let mut v = Vec::new();
    let t0 = 1_761_900_000_000_000_000u64;
    let s = 1_000_000_000u64;
    let bbo = |ts: u64, ex: &str, mid: f64| Rec {
        ts,
        ev: Event::Bbo {
            ex: Name::new(ex),
            sym: Name::new("XUSDT"),
            ts_ex: 0,
            bid: mid - 0.005,
            bid_qty: 100.0,
            ask: mid + 0.005,
            ask_qty: 100.0,
        },
    };
    for k in 0..30u64 {
        let t = t0 + k * 20 * s;
        for j in 0..40u64 {
            v.push(bbo(t + j * s / 2, "binance", 100.0));
            let gap = if (10..20).contains(&j) { 101.5 } else { 100.0 };
            v.push(bbo(t + j * s / 2 + 1, "gate", gap));
        }
    }
    let mut c = cross_cfg(50);
    c.strategy = super::cross::Strategy::PerpArb;
    for p in c.params.values_mut() {
        p.insert("market".into(), "perp".into());
    }
    c.settings
        .insert("perpArbMinProfitPercent".into(), 0.1.into());
    c.settings.insert("perpArbOrderQuote".into(), 100.into());
    // выход при спреде ≤ 0: с бидом и аском обеих бирж сошедшиеся цены дают +0.01 % — пара ждала бы таймаута
    c.settings
        .insert("perpArbExitSpreadPercent".into(), 0.05.into());
    let r = super::cross::run(&v, &c);
    assert!(r["fills"].as_u64().unwrap() >= 20, "{}", r["strategyState"]);
    assert!(r["pnl"].as_f64().unwrap() > 0.0, "{r}");
    assert!(
        r["warnings"].as_array().unwrap().is_empty(),
        "{}",
        r["warnings"]
    );
}
