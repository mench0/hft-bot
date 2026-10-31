//! Запись событий для аналитики и бэктестов («Исследования» в админке, настройки record*).
//!
//! Горячий путь (фиды, счета) только кладёт событие в очередь фиксированного размера без ожидания: выключено —
//! одна проверка флага; очередь полна — событие теряется (счётчик dropped), торговля никогда не ждёт записи.
//! Отдельный поток собирает пачки, кодирует (format.rs), сжимает zstd и пишет файлы по часам:
//! `recordDir/ГГГГ-ММ-ДД/ЧЧ.bin.zst` (время UTC). Дни старше recordKeepDays удаляются. Чтение — reader.rs и
//! `hft-bot record-cat <файл>`.

pub mod event;
pub mod format;
pub mod reader;
mod writer;

use crate::market::book::Book;
use event::{Event, Name, Rec};
use serde_json::{Map, Value, json};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::{Mutex, OnceLock};

/// Ёмкость очереди событий.
const QUEUE: usize = 65_536;

static ENABLED: AtomicBool = AtomicBool::new(false);
static DEPTH_LEVELS: AtomicUsize = AtomicUsize::new(0);
static DEPTH_MS: AtomicU64 = AtomicU64::new(1000);

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub enabled: bool,
    pub dir: PathBuf,
    pub keep_days: u64,
}

pub(crate) struct Shared {
    tx: SyncSender<Rec>,
    pub(crate) cfg: Mutex<Config>,
    pub(crate) stats: writer::Stats,
    queued: AtomicU64,
    dropped: AtomicU64,
}

static SHARED: OnceLock<Shared> = OnceLock::new();

fn shared(cfg: &Config) -> &'static Shared {
    SHARED.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::sync_channel(QUEUE);
        std::thread::Builder::new()
            .name("recorder".into())
            .spawn(move || writer::run(rx, &SHARED))
            .expect("поток записи");
        Shared {
            tx,
            cfg: Mutex::new(cfg.clone()),
            stats: writer::Stats::default(),
            queued: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    })
}

fn get(m: &Map<String, Value>, k: &str) -> Option<String> {
    m.get(k).map(|v| {
        v.as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| v.to_string())
    })
}

/// Настройки из настроек процесса (на лету): recordEnabled, recordDir, recordKeepDays, recordDepthLevels, recordDepthMs.
pub fn configure(settings: &Map<String, Value>) {
    let num = |k: &str, d: f64| {
        get(settings, k)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(d)
    };
    let cfg = Config {
        enabled: get(settings, "recordEnabled").is_some_and(|v| v == "true"),
        dir: get(settings, "recordDir")
            .filter(|d| !d.trim().is_empty())
            .unwrap_or_else(|| "data/rec".into())
            .into(),
        keep_days: num("recordKeepDays", 14.0).max(1.0) as u64,
    };
    DEPTH_LEVELS.store(num("recordDepthLevels", 0.0).max(0.0) as usize, Relaxed);
    DEPTH_MS.store(num("recordDepthMs", 1000.0).max(10.0) as u64, Relaxed);
    if !cfg.enabled && SHARED.get().is_none() {
        return; // поток записи не нужен, пока запись ни разу не включали
    }
    let s = shared(&cfg);
    let changed = {
        let mut c = s.cfg.lock().unwrap();
        let ch = *c != cfg;
        *c = cfg.clone();
        ch
    };
    if changed {
        tracing::info!(
            "Запись событий {}: {}",
            if cfg.enabled {
                "включена"
            } else {
                "выключена"
            },
            cfg.dir.display()
        );
    }
    ENABLED.store(cfg.enabled, Relaxed);
}

pub fn enabled() -> bool {
    ENABLED.load(Relaxed)
}

/// Положить событие в очередь (не ждёт).
pub fn push(ev: Event) {
    if !enabled() {
        return;
    }
    let Some(s) = SHARED.get() else { return };
    match s.tx.try_send(Rec {
        ts: event::now_ns(),
        ev,
    }) {
        Ok(()) => {
            s.queued.fetch_add(1, Relaxed);
        }
        Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
            s.dropped.fetch_add(1, Relaxed);
        }
    }
}

/// После обновления стакана: лучшие цены (если изменились) и, при recordDepthLevels > 0, снимок уровней не чаще
/// recordDepthMs по символу.
pub fn book(ex: &str, sym: &str, b: &mut Book) {
    if !enabled() {
        return;
    }
    let (Some(bid), Some(ask)) = (b.best_bid(), b.best_ask()) else {
        return;
    };
    let top = (bid.0, bid.1, ask.0, ask.1);
    if top != b.rec_top {
        b.rec_top = top;
        push(Event::Bbo {
            ex: Name::new(ex),
            sym: Name::new(sym),
            bid: bid.0,
            bid_qty: bid.1,
            ask: ask.0,
            ask_qty: ask.1,
        });
    }
    let n = DEPTH_LEVELS.load(Relaxed);
    if n > 0 {
        let now = crate::infra::util::now_ms();
        if now.saturating_sub(b.rec_depth_ms) >= DEPTH_MS.load(Relaxed) {
            b.rec_depth_ms = now;
            push(Event::Depth {
                ex: Name::new(ex),
                sym: Name::new(sym),
                bids: b.levels(false).take(n).collect(),
                asks: b.levels(true).take(n).collect(),
            });
        }
    }
}

/// Состояние записи для админки (/research/status).
pub fn status_json() -> Value {
    let Some(s) = SHARED.get() else {
        return json!({"enabled": false, "started": false, "events": 0, "dropped": 0, "bytes": 0, "queue": 0,
            "file": "", "error": "", "depthLevels": DEPTH_LEVELS.load(Relaxed)});
    };
    let cfg = s.cfg.lock().unwrap().clone();
    let (file, error) = (
        s.stats.file.lock().unwrap().clone(),
        s.stats.error.lock().unwrap().clone(),
    );
    let (q, w, d) = (
        s.queued.load(Relaxed),
        s.stats.written.load(Relaxed),
        s.dropped.load(Relaxed),
    );
    json!({"enabled": cfg.enabled, "started": true, "dir": cfg.dir.display().to_string(), "keepDays": cfg.keep_days,
        "events": w, "dropped": d, "queue": q.saturating_sub(w), "bytes": s.stats.bytes.load(Relaxed),
        "file": file, "error": error, "depthLevels": DEPTH_LEVELS.load(Relaxed)})
}

/// Метрики для /metrics: (включено, записано событий, потеряно, байт на диске, в очереди).
pub fn metrics() -> (bool, u64, u64, u64, u64) {
    match SHARED.get() {
        None => (false, 0, 0, 0, 0),
        Some(s) => {
            let w = s.stats.written.load(Relaxed);
            (
                enabled(),
                w,
                s.dropped.load(Relaxed),
                s.stats.bytes.load(Relaxed),
                s.queued.load(Relaxed).saturating_sub(w),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::event;
    use super::event::*;
    use super::format::*;

    fn n(s: &str) -> Name {
        Name::new(s)
    }

    #[test]
    fn encode_decode_round_trip_and_truncated_tail() {
        let evs = vec![
            Event::Bbo {
                ex: n("binance"),
                sym: n("BTCUSDT"),
                bid: 100.5,
                bid_qty: 1.0,
                ask: 100.6,
                ask_qty: 2.0,
            },
            Event::Depth {
                ex: n("okx"),
                sym: n("ETHUSDT"),
                bids: vec![(10.0, 1.0), (9.9, 2.0)],
                asks: vec![(10.1, 3.0)],
            },
            Event::Order {
                ex: n("binance"),
                sym: n("BTCUSDT"),
                id: 7,
                side: BUY,
                kind: kind::LIMIT,
                qty: 0.01,
                price: 100.5,
                status: status::NEW,
                executed: 0.0,
                avg: 0.0,
            },
            Event::Fill {
                ex: n("binance"),
                sym: n("BTCUSDT"),
                id: 7,
                side: BUY,
                qty: 0.01,
                price: 100.5,
                fee: 0.001,
            },
            Event::Cancel {
                ex: n("okx"),
                sym: n(""),
                id: 0,
                n: 3,
            },
            Event::FeedError { ex: n("okx") },
        ];
        let recs: Vec<Rec> = evs
            .into_iter()
            .enumerate()
            .map(|(i, ev)| Rec {
                ts: 1000 + i as u64,
                ev,
            })
            .collect();
        let mut out = header();
        let mut e = Encoder::default();
        for r in &recs {
            e.encode(&mut out, r);
        }
        assert_eq!(decode(&out).unwrap(), recs);
        // файл ещё пишется: последний кадр оборван — читаются целые
        assert_eq!(decode(&out[..out.len() - 3]).unwrap(), recs[..5].to_vec());
        assert!(decode(b"nope").is_err());
        assert!(n("абвгдежзийклмнопрстуф").as_str().len() <= 31);
    }

    #[test]
    fn writes_hourly_zstd_files_and_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, rx) = std::sync::mpsc::sync_channel(16);
        let stats = super::writer::Stats::default();
        let cfg = super::Config {
            enabled: true,
            dir: dir.path().into(),
            keep_days: 14,
        };
        let h = 3_600_000_000_000u64;
        // начало текущего часа: старые дни удаляются при открытии файла
        let t0 = event::now_ns() / h * h;
        for (i, ts) in [t0, t0 + 1, t0 + h].into_iter().enumerate() {
            tx.send(Rec {
                ts,
                ev: Event::Bbo {
                    ex: n("gate"),
                    sym: n("XUSDT"),
                    bid: i as f64,
                    bid_qty: 1.0,
                    ask: i as f64 + 1.0,
                    ask_qty: 1.0,
                },
            })
            .unwrap();
        }
        drop(tx);
        super::writer::run_with(rx, &|| cfg.clone(), &stats);
        let path = |ts: u64| {
            let iso = crate::infra::util::iso_ms(ts / 1_000_000);
            dir.path()
                .join(&iso[..10])
                .join(format!("{}.bin.zst", &iso[11..13]))
        };
        let (a, b) = (path(t0), path(t0 + h));
        assert_eq!(super::reader::read_file(&a).unwrap().len(), 2);
        assert_eq!(super::reader::read_file(&b).unwrap()[0].ts, t0 + h);
        assert_eq!(stats.written.load(std::sync::atomic::Ordering::Relaxed), 3);
        // старые дни удаляются, чужие папки не трогаются
        std::fs::create_dir_all(dir.path().join("2000-01-01")).unwrap();
        std::fs::create_dir_all(dir.path().join("keep-me")).unwrap();
        super::writer::cleanup(dir.path(), 14);
        assert!(!dir.path().join("2000-01-01").exists());
        assert!(dir.path().join("keep-me").exists());
    }

    /// Сквозной путь: стакан из фида и ордер бумажного счёта → очередь → файл; выключение закрывает файл.
    #[test]
    fn feed_and_account_events_reach_file() {
        use crate::trading::account::Account;
        use crate::trading::paper::{Amount, Limits, Paper, Side};
        let dir = tempfile::tempdir().unwrap();
        let mut st = serde_json::Map::new();
        st.insert("recordEnabled".into(), true.into());
        st.insert("recordDir".into(), dir.path().display().to_string().into());
        st.insert("recordDepthLevels".into(), 2.into());
        super::configure(&st);
        let m = crate::market::Market::default();
        let best = |bid: f64| crate::market::feed::Update::Best {
            sym: "RECUSDT".into(),
            bid: (bid, 5.0),
            ask: (100.0, 5.0),
            id: 0,
        };
        crate::market::feed::apply(&m, "rectest", best(99.0));
        crate::market::feed::apply(&m, "rectest", best(99.0)); // те же лучшие цены — не пишутся повторно
        crate::market::feed::apply(&m, "rectest", best(99.5));
        let mut a = Account::Paper(Paper::new(&["RECUSDT".into()], 1000.0, 0.0, 0.1));
        a.set_venue("rectest");
        let mut lim = Limits::from_params(&serde_json::Map::new());
        lim.trading_enabled = true;
        let b = m.book("rectest", "RECUSDT").unwrap();
        a.order("RECUSDT", Side::Buy, Amount::Qty(1.0), None, Some(&b), &lim)
            .unwrap();
        a.order(
            "RECUSDT",
            Side::Buy,
            Amount::Qty(1.0),
            Some(90.0),
            Some(&b),
            &lim,
        )
        .unwrap();
        assert_eq!(a.cancel_all(Some("RECUSDT")), 1);
        let t0 = std::time::Instant::now();
        while super::status_json()["queue"] != 0 && t0.elapsed().as_secs() < 5 {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        st.insert("recordEnabled".into(), false.into());
        super::configure(&st);
        super::push(Event::FeedError { ex: n("rectest") }); // выключено — не пишется
        // поток записи закрывает файл на следующем цикле
        std::thread::sleep(std::time::Duration::from_millis(600));
        let mut evs = Vec::new();
        for day in std::fs::read_dir(dir.path()).unwrap().flatten() {
            for f in std::fs::read_dir(day.path()).unwrap().flatten() {
                evs.extend(super::reader::read_file(&f.path()).unwrap());
            }
        }
        let mine: Vec<&Event> = evs
            .iter()
            .map(|r| &r.ev)
            .filter(|e| match e {
                Event::Bbo { ex, .. }
                | Event::Depth { ex, .. }
                | Event::Order { ex, .. }
                | Event::Fill { ex, .. }
                | Event::Cancel { ex, .. }
                | Event::FeedError { ex } => ex.as_str() == "rectest",
            })
            .collect();
        let count = |f: fn(&Event) -> bool| mine.iter().filter(|e| f(e)).count();
        assert_eq!(count(|e| matches!(e, Event::Bbo { .. })), 2, "{mine:?}");
        assert_eq!(
            count(|e| matches!(e, Event::Depth { .. })),
            1,
            "снимок не чаще recordDepthMs"
        );
        assert_eq!(count(|e| matches!(e, Event::Fill { side: BUY, .. })), 1);
        assert_eq!(
            count(|e| matches!(
                e,
                Event::Order {
                    status: status::FILLED,
                    kind: kind::MARKET,
                    ..
                }
            )),
            1
        );
        assert_eq!(
            count(|e| matches!(
                e,
                Event::Order {
                    status: status::NEW,
                    kind: kind::LIMIT,
                    ..
                }
            )),
            1
        );
        assert_eq!(count(|e| matches!(e, Event::Cancel { n: 1, id: 0, .. })), 1);
        assert_eq!(count(|e| matches!(e, Event::FeedError { .. })), 0);
        assert_eq!(super::status_json()["enabled"], false);
    }
}
