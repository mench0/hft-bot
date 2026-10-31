//! Часы бота: в живой торговле — реальное время, в бэктесте — время из записи (sim), чтобы бумажный счёт,
//! стакан и стратегии считали частоту ордеров, возраст данных и таймауты по времени рынка, а не по скорости прогона.
//! Симуляция включается на поток (thread_local): живой движок её не видит.

use std::cell::Cell;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

thread_local! {
    /// (Instant в начале симуляции, время начала симуляции, текущее время симуляции) — нс эпохи.
    static SIM: Cell<Option<(Instant, u64, u64)>> = const { Cell::new(None) };
}

/// Сейчас (монотонное).
pub fn now() -> Instant {
    match SIM.with(Cell::get) {
        Some((base, t0, t)) => base + Duration::from_nanos(t.saturating_sub(t0)),
        None => Instant::now(),
    }
}

/// Миллисекунды эпохи UTC.
pub fn now_ms() -> u64 {
    match SIM.with(Cell::get) {
        Some((_, _, t)) => t / 1_000_000,
        None => SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
    }
}

/// Идёт бэктест на этом потоке: запись и лента графиков его не видят.
pub fn simulated() -> bool {
    SIM.with(Cell::get).is_some()
}

/// Симуляция времени на этом потоке, пока жив guard.
pub struct Sim(());

impl Sim {
    /// Начать с момента ns (нс эпохи).
    pub fn start(ns: u64) -> Sim {
        SIM.with(|s| s.set(Some((Instant::now(), ns, ns))));
        Sim(())
    }

    /// Перевести часы вперёд (назад — не двигаются).
    pub fn set(&self, ns: u64) {
        SIM.with(|s| {
            if let Some((b, t0, t)) = s.get() {
                s.set(Some((b, t0, t.max(ns))));
            }
        });
    }
}

impl Drop for Sim {
    fn drop(&mut self) {
        SIM.with(|s| s.set(None));
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn sim_time_moves_only_by_set() {
        let real = super::now_ms();
        {
            let sim = super::Sim::start(1_000_000_000_000_000_000);
            let a = super::now();
            assert_eq!(super::now_ms(), 1_000_000_000_000);
            sim.set(1_000_000_000_060_000_000);
            assert_eq!(super::now().duration_since(a).as_millis(), 60);
            sim.set(1); // назад — нет
            assert_eq!(super::now_ms(), 1_000_000_000_060);
        }
        assert!(
            super::now_ms() >= real,
            "после симуляции — снова реальное время"
        );
    }
}
