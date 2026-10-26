//! Локальный стакан символа: уровни из снимков depth и лучшие цены из bookTicker.
//! Обновление с номером меньше уже применённого не применяется — потоки приходят вперемешку.

use std::time::Instant;

/// Стакан: биды по убыванию цены, аски по возрастанию.
#[derive(Debug, Clone, Default)]
pub struct Book {
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
    /// Номер последнего применённого обновления у биржи (0 — ещё не было).
    update_id: u64,
    /// Когда стакан последний раз обновлялся (локальное время).
    updated: Option<Instant>,
    /// Сколько обновлений отброшено как устаревшие.
    pub stale: u64,
}

impl Book {
    /// Снимок стакана (частичный: N лучших уровней каждой стороны). false — устарел, не применён.
    pub fn apply_snapshot(
        &mut self,
        mut bids: Vec<(f64, f64)>,
        mut asks: Vec<(f64, f64)>,
        update_id: u64,
    ) -> bool {
        if update_id != 0 && update_id < self.update_id {
            self.stale += 1;
            return false;
        }
        bids.retain(|l| l.1 > 0.0);
        asks.retain(|l| l.1 > 0.0);
        bids.sort_by(|a, b| b.0.total_cmp(&a.0));
        asks.sort_by(|a, b| a.0.total_cmp(&b.0));
        self.bids = bids;
        self.asks = asks;
        self.update_id = update_id.max(self.update_id);
        self.updated = Some(Instant::now());
        true
    }

    /// Лучшие цены (bookTicker): верх стакана заменяется, уровни лучше новых лучших цен убираются. false — устарели.
    pub fn apply_best(
        &mut self,
        bid: f64,
        bid_qty: f64,
        ask: f64,
        ask_qty: f64,
        update_id: u64,
    ) -> bool {
        if update_id != 0 && update_id < self.update_id {
            self.stale += 1;
            return false;
        }
        self.bids.retain(|l| l.0 < bid);
        self.bids.insert(0, (bid, bid_qty));
        self.asks.retain(|l| l.0 > ask);
        self.asks.insert(0, (ask, ask_qty));
        self.update_id = update_id.max(self.update_id);
        self.updated = Some(Instant::now());
        true
    }

    /// Есть обе стороны и они не перекрещены.
    pub fn is_ready(&self) -> bool {
        matches!((self.bids.first(), self.asks.first()), (Some(b), Some(a)) if b.0 < a.0)
    }

    pub fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids.first().copied()
    }
    pub fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks.first().copied()
    }

    /// Середина цены.
    pub fn mid(&self) -> Option<f64> {
        Some((self.best_bid()?.0 + self.best_ask()?.0) / 2.0)
    }

    /// Спред в процентах от середины.
    pub fn spread_percent(&self) -> Option<f64> {
        let (b, a) = (self.best_bid()?.0, self.best_ask()?.0);
        Some((a - b) / ((a + b) / 2.0) * 100.0)
    }

    /// Перевес бидов над асками по объёму N лучших уровней: от −1 (одни аски) до 1 (одни биды).
    pub fn imbalance(&self, levels: usize) -> f64 {
        let b: f64 = self.bids.iter().take(levels).map(|l| l.1).sum();
        let a: f64 = self.asks.iter().take(levels).map(|l| l.1).sum();
        if b + a > 0.0 { (b - a) / (b + a) } else { 0.0 }
    }

    /// Сколько миллисекунд назад было последнее обновление (None — не было).
    pub fn age_ms(&self) -> Option<u64> {
        self.updated.map(|t| t.elapsed().as_millis() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_sorted_and_ready() {
        let mut b = Book::default();
        assert!(!b.is_ready());
        assert!(b.apply_snapshot(
            vec![(99.0, 1.0), (100.0, 2.0), (98.0, 0.0)],
            vec![(102.0, 1.0), (101.0, 3.0)],
            10
        ));
        assert_eq!(b.best_bid(), Some((100.0, 2.0)));
        assert_eq!(b.best_ask(), Some((101.0, 3.0)));
        assert!(b.is_ready());
        assert_eq!(b.mid(), Some(100.5));
        assert!((b.spread_percent().unwrap() - 0.995).abs() < 1e-3);
    }

    #[test]
    fn best_prices_replace_top_and_stale_dropped() {
        let mut b = Book::default();
        b.apply_snapshot(
            vec![(100.0, 2.0), (99.0, 1.0)],
            vec![(101.0, 3.0), (102.0, 1.0)],
            10,
        );
        assert!(b.apply_best(99.5, 4.0, 101.5, 5.0, 11)); // лучшие цены отошли: уровни 100 и 101 съели
        assert_eq!(b.best_bid(), Some((99.5, 4.0)));
        assert_eq!(b.best_ask(), Some((101.5, 5.0)));
        assert!(!b.apply_snapshot(vec![(100.0, 1.0)], vec![(101.0, 1.0)], 9)); // старый снимок
        assert_eq!(b.best_bid(), Some((99.5, 4.0)));
        assert_eq!(b.stale, 1);
    }

    #[test]
    fn imbalance_range() {
        let mut b = Book::default();
        b.apply_snapshot(vec![(100.0, 3.0)], vec![(101.0, 1.0)], 1);
        assert!((b.imbalance(5) - 0.5).abs() < 1e-12);
    }
}
