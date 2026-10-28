//! Счёт биржи: бумажный или живой (Binance, Bybit), спот или перпы. Движок, стратегии и админка работают с ним одинаково.

use crate::exchange::rest::Filter;
use crate::market::book::Book;
use crate::trading::live::Live;
use crate::trading::paper::{Amount, Exec, Fill, Limits, Paper, Side};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, VecDeque};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // счетов единицы, хранятся в карте — размер не важен
pub enum Account {
    Paper(Paper),
    Live(Live),
}

impl Account {
    fn ledger(&self) -> &Paper {
        match self {
            Account::Paper(p) => p,
            Account::Live(l) => &l.ledger,
        }
    }

    fn ledger_mut(&mut self) -> &mut Paper {
        match self {
            Account::Paper(p) => p,
            Account::Live(l) => &mut l.ledger,
        }
    }

    /// Защитный стоп перпа (exchangeStopLossPercent): позиция есть — стоп на pct % от цены входа против неё на весь
    /// объём, нет — стопа нет. Бумага: цена дошла до стопа — рыночный выход; живой счёт — стоп ставится на бирже.
    pub fn protect(&mut self, symbol: &str, pct: f64, book: Option<&Book>) {
        let tick = self.filter(symbol).map(|f| f.tick).unwrap_or(0.0);
        let led = self.ledger_mut();
        let p = led.positions.get(symbol).cloned();
        let want = p
            .filter(|p| pct > 0.0 && p.qty.abs() > 1e-12 && p.entry > 0.0)
            .map(|p| {
                let long = p.qty > 0.0;
                let raw = p.entry
                    * if long {
                        1.0 - pct / 100.0
                    } else {
                        1.0 + pct / 100.0
                    };
                // по шагу цены, без него — 6 значащих цифр
                let px = if tick > 0.0 {
                    let r = if long {
                        (raw / tick).floor()
                    } else {
                        (raw / tick).ceil()
                    } * tick;
                    let d = (-tick.log10()).ceil().max(0.0) as usize;
                    format!("{r:.d$}").parse().unwrap_or(r)
                } else {
                    let d = (5 - raw.abs().log10().floor() as i32).max(0) as usize;
                    format!("{raw:.d$}").parse().unwrap_or(raw)
                };
                (if long { Side::Sell } else { Side::Buy }, p.qty.abs(), px)
            });
        match self {
            Account::Paper(pp) => {
                let Some((side, qty, px)) = want else {
                    pp.stops.remove(symbol);
                    return;
                };
                pp.stops.insert(symbol.to_owned(), (side, px));
                let hit = book.is_some_and(|b| match side {
                    Side::Sell => b.best_bid().is_some_and(|x| x.0 <= px),
                    Side::Buy => b.best_ask().is_some_and(|x| x.0 >= px),
                });
                if hit {
                    pp.stops.remove(symbol);
                    pp.stops_triggered += 1;
                    match pp.exit_market(symbol, side, qty, book) {
                        Ok(f) => tracing::warn!(
                            "[стоп] {symbol}: сработал стоп {px} — закрыто {} по {}",
                            f.executed,
                            f.avg_price
                        ),
                        Err(e) => tracing::warn!(
                            "[стоп] {symbol}: стоп {px} сработал, выход не удался: {e}"
                        ),
                    }
                }
            }
            Account::Live(l) => l.want_stop(symbol, want),
        }
    }

    /// Позиции перпов: (символ, объём со знаком, цена входа).
    pub fn positions(&mut self) -> Vec<(String, f64, f64)> {
        self.ledger_mut()
            .positions
            .iter()
            .map(|(s, p)| (s.clone(), p.qty, p.entry))
            .collect()
    }

    /// Символы, по которым есть позиция или стоп.
    pub fn stop_symbols(&mut self) -> Vec<String> {
        let led = self.ledger_mut();
        let mut v: Vec<String> = led
            .positions
            .keys()
            .chain(led.stops.keys())
            .cloned()
            .collect();
        if let Account::Live(l) = self {
            v.extend(l.stop_keys());
        }
        v.sort();
        v.dedup();
        v
    }

    /// Сокет ордеров живого счёта и входов, отклонённых без него.
    pub fn trade_link(&self) -> Option<(std::sync::Arc<crate::trading::trade_ws::TradeLink>, u64)> {
        match self {
            Account::Live(l) => l.trade.clone().map(|t| (t, l.ws_entries_blocked)),
            Account::Paper(_) => None,
        }
    }

    /// Запретить или разрешить новые входы (reason пусто — разрешены).
    pub fn block_entries(&mut self, reason: &str) {
        let led = self.ledger_mut();
        if led.entries_blocked != reason {
            if reason.is_empty() {
                tracing::info!(
                    "[{}] стакан снова по WebSocket — входы разрешены",
                    led.venue
                );
            } else {
                tracing::warn!("[{}] {reason}", led.venue);
            }
            led.entries_blocked = reason.to_owned();
        }
    }

    /// Причина запрета входов, отклонено входов и прошло выходов, пока сокет стакана не был подключён.
    pub fn rest_gate(&self) -> (String, u64, u64) {
        let l = self.ledger();
        (
            l.entries_blocked.clone(),
            l.rest_entries_blocked,
            l.rest_exits,
        )
    }

    /// "PAPER", "TESTNET" или "LIVE".
    pub fn mode(&self) -> &'static str {
        match self {
            Account::Paper(_) => "PAPER",
            Account::Live(l) => l.mode,
        }
    }

    pub fn order(
        &mut self,
        symbol: &str,
        side: Side,
        amount: Amount,
        limit: Option<f64>,
        book: Option<&Book>,
        lim: &Limits,
    ) -> Result<Fill, String> {
        // сокет стакана не подключён: новые входы запрещены, выход (продажа спота, уменьшение позиции перпа) — можно
        if !self.ledger().entries_blocked.is_empty() {
            let room = self.ledger().exit_room(&symbol.to_uppercase(), side);
            let exit = match amount {
                Amount::Qty(q) => q <= room + 1e-12,
                Amount::Portion(_) | Amount::All => room > 0.0,
            };
            let led = self.ledger_mut();
            if !exit {
                led.rest_entries_blocked += 1;
                let e = led.entries_blocked.clone();
                led.note_rejected(&e);
                return Err(e);
            }
            led.rest_exits += 1;
        }
        let r = match self {
            Account::Paper(p) => p.order(symbol, side, amount, limit, book, lim),
            Account::Live(l) => l.order(symbol, side, amount, limit, book, lim),
        };
        if crate::infra::journal::on() {
            let what = format!(
                "ордер {side:?} {amount:?}{}",
                limit.map_or(" по рынку".into(), |p| format!(" лимит {p}"))
            );
            let res = match &r {
                Ok(f) => format!("{}: исполнено {} по {}", f.status, f.executed, f.avg_price),
                Err(e) => format!("отказ: {e}"),
            };
            crate::infra::journal::action(
                &self.ledger().venue,
                Some(symbol),
                &format!("{what} → {res}"),
            );
        }
        r
    }

    /// Задать биржу счёта (для журнала).
    pub fn set_venue(&mut self, ex: &str) {
        self.ledger_mut().venue = ex.to_owned();
    }

    pub fn sell_exit(
        &mut self,
        symbol: &str,
        qty: f64,
        book: Option<&Book>,
    ) -> Result<Fill, String> {
        match self {
            Account::Paper(p) => p.sell_exit(symbol, qty, book),
            Account::Live(l) => l.sell_exit(symbol, qty, book),
        }
    }

    /// Рыночный выход без проверок торговли (спот — продажа, перп — уменьшение позиции в любую сторону).
    pub fn exit_market(
        &mut self,
        symbol: &str,
        side: Side,
        qty: f64,
        book: Option<&Book>,
    ) -> Result<Fill, String> {
        match self {
            Account::Paper(p) => p.exit_market(symbol, side, qty, book),
            Account::Live(l) => l.exit_market(symbol, side, qty, book),
        }
    }

    pub fn is_live(&self) -> bool {
        matches!(self, Account::Live(_))
    }

    /// Бумажный счёт.
    #[cfg(test)]
    pub fn paper(&mut self) -> Option<&mut Paper> {
        match self {
            Account::Paper(p) => Some(p),
            Account::Live(_) => None,
        }
    }

    /// Позиция перпа по символу (спот — 0).
    pub fn position_qty(&self, symbol: &str) -> f64 {
        self.ledger().position(symbol).qty
    }

    /// Счёт перпов.
    pub fn is_perp(&self) -> bool {
        self.ledger().leverage().is_some()
    }

    /// Комиссия тейкера, доля.
    pub fn taker_fee(&self) -> f64 {
        self.ledger().fees().1
    }

    pub fn cancel(&mut self, id: u64) -> bool {
        match self {
            Account::Paper(p) => p.cancel(id),
            Account::Live(l) => l.cancel(id),
        }
    }

    pub fn is_open(&self, id: u64) -> bool {
        match self {
            Account::Paper(p) => p.is_open(id),
            Account::Live(l) => l.is_open(id),
        }
    }

    pub fn cancel_all(&mut self, symbol: Option<&str>) -> usize {
        let n = match self {
            Account::Paper(p) => p.cancel_all(symbol),
            Account::Live(l) => l.cancel_all(symbol),
        };
        if n > 0 {
            crate::infra::journal::action(
                &self.ledger().venue,
                symbol,
                &format!("снято заявок: {n}"),
            );
        }
        n
    }

    pub fn open_orders(&self) -> usize {
        match self {
            Account::Paper(p) => p.open_orders(),
            Account::Live(l) => l.open_orders(),
        }
    }

    /// Исполнить стоящие заявки по стакану (только бумага; живые исполняет биржа).
    pub fn match_resting(&mut self, symbol: &str, book: &Book, lim: &Limits) {
        match self {
            Account::Paper(p) => p.match_resting(symbol, book, lim),
            Account::Live(l) => l.lim = lim.clone(),
        }
    }

    /// Стартовый баланс валюты новой монеты (только бумага).
    pub fn ensure_quote(&mut self, symbol: &str, start: f64) {
        if let Account::Paper(p) = self {
            p.ensure_quote(symbol, start)
        }
    }

    pub fn execs(&self) -> &VecDeque<Exec> {
        &self.ledger().execs
    }
    pub fn free(&self, asset: &str) -> f64 {
        self.ledger().free(asset)
    }
    pub fn balances(&self) -> BTreeMap<String, f64> {
        self.ledger().balances()
    }
    pub fn filter(&self, symbol: &str) -> Option<Filter> {
        self.ledger().filter(symbol)
    }
    pub fn set_filters(&mut self, f: &HashMap<String, Filter>) {
        self.ledger_mut().set_filters(f)
    }
    pub fn reset_kill_switch(&mut self) {
        self.ledger_mut().kill_switch = false
    }

    /// Счёт перпов с плечом.
    pub fn set_perp(&mut self, leverage: f64) {
        self.ledger_mut().set_perp(leverage)
    }

    /// Funding по позиции (только бумага: на живом счёте его списывает биржа, балансы приходят с неё).
    pub fn apply_funding(&mut self, symbol: &str, rate: f64, mark: f64) -> f64 {
        match self {
            Account::Paper(p) => p.apply_funding(symbol, rate, mark),
            Account::Live(_) => 0.0,
        }
    }

    /// Показатели для /status: kill switch, дневной результат, принято/отклонено, последний отказ.
    pub fn stats(&self) -> Value {
        let l = self.ledger();
        let mut v = json!({"kill_switch": l.kill_switch, "дневной_pnl": l.daily_pnl, "ордеров_принято": l.accepted,
            "ордеров_отклонено": l.rejected, "последний_отказ": l.last_reject});
        if let Account::Live(live) = self {
            v["поток_событий_счёта"] = Value::Bool(live.stream_ok);
            v["событий_счёта"] = Value::from(live.stream_events);
            if !live.stream_error.is_empty() {
                v["ошибка_потока_счёта"] = Value::from(live.stream_error.clone());
            }
        }
        v
    }

    /// Тестовый счёт (бумага и testnet; у реального счёта — None, как в Java).
    pub fn account_json(&mut self, mids: &HashMap<String, f64>) -> Option<Value> {
        let mode = self.mode();
        if mode == "LIVE" {
            return None;
        }
        let mut v = self.ledger_mut().account_json(mids);
        v["mode"] = Value::from(mode);
        v["openOrders"] = Value::from(self.open_orders()); // у живого счёта ордера — на бирже, не в учёте
        Some(v)
    }

    /// Ошибка связи с биржей (живой счёт).
    pub fn error(&self) -> &str {
        match self {
            Account::Paper(_) => "",
            Account::Live(l) => &l.error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trading::paper::Amount;

    fn book(bid: f64, ask: f64) -> Book {
        let mut b = Book::default();
        b.apply_snapshot(vec![(bid, 100.0)], vec![(ask, 100.0)], 0);
        b
    }

    #[test]
    fn book_socket_down_blocks_entries_but_not_exits() {
        let mut a = Account::Paper(Paper::new(&["XUSDT".into()], 1000.0, 0.0, 0.0));
        a.set_perp(2.0);
        let mut lim = Limits::from_params(&serde_json::Map::new());
        lim.trading_enabled = true;
        let b = book(99.9, 100.0);
        a.order("XUSDT", Side::Buy, Amount::Qty(1.0), None, Some(&b), &lim)
            .unwrap();
        a.block_entries("сокет стакана не подключён");
        // добавить к лонгу или открыть шорт больше позиции — вход
        assert_eq!(
            a.order("XUSDT", Side::Buy, Amount::Qty(0.5), None, Some(&b), &lim)
                .unwrap_err(),
            "сокет стакана не подключён"
        );
        assert!(
            a.order("XUSDT", Side::Sell, Amount::Qty(2.0), None, Some(&b), &lim)
                .is_err()
        );
        // уменьшить лонг — выход
        a.order("XUSDT", Side::Sell, Amount::Qty(1.0), None, Some(&b), &lim)
            .unwrap();
        assert!(a.position_qty("XUSDT").abs() < 1e-9);
        assert_eq!(a.rest_gate(), ("сокет стакана не подключён".into(), 2, 1));
        a.block_entries("");
        a.order("XUSDT", Side::Buy, Amount::Qty(0.5), None, Some(&b), &lim)
            .unwrap();
    }

    #[test]
    fn paper_stop_follows_position_and_fires() {
        let mut a = Account::Paper(Paper::new(&["XUSDT".into()], 1000.0, 0.0, 0.0));
        a.set_perp(2.0);
        let mut lim = Limits::from_params(&serde_json::Map::new());
        lim.trading_enabled = true;
        a.order(
            "XUSDT",
            Side::Buy,
            Amount::Qty(1.0),
            None,
            Some(&book(99.9, 100.0)),
            &lim,
        )
        .unwrap();
        // лонг от 100: стоп на 2 % ниже
        a.protect("XUSDT", 2.0, Some(&book(99.0, 99.1)));
        assert_eq!(
            a.paper().unwrap().stops.get("XUSDT"),
            Some(&(Side::Sell, 98.0))
        );
        assert!(a.position_qty("XUSDT") > 0.99);
        // бид дошёл до стопа — позиция закрыта рыночным
        a.protect("XUSDT", 2.0, Some(&book(97.9, 98.0)));
        assert!(a.position_qty("XUSDT").abs() < 1e-9);
        let p = a.paper().unwrap();
        assert_eq!((p.stops_triggered, p.stops.len()), (1, 0));
        // выключено — стопа нет
        let mut b = Account::Paper(Paper::new(&["XUSDT".into()], 1000.0, 0.0, 0.0));
        b.set_perp(2.0);
        b.order(
            "XUSDT",
            Side::Sell,
            Amount::Qty(1.0),
            None,
            Some(&book(100.0, 100.1)),
            &lim,
        )
        .unwrap();
        b.protect("XUSDT", 0.0, Some(&book(150.0, 150.1)));
        assert!(b.position_qty("XUSDT") < -0.99);
    }
}
