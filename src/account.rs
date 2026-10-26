//! Счёт биржи: бумажный или живой (Binance, Bybit), спот или перпы. Движок, стратегии и админка работают с ним одинаково.

use crate::book::Book;
use crate::live::Live;
use crate::paper::{Amount, Exec, Fill, Limits, Paper, Side};
use crate::rest::Filter;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, VecDeque};

#[derive(Debug)]
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
        match self {
            Account::Paper(p) => p.order(symbol, side, amount, limit, book, lim),
            Account::Live(l) => l.order(symbol, side, amount, limit, book, lim),
        }
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
        match self {
            Account::Paper(p) => p.cancel_all(symbol),
            Account::Live(l) => l.cancel_all(symbol),
        }
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
