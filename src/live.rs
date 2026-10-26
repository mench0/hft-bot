//! Живой счёт Binance или Bybit (спот, основная сеть или testnet). Ордер проверяется теми же правилами, что бумажный,
//! и уходит на биржу в фоновой задаче — движок не ждёт сеть. Статусы своих ордеров опрашиваются раз в 0.5 с,
//! балансы — раз в 5 с. Исполнения учитываются тем же учётом, что на бумаге (средняя цена, результат, журнал).

use crate::book::Book;
use crate::paper::{Amount, Fill, Limits, Paper, Side};
use crate::signed::OrderState;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// Команда фоновой задаче.
#[derive(Debug, Clone, PartialEq)]
pub enum Cmd {
    Place {
        local: u64,
        symbol: String,
        side: Side,
        qty: f64,
        price: Option<f64>,
        /// Ориентир цены (рыночная покупка Gate задаётся суммой в котируемой валюте).
        ref_px: f64,
    },
    Cancel {
        local: u64,
    },
    /// Узнать сети монеты.
    Networks {
        coin: String,
    },
    /// Узнать адрес депозита монеты в сети.
    Address {
        coin: String,
        network: String,
    },
    /// Вывести монету (номер перевода — свой).
    Withdraw {
        tid: u64,
        coin: String,
        network: String,
        address: String,
        tag: String,
        amount: f64,
    },
}

#[derive(Debug)]
struct Order {
    symbol: String,
    side: Side,
    /// Лимитный ордер — комиссия мейкера (оценка: биржа может взять тейкера, если ордер сразу исполнился).
    maker: bool,
    exch: Option<String>,
    executed: f64,
    quote: f64,
    done: bool,
    cancel: bool,
}

/// Живой счёт биржи.
#[derive(Debug)]
pub struct Live {
    pub ledger: Paper,
    orders: HashMap<u64, Order>,
    next: u64,
    tx: mpsc::UnboundedSender<Cmd>,
    /// "LIVE" или "TESTNET".
    pub mode: &'static str,
    /// Лимиты (для kill switch при учёте исполнений из фоновой задачи).
    pub lim: Limits,
    /// Последняя ошибка связи с биржей.
    pub error: String,
    /// Поток событий счёта (WebSocket API) подключён и подписан — статусы приходят сами, опрос редкий.
    pub stream_ok: bool,
    /// Событий счёта получено.
    pub stream_events: u64,
    /// Последняя ошибка приватного потока (не стирается удачным опросом балансов).
    pub stream_error: String,
    /// Сети монет (ответы на Cmd::Networks): Err — ошибка запроса.
    pub networks: HashMap<String, Result<Vec<crate::transfer::Net>, String>>,
    /// Адреса депозита по (монета, сеть).
    pub addresses: HashMap<(String, String), Result<(String, String), String>>,
    /// Итог выводов по своему номеру: номер на бирже или ошибка.
    pub withdrawals: HashMap<u64, Result<String, String>>,
}

impl Live {
    pub fn new(
        symbols: &[String],
        maker_pct: f64,
        taker_pct: f64,
        mode: &'static str,
        tx: mpsc::UnboundedSender<Cmd>,
    ) -> Live {
        Live {
            ledger: Paper::new(symbols, 0.0, maker_pct, taker_pct),
            orders: HashMap::new(),
            next: 1,
            tx,
            mode,
            lim: Limits::from_params(&serde_json::Map::new()),
            error: String::new(),
            stream_ok: false,
            stream_events: 0,
            stream_error: String::new(),
            networks: HashMap::new(),
            addresses: HashMap::new(),
            withdrawals: HashMap::new(),
        }
    }

    fn send(
        &mut self,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
        ref_px: f64,
    ) -> Fill {
        let local = self.next;
        self.next += 1;
        self.orders.insert(
            local,
            Order {
                symbol: symbol.to_uppercase(),
                side,
                maker: price.is_some(),
                exch: None,
                executed: 0.0,
                quote: 0.0,
                done: false,
                cancel: false,
            },
        );
        let _ = self.tx.send(Cmd::Place {
            local,
            symbol: symbol.to_uppercase(),
            side,
            qty,
            price,
            ref_px,
        });
        Fill {
            order_id: local,
            status: "NEW",
            executed: 0.0,
            avg_price: 0.0,
        }
    }

    /// Ордер: проверки как на бумаге и баланс счёта; уходит на биржу в фоне (статус NEW, исполнение — позже).
    pub fn order(
        &mut self,
        symbol: &str,
        side: Side,
        amount: Amount,
        limit: Option<f64>,
        book: Option<&Book>,
        lim: &Limits,
    ) -> Result<Fill, String> {
        self.lim = lim.clone();
        let pr = self
            .ledger
            .prepare(symbol, side, amount, limit, book, lim)?;
        let (_, taker) = self.ledger.fees();
        let (asset, need) = self
            .ledger
            .need(symbol, &pr.base, &pr.quote, side, pr.qty, pr.ref_px, taker);
        if self.ledger.free(&asset) + 1e-12 < need {
            let e = format!(
                "недостаточно {asset}: нужно {need:.8}, свободно {:.8}",
                self.ledger.free(&asset)
            );
            self.ledger.note_rejected(&e);
            return Err(e);
        }
        self.ledger.note_accepted();
        Ok(self.send(symbol, side, pr.qty, pr.limit, pr.ref_px))
    }

    /// Выход рыночной продажей без проверок торговли (стоп, выключение стратегии).
    pub fn sell_exit(
        &mut self,
        symbol: &str,
        qty: f64,
        book: Option<&Book>,
    ) -> Result<Fill, String> {
        self.exit_market(symbol, Side::Sell, qty, book)
    }

    /// Рыночный выход без проверок торговли: спот — продажа, перп — уменьшение позиции.
    pub fn exit_market(
        &mut self,
        symbol: &str,
        side: Side,
        qty: f64,
        book: Option<&Book>,
    ) -> Result<Fill, String> {
        let b = book.filter(|b| b.is_ready()).ok_or("стакан не готов")?;
        let ref_px = if side == Side::Buy {
            b.best_ask()
        } else {
            b.best_bid()
        }
        .map_or(0.0, |x| x.0);
        let qty = qty.min(self.ledger.exit_room(symbol, side));
        let qty = self
            .ledger
            .filter(&symbol.to_uppercase())
            .map_or(qty, |f| f.round_qty(qty));
        if qty <= 0.0 {
            return Err(format!("нечего закрывать по {symbol}"));
        }
        Ok(self.send(symbol, side, qty, None, ref_px))
    }

    /// Снять ордер; false — его уже нет.
    pub fn cancel(&mut self, local: u64) -> bool {
        match self.orders.get_mut(&local) {
            Some(o) if !o.done && !o.cancel => {
                o.cancel = true;
                let _ = self.tx.send(Cmd::Cancel { local });
                true
            }
            _ => false,
        }
    }

    /// Стоит ли ордер (снятие запрошено — уже не считается стоящим).
    pub fn is_open(&self, local: u64) -> bool {
        self.orders
            .get(&local)
            .is_some_and(|o| !o.done && !o.cancel)
    }

    pub fn open_orders(&self) -> usize {
        self.orders
            .values()
            .filter(|o| !o.done && !o.cancel)
            .count()
    }

    /// Снять ордера символа (None — все).
    pub fn cancel_all(&mut self, symbol: Option<&str>) -> usize {
        let ids: Vec<u64> = self
            .orders
            .iter()
            .filter(|(_, o)| {
                !o.done && !o.cancel && symbol.is_none_or(|s| o.symbol == s.to_uppercase())
            })
            .map(|(id, _)| *id)
            .collect();
        ids.iter().filter(|id| self.cancel(**id)).count()
    }

    /// Ответ биржи по ордеру: номер, новое исполнение (в учёт), конец жизни ордера.
    pub fn on_state(&mut self, local: u64, st: &OrderState) {
        let Some(o) = self.orders.get_mut(&local) else {
            return;
        };
        if st.executed < o.executed - 1e-12 {
            return; // устаревший ответ (опрос пришёл позже события) — исполнение уже учтено
        }
        if !st.id.is_empty() {
            o.exch = Some(st.id.clone());
        }
        let (dq, dquote) = (st.executed - o.executed, st.quote - o.quote);
        o.executed = st.executed;
        o.quote = st.quote;
        if st.terminal() {
            o.done = true;
        }
        let (symbol, side, maker) = (o.symbol.clone(), o.side, o.maker);
        if dq > 1e-12 {
            let (m, t) = self.ledger.fees();
            let fee = dquote * if maker { m } else { t };
            self.ledger.ledger_fill(
                local,
                &symbol,
                side,
                dq,
                dquote / dq,
                fee,
                maker,
                &self.lim.clone(),
            );
        }
        if st.status == "REJECTED" || st.status == "EXPIRED" {
            self.ledger.note_rejected(&format!("биржа: {}", st.status));
        }
    }

    /// Событие счёта: executionReport — состояние своего ордера (по clientOrderId hft<номер>), outboundAccountPosition — балансы.
    pub fn on_event(&mut self, e: &serde_json::Value) {
        self.stream_events += 1;
        let n = |k: &str| {
            e[k].as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0)
        };
        match e["e"].as_str() {
            Some("executionReport") => {
                // при снятии c — номер запроса на снятие, исходный — в C
                let status = e["X"].as_str().unwrap_or("");
                let client = match e["C"].as_str() {
                    Some(c) if !c.is_empty() && status == "CANCELED" => c,
                    _ => e["c"].as_str().unwrap_or(""),
                };
                let Some(local) = client
                    .strip_prefix("hft")
                    .and_then(|x| x.parse::<u64>().ok())
                else {
                    return;
                };
                let Some(id) = e["i"].as_u64() else { return };
                self.on_state(
                    local,
                    &OrderState {
                        id: id.to_string(),
                        status: status.to_owned(),
                        executed: n("z"),
                        quote: n("Z"),
                    },
                );
            }
            // перпы: ORDER_TRADE_UPDATE — ордер (z — исполнено, ap — средняя цена), ACCOUNT_UPDATE — позиции
            Some("ORDER_TRADE_UPDATE") => {
                let o = &e["o"];
                let Some(local) = o["c"]
                    .as_str()
                    .and_then(|c| c.strip_prefix("hft"))
                    .and_then(|x| x.parse::<u64>().ok())
                else {
                    return;
                };
                let n = |k: &str| {
                    o[k].as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0)
                };
                let Some(id) = o["i"].as_u64() else { return };
                self.on_state(
                    local,
                    &OrderState {
                        id: id.to_string(),
                        status: o["X"].as_str().unwrap_or("").to_owned(),
                        executed: n("z"),
                        quote: n("z") * n("ap"),
                    },
                );
            }
            Some("ACCOUNT_UPDATE") => {
                for p in e["a"]["P"].as_array().into_iter().flatten() {
                    let n = |k: &str| {
                        p[k].as_str()
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0)
                    };
                    if let Some(s) = p["s"].as_str() {
                        self.ledger.set_position(s, n("pa"), n("ep"));
                    }
                }
            }
            Some("outboundAccountPosition") => {
                for b in e["B"].as_array().into_iter().flatten() {
                    let f = b["f"]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    if let Some(a) = b["a"].as_str() {
                        self.ledger.set_balance(a, f);
                    }
                }
            }
            _ => {}
        }
    }

    /// Событие приватного потока Bybit: order — состояние своих ордеров (orderLinkId hft<номер>), wallet — балансы.
    pub fn on_bybit(&mut self, topic: &str, data: &serde_json::Value) {
        self.stream_events += 1;
        for d in data.as_array().into_iter().flatten() {
            match topic {
                "order" => {
                    let local = d["orderLinkId"]
                        .as_str()
                        .and_then(|c| c.strip_prefix("hft"))
                        .and_then(|x| x.parse::<u64>().ok());
                    if let (Some(local), Ok(st)) = (local, crate::bybit_api::order_state(d)) {
                        self.on_state(local, &st);
                    }
                }
                "position" => {
                    if let Some((s, q, e)) = crate::bybit_api::position(d) {
                        self.ledger.set_position(&s, q, e);
                    }
                }
                "wallet" => {
                    for c in d["coin"].as_array().into_iter().flatten() {
                        if let Some(a) = c["coin"].as_str() {
                            self.ledger.set_balance(a, crate::bybit_api::free_of(c));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Запросить у биржи сети, адрес или вывод (ответ — в networks / addresses / withdrawals).
    pub fn request(&self, cmd: Cmd) {
        let _ = self.tx.send(cmd);
    }

    /// Позиции с биржи: заменяют учётные (у кого на бирже позиции нет — закрыта).
    pub fn sync_positions(&mut self, ps: &[(String, f64, f64)]) {
        let known: Vec<String> = self.ledger.positions.keys().cloned().collect();
        for k in known {
            if !ps.iter().any(|p| p.0 == k) {
                self.ledger.set_position(&k, 0.0, 0.0);
            }
        }
        for (s, q, e) in ps {
            self.ledger.set_position(s, *q, *e);
        }
    }

    /// Биржа отказала в ордере.
    pub fn on_error(&mut self, local: u64, err: &str) {
        if let Some(o) = self.orders.get_mut(&local) {
            o.done = true;
        }
        self.ledger.note_rejected(err);
        self.error = err.to_owned();
    }

    /// Ордера, которые надо опросить: стоят на бирже (номер известен) и не закончились.
    fn pending(&self) -> Vec<(u64, String, String)> {
        self.orders
            .iter()
            .filter(|(_, o)| !o.done)
            .filter_map(|(id, o)| Some((*id, o.symbol.clone(), o.exch.clone()?)))
            .collect()
    }

    /// Снятие запрошено, а номер ордера на бирже уже известен.
    fn cancel_target(&self, local: u64) -> Option<(String, String)> {
        self.orders
            .get(&local)
            .filter(|o| !o.done)
            .and_then(|o| Some((o.symbol.clone(), o.exch.clone()?)))
    }

    fn wants_cancel(&self, local: u64) -> bool {
        self.orders.get(&local).is_some_and(|o| o.cancel && !o.done)
    }

    /// Забыть закончившиеся ордера старше последних 1000.
    fn trim(&mut self) {
        if self.orders.len() > 2000 {
            let mut done: Vec<u64> = self
                .orders
                .iter()
                .filter(|(_, o)| o.done)
                .map(|(id, _)| *id)
                .collect();
            done.sort_unstable();
            for id in done.iter().take(done.len().saturating_sub(1000)) {
                self.orders.remove(id);
            }
        }
    }
}

/// Доступ фоновой задачи к живому счёту биржи.
pub type Shared = Arc<Mutex<HashMap<String, crate::account::Account>>>;

fn with_live<R>(acc: &Shared, ex: &str, f: impl FnOnce(&mut Live) -> R) -> Option<R> {
    match acc.lock().unwrap().get_mut(ex) {
        Some(crate::account::Account::Live(l)) => Some(f(l)),
        _ => None,
    }
}

/// Торговый API биржи для живого счёта.
pub enum Venue {
    Binance(crate::signed::Client),
    Bybit(crate::bybit_api::Client),
    Aster(crate::aster_api::Client),
    Okx(crate::okx_api::Client),
    Gate(crate::gate_api::Client),
    Kucoin(crate::kucoin_api::Client),
    Bingx(crate::bingx_api::Client),
    Mexc(crate::mexc_api::Client),
    Hyperliquid(crate::hyperliquid_api::Client),
    Lbank(crate::lbank_api::Client),
}

impl Venue {
    async fn sync_time(&self) -> Result<(), String> {
        match self {
            Venue::Binance(c) => c.sync_time().await,
            Venue::Bybit(c) => c.sync_time().await,
            Venue::Aster(_) => Ok(()),
            Venue::Okx(c) => c.sync_time().await,
            Venue::Gate(c) => c.sync_time().await,
            Venue::Kucoin(c) => c.sync_time().await,
            Venue::Bingx(c) => c.sync_time().await,
            Venue::Mexc(c) => c.sync_time().await,
            Venue::Hyperliquid(c) => c.sync_time().await,
            Venue::Lbank(c) => c.sync_time().await,
        }
    }
    async fn place(
        &self,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
        ref_px: f64,
    ) -> Result<OrderState, String> {
        match self {
            Venue::Gate(c) => c.place(local, symbol, side, qty, price, ref_px).await,
            Venue::Kucoin(c) => c.place(local, symbol, side, qty, price).await,
            Venue::Bingx(c) => c.place(local, symbol, side, qty, price).await,
            Venue::Mexc(c) => c.place(local, symbol, side, qty, price).await,
            Venue::Hyperliquid(c) => c.place(local, symbol, side, qty, price, ref_px).await,
            Venue::Lbank(c) => c.place(local, symbol, side, qty, price, ref_px).await,
            Venue::Binance(c) => c.place(local, symbol, side, qty, price).await,
            Venue::Bybit(c) => c.place(local, symbol, side, qty, price).await,
            Venue::Aster(c) => c.place(local, symbol, side, qty, price).await,
            Venue::Okx(c) => c.place(local, symbol, side, qty, price).await,
        }
    }
    async fn query(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        match self {
            Venue::Binance(c) => c.query(symbol, id).await,
            Venue::Bybit(c) => c.query(symbol, id).await,
            Venue::Aster(c) => c.query(symbol, id).await,
            Venue::Okx(c) => c.query(symbol, id).await,
            Venue::Gate(c) => c.query(symbol, id).await,
            Venue::Kucoin(c) => c.query(symbol, id).await,
            Venue::Bingx(c) => c.query(symbol, id).await,
            Venue::Mexc(c) => c.query(symbol, id).await,
            Venue::Hyperliquid(c) => c.query(symbol, id).await,
            Venue::Lbank(c) => c.query(symbol, id).await,
        }
    }
    async fn cancel(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        match self {
            Venue::Binance(c) => c.cancel(symbol, id).await,
            Venue::Bybit(c) => c.cancel(symbol, id).await,
            Venue::Aster(c) => c.cancel(symbol, id).await,
            Venue::Okx(c) => c.cancel(symbol, id).await,
            Venue::Gate(c) => c.cancel(symbol, id).await,
            Venue::Kucoin(c) => c.cancel(symbol, id).await,
            Venue::Bingx(c) => c.cancel(symbol, id).await,
            Venue::Mexc(c) => c.cancel(symbol, id).await,
            Venue::Hyperliquid(c) => c.cancel(symbol, id).await,
            Venue::Lbank(c) => c.cancel(symbol, id).await,
        }
    }
    fn perp(&self) -> bool {
        match self {
            Venue::Binance(c) => c.perp,
            Venue::Bybit(c) => c.category == "linear",
            Venue::Aster(c) => c.perp,
            Venue::Okx(c) => c.perp,
            Venue::Gate(c) => c.perp,
            Venue::Kucoin(c) => c.perp,
            Venue::Bingx(c) => c.perp,
            Venue::Mexc(c) => c.perp,
            Venue::Hyperliquid(c) => c.perp,
            Venue::Lbank(_) => false,
        }
    }
    async fn positions(&self) -> Result<Vec<(String, f64, f64)>, String> {
        match self {
            Venue::Binance(c) => c.positions().await,
            Venue::Bybit(c) => c.positions().await,
            Venue::Aster(c) => c.positions().await,
            Venue::Okx(c) => c.positions().await,
            Venue::Gate(c) => c.positions().await,
            Venue::Kucoin(c) => c.positions().await,
            Venue::Bingx(c) => c.positions().await,
            Venue::Mexc(c) => c.positions().await,
            Venue::Hyperliquid(c) => c.positions().await,
            Venue::Lbank(_) => Ok(Vec::new()),
        }
    }
    async fn set_leverage(&self, symbol: &str, lev: f64) -> Result<(), String> {
        match self {
            Venue::Binance(c) => c.set_leverage(symbol, lev).await,
            Venue::Bybit(c) => c.set_leverage(symbol, lev).await,
            Venue::Aster(c) => c.set_leverage(symbol, lev).await,
            Venue::Okx(c) => c.set_leverage(symbol, lev).await,
            Venue::Gate(c) => c.set_leverage(symbol, lev).await,
            Venue::Kucoin(c) => c.set_leverage(symbol, lev).await,
            Venue::Bingx(c) => c.set_leverage(symbol, lev).await,
            Venue::Mexc(c) => c.set_leverage(symbol, lev).await,
            Venue::Hyperliquid(c) => c.set_leverage(symbol, lev).await,
            Venue::Lbank(_) => Ok(()),
        }
    }
    async fn networks(&self, coin: &str) -> Result<Vec<crate::transfer::Net>, String> {
        match self {
            Venue::Binance(c) => c.networks(coin).await,
            Venue::Bybit(c) => c.networks(coin).await,
            _ => Err("переводы с этой биржи в Rust-бэкенде не поддерживаются".into()),
        }
    }
    async fn deposit_address(&self, coin: &str, net: &str) -> Result<(String, String), String> {
        match self {
            Venue::Binance(c) => c.deposit_address(coin, net).await,
            Venue::Bybit(c) => c.deposit_address(coin, net).await,
            _ => Err("переводы с этой биржи в Rust-бэкенде не поддерживаются".into()),
        }
    }
    async fn withdraw(
        &self,
        coin: &str,
        net: &str,
        address: &str,
        tag: &str,
        amount: f64,
    ) -> Result<String, String> {
        match self {
            Venue::Binance(c) => c.withdraw(coin, net, address, tag, amount).await,
            Venue::Bybit(c) => c.withdraw(coin, net, address, tag, amount).await,
            _ => Err("переводы с этой биржи в Rust-бэкенде не поддерживаются".into()),
        }
    }
    async fn balances(&self) -> Result<std::collections::BTreeMap<String, f64>, String> {
        match self {
            Venue::Binance(c) => c.balances().await,
            Venue::Bybit(c) => c.balances().await,
            Venue::Aster(c) => c.balances().await,
            Venue::Okx(c) => c.balances().await,
            Venue::Gate(c) => c.balances().await,
            Venue::Kucoin(c) => c.balances().await,
            Venue::Bingx(c) => c.balances().await,
            Venue::Mexc(c) => c.balances().await,
            Venue::Hyperliquid(c) => c.balances().await,
            Venue::Lbank(c) => c.balances().await,
        }
    }
}

/// Фоновая задача живого счёта: сверка часов, балансы, отправка и снятие ордеров, опрос статусов.
pub async fn worker(
    acc: Shared,
    ex: String,
    client: Arc<Venue>,
    mut rx: mpsc::UnboundedReceiver<Cmd>,
    symbols: Vec<String>,
    leverage: Option<f64>,
) {
    if let Err(e) = client.sync_time().await {
        tracing::warn!("[{ex}] время сервера не получено: {e}");
    }
    // перпы: плечо на бирже — как в параметрах
    if let Some(lev) = leverage {
        for s in &symbols {
            if let Err(e) = client.set_leverage(s, lev).await {
                tracing::warn!("[{ex}] плечо {lev} для {s} не выставлено: {e}");
                with_live(&acc, &ex, |l| l.error = format!("плечо {s}: {e}"));
            }
        }
    }
    let testnet = with_live(&acc, &ex, |l| l.mode == "TESTNET").unwrap_or(false);
    let _stream = AbortOnDrop(tokio::spawn(user_stream(
        acc.clone(),
        ex.clone(),
        client.clone(),
        testnet,
    )));
    let mut poll = tokio::time::interval(Duration::from_millis(500));
    let mut n: u64 = 0;
    loop {
        tokio::select! {
            cmd = rx.recv() => {
                let Some(cmd) = cmd else { return };
                match cmd {
                    Cmd::Place { local, symbol, side, qty, price, ref_px } => {
                        let r = client.place(local, &symbol, side, qty, price, ref_px).await;
                        let cancel_now = with_live(&acc, &ex, |l| match &r {
                            Ok(st) => { l.on_state(local, st); l.wants_cancel(local) }
                            Err(e) => { tracing::warn!("[{ex}] ордер {symbol} отклонён: {e}"); l.on_error(local, e); false }
                        }).unwrap_or(false);
                        if cancel_now {
                            cancel(&acc, &ex, &client, local).await;
                        }
                    }
                    Cmd::Cancel { local } => cancel(&acc, &ex, &client, local).await,
                    Cmd::Networks { coin } => {
                        let r = client.networks(&coin).await;
                        with_live(&acc, &ex, |l| { l.networks.insert(coin, r); });
                    }
                    Cmd::Address { coin, network } => {
                        let r = client.deposit_address(&coin, &network).await;
                        with_live(&acc, &ex, |l| { l.addresses.insert((coin, network), r); });
                    }
                    Cmd::Withdraw { tid, coin, network, address, tag, amount } => {
                        let r = client.withdraw(&coin, &network, &address, &tag, amount).await;
                        match &r {
                            Ok(id) => tracing::warn!("[{ex}] вывод {amount} {coin} по {network} на {address}: номер {id}"),
                            Err(e) => tracing::warn!("[{ex}] вывод {amount} {coin} по {network} отклонён: {e}"),
                        }
                        with_live(&acc, &ex, |l| { l.withdrawals.insert(tid, r); });
                    }
                }
            }
            _ = poll.tick() => {
                n += 1;
                // поток событий работает — опрос раз в 5 с для подстраховки, иначе каждые 0.5 с
                let stream_ok = with_live(&acc, &ex, |l| l.stream_ok).unwrap_or(false);
                let pending = if stream_ok && !n.is_multiple_of(10) {
                    Vec::new()
                } else {
                    with_live(&acc, &ex, |l| { l.trim(); l.pending() }).unwrap_or_default()
                };
                for (local, symbol, id) in pending {
                    match client.query(&symbol, &id).await {
                        Ok(st) => { with_live(&acc, &ex, |l| l.on_state(local, &st)); }
                        Err(e) => { with_live(&acc, &ex, |l| l.error = e); }
                    }
                }
                if n % 10 == 1 && client.perp() {
                    match client.positions().await {
                        Ok(ps) => { with_live(&acc, &ex, |l| l.sync_positions(&ps)); }
                        Err(e) => { tracing::warn!("[{ex}] позиции не получены: {e}"); with_live(&acc, &ex, |l| l.error = e); }
                    }
                }
                if n % 10 == 1 {
                    match client.balances().await {
                        Ok(b) => { with_live(&acc, &ex, |l| { l.ledger.set_balances(b); l.error.clear(); }); }
                        Err(e) => { tracing::warn!("[{ex}] балансы не получены: {e}"); with_live(&acc, &ex, |l| l.error = e); }
                    }
                }
            }
        }
    }
}

/// Снять задачу, когда фоновая задача счёта закончилась.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Что сделать с сообщением приватного потока.
pub enum StreamMsg {
    Ignore,
    /// Вход и подписка подтверждены — события пойдут.
    Ready,
    /// Отправить ответ (подписка после входа).
    Send(String),
    /// Отправить несколько сообщений (подписки после приветствия).
    SendMany(Vec<String>),
    Fail(String),
    /// Применить событие к живому счёту.
    Apply(Box<dyn FnOnce(&mut Live) + Send>),
}

impl Venue {
    /// Адрес приватного потока (для потоков по listenKey — с новым ключом).
    async fn stream_url(&self, testnet: bool) -> Result<String, String> {
        match self {
            Venue::Binance(c) if c.perp => {
                let k = c
                    .listen_key(reqwest::Method::POST)
                    .await
                    .map_err(|e| format!("listenKey: {e}"))?;
                Ok(format!("{}/{k}", crate::signed::fstream_url(testnet)))
            }
            Venue::Binance(_) => Ok(crate::signed::ws_api_url(testnet)),
            Venue::Bybit(_) => Ok(crate::bybit_api::private_url(testnet)),
            Venue::Okx(_) => Ok(crate::okx_api::private_url(testnet)),
            Venue::Gate(c) => c.stream_url().await,
            Venue::Kucoin(c) => c.stream_url().await,
            Venue::Bingx(c) => c.stream_url().await,
            Venue::Mexc(c) => c.stream_url().await,
            Venue::Lbank(c) => c.stream_url().await,
            Venue::Hyperliquid(_) => Ok(crate::hyperliquid_api::stream_url(testnet)),
            Venue::Aster(c) => {
                let k = c
                    .listen_key(reqwest::Method::POST)
                    .await
                    .map_err(|e| format!("listenKey: {e}"))?;
                Ok(format!(
                    "{}/{k}",
                    crate::aster_api::stream_url(c.perp, testnet)
                ))
            }
        }
    }

    /// Сообщения сразу после подключения (вход, подписка); None — поток готов сразу (listenKey), пустой список —
    /// ждать приветствия сервера (KuCoin).
    fn stream_hello(&self, sub: &str) -> Option<Vec<String>> {
        match self {
            Venue::Binance(c) if !c.perp => Some(vec![
                c.ws_request(sub, "userDataStream.subscribe.signature"),
            ]),
            Venue::Bybit(c) => Some(vec![c.auth_message()]),
            Venue::Okx(c) => Some(vec![c.login()]),
            Venue::Gate(c) => Some(c.subscriptions()),
            Venue::Kucoin(_) => Some(Vec::new()),
            Venue::Bingx(c) if !c.perp => Some(vec![c.subscription()]),
            Venue::Mexc(c) => Some(c.hello()),
            Venue::Hyperliquid(c) => Some(c.hello()),
            Venue::Lbank(c) => Some(c.hello()),
            _ => None,
        }
    }

    /// Пинг приложения раз в 20 с.
    fn stream_ping(&self) -> Option<String> {
        match self {
            Venue::Bybit(_) => Some(r#"{"op":"ping"}"#.into()),
            Venue::Okx(_) => Some("ping".into()),
            Venue::Gate(c) => Some(c.ping()),
            Venue::Kucoin(c) => Some(c.ping()),
            Venue::Mexc(c) => Some(c.ping()),
            Venue::Lbank(c) => Some(c.ping()),
            Venue::Hyperliquid(_) => Some(r#"{"method":"ping"}"#.into()),
            _ => None,
        }
    }

    /// Продлить listenKey (раз в 30 мин).
    async fn keepalive(&self) -> Result<(), String> {
        match self {
            Venue::Binance(c) if c.perp => c.listen_key(reqwest::Method::PUT).await.map(|_| ()),
            Venue::Aster(c) => c.listen_key(reqwest::Method::PUT).await.map(|_| ()),
            Venue::Bingx(c) => c.keepalive().await,
            Venue::Mexc(c) => c.keepalive().await,
            Venue::Lbank(c) => c.keepalive().await,
            _ => Ok(()),
        }
    }

    /// Разбор сообщения приватного потока.
    fn stream_parse(&self, text: &str, sub: &str) -> StreamMsg {
        if let Venue::Bingx(c) = self {
            return c.stream_parse(text);
        }
        if let Venue::Mexc(c) = self {
            return c.stream_parse(text);
        }
        if let Venue::Hyperliquid(c) = self {
            return c.stream_parse(text);
        }
        if let Venue::Lbank(c) = self {
            return c.stream_parse(text);
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
            return StreamMsg::Ignore;
        };
        match self {
            Venue::Bybit(c) => match v["op"].as_str() {
                // перпам — ещё позиции
                Some("auth") if v["success"] == true => {
                    StreamMsg::Send(if c.category == "linear" {
                        r#"{"op":"subscribe","args":["order","wallet","position"]}"#.into()
                    } else {
                        r#"{"op":"subscribe","args":["order","wallet"]}"#.into()
                    })
                }
                Some("auth") => {
                    StreamMsg::Fail(format!("вход в приватный поток отклонён: {}", v["ret_msg"]))
                }
                Some("subscribe") if v["success"] == true => StreamMsg::Ready,
                Some("subscribe") => StreamMsg::Fail(format!(
                    "подписка на события счёта отклонена: {}",
                    v["ret_msg"]
                )),
                _ => match v["topic"].as_str() {
                    Some(t) => {
                        let t = t.to_owned();
                        StreamMsg::Apply(Box::new(move |l: &mut Live| l.on_bybit(&t, &v["data"])))
                    }
                    None => StreamMsg::Ignore,
                },
            },
            Venue::Gate(c) => c.stream_parse(&v),
            Venue::Kucoin(c) => c.stream_parse(&v),
            Venue::Okx(c) => match v["event"].as_str() {
                Some("login") if v["code"] == "0" => StreamMsg::Send(c.subscribe()),
                Some("login") | Some("error") => {
                    StreamMsg::Fail(format!("OKX {}: {}", v["code"], v["msg"]))
                }
                Some("subscribe") => StreamMsg::Ready,
                Some(_) => StreamMsg::Ignore,
                None => {
                    let data: Vec<serde_json::Value> =
                        v["data"].as_array().cloned().unwrap_or_default();
                    match v["arg"]["channel"].as_str() {
                        Some("orders") => {
                            let states: Vec<(u64, OrderState)> = data
                                .iter()
                                .filter_map(|o| {
                                    let local =
                                        o["clOrdId"].as_str()?.strip_prefix("hft")?.parse().ok()?;
                                    Some((local, c.order_state(o)))
                                })
                                .collect();
                            StreamMsg::Apply(Box::new(move |l: &mut Live| {
                                l.stream_events += 1;
                                for (local, st) in &states {
                                    l.on_state(*local, st);
                                }
                            }))
                        }
                        Some("account") => {
                            let b = data
                                .first()
                                .map(|d| crate::okx_api::parse_balances(d, c.perp))
                                .unwrap_or_default();
                            StreamMsg::Apply(Box::new(move |l: &mut Live| {
                                l.stream_events += 1;
                                for (a, f) in &b {
                                    l.ledger.set_balance(a, *f);
                                }
                            }))
                        }
                        Some("positions") => {
                            let ps: Vec<(String, f64, f64)> =
                                data.iter().filter_map(|p| c.position(p)).collect();
                            StreamMsg::Apply(Box::new(move |l: &mut Live| {
                                l.stream_events += 1;
                                for (s, q, e) in &ps {
                                    l.ledger.set_position(s, *q, *e);
                                }
                            }))
                        }
                        _ => StreamMsg::Ignore,
                    }
                }
            },
            Venue::Binance(c) if !c.perp => {
                if v.get("event").is_some() {
                    StreamMsg::Apply(Box::new(move |l: &mut Live| l.on_event(&v["event"])))
                } else if v["id"] == sub {
                    if v["status"] == 200 {
                        StreamMsg::Ready
                    } else {
                        StreamMsg::Fail(format!(
                            "подписка на события счёта отклонена: {}",
                            v["error"]
                        ))
                    }
                } else {
                    StreamMsg::Ignore
                }
            }
            // потоки по listenKey: события сразу в формате Binance
            _ if v["e"] == "listenKeyExpired" => StreamMsg::Fail("listenKey истёк".into()),
            _ => StreamMsg::Apply(Box::new(move |l: &mut Live| l.on_event(&v))),
        }
    }
}

/// Приватный поток событий счёта: подключение, вход и подписка (по бирже), пинги, события в учёт; при обрыве —
/// переподключение с паузой 0.5 → 30 с (пока нет потока — частый опрос).
async fn user_stream(acc: Shared, ex: String, venue: Arc<Venue>, testnet: bool) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let mut backoff = crate::feed::Backoff::default();
    let mut seq = 0u64;
    loop {
        let url = match venue.stream_url(testnet).await {
            Ok(u) => u,
            Err(e) => {
                with_live(&acc, &ex, |l| {
                    l.stream_error = e.clone();
                    l.error = e;
                });
                tokio::time::sleep(backoff.next()).await;
                continue;
            }
        };
        let mut pings = 0u32;
        // зависшее рукопожатие не держит поток вечно
        let conn = match tokio::time::timeout(
            Duration::from_secs(10),
            tokio_tungstenite::connect_async(url.as_str()),
        )
        .await
        {
            Ok(c) => c,
            Err(_) => Err(tokio_tungstenite::tungstenite::Error::Io(
                std::io::Error::other("подключение не ответило за 10 с"),
            )),
        };
        let err = match conn {
            Ok((mut ws, _)) => {
                // первый пинг — через 20 с после подключения, не раньше входа и подписки
                let mut ping = tokio::time::interval_at(
                    tokio::time::Instant::now() + Duration::from_secs(20),
                    Duration::from_secs(20),
                );
                seq += 1;
                let sub = format!("sub{seq}");
                let hello = venue.stream_hello(&sub);
                let mut err = String::new();
                let ready = |acc: &Shared| {
                    tracing::info!("[{ex}] поток событий счёта подключён");
                    with_live(acc, &ex, |l| l.stream_ok = true);
                };
                if hello.is_none() {
                    backoff.reset();
                    ready(&acc);
                }
                for m in hello.unwrap_or_default() {
                    if let Err(e) = ws.send(Message::text(m)).await {
                        err = e.to_string();
                    }
                }
                while err.is_empty() {
                    let msg = tokio::select! {
                        m = ws.next() => m,
                        _ = ping.tick() => {
                            pings += 1;
                            if let Some(p) = venue.stream_ping() {
                                err = ws.send(Message::text(p)).await.err().map(|e| e.to_string()).unwrap_or_default();
                            }
                            if pings.is_multiple_of(90)
                                && let Err(e) = venue.keepalive().await
                            {
                                tracing::warn!("[{ex}] listenKey не продлён: {e}");
                            }
                            continue;
                        }
                    };
                    // бинарные кадры (BingX — gzip) — в текст
                    let msg = match msg {
                        // MEXC спот — protobuf
                        Some(Ok(Message::Binary(b))) if matches!(venue.as_ref(), Venue::Mexc(c) if !c.perp) =>
                        {
                            if let Venue::Mexc(c) = venue.as_ref() {
                                match c.stream_binary(&b) {
                                    StreamMsg::Apply(f) => {
                                        with_live(&acc, &ex, f);
                                    }
                                    StreamMsg::Fail(e) => err = e,
                                    _ => {}
                                }
                            }
                            continue;
                        }
                        Some(Ok(Message::Binary(b))) => match crate::feed::bingx::gunzip(&b) {
                            Some(t) => Some(Ok(Message::text(t))),
                            None => continue,
                        },
                        m => m,
                    };
                    match msg {
                        Some(Ok(Message::Text(t))) => match venue.stream_parse(&t, &sub) {
                            StreamMsg::Ignore => {}
                            StreamMsg::Ready => {
                                backoff.reset();
                                ready(&acc);
                            }
                            StreamMsg::SendMany(ms) => {
                                for m in ms {
                                    if let Err(e) = ws.send(Message::text(m)).await {
                                        err = e.to_string();
                                    }
                                }
                            }
                            StreamMsg::Send(m) => {
                                err = ws
                                    .send(Message::text(m))
                                    .await
                                    .err()
                                    .map(|e| e.to_string())
                                    .unwrap_or_default()
                            }
                            StreamMsg::Fail(e) => err = e,
                            StreamMsg::Apply(f) => {
                                with_live(&acc, &ex, f);
                            }
                        },
                        Some(Ok(Message::Ping(p))) => {
                            let _ = ws.send(Message::Pong(p)).await;
                        }
                        Some(Ok(Message::Close(c))) => err = format!("закрыто биржей: {c:?}"),
                        Some(Ok(_)) => {}
                        Some(Err(e)) => err = e.to_string(),
                        None => err = "соединение закрыто".into(),
                    }
                }
                err
            }
            Err(e) => e.to_string(),
        };
        with_live(&acc, &ex, |l| {
            l.stream_ok = false;
            l.error = format!("поток событий: {err}");
            l.stream_error = err.clone();
        });
        let pause = backoff.next();
        tracing::warn!(
            "[{ex}] поток событий счёта: {err} — переподключение через {} мс",
            pause.as_millis()
        );
        tokio::time::sleep(pause).await;
    }
}

/// Снять ордер на бирже; номера ещё нет — снимется, когда ордер встанет (см. Place).
async fn cancel(acc: &Shared, ex: &str, client: &Venue, local: u64) {
    let Some((symbol, id)) = with_live(acc, ex, |l| l.cancel_target(local)).flatten() else {
        return;
    };
    match client.cancel(&symbol, &id).await {
        Ok(st) => {
            with_live(acc, ex, |l| l.on_state(local, &st));
        }
        Err(e) => tracing::warn!("[{ex}] не снят ордер {symbol} {id}: {e}"),
    }
}
