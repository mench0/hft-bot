//! Живой счёт Binance или Bybit (спот, основная сеть или testnet). Ордер проверяется теми же правилами, что бумажный,
//! и уходит на биржу в фоновой задаче — движок не ждёт сеть. Статусы своих ордеров опрашиваются раз в 0.5 с,
//! балансы — раз в 5 с. Исполнения учитываются тем же учётом, что на бумаге (средняя цена, результат, журнал).

use crate::exchange::binance::OrderState;
use crate::market::book::Book;
use crate::trading::paper::{Amount, Fill, Limits, Paper, Side};
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
    /// Поставить защитный стоп (перпы); old — прежний стоп, снимается после установки нового.
    PlaceStop {
        symbol: String,
        side: Side,
        qty: f64,
        price: f64,
        old: Option<String>,
    },
    /// Снять защитный стоп.
    CancelStop {
        symbol: String,
        id: String,
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
    pub networks: HashMap<String, Result<Vec<crate::exchange::transfer::Net>, String>>,
    /// Адреса депозита по (монета, сеть).
    pub addresses: HashMap<(String, String), Result<(String, String), String>>,
    /// Итог выводов по своему номеру: номер на бирже или ошибка.
    pub withdrawals: HashMap<u64, Result<String, String>>,
    /// Защитные стопы на бирже по символам.
    stops: HashMap<String, Stop>,
    /// Последняя неудача стопа по символу (повтор не раньше чем через STOP_RETRY).
    stop_failed: HashMap<String, std::time::Instant>,
    /// Биржа (рынок) не поддерживает стопы в боте — защищает только стоп-лосс бота.
    pub stops_unsupported: bool,
    /// Сокет ордеров (биржи с торговлей по WebSocket); без сокета вход запрещён, выход — по REST.
    pub trade: Option<Arc<crate::trading::trade_ws::TradeLink>>,
    /// Входов отклонено, потому что сокет ордеров не подключён.
    pub ws_entries_blocked: u64,
}

#[derive(Debug, Clone)]
struct Stop {
    /// Номер на бирже; None — запрос ещё идёт.
    id: Option<String>,
    side: Side,
    qty: f64,
    price: f64,
}

const STOP_RETRY: Duration = Duration::from_secs(30);

fn near(a: f64, b: f64) -> bool {
    (a - b).abs() <= (b.abs() * 1e-9).max(1e-12)
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
            stops: HashMap::new(),
            stop_failed: HashMap::new(),
            stops_unsupported: false,
            trade: None,
            ws_entries_blocked: 0,
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
        // торговля по сокету: сокет упал — новые входы нельзя, выход (продажа спота, уменьшение позиции) — по REST
        if self.trade.as_ref().is_some_and(|t| !t.ready())
            && pr.qty > self.ledger.exit_room(symbol, side) + 1e-12
        {
            self.ws_entries_blocked += 1;
            let e = "сокет ордеров не подключён: вход запрещён, выход — по REST".to_owned();
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

    /// Привести стоп символа на бирже к желаемому (None — снять). Новый ставится до снятия старого.
    pub fn want_stop(&mut self, symbol: &str, want: Option<(Side, f64, f64)>) {
        if self.stops_unsupported {
            return;
        }
        let cur = self.stops.get(symbol).cloned();
        match (want, cur) {
            (None, None) => {}
            (None, Some(c)) => {
                if let Some(id) = c.id {
                    self.stops.remove(symbol);
                    self.ledger.stops.remove(symbol);
                    let _ = self.tx.send(Cmd::CancelStop {
                        symbol: symbol.to_owned(),
                        id,
                    });
                }
            }
            (Some(_), Some(c)) if c.id.is_none() => {} // запрос идёт
            (Some((side, qty, price)), cur) => {
                if cur
                    .as_ref()
                    .is_some_and(|c| c.side == side && near(c.qty, qty) && near(c.price, price))
                {
                    return;
                }
                if self
                    .stop_failed
                    .get(symbol)
                    .is_some_and(|t| t.elapsed() < STOP_RETRY)
                {
                    return;
                }
                let old = cur.and_then(|c| c.id);
                self.stops.insert(
                    symbol.to_owned(),
                    Stop {
                        id: None,
                        side,
                        qty,
                        price,
                    },
                );
                let _ = self.tx.send(Cmd::PlaceStop {
                    symbol: symbol.to_owned(),
                    side,
                    qty,
                    price,
                    old,
                });
            }
        }
    }

    pub fn stop_keys(&self) -> Vec<String> {
        self.stops.keys().cloned().collect()
    }

    /// Итог установки стопа (из фоновой задачи).
    fn stop_placed(
        &mut self,
        symbol: &str,
        r: &Result<Option<String>, String>,
        old: Option<String>,
    ) {
        match r {
            Ok(Some(id)) => {
                if let Some(s) = self.stops.get_mut(symbol) {
                    s.id = Some(id.clone());
                    self.ledger
                        .stops
                        .insert(symbol.to_owned(), (s.side, s.price));
                }
                self.stop_failed.remove(symbol);
            }
            Ok(None) => {
                self.stops_unsupported = true;
                self.stops.clear();
                tracing::warn!(
                    "биржа не поддерживает стоп-ордера в боте — защищает только стоп-лосс бота"
                );
            }
            Err(e) => {
                self.stop_failed
                    .insert(symbol.to_owned(), std::time::Instant::now());
                self.error = format!("стоп {symbol}: {e}");
                // прежний стоп остаётся на бирже
                match old {
                    Some(id) => {
                        if let Some(s) = self.stops.get_mut(symbol) {
                            s.id = Some(id);
                            s.price = f64::NAN; // не совпадёт с желаемым — повтор после паузы
                        }
                    }
                    None => {
                        self.stops.remove(symbol);
                    }
                }
            }
        }
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
                    if let (Some(local), Ok(st)) = (local, crate::exchange::bybit::order_state(d)) {
                        self.on_state(local, &st);
                    }
                }
                "position" => {
                    if let Some((s, q, e)) = crate::exchange::bybit::position(d) {
                        self.ledger.set_position(&s, q, e);
                    }
                }
                "wallet" => {
                    for c in d["coin"].as_array().into_iter().flatten() {
                        if let Some(a) = c["coin"].as_str() {
                            self.ledger
                                .set_balance(a, crate::exchange::bybit::free_of(c));
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
pub type Shared = Arc<Mutex<HashMap<String, crate::trading::account::Account>>>;

fn with_live<R>(acc: &Shared, ex: &str, f: impl FnOnce(&mut Live) -> R) -> Option<R> {
    match acc.lock().unwrap().get_mut(ex) {
        Some(crate::trading::account::Account::Live(l)) => Some(f(l)),
        _ => None,
    }
}

/// Торговый API биржи для живого счёта.
pub enum Venue {
    Binance(crate::exchange::binance::Client),
    Bybit(crate::exchange::bybit::Client),
    Aster(crate::exchange::aster::Client),
    Okx(crate::exchange::okx::Client),
    Gate(crate::exchange::gate::Client),
    Kucoin(crate::exchange::kucoin::Client),
    Bingx(crate::exchange::bingx::Client),
    Mexc(crate::exchange::mexc::Client),
    Hyperliquid(crate::exchange::hyperliquid::Client),
    Lbank(crate::exchange::lbank::Client),
    Upbit(crate::exchange::upbit::Client),
    Uniswap(crate::exchange::uniswap::Client),
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
            Venue::Upbit(c) => c.sync_time().await,
            Venue::Uniswap(c) => c.sync_time().await,
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
            Venue::Upbit(c) => c.place(local, symbol, side, qty, price, ref_px).await,
            Venue::Uniswap(c) => c.place(local, symbol, side, qty, price).await,
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
            Venue::Upbit(c) => c.query(symbol, id).await,
            Venue::Uniswap(c) => c.query(symbol, id).await,
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
            Venue::Upbit(c) => c.cancel(symbol, id).await,
            Venue::Uniswap(c) => c.cancel(symbol, id).await,
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
            Venue::Lbank(_) | Venue::Upbit(_) | Venue::Uniswap(_) => false,
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
            Venue::Lbank(_) | Venue::Upbit(_) | Venue::Uniswap(_) => Ok(Vec::new()),
        }
    }
    /// Защитный стоп (перпы): None — не поддерживается.
    async fn place_stop(
        &self,
        symbol: &str,
        side: Side,
        qty: f64,
        price: f64,
    ) -> Result<Option<String>, String> {
        match self {
            Venue::Binance(c) => c.place_stop(symbol, side, price).await,
            Venue::Bybit(c) => c.place_stop(symbol, side, price).await,
            Venue::Aster(c) => c.place_stop(symbol, side, price).await,
            Venue::Okx(c) => c.place_stop(symbol, side, price).await,
            Venue::Gate(c) => c.place_stop(symbol, side, price).await,
            Venue::Kucoin(c) => c.place_stop(symbol, side, price).await,
            Venue::Bingx(c) => c.place_stop(symbol, side, price).await,
            Venue::Hyperliquid(c) => c.place_stop(symbol, side, qty, price).await,
            _ => Ok(None),
        }
    }

    async fn cancel_stop(&self, symbol: &str, id: &str) -> Result<(), String> {
        match self {
            Venue::Binance(c) => c.cancel_stop(symbol, id).await,
            Venue::Bybit(c) => c.cancel_stop(symbol, id).await,
            Venue::Aster(c) => c.cancel_stop(symbol, id).await,
            Venue::Okx(c) => c.cancel_stop(symbol, id).await,
            Venue::Gate(c) => c.cancel_stop(symbol, id).await,
            Venue::Kucoin(c) => c.cancel_stop(symbol, id).await,
            Venue::Bingx(c) => c.cancel_stop(symbol, id).await,
            Venue::Hyperliquid(c) => c.cancel_stop(symbol, id).await,
            _ => Ok(()),
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
            Venue::Lbank(_) | Venue::Upbit(_) | Venue::Uniswap(_) => Ok(()),
        }
    }
    async fn networks(&self, coin: &str) -> Result<Vec<crate::exchange::transfer::Net>, String> {
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
            Venue::Upbit(c) => c.balances().await,
            Venue::Uniswap(c) => c.balances().await,
        }
    }
}

/// Где у биржи ордера по сокету.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum TradeSocket {
    /// Торговли по сокету нет — только REST.
    None,
    /// Тот же сокет, что события счёта (Binance спот ws-api, OKX, Hyperliquid).
    Shared,
    /// Отдельный торговый сокет (Binance перпы ws-fapi, Bybit /v5/trade).
    Own,
}

/// Итог ордера или отмены по сокету.
enum WsOut<T> {
    NotReady,
    Unknown(String),
    Done(Result<T, String>),
}

impl Venue {
    pub fn trade_socket(&self) -> TradeSocket {
        match self {
            Venue::Binance(c) if c.perp => TradeSocket::Own,
            Venue::Binance(_) | Venue::Okx(_) | Venue::Hyperliquid(_) | Venue::Gate(_) => {
                TradeSocket::Shared
            }
            Venue::Bybit(_) | Venue::Kucoin(_) => TradeSocket::Own,
            _ => TradeSocket::None,
        }
    }

    /// Ответ на запрос по сокету ордеров: (id, данные или ошибка).
    fn trade_reply(&self, text: &str) -> Option<(String, Result<serde_json::Value, String>)> {
        let v: serde_json::Value = serde_json::from_str(text).ok()?;
        match self {
            Venue::Binance(_) => crate::exchange::binance::ws_reply(&v),
            Venue::Okx(_) => crate::exchange::okx::ws_reply(&v),
            Venue::Bybit(_) => crate::exchange::bybit::ws_reply(&v),
            Venue::Hyperliquid(_) => crate::exchange::hyperliquid::ws_reply(&v),
            Venue::Gate(_) => crate::exchange::gate::ws_reply(&v),
            Venue::Kucoin(_) => crate::exchange::kucoin::ws_reply(&v),
            _ => None,
        }
    }

    /// Ордер по сокету.
    #[allow(clippy::too_many_arguments)]
    async fn place_ws(
        &self,
        link: &crate::trading::trade_ws::TradeLink,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
        ref_px: f64,
    ) -> WsOut<OrderState> {
        use crate::trading::trade_ws::Call;
        if !link.ready() {
            link.fallbacks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return WsOut::NotReady;
        }
        let mut hl = None;
        let (id, msg) = match self {
            Venue::Binance(c) => {
                let id = link.next_id("t");
                let m = c.ws_place(&id, local, symbol, side, qty, price);
                (id, m)
            }
            Venue::Okx(c) => {
                let id = link.next_id("t");
                match c.ws_place(&id, local, symbol, side, qty, price) {
                    Ok(m) => (id, m),
                    Err(e) => return WsOut::Done(Err(e)),
                }
            }
            Venue::Bybit(c) => {
                let id = link.next_id("t");
                let m = c.ws_place(&id, local, symbol, side, qty, price);
                (id, m)
            }
            Venue::Gate(c) => {
                let id = link.next_id("g");
                match c.ws_place(&id, local, symbol, side, qty, price, ref_px) {
                    Ok(m) => (id, m),
                    Err(e) => return WsOut::Done(Err(e)),
                }
            }
            Venue::Kucoin(c) => {
                let id = link.next_id("k");
                match c.ws_place(&id, local, symbol, side, qty, price) {
                    Ok(m) => (id, m),
                    Err(e) => return WsOut::Done(Err(e)),
                }
            }
            Venue::Hyperliquid(c) => {
                let r = match c.place_req(local, symbol, side, qty, price, ref_px) {
                    Ok(r) => r,
                    Err(e) => return WsOut::Done(Err(e)),
                };
                let id = link.next_id("");
                let m = c.ws_place(id.parse().unwrap_or(0), &r);
                hl = Some(r);
                (id, m)
            }
            _ => return WsOut::NotReady,
        };
        match link.call(&id, msg, WS_TIMEOUT).await {
            Call::NotReady => WsOut::NotReady,
            Call::Unknown(e) => WsOut::Unknown(e),
            Call::Reply(Err(e)) => WsOut::Done(Err(e)),
            Call::Reply(Ok(v)) => WsOut::Done(match self {
                Venue::Binance(_) => OrderState::from(&v),
                Venue::Okx(_) => Ok(OrderState {
                    id: v["ordId"].as_str().unwrap_or("").to_owned(),
                    status: "NEW".into(),
                    executed: 0.0,
                    quote: 0.0,
                }),
                Venue::Gate(c) => Ok(c.order_state(&v)),
                Venue::Bybit(_) | Venue::Kucoin(_) => Ok(OrderState {
                    id: v["orderId"].as_str().unwrap_or("").to_owned(),
                    status: "NEW".into(),
                    executed: 0.0,
                    quote: 0.0,
                }),
                _ => hl
                    .as_ref()
                    .map_or(Err("нет запроса".into()), |r| r.result(&v)),
            }),
        }
    }

    /// Отмена по сокету: Some — состояние сразу (Binance), None — итог придёт потоком событий.
    async fn cancel_ws(
        &self,
        link: &crate::trading::trade_ws::TradeLink,
        symbol: &str,
        oid: &str,
    ) -> WsOut<Option<OrderState>> {
        use crate::trading::trade_ws::Call;
        if !link.ready() {
            return WsOut::NotReady;
        }
        let (id, msg) = match self {
            Venue::Binance(c) => {
                let id = link.next_id("c");
                let m = c.ws_cancel(&id, symbol, oid);
                (id, m)
            }
            Venue::Okx(c) => {
                let id = link.next_id("c");
                let m = c.ws_cancel(&id, symbol, oid);
                (id, m)
            }
            Venue::Bybit(c) => {
                let id = link.next_id("c");
                let m = c.ws_cancel(&id, symbol, oid);
                (id, m)
            }
            Venue::Gate(c) => {
                let id = link.next_id("gc");
                let m = c.ws_cancel(&id, symbol, oid);
                (id, m)
            }
            Venue::Kucoin(c) => {
                let id = link.next_id("kc");
                let m = c.ws_cancel(&id, symbol, oid);
                (id, m)
            }
            Venue::Hyperliquid(c) => {
                let id = link.next_id("");
                match c.ws_cancel(id.parse().unwrap_or(0), symbol, oid) {
                    Ok(m) => (id, m),
                    Err(e) => return WsOut::Done(Err(e)),
                }
            }
            _ => return WsOut::NotReady,
        };
        match link.call(&id, msg, WS_TIMEOUT).await {
            Call::NotReady => WsOut::NotReady,
            Call::Unknown(e) => WsOut::Unknown(e),
            Call::Reply(Err(e)) => WsOut::Done(Err(e)),
            Call::Reply(Ok(v)) => match self {
                Venue::Binance(_) => WsOut::Done(OrderState::from(&v).map(Some)),
                Venue::Gate(c) => WsOut::Done(Ok(Some(c.order_state(&v)))),
                // ответ Hyperliquid «ok» бывает и с ошибкой внутри
                Venue::Hyperliquid(_) => match v["response"]["data"]["statuses"][0].get("error") {
                    Some(e) => WsOut::Done(Err(format!("Hyperliquid: {e}"))),
                    None => WsOut::Done(Ok(None)),
                },
                _ => WsOut::Done(Ok(None)),
            },
        }
    }

    /// Ордер по своему номеру (исход запроса по сокету неизвестен).
    async fn query_client(&self, symbol: &str, local: u64) -> Result<OrderState, String> {
        match self {
            Venue::Binance(c) => c.query_client(symbol, local).await,
            Venue::Okx(c) => c.query_client(symbol, local).await,
            Venue::Bybit(c) => c.query_client(symbol, local).await,
            Venue::Hyperliquid(c) => c.query_client(symbol, local).await,
            Venue::Gate(c) => c.query_client(symbol, local).await,
            Venue::Kucoin(c) => c.query_client(symbol, local).await,
            _ => Err("поиск по своему номеру не поддерживается".into()),
        }
    }

    /// Адрес отдельного торгового сокета.
    fn trade_url(&self, testnet: bool) -> String {
        match self {
            Venue::Bybit(_) => crate::exchange::bybit::trade_url(testnet),
            Venue::Kucoin(c) => c.trade_url(),
            _ => crate::exchange::binance::ws_fapi_url(testnet),
        }
    }
}

/// Сколько ждать ответа на запрос по сокету ордеров.
const WS_TIMEOUT: Duration = Duration::from_secs(5);

/// Ордер: по сокету, если он подключён, иначе по REST; исход по сокету неизвестен — найти ордер по своему номеру.
#[allow(clippy::too_many_arguments)]
async fn place_order(
    ex: &str,
    client: &Venue,
    link: Option<&crate::trading::trade_ws::TradeLink>,
    local: u64,
    symbol: &str,
    side: Side,
    qty: f64,
    price: Option<f64>,
    ref_px: f64,
) -> Result<OrderState, String> {
    if let Some(link) = link {
        if link.ready() {
            let _ = crate::infra::ratelimit::take(ex, "order").await;
        }
        let t = std::time::Instant::now();
        match client
            .place_ws(link, local, symbol, side, qty, price, ref_px)
            .await
        {
            WsOut::NotReady => {}
            WsOut::Done(r) => {
                crate::infra::metrics::record(
                    ex,
                    "order_ws",
                    t.elapsed().as_micros() as u64,
                    r.is_ok(),
                );
                return r;
            }
            WsOut::Unknown(why) => {
                tracing::warn!(
                    "[{ex}] ордер #{local} {symbol}: {why} — ищу по своему номеру через REST"
                );
                crate::infra::journal::action(
                    ex,
                    Some(symbol),
                    &format!("ордер #{local}: {why} — проверка по REST"),
                );
                let mut last = why;
                for i in 0..3 {
                    if i > 0 {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    match crate::infra::metrics::timed(
                        ex,
                        "query",
                        client.query_client(symbol, local),
                    )
                    .await
                    {
                        Ok(st) => return Ok(st),
                        Err(e) => last = e,
                    }
                }
                return Err(format!(
                    "исход ордера по сокету неизвестен, по REST не найден: {last}"
                ));
            }
        }
    }
    crate::infra::metrics::timed(
        ex,
        "order",
        client.place(local, symbol, side, qty, price, ref_px),
    )
    .await
}

/// Фоновая задача живого счёта: сверка часов, балансы, отправка и снятие ордеров, опрос статусов.
pub async fn worker(
    acc: Shared,
    ex: String,
    client: Arc<Venue>,
    mut rx: mpsc::UnboundedReceiver<Cmd>,
    symbols: Vec<String>,
    leverage: Option<f64>,
    ws_orders: bool,
) {
    if let Err(e) = crate::infra::metrics::timed(&ex, "time", client.sync_time()).await {
        tracing::warn!("[{ex}] время сервера не получено: {e}");
    }
    // перпы: плечо на бирже — как в параметрах
    if let Some(lev) = leverage {
        for s in &symbols {
            if let Err(e) =
                crate::infra::metrics::timed(&ex, "leverage", client.set_leverage(s, lev)).await
            {
                tracing::warn!("[{ex}] плечо {lev} для {s} не выставлено: {e}");
                with_live(&acc, &ex, |l| l.error = format!("плечо {s}: {e}"));
            }
        }
    }
    let testnet = with_live(&acc, &ex, |l| l.mode == "TESTNET").unwrap_or(false);
    let mode = if ws_orders {
        client.trade_socket()
    } else {
        TradeSocket::None
    };
    let link = (mode != TradeSocket::None)
        .then(|| Arc::new(crate::trading::trade_ws::TradeLink::default()));
    with_live(&acc, &ex, |l| l.trade = link.clone());
    // Uniswap: потока событий нет — итог свопа известен по квитанции
    let _stream = (!matches!(client.as_ref(), Venue::Uniswap(_))).then(|| {
        AbortOnDrop(tokio::spawn(user_stream(
            acc.clone(),
            ex.clone(),
            client.clone(),
            testnet,
            link.clone().filter(|_| mode == TradeSocket::Shared),
        )))
    });
    let _trade = link.clone().filter(|_| mode == TradeSocket::Own).map(|l| {
        AbortOnDrop(tokio::spawn(trade_socket(
            ex.clone(),
            client.clone(),
            testnet,
            l,
        )))
    });
    let mut poll = tokio::time::interval(Duration::from_millis(500));
    let mut n: u64 = 0;
    loop {
        tokio::select! {
            cmd = rx.recv() => {
                let Some(cmd) = cmd else { return };
                match cmd {
                    Cmd::Place { local, symbol, side, qty, price, ref_px } => {
                        let r = place_order(&ex, &client, link.as_deref(), local, &symbol, side, qty, price, ref_px).await;
                        crate::infra::journal::action(&ex, Some(&symbol), &match &r {
                            Ok(st) => format!("биржа приняла ордер #{local} {side:?} {qty}{}: номер {}, {} {}", price.map_or(String::new(), |p| format!(" @ {p}")), st.id, st.status, st.executed),
                            Err(e) => format!("биржа отклонила ордер #{local} {side:?} {qty}: {e}"),
                        });
                        let cancel_now = with_live(&acc, &ex, |l| match &r {
                            Ok(st) => { l.on_state(local, st); l.wants_cancel(local) }
                            Err(e) => { tracing::warn!("[{ex}] ордер {symbol} отклонён: {e}"); l.on_error(local, e); false }
                        }).unwrap_or(false);
                        if cancel_now {
                            cancel(&acc, &ex, &client, link.as_deref(), local).await;
                        }
                    }
                    Cmd::Cancel { local } => cancel(&acc, &ex, &client, link.as_deref(), local).await,
                    Cmd::PlaceStop { symbol, side, qty, price, old } => {
                        let r = crate::infra::metrics::timed(&ex, "stop", client.place_stop(&symbol, side, qty, price)).await;
                        match &r {
                            Ok(Some(id)) => tracing::warn!("[{ex}] стоп на бирже {symbol} {side:?} {qty} @ {price} (id {id})"),
                            Ok(None) => {}
                            Err(e) => tracing::error!("[{ex}] стоп по {symbol} не поставлен: {e} — повтор через 30 с"),
                        }
                        // старый снимается только после нового (у Bybit стоп позиции один — id тот же)
                        if let (Ok(Some(id)), Some(o)) = (&r, &old)
                            && id != o
                            && let Err(e) = crate::infra::metrics::timed(&ex, "stop_cancel", client.cancel_stop(&symbol, o)).await
                        {
                            tracing::warn!("[{ex}] стоп {o} по {symbol} не снят: {e}");
                        }
                        with_live(&acc, &ex, |l| l.stop_placed(&symbol, &r, old));
                    }
                    Cmd::CancelStop { symbol, id } => match crate::infra::metrics::timed(&ex, "stop_cancel", client.cancel_stop(&symbol, &id)).await {
                        Ok(()) => tracing::info!("[{ex}] стоп {id} по {symbol} снят"),
                        Err(e) => tracing::warn!("[{ex}] стоп {id} по {symbol} не снят (возможно, сработал): {e}"),
                    },
                    Cmd::Networks { coin } => {
                        let r = crate::infra::metrics::timed(&ex, "transfer", client.networks(&coin)).await;
                        with_live(&acc, &ex, |l| { l.networks.insert(coin, r); });
                    }
                    Cmd::Address { coin, network } => {
                        let r = crate::infra::metrics::timed(&ex, "transfer", client.deposit_address(&coin, &network)).await;
                        with_live(&acc, &ex, |l| { l.addresses.insert((coin, network), r); });
                    }
                    Cmd::Withdraw { tid, coin, network, address, tag, amount } => {
                        let r = crate::infra::metrics::timed(&ex, "transfer", client.withdraw(&coin, &network, &address, &tag, amount)).await;
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
                    match crate::infra::metrics::timed(&ex, "query", client.query(&symbol, &id)).await {
                        Ok(st) => { with_live(&acc, &ex, |l| l.on_state(local, &st)); }
                        Err(e) => { with_live(&acc, &ex, |l| l.error = e); }
                    }
                }
                if n % 10 == 1 && client.perp() {
                    match crate::infra::metrics::timed(&ex, "positions", client.positions()).await {
                        Ok(ps) => { with_live(&acc, &ex, |l| l.sync_positions(&ps)); }
                        Err(e) => { tracing::warn!("[{ex}] позиции не получены: {e}"); with_live(&acc, &ex, |l| l.error = e); }
                    }
                }
                if n % 10 == 1 {
                    match crate::infra::metrics::timed(&ex, "balances", client.balances()).await {
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
                Ok(format!(
                    "{}/{k}",
                    crate::exchange::binance::fstream_url(testnet)
                ))
            }
            Venue::Binance(_) => Ok(crate::exchange::binance::ws_api_url(testnet)),
            Venue::Bybit(_) => Ok(crate::exchange::bybit::private_url(testnet)),
            Venue::Okx(_) => Ok(crate::exchange::okx::private_url(testnet)),
            Venue::Gate(c) => c.stream_url().await,
            Venue::Kucoin(c) => c.stream_url().await,
            Venue::Bingx(c) => c.stream_url().await,
            Venue::Mexc(c) => c.stream_url().await,
            Venue::Lbank(c) => c.stream_url().await,
            Venue::Upbit(_) => Ok(crate::exchange::upbit::stream_url()),
            Venue::Uniswap(_) => Err("у Uniswap нет потока событий".into()),
            Venue::Hyperliquid(_) => Ok(crate::exchange::hyperliquid::stream_url(testnet)),
            Venue::Aster(c) => {
                let k = c
                    .listen_key(reqwest::Method::POST)
                    .await
                    .map_err(|e| format!("listenKey: {e}"))?;
                Ok(format!(
                    "{}/{k}",
                    crate::exchange::aster::stream_url(c.perp, testnet)
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
            Venue::Gate(c) => Some([vec![c.login()], c.subscriptions()].concat()),
            Venue::Kucoin(_) => Some(Vec::new()),
            Venue::Bingx(c) if !c.perp => Some(vec![c.subscription()]),
            Venue::Mexc(c) => Some(c.hello()),
            Venue::Hyperliquid(c) => Some(c.hello()),
            Venue::Lbank(c) => Some(c.hello()),
            Venue::Upbit(c) => Some(c.hello()),
            _ => None,
        }
    }

    /// Запрос подключения потока: Upbit — с JWT в заголовке Authorization.
    fn stream_request(
        &self,
        url: &str,
    ) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, String> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut r = url.into_client_request().map_err(|e| e.to_string())?;
        if let Venue::Upbit(c) = self {
            let h = c
                .auth("")
                .parse()
                .map_err(|_| "JWT не годится для заголовка".to_owned())?;
            r.headers_mut().insert("Authorization", h);
        }
        Ok(r)
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
            Venue::Upbit(_) => Some("PING".into()),
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
        if let Venue::Upbit(c) = self {
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
                                .map(|d| crate::exchange::okx::parse_balances(d, c.perp))
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
async fn user_stream(
    acc: Shared,
    ex: String,
    venue: Arc<Venue>,
    testnet: bool,
    link: Option<Arc<crate::trading::trade_ws::TradeLink>>,
) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let mut backoff = crate::market::feed::Backoff::default();
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
        let _ = crate::infra::ratelimit::take(&ex, "ws_connect").await;
        let conn = match tokio::time::timeout(Duration::from_secs(10), async {
            match venue.stream_request(&url) {
                Ok(r) => tokio_tungstenite::connect_async(r).await,
                Err(e) => Err(tokio_tungstenite::tungstenite::Error::Io(
                    std::io::Error::other(e),
                )),
            }
        })
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
                // ордера по этому же сокету: запросы из worker идут сюда, пока сокет вошёл
                let (otx, mut orx) = mpsc::unbounded_channel::<String>();
                let ready = |acc: &Shared| {
                    tracing::info!("[{ex}] поток событий счёта подключён");
                    with_live(acc, &ex, |l| l.stream_ok = true);
                    if let Some(k) = &link {
                        k.attach(otx.clone());
                    }
                };
                if hello.is_none() {
                    backoff.reset();
                    ready(&acc);
                }
                for m in hello.unwrap_or_default() {
                    let _ = crate::infra::ratelimit::take(&ex, "ws_send").await;
                    if let Err(e) = ws.send(Message::text(m)).await {
                        err = e.to_string();
                    }
                }
                while err.is_empty() {
                    let msg = tokio::select! {
                        m = ws.next() => m,
                        Some(o) = orx.recv() => {
                            err = ws.send(Message::text(o)).await.err().map(|e| e.to_string()).unwrap_or_default();
                            continue;
                        }
                        _ = ping.tick() => {
                            pings += 1;
                            if let Some(p) = venue.stream_ping() {
                                let _ = crate::infra::ratelimit::take(&ex, "ws_send").await;
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
                    if matches!(msg, Some(Ok(Message::Text(_) | Message::Binary(_)))) {
                        crate::infra::metrics::inbound(&ex, "private");
                    }
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
                        Some(Ok(Message::Binary(b))) => {
                            match crate::market::feed::bingx::gunzip(&b) {
                                Some(t) => Some(Ok(Message::text(t))),
                                None => continue,
                            }
                        }
                        m => m,
                    };
                    if let (Some(k), Some(Ok(Message::Text(t)))) = (&link, &msg)
                        && let Some((id, r)) = venue.trade_reply(t)
                        && k.resolve(&id, r)
                    {
                        continue;
                    }
                    match msg {
                        Some(Ok(Message::Text(t))) => match venue.stream_parse(&t, &sub) {
                            StreamMsg::Ignore => {}
                            StreamMsg::Ready => {
                                backoff.reset();
                                ready(&acc);
                            }
                            StreamMsg::SendMany(ms) => {
                                for m in ms {
                                    let _ = crate::infra::ratelimit::take(&ex, "ws_send").await;
                                    if let Err(e) = ws.send(Message::text(m)).await {
                                        err = e.to_string();
                                    }
                                }
                            }
                            StreamMsg::Send(m) => {
                                let _ = crate::infra::ratelimit::take(&ex, "ws_send").await;
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
        if let Some(k) = &link {
            k.detach();
        }
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

/// Отдельный торговый сокет (Binance перпы ws-fapi, Bybit /v5/trade): подключение, вход (Bybit), пинги, ответы
/// по id; при обрыве ордера идут по REST, переподключение с паузой 0.5 → 30 с.
async fn trade_socket(
    ex: String,
    venue: Arc<Venue>,
    testnet: bool,
    link: Arc<crate::trading::trade_ws::TradeLink>,
) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let mut backoff = crate::market::feed::Backoff::default();
    loop {
        let url = venue.trade_url(testnet);
        let _ = crate::infra::ratelimit::take(&ex, "ws_connect").await;
        let conn = tokio::time::timeout(
            Duration::from_secs(10),
            tokio_tungstenite::connect_async(url.as_str()),
        )
        .await;
        let err = match conn {
            Err(_) => "подключение не ответило за 10 с".to_owned(),
            Ok(Err(e)) => e.to_string(),
            Ok(Ok((mut ws, _))) => {
                let (otx, mut orx) = mpsc::unbounded_channel::<String>();
                let mut err = String::new();
                match venue.as_ref() {
                    Venue::Bybit(c) => {
                        err = ws
                            .send(Message::text(c.auth_message()))
                            .await
                            .err()
                            .map(|e| e.to_string())
                            .unwrap_or_default()
                    }
                    // KuCoin: ключ в адресе, вход по приветствию сервера
                    Venue::Kucoin(_) => {}
                    _ => {
                        backoff.reset();
                        link.attach(otx.clone());
                        tracing::info!("[{ex}] сокет ордеров подключён");
                    }
                }
                let mut ping = tokio::time::interval_at(
                    tokio::time::Instant::now() + Duration::from_secs(20),
                    Duration::from_secs(20),
                );
                while err.is_empty() {
                    let msg = tokio::select! {
                        m = ws.next() => m,
                        Some(o) = orx.recv() => {
                            err = ws.send(Message::text(o)).await.err().map(|e| e.to_string()).unwrap_or_default();
                            continue;
                        }
                        _ = ping.tick() => {
                            let p = match venue.as_ref() {
                                Venue::Bybit(_) => Some(r#"{"op":"ping"}"#.to_owned()),
                                Venue::Kucoin(_) => Some(format!(r#"{{"id":"ping-{0}","op":"ping","timestamp":"{0}"}}"#, crate::infra::util::now_ms())),
                                _ => None,
                            };
                            if let Some(p) = p {
                                err = ws.send(Message::text(p)).await.err().map(|e| e.to_string()).unwrap_or_default();
                            }
                            continue;
                        }
                    };
                    match msg {
                        Some(Ok(Message::Text(t))) => {
                            let v: serde_json::Value = serde_json::from_str(&t).unwrap_or_default();
                            // KuCoin: приветствие → подписанный ответ, второе сообщение с sessionId — сессия готова
                            if let (Venue::Kucoin(c), true) =
                                (venue.as_ref(), v.get("sessionId").is_some())
                            {
                                if v["data"] == "welcome" {
                                    err = ws
                                        .send(Message::text(c.welcome_answer(&t)))
                                        .await
                                        .err()
                                        .map(|e| e.to_string())
                                        .unwrap_or_default();
                                } else {
                                    backoff.reset();
                                    link.attach(otx.clone());
                                    tracing::info!("[{ex}] сокет ордеров подключён");
                                }
                            } else if v["op"] == "auth" {
                                if v["retCode"] == 0 {
                                    backoff.reset();
                                    link.attach(otx.clone());
                                    tracing::info!("[{ex}] сокет ордеров подключён");
                                } else {
                                    err = format!("вход в сокет ордеров отклонён: {}", v["retMsg"]);
                                }
                            } else if let Some((id, r)) = venue.trade_reply(&t) {
                                link.resolve(&id, r);
                            }
                        }
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
        };
        link.detach();
        let pause = backoff.next();
        tracing::warn!(
            "[{ex}] сокет ордеров: {err} — ордера по REST, переподключение через {} мс",
            pause.as_millis()
        );
        tokio::time::sleep(pause).await;
    }
}

/// Снять ордер на бирже; номера ещё нет — снимется, когда ордер встанет (см. Place).
async fn cancel(
    acc: &Shared,
    ex: &str,
    client: &Venue,
    link: Option<&crate::trading::trade_ws::TradeLink>,
    local: u64,
) {
    let Some((symbol, id)) = with_live(acc, ex, |l| l.cancel_target(local)).flatten() else {
        return;
    };
    if let Some(link) = link {
        if link.ready() {
            let _ = crate::infra::ratelimit::take(ex, "cancel").await;
        }
        let t = std::time::Instant::now();
        match client.cancel_ws(link, &symbol, &id).await {
            WsOut::Done(Ok(st)) => {
                crate::infra::metrics::record(
                    ex,
                    "cancel_ws",
                    t.elapsed().as_micros() as u64,
                    true,
                );
                crate::infra::journal::action(
                    ex,
                    Some(&symbol),
                    &format!("ордер {id} снят по сокету"),
                );
                if let Some(st) = st {
                    with_live(acc, ex, |l| l.on_state(local, &st));
                }
                return;
            }
            // отказ (уже исполнен, снят) или исход неизвестен — итог узнаём по REST
            WsOut::Done(Err(e)) | WsOut::Unknown(e) => {
                crate::infra::metrics::record(
                    ex,
                    "cancel_ws",
                    t.elapsed().as_micros() as u64,
                    false,
                );
                tracing::info!("[{ex}] отмена {id} по сокету: {e} — проверка по REST");
            }
            WsOut::NotReady => {}
        }
    }
    match crate::infra::metrics::timed(ex, "cancel", client.cancel(&symbol, &id)).await {
        Ok(st) => {
            crate::infra::journal::action(
                ex,
                Some(&symbol),
                &format!("ордер {id} снят: {} {}", st.status, st.executed),
            );
            with_live(acc, ex, |l| l.on_state(local, &st));
        }
        Err(e) => {
            crate::infra::journal::action(ex, Some(&symbol), &format!("ордер {id} не снят: {e}"));
            tracing::warn!("[{ex}] не снят ордер {symbol} {id}: {e}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trading::trade_ws::TradeLink;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{Value, json};
    use tokio_tungstenite::tungstenite::Message;

    /// Мок торгового сокета: на каждый запрос — ответ из reply; первый запрос сохраняется в seen.
    async fn mock(
        reply: fn(&Value) -> Option<Value>,
    ) -> (String, Arc<Mutex<Vec<Value>>>, Arc<tokio::sync::Notify>) {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let kill = Arc::new(tokio::sync::Notify::new());
        let (s2, k2) = (seen.clone(), kill.clone());
        tokio::spawn(async move {
            let (tcp, _) = l.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            loop {
                tokio::select! {
                    m = ws.next() => {
                        let Some(Ok(Message::Text(t))) = m else { break };
                        let v: Value = serde_json::from_str(&t).unwrap();
                        s2.lock().unwrap().push(v.clone());
                        if let Some(r) = reply(&v) {
                            ws.send(Message::text(r.to_string())).await.unwrap();
                        }
                    }
                    _ = k2.notified() => break,
                }
            }
            drop(ws);
            loop {
                let _ = l.accept().await;
            }
        });
        (format!("ws://{addr}"), seen, kill)
    }

    fn keys() -> crate::exchange::binance::Keys {
        crate::exchange::binance::Keys {
            key: "k".into(),
            secret: "s".into(),
        }
    }

    async fn wait(f: impl Fn() -> bool) {
        let until = tokio::time::Instant::now() + Duration::from_secs(5);
        while !f() {
            assert!(tokio::time::Instant::now() < until, "не дождались");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn bybit_orders_over_trade_socket_and_rest_when_down() {
        let (url, seen, kill) = mock(|v| match v["op"].as_str()? {
            "auth" => Some(json!({"op": "auth", "retCode": 0, "retMsg": "OK"})),
            "order.create" => Some(json!({"reqId": v["reqId"], "retCode": 0, "retMsg": "OK", "op": "order.create",
                "data": {"orderId": "B-1", "orderLinkId": v["args"][0]["orderLinkId"]}})),
            "order.cancel" => Some(json!({"reqId": v["reqId"], "retCode": 110001, "retMsg": "order not exists", "op": "order.cancel"})),
            _ => None,
        })
        .await;
        crate::infra::util::set_env("BYBIT_WS_TRADE_URL", &url);
        let mut c = crate::exchange::bybit::Client::new("http://127.0.0.1:9", keys());
        c.category = "linear";
        let venue = Arc::new(Venue::Bybit(c));
        assert_eq!(venue.trade_socket(), TradeSocket::Own);
        let link = Arc::new(TradeLink::default());
        let _t = AbortOnDrop(tokio::spawn(trade_socket(
            "bybit".into(),
            venue.clone(),
            true,
            link.clone(),
        )));
        wait(|| link.ready()).await;
        let st = place_order(
            "bybit",
            &venue,
            Some(&link),
            7,
            "BTCUSDT",
            Side::Buy,
            0.5,
            Some(100.0),
            100.0,
        )
        .await
        .unwrap();
        assert_eq!((st.id.as_str(), st.status.as_str()), ("B-1", "NEW"));
        let v = seen.lock().unwrap()[1].clone();
        assert_eq!(v["args"][0]["orderLinkId"], "hft7");
        assert_eq!(v["args"][0]["category"], "linear");
        assert!(v["header"]["X-BAPI-TIMESTAMP"].is_string(), "{v}");
        // отказ биржи по сокету — ошибка как есть
        assert!(
            matches!(venue.cancel_ws(&link, "BTCUSDT", "B-1").await, WsOut::Done(Err(e)) if e.contains("110001"))
        );
        // сокет упал — запрос не уходит в сокет (вызывающий идёт в REST)
        kill.notify_one();
        wait(|| !link.ready()).await;
        assert!(matches!(
            venue
                .place_ws(&link, 8, "BTCUSDT", Side::Sell, 0.5, None, 100.0)
                .await,
            WsOut::NotReady
        ));
        assert!(link.fallbacks.load(std::sync::atomic::Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn binance_perp_orders_over_ws_fapi() {
        let (url, seen, _kill) = mock(|v| {
            (v["method"] == "order.place").then(|| {
                json!({"id": v["id"], "status": 200, "result": {"orderId": 55, "status": "FILLED",
                "executedQty": "0.010", "cumQuote": "1000.5"}})
            })
        })
        .await;
        crate::infra::util::set_env("BINANCE_WS_FAPI_URL", &url);
        let venue = Arc::new(Venue::Binance(crate::exchange::binance::Client::new_perp(
            "http://127.0.0.1:9",
            keys(),
        )));
        assert_eq!(venue.trade_socket(), TradeSocket::Own);
        let link = Arc::new(TradeLink::default());
        let _t = AbortOnDrop(tokio::spawn(trade_socket(
            "binance".into(),
            venue.clone(),
            true,
            link.clone(),
        )));
        wait(|| link.ready()).await;
        let st = place_order(
            "binance",
            &venue,
            Some(&link),
            3,
            "BTCUSDT",
            Side::Sell,
            0.01,
            None,
            100000.0,
        )
        .await
        .unwrap();
        assert_eq!(
            (st.id.as_str(), st.status.as_str(), st.executed, st.quote),
            ("55", "FILLED", 0.01, 1000.5)
        );
        let p = seen.lock().unwrap()[0]["params"].clone();
        assert_eq!(
            (
                p["newClientOrderId"].as_str(),
                p["type"].as_str(),
                p["side"].as_str()
            ),
            (Some("hft3"), Some("MARKET"), Some("SELL"))
        );
        // подпись: параметры без signature по алфавиту
        let mut kv: Vec<(String, String)> = p
            .as_object()
            .unwrap()
            .iter()
            .filter(|(k, _)| *k != "signature")
            .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
            .collect();
        kv.sort();
        let q: Vec<String> = kv.iter().map(|(k, v)| format!("{k}={v}")).collect();
        assert_eq!(
            p["signature"].as_str().unwrap(),
            crate::exchange::binance::sign("s", &q.join("&"))
        );
    }

    /// KuCoin: подпись в адресе, приветствие → ответ HMAC, сессия, ордер; обрыв → REST, затем переподключение.
    #[tokio::test]
    #[allow(clippy::result_large_err)]
    async fn kucoin_trade_socket_handshake_order_and_reconnect() {
        use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let kill = Arc::new(tokio::sync::Notify::new());
        let urls = Arc::new(Mutex::new(Vec::<String>::new()));
        let (k2, u2) = (kill.clone(), urls.clone());
        tokio::spawn(async move {
            for session in 0..2 {
                let (tcp, _) = l.accept().await.unwrap();
                let u3 = u2.clone();
                let mut ws =
                    tokio_tungstenite::accept_hdr_async(tcp, move |r: &Request, resp: Response| {
                        u3.lock().unwrap().push(r.uri().to_string());
                        Ok(resp)
                    })
                    .await
                    .unwrap();
                let welcome =
                    json!({"sessionId": "s1", "data": "welcome", "timestamp": 1}).to_string();
                ws.send(Message::text(welcome.clone())).await.unwrap();
                let Some(Ok(Message::Text(a))) = ws.next().await else {
                    return;
                };
                assert_eq!(
                    a.as_str(),
                    crate::infra::util::hmac_b64("s", &welcome),
                    "ответ на приветствие"
                );
                ws.send(Message::text(
                    json!({"sessionId": "s1", "pingInterval": 18000}).to_string(),
                ))
                .await
                .unwrap();
                loop {
                    tokio::select! {
                        m = ws.next() => {
                            let Some(Ok(Message::Text(t))) = m else { break };
                            let v: Value = serde_json::from_str(&t).unwrap();
                            if v["op"] == "spot.order" {
                                assert_eq!(v["args"]["clientOid"], format!("hft{}", 10 + session));
                                ws.send(Message::text(json!({"id": v["id"], "op": "spot.order", "code": "200000",
                                    "data": {"orderId": format!("K{session}")}}).to_string())).await.unwrap();
                            }
                        }
                        _ = k2.notified() => break,
                    }
                }
            }
            loop {
                let _ = l.accept().await;
            }
        });
        crate::infra::util::set_env("KUCOIN_WS_TRADE_URL", &format!("ws://{addr}/v1/private"));
        let venue = Arc::new(Venue::Kucoin(crate::exchange::kucoin::Client::new(
            "http://127.0.0.1:9",
            keys(),
            "pp".into(),
            false,
        )));
        assert_eq!(venue.trade_socket(), TradeSocket::Own);
        let link = Arc::new(TradeLink::default());
        let _t = AbortOnDrop(tokio::spawn(trade_socket(
            "kucoin".into(),
            venue.clone(),
            false,
            link.clone(),
        )));
        wait(|| link.ready()).await;
        let st = place_order(
            "kucoin",
            &venue,
            Some(&link),
            10,
            "BTCUSDT",
            Side::Buy,
            0.1,
            Some(100.0),
            100.0,
        )
        .await
        .unwrap();
        assert_eq!(st.id, "K0");
        let u = urls.lock().unwrap()[0].clone();
        assert!(
            u.contains("apikey=k&timestamp=") && u.contains("&sign=") && u.contains("&passphrase="),
            "{u}"
        );
        // обрыв: сокет не готов (ордера — по REST), затем переподключение с новым входом
        kill.notify_one();
        wait(|| !link.ready()).await;
        wait(|| link.ready()).await;
        let st = place_order(
            "kucoin",
            &venue,
            Some(&link),
            11,
            "BTCUSDT",
            Side::Buy,
            0.1,
            Some(100.0),
            100.0,
        )
        .await
        .unwrap();
        assert_eq!(st.id, "K1");
        assert_eq!(urls.lock().unwrap().len(), 2, "второе подключение");
    }

    #[test]
    fn replies_of_shared_sockets() {
        let okx = crate::exchange::okx::ws_reply(
            &json!({"id": "t1", "op": "order", "code": "0", "msg": "",
            "data": [{"ordId": "777", "clOrdId": "hft1", "sCode": "0", "sMsg": ""}]}),
        )
        .unwrap();
        assert_eq!(
            (okx.0.as_str(), okx.1.unwrap()["ordId"].as_str()),
            ("t1", Some("777"))
        );
        let bad = crate::exchange::okx::ws_reply(
            &json!({"id": "t2", "op": "order", "code": "1", "msg": "",
            "data": [{"sCode": "51008", "sMsg": "Insufficient balance"}]}),
        )
        .unwrap();
        assert_eq!(bad.1.unwrap_err(), "OKX 51008: Insufficient balance");
        assert!(crate::exchange::okx::ws_reply(&json!({"event": "login", "code": "0"})).is_none());
        let hl = crate::exchange::hyperliquid::ws_reply(&json!({"channel": "post", "data": {"id": 4, "response": {"type": "action",
            "payload": {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 9}}]}}}}}})).unwrap();
        assert_eq!(hl.0, "4");
        assert_eq!(
            hl.1.unwrap()["response"]["data"]["statuses"][0]["resting"]["oid"],
            9
        );
        let he = crate::exchange::hyperliquid::ws_reply(&json!({"channel": "post", "data": {"id": 5, "response": {"type": "error", "payload": "bad nonce"}}})).unwrap();
        assert_eq!(he.1.unwrap_err(), "Hyperliquid: bad nonce");
        let bn = crate::exchange::binance::ws_reply(&json!({"id": "t9", "status": 400, "error": {"code": -2010, "msg": "insufficient balance"}})).unwrap();
        assert_eq!(bn.1.unwrap_err(), "Binance -2010: insufficient balance");
    }
}
