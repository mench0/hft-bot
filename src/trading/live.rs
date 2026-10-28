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
        /// Выход по старому стакану (сокет стакана лежит): перед отправкой взять свежую цену по REST.
        refresh: bool,
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
    /// Перенести пришедший перевод со счёта фондирования на торговый (OKX, KuCoin; у остальных — ничего): только
    /// депозиты с момента since_ms и не больше max — свои монеты на счёте фондирования не трогаются.
    Sweep {
        coin: String,
        since_ms: u64,
        max: f64,
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
/// Стакан старше — перед рыночным выходом берётся свежая цена по REST.
const STALE_MS: u64 = 2_000;

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
        refresh: bool,
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
            refresh,
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
        Ok(self.send(symbol, side, pr.qty, pr.limit, pr.ref_px, false))
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
        // стакан старый (сокет стакана лежит) или его нет — выход всё равно идёт, а свежую цену возьмёт фоновая задача
        let b = book.filter(|b| b.is_ready());
        let ref_px = b
            .and_then(|b| {
                if side == Side::Buy {
                    b.best_ask()
                } else {
                    b.best_bid()
                }
            })
            .map_or(0.0, |x| x.0);
        let refresh = b.and_then(Book::age_ms).is_none_or(|a| a > STALE_MS);
        let qty = qty.min(self.ledger.exit_room(symbol, side));
        let qty = self
            .ledger
            .filter(&symbol.to_uppercase())
            .map_or(qty, |f| f.round_qty(qty));
        if qty <= 0.0 {
            return Err(format!("нечего закрывать по {symbol}"));
        }
        Ok(self.send(symbol, side, qty, None, ref_px, refresh))
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
    /// Свежие лучшие бид и аск по REST — только у бирж, где рыночный ордер строится от опорной цены (Gate спот —
    /// покупка суммой; Hyperliquid — IOC ±5 %; Upbit, LBank — покупка суммой); у остальных опорная цена не нужна (None).
    async fn fresh_top(&self, symbol: &str) -> Option<Result<(f64, f64), String>> {
        let (ex, url) = match self {
            Venue::Hyperliquid(c) => return Some(c.top(symbol).await),
            Venue::Gate(c) if !c.perp => ("gate", c.top_url(symbol)),
            Venue::Upbit(c) => ("upbit", c.top_url(symbol)),
            Venue::Lbank(c) => ("lbank", c.top_url(symbol)),
            _ => return None,
        };
        let r = async {
            let v: serde_json::Value = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .map_err(|e| e.to_string())?
                .get(&url)
                .send()
                .await
                .map_err(|e| e.to_string())?
                .json()
                .await
                .map_err(|e| e.to_string())?;
            let (b, a) = crate::discovery::screener::parse_book(ex, &v)
                .ok_or_else(|| format!("пустой стакан {symbol}"))?;
            Ok((b[0].0, a[0].0))
        };
        Some(r.await)
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
            Venue::Okx(c) if !c.perp => c.networks(coin).await,
            Venue::Gate(c) if !c.perp => c.networks(coin).await,
            Venue::Kucoin(c) if !c.perp => c.networks(coin).await,
            _ => Err("переводы с этой биржи не поддерживаются (есть у Binance, Bybit, OKX, Gate, KuCoin — спот)".into()),
        }
    }
    async fn deposit_address(&self, coin: &str, net: &str) -> Result<(String, String), String> {
        match self {
            Venue::Binance(c) => c.deposit_address(coin, net).await,
            Venue::Bybit(c) => c.deposit_address(coin, net).await,
            Venue::Okx(c) if !c.perp => c.deposit_address(coin, net).await,
            Venue::Gate(c) if !c.perp => c.deposit_address(coin, net).await,
            Venue::Kucoin(c) if !c.perp => c.deposit_address(coin, net).await,
            _ => Err("переводы с этой биржи не поддерживаются (есть у Binance, Bybit, OKX, Gate, KuCoin — спот)".into()),
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
            Venue::Okx(c) if !c.perp => c.withdraw(coin, net, address, tag, amount).await,
            Venue::Gate(c) if !c.perp => c.withdraw(coin, net, address, tag, amount).await,
            Venue::Kucoin(c) if !c.perp => c.withdraw(coin, net, address, tag, amount).await,
            _ => Err("переводы с этой биржи не поддерживаются (есть у Binance, Bybit, OKX, Gate, KuCoin — спот)".into()),
        }
    }
    /// Перевод, пришедший на счёт фондирования (OKX, KuCoin), — на торговый: депозиты с since_ms за вычетом уже
    /// перенесённого (moved), не больше max; сколько перенесено сейчас.
    async fn sweep(&self, coin: &str, since_ms: u64, max: f64, moved: f64) -> Result<f64, String> {
        let arrived = match self {
            Venue::Okx(c) if !c.perp => c.deposits_since(coin, since_ms).await?,
            Venue::Kucoin(c) if !c.perp => c.deposits_since(coin, since_ms).await?,
            _ => return Ok(0.0),
        };
        let n = arrived.min(max) - moved;
        if n <= 1e-12 {
            return Ok(0.0);
        }
        match self {
            Venue::Okx(c) => c.sweep(coin, n).await,
            Venue::Kucoin(c) => c.sweep(coin, n).await,
            _ => Ok(0.0),
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

/// Начало ошибки «исход неизвестен»: ордер не считается отклонённым, его ищут по своему номеру ещё LOST_FOR.
const LOST: &str = "исход ордера по сокету неизвестен, ";
/// Сколько искать ордер с неизвестным исходом, прежде чем считать его не дошедшим до биржи.
const LOST_FOR: Duration = Duration::from_secs(120);

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
            let _ = crate::infra::ratelimit::take(ex, "orders").await;
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
                return Err(format!("{LOST}по REST пока не найден: {last}"));
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
    // ордера с неизвестным исходом: (свой номер, символ, до какого времени искать)
    let mut lost: Vec<(u64, String, tokio::time::Instant)> = Vec::new();
    // уже перенесено на торговый счёт по переводу (монета, начало перевода)
    let mut swept: HashMap<(String, u64), f64> = HashMap::new();
    loop {
        tokio::select! {
            cmd = rx.recv() => {
                let Some(cmd) = cmd else { return };
                match cmd {
                    Cmd::Place { local, symbol, side, qty, price, mut ref_px, refresh } => {
                        // выход по старому стакану: свежая цена по REST (одним запросом, только для этого ордера)
                        if refresh && price.is_none() {
                            let _ = crate::infra::ratelimit::take(&ex, "market").await;
                            match crate::infra::metrics::timed(&ex, "exit_price", async { client.fresh_top(&symbol).await.unwrap_or(Err(String::new())) }).await {
                                Ok((bid, ask)) => {
                                    let px = if side == Side::Buy { ask } else { bid };
                                    tracing::warn!("[{ex}] выход {symbol}: стакан старый — свежая цена по REST {px} (было {ref_px})");
                                    ref_px = px;
                                }
                                Err(e) if !e.is_empty() => tracing::warn!("[{ex}] выход {symbol}: свежая цена по REST не получена ({e}) — по последнему стакану {ref_px}"),
                                Err(_) => {}
                            }
                        }
                        let r = place_order(&ex, &client, link.as_deref(), local, &symbol, side, qty, price, ref_px).await;
                        crate::infra::journal::action(&ex, Some(&symbol), &match &r {
                            Ok(st) => format!("биржа приняла ордер #{local} {side:?} {qty}{}: номер {}, {} {}", price.map_or(String::new(), |p| format!(" @ {p}")), st.id, st.status, st.executed),
                            Err(e) if e.starts_with(LOST) => format!("ордер #{local} {side:?} {qty}: {e}"),
                            Err(e) => format!("биржа отклонила ордер #{local} {side:?} {qty}: {e}"),
                        });
                        // исход неизвестен — не «отклонён»: ордер мог встать; искать по своему номеру ещё 2 мин
                        if let Err(e) = &r && e.starts_with(LOST) {
                            tracing::warn!("[{ex}] ордер #{local} {symbol}: {e} — ищу ещё {} с", LOST_FOR.as_secs());
                            lost.push((local, symbol.clone(), tokio::time::Instant::now() + LOST_FOR));
                            continue;
                        }
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
                    Cmd::Sweep { coin, since_ms, max } => {
                        let done = swept.get(&(coin.clone(), since_ms)).copied().unwrap_or(0.0);
                        match crate::infra::metrics::timed(&ex, "transfer", client.sweep(&coin, since_ms, max, done)).await {
                            Ok(n) if n > 0.0 => {
                                *swept.entry((coin.clone(), since_ms)).or_default() += n;
                                tracing::warn!("[{ex}] пришедший перевод {n} {coin} перенесён на торговый счёт");
                            }
                            Ok(_) => {}
                            Err(e) => tracing::warn!("[{ex}] перенос {coin} на торговый счёт не удался: {e}"),
                        }
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
                // ордера с неизвестным исходом — раз в 2 с по своему номеру
                if n.is_multiple_of(4) && !lost.is_empty() {
                    let mut keep = Vec::new();
                    for (local, symbol, until) in std::mem::take(&mut lost) {
                        match crate::infra::metrics::timed(&ex, "query", client.query_client(&symbol, local)).await {
                            Ok(st) => {
                                tracing::warn!("[{ex}] ордер #{local} {symbol} найден: {} {}", st.id, st.status);
                                crate::infra::journal::action(&ex, Some(&symbol), &format!("ордер #{local} с неизвестным исходом найден: {} {}", st.id, st.status));
                                let cancel_now = with_live(&acc, &ex, |l| { l.on_state(local, &st); l.wants_cancel(local) }).unwrap_or(false);
                                if cancel_now {
                                    cancel(&acc, &ex, &client, link.as_deref(), local).await;
                                }
                            }
                            Err(_) if tokio::time::Instant::now() < until => keep.push((local, symbol, until)),
                            Err(e) => {
                                let e = format!("ордер не найден на бирже за {} с после обрыва сокета: {e}", LOST_FOR.as_secs());
                                tracing::warn!("[{ex}] #{local} {symbol}: {e}");
                                with_live(&acc, &ex, |l| l.on_error(local, &e));
                            }
                        }
                    }
                    lost = keep;
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
                // KuCoin — раз в 15 с (как в Java), остальные — раз в 20 с
                let every = Duration::from_secs(if matches!(venue.as_ref(), Venue::Kucoin(_)) {
                    15
                } else {
                    20
                });
                let mut ping = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
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
            let _ = crate::infra::ratelimit::take(ex, "orders").await;
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

    /// Сокет упал сразу после отправки ордера (ответа нет): ордер не повторяется, а ищется по REST по orderLinkId.
    #[tokio::test]
    async fn unknown_outcome_resolved_over_rest_by_client_id() {
        use axum::{Json, Router, extract::RawQuery, routing::get};
        let queries = Arc::new(Mutex::new(Vec::<String>::new()));
        let q2 = queries.clone();
        let rest = Router::new().route(
            "/v5/order/realtime",
            get(move |RawQuery(q): RawQuery| async move {
                let q = q.unwrap_or_default();
                q2.lock().unwrap().push(q.clone());
                Json(json!({"retCode": 0, "retMsg": "OK", "result": {"list": if q.contains("orderLinkId=hft21") {
                    json!([{"orderId": "B-21", "orderStatus": "Filled", "cumExecQty": "0.5", "cumExecValue": "50"}]) } else { json!([]) }}}))
            }),
        );
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", rl.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        // торговый сокет: вход — да, на ордер не отвечает и рвёт соединение
        let (url, seen, kill) =
            mock(|v| (v["op"] == "auth").then(|| json!({"op": "auth", "retCode": 0}))).await;
        let s2 = seen.clone();
        tokio::spawn(async move {
            loop {
                if s2.lock().unwrap().iter().any(|v| v["op"] == "order.create") {
                    kill.notify_one();
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        crate::infra::util::set_env("BYBIT_WS_TRADE_URL", &url);
        let venue = Arc::new(Venue::Bybit(crate::exchange::bybit::Client::new(
            &base,
            keys(),
        )));
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
            21,
            "BTCUSDT",
            Side::Buy,
            0.5,
            None,
            100.0,
        )
        .await
        .unwrap();
        assert_eq!(
            (st.id.as_str(), st.status.as_str(), st.executed),
            ("B-21", "FILLED", 0.5)
        );
        assert_eq!(
            seen.lock()
                .unwrap()
                .iter()
                .filter(|v| v["op"] == "order.create")
                .count(),
            1,
            "ордер не повторён"
        );
        assert!(queries.lock().unwrap()[0].contains("orderLinkId=hft21"));
        assert_eq!(link.unknown.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    /// Переводы OKX: вывод — перевод с торгового счёта на фондирования, затем вывод суммы за вычетом комиссии;
    /// пришедший депозит — обратно на торговый. Gate: адрес из multichain_addresses, вывод со спота.
    #[tokio::test]
    async fn okx_and_gate_transfers_against_mock() {
        use axum::{
            Json, Router,
            routing::{get, post},
        };
        let calls = Arc::new(Mutex::new(Vec::<(String, Value)>::new()));
        let (c1, c2, c3) = (calls.clone(), calls.clone(), calls.clone());
        let okx = Router::new()
            .route("/api/v5/asset/currencies", get(|| async { Json(json!({"code": "0", "data": [
                {"ccy": "USDT", "chain": "USDT-TRC20", "canDep": true, "canWd": true, "fee": "1", "minWd": "2"}]})) }))
            .route("/api/v5/asset/transfer", post(move |b: String| async move {
                c1.lock().unwrap().push(("transfer".into(), serde_json::from_str(&b).unwrap()));
                Json(json!({"code": "0", "data": [{"transId": "1"}]}))
            }))
            .route("/api/v5/asset/withdrawal", post(move |b: String| async move {
                c2.lock().unwrap().push(("withdrawal".into(), serde_json::from_str(&b).unwrap()));
                Json(json!({"code": "0", "data": [{"wdId": "W9"}]}))
            }))
            // на счёте фондирования 548.5: 500 — свои монеты, 48.5 — пришедший перевод (депозит после начала вывода)
            .route("/api/v5/asset/balances", get(|| async { Json(json!({"code": "0", "data": [{"ccy": "USDT", "availBal": "548.5"}]})) }))
            .route("/api/v5/asset/deposit-history", get(|| async { Json(json!({"code": "0", "data": [
                {"ccy": "USDT", "amt": "48.5", "state": "2", "ts": "2000"}, {"ccy": "USDT", "amt": "500", "state": "2", "ts": "500"}]})) }))
            .route("/api/v5/asset/deposit-address", get(|| async { Json(json!({"code": "0", "data": [
                {"chain": "USDT-ERC20", "addr": "0xe"}, {"chain": "USDT-TRC20", "addr": "Tabc", "tag": ""}]})) }));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, okx).await.unwrap() });
        let v = Venue::Okx(crate::exchange::okx::Client::new(
            &base,
            keys(),
            "p".into(),
            false,
            true,
        ));
        assert_eq!(
            v.deposit_address("USDT", "USDT-TRC20").await.unwrap().0,
            "Tabc"
        );
        assert_eq!(
            v.withdraw("USDT", "USDT-TRC20", "Txyz", "", 50.0)
                .await
                .unwrap(),
            "W9"
        );
        // переносится только перевод (депозит после 1000), не свои 500
        assert_eq!(v.sweep("USDT", 1000, 49.0, 0.0).await.unwrap(), 48.5);
        // уже перенесено — второй раз ничего
        assert_eq!(v.sweep("USDT", 1000, 49.0, 48.5).await.unwrap(), 0.0);
        let c = calls.lock().unwrap().clone();
        assert_eq!(
            (
                c[0].1["from"].as_str(),
                c[0].1["to"].as_str(),
                c[0].1["amt"].as_str()
            ),
            (Some("18"), Some("6"), Some("50"))
        );
        assert_eq!(
            (
                c[1].1["amt"].as_str(),
                c[1].1["toAddr"].as_str(),
                c[1].1["chain"].as_str()
            ),
            (Some("49"), Some("Txyz"), Some("USDT-TRC20"))
        );
        assert_eq!(
            (
                c[2].1["from"].as_str(),
                c[2].1["to"].as_str(),
                c[2].1["amt"].as_str()
            ),
            (Some("6"), Some("18"), Some("48.5"))
        );
        let gate = Router::new()
            .route("/api/v4/wallet/currency_chains", get(|| async { Json(json!([{"chain": "TRX", "is_disabled": 0, "is_deposit_disabled": 0, "is_withdraw_disabled": 0}])) }))
            .route("/api/v4/wallet/withdraw_status", get(|| async { Json(json!([{"currency": "USDT", "withdraw_amount_mini": "1", "withdraw_fix_on_chains": {"TRX": "1"}}])) }))
            .route("/api/v4/wallet/deposit_address", get(|| async { Json(json!({"currency": "USDT", "multichain_addresses": [
                {"chain": "TRX", "address": "Tgate", "payment_id": "", "obtain_failed": 0}]})) }))
            .route("/api/v4/withdrawals", post(move |b: String| async move {
                c3.lock().unwrap().push(("gate".into(), serde_json::from_str(&b).unwrap()));
                Json(json!({"id": "g1", "status": "REQUEST"}))
            }));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, gate).await.unwrap() });
        let g = Venue::Gate(crate::exchange::gate::Client::new(
            &base,
            keys(),
            false,
            true,
            &[],
        ));
        let n = g.networks("USDT").await.unwrap();
        assert_eq!((n[0].name.as_str(), n[0].withdraw_fee), ("TRX", 1.0));
        assert_eq!(g.deposit_address("USDT", "TRX").await.unwrap().0, "Tgate");
        assert_eq!(
            g.withdraw("USDT", "TRX", "Tokx", "", 20.0).await.unwrap(),
            "g1"
        );
        let w = calls.lock().unwrap().last().unwrap().1.clone();
        assert_eq!(
            (
                w["chain"].as_str(),
                w["amount"].as_str(),
                w["address"].as_str()
            ),
            (Some("TRX"), Some("20"), Some("Tokx"))
        );
        assert!(
            w["withdraw_order_id"].as_str().unwrap().starts_with("hft"),
            "свой номер вывода"
        );
    }

    /// Выход по старому стакану (или без стакана): ордер всё равно уходит, фоновая задача берёт свежую цену по REST.
    #[tokio::test]
    async fn exit_with_stale_book_refreshes_price_over_rest() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut l = Live::new(&["XUSDT".into()], 0.1, 0.1, "TESTNET", tx);
        l.ledger.set_balance("X", 5.0);
        // стакана нет — выход всё равно отправлен, цена — свежая по REST
        l.exit_market("XUSDT", Side::Sell, 2.0, None).unwrap();
        assert!(
            matches!(rx.recv().await.unwrap(), Cmd::Place { refresh: true, ref_px, .. } if ref_px == 0.0)
        );
        // свежий стакан — без запроса
        let mut b = Book::default();
        b.apply_snapshot(vec![(9.9, 1.0)], vec![(10.0, 1.0)], 1);
        l.exit_market("XUSDT", Side::Sell, 1.0, Some(&b)).unwrap();
        assert!(
            matches!(rx.recv().await.unwrap(), Cmd::Place { refresh: false, ref_px, .. } if ref_px == 9.9)
        );

        use axum::{Json, Router, routing::get};
        let app = Router::new()
            .route("/api/v4/spot/order_book", get(|| async { Json(json!({"bids": [["9.5", "1"]], "asks": [["9.6", "1"]]})) }))
            .route("/v1/orderbook", get(|| async { Json(json!([{"market": "USDT-X", "orderbook_units": [{"bid_price": 8.1, "ask_price": 8.2, "bid_size": 1, "ask_size": 1}]}])) }))
            .route("/v2/depth.do", get(|| async { Json(json!({"result": "true", "data": {"bids": [["7.1", "1"]], "asks": [["7.2", "1"]]}})) }));
        let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", ln.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(ln, app).await.unwrap() });
        let g = Venue::Gate(crate::exchange::gate::Client::new(
            &base,
            keys(),
            false,
            true,
            &[],
        ));
        assert_eq!(g.fresh_top("XUSDT").await.unwrap().unwrap(), (9.5, 9.6));
        let u = Venue::Upbit(crate::exchange::upbit::Client::new(&base, keys(), &[]));
        assert_eq!(u.fresh_top("XUSDT").await.unwrap().unwrap(), (8.1, 8.2));
        let lb = Venue::Lbank(crate::exchange::lbank::Client::new(&base, keys()));
        assert_eq!(lb.fresh_top("XUSDT").await.unwrap().unwrap(), (7.1, 7.2));
        // Binance: рыночному ордеру опорная цена не нужна — запроса нет
        let bn = Venue::Binance(crate::exchange::binance::Client::new(&base, keys()));
        assert!(bn.fresh_top("XUSDT").await.is_none());
    }

    /// Исход неизвестен и по REST ордер пока не найден — ошибка «исход неизвестен», а не «отклонён» (ордер ищут дальше).
    #[tokio::test]
    async fn unknown_outcome_not_found_is_lost_not_rejected() {
        use axum::{Json, Router, routing::get};
        let empty =
            || async { Json(json!({"retCode": 0, "retMsg": "OK", "result": {"list": []}})) };
        let rest = Router::new()
            .route("/v5/order/realtime", get(empty))
            .route("/v5/order/history", get(empty));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", rl.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        let link = Arc::new(TradeLink::default());
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        link.attach(tx);
        // сокет «принял» запрос и упал — ответа нет
        let l2 = link.clone();
        tokio::spawn(async move {
            let _ = rx.recv().await;
            l2.detach();
        });
        let venue = Venue::Bybit(crate::exchange::bybit::Client::new(&base, keys()));
        let e = place_order(
            "bybit",
            &venue,
            Some(&link),
            31,
            "BTCUSDT",
            Side::Buy,
            0.5,
            Some(100.0),
            100.0,
        )
        .await
        .unwrap_err();
        assert!(e.starts_with(LOST), "{e}");
    }

    /// Вывод OKX ответил ошибкой (таймаут), но есть в истории по своему clientId — это успех, монета не возвращается.
    #[tokio::test]
    async fn okx_withdraw_error_but_present_in_history() {
        use axum::{
            Json, Router,
            extract::RawQuery,
            routing::{get, post},
        };
        let moves = Arc::new(Mutex::new(Vec::<Value>::new()));
        let cid = Arc::new(Mutex::new(String::new()));
        let (m1, c1, c2) = (moves.clone(), cid.clone(), cid.clone());
        let app = Router::new()
            .route("/api/v5/asset/currencies", get(|| async { Json(json!({"code": "0", "data": [
                {"ccy": "USDT", "chain": "USDT-TRC20", "canDep": true, "canWd": true, "fee": "1", "minWd": "2"}]})) }))
            .route("/api/v5/asset/transfer", post(move |b: String| async move {
                m1.lock().unwrap().push(serde_json::from_str(&b).unwrap());
                Json(json!({"code": "0", "data": [{}]}))
            }))
            .route("/api/v5/asset/withdrawal", post(move |b: String| async move {
                let v: Value = serde_json::from_str(&b).unwrap();
                *c1.lock().unwrap() = v["clientId"].as_str().unwrap().to_owned();
                // вывод на бирже создан, а ответ — ошибка (как при обрыве)
                Json(json!({"code": "50001", "msg": "service temporarily unavailable", "data": []}))
            }))
            .route("/api/v5/asset/withdrawal-history", get(move |RawQuery(q): RawQuery| async move {
                let want = c2.lock().unwrap().clone();
                let hit = q.unwrap_or_default().contains(&format!("clientId={want}"));
                Json(json!({"code": "0", "data": if hit { json!([{"wdId": "W77", "clientId": want}]) } else { json!([]) }}))
            }));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        let v = Venue::Okx(crate::exchange::okx::Client::new(
            &base,
            keys(),
            "p".into(),
            false,
            true,
        ));
        assert_eq!(
            v.withdraw("USDT", "USDT-TRC20", "Txyz", "", 50.0)
                .await
                .unwrap(),
            "W77"
        );
        assert!(cid.lock().unwrap().starts_with("hft"));
        assert_eq!(
            moves.lock().unwrap().len(),
            1,
            "только перевод на фондирование, без возврата"
        );
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
