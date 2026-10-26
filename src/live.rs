//! Живой счёт Binance (спот, основная сеть или testnet). Ордер проверяется теми же правилами, что бумажный,
//! и уходит на биржу в фоновой задаче — движок не ждёт сеть. Статусы своих ордеров опрашиваются раз в 0.5 с,
//! балансы — раз в 5 с. Исполнения учитываются тем же учётом, что на бумаге (средняя цена, результат, журнал).

use crate::book::Book;
use crate::paper::{Amount, Fill, Limits, Paper, Side};
use crate::signed::{Client, OrderState, dec};
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
    },
    Cancel {
        local: u64,
    },
}

#[derive(Debug)]
struct Order {
    symbol: String,
    side: Side,
    /// Лимитный ордер — комиссия мейкера (оценка: биржа может взять тейкера, если ордер сразу исполнился).
    maker: bool,
    exch: Option<u64>,
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
        }
    }

    fn send(&mut self, symbol: &str, side: Side, qty: f64, price: Option<f64>) -> Fill {
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
        let (asset, need) = match side {
            Side::Buy => (&pr.quote, pr.qty * pr.ref_px * (1.0 + taker)),
            Side::Sell => (&pr.base, pr.qty),
        };
        if self.ledger.free(asset) + 1e-12 < need {
            let e = format!(
                "недостаточно {asset}: нужно {need:.8}, свободно {:.8}",
                self.ledger.free(asset)
            );
            self.ledger.note_rejected(&e);
            return Err(e);
        }
        self.ledger.note_accepted();
        Ok(self.send(symbol, side, pr.qty, pr.limit))
    }

    /// Выход рыночной продажей без проверок торговли (стоп, выключение стратегии).
    pub fn sell_exit(
        &mut self,
        symbol: &str,
        qty: f64,
        book: Option<&Book>,
    ) -> Result<Fill, String> {
        book.filter(|b| b.is_ready()).ok_or("стакан не готов")?;
        let base = crate::paper::split(symbol)
            .map(|x| x.0)
            .ok_or("не разобрать символ")?;
        let qty = qty.min(self.ledger.free(&base));
        let qty = self
            .ledger
            .filter(&symbol.to_uppercase())
            .map_or(qty, |f| f.round_qty(qty));
        if qty <= 0.0 {
            return Err(format!("нет {base} для продажи"));
        }
        Ok(self.send(symbol, Side::Sell, qty, None))
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
        o.exch = Some(st.id);
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

    /// Биржа отказала в ордере.
    pub fn on_error(&mut self, local: u64, err: &str) {
        if let Some(o) = self.orders.get_mut(&local) {
            o.done = true;
        }
        self.ledger.note_rejected(err);
        self.error = err.to_owned();
    }

    /// Ордера, которые надо опросить: стоят на бирже (номер известен) и не закончились.
    fn pending(&self) -> Vec<(u64, String, u64)> {
        self.orders
            .iter()
            .filter(|(_, o)| !o.done)
            .filter_map(|(id, o)| Some((*id, o.symbol.clone(), o.exch?)))
            .collect()
    }

    /// Снятие запрошено, а номер ордера на бирже уже известен.
    fn cancel_target(&self, local: u64) -> Option<(String, u64)> {
        self.orders
            .get(&local)
            .filter(|o| !o.done)
            .and_then(|o| Some((o.symbol.clone(), o.exch?)))
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

/// Фоновая задача живого счёта: сверка часов, балансы, отправка и снятие ордеров, опрос статусов.
pub async fn worker(
    acc: Shared,
    ex: String,
    client: Arc<Client>,
    mut rx: mpsc::UnboundedReceiver<Cmd>,
) {
    if let Err(e) = client.sync_time().await {
        tracing::warn!("[{ex}] время сервера не получено: {e}");
    }
    let mut poll = tokio::time::interval(Duration::from_millis(500));
    let mut n: u64 = 0;
    loop {
        tokio::select! {
            cmd = rx.recv() => {
                let Some(cmd) = cmd else { return };
                match cmd {
                    Cmd::Place { local, symbol, side, qty, price } => {
                        let mut p = vec![("symbol", symbol.clone()), ("side", if side == Side::Buy { "BUY" } else { "SELL" }.into()),
                            ("type", if price.is_some() { "LIMIT" } else { "MARKET" }.into()), ("quantity", dec(qty)),
                            ("newClientOrderId", format!("hft{local}")), ("newOrderRespType", "RESULT".into())];
                        if let Some(px) = price {
                            p.push(("price", dec(px)));
                            p.push(("timeInForce", "GTC".into()));
                        }
                        let r = client.signed(reqwest::Method::POST, "/api/v3/order", &p).await.and_then(|v| OrderState::from(&v));
                        let cancel_now = with_live(&acc, &ex, |l| match &r {
                            Ok(st) => { l.on_state(local, st); l.wants_cancel(local) }
                            Err(e) => { tracing::warn!("[{ex}] ордер {symbol} отклонён: {e}"); l.on_error(local, e); false }
                        }).unwrap_or(false);
                        if cancel_now {
                            cancel(&acc, &ex, &client, local).await;
                        }
                    }
                    Cmd::Cancel { local } => cancel(&acc, &ex, &client, local).await,
                }
            }
            _ = poll.tick() => {
                n += 1;
                let pending = with_live(&acc, &ex, |l| { l.trim(); l.pending() }).unwrap_or_default();
                for (local, symbol, id) in pending {
                    match client.signed(reqwest::Method::GET, "/api/v3/order", &[("symbol", symbol), ("orderId", id.to_string())]).await
                        .and_then(|v| OrderState::from(&v)) {
                        Ok(st) => { with_live(&acc, &ex, |l| l.on_state(local, &st)); }
                        Err(e) => { with_live(&acc, &ex, |l| l.error = e); }
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

/// Снять ордер на бирже; номера ещё нет — снимется, когда ордер встанет (см. Place).
async fn cancel(acc: &Shared, ex: &str, client: &Client, local: u64) {
    let Some((symbol, id)) = with_live(acc, ex, |l| l.cancel_target(local)).flatten() else {
        return;
    };
    let p = [("symbol", symbol.clone()), ("orderId", id.to_string())];
    let r = match client
        .signed(reqwest::Method::DELETE, "/api/v3/order", &p)
        .await
    {
        Ok(v) => OrderState::from(&v),
        // уже исполнен или снят — узнать итог
        Err(_) => client
            .signed(reqwest::Method::GET, "/api/v3/order", &p)
            .await
            .and_then(|v| OrderState::from(&v)),
    };
    match r {
        Ok(st) => {
            with_live(acc, ex, |l| l.on_state(local, &st));
        }
        Err(e) => tracing::warn!("[{ex}] не снят ордер {symbol} {id}: {e}"),
    }
}
