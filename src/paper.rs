//! Бумажный счёт биржи (спот): балансы, ордера по живому стакану, комиссии, учёт средней цены и результата,
//! риск-проверки как у Java-бота (торговля включена, размер, проскальзывание, частота, свежесть стакана, дневной убыток).

use crate::book::Book;
use crate::rest::Filter;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Известные котируемые валюты (как BalanceStore.QUOTE_ASSETS в Java), по порядку проверки.
const QUOTES: &[&str] = &[
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "BTC", "ETH", "BNB", "EUR", "TRY", "KRW", "USD",
];

/// BTCUSDT → (BTC, USDT); None — котируемая валюта неизвестна.
pub fn split(symbol: &str) -> Option<(String, String)> {
    let s = symbol.to_uppercase();
    QUOTES
        .iter()
        .find(|q| s.len() > q.len() && s.ends_with(*q))
        .map(|q| (s[..s.len() - q.len()].to_owned(), (*q).to_owned()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn parse(s: &str) -> Option<Side> {
        match s.to_uppercase().as_str() {
            "BUY" => Some(Side::Buy),
            "SELL" => Some(Side::Sell),
            _ => None,
        }
    }
}

/// Сколько купить или продать.
#[derive(Clone, Copy, Debug)]
pub enum Amount {
    Qty(f64),
    Portion(f64),
    All,
}

/// Лимиты риска (из параметров биржи).
#[derive(Clone, Debug)]
pub struct Limits {
    pub trading_enabled: bool,
    pub max_order_quote: f64,
    pub max_daily_loss_quote: f64,
    pub max_slippage_percent: f64,
    pub max_orders_per_minute: usize,
    pub max_data_age_ms: u64,
}

impl Limits {
    pub fn from_params(p: &Map<String, Value>) -> Limits {
        let f = |k: &str, d: f64| p.get(k).and_then(Value::as_f64).unwrap_or(d);
        Limits {
            trading_enabled: p
                .get("tradingEnabled")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            max_order_quote: f("maxPositionQuote", 100.0),
            max_daily_loss_quote: f("maxDailyLossQuote", 50.0),
            max_slippage_percent: f("maxSlippagePercent", 0.3),
            max_orders_per_minute: f("maxOrdersPerMinute", 30.0) as usize,
            max_data_age_ms: f("maxDataAgeMs", 5000.0) as u64,
        }
    }
}

/// Рассчитанный ордер.
#[derive(Clone, Debug)]
pub struct Prepared {
    pub base: String,
    pub quote: String,
    pub qty: f64,
    pub ref_px: f64,
    pub limit: Option<f64>,
    pub crosses: bool,
    pub filled: f64,
    pub avg: f64,
}

/// Итог ордера (как OrderResult в Java).
#[derive(Clone, Debug, PartialEq)]
pub struct Fill {
    pub order_id: u64,
    pub status: &'static str,
    pub executed: f64,
    pub avg_price: f64,
}

/// Лимитная заявка, стоящая в бумажном стакане.
#[derive(Clone, Debug)]
struct Resting {
    id: u64,
    symbol: String,
    side: Side,
    price: f64,
    qty: f64,
}

/// Купленная монета: количество и средняя цена с комиссией.
#[derive(Clone, Copy, Debug, Default)]
struct Lot {
    qty: f64,
    avg: f64,
}

/// Бумажный счёт одной биржи.
#[derive(Debug)]
pub struct Paper {
    maker: f64,
    taker: f64,
    free: BTreeMap<String, f64>,
    locked: BTreeMap<String, f64>,
    lots: HashMap<String, Lot>,
    orders: Vec<Resting>,
    next_id: u64,
    recent: VecDeque<Instant>,
    start: BTreeMap<String, f64>,
    started_at: u64,
    day: u64,
    pub daily_pnl: f64,
    pub realized: f64,
    pub maker_fees: f64,
    pub taker_fees: f64,
    pub closed: u64,
    pub wins: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub last_reject: String,
    pub kill_switch: bool,
    /// Исполнения по порядку (стратегии забирают свои по номеру заявки); хранятся последние EXECS_KEEP.
    pub execs: VecDeque<Exec>,
    exec_seq: u64,
    /// Правила торговли пар (шаг цены и объёма, минимумы); нет — ордер не округляется.
    filters: HashMap<String, Filter>,
}

/// Исполнение заявки.
#[derive(Clone, Debug, PartialEq)]
pub struct Exec {
    /// Порядковый номер исполнения на счёте (растёт).
    pub seq: u64,
    pub order_id: u64,
    pub symbol: String,
    pub side: Side,
    pub qty: f64,
    pub price: f64,
    /// Комиссия в котируемой валюте.
    pub fee: f64,
}

/// Сколько исполнений хранить.
const EXECS_KEEP: usize = 10_000;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn utc_day() -> u64 {
    now_ms() / 86_400_000
}

impl Paper {
    /// Новый счёт: каждой котируемой валюте символов — стартовый баланс; комиссии в процентах.
    pub fn new(symbols: &[String], start_balance: f64, maker_pct: f64, taker_pct: f64) -> Paper {
        let mut p = Paper {
            maker: maker_pct / 100.0,
            taker: taker_pct / 100.0,
            free: BTreeMap::new(),
            locked: BTreeMap::new(),
            lots: HashMap::new(),
            orders: Vec::new(),
            next_id: 1,
            recent: VecDeque::new(),
            start: BTreeMap::new(),
            started_at: 0,
            day: utc_day(),
            daily_pnl: 0.0,
            realized: 0.0,
            maker_fees: 0.0,
            taker_fees: 0.0,
            closed: 0,
            wins: 0,
            accepted: 0,
            rejected: 0,
            last_reject: String::new(),
            kill_switch: false,
            execs: VecDeque::new(),
            exec_seq: 0,
            filters: HashMap::new(),
        };
        for s in symbols {
            if let Some((_, q)) = split(s) {
                p.free.entry(q).or_insert(start_balance);
            }
        }
        p
    }

    /// Новой монете — стартовый баланс её котируемой валюты, если такой валюты на счёте ещё не было.
    pub fn ensure_quote(&mut self, symbol: &str, start_balance: f64) {
        if let Some((_, q)) = split(symbol)
            && !self.free.contains_key(&q)
            && !self.locked.contains_key(&q)
        {
            self.free.insert(q, start_balance);
        }
    }

    /// Задать правила торговли пар.
    pub fn set_filters(&mut self, f: &HashMap<String, Filter>) {
        for (k, v) in f {
            self.filters.insert(k.clone(), *v);
        }
    }

    /// Правила торговли пары.
    pub fn filter(&self, symbol: &str) -> Option<Filter> {
        self.filters.get(symbol).copied()
    }

    /// Свободный баланс актива.
    pub fn free(&self, asset: &str) -> f64 {
        self.free.get(asset).copied().unwrap_or(0.0)
    }

    /// Свободные балансы больше нуля.
    pub fn balances(&self) -> BTreeMap<String, f64> {
        self.free
            .iter()
            .filter(|(_, v)| **v > 1e-12)
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    pub fn open_orders(&self) -> usize {
        self.orders.len()
    }

    fn add(&mut self, asset: &str, d: f64) {
        *self.free.entry(asset.to_owned()).or_default() += d;
    }

    fn reject(&mut self, why: String) -> Result<Fill, String> {
        self.rejected += 1;
        self.last_reject = why.clone();
        Err(why)
    }

    /// Проверки до ордера: торговля, kill switch, частота, стакан.
    fn precheck(&mut self, lim: &Limits, book: Option<&Book>) -> Result<(), String> {
        if utc_day() != self.day {
            self.day = utc_day();
            self.daily_pnl = 0.0;
            self.kill_switch = false;
        }
        if !lim.trading_enabled {
            return Err("торговля на бирже выключена".into());
        }
        if self.kill_switch {
            return Err("kill switch: дневной лимит убытка исчерпан".into());
        }
        let cut = Instant::now() - Duration::from_secs(60);
        while self.recent.front().is_some_and(|t| *t < cut) {
            self.recent.pop_front();
        }
        if self.recent.len() >= lim.max_orders_per_minute {
            return Err(format!(
                "больше {} ордеров в минуту",
                lim.max_orders_per_minute
            ));
        }
        let b = book.filter(|b| b.is_ready()).ok_or("стакан не готов")?;
        let age = b.age_ms().unwrap_or(u64::MAX);
        if age > lim.max_data_age_ms {
            return Err(format!(
                "стакан устарел: {age} мс > {} мс",
                lim.max_data_age_ms
            ));
        }
        Ok(())
    }

    /// Проверить и рассчитать ордер: риск, объём (доля, «весь»), округление по правилам пары, минимумы, лимит размера,
    /// пересекает ли стакан и (для пересекающего) по какой средней цене исполнится, проскальзывание. Отказ учитывается.
    pub fn prepare(
        &mut self,
        symbol: &str,
        side: Side,
        amount: Amount,
        limit: Option<f64>,
        book: Option<&Book>,
        lim: &Limits,
    ) -> Result<Prepared, String> {
        self.prepare_inner(symbol, side, amount, limit, book, lim)
            .inspect_err(|e| {
                self.rejected += 1;
                self.last_reject = e.clone();
            })
    }

    fn prepare_inner(
        &mut self,
        symbol: &str,
        side: Side,
        amount: Amount,
        limit: Option<f64>,
        book: Option<&Book>,
        lim: &Limits,
    ) -> Result<Prepared, String> {
        self.precheck(lim, book)?;
        let (base, quote) = split(symbol).ok_or_else(|| format!("не разобрать символ {symbol}"))?;
        let b = book.expect("проверено в precheck");
        let (best_bid, best_ask) = (b.best_bid().unwrap().0, b.best_ask().unwrap().0);
        let filter = self.filters.get(&symbol.to_uppercase()).copied();
        let limit = limit.map(|p| filter.map_or(p, |f| f.round_price(p, side == Side::Sell)));
        let ref_px = limit.unwrap_or(if side == Side::Buy {
            best_ask
        } else {
            best_bid
        });
        if ref_px <= 0.0 {
            return Err("цена должна быть больше нуля".into());
        }
        let qty = match amount {
            Amount::Qty(q) => q,
            Amount::All | Amount::Portion(_) => {
                let share = if let Amount::Portion(x) = amount {
                    x.clamp(0.0, 1.0)
                } else {
                    1.0
                };
                match side {
                    Side::Buy => {
                        self.free(&quote).min(lim.max_order_quote) * share
                            / (ref_px * (1.0 + self.taker))
                    }
                    Side::Sell => self.free(&base) * share,
                }
            }
        };
        let qty = filter.map_or(qty, |f| f.round_qty(qty));
        if qty.is_nan() || qty <= 0.0 {
            return Err("объём должен быть больше нуля".into());
        }
        if let Some(f) = filter {
            if qty < f.min_qty - 1e-12 {
                return Err(format!(
                    "объём {qty} меньше минимального {} для {symbol}",
                    f.min_qty
                ));
            }
            if qty * ref_px < f.min_notional - 1e-9 {
                return Err(format!(
                    "сумма ордера {:.4} меньше минимальной {} для {symbol}",
                    qty * ref_px,
                    f.min_notional
                ));
            }
        }
        let notional = qty * ref_px;
        if notional > lim.max_order_quote + 1e-9 {
            return Err(format!(
                "ордер {notional:.2} {quote} больше лимита {} (maxPositionQuote)",
                lim.max_order_quote
            ));
        }
        let crosses = match (side, limit) {
            (_, None) => true,
            (Side::Buy, Some(p)) => p >= best_ask,
            (Side::Sell, Some(p)) => p <= best_bid,
        };
        let (filled, avg) = if crosses {
            walk(b, side, qty, limit)
        } else {
            (0.0, 0.0)
        };
        if crosses && limit.is_none() {
            if filled < qty * (1.0 - 1e-9) {
                return Err("в стакане не хватает объёма".into());
            }
            let best = if side == Side::Buy {
                best_ask
            } else {
                best_bid
            };
            let slip = (avg - best).abs() / best * 100.0;
            if slip > lim.max_slippage_percent {
                return Err(format!(
                    "проскальзывание {slip:.3}% больше {}%",
                    lim.max_slippage_percent
                ));
            }
        }
        Ok(Prepared {
            base,
            quote,
            qty,
            ref_px,
            limit,
            crosses,
            filled,
            avg,
        })
    }

    /// Ордер принят (для частоты и счётчика).
    pub fn note_accepted(&mut self) {
        self.recent.push_back(Instant::now());
        self.accepted += 1;
    }

    /// Учесть отказ, пришедший от биржи.
    pub fn note_rejected(&mut self, why: &str) {
        self.rejected += 1;
        self.last_reject = why.to_owned();
    }

    /// Рыночный или лимитный ордер. Лимитный, который пересекает стакан, исполняется сразу как тейкер.
    pub fn order(
        &mut self,
        symbol: &str,
        side: Side,
        amount: Amount,
        limit: Option<f64>,
        book: Option<&Book>,
        lim: &Limits,
    ) -> Result<Fill, String> {
        let pr = self.prepare(symbol, side, amount, limit, book, lim)?;
        let Prepared {
            base,
            quote,
            qty,
            ref_px,
            limit,
            crosses,
            filled,
            avg,
        } = pr;
        if !crosses {
            return self.rest(symbol, &base, &quote, side, qty, ref_px);
        }
        if let Err(e) = self.settle(&base, &quote, side, filled, avg, self.taker) {
            return self.reject(e);
        }
        self.note_accepted();
        self.check_loss(lim);
        let id = self.next_id;
        self.next_id += 1;
        self.exec(id, symbol, side, filled, avg, filled * avg * self.taker);
        let rest = qty - filled;
        if rest > 1e-12 && limit.is_some() {
            self.lock_for(&base, &quote, side, rest, ref_px)?;
            self.orders.push(Resting {
                id,
                symbol: symbol.to_uppercase(),
                side,
                price: ref_px,
                qty: rest,
            });
            return Ok(Fill {
                order_id: id,
                status: "PARTIALLY_FILLED",
                executed: filled,
                avg_price: avg,
            });
        }
        Ok(Fill {
            order_id: id,
            status: "FILLED",
            executed: filled,
            avg_price: avg,
        })
    }

    /// Учесть реальное исполнение (живой счёт): балансы, средняя цена, результат, журнал. Баланс биржи главный —
    /// нехватка в учёте не ошибка.
    #[allow(clippy::too_many_arguments)]
    pub fn ledger_fill(
        &mut self,
        order_id: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        px: f64,
        fee: f64,
        maker: bool,
        lim: &Limits,
    ) {
        let Some((base, quote)) = split(symbol) else {
            return;
        };
        let notional = qty * px;
        match side {
            Side::Buy => {
                self.add(&quote, -(notional + fee));
                self.add(&base, qty);
                let lot = self.lots.entry(base.clone()).or_default();
                lot.avg = (lot.avg * lot.qty + notional + fee) / (lot.qty + qty);
                lot.qty += qty;
            }
            Side::Sell => {
                self.add(&base, -qty);
                self.add(&quote, notional - fee);
                let lot = self.lots.entry(base.clone()).or_default();
                let sold = qty.min(lot.qty);
                let pnl = sold * px - fee * (sold / qty.max(1e-18)) - sold * lot.avg;
                lot.qty -= sold;
                self.realized += pnl;
                self.daily_pnl += pnl;
                self.closed += 1;
                if pnl > 0.0 {
                    self.wins += 1;
                }
            }
        }
        if maker {
            self.maker_fees += fee
        } else {
            self.taker_fees += fee
        }
        self.exec(order_id, symbol, side, qty, px, fee);
        self.check_loss(lim);
    }

    /// Баланс одного актива с биржи (событие счёта).
    pub fn set_balance(&mut self, asset: &str, free: f64) {
        self.free.insert(asset.to_owned(), free);
    }

    /// Балансы с биржи (живой счёт): заменяют учётные.
    pub fn set_balances(&mut self, b: BTreeMap<String, f64>) {
        self.free = b;
        self.locked.clear();
    }

    /// Комиссии мейкера и тейкера, доли.
    pub fn fees(&self) -> (f64, f64) {
        (self.maker, self.taker)
    }

    fn lock_for(
        &mut self,
        base: &str,
        quote: &str,
        side: Side,
        qty: f64,
        px: f64,
    ) -> Result<(), String> {
        let (asset, need) = match side {
            Side::Buy => (quote, qty * px * (1.0 + self.maker)),
            Side::Sell => (base, qty),
        };
        if self.free(asset) + 1e-12 < need {
            return Err(format!(
                "недостаточно {asset}: нужно {need:.8}, свободно {:.8}",
                self.free(asset)
            ));
        }
        self.add(asset, -need);
        *self.locked.entry(asset.to_owned()).or_default() += need;
        Ok(())
    }

    fn rest(
        &mut self,
        symbol: &str,
        base: &str,
        quote: &str,
        side: Side,
        qty: f64,
        px: f64,
    ) -> Result<Fill, String> {
        if let Err(e) = self.lock_for(base, quote, side, qty, px) {
            return self.reject(e);
        }
        self.recent.push_back(Instant::now());
        self.accepted += 1;
        let id = self.next_id;
        self.next_id += 1;
        self.orders.push(Resting {
            id,
            symbol: symbol.to_uppercase(),
            side,
            price: px,
            qty,
        });
        Ok(Fill {
            order_id: id,
            status: "NEW",
            executed: 0.0,
            avg_price: 0.0,
        })
    }

    /// Применить исполнение к балансам и учёту: покупка — средняя цена с комиссией, продажа — результат.
    fn settle(
        &mut self,
        base: &str,
        quote: &str,
        side: Side,
        qty: f64,
        px: f64,
        fee_rate: f64,
    ) -> Result<(), String> {
        let notional = qty * px;
        let fee = notional * fee_rate;
        match side {
            Side::Buy => {
                if self.free(quote) + 1e-9 < notional + fee {
                    return Err(format!(
                        "недостаточно {quote}: нужно {:.8}, свободно {:.8}",
                        notional + fee,
                        self.free(quote)
                    ));
                }
                self.add(quote, -(notional + fee));
                self.add(base, qty);
                let lot = self.lots.entry(base.to_owned()).or_default();
                lot.avg = (lot.avg * lot.qty + notional + fee) / (lot.qty + qty);
                lot.qty += qty;
            }
            Side::Sell => {
                if self.free(base) + 1e-12 < qty {
                    return Err(format!(
                        "недостаточно {base}: нужно {qty:.8}, свободно {:.8}",
                        self.free(base)
                    ));
                }
                self.add(base, -qty);
                self.add(quote, notional - fee);
                let lot = self.lots.entry(base.to_owned()).or_default();
                let sold = qty.min(lot.qty); // монета, купленная до учёта, считается по цене продажи
                let pnl = sold * px - fee - sold * lot.avg;
                lot.qty -= sold;
                self.realized += pnl;
                self.daily_pnl += pnl;
                self.closed += 1;
                if pnl > 0.0 {
                    self.wins += 1;
                }
            }
        }
        if fee_rate == self.maker {
            self.maker_fees += fee
        } else {
            self.taker_fees += fee
        }
        Ok(())
    }

    fn check_loss(&mut self, lim: &Limits) {
        if lim.max_daily_loss_quote > 0.0
            && self.daily_pnl <= -lim.max_daily_loss_quote
            && !self.kill_switch
        {
            self.kill_switch = true;
            tracing::warn!(
                "Kill switch: дневной убыток {:.2} ≥ {}",
                -self.daily_pnl,
                lim.max_daily_loss_quote
            );
        }
    }

    /// Исполнить стоящие заявки, через которые прошла цена (как мейкер, по цене заявки).
    pub fn match_resting(&mut self, symbol: &str, book: &Book, lim: &Limits) {
        let (Some((bid, _)), Some((ask, _))) = (book.best_bid(), book.best_ask()) else {
            return;
        };
        let Some((base, quote)) = split(symbol) else {
            return;
        };
        let mut i = 0;
        while i < self.orders.len() {
            let o = &self.orders[i];
            let hit = o.symbol == symbol
                && match o.side {
                    Side::Buy => ask <= o.price,
                    Side::Sell => bid >= o.price,
                };
            if !hit {
                i += 1;
                continue;
            }
            let o = self.orders.remove(i);
            self.unlock(&base, &quote, &o);
            match self.settle(&base, &quote, o.side, o.qty, o.price, self.maker) {
                Ok(()) => self.exec(
                    o.id,
                    &o.symbol,
                    o.side,
                    o.qty,
                    o.price,
                    o.qty * o.price * self.maker,
                ),
                Err(e) => tracing::warn!("бумажная заявка {} не исполнена: {e}", o.id),
            }
        }
        self.check_loss(lim);
    }

    fn unlock(&mut self, base: &str, quote: &str, o: &Resting) {
        let (asset, amt) = match o.side {
            Side::Buy => (quote, o.qty * o.price * (1.0 + self.maker)),
            Side::Sell => (base, o.qty),
        };
        *self.locked.entry(asset.to_owned()).or_default() -= amt;
        self.add(asset, amt);
    }

    fn exec(&mut self, order_id: u64, symbol: &str, side: Side, qty: f64, price: f64, fee: f64) {
        if qty <= 0.0 {
            return;
        }
        if self.execs.len() >= EXECS_KEEP {
            self.execs.pop_front();
        }
        self.exec_seq += 1;
        let seq = self.exec_seq;
        self.execs.push_back(Exec {
            seq,
            order_id,
            symbol: symbol.to_uppercase(),
            side,
            qty,
            price,
            fee,
        });
    }

    /// Выход рыночной продажей (стоп, таймаут, выключение стратегии): без проверок «торговля включена»,
    /// kill switch и частоты — выходы разрешены всегда; нужен только живой стакан.
    pub fn sell_exit(
        &mut self,
        symbol: &str,
        qty: f64,
        book: Option<&Book>,
    ) -> Result<Fill, String> {
        let b = book.filter(|b| b.is_ready()).ok_or("стакан не готов")?;
        let (base, quote) = split(symbol).ok_or_else(|| format!("не разобрать символ {symbol}"))?;
        let qty = qty.min(self.free(&base));
        let qty = self
            .filters
            .get(&symbol.to_uppercase())
            .map_or(qty, |f| f.round_qty(qty));
        if qty <= 0.0 {
            return Err(format!("нет {base} для продажи"));
        }
        let (filled, avg) = walk(b, Side::Sell, qty, None);
        self.settle(&base, &quote, Side::Sell, filled, avg, self.taker)?;
        let id = self.next_id;
        self.next_id += 1;
        self.exec(
            id,
            symbol,
            Side::Sell,
            filled,
            avg,
            filled * avg * self.taker,
        );
        Ok(Fill {
            order_id: id,
            status: "FILLED",
            executed: filled,
            avg_price: avg,
        })
    }

    /// Снять одну заявку; false — её уже нет (исполнилась или снята).
    pub fn cancel(&mut self, id: u64) -> bool {
        let Some(i) = self.orders.iter().position(|o| o.id == id) else {
            return false;
        };
        let o = self.orders.remove(i);
        if let Some((b, q)) = split(&o.symbol) {
            self.unlock(&b, &q, &o);
        }
        true
    }

    /// Стоит ли заявка.
    pub fn is_open(&self, id: u64) -> bool {
        self.orders.iter().any(|o| o.id == id)
    }

    /// Снять заявки символа (None — все); сколько снято.
    pub fn cancel_all(&mut self, symbol: Option<&str>) -> usize {
        let (keep, gone): (Vec<_>, Vec<_>) = std::mem::take(&mut self.orders)
            .into_iter()
            .partition(|o| symbol.is_some_and(|s| o.symbol != s.to_uppercase()));
        self.orders = keep;
        for o in &gone {
            if let Some((b, q)) = split(&o.symbol) {
                self.unlock(&b, &q, o);
            }
        }
        gone.len()
    }

    /// Стоимость по котируемым валютам: валюта + монеты по середине цены; None — цены есть не у всех символов.
    fn equity(&self, mids: &HashMap<String, f64>) -> Option<BTreeMap<String, f64>> {
        let mut out = BTreeMap::new();
        let quotes: Vec<String> = self
            .start
            .keys()
            .cloned()
            .chain(mids.keys().filter_map(|s| split(s).map(|x| x.1)))
            .collect();
        for q in quotes {
            out.insert(
                q.clone(),
                self.free(&q) + self.locked.get(&q).copied().unwrap_or(0.0),
            );
        }
        for (sym, mid) in mids {
            let (b, q) = split(sym)?;
            if out.contains_key(&b) {
                continue;
            }
            let held = self.free(&b) + self.locked.get(&b).copied().unwrap_or(0.0);
            *out.entry(q).or_default() += held * mid;
        }
        Some(out)
    }

    /// Тестовый счёт для админки (как testAccount в Java); старт фиксируется при первом расчёте с ценами.
    pub fn account_json(&mut self, mids: &HashMap<String, f64>) -> Value {
        let now = self.equity(mids);
        if self.start.is_empty()
            && let Some(n) = &now
        {
            self.start = n.clone();
            self.started_at = now_ms();
        }
        let mut by = Map::new();
        if let Some(n) = &now {
            for (q, st) in &self.start {
                let eq = n.get(q).copied().unwrap_or(0.0);
                by.insert(q.clone(), json!({"start": st, "equity": eq, "pnl": eq - st, "pnlPercent": if *st > 0.0 { (eq - st) / st * 100.0 } else { 0.0 }}));
            }
        }
        json!({
            "mode": "PAPER", "startedAt": self.started_at, "byQuote": by, "realizedPnl": self.realized, "dailyPnl": self.daily_pnl,
            "fees": self.maker_fees + self.taker_fees, "takerFees": self.taker_fees, "makerFees": self.maker_fees,
            "closedTrades": self.closed, "winningTrades": self.wins,
            "winRatePercent": if self.closed > 0 { self.wins as f64 * 100.0 / self.closed as f64 } else { 0.0 },
            "openOrders": self.open_orders(), "openPositions": self.lots.values().filter(|l| l.qty > 1e-12).count(),
        })
    }
}

/// Пройти по стакану: сколько исполнится (до цены limit, если задана) и по какой средней цене.
fn walk(b: &Book, side: Side, qty: f64, limit: Option<f64>) -> (f64, f64) {
    let (mut left, mut cost) = (qty, 0.0);
    for (px, q) in b.levels(side == Side::Buy) {
        if let Some(l) = limit
            && ((side == Side::Buy && px > l) || (side == Side::Sell && px < l))
        {
            break;
        }
        let take = left.min(q);
        cost += take * px;
        left -= take;
        if left <= 1e-12 {
            break;
        }
    }
    let filled = qty - left.max(0.0);
    (filled, if filled > 0.0 { cost / filled } else { 0.0 })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book() -> Book {
        let mut b = Book::default();
        b.apply_snapshot(
            vec![(100.0, 1.0), (99.0, 5.0)],
            vec![(101.0, 1.0), (102.0, 5.0)],
            1,
        );
        b
    }

    fn lim() -> Limits {
        Limits {
            trading_enabled: true,
            max_order_quote: 1000.0,
            max_daily_loss_quote: 50.0,
            max_slippage_percent: 5.0,
            max_orders_per_minute: 100,
            max_data_age_ms: 5000,
        }
    }

    #[test]
    fn split_symbols() {
        assert_eq!(split("btcusdt"), Some(("BTC".into(), "USDT".into())));
        assert_eq!(split("ETHBTC"), Some(("ETH".into(), "BTC".into())));
        assert_eq!(split("USDT"), None);
    }

    #[test]
    fn market_buy_walks_book_and_sell_realizes() {
        let mut p = Paper::new(&["BTCUSDT".into()], 1000.0, 0.1, 0.1);
        let b = book();
        let f = p
            .order(
                "BTCUSDT",
                Side::Buy,
                Amount::Qty(2.0),
                None,
                Some(&b),
                &lim(),
            )
            .unwrap();
        assert_eq!(f.status, "FILLED");
        assert!((f.avg_price - 101.5).abs() < 1e-9); // 1 по 101 и 1 по 102
        assert!((p.free("USDT") - (1000.0 - 203.0 * 1.001)).abs() < 1e-9);
        assert!((p.free("BTC") - 2.0).abs() < 1e-12);
        let s = p
            .order("BTCUSDT", Side::Sell, Amount::All, None, Some(&b), &lim())
            .unwrap();
        assert!((s.avg_price - 99.5).abs() < 1e-9);
        assert!(p.realized < 0.0 && p.closed == 1 && p.wins == 0);
        assert!((p.realized - (199.0 * 0.999 - 203.0 * 1.001)).abs() < 1e-9);
    }

    #[test]
    fn risk_rejections() {
        let mut p = Paper::new(&["BTCUSDT".into()], 1000.0, 0.1, 0.1);
        let b = book();
        let mut l = lim();
        l.trading_enabled = false;
        assert!(
            p.order("BTCUSDT", Side::Buy, Amount::Qty(1.0), None, Some(&b), &l)
                .unwrap_err()
                .contains("выключена")
        );
        l = lim();
        l.max_order_quote = 50.0;
        assert!(
            p.order("BTCUSDT", Side::Buy, Amount::Qty(1.0), None, Some(&b), &l)
                .unwrap_err()
                .contains("больше лимита")
        );
        l = lim();
        l.max_slippage_percent = 0.1;
        assert!(
            p.order("BTCUSDT", Side::Buy, Amount::Qty(3.0), None, Some(&b), &l)
                .unwrap_err()
                .contains("проскальзывание")
        );
        assert!(
            p.order(
                "BTCUSDT",
                Side::Sell,
                Amount::Qty(1.0),
                None,
                Some(&b),
                &lim()
            )
            .unwrap_err()
            .contains("недостаточно BTC")
        );
        assert!(
            p.order("BTCUSDT", Side::Buy, Amount::Qty(1.0), None, None, &lim())
                .unwrap_err()
                .contains("стакан")
        );
        l = lim();
        l.max_orders_per_minute = 1;
        p.order("BTCUSDT", Side::Buy, Amount::Qty(0.1), None, Some(&b), &l)
            .unwrap();
        assert!(
            p.order("BTCUSDT", Side::Buy, Amount::Qty(0.1), None, Some(&b), &l)
                .unwrap_err()
                .contains("в минуту")
        );
        assert_eq!(p.rejected, 6);
    }

    #[test]
    fn limit_rests_locks_and_fills_as_maker() {
        let mut p = Paper::new(&["BTCUSDT".into()], 1000.0, 0.1, 0.2);
        let mut b = book();
        let f = p
            .order(
                "BTCUSDT",
                Side::Buy,
                Amount::Qty(1.0),
                Some(100.5),
                Some(&b),
                &lim(),
            )
            .unwrap();
        assert_eq!(f.status, "NEW");
        assert!((p.free("USDT") - (1000.0 - 100.5 * 1.001)).abs() < 1e-9);
        b.apply_best(100.0, 1.0, 100.4, 1.0, 2); // аск опустился ниже заявки
        p.match_resting("BTCUSDT", &b, &lim());
        assert_eq!(p.open_orders(), 0);
        assert!((p.free("BTC") - 1.0).abs() < 1e-12);
        assert!((p.maker_fees - 100.5 * 0.001).abs() < 1e-12);
        // снять стоящую заявку — деньги возвращаются
        p.order(
            "BTCUSDT",
            Side::Buy,
            Amount::Qty(1.0),
            Some(90.0),
            Some(&b),
            &lim(),
        )
        .unwrap();
        let before = p.free("USDT");
        assert_eq!(p.cancel_all(Some("BTCUSDT")), 1);
        assert!((p.free("USDT") - before - 90.0 * 1.001).abs() < 1e-9);
    }

    #[test]
    fn kill_switch_after_daily_loss() {
        let mut p = Paper::new(&["BTCUSDT".into()], 1000.0, 0.1, 0.1);
        let b = book();
        let mut l = lim();
        l.max_daily_loss_quote = 1.0;
        p.order("BTCUSDT", Side::Buy, Amount::Qty(1.0), None, Some(&b), &l)
            .unwrap();
        p.order("BTCUSDT", Side::Sell, Amount::Qty(1.0), None, Some(&b), &l)
            .unwrap(); // −1 по спреду и комиссиям
        assert!(p.kill_switch);
        assert!(
            p.order("BTCUSDT", Side::Buy, Amount::Qty(0.1), None, Some(&b), &l)
                .unwrap_err()
                .contains("kill switch")
        );
    }

    #[test]
    fn account_equity() {
        let mut p = Paper::new(&["BTCUSDT".into()], 1000.0, 0.0, 0.0);
        let b = book();
        let mids = HashMap::from([("BTCUSDT".to_owned(), 100.5)]);
        let a = p.account_json(&mids);
        assert_eq!(a["byQuote"]["USDT"]["start"], json!(1000.0));
        p.order(
            "BTCUSDT",
            Side::Buy,
            Amount::Qty(1.0),
            None,
            Some(&b),
            &lim(),
        )
        .unwrap();
        let a = p.account_json(&mids);
        assert!((a["byQuote"]["USDT"]["pnl"].as_f64().unwrap() - (-0.5)).abs() < 1e-9); // купили по 101, стоит 100.5
        assert_eq!(a["openPositions"], json!(1));
    }

    #[test]
    fn filters_round_and_minimums() {
        let mut p = Paper::new(&["BTCUSDT".into()], 1000.0, 0.1, 0.1);
        p.set_filters(&HashMap::from([(
            "BTCUSDT".to_owned(),
            Filter {
                tick: 0.5,
                step: 0.01,
                min_qty: 0.02,
                min_notional: 5.0,
            },
        )]));
        let b = book();
        let f = p
            .order(
                "BTCUSDT",
                Side::Buy,
                Amount::Qty(0.0667),
                Some(90.3),
                Some(&b),
                &lim(),
            )
            .unwrap();
        assert_eq!(f.status, "NEW");
        assert!(
            (p.free("USDT") - (1000.0 - 0.06 * 90.0 * 1.001)).abs() < 1e-9,
            "объём 0.06, цена 90.0"
        );
        assert!(
            p.order(
                "BTCUSDT",
                Side::Buy,
                Amount::Qty(0.015),
                None,
                Some(&b),
                &lim()
            )
            .unwrap_err()
            .contains("меньше минимального")
        );
        assert!(
            p.order(
                "BTCUSDT",
                Side::Buy,
                Amount::Qty(0.05),
                Some(90.0),
                Some(&b),
                &lim()
            )
            .unwrap_err()
            .contains("сумма ордера")
        );
    }
}
