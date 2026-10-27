//! Бумажный счёт биржи (спот): балансы, ордера по живому стакану, комиссии, учёт средней цены и результата,
//! риск-проверки как у Java-бота (торговля включена, размер, проскальзывание, частота, свежесть стакана, дневной убыток).

use crate::exchange::rest::Filter;
use crate::market::book::Book;
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
    /// Занято под заявку (спот — валюта или монета по стороне, перп — маржа в котируемой валюте).
    lock: f64,
}

/// Позиция перпа: количество со знаком (лонг > 0, шорт < 0), цена входа, маржа (изолированная).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Pos {
    pub qty: f64,
    pub entry: f64,
    pub margin: f64,
}

/// Поддерживающая маржа перпа: позиция ликвидируется, когда маржа + нереализованный результат ниже этой доли объёма.
const MAINT: f64 = 0.005;

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
    /// Перпы: плечо (None — спот).
    leverage: Option<f64>,
    /// Позиции перпов по символам.
    pub positions: HashMap<String, Pos>,
    /// Funding: уплачено (минус — получено).
    pub funding_paid: f64,
    pub liquidations: u64,
    /// Защитные стопы перпов по символам: (сторона закрытия, цена срабатывания) — у бумаги срабатывают в учёте,
    /// у живого счёта стоят на бирже.
    pub stops: HashMap<String, (Side, f64)>,
    pub stops_triggered: u64,
    /// Биржа счёта — для журнала отладки.
    pub venue: String,
    /// Почему новые входы сейчас запрещены (стакан по REST); пусто — разрешены. Выходы проходят всегда.
    pub entries_blocked: String,
    /// Входов отклонено, пока стакан шёл по REST.
    pub rest_entries_blocked: u64,
    /// Выходов, прошедших, пока стакан шёл по REST.
    pub rest_exits: u64,
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
            leverage: None,
            positions: HashMap::new(),
            funding_paid: 0.0,
            liquidations: 0,
            stops: HashMap::new(),
            stops_triggered: 0,
            venue: String::new(),
            entries_blocked: String::new(),
            rest_entries_blocked: 0,
            rest_exits: 0,
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

    /// Счёт перпов с плечом (лонг и шорт, маржа в котируемой валюте, funding).
    pub fn set_perp(&mut self, leverage: f64) {
        self.leverage = Some(leverage.max(1.0));
    }

    /// Плечо перпов; None — спот.
    pub fn leverage(&self) -> Option<f64> {
        self.leverage
    }

    /// Позиция символа (перпы).
    pub fn position(&self, symbol: &str) -> Pos {
        self.positions
            .get(&symbol.to_uppercase())
            .copied()
            .unwrap_or_default()
    }

    /// Позиция с биржи (живой счёт): заменяет учётную, маржа — по плечу.
    pub fn set_position(&mut self, symbol: &str, qty: f64, entry: f64) {
        let lev = self.leverage.unwrap_or(1.0);
        let k = symbol.to_uppercase();
        if qty.abs() < 1e-12 {
            self.positions.remove(&k);
        } else {
            self.positions.insert(
                k,
                Pos {
                    qty,
                    entry,
                    margin: qty.abs() * entry / lev,
                },
            );
        }
    }

    /// Какая часть ордера открывает позицию (остальное закрывает встречную).
    fn opening(&self, symbol: &str, side: Side, qty: f64) -> f64 {
        let q = self.position(symbol).qty;
        let against = (side == Side::Buy && q < 0.0) || (side == Side::Sell && q > 0.0);
        if against {
            (qty - q.abs()).max(0.0)
        } else {
            qty
        }
    }

    /// Что нужно иметь свободным под ордер: спот — валюта (покупка) или монета (продажа); перп — маржа открываемой
    /// части и комиссия в котируемой валюте.
    #[allow(clippy::too_many_arguments)]
    pub fn need(
        &self,
        symbol: &str,
        base: &str,
        quote: &str,
        side: Side,
        qty: f64,
        px: f64,
        fee: f64,
    ) -> (String, f64) {
        match self.leverage {
            Some(lev) => (
                quote.to_owned(),
                self.opening(symbol, side, qty) * px / lev + qty * px * fee,
            ),
            None => match side {
                Side::Buy => (quote.to_owned(), qty * px * (1.0 + fee)),
                Side::Sell => (base.to_owned(), qty),
            },
        }
    }

    /// Исполнение перпа: сначала закрывается встречная позиция (результат и маржа — на счёт), остаток открывает или
    /// наращивает позицию (маржа со счёта). check — проверять свободную маржу (живой счёт не проверяет: биржа главная).
    #[allow(clippy::too_many_arguments)]
    fn perp_settle(
        &mut self,
        symbol: &str,
        quote: &str,
        side: Side,
        qty: f64,
        px: f64,
        fee: f64,
        maker: bool,
        check: bool,
    ) -> Result<(), String> {
        let lev = self.leverage.unwrap_or(1.0);
        let key = symbol.to_uppercase();
        if check {
            let need = self.opening(&key, side, qty) * px / lev + fee;
            if self.free(quote) + 1e-9 < need {
                return Err(format!(
                    "недостаточно маржи {quote}: нужно {need:.8}, свободно {:.8}",
                    self.free(quote)
                ));
            }
        }
        let dir = if side == Side::Buy { 1.0 } else { -1.0 };
        let mut p = self.positions.get(&key).copied().unwrap_or_default();
        let (mut rest, mut gain, mut closed) = (qty, 0.0, false);
        if p.qty * dir < 0.0 {
            let c = qty.min(p.qty.abs());
            let release = p.margin * c / p.qty.abs();
            gain = c * (px - p.entry) * p.qty.signum();
            self.add(quote, release + gain);
            p.margin -= release;
            p.qty -= c * p.qty.signum();
            rest -= c;
            closed = true;
            if p.qty.abs() < 1e-12 {
                p = Pos::default();
            }
        }
        if rest > 1e-12 {
            let m = rest * px / lev;
            self.add(quote, -m);
            p.entry = (p.qty.abs() * p.entry + rest * px) / (p.qty.abs() + rest);
            p.qty += rest * dir;
            p.margin += m;
        }
        self.add(quote, -fee);
        if p.qty.abs() < 1e-12 {
            self.positions.remove(&key);
        } else {
            self.positions.insert(key, p);
        }
        let pnl = gain - fee;
        self.realized += pnl;
        self.daily_pnl += pnl;
        if closed {
            self.closed += 1;
            if pnl > 0.0 {
                self.wins += 1;
            }
        }
        if maker {
            self.maker_fees += fee
        } else {
            self.taker_fees += fee
        }
        Ok(())
    }

    /// Funding по позиции символа: лонг платит при ставке > 0, шорт получает (и наоборот).
    pub fn apply_funding(&mut self, symbol: &str, rate: f64, mark: f64) -> f64 {
        let Some(p) = self.positions.get(&symbol.to_uppercase()).copied() else {
            return 0.0;
        };
        let Some((_, quote)) = split(symbol) else {
            return 0.0;
        };
        let pay = p.qty * mark * rate;
        self.add(&quote, -pay);
        self.funding_paid += pay;
        self.realized -= pay;
        self.daily_pnl -= pay;
        pay
    }

    /// Ликвидация (бумага): маржа + нереализованный результат ниже поддерживающей — позиция закрывается,
    /// маржа теряется (изолированная маржа, убыток не больше её).
    fn check_liquidation(&mut self, symbol: &str, mark: f64) {
        let key = symbol.to_uppercase();
        let Some(p) = self.positions.get(&key).copied() else {
            return;
        };
        let upnl = p.qty * (mark - p.entry);
        if p.margin + upnl > MAINT * p.qty.abs() * mark {
            return;
        }
        self.positions.remove(&key);
        self.realized -= p.margin;
        self.daily_pnl -= p.margin;
        self.closed += 1;
        self.liquidations += 1;
        let id = self.next_id;
        self.next_id += 1;
        let side = if p.qty > 0.0 { Side::Sell } else { Side::Buy };
        self.exec(id, &key, side, p.qty.abs(), mark, 0.0);
        tracing::warn!(
            "ликвидация {key}: позиция {} по {mark}, потеряна маржа {:.4}",
            p.qty,
            p.margin
        );
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
                let q = self.position(symbol).qty;
                match (self.leverage, side) {
                    // перп: встречная позиция — закрыть её (долю), иначе — на свободную маржу с плечом
                    (Some(_), Side::Buy) if q < 0.0 => -q * share,
                    (Some(_), Side::Sell) if q > 0.0 => q * share,
                    (Some(lev), _) => {
                        (self.free(&quote) * lev).min(lim.max_order_quote) * share
                            / (ref_px * (1.0 + self.taker * lev))
                    }
                    (None, Side::Buy) => {
                        self.free(&quote).min(lim.max_order_quote) * share
                            / (ref_px * (1.0 + self.taker))
                    }
                    (None, Side::Sell) => self.free(&base) * share,
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
        if let Err(e) = self.fill(symbol, &base, &quote, side, filled, avg, false, true) {
            return self.reject(e);
        }
        self.note_accepted();
        self.check_loss(lim);
        let id = self.next_id;
        self.next_id += 1;
        self.exec(id, symbol, side, filled, avg, filled * avg * self.taker);
        let rest = qty - filled;
        if rest > 1e-12 && limit.is_some() {
            let lock = self.lock_for(symbol, &base, &quote, side, rest, ref_px)?;
            self.orders.push(Resting {
                id,
                symbol: symbol.to_uppercase(),
                side,
                price: ref_px,
                qty: rest,
                lock,
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
        if self.leverage.is_some() {
            let _ = self.perp_settle(symbol, &quote, side, qty, px, fee, maker, false);
            self.exec(order_id, symbol, side, qty, px, fee);
            self.check_loss(lim);
            return;
        }
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

    /// Перевод с бумажного счёта: монета уходит со свободного баланса вместе с долей учёта средней цены.
    /// Возвращает среднюю цену покупки уходящей монеты (0 — неизвестна).
    pub fn withdraw(&mut self, asset: &str, qty: f64) -> Result<f64, String> {
        if self.free(asset) + 1e-12 < qty {
            return Err(format!(
                "недостаточно {asset} для перевода: нужно {qty}, свободно {}",
                self.free(asset)
            ));
        }
        self.add(asset, -qty);
        let lot = self.lots.entry(asset.to_owned()).or_default();
        let avg = lot.avg;
        lot.qty = (lot.qty - qty).max(0.0);
        Ok(avg)
    }

    /// Перевод пришёл: монета на свободный баланс, учёт средней цены — с биржи-источника.
    pub fn deposit(&mut self, asset: &str, qty: f64, avg: f64) {
        self.add(asset, qty);
        let lot = self.lots.entry(asset.to_owned()).or_default();
        lot.avg = (lot.avg * lot.qty + avg * qty) / (lot.qty + qty).max(1e-18);
        lot.qty += qty;
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

    /// Занять под заявку; сколько занято.
    fn lock_for(
        &mut self,
        symbol: &str,
        base: &str,
        quote: &str,
        side: Side,
        qty: f64,
        px: f64,
    ) -> Result<f64, String> {
        let (asset, need) = self.need(symbol, base, quote, side, qty, px, self.maker);
        if self.free(&asset) + 1e-12 < need {
            return Err(format!(
                "недостаточно {asset}: нужно {need:.8}, свободно {:.8}",
                self.free(&asset)
            ));
        }
        self.add(&asset, -need);
        *self.locked.entry(asset).or_default() += need;
        Ok(need)
    }

    /// Исполнение: спот — балансы и средняя цена, перп — позиция и маржа.
    #[allow(clippy::too_many_arguments)]
    fn fill(
        &mut self,
        symbol: &str,
        base: &str,
        quote: &str,
        side: Side,
        qty: f64,
        px: f64,
        maker: bool,
        check: bool,
    ) -> Result<(), String> {
        let rate = if maker { self.maker } else { self.taker };
        if self.leverage.is_some() {
            self.perp_settle(symbol, quote, side, qty, px, qty * px * rate, maker, check)
        } else {
            self.settle(base, quote, side, qty, px, rate)
        }
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
        let lock = match self.lock_for(symbol, base, quote, side, qty, px) {
            Ok(l) => l,
            Err(e) => return self.reject(e),
        };
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
            lock,
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
            match self.fill(&o.symbol, &base, &quote, o.side, o.qty, o.price, true, true) {
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
        if self.leverage.is_some() {
            self.check_liquidation(symbol, (bid + ask) / 2.0);
        }
        self.check_loss(lim);
    }

    fn unlock(&mut self, base: &str, quote: &str, o: &Resting) {
        let asset = if self.leverage.is_none() && o.side == Side::Sell {
            base
        } else {
            quote
        };
        *self.locked.entry(asset.to_owned()).or_default() -= o.lock;
        self.add(asset, o.lock);
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
        self.exit_market(symbol, Side::Sell, qty, book)
    }

    /// Сколько можно закрыть выходом: спот — продать имеющееся; перп — уменьшить позицию (не перевернуть).
    pub fn exit_room(&self, symbol: &str, side: Side) -> f64 {
        match (self.leverage, side) {
            (Some(_), Side::Sell) => self.position(symbol).qty.max(0.0),
            (Some(_), Side::Buy) => (-self.position(symbol).qty).max(0.0),
            (None, Side::Sell) => split(symbol).map_or(0.0, |(b, _)| self.free(&b)),
            (None, Side::Buy) => 0.0,
        }
    }

    /// Рыночный выход без проверок торговли: спот — продажа, перп — уменьшение позиции в любую сторону.
    pub fn exit_market(
        &mut self,
        symbol: &str,
        side: Side,
        qty: f64,
        book: Option<&Book>,
    ) -> Result<Fill, String> {
        let b = book.filter(|b| b.is_ready()).ok_or("стакан не готов")?;
        let (base, quote) = split(symbol).ok_or_else(|| format!("не разобрать символ {symbol}"))?;
        let qty = qty.min(self.exit_room(symbol, side));
        let qty = self
            .filters
            .get(&symbol.to_uppercase())
            .map_or(qty, |f| f.round_qty(qty));
        if qty <= 0.0 {
            return Err(format!("нечего закрывать по {symbol}"));
        }
        let (filled, avg) = walk(b, side, qty, None);
        self.fill(symbol, &base, &quote, side, filled, avg, false, false)?;
        let id = self.next_id;
        self.next_id += 1;
        self.exec(id, symbol, side, filled, avg, filled * avg * self.taker);
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
        for (sym, p) in &self.positions {
            let (_, q) = split(sym)?;
            let mark = mids.get(sym).copied()?;
            *out.entry(q).or_default() += p.margin + p.qty * (mark - p.entry);
        }
        for (sym, mid) in mids {
            if self.leverage.is_some() {
                break;
            }
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
            "openOrders": self.open_orders(),
            "openPositions": if self.leverage.is_some() { self.positions.len() } else { self.lots.values().filter(|l| l.qty > 1e-12).count() },
            "market": if self.leverage.is_some() { "perp" } else { "spot" }, "leverage": self.leverage,
            "fundingPaid": self.funding_paid, "liquidations": self.liquidations, "stopsTriggered": self.stops_triggered,
            "positions": self.positions.iter().map(|(s, p)| {
                let mark = mids.get(s).copied();
                json!({"symbol": s, "qty": p.qty, "entry": p.entry, "margin": p.margin, "mark": mark, "stop": self.stops.get(s).map(|x| x.1),
                    "unrealizedPnl": mark.map(|m| p.qty * (m - p.entry))})
            }).collect::<Vec<_>>(),
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

    fn book_at(bid: f64, ask: f64) -> Book {
        let mut b = Book::default();
        b.apply_snapshot(vec![(bid, 100.0)], vec![(ask, 100.0)], 1);
        b
    }

    #[test]
    fn perp_long_short_flip_and_margin() {
        let mut p = Paper::new(&["BTCUSDT".into()], 1000.0, 0.0, 0.0);
        p.set_perp(10.0);
        let l = Limits {
            max_order_quote: 100_000.0,
            ..lim()
        };
        // шорт 5 по 100 без монет: маржа 50
        p.order(
            "BTCUSDT",
            Side::Sell,
            Amount::Qty(5.0),
            None,
            Some(&book_at(100.0, 101.0)),
            &l,
        )
        .unwrap();
        assert_eq!(
            p.position("BTCUSDT"),
            Pos {
                qty: -5.0,
                entry: 100.0,
                margin: 50.0
            }
        );
        assert!((p.free("USDT") - 950.0).abs() < 1e-9);
        // цена упала: откуп 8 по 91 — закрыть 5 (+45) и открыть лонг 3 (маржа 27.3)
        p.order(
            "BTCUSDT",
            Side::Buy,
            Amount::Qty(8.0),
            None,
            Some(&book_at(90.0, 91.0)),
            &l,
        )
        .unwrap();
        let pos = p.position("BTCUSDT");
        assert!(
            (pos.qty - 3.0).abs() < 1e-9 && (pos.entry - 91.0).abs() < 1e-9,
            "{pos:?}"
        );
        assert!((p.realized - 45.0).abs() < 1e-9);
        assert!((p.free("USDT") - (1000.0 + 45.0 - 27.3)).abs() < 1e-9);
        // «весь» на продажу закрывает лонг
        p.order(
            "BTCUSDT",
            Side::Sell,
            Amount::All,
            None,
            Some(&book_at(92.0, 93.0)),
            &l,
        )
        .unwrap();
        assert!(p.positions.is_empty());
        assert!((p.free("USDT") - 1048.0).abs() < 1e-9);
        assert_eq!(p.closed, 2);
        // маржи не хватает: 2000 при балансе 1048 и плече 1
        p.set_perp(1.0);
        let e = p
            .order(
                "BTCUSDT",
                Side::Buy,
                Amount::Qty(20.0),
                None,
                Some(&book_at(99.0, 100.0)),
                &l,
            )
            .unwrap_err();
        assert!(e.contains("маржи"), "{e}");
    }

    #[test]
    fn perp_funding_and_liquidation() {
        let mut p = Paper::new(&["BTCUSDT".into()], 100.0, 0.0, 0.0);
        p.set_perp(10.0);
        let l = Limits {
            max_order_quote: 100_000.0,
            ..lim()
        };
        p.order(
            "BTCUSDT",
            Side::Buy,
            Amount::Qty(5.0),
            None,
            Some(&book_at(99.0, 100.0)),
            &l,
        )
        .unwrap();
        // лонг платит при положительной ставке: 5 × 100 × 0.0001
        assert!((p.apply_funding("BTCUSDT", 0.0001, 100.0) - 0.05).abs() < 1e-12);
        assert!((p.free("USDT") - 49.95).abs() < 1e-9);
        // -9.6%: маржа 50 + (-48) = 2 < 0.5% × 452 — ликвидация, маржа потеряна
        p.match_resting("BTCUSDT", &book_at(90.3, 90.5), &l);
        assert!(p.positions.is_empty());
        assert_eq!(p.liquidations, 1);
        assert!((p.free("USDT") - 49.95).abs() < 1e-9);
        let a = p.account_json(&HashMap::from([("BTCUSDT".to_string(), 90.4)]));
        assert_eq!(a["market"], json!("perp"));
        assert_eq!(a["liquidations"], json!(1));
    }

    #[test]
    fn perp_limit_locks_margin_only_for_opening() {
        let mut p = Paper::new(&["BTCUSDT".into()], 1000.0, 0.0, 0.0);
        p.set_perp(5.0);
        let l = Limits {
            max_order_quote: 100_000.0,
            ..lim()
        };
        let b = book_at(100.0, 101.0);
        p.order("BTCUSDT", Side::Buy, Amount::Qty(2.0), None, Some(&b), &l)
            .unwrap();
        let free = p.free("USDT");
        // продажа 2 по 105 только закрывает лонг — маржа не занимается
        p.order(
            "BTCUSDT",
            Side::Sell,
            Amount::Qty(2.0),
            Some(105.0),
            Some(&b),
            &l,
        )
        .unwrap();
        assert!((p.free("USDT") - free).abs() < 1e-9);
        p.match_resting("BTCUSDT", &book_at(105.0, 106.0), &l);
        assert!(p.positions.is_empty());
        assert!((p.realized - 8.0).abs() < 1e-9);
    }
}
