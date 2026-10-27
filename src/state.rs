//! Состояние бота: выбранные биржи и монеты, привязка монет к стратегиям, параметры бирж, настройки процесса.
//! Хранится в JSON-файле (запись через временный файл и rename — файл не бывает наполовину записан).

use crate::account::Account;
use crate::book::Book;
use crate::data;
use crate::feed;
use crate::market::Market;
use crate::paper::{Amount, Fill, Limits, Paper, Side};
use crate::spread::{self, SpreadCapture};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

/// Сохраняемая часть состояния.
#[derive(Default, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Saved {
    /// Биржа → выбранные монеты (порядок выбора).
    #[serde(default)]
    pub selection: BTreeMap<String, Vec<String>>,
    /// Биржа → монета → стратегии, которые ею торгуют (нет записи — все включённые).
    #[serde(default)]
    pub symbol_strategies: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    /// Биржа → параметры, отличные от значений по умолчанию.
    #[serde(default)]
    pub params: BTreeMap<String, Map<String, Value>>,
    /// Настройки процесса, отличные от значений по умолчанию.
    #[serde(default)]
    pub settings: Map<String, Value>,
}

/// Ошибка действия: текст для админки.
pub type ActionResult<T> = Result<T, String>;

/// Бот: состояние под блокировкой и файл, куда оно пишется.
pub struct Bot {
    saved: Mutex<Saved>,
    running: Mutex<bool>,
    path: PathBuf,
    /// Стаканы и состояние фидов.
    pub market: Arc<Market>,
    /// Задачи фидов по биржам (пока соединения подняты).
    feeds: Mutex<HashMap<String, JoinHandle<()>>>,
    /// Бумажные счета подключённых бирж.
    pub accounts: Arc<Mutex<HashMap<String, Account>>>,
    /// Исполнение стоящих бумажных заявок по стакану (пока соединения подняты).
    matcher: Mutex<Option<JoinHandle<()>>>,
    /// «Сбор спреда» по биржам.
    spreads: Mutex<HashMap<String, SpreadCapture>>,
    /// Статарб по биржам.
    statarbs: Mutex<HashMap<String, crate::statarb::StatArb>>,
    /// Межбиржевой арбитраж на споте (один на процесс).
    spotarb: Mutex<crate::spotarb::SpotArb>,
    /// Lead-lag (один на процесс).
    leadlag: Mutex<crate::leadlag::LeadLag>,
    /// Арбитраж цен перпов между биржами (один на процесс).
    perparb: Mutex<crate::perparb::PerpArb>,
    /// Скринер (подбор тикеров).
    pub screener: Arc<Mutex<crate::screener::Screener>>,
    pub screener_sources: crate::screener::Sources,
    /// Сигналы: листинги и анонсы (алерты в Discord).
    pub signals: crate::signals::Signals,
    /// Последнее учтённое время funding по (биржа, символ).
    funding_applied: Mutex<HashMap<(String, String), u64>>,
    /// Сторож перпов: позиции без стратегии — с какого момента замечены (биржа, символ) → мс; закрыто по биржам.
    /// (замечена, отправлен выход), мс.
    pub orphans: Mutex<HashMap<(String, String), (u64, u64)>>,
    pub orphans_closed: Mutex<HashMap<String, u64>>,
    /// Перепроверка: монеты «только выход» (биржа → символ → причина) и итог последней перепроверки.
    pub exit_only: Mutex<HashMap<String, BTreeMap<String, String>>>,
    pub last_recheck: Mutex<Value>,
    rechecking: std::sync::atomic::AtomicBool,
}

/// Комиссия биржи из каталога, % (makerFeePct / takerFeePct).
fn fee(ex: &str, key: &str) -> f64 {
    data::catalog()["exchanges"]
        .as_array()
        .and_then(|a| a.iter().find(|e| e["id"] == ex))
        .and_then(|e| e[key].as_f64())
        .unwrap_or(0.1)
}

/// Плечо перпов из параметров (число или строка), 1…125.
fn leverage(p: &Map<String, Value>) -> f64 {
    p.get("leverage")
        .and_then(|v| {
            v.as_f64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(2.0)
        .clamp(1.0, 125.0)
}

impl Bot {
    /// Загрузить состояние: файл .db — SQLite (как в Java, таблица app_state, ключ state), иначе JSON-файл.
    /// Нет записи или она не читается — пустое состояние (непрочитанная запись SQLite сохраняется под state_backup_<время>).
    pub fn load(path: PathBuf) -> Self {
        let saved = if crate::store::is_sqlite(&path) {
            crate::store::load(&path)
        } else {
            std::fs::read_to_string(&path).ok()
        }
        .and_then(|s| match serde_json::from_str::<Saved>(&s) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!(
                    "Состояние {} не прочитано ({e}) — начинаю с пустого",
                    path.display()
                );
                if crate::store::is_sqlite(&path) {
                    crate::store::backup(&path);
                }
                None
            }
        })
        .unwrap_or_default();
        let bot = Bot {
            saved: Mutex::new(saved),
            running: Mutex::new(false),
            path,
            market: Arc::default(),
            feeds: Mutex::default(),
            accounts: Arc::default(),
            matcher: Mutex::default(),
            spreads: Mutex::default(),
            statarbs: Mutex::default(),
            spotarb: Mutex::default(),
            leadlag: Mutex::default(),
            perparb: Mutex::default(),
            screener: Arc::default(),
            screener_sources: crate::screener::Sources::default(),
            signals: crate::signals::Signals::default(),
            funding_applied: Mutex::default(),
            orphans: Mutex::default(),
            orphans_closed: Mutex::default(),
            exit_only: Mutex::default(),
            last_recheck: Mutex::new(json!({"status": "ещё не выполнялась"})),
            rechecking: Default::default(),
        };
        bot.persist();
        bot.apply_debug();
        bot
    }

    /// Снимок сохраняемого состояния.
    pub fn snapshot(&self) -> Saved {
        self.saved.lock().unwrap().clone()
    }

    /// Подняты ли соединения.
    pub fn running(&self) -> bool {
        *self.running.lock().unwrap()
    }

    fn persist(&self) {
        let s = self.saved.lock().unwrap().clone();
        if crate::store::is_sqlite(&self.path) {
            match serde_json::to_string(&s) {
                Ok(j) => crate::store::save(&self.path, &j),
                Err(e) => tracing::error!("Состояние не сериализовано: {e}"),
            }
            return;
        }
        let tmp = self.path.with_extension("tmp");
        let res = serde_json::to_vec_pretty(&s)
            .map_err(|e| e.to_string())
            .and_then(|b| std::fs::write(&tmp, b).map_err(|e| e.to_string()))
            .and_then(|_| std::fs::rename(&tmp, &self.path).map_err(|e| e.to_string()));
        if let Err(e) = res {
            tracing::error!(
                "Не удалось записать состояние в {}: {e}",
                self.path.display()
            );
        }
    }

    fn supported(ex: &str) -> ActionResult<()> {
        if data::exchange_ids().iter().any(|x| x == ex) {
            Ok(())
        } else {
            Err(format!("Неизвестная биржа: {ex}"))
        }
    }

    fn bind(s: &mut Saved, ex: &str, symbols: &[String], strategy: Option<&str>) {
        let Some(st) = strategy.map(str::trim).filter(|x| !x.is_empty()) else {
            return;
        };
        let m = s.symbol_strategies.entry(ex.to_owned()).or_default();
        for sym in symbols {
            m.entry(sym.clone()).or_default().insert(st.to_owned());
        }
    }

    fn unbind(s: &mut Saved, ex: &str, sym: &str) {
        if let Some(m) = s.symbol_strategies.get_mut(ex) {
            m.remove(sym);
            if m.is_empty() {
                s.symbol_strategies.remove(ex);
            }
        }
    }

    /// Выбрать биржу с монетами (параметры поверх уже заданных); монеты привязываются к стратегии, если она указана.
    pub fn select(
        &self,
        ex: &str,
        symbols: Vec<String>,
        params: &Map<String, Value>,
        strategy: Option<&str>,
    ) -> ActionResult<()> {
        Self::supported(ex)?;
        if symbols.is_empty() {
            return Err("Нужен хотя бы один символ".into());
        }
        let checked = Self::check_params(params)?;
        {
            let mut s = self.saved.lock().unwrap();
            let before = s.selection.get(ex).cloned().unwrap_or_default();
            for old in before.iter().filter(|o| !symbols.contains(o)) {
                Self::unbind(&mut s, ex, old);
            }
            s.selection.insert(ex.to_owned(), symbols.clone());
            s.params.entry(ex.to_owned()).or_default().extend(checked);
            Self::bind(&mut s, ex, &symbols, strategy);
        }
        tracing::info!("Биржа {ex} выбрана с символами {symbols:?}");
        self.persist();
        self.restart_feed(ex);
        Ok(())
    }

    /// Добавить монету к выбранной бирже.
    pub fn add_symbol(&self, ex: &str, sym: &str, strategy: Option<&str>) -> ActionResult<()> {
        let sym = sym.trim().to_uppercase();
        {
            let mut s = self.saved.lock().unwrap();
            let list = s
                .selection
                .get_mut(ex)
                .ok_or_else(|| format!("Биржа {ex} не выбрана"))?;
            if !list.contains(&sym) {
                list.push(sym.clone());
            }
            Self::bind(&mut s, ex, std::slice::from_ref(&sym), strategy);
        }
        self.persist();
        self.restart_feed(ex); // соединения подняты — подписаться на новую монету сразу
        Ok(())
    }

    /// Убрать монету из выбранных (биржа без монет из выбора не пропадает — у неё остаются параметры).
    pub fn remove_symbol(&self, ex: &str, sym: &str) -> ActionResult<()> {
        let sym = sym.trim().to_uppercase();
        {
            let mut s = self.saved.lock().unwrap();
            let list = s
                .selection
                .get_mut(ex)
                .ok_or_else(|| format!("Биржа {ex} не выбрана"))?;
            list.retain(|x| *x != sym);
            Self::unbind(&mut s, ex, &sym);
        }
        self.persist();
        self.restart_feed(ex);
        Ok(())
    }

    /// Убрать биржу из выбора вместе с монетами и параметрами.
    pub fn deselect(&self, ex: &str) -> ActionResult<()> {
        {
            let mut s = self.saved.lock().unwrap();
            if s.selection.remove(ex).is_none() {
                return Err(format!("Биржа {ex} не выбрана"));
            }
            s.symbol_strategies.remove(ex);
            s.params.remove(ex);
        }
        self.persist();
        Ok(())
    }

    fn check_params(params: &Map<String, Value>) -> ActionResult<Map<String, Value>> {
        let specs = data::exchange_schema();
        params
            .iter()
            .map(|(k, v)| {
                let raw = v
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| v.to_string());
                data::validate(specs, k, &raw).map(|x| (k.clone(), x))
            })
            .collect()
    }

    /// Параметры биржи: значения по умолчанию плюс заданные.
    pub fn params(&self, ex: &str) -> Map<String, Value> {
        let mut m = data::defaults(data::exchange_schema());
        if let Some(own) = self.saved.lock().unwrap().params.get(ex) {
            m.extend(own.clone());
        }
        m
    }

    /// Изменить параметры биржи (ошибка в любом — не меняется ничего).
    pub fn update_params(
        &self,
        ex: &str,
        params: &Map<String, Value>,
    ) -> ActionResult<Map<String, Value>> {
        Self::supported(ex)?;
        let checked = Self::check_params(params)?;
        self.saved
            .lock()
            .unwrap()
            .params
            .entry(ex.to_owned())
            .or_default()
            .extend(checked);
        self.persist();
        Ok(self.params(ex))
    }

    /// Настройки процесса: значения по умолчанию плюс заданные.
    pub fn settings(&self) -> Map<String, Value> {
        let mut m = data::defaults(data::settings_specs());
        m.extend(self.saved.lock().unwrap().settings.clone());
        m
    }

    /// Изменить настройки процесса (ошибка в любой — не меняется ничего).
    /// Режим отладки из настроек (debugRequests, строка или флаг).
    fn apply_debug(&self) {
        let v = self
            .settings()
            .get("debugRequests")
            .cloned()
            .unwrap_or_default();
        crate::journal::set_on(v == true || v == "true");
    }

    pub fn update_settings(&self, upd: &Map<String, Value>) -> ActionResult<()> {
        let specs = data::settings_specs();
        let checked: Map<String, Value> = upd
            .iter()
            .map(|(k, v)| {
                let raw = v
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| v.to_string());
                data::validate(specs, k, &raw).map(|x| (k.clone(), x))
            })
            .collect::<ActionResult<_>>()?;
        self.saved.lock().unwrap().settings.extend(checked);
        self.persist();
        self.apply_debug();
        Ok(())
    }

    /// Поднять соединения: фиды рыночных данных по выбранным биржам (ордеров пока нет).
    pub fn start(self: &Arc<Self>) -> ActionResult<()> {
        if self
            .saved
            .lock()
            .unwrap()
            .selection
            .values()
            .all(Vec::is_empty)
        {
            return Err("Не выбрано ни одной монеты — выберите их в «Торговле»".into());
        }
        // доля официального лимита запросов бирж — до создания первого бюджета
        if let Some(s) = self
            .settings()
            .get("rateLimitSafety")
            .and_then(Value::as_f64)
        {
            crate::ratelimit::set_safety(s);
        }
        {
            let mut r = self.running.lock().unwrap();
            if *r {
                return Err("Бот уже запущен".into());
            }
            *r = true;
        }
        let exchanges: Vec<String> = self
            .saved
            .lock()
            .unwrap()
            .selection
            .keys()
            .cloned()
            .collect();
        for ex in &exchanges {
            self.restart_feed(ex);
        }
        *self.matcher.lock().unwrap() = Some(self.spawn_engine());
        tracing::info!("Соединения подняты: {exchanges:?}");
        Ok(())
    }

    /// Остановить соединения: фиды снимаются, стаканы забываются.
    pub fn stop(&self) {
        *self.running.lock().unwrap() = false;
        for (_, t) in self.feeds.lock().unwrap().drain() {
            t.abort();
        }
        if let Some(t) = self.matcher.lock().unwrap().take() {
            t.abort();
        }
        self.market.clear();
        self.accounts.lock().unwrap().clear();
        self.spreads.lock().unwrap().clear();
        tracing::info!("Соединения остановлены");
    }

    /// Перезапустить фид биржи с текущими монетами и параметрами (соединения не подняты — ничего).
    fn restart_feed(&self, ex: &str) {
        if !self.running() {
            return;
        }
        if let Some(t) = self.feeds.lock().unwrap().remove(ex) {
            t.abort();
        }
        let symbols = self
            .saved
            .lock()
            .unwrap()
            .selection
            .get(ex)
            .cloned()
            .unwrap_or_default();
        if symbols.is_empty() {
            self.market.with_feed(ex, |f| *f = Default::default());
            return;
        }
        self.ensure_account(ex, &symbols);
        match feed::start(self.market.clone(), ex, symbols.clone(), &self.params(ex)) {
            feed::Plan::Run(h) => {
                self.feeds.lock().unwrap().insert(ex.to_owned(), h);
            }
            feed::Plan::Unsupported(why) => {
                tracing::warn!("{why}");
                self.market.with_feed(ex, |f| {
                    f.symbols = symbols;
                    f.unsupported = why;
                    f.connected = false;
                });
            }
        }
    }

    /// Режим счёта биржи по параметрам и ключам, как в Java: paper — бумага; live или testnet с ключами — реальные
    /// ордера (в Rust — Binance и Bybit, спот и USDT-перпы); иначе — бумага. Возвращает ключи и testnet для живого счёта.
    fn live_mode(&self, ex: &str) -> Option<(crate::signed::Keys, bool)> {
        let p = self.params(ex);
        let flag = |k: &str| p.get(k).and_then(Value::as_bool).unwrap_or(false);
        let testnet = flag("testnet");
        if flag("paper") || !(flag("live") || testnet) {
            return None;
        }
        if !matches!(
            ex,
            "binance"
                | "bybit"
                | "aster"
                | "okx"
                | "gate"
                | "kucoin"
                | "bingx"
                | "mexc"
                | "hyperliquid"
                | "lbank"
                | "upbit"
                | "uniswapv2"
        ) {
            tracing::warn!(
                "[{ex}] реальные ордера в Rust-бэкенде пока только для Binance, Bybit, Aster, OKX, Gate, KuCoin, BingX, MEXC, Hyperliquid, LBank, Upbit и Uniswap V2 (спот и USDT-перпы) — бумажный режим"
            );
            return None;
        }
        match crate::signed::Keys::from_env(ex, testnet) {
            // Aster: ASTER_API_SECRET — приватный ключ кошелька-агента
            Some(k)
                if ex == "hyperliquid"
                    && (crate::evm::Wallet::from_hex(&k.secret).is_err()
                        || !k.key.starts_with("0x")
                        || k.key.len() != 42) =>
            {
                tracing::warn!(
                    "[hyperliquid] HYPERLIQUID_API_KEY — адрес счёта 0x…, HYPERLIQUID_API_SECRET — приватный ключ агента (hex) — бумажный режим"
                );
                None
            }
            Some(k) if ex == "uniswapv2" => {
                match uniswap_client(&p, crate::uniswap_api::DEFAULT_RPC, &k.secret) {
                    Ok(_) => Some((k, testnet)),
                    Err(e) => {
                        tracing::warn!("[uniswapv2] {e} — бумажный режим");
                        None
                    }
                }
            }
            Some(k) if ex == "aster" && crate::evm::Wallet::from_hex(&k.secret).is_err() => {
                tracing::warn!(
                    "[aster] ASTER_API_SECRET — не приватный ключ кошелька (hex) — бумажный режим"
                );
                None
            }
            Some(_)
                if ex == "okx"
                    && crate::util::env(if testnet {
                        "OKX_TESTNET_PASSPHRASE"
                    } else {
                        "OKX_PASSPHRASE"
                    })
                    .is_ok_and(|v| v.trim().is_empty())
                    || ex == "okx"
                        && crate::util::env(if testnet {
                            "OKX_TESTNET_PASSPHRASE"
                        } else {
                            "OKX_PASSPHRASE"
                        })
                        .is_err() =>
            {
                tracing::warn!(
                    "[okx] нужен OKX_PASSPHRASE (testnet — OKX_TESTNET_PASSPHRASE) — бумажный режим"
                );
                None
            }
            Some(_)
                if ex == "kucoin"
                    && crate::util::env("KUCOIN_PASSPHRASE").is_ok_and(|v| v.trim().is_empty())
                    || ex == "kucoin" && crate::util::env("KUCOIN_PASSPHRASE").is_err() =>
            {
                tracing::warn!("[kucoin] нужен KUCOIN_PASSPHRASE — бумажный режим");
                None
            }
            Some(k) => Some((k, testnet)),
            None => {
                tracing::warn!(
                    "[{ex}] ключи {}_API_KEY/_SECRET не заданы — бумажный режим",
                    format!(
                        "{}{}",
                        ex.to_uppercase(),
                        if testnet { "_TESTNET" } else { "" }
                    )
                );
                None
            }
        }
    }

    /// Позиции перпов без стратегии (остались после перезапуска, открыты вручную, стратегия выключена) — под защиту
    /// (guardOrphans, по умолчанию да): стоп-лосс orphanStopLossPercent (по умолчанию 2 %) от цены входа по середине стакана
    /// и, если задан, таймаут orphanMaxHoldMinutes с момента, как бот их заметил. Закрытие — рыночным выходом (проходит и при
    /// выключенной торговле).
    fn guard_orphans(
        &self,
        ex: &str,
        acc: &mut Account,
        params: &serde_json::Map<String, Value>,
        owned: &std::collections::HashSet<String>,
        books: &HashMap<String, Book>,
    ) {
        let on = params
            .get("guardOrphans")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let sl = params
            .get("orphanStopLossPercent")
            .and_then(Value::as_f64)
            .unwrap_or(2.0);
        let hold = params
            .get("orphanMaxHoldMinutes")
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let now = crate::util::now_ms();
        let mut orphans = self.orphans.lock().unwrap();
        let positions: Vec<(String, f64, f64)> = acc
            .positions()
            .into_iter()
            .filter(|(_, q, _)| q.abs() > 1e-12)
            .collect();
        // закрытые и снова занятые стратегиями — не сироты
        orphans.retain(|(e, s), _| {
            e != ex || positions.iter().any(|(p, _, _)| p == s) && !owned.contains(s)
        });
        if !on {
            return;
        }
        for (sym, qty, entry) in positions {
            if owned.contains(&sym) {
                continue;
            }
            let key = (ex.to_owned(), sym.clone());
            let (since, closing) = *orphans.entry(key.clone()).or_insert_with(|| {
                tracing::warn!(
                    "[{ex}] позиция {sym} {qty} @ {entry} без стратегии — беру под защиту: стоп-лосс {sl}%{}",
                    if hold > 0.0 { format!(", закрытие через {hold} мин") } else { String::new() }
                );
                (now, 0)
            });
            // выход уже отправлен (живой счёт исполняет его в фоне) — ждём 10 с, прежде чем повторить
            if closing > 0 && now.saturating_sub(closing) < 10_000 {
                continue;
            }
            let book = books
                .get(&sym)
                .cloned()
                .or_else(|| self.market.book(ex, &sym));
            let Some(mid) = book.as_ref().filter(|b| b.is_ready()).and_then(Book::mid) else {
                continue; // нет свежей цены — ждём
            };
            if entry <= 0.0 {
                continue;
            }
            let pnl = qty.signum() * (mid - entry) / entry * 100.0;
            let why = if pnl <= -sl {
                format!("стоп-лосс {pnl:.2}%")
            } else if hold > 0.0 && now.saturating_sub(since) as f64 >= hold * 60_000.0 {
                "таймаут".to_owned()
            } else {
                continue;
            };
            tracing::warn!("[{ex}] закрываю позицию {sym} без стратегии ({why})");
            let side = if qty > 0.0 {
                crate::paper::Side::Sell
            } else {
                crate::paper::Side::Buy
            };
            match acc.exit_market(&sym, side, qty.abs(), book.as_ref()) {
                Ok(f) if f.executed > 0.0 => {
                    orphans.remove(&key);
                    *self
                        .orphans_closed
                        .lock()
                        .unwrap()
                        .entry(ex.to_owned())
                        .or_default() += 1;
                }
                // живой счёт: ордер ушёл, позиция исчезнет после исполнения
                Ok(_) if acc.is_live() => {
                    if closing == 0 {
                        *self
                            .orphans_closed
                            .lock()
                            .unwrap()
                            .entry(ex.to_owned())
                            .or_default() += 1;
                    }
                    orphans.insert(key, (since, now));
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("[{ex}] позиция {sym} без стратегии не закрыта: {e}"),
            }
        }
    }

    /// z-score пар статарба по биржам.
    pub fn statarb_zscores(&self, exchanges: &[String]) -> Vec<(String, Vec<(String, f64)>)> {
        let sas = self.statarbs.lock().unwrap();
        exchanges
            .iter()
            .filter_map(|ex| Some((ex.clone(), sas.get(ex)?.zscores())))
            .collect()
    }

    /// Статарб по биржам: (позиций, сделок, результат).
    pub fn statarb_numbers(&self, exchanges: &[String]) -> Vec<(String, (usize, u64, f64))> {
        let sas = self.statarbs.lock().unwrap();
        exchanges
            .iter()
            .filter_map(|ex| {
                let s = sas.get(ex)?;
                Some((ex.clone(), (s.position_count(), s.trades, s.total_pnl)))
            })
            .collect()
    }

    /// Сироты биржи для /status.
    pub fn orphans_json(&self, ex: &str) -> Value {
        let o: Vec<String> = self
            .orphans
            .lock()
            .unwrap()
            .keys()
            .filter(|(e, _)| e == ex)
            .map(|(_, s)| s.clone())
            .collect();
        json!({"позиции": o, "закрыто": self.orphans_closed.lock().unwrap().get(ex).copied().unwrap_or(0)})
    }

    /// Счёт биржи: создать (бумажный со стартовым балансом или живой) или добавить валюту новой монеты.
    fn ensure_account(&self, ex: &str, symbols: &[String]) {
        let p = self.params(ex);
        let start = p
            .get("paperStartBalance")
            .and_then(Value::as_f64)
            .unwrap_or(1000.0);
        let perp = p.get("market").and_then(Value::as_str) == Some("perp");
        // у перпов свои комиссии (perpMakerFeePct / perpTakerFeePct в каталоге)
        let fee = |k: &str| {
            if perp {
                fee(ex, &format!("perp{}{}", k[..1].to_uppercase(), &k[1..]))
            } else {
                fee(ex, k)
            }
        };
        if !self.accounts.lock().unwrap().contains_key(ex) {
            let mut worker = None;
            let account = match self.live_mode(ex) {
                Some((keys, testnet)) => {
                    let base = p
                        .get("restUrl")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .map(str::to_owned)
                        .unwrap_or_else(|| {
                            match ex {
                                "bybit" => crate::bybit_api::rest_url(testnet),
                                "aster" => crate::aster_api::rest_url(perp, testnet),
                                "okx" => crate::okx_api::REST,
                                "gate" => crate::gate_api::rest_url(perp, testnet),
                                "kucoin" => crate::kucoin_api::rest_url(perp),
                                "bingx" => crate::bingx_api::REST,
                                "mexc" => crate::mexc_api::rest_url(perp),
                                "hyperliquid" => crate::hyperliquid_api::rest_url(testnet),
                                "lbank" => crate::lbank_api::REST,
                                "upbit" => crate::upbit_api::REST,
                                "uniswapv2" => crate::uniswap_api::DEFAULT_RPC,
                                _ => crate::rest::default_url(perp, testnet),
                            }
                            .to_owned()
                        });
                    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                    let mode = if testnet { "TESTNET" } else { "LIVE" };
                    tracing::warn!("[{ex}] режим {mode}: ордера пойдут на биржу ({base})");
                    let client = Arc::new(if ex == "uniswapv2" {
                        // параметры проверены в live_mode
                        crate::live::Venue::Uniswap(
                            uniswap_client(&p, &base, &keys.secret).expect("параметры Uniswap"),
                        )
                    } else if ex == "upbit" {
                        crate::live::Venue::Upbit(crate::upbit_api::Client::new(
                            &base, keys, symbols,
                        ))
                    } else if ex == "lbank" {
                        crate::live::Venue::Lbank(crate::lbank_api::Client::new(&base, keys))
                    } else if ex == "hyperliquid" {
                        // ключи проверены в live_mode
                        crate::live::Venue::Hyperliquid(
                            crate::hyperliquid_api::Client::new(
                                &base, keys, perp, testnet, symbols,
                            )
                            .expect("ключ агента Hyperliquid"),
                        )
                    } else if ex == "mexc" {
                        crate::live::Venue::Mexc(crate::mexc_api::Client::new(&base, keys, perp))
                    } else if ex == "bingx" {
                        crate::live::Venue::Bingx(crate::bingx_api::Client::new(
                            &base, keys, perp, testnet,
                        ))
                    } else if ex == "kucoin" {
                        let pass = crate::util::env("KUCOIN_PASSPHRASE").unwrap_or_default();
                        crate::live::Venue::Kucoin(crate::kucoin_api::Client::new(
                            &base, keys, pass, perp,
                        ))
                    } else if ex == "gate" {
                        crate::live::Venue::Gate(crate::gate_api::Client::new(
                            &base, keys, perp, testnet, symbols,
                        ))
                    } else if ex == "okx" {
                        // passphrase проверен в live_mode
                        let pass = crate::util::env(if testnet {
                            "OKX_TESTNET_PASSPHRASE"
                        } else {
                            "OKX_PASSPHRASE"
                        })
                        .unwrap_or_default();
                        crate::live::Venue::Okx(crate::okx_api::Client::new(
                            &base, keys, pass, perp, testnet,
                        ))
                    } else if ex == "aster" {
                        // ключ агента проверен в live_mode
                        crate::live::Venue::Aster(
                            crate::aster_api::Client::new(&base, keys, perp, testnet)
                                .expect("ключ агента Aster"),
                        )
                    } else if ex == "bybit" {
                        let mut c = crate::bybit_api::Client::new(&base, keys);
                        if perp {
                            c.category = "linear";
                        }
                        crate::live::Venue::Bybit(c)
                    } else if perp {
                        crate::live::Venue::Binance(crate::signed::Client::new_perp(&base, keys))
                    } else {
                        crate::live::Venue::Binance(crate::signed::Client::new(&base, keys))
                    });
                    // задача счёта запускается после того, как счёт попал в таблицу (иначе её первые ответы теряются)
                    worker = Some(crate::live::worker(
                        self.accounts.clone(),
                        ex.to_owned(),
                        client,
                        rx,
                        symbols.to_vec(),
                        perp.then(|| leverage(&p)),
                        // ордера по WebSocket, где биржа умеет (по умолчанию); false — только REST
                        p.get("wsOrders").and_then(Value::as_bool) != Some(false),
                    ));
                    Account::Live(crate::live::Live::new(
                        symbols,
                        fee("makerFeePct"),
                        fee("takerFeePct"),
                        mode,
                        tx,
                    ))
                }
                None => Account::Paper(Paper::new(
                    symbols,
                    start,
                    fee("makerFeePct"),
                    fee("takerFeePct"),
                )),
            };
            let mut account = account;
            if p.get("market").and_then(Value::as_str) == Some("perp") {
                account.set_perp(leverage(&p));
            }
            let mut account = account;
            account.set_venue(ex);
            self.accounts.lock().unwrap().insert(ex.to_owned(), account);
            if let Some(w) = worker.take() {
                tokio::spawn(w);
            }
        }
        if let Some(a) = self.accounts.lock().unwrap().get_mut(ex) {
            for s in symbols {
                a.ensure_quote(s, start);
            }
        }
    }

    /// Раз в 50 мс: исполнить стоящие бумажные заявки, через которые прошла цена, и сделать шаг стратегий.
    fn spawn_engine(self: &Arc<Self>) -> JoinHandle<()> {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(50));
            loop {
                tick.tick().await;
                let Some(bot) = weak.upgrade() else { return };
                bot.tick();
            }
        })
    }

    /// Шаг движка по всем подключённым биржам.
    fn tick(&self) {
        let saved = self.snapshot();
        let now = std::time::Instant::now();
        let exchanges: Vec<String> = self.accounts.lock().unwrap().keys().cloned().collect();
        let mut cross: HashMap<String, (HashMap<String, Book>, Limits)> = HashMap::new();
        // «биржа:символ», занятые стратегиями одной биржи (lead-lag их не трогает)
        let mut busy_cross: std::collections::HashSet<String> = std::collections::HashSet::new();
        for ex in exchanges {
            let params = self.params(&ex);
            let mut lim = Limits::from_params(&params);
            // источник данных сдался (feedMaxFailures) — торговля на бирже на паузе, заявки сняты
            if self.market.feed(&ex).is_some_and(|f| f.gave_up) {
                lim.trading_enabled = false;
                if let Some(a) = self.accounts.lock().unwrap().get_mut(&ex) {
                    let n = a.cancel_all(None);
                    if n > 0 {
                        tracing::warn!("[{ex}] источник данных сдался — снято заявок: {n}");
                    }
                }
            }
            let sp = spread::Params::from(&params);
            let selected = saved.selection.get(&ex).cloned().unwrap_or_default();
            let books: HashMap<String, Book> = selected
                .iter()
                .filter_map(|s| self.market.book(&ex, s).map(|b| (s.clone(), b)))
                .collect();
            cross.insert(ex.clone(), (books.clone(), lim.clone()));
            let mut acc = self.accounts.lock().unwrap();
            let Some(paper) = acc.get_mut(&ex) else {
                continue;
            };
            paper.set_filters(&self.market.filters(&ex));
            for (sym, b) in &books {
                if b.is_ready() {
                    paper.match_resting(sym, b, &lim);
                }
            }
            // перпы: защитный стоп по каждой позиции (и снятие стопов закрытых)
            if paper.is_perp() {
                let pct = params
                    .get("exchangeStopLossPercent")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
                for sym in paper.stop_symbols() {
                    let book = books
                        .get(&sym)
                        .cloned()
                        .or_else(|| self.market.book(&ex, &sym));
                    paper.protect(&sym, pct, book.as_ref().filter(|b| b.is_ready()));
                }
            }
            // funding перпов: когда наступило время списания — один раз по последней известной ставке
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let mut applied = self.funding_applied.lock().unwrap();
            for sym in &selected {
                let Some(f) = self.market.funding(&ex, sym) else {
                    continue;
                };
                let key = (ex.clone(), sym.clone());
                let last = applied.get(&key).copied();
                if f.next_ms == 0 || now_ms < f.next_ms || last == Some(f.next_ms) {
                    continue;
                }
                applied.insert(key, f.next_ms);
                let mark = books.get(sym).and_then(Book::mid).unwrap_or(f.mark);
                let pay = paper.apply_funding(sym, f.rate, mark);
                if pay != 0.0 {
                    tracing::info!("[{ex}] funding {sym}: ставка {} — {pay:+.6}", f.rate);
                }
            }
            drop(applied);
            let mut sas = self.statarbs.lock().unwrap();
            let sa = sas.entry(ex.clone()).or_default();
            let sa_held = sa.held();
            // монеты этой биржи, занятые позициями lead-lag
            let ll_held: std::collections::HashSet<String> = self
                .leadlag
                .lock()
                .unwrap()
                .held()
                .into_iter()
                .chain(self.perparb.lock().unwrap().held())
                .filter_map(|k| k.strip_prefix(&format!("{ex}:")).map(str::to_owned))
                .collect();
            let mut all = self.spreads.lock().unwrap();
            let sc = all
                .entry(ex.clone())
                .or_insert_with(|| SpreadCapture::new(fee(&ex, "makerFeePct")));
            // перепроверка: монеты «только выход» — новых входов нет, позиции доводятся до выхода
            let blocked: std::collections::HashSet<String> = self
                .exit_only
                .lock()
                .unwrap()
                .get(&ex)
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default();
            for sym in &selected {
                let bound = saved.symbol_strategies.get(&ex).and_then(|m| m.get(sym));
                let mine = bound.is_none_or(|ids| ids.is_empty() || ids.contains(spread::ID))
                    && !sa_held.contains(sym)
                    && !ll_held.contains(sym)
                    && !blocked.contains(sym);
                let book = books.get(sym);
                if sp.enabled && lim.trading_enabled && mine {
                    let minutes =
                        self.market
                            .minutes(&ex, sym, std::time::Duration::from_secs(180));
                    let filter = paper.filter(sym);
                    sc.step(sym, book, paper, &sp, &lim, now, minutes.as_deref(), filter);
                } else if sc.active() {
                    sc.stop_symbol(paper, sym, book, "стратегия или торговля выключена");
                }
            }
            for sym in sc.symbols().into_iter().filter(|s| !selected.contains(s)) {
                sc.stop_symbol(paper, &sym, books.get(&sym), "монета убрана из выбранных");
            }
            // статарб: монеты без привязки или привязанные к нему; занятые сбором спреда — не трогает
            let sa_syms: Vec<String> = selected
                .iter()
                .filter(|s| !blocked.contains(*s))
                .filter(|s| {
                    saved
                        .symbol_strategies
                        .get(&ex)
                        .and_then(|m| m.get(*s))
                        .is_none_or(|ids| ids.is_empty() || ids.contains(crate::statarb::ID))
                })
                .cloned()
                .collect();
            let busy: std::collections::HashSet<String> = sc
                .symbols()
                .into_iter()
                .chain(ll_held.iter().cloned())
                .collect();
            busy_cross.extend(
                busy.iter()
                    .chain(sa_held.iter())
                    .map(|s| format!("{ex}:{s}")),
            );
            sa.step(
                &books,
                paper,
                &crate::statarb::Params::from(&params),
                &lim,
                &sa_syms,
                &busy,
                now,
            );
            // сторож перпов: позиции, которыми не управляет ни одна стратегия
            if paper.is_perp() {
                let owned: std::collections::HashSet<String> =
                    busy.into_iter().chain(sa.held()).collect();
                self.guard_orphans(&ex, paper, &params, &owned, &books);
            }
        }
        // межбиржевой арбитраж на споте: все счета сразу
        let sp = crate::spotarb::Params::from(&self.settings());
        let mut acc = self.accounts.lock().unwrap();
        let mut venues: HashMap<String, crate::spotarb::Venue> = acc
            .iter_mut()
            .filter_map(|(ex, a)| {
                let (books, lim) = cross.remove(ex)?;
                Some((
                    ex.clone(),
                    crate::spotarb::Venue {
                        account: a,
                        trading: lim.trading_enabled,
                        lim,
                        books,
                    },
                ))
            })
            .collect();
        self.spotarb.lock().unwrap().step(&mut venues, &sp, now);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        // монеты пар perp-arb lead-lag не трогает, и наоборот
        let pa_held = self.perparb.lock().unwrap().held();
        let ll_busy: std::collections::HashSet<String> =
            busy_cross.union(&pa_held).cloned().collect();
        self.leadlag.lock().unwrap().step(
            &mut venues,
            &crate::leadlag::Params::from(&self.settings()),
            &ll_busy,
            now_ms,
        );
        let pa_busy: std::collections::HashSet<String> = busy_cross
            .union(&self.leadlag.lock().unwrap().held())
            .cloned()
            .collect();
        self.perparb.lock().unwrap().step(
            &mut venues,
            &crate::perparb::Params::from(&self.settings()),
            &pa_busy,
            now_ms,
        );
    }

    /// Биржи подбора (discoveryExchanges) и период автопересчёта.
    fn discovery_settings(&self) -> (Vec<String>, u64) {
        let st = self.settings();
        let chosen = st
            .get("discoveryExchanges")
            .and_then(Value::as_str)
            .unwrap_or("")
            .split(',')
            .map(|x| x.trim().to_lowercase())
            .filter(|x| !x.is_empty())
            .collect();
        let sec = st
            .get("discoveryRefreshSec")
            .and_then(|v| {
                v.as_f64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(0.0) as u64;
        (chosen, sec)
    }

    /// Ответ /discovery.
    pub fn discovery_json(&self) -> Value {
        let (chosen, sec) = self.discovery_settings();
        self.screener.lock().unwrap().json(&chosen, sec)
    }

    /// Запустить пересчёт подбора в фоне; false — уже идёт или биржи не выбраны.
    pub fn discovery_refresh(&self) -> ActionResult<()> {
        let (chosen, sec) = self.discovery_settings();
        if chosen.is_empty() {
            return Err("Сначала выберите биржи для подбора (discoveryExchanges)".into());
        }
        {
            let mut s = self.screener.lock().unwrap();
            if s.running {
                return Err("Пересчёт уже идёт".into());
            }
            s.running = true;
        }
        let (scr, src, p) = (
            self.screener.clone(),
            self.screener_sources.clone(),
            self.settings(),
        );
        tokio::spawn(async move {
            let t0 = std::time::Instant::now();
            let (ex, strategies) = crate::screener::run(&src, &chosen, &p).await;
            let mut s = scr.lock().unwrap();
            s.exchanges = ex;
            s.strategies = strategies;
            s.running = false;
            s.duration_ms = t0.elapsed().as_millis() as u64;
            s.updated_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            s.next_at = (sec > 0).then(|| s.updated_at + sec * 1000);
            tracing::info!("[screener] пересчёт за {} мс", s.duration_ms);
        });
        Ok(())
    }

    /// Профили подбора стратегий, включённых на бирже (как enabledProfiles в Java).
    fn enabled_profiles(&self, ex: &str) -> Vec<&'static str> {
        let (p, g) = (self.params(ex), self.settings());
        let on = |k: &str, d: bool| g.get(k).map_or(d, |v| *v == true || v == "true");
        let mut ids = Vec::new();
        if crate::statarb::Params::from(&p).enabled {
            ids.push(crate::statarb::ID);
        }
        if crate::spread::Params::from(&p).enabled {
            ids.push(crate::spread::ID);
        }
        if on("spotArbEnabled", false) {
            ids.push("cross-exchange-arb");
        }
        if on("perpArbEnabled", false) {
            ids.push("perp-arb");
        }
        if on("leadLagEnabled", false) {
            ids.push("lead-lag");
        }
        ids
    }

    /// Перепроверка выбранных монет по итогу подбора (как TradeRecheck в Java): монета подходит, если её предлагает
    /// как «подходит» хотя бы одна стратегия, включённая на бирже (привязанная монета — по своим стратегиям). Не подходит —
    /// входы запрещены; при tradeRecheckRetire (по умолчанию) — вывод из торговли: заявки снимаются, позиция перпа
    /// закрывается рыночным выходом, монета убирается из выбранных. Сводки биржи нет — монеты не трогаются.
    fn apply_recheck(
        &self,
        statuses: &BTreeMap<String, crate::screener::ExStatus>,
        strategies: &[Value],
        t0: std::time::Instant,
    ) -> Value {
        let titles: HashMap<&str, &str> = [
            ("stat-arb", "статарбитраж"),
            ("perp-arb", "перп – перп на разных биржах"),
            ("spread-capture", "сбор спреда"),
            ("cross-exchange-arb", "спот – спот на разных биржах"),
            ("lead-lag", "lead-lag"),
        ]
        .into();
        let saved = self.snapshot();
        let retire = self
            .settings()
            .get("tradeRecheckRetire")
            .is_none_or(|v| *v == true || v == "true");
        let mut rows = Vec::new();
        for (ex, syms) in saved
            .selection
            .iter()
            .filter(|(ex, _)| statuses.contains_key(*ex))
        {
            let enabled = self.enabled_profiles(ex);
            let data_ok = statuses[ex].ok;
            for sym in syms {
                let own: Vec<String> = saved
                    .symbol_strategies
                    .get(ex)
                    .and_then(|m| m.get(sym))
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                let ids: Vec<&str> = if own.is_empty() {
                    enabled.clone()
                } else {
                    enabled
                        .iter()
                        .copied()
                        .filter(|i| own.iter().any(|o| o == i))
                        .collect()
                };
                let (status, reason) = if ids.is_empty() && !own.is_empty() {
                    (
                        "UNKNOWN",
                        format!("стратегия монеты ({}) выключена на бирже", own.join(", ")),
                    )
                } else if ids.is_empty() {
                    (
                        "UNKNOWN",
                        "на бирже не включена ни одна стратегия".to_owned(),
                    )
                } else if !data_ok {
                    (
                        "UNKNOWN",
                        "нет данных с биржи — оставлено как было".to_owned(),
                    )
                } else {
                    let mut why = Vec::new();
                    let mut ok_by = None;
                    for id in &ids {
                        let cands = strategies
                            .iter()
                            .find(|s| s["id"] == *id)
                            .and_then(|s| s["candidates"].as_array())
                            .cloned()
                            .unwrap_or_default();
                        let covers = |c: &Value| {
                            c["metrics"]["symbols"][ex.as_str()]
                                .as_array()
                                .is_some_and(|l| l.iter().any(|x| x == sym.as_str()))
                                || (c["exchange"] == ex.as_str() && c["symbol"] == sym.as_str())
                        };
                        let hits: Vec<&Value> = cands.iter().filter(|c| covers(c)).collect();
                        let title = titles.get(id).copied().unwrap_or(id);
                        if hits.iter().any(|c| c["suitable"] == true) {
                            ok_by = Some(title);
                            break;
                        }
                        why.push(match hits.first() {
                            None => format!("{title}: нет среди прибыльных кандидатов"),
                            Some(c) => format!(
                                "{title}: {}{}",
                                c["verdict"].as_str().unwrap_or(""),
                                c["reasons"]
                                    .as_array()
                                    .and_then(|r| r.last())
                                    .and_then(Value::as_str)
                                    .map_or(String::new(), |r| format!(" ({r})"))
                            ),
                        });
                    }
                    match ok_by {
                        Some(t) => ("OK", format!("подходит под {t}")),
                        None => ("EXIT_ONLY", why.join("; ")),
                    }
                };
                rows.push((ex.clone(), sym.clone(), status, reason));
            }
        }
        let mut out = Vec::new();
        for (ex, sym, status, reason) in rows {
            match status {
                "OK" => {
                    if let Some(m) = self.exit_only.lock().unwrap().get_mut(&ex) {
                        m.remove(&sym);
                    }
                }
                "EXIT_ONLY" => {
                    self.exit_only
                        .lock()
                        .unwrap()
                        .entry(ex.clone())
                        .or_default()
                        .insert(sym.clone(), reason.clone());
                    crate::journal::action(
                        &ex,
                        Some(&sym),
                        &format!("перепроверка: не подходит — {reason}"),
                    );
                    if retire && let Err(e) = self.retire_symbol(&ex, &sym, &reason) {
                        tracing::error!("[{ex}] {sym}: не удалось вывести из торговли — {e}");
                    }
                }
                _ => {}
            }
            let title = match status {
                "OK" => "подходит — входы разрешены",
                "EXIT_ONLY" => "не подходит — только выход",
                _ => "не проверялась",
            };
            out.push(json!({"exchange": ex, "symbol": sym, "status": status, "statusText": title, "reason": reason}));
        }
        let sec = self.discovery_settings().1;
        let v = json!({"checkedAt": crate::util::now_ms(), "durationMs": t0.elapsed().as_millis() as u64, "everySec": sec, "symbols": out});
        tracing::info!("Перепроверка монет: {}", v["symbols"]);
        *self.last_recheck.lock().unwrap() = v.clone();
        v
    }

    /// Вывести монету из торговли (как retireOn в Java): входы запрещены, заявки сняты, позиция перпа закрыта рыночным
    /// выходом, монета убрана из выбранных (стратегии доводят свои позиции до выхода — она больше не их).
    pub fn retire_symbol(&self, ex: &str, sym: &str, reason: &str) -> ActionResult<Value> {
        let sym = sym.to_uppercase();
        tracing::warn!("[{ex}] {sym} выводится из торговли: {reason}");
        let mut v = json!({"exchange": ex, "symbol": sym, "status": "выведена из торговли"});
        if let Some(a) = self.accounts.lock().unwrap().get_mut(ex) {
            v["ordersCancelled"] = json!(a.cancel_all(Some(&sym)));
            let q = a.position_qty(&sym);
            if a.is_perp() && q.abs() > 1e-12 {
                let side = if q > 0.0 {
                    crate::paper::Side::Sell
                } else {
                    crate::paper::Side::Buy
                };
                let book = self.market.book(ex, &sym);
                v["positionClose"] = match a.exit_market(&sym, side, q.abs(), book.as_ref()) {
                    Ok(f) => json!(format!("{} {} @ {}", f.status, f.executed, f.avg_price)),
                    Err(e) => json!(format!("ошибка: {e}")),
                };
            }
        }
        self.remove_symbol(ex, &sym)?;
        if let Some(m) = self.exit_only.lock().unwrap().get_mut(ex) {
            m.remove(&sym);
        }
        Ok(v)
    }

    /// POST /trading/recheck: подбор по биржам с выбранными монетами и перепроверка по его итогу.
    pub async fn recheck_now(self: &Arc<Self>) -> ActionResult<Value> {
        if !self.running() {
            return Err("Соединения не подняты — перепроверять нечего".into());
        }
        if self
            .rechecking
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err("Перепроверка уже идёт".into());
        }
        let t0 = std::time::Instant::now();
        let exs: Vec<String> = self
            .snapshot()
            .selection
            .keys()
            .filter(|e| crate::screener::SUPPORTED.contains(&e.as_str()))
            .cloned()
            .collect();
        let (st, strategies) =
            crate::screener::run(&self.screener_sources, &exs, &self.settings()).await;
        self.rechecking
            .store(false, std::sync::atomic::Ordering::SeqCst);
        Ok(self.apply_recheck(&st, &strategies, t0))
    }

    /// GET /trading/recheck: итог последней перепроверки (монеты, убранные из выбранных, не показываются).
    pub fn recheck_json(&self) -> Value {
        let mut v = self.last_recheck.lock().unwrap().clone();
        let sel = self.snapshot().selection;
        if let Some(rows) = v["symbols"].as_array() {
            let keep: Vec<Value> = rows
                .iter()
                .filter(|r| {
                    sel.get(r["exchange"].as_str().unwrap_or(""))
                        .is_some_and(|l| l.iter().any(|s| r["symbol"] == s.as_str()))
                })
                .cloned()
                .collect();
            v["symbols"] = json!(keep);
        }
        let (_, sec) = self.discovery_settings();
        v["everySec"] = json!(sec);
        v["scheduled"] = json!(sec > 0);
        v["running"] = json!(
            self.rechecking.load(std::sync::atomic::Ordering::SeqCst)
                || (self.screener.lock().unwrap().running && self.running())
        );
        if let Some(n) = self.screener.lock().unwrap().next_at {
            v["nextAt"] = json!(n);
        }
        v
    }

    /// Автопересчёт подбора (discoveryRefreshSec): проверка раз в секунду.
    /// Сигналы (листинги, анонсы) в фоне — с настройками процесса, пока бот жив.
    pub fn spawn_signals(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        self.signals
            .spawn(move || weak.upgrade().map(|b| b.settings()));
    }

    pub fn spawn_screener(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
            let mut seen = 0u64;
            loop {
                tick.tick().await;
                let Some(bot) = weak.upgrade() else { return };
                let (chosen, sec) = bot.discovery_settings();
                if sec == 0 || chosen.is_empty() {
                    continue;
                }
                let due = {
                    let s = bot.screener.lock().unwrap();
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    !s.running && s.updated_at + sec * 1000 <= now
                };
                if due {
                    let _ = bot.discovery_refresh();
                }
                // перепроверка — по каждому новому итогу подбора, пока идёт торговля
                let fresh = {
                    let s = bot.screener.lock().unwrap();
                    (!s.running && s.updated_at > seen)
                        .then(|| (s.updated_at, s.exchanges.clone(), s.strategies.clone()))
                };
                if let Some((at, st, strategies)) = fresh {
                    seen = at;
                    if bot.running() && !bot.rechecking.load(std::sync::atomic::Ordering::SeqCst) {
                        bot.apply_recheck(&st, &strategies, std::time::Instant::now());
                    }
                }
            }
        });
    }

    /// Межбиржевые стратегии для /arbitrage (как в Java): spotPrice — арбитраж на споте; перп-арбитража и
    /// lead-lag в Rust ещё нет.
    pub fn arbitrage_json(&self) -> Value {
        let off = json!({"running": false});
        let spot = if self.running() {
            self.spotarb
                .lock()
                .unwrap()
                .stats(&crate::spotarb::Params::from(&self.settings()))
        } else {
            off.clone()
        };
        let ll = if self.running() {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            self.leadlag
                .lock()
                .unwrap()
                .stats(&crate::leadlag::Params::from(&self.settings()), now_ms)
        } else {
            off.clone()
        };
        let pa = if self.running() {
            self.perparb
                .lock()
                .unwrap()
                .stats(&crate::perparb::Params::from(&self.settings()), None)
        } else {
            off.clone()
        };
        json!({"perpPrice": pa, "spotPrice": spot, "leadLag": ll})
    }

    /// Состояние стратегий биржи для админки (/strategies).
    pub fn strategies_json(&self, ex: &str) -> ActionResult<Value> {
        self.running_exchange(ex)?;
        let sp = spread::Params::from(&self.params(ex));
        let sc = self
            .spreads
            .lock()
            .unwrap()
            .get(ex)
            .map(|s| s.stats(sp.enabled))
            .unwrap_or_else(|| SpreadCapture::new(fee(ex, "makerFeePct")).stats(sp.enabled));
        let p = self.params(ex);
        let syms = self
            .snapshot()
            .selection
            .get(ex)
            .cloned()
            .unwrap_or_default();
        let sa = self
            .statarbs
            .lock()
            .unwrap()
            .get(ex)
            .map(|s| s.stats(&crate::statarb::Params::from(&p), &syms))
            .unwrap_or_else(|| {
                crate::statarb::StatArb::default().stats(&crate::statarb::Params::from(&p), &syms)
            });
        let mut m = Map::new();
        m.insert("spreadCapture".into(), sc);
        m.insert("statArb".into(), sa);
        Ok(Value::Object(m))
    }

    fn running_exchange(&self, ex: &str) -> ActionResult<()> {
        if !self.running() {
            return Err("Соединения не подняты".into());
        }
        if !self.accounts.lock().unwrap().contains_key(ex) {
            return Err(format!("биржа {ex} не запущена"));
        }
        Ok(())
    }

    /// Ручной ордер на бумажном счёте (спот).
    pub fn place_order(
        &self,
        ex: &str,
        symbol: &str,
        side: Side,
        amount: Amount,
        price: Option<f64>,
    ) -> ActionResult<Fill> {
        self.running_exchange(ex)?;
        let p = self.params(ex);
        let sym = symbol.trim().to_uppercase();
        if !self
            .snapshot()
            .selection
            .get(ex)
            .is_some_and(|l| l.contains(&sym))
        {
            return Err(format!("{sym} не выбран на {ex}"));
        }
        let book = self.market.book(ex, &sym);
        let filters = self.market.filters(ex);
        let mut acc = self.accounts.lock().unwrap();
        let paper = acc.get_mut(ex).unwrap();
        paper.set_filters(&filters);
        let res = paper.order(
            &sym,
            side,
            amount,
            price,
            book.as_ref(),
            &Limits::from_params(&p),
        );
        match &res {
            Ok(f) => tracing::info!(
                "[{ex}] ордер {sym} {side:?}: {} {} @ {}",
                f.status,
                f.executed,
                f.avg_price
            ),
            Err(e) => tracing::warn!("[{ex}] ордер {sym} {side:?} отклонён: {e}"),
        }
        res
    }

    /// Снять бумажные заявки символа.
    pub fn cancel_all(&self, ex: &str, symbol: &str) -> ActionResult<usize> {
        self.running_exchange(ex)?;
        Ok(self
            .accounts
            .lock()
            .unwrap()
            .get_mut(ex)
            .unwrap()
            .cancel_all(Some(symbol)))
    }

    /// Свободные балансы бумажного счёта.
    pub fn balances(&self, ex: &str) -> ActionResult<std::collections::BTreeMap<String, f64>> {
        self.running_exchange(ex)?;
        Ok(self.accounts.lock().unwrap()[ex].balances())
    }

    /// Ставки funding выбранных символов биржи (как PerpAccount.stats().funding в Java): интервал — 8 ч,
    /// у Hyperliquid — 1 ч.
    fn funding_rates(&self, ex: &str) -> Vec<Value> {
        let hours = if ex == "hyperliquid" { 1.0 } else { 8.0 };
        let selected = self
            .snapshot()
            .selection
            .get(ex)
            .cloned()
            .unwrap_or_default();
        selected
            .iter()
            .filter_map(|s| {
                let f = self.market.funding(ex, s)?;
                Some(json!({"symbol": s, "ratePercent": f.rate * 100.0, "intervalHours": hours,
                    "per8hPercent": f.rate * 8.0 / hours * 100.0, "aprPercent": f.rate * 24.0 / hours * 365.0 * 100.0,
                    "nextFundingMs": f.next_ms}))
            })
            .collect()
    }

    /// GET /positions: позиции перпов, плечо и ставки по биржам (exchange — одна или all).
    pub fn positions_json(&self, target: &str) -> Value {
        let mut out = serde_json::Map::new();
        let mut acc = self.accounts.lock().unwrap();
        for (ex, a) in acc
            .iter_mut()
            .filter(|(ex, _)| target == "all" || target == ex.as_str())
        {
            if !a.is_perp() {
                continue;
            }
            let ps: Vec<Value> = a
                .positions()
                .into_iter()
                .filter(|(_, q, _)| q.abs() > 1e-12)
                .map(|(s, q, e)| {
                    let mark = self.market.book(ex, &s).and_then(|b| b.mid());
                    json!({"symbol": s, "side": if q > 0.0 { "LONG" } else { "SHORT" }, "qty": q.abs(), "entryPrice": e,
                        "markPrice": mark, "notional": mark.map(|m| q.abs() * m), "unrealizedPnl": mark.map(|m| q * (m - e))})
                })
                .collect();
            out.insert(
                ex.clone(),
                json!({"leverage": leverage(&self.params(ex)), "positions": ps,
                "funding": self.funding_rates(ex)}),
            );
        }
        Value::Object(out)
    }

    /// GET /funding: ставки funding по биржам перпов.
    pub fn funding_json(&self) -> Value {
        let perps: Vec<String> = self
            .accounts
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, a)| a.is_perp())
            .map(|(e, _)| e.clone())
            .collect();
        let rates: serde_json::Map<String, Value> = perps
            .into_iter()
            .map(|ex| {
                let r = self.funding_rates(&ex);
                (ex, Value::from(r))
            })
            .collect();
        json!({"rates": rates})
    }

    /// POST /positions/close: закрыть позицию рыночным выходом (symbol=all — все позиции биржи).
    pub fn close_positions(&self, ex: &str, symbol: &str) -> ActionResult<Value> {
        self.running_exchange(ex)?;
        let mut acc = self.accounts.lock().unwrap();
        let a = acc.get_mut(ex).expect("счёт запущенной биржи");
        if !a.is_perp() {
            return Err(format!("Биржа {ex} торгует спотом — позиций нет"));
        }
        let symbols: Vec<String> = if symbol.eq_ignore_ascii_case("all") {
            a.positions()
                .into_iter()
                .filter(|(_, q, _)| q.abs() > 1e-12)
                .map(|(s, _, _)| s)
                .collect()
        } else {
            vec![symbol.to_uppercase()]
        };
        let closed: Vec<Value> = symbols
            .into_iter()
            .map(|s| {
                let q = a.position_qty(&s);
                if q.abs() <= 1e-12 {
                    return json!({"symbol": s, "status": "позиции нет"});
                }
                let side = if q > 0.0 { crate::paper::Side::Sell } else { crate::paper::Side::Buy };
                let book = self.market.book(ex, &s);
                match a.exit_market(&s, side, q.abs(), book.as_ref()) {
                    Ok(f) => {
                        tracing::warn!("[{ex}] позиция {s} закрывается через админку: {q}");
                        json!({"symbol": s, "status": f.status, "executedQty": f.executed, "avgPrice": f.avg_price})
                    }
                    Err(e) => json!({"symbol": s, "status": "ошибка", "error": e}),
                }
            })
            .collect();
        Ok(json!({"биржа": ex, "closed": closed}))
    }

    /// Сбросить тестовый счёт: бумажный — к стартовому балансу, живой — начать учёт результата заново
    /// (балансы на бирже не меняются).
    pub fn reset_account(&self, ex: &str) -> ActionResult<Value> {
        self.running_exchange(ex)?;
        {
            let mut acc = self.accounts.lock().unwrap();
            if let Some(Account::Live(l)) = acc.get_mut(ex) {
                let b = l.ledger.balances();
                let (m, t) = l.ledger.fees();
                l.ledger = Paper::new(&[], 0.0, m * 100.0, t * 100.0);
                l.ledger.set_balances(b);
                drop(acc);
                return Ok(self.account_json(ex).unwrap_or(Value::Null));
            }
            acc.remove(ex);
        }
        let symbols = self
            .snapshot()
            .selection
            .get(ex)
            .cloned()
            .unwrap_or_default();
        self.ensure_account(ex, &symbols);
        Ok(self.account_json(ex).unwrap_or(Value::Null))
    }

    /// Тестовый счёт биржи для админки; None — биржа не запущена.
    pub fn account_json(&self, ex: &str) -> Option<Value> {
        let mids: HashMap<String, f64> = self
            .market
            .feed(ex)
            .map(|f| f.symbols)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|s| {
                self.market
                    .book(ex, &s)
                    .and_then(|b| b.mid())
                    .map(|m| (s, m))
            })
            .collect();
        self.accounts
            .lock()
            .unwrap()
            .get_mut(ex)
            .and_then(|a| a.account_json(&mids))
    }

    /// Биржи для команды торговли: «all» — все выбранные.
    fn targets(&self, target: &str) -> Vec<String> {
        let sel = self.snapshot().selection;
        if target == "all" {
            sel.keys().cloned().collect()
        } else {
            sel.keys().filter(|k| *k == target).cloned().collect()
        }
    }

    /// Включить или выключить торговлю (параметр tradingEnabled); включение снимает kill switch.
    pub fn set_trading(&self, target: &str, on: bool) -> ActionResult<()> {
        let t = self.targets(target);
        if t.is_empty() {
            return Err(format!("Нет выбранной биржи {target}"));
        }
        for ex in &t {
            let mut m = Map::new();
            m.insert("tradingEnabled".into(), Value::String(on.to_string()));
            self.update_params(ex, &m)?;
            if on && let Some(a) = self.accounts.lock().unwrap().get_mut(ex) {
                a.reset_kill_switch();
            }
        }
        Ok(())
    }

    /// Аварийная остановка: торговля выключена, все бумажные заявки сняты.
    pub fn panic(&self, target: &str) -> ActionResult<()> {
        self.set_trading(target, false)?;
        for ex in self.targets(target) {
            if let Some(a) = self.accounts.lock().unwrap().get_mut(&ex) {
                a.cancel_all(None);
            }
        }
        Ok(())
    }
}

/// Клиент Uniswap V2 из параметров биржи: uniRouter, uniTokens, uniSlippagePercent (по умолчанию 0.5).
fn uniswap_client(
    p: &serde_json::Map<String, Value>,
    rpc: &str,
    secret: &str,
) -> Result<crate::uniswap_api::Client, String> {
    let s = |k: &str| p.get(k).and_then(Value::as_str).unwrap_or("").to_owned();
    let slip = p
        .get("uniSlippagePercent")
        .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse().ok()))
        .unwrap_or(0.5);
    crate::uniswap_api::Client::new(rpc, secret, &s("uniRouter"), &s("uniTokens"), slip)
        .map_err(|e| e.replace("uniswapv2: ", ""))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bot() -> (tempfile::TempDir, Bot) {
        let dir = tempfile::tempdir().unwrap();
        let b = Bot::load(dir.path().join("state.json"));
        (dir, b)
    }

    #[test]
    fn select_bind_persist() {
        let (dir, b) = bot();
        b.select(
            "binance",
            vec!["AAAUSDT".into()],
            &Map::new(),
            Some("spread-capture"),
        )
        .unwrap();
        b.add_symbol("binance", "bbbusdt", Some("stat-arb"))
            .unwrap();
        let again = Bot::load(dir.path().join("state.json"));
        let s = again.snapshot();
        assert_eq!(s.selection["binance"], vec!["AAAUSDT", "BBBUSDT"]);
        assert!(s.symbol_strategies["binance"]["AAAUSDT"].contains("spread-capture"));
        again.remove_symbol("binance", "AAAUSDT").unwrap();
        assert!(!again.snapshot().symbol_strategies["binance"].contains_key("AAAUSDT"));
    }

    /// Перепроверка: подошла — входы разрешены; не подошла — вывод из торговли (по умолчанию) или «только выход».
    #[test]
    fn recheck_by_discovery_result() {
        let (_dir, b) = bot();
        b.select(
            "binance",
            vec!["AAAUSDT".into(), "BBBUSDT".into(), "CCCUSDT".into()],
            &Map::new(),
            None,
        )
        .unwrap();
        let st = BTreeMap::from([(
            "binance".to_string(),
            crate::screener::ExStatus {
                ok: true,
                ..Default::default()
            },
        )]);
        let cand = |sym: &str, ok: bool| {
            json!({"exchange": "binance", "symbol": sym, "suitable": ok, "verdict": if ok { "подходит" } else { "не подходит" },
            "reasons": ["спред 0.05%"], "metrics": {"symbols": {"binance": [sym]}}})
        };
        let strategies = vec![
            json!({"id": "spread-capture", "candidates": [cand("AAAUSDT", true), cand("BBBUSDT", false)]}),
        ];
        // без вывода из торговли — только выход
        b.update_settings(&serde_json::from_value(json!({"tradeRecheckRetire": "false"})).unwrap())
            .unwrap();
        let v = b.apply_recheck(&st, &strategies, std::time::Instant::now());
        let row = |s: &str| {
            v["symbols"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["symbol"] == s)
                .unwrap()["status"]
                .clone()
        };
        assert_eq!(
            (row("AAAUSDT"), row("BBBUSDT"), row("CCCUSDT")),
            (json!("OK"), json!("EXIT_ONLY"), json!("EXIT_ONLY")),
            "{v}"
        );
        assert!(
            v["symbols"][1]["reason"]
                .as_str()
                .unwrap()
                .contains("сбор спреда: не подходит (спред 0.05%)"),
            "{v}"
        );
        assert!(
            v["symbols"][2]["reason"]
                .as_str()
                .unwrap()
                .contains("нет среди прибыльных кандидатов")
        );
        assert_eq!(b.exit_only.lock().unwrap()["binance"].len(), 2);
        // снова подошла — входы разрешены
        let strategies2 =
            vec![json!({"id": "spread-capture", "candidates": [cand("BBBUSDT", true)]})];
        b.apply_recheck(&st, &strategies2, std::time::Instant::now());
        assert!(!b.exit_only.lock().unwrap()["binance"].contains_key("BBBUSDT"));
        // по умолчанию — вывод из торговли: монета убрана из выбранных
        b.update_settings(&serde_json::from_value(json!({"tradeRecheckRetire": "true"})).unwrap())
            .unwrap();
        b.apply_recheck(&st, &strategies, std::time::Instant::now());
        assert_eq!(b.snapshot().selection["binance"], vec!["AAAUSDT"]);
        // сводки биржи нет — не трогаем
        let none = BTreeMap::from([("binance".to_string(), crate::screener::ExStatus::default())]);
        let v = b.apply_recheck(&none, &[], std::time::Instant::now());
        assert_eq!(v["symbols"][0]["status"], "UNKNOWN");
        assert_eq!(b.recheck_json()["symbols"].as_array().unwrap().len(), 1);
    }

    /// То же на SQLite (как в Java): состояние переживает перезапуск.
    #[test]
    fn select_persist_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("data/state.db");
        let b = Bot::load(db.clone());
        b.select("okx", vec!["XUSDT".into()], &Map::new(), None)
            .unwrap();
        drop(b);
        let again = Bot::load(db.clone());
        assert_eq!(again.snapshot().selection["okx"], vec!["XUSDT"]);
        // испорченная запись — пустое состояние, прежняя сохранена резервной копией
        crate::store::save(&db, "{не json");
        let fresh = Bot::load(db.clone());
        assert!(fresh.snapshot().selection.is_empty());
        let c = rusqlite::Connection::open(&db).unwrap();
        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM app_state WHERE key LIKE 'state_backup_%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn params_validated_atomically() {
        let (_d, b) = bot();
        let mut p = Map::new();
        p.insert("scJumpTicks".into(), "1".into());
        p.insert("tradingEnabled".into(), "maybe".into());
        assert!(b.update_params("binance", &p).is_err());
        assert_eq!(b.params("binance")["scJumpTicks"], Value::from(0));
        p.insert("tradingEnabled".into(), "true".into());
        let m = b.update_params("binance", &p).unwrap();
        assert_eq!(m["scJumpTicks"], Value::from(1));
        assert_eq!(m["tradingEnabled"], Value::Bool(true));
    }

    #[tokio::test]
    async fn start_needs_symbols() {
        let (_d, b) = bot();
        let b = Arc::new(b);
        assert!(b.start().is_err());
        b.select("bybit", vec!["BTCUSDT".into()], &Map::new(), None)
            .unwrap();
        b.start().unwrap();
        assert!(b.running());
        assert!(b.start().is_err());
        b.stop();
        assert!(!b.running());
    }

    #[test]
    fn unknown_exchange_rejected() {
        let (_d, b) = bot();
        assert!(
            b.select("nope", vec!["X".into()], &Map::new(), None)
                .is_err()
        );
    }
}
