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
    /// Последнее учтённое время funding по (биржа, символ).
    funding_applied: Mutex<HashMap<(String, String), u64>>,
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
    /// Загрузить состояние из файла (нет файла или он повреждён — пустое состояние, файл создаётся).
    pub fn load(path: PathBuf) -> Self {
        let saved = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| match serde_json::from_str::<Saved>(&s) {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!(
                        "Файл состояния {} не прочитан ({e}) — начинаю с пустого",
                        path.display()
                    );
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
            funding_applied: Mutex::default(),
        };
        bot.persist();
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
            let lim = Limits::from_params(&params);
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
            for sym in &selected {
                let bound = saved.symbol_strategies.get(&ex).and_then(|m| m.get(sym));
                let mine = bound.is_none_or(|ids| ids.is_empty() || ids.contains(spread::ID))
                    && !sa_held.contains(sym)
                    && !ll_held.contains(sym);
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
            crate::screener::Params::from(&self.settings()),
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

    /// Автопересчёт подбора (discoveryRefreshSec): проверка раз в секунду.
    pub fn spawn_screener(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
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
