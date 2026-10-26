//! Состояние бота: выбранные биржи и монеты, привязка монет к стратегиям, параметры бирж, настройки процесса.
//! Хранится в JSON-файле (запись через временный файл и rename — файл не бывает наполовину записан).

use crate::data;
use crate::feed;
use crate::market::Market;
use crate::paper::{Amount, Fill, Limits, Paper, Side};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
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
    pub accounts: Arc<Mutex<HashMap<String, Paper>>>,
    /// Исполнение стоящих бумажных заявок по стакану (пока соединения подняты).
    matcher: Mutex<Option<JoinHandle<()>>>,
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
    pub fn start(&self) -> ActionResult<()> {
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
        *self.matcher.lock().unwrap() = Some(self.spawn_matcher());
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

    /// Бумажный счёт биржи: создать со стартовым балансом или добавить валюту новой монеты.
    fn ensure_account(&self, ex: &str, symbols: &[String]) {
        let p = self.params(ex);
        let start = p
            .get("paperStartBalance")
            .and_then(Value::as_f64)
            .unwrap_or(1000.0);
        let fee = |k: &str| {
            data::catalog()["exchanges"]
                .as_array()
                .and_then(|a| a.iter().find(|e| e["id"] == ex))
                .and_then(|e| e[k].as_f64())
                .unwrap_or(0.1)
        };
        let mut acc = self.accounts.lock().unwrap();
        let a = acc
            .entry(ex.to_owned())
            .or_insert_with(|| Paper::new(symbols, start, fee("makerFeePct"), fee("takerFeePct")));
        for s in symbols {
            a.ensure_quote(s, start);
        }
    }

    /// Раз в 50 мс исполнить стоящие бумажные заявки, через которые прошла цена.
    fn spawn_matcher(&self) -> JoinHandle<()> {
        let (accounts, market) = (self.accounts.clone(), self.market.clone());
        let limits: HashMap<String, Limits> = self
            .snapshot()
            .selection
            .keys()
            .map(|ex| (ex.clone(), Limits::from_params(&self.params(ex))))
            .collect();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(50));
            loop {
                tick.tick().await;
                let mut acc = accounts.lock().unwrap();
                for (ex, a) in acc.iter_mut() {
                    if a.open_orders() == 0 {
                        continue;
                    }
                    let lim = limits
                        .get(ex)
                        .cloned()
                        .unwrap_or_else(|| Limits::from_params(&Map::new()));
                    for sym in market.feed(ex).map(|f| f.symbols).unwrap_or_default() {
                        if let Some(b) = market.book(ex, &sym).filter(|b| b.is_ready()) {
                            a.match_resting(&sym, &b, &lim);
                        }
                    }
                }
            }
        })
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
        if p.get("market").and_then(Value::as_str) == Some("perp") {
            return Err(
                "бумажные перпы в Rust-бэкенде пока не поддерживаются — переключите рынок на spot"
                    .into(),
            );
        }
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
        let res = self.accounts.lock().unwrap().get_mut(ex).unwrap().order(
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

    /// Сбросить бумажный счёт к стартовому балансу.
    pub fn reset_account(&self, ex: &str) -> ActionResult<Value> {
        self.running_exchange(ex)?;
        self.accounts.lock().unwrap().remove(ex);
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
            .map(|a| a.account_json(&mids))
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
                a.kill_switch = false;
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
