//! GET /metrics — торговые метрики в текстовом формате Prometheus exposition (как Java PrometheusExporter).
//! Prometheus сам забирает их по расписанию (scrape_configs → targets ["<хост бота>:8080"], path /metrics) и отдаёт
//! Grafana; бот хранит только текущие значения. Вместо метрик JVM — память и потоки процесса (Linux, /proc/self/status).

use crate::state::Bot;
use serde_json::Value;
use std::fmt::Write;

struct Out(String);

impl Out {
    fn gauge(&mut self, name: &str, help: &str) {
        let _ = writeln!(self.0, "# HELP {name} {help}\n# TYPE {name} gauge");
    }
    fn counter(&mut self, name: &str, help: &str) {
        let _ = writeln!(self.0, "# HELP {name} {help}\n# TYPE {name} counter");
    }
    /// Значение с метками; NaN и бесконечность пропускаются.
    fn line(&mut self, name: &str, labels: &[(&str, &str)], v: f64) {
        if !v.is_finite() {
            return;
        }
        let l: Vec<String> = labels
            .iter()
            .map(|(k, x)| format!("{k}=\"{}\"", x.replace('\\', "\\\\").replace('"', "\\\"")))
            .collect();
        let num = if v == v.trunc() && v.abs() < 1e15 {
            format!("{}", v as i64)
        } else {
            format!("{v}")
        };
        if l.is_empty() {
            let _ = writeln!(self.0, "{name} {num}");
        } else {
            let _ = writeln!(self.0, "{name}{{{}}} {num}", l.join(","));
        }
    }
}

fn b(x: bool) -> f64 {
    if x { 1.0 } else { 0.0 }
}

fn f(v: &Value) -> f64 {
    v.as_f64().unwrap_or(f64::NAN)
}

/// Запросы к бирже по (бирже, виду): вызовов, ошибок, последние задержки в микросекундах.
#[derive(Default)]
struct Calls {
    total: u64,
    errors: u64,
    lat: std::collections::VecDeque<u64>,
}

const LAT_KEEP: usize = 1000;

static CALLS: std::sync::Mutex<std::collections::BTreeMap<(String, &'static str), Calls>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());

/// Запросы в секунду по источникам и входящие WS по каналам — для вкладки «Запросы» (как Java RequestRates):
/// всего и по секундам за последние 60 с.
const WINDOW: usize = 60;

#[derive(Default, Clone)]
struct Rate {
    total: u64,
    /// Счёт по секундам: (секунда, число), не старше WINDOW.
    secs: std::collections::VecDeque<(u64, u64)>,
}

impl Rate {
    fn inc(&mut self, now: u64) {
        self.total += 1;
        match self.secs.back_mut() {
            Some((t, n)) if *t == now => *n += 1,
            _ => self.secs.push_back((now, 1)),
        }
        while self
            .secs
            .front()
            .is_some_and(|(t, _)| now.saturating_sub(*t) >= WINDOW as u64)
        {
            self.secs.pop_front();
        }
    }
    /// Число по секундам за WINDOW (старые — первыми).
    fn history(&self, now: u64) -> Vec<u64> {
        let mut h = vec![0; WINDOW];
        for (t, n) in &self.secs {
            let age = now.saturating_sub(*t) as usize;
            if age < WINDOW {
                h[WINDOW - 1 - age] = *n;
            }
        }
        h
    }
    /// Среднее в секунду за последние 5 полных секунд.
    fn per_sec(&self, now: u64) -> f64 {
        let n: u64 = self
            .secs
            .iter()
            .filter(|(t, _)| *t < now && now - *t <= 5)
            .map(|x| x.1)
            .sum();
        n as f64 / 5.0
    }
}

/// Источники запросов (метки и названия, как в Java).
pub const SOURCES: &[(&str, &str)] = &[
    ("orders", "ордера"),
    ("account", "аккаунт"),
    ("public", "публичные"),
    ("book_poll", "опрос стакана"),
    ("discovery", "подбор"),
    ("funding", "ставки funding"),
    ("reference", "справочники"),
    ("ws_connect", "подключения WS"),
    ("ws_send", "сообщения WS"),
];
const CHANNELS: &[(&str, &str)] = &[("market", "рыночные данные"), ("private", "торговый сокет")];

static RATES: std::sync::Mutex<std::collections::BTreeMap<(String, &'static str, bool), Rate>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());

fn now_s() -> u64 {
    crate::util::now_ms() / 1000
}

/// Запрос к бирже по источнику (метка из SOURCES).
pub fn out(ex: &str, source: &'static str) {
    RATES
        .lock()
        .unwrap()
        .entry((ex.to_owned(), source, false))
        .or_default()
        .inc(now_s());
}

/// Входящее сообщение WebSocket по каналу (market, private).
pub fn inbound(ex: &str, channel: &'static str) {
    RATES
        .lock()
        .unwrap()
        .entry((ex.to_owned(), channel, true))
        .or_default()
        .inc(now_s());
}

/// Вид запроса торгового клиента → источник.
fn source_of(kind: &str) -> &'static str {
    match kind {
        "order" | "cancel" | "stop" | "stop_cancel" => "orders",
        "time" => "public",
        _ => "account",
    }
}

/// Биржи, по которым были запросы или входящие сообщения.
pub fn rate_exchanges() -> Vec<String> {
    let mut v: Vec<String> = RATES.lock().unwrap().keys().map(|k| k.0.clone()).collect();
    v.dedup();
    v
}

/// Снимок биржи для /requests/live: perSec, total, history, sources, wsIn, wsInPerSec, wsInHistory.
pub fn rates_snapshot(ex: &str) -> Value {
    let now = now_s();
    let m = RATES.lock().unwrap();
    let part = |inbound: bool, names: &[(&str, &str)]| {
        let (mut map, mut sum, mut ps, mut total) =
            (serde_json::Map::new(), vec![0u64; WINDOW], 0.0, 0u64);
        for (k, title) in names {
            let Some(r) = m.get(&(ex.to_owned(), *k, inbound)) else {
                continue;
            };
            let h = r.history(now);
            for (a, b) in sum.iter_mut().zip(&h) {
                *a += b;
            }
            let p = r.per_sec(now);
            ps += p;
            total += r.total;
            map.insert(k.to_string(), serde_json::json!({"title": title, "total": r.total, "perSec": (p * 100.0).round() / 100.0, "history": h}));
        }
        (map, sum, (ps * 100.0).round() / 100.0, total)
    };
    let (sources, hist, ps, total) = part(false, SOURCES);
    let (ws, ws_hist, ws_ps, _) = part(true, CHANNELS);
    serde_json::json!({"perSec": ps, "total": total, "history": hist, "sources": sources,
        "wsInPerSec": ws_ps, "wsInHistory": ws_hist, "wsIn": ws})
}

/// Время старта процесса (для hft_process_uptime_seconds).
static STARTED: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

pub fn mark_start() {
    STARTED.get_or_init(std::time::Instant::now);
}

/// Выполнить запрос к бирже и учесть его: вызов, ошибка, задержка.
pub async fn timed<T>(
    ex: &str,
    kind: &'static str,
    f: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    let t = std::time::Instant::now();
    crate::ratelimit::take(ex, source_of(kind)).await?;
    let r = f.await;
    record(ex, kind, t.elapsed().as_micros() as u64, r.is_ok());
    if let Err(e) = &r
        && crate::ratelimit::is_limit_error(e)
    {
        crate::ratelimit::hit(ex, e);
    }
    r
}

pub fn record(ex: &str, kind: &'static str, micros: u64, ok: bool) {
    let mut m = CALLS.lock().unwrap();
    let c = m.entry((ex.to_owned(), kind)).or_default();
    c.total += 1;
    if !ok {
        c.errors += 1;
    }
    if c.lat.len() >= LAT_KEEP {
        c.lat.pop_front();
    }
    c.lat.push_back(micros);
}

fn pct(v: &[u64], q: f64) -> f64 {
    let mut v = v.to_vec();
    v.sort_unstable();
    v.get(((v.len() as f64 - 1.0) * q).round() as usize)
        .map_or(f64::NAN, |x| *x as f64)
}

/// Запросы к биржам и задержка ордеров.
fn calls(o: &mut Out) {
    let m = CALLS.lock().unwrap();
    if m.is_empty() {
        return;
    }
    o.counter(
        "hft_requests_total",
        "Запросов к бирже по видам (order, cancel, query, balances, positions, stop, …)",
    );
    for ((ex, k), c) in m.iter() {
        o.line(
            "hft_requests_total",
            &[("exchange", ex), ("kind", k)],
            c.total as f64,
        );
    }
    o.counter("hft_request_errors_total", "Запросов к бирже с ошибкой");
    for ((ex, k), c) in m.iter() {
        o.line(
            "hft_request_errors_total",
            &[("exchange", ex), ("kind", k)],
            c.errors as f64,
        );
    }
    for (name, q, help) in [
        (
            "hft_request_latency_p50_micros",
            0.5,
            "Задержка запроса к бирже, p50 последних 1000, микросекунды",
        ),
        (
            "hft_request_latency_p99_micros",
            0.99,
            "Задержка запроса к бирже, p99 последних 1000, микросекунды",
        ),
    ] {
        o.gauge(name, help);
        for ((ex, k), c) in m.iter() {
            let v: Vec<u64> = c.lat.iter().copied().collect();
            o.line(name, &[("exchange", ex), ("kind", k)], pct(&v, q));
        }
    }
    // как в Java: латентность отправки ордера
    for (name, q, help) in [
        (
            "hft_order_latency_p50_micros",
            0.5,
            "Латентность отправки ордера, p50, микросекунды",
        ),
        (
            "hft_order_latency_p99_micros",
            0.99,
            "Латентность отправки ордера, p99, микросекунды",
        ),
    ] {
        o.gauge(name, help);
        for ((ex, k), c) in m.iter().filter(|((_, k), _)| *k == "order") {
            let _ = k;
            let v: Vec<u64> = c.lat.iter().copied().collect();
            o.line(name, &[("exchange", ex)], pct(&v, q));
        }
    }
}

/// Лимиты запросов (как в Java): заполненность вёдер, ответы «слишком часто», свои отказы, ожидание, пауза.
fn rate_budgets(o: &mut Out) {
    let ids = crate::ratelimit::exchanges();
    if ids.is_empty() {
        return;
    }
    let snaps: Vec<(String, Value)> = ids
        .into_iter()
        .map(|e| {
            let s = crate::ratelimit::snapshot(&e);
            (e, s)
        })
        .collect();
    o.gauge("hft_rate_used_ratio", "Заполненность ведра лимита запросов (1 = исчерпан запас бота, rateLimitSafety от лимита биржи)");
    for (ex, s) in &snaps {
        for b in s["buckets"].as_array().into_iter().flatten() {
            o.line(
                "hft_rate_used_ratio",
                &[
                    ("exchange", ex),
                    ("bucket", b["name"].as_str().unwrap_or("")),
                ],
                f(&b["usedRatio"]),
            );
        }
    }
    for (name, key, help, counter) in [
        (
            "hft_rate_limited_total",
            "rateLimited",
            "Ответов биржи о превышении лимита (429/418/403 и коды лимита)",
            true,
        ),
        (
            "hft_rate_local_rejects_total",
            "localRejects",
            "Запросов, не отправленных из-за исчерпанного своего бюджета",
            true,
        ),
        (
            "hft_rate_waited_ms_total",
            "waitedMs",
            "Суммарное ожидание в очереди лимита, мс",
            true,
        ),
        (
            "hft_rate_blocked_ms",
            "blockedMs",
            "Сколько ещё мс запросы к бирже приостановлены",
            false,
        ),
    ] {
        if counter {
            o.counter(name, help)
        } else {
            o.gauge(name, help)
        }
        for (ex, s) in &snaps {
            o.line(name, &[("exchange", ex)], f(&s[key]));
        }
    }
}

/// Рантайм tokio: рабочие потоки, живые задачи, очередь.
fn runtime(o: &mut Out) {
    let Ok(h) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let m = h.metrics();
    o.gauge("hft_tokio_workers", "Рабочих потоков рантайма tokio");
    o.line("hft_tokio_workers", &[], m.num_workers() as f64);
    o.gauge(
        "hft_tokio_alive_tasks",
        "Живых задач tokio (фиды, потоки счетов, фоновые циклы)",
    );
    o.line("hft_tokio_alive_tasks", &[], m.num_alive_tasks() as f64);
    o.gauge(
        "hft_tokio_global_queue_depth",
        "Задач в глобальной очереди tokio (растёт — рантайм не успевает)",
    );
    o.line(
        "hft_tokio_global_queue_depth",
        &[],
        m.global_queue_depth() as f64,
    );
}

/// Все метрики.
pub fn render(bot: &Bot) -> String {
    let mut o = Out(String::with_capacity(4096));
    process(&mut o);
    runtime(&mut o);
    calls(&mut o);
    rate_budgets(&mut o);
    let names = [
        (
            "hft_connected",
            "1 если WebSocket-соединение со стаканом биржи активно",
            false,
        ),
        (
            "hft_feed_realtime",
            "1 если стакан идёт по WebSocket, 0 — по REST-опросу или нет данных",
            false,
        ),
        (
            "hft_feed_gave_up",
            "1 если источник данных сдался (feedMaxFailures) — торговля на бирже на паузе",
            false,
        ),
        (
            "hft_feed_rest_polling",
            "1 если стакан опрашивается по REST (WebSocket не отвечает)",
            false,
        ),
        (
            "hft_trading_enabled",
            "1 если торговля на бирже включена в параметрах",
            false,
        ),
        (
            "hft_messages_total",
            "Всего сообщений получено с биржи",
            true,
        ),
        (
            "hft_reconnects_total",
            "Переподключений WebSocket стакана",
            true,
        ),
        (
            "hft_silence_reconnects_total",
            "Переподключений из-за тишины в WebSocket",
            true,
        ),
        (
            "hft_parse_errors_total",
            "Ошибок биржи и разбора в потоке стакана",
            true,
        ),
        (
            "hft_kill_switch",
            "1 если торговля остановлена риск-менеджером",
            false,
        ),
        (
            "hft_orders_accepted_total",
            "Ордеров прошло проверку риск-менеджера",
            true,
        ),
        (
            "hft_orders_rejected_total",
            "Ордеров отклонено риск-менеджером",
            true,
        ),
        (
            "hft_daily_pnl_quote",
            "Реализованный PnL за сутки UTC после комиссий, в котируемой валюте",
            false,
        ),
        (
            "hft_account_stream",
            "1 если поток событий живого счёта подключён",
            false,
        ),
    ];
    let running = bot.running();
    let exchanges: Vec<String> = if running {
        bot.snapshot().selection.keys().cloned().collect()
    } else {
        Vec::new()
    };
    for (name, help, counter) in names {
        if counter {
            o.counter(name, help)
        } else {
            o.gauge(name, help)
        }
        for ex in &exchanges {
            let l = [("exchange", ex.as_str())];
            let fd = bot.market.feed(ex).unwrap_or_default();
            let st = bot
                .accounts
                .lock()
                .unwrap()
                .get(ex)
                .map(|a| a.stats())
                .unwrap_or(Value::Null);
            let v = match name {
                "hft_connected" => b(fd.connected),
                "hft_feed_realtime" => b(fd.connected && !fd.rest_polling),
                "hft_feed_gave_up" => b(fd.gave_up),
                "hft_feed_rest_polling" => b(fd.rest_polling),
                "hft_trading_enabled" => b(bot
                    .params(ex)
                    .get("tradingEnabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)),
                "hft_messages_total" => fd.messages as f64,
                "hft_reconnects_total" => fd.reconnects as f64,
                "hft_silence_reconnects_total" => fd.silence_reconnects as f64,
                "hft_parse_errors_total" => fd.parse_errors as f64,
                "hft_kill_switch" => b(st["kill_switch"] == true),
                "hft_orders_accepted_total" => f(&st["ордеров_принято"]),
                "hft_orders_rejected_total" => f(&st["ордеров_отклонено"]),
                "hft_daily_pnl_quote" => f(&st["дневной_pnl"]),
                "hft_account_stream" => match st.get("поток_событий_счёта") {
                    Some(x) => b(*x == true),
                    None => f64::NAN, // бумажный счёт — потока нет
                },
                _ => f64::NAN,
            };
            o.line(name, &l, v);
        }
    }
    account(&mut o, bot, &exchanges);
    strategies(&mut o, bot, &exchanges);
    books(&mut o, bot, &exchanges);
    o.0
}

/// Тестовый счёт (бумага, testnet): итоги, стоимость по котируемым валютам (метки quote и mode), стопы, сироты.
fn account(o: &mut Out, bot: &Bot, exchanges: &[String]) {
    let accs: Vec<(String, Value)> = exchanges
        .iter()
        .filter_map(|ex| Some((ex.clone(), bot.account_json(ex)?)))
        .collect();
    let per = [
        (
            "hft_pnl_total_quote",
            "Реализованный PnL после комиссий тестового счёта, в котируемой валюте",
            "realizedPnl",
            false,
        ),
        (
            "hft_fees_total_quote",
            "Комиссии исполнений тестового счёта, в котируемой валюте",
            "fees",
            false,
        ),
        (
            "hft_paper_maker_fees_quote",
            "Тестовый счёт: комиссии мейкера",
            "makerFees",
            false,
        ),
        (
            "hft_open_positions",
            "Открытых позиций тестового счёта",
            "openPositions",
            false,
        ),
        (
            "hft_open_orders",
            "Открытых заявок тестового счёта",
            "openOrders",
            false,
        ),
        (
            "hft_closed_trades_total",
            "Закрытых сделок с результатом",
            "closedTrades",
            true,
        ),
        (
            "hft_winning_trades_total",
            "Прибыльных закрытых сделок",
            "winningTrades",
            true,
        ),
        (
            "hft_win_rate_percent",
            "Доля прибыльных закрытых сделок, %",
            "winRatePercent",
            false,
        ),
        (
            "hft_funding_paid_quote",
            "Перпы: funding уплачено (минус — получено)",
            "fundingPaid",
            false,
        ),
        (
            "hft_liquidations_total",
            "Перпы: ликвидаций на бумаге",
            "liquidations",
            true,
        ),
        (
            "hft_stops_triggered_total",
            "Перпы: сработавших стопов (бумага)",
            "stopsTriggered",
            true,
        ),
    ];
    for (name, help, key, counter) in per {
        if counter {
            o.counter(name, help)
        } else {
            o.gauge(name, help)
        }
        for (ex, a) in &accs {
            o.line(name, &[("exchange", ex)], f(&a[key]));
        }
    }
    for (name, help, key) in [
        (
            "hft_account_start_quote",
            "Тестовый счёт: стоимость на старте, в котируемой валюте (метка quote)",
            "start",
        ),
        (
            "hft_account_equity_quote",
            "Тестовый счёт: стоимость сейчас — валюта, монеты по середине цены и нереализованный результат",
            "equity",
        ),
        (
            "hft_account_pnl_quote",
            "Тестовый счёт: PnL всего (стоимость сейчас − старт)",
            "pnl",
        ),
        (
            "hft_account_pnl_percent",
            "Тестовый счёт: PnL всего, % от старта",
            "pnlPercent",
        ),
    ] {
        o.gauge(name, help);
        for (ex, a) in &accs {
            let mode = a["mode"].as_str().unwrap_or("PAPER");
            for (q, m) in a["byQuote"].as_object().into_iter().flatten() {
                o.line(
                    name,
                    &[("exchange", ex), ("quote", q), ("mode", mode)],
                    f(&m[key]),
                );
            }
        }
    }
    o.counter(
        "hft_ws_messages_in_total",
        "Входящих сообщений WebSocket от биржи (channel: market — стакан, private — события счёта)",
    );
    for ex in exchanges {
        let fd = bot.market.feed(ex).unwrap_or_default();
        o.line(
            "hft_ws_messages_in_total",
            &[("exchange", ex), ("channel", "market")],
            fd.messages as f64,
        );
        let ev = bot
            .accounts
            .lock()
            .unwrap()
            .get(ex)
            .map(|a| a.stats())
            .and_then(|s| s["событий_счёта"].as_f64());
        if let Some(e) = ev {
            o.line(
                "hft_ws_messages_in_total",
                &[("exchange", ex), ("channel", "private")],
                e,
            );
        }
    }
    o.gauge(
        "hft_orphan_positions",
        "Перпы: позиций без стратегии под защитой сторожа",
    );
    o.counter(
        "hft_orphans_closed_total",
        "Перпы: закрыто позиций без стратегии",
    );
    for ex in exchanges {
        let v = bot.orphans_json(ex);
        o.line(
            "hft_orphan_positions",
            &[("exchange", ex)],
            v["позиции"].as_array().map_or(0, Vec::len) as f64,
        );
        o.line(
            "hft_orphans_closed_total",
            &[("exchange", ex)],
            f(&v["закрыто"]),
        );
    }
}

/// Стратегии по биржам: включены ли (метка strategy), статарб — позиции, сделки, результат, z-score пар.
fn strategies(o: &mut Out, bot: &Bot, exchanges: &[String]) {
    o.gauge(
        "hft_strategy_enabled",
        "1 если стратегия включена в параметрах биржи (метка strategy)",
    );
    for ex in exchanges {
        let p = bot.params(ex);
        let sc = crate::spread::Params::from(&p).enabled;
        let sa = crate::statarb::Params::from(&p).enabled;
        o.line(
            "hft_strategy_enabled",
            &[("exchange", ex), ("strategy", "spread-capture")],
            b(sc),
        );
        o.line(
            "hft_strategy_enabled",
            &[("exchange", ex), ("strategy", "statarb")],
            b(sa),
        );
    }
    o.gauge("hft_statarb_zscore", "z-score спреда пары статарба");
    for (ex, pairs) in bot.statarb_zscores(exchanges) {
        for (pair, z) in pairs {
            o.line(
                "hft_statarb_zscore",
                &[("exchange", &ex), ("pair", &pair)],
                z,
            );
        }
    }
    let sas = bot.statarb_numbers(exchanges);
    o.gauge(
        "hft_statarb_positions",
        "Открытых позиций статистического арбитража",
    );
    for (ex, (pos, _, _)) in &sas {
        o.line("hft_statarb_positions", &[("exchange", ex)], *pos as f64);
    }
    o.counter(
        "hft_statarb_trades_total",
        "Закрытых сделок статистического арбитража",
    );
    for (ex, (_, t, _)) in &sas {
        o.line("hft_statarb_trades_total", &[("exchange", ex)], *t as f64);
    }
    o.gauge(
        "hft_statarb_pnl_quote",
        "Результат статистического арбитража с запуска",
    );
    for (ex, (_, _, p)) in &sas {
        o.line("hft_statarb_pnl_quote", &[("exchange", ex)], *p);
    }
}

/// Стаканы выбранных символов.
fn books(o: &mut Out, bot: &Bot, exchanges: &[String]) {
    let sel = bot.snapshot().selection;
    let mut rows = Vec::new();
    for ex in exchanges {
        for s in sel.get(ex).into_iter().flatten() {
            if let Some(bk) = bot.market.book(ex, s).filter(|b| b.is_ready()) {
                rows.push((ex.clone(), s.clone(), bk));
            }
        }
    }
    for (name, help) in [
        ("hft_best_bid", "Лучшая цена покупки в стакане"),
        ("hft_best_ask", "Лучшая цена продажи в стакане"),
        ("hft_spread_percent", "Спред в процентах от середины"),
        ("hft_imbalance", "Дисбаланс стакана от -1 до 1 (5 уровней)"),
        ("hft_book_age_ms", "Сколько мс назад обновлялся стакан"),
    ] {
        o.gauge(name, help);
        for (ex, s, bk) in &rows {
            let v = match name {
                "hft_best_bid" => bk.best_bid().map_or(f64::NAN, |x| x.0),
                "hft_best_ask" => bk.best_ask().map_or(f64::NAN, |x| x.0),
                "hft_spread_percent" => bk.spread_percent().unwrap_or(f64::NAN),
                "hft_imbalance" => bk.imbalance(5),
                _ => bk.age_ms().map_or(f64::NAN, |x| x as f64),
            };
            o.line(name, &[("exchange", ex), ("symbol", s)], v);
        }
    }
}

/// Память и потоки процесса (Linux).
fn process(o: &mut Out) {
    let st = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |k: &str| -> Option<f64> {
        st.lines()
            .find(|l| l.starts_with(k))?
            .split_whitespace()
            .nth(1)?
            .parse()
            .ok()
    };
    if let Some(kb) = field("VmRSS:") {
        o.gauge(
            "hft_process_resident_bytes",
            "Занято памяти процессом (RSS)",
        );
        o.line("hft_process_resident_bytes", &[], kb * 1024.0);
    }
    if let Some(t) = field("Threads:") {
        o.gauge("hft_process_threads", "Потоков процесса");
        o.line("hft_process_threads", &[], t);
    }
    if let Some(kb) = field("VmSize:") {
        o.gauge("hft_process_virtual_bytes", "Виртуальная память процесса");
        o.line("hft_process_virtual_bytes", &[], kb * 1024.0);
    }
    // CPU: поля utime и stime из /proc/self/stat (такты, обычно 100 в секунду)
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let f: Vec<&str> = stat
        .rsplit_once(')')
        .map(|x| x.1)
        .unwrap_or("")
        .split_whitespace()
        .collect();
    if let (Some(u), Some(st)) = (
        f.get(11).and_then(|x| x.parse::<f64>().ok()),
        f.get(12).and_then(|x| x.parse::<f64>().ok()),
    ) {
        o.counter(
            "hft_process_cpu_seconds_total",
            "Процессорное время процесса (user + system), секунды",
        );
        o.line("hft_process_cpu_seconds_total", &[], (u + st) / 100.0);
    }
    if let Ok(d) = std::fs::read_dir("/proc/self/fd") {
        o.gauge(
            "hft_process_open_fds",
            "Открытых файловых дескрипторов (сокеты бирж, файлы)",
        );
        o.line("hft_process_open_fds", &[], d.count() as f64);
    }
    if let Some(t) = STARTED.get() {
        o.gauge(
            "hft_process_uptime_seconds",
            "Сколько секунд работает процесс",
        );
        o.line(
            "hft_process_uptime_seconds",
            &[],
            t.elapsed().as_secs() as f64,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_and_percentiles() {
        for i in 1..=100u64 {
            record("testex", "order", i, i % 10 != 0);
        }
        let mut o = Out(String::new());
        calls(&mut o);
        assert!(
            o.0.contains("hft_requests_total{exchange=\"testex\",kind=\"order\"} 100"),
            "{}",
            o.0
        );
        assert!(o.0.contains("hft_request_errors_total{exchange=\"testex\",kind=\"order\"} 10"));
        assert!(
            o.0.contains("hft_order_latency_p50_micros{exchange=\"testex\"} 51"),
            "{}",
            o.0
        );
        assert!(o.0.contains("hft_order_latency_p99_micros{exchange=\"testex\"} 99"));
    }

    #[test]
    fn rates_by_second() {
        let mut r = Rate::default();
        for t in [100, 100, 101, 103, 103, 103] {
            r.inc(t);
        }
        let h = r.history(104);
        assert_eq!((r.total, &h[WINDOW - 5..]), (6, &[2, 1, 0, 3, 0][..]));
        assert_eq!(r.per_sec(104), 1.2);
        r.inc(200); // старше минуты — забыто
        assert_eq!(r.history(200).iter().sum::<u64>(), 1);
    }

    #[test]
    fn format() {
        let mut o = Out(String::new());
        o.gauge("x", "проба");
        o.line("x", &[("exchange", "bin\"ance")], 2.0);
        o.line("x", &[], 0.25);
        o.line("x", &[], f64::NAN);
        assert_eq!(
            o.0,
            "# HELP x проба\n# TYPE x gauge\nx{exchange=\"bin\\\"ance\"} 2\nx 0.25\n"
        );
    }
}
