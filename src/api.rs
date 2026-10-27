//! Админ-API в формате Java-бота: тот же набор адресов, параметры в строке запроса (и для POST),
//! ответы — JSON с русскими ключами, ошибка — `{"результат": "ошибка", "сообщение": "..."}`.

use crate::data;
use crate::paper::{Amount, Side};
use crate::state::Bot;
use axum::extract::{Query, Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::stream::{self, Stream};
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tower_http::cors::{AllowOrigin, CorsLayer};

/// Настройки админки.
#[derive(Clone, Debug, Default)]
pub struct AdminConfig {
    /// Токен для заголовка X-Admin-Token; пусто — без проверки.
    pub token: String,
    /// С каких страниц можно обращаться (CORS); пусто — с любых.
    pub cors_origins: Vec<String>,
    /// /metrics тоже под токеном (METRICS_AUTH=true); иначе — открыт для Prometheus, как в Java.
    pub metrics_auth: bool,
}

#[derive(Clone)]
struct App {
    bot: Arc<Bot>,
    cfg: AdminConfig,
}

type Q = Query<HashMap<String, String>>;

/// Ошибка обработчика: код и текст.
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.0,
            Json(json!({"результат": "ошибка", "сообщение": self.1})),
        )
            .into_response()
    }
}

fn bad(msg: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, msg.into())
}

fn ok(msg: impl Into<String>) -> Json<Value> {
    Json(json!({"результат": "успех", "сообщение": msg.into()}))
}

fn need<'a>(q: &'a HashMap<String, String>, k: &str) -> Result<&'a str, ApiError> {
    q.get(k)
        .map(String::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| bad(format!("Не указан параметр {k}")))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Темы потока /events (как в Java: только данные, которые меняются в реальном времени).
const EVENT_TOPICS: &[&str] = &[
    "/control/status",
    "/status",
    "/exchanges/request-stats",
    "/discovery",
];

/// Собрать роутер админки.
pub fn router(bot: Arc<Bot>, cfg: AdminConfig) -> Router {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([
            header::CONTENT_TYPE,
            header::HeaderName::from_static("x-admin-token"),
        ])
        .allow_origin(if cfg.cors_origins.is_empty() {
            AllowOrigin::any()
        } else {
            AllowOrigin::list(
                cfg.cors_origins
                    .iter()
                    .filter_map(|o| HeaderValue::from_str(o).ok()),
            )
        })
        .max_age(Duration::from_secs(600));
    let app = App { bot, cfg };
    let api = Router::new()
        .route("/control/status", get(control_status))
        .route("/control/select", axum::routing::post(select))
        .route("/control/symbols", axum::routing::post(symbols))
        .route("/control/retire", axum::routing::post(retire))
        .route("/control/deselect", axum::routing::post(deselect))
        .route("/control/start", axum::routing::post(start))
        .route("/control/stop", axum::routing::post(stop))
        .route("/status", get(status))
        .route(
            "/exchanges/catalog",
            get(|| async { Json(data::catalog().clone()) }),
        )
        .route("/exchanges/request-stats", get(request_stats))
        .route(
            "/requests/live",
            get(|| async { Json(json!({"ts": now_ms(), "exchanges": {}})) }),
        )
        .route("/discovery", get(discovery))
        .route("/discovery/refresh", axum::routing::post(discovery_refresh))
        .route("/market", get(market))
        .route("/strategies", get(strategies))
        .route("/arbitrage", get(arbitrage))
        .route("/order", axum::routing::post(order))
        .route("/cancel-all", axum::routing::post(cancel_all))
        .route("/balances", get(balances))
        .route("/positions", get(positions))
        .route(
            "/metrics",
            get(|State(a): State<App>| async move {
                (
                    [(
                        header::CONTENT_TYPE,
                        "text/plain; version=0.0.4; charset=utf-8",
                    )],
                    crate::metrics::render(&a.bot),
                )
            }),
        )
        .route("/positions/close", axum::routing::post(positions_close))
        .route("/funding", get(funding))
        .route(
            "/control/exchanges",
            get(|| async { Json(json!({"поддерживаемые": data::exchange_ids()})) }),
        )
        .route("/paper/reset", axum::routing::post(reset_account))
        .route("/account/reset", axum::routing::post(reset_account))
        .route("/trading/start", axum::routing::post(trading_start))
        .route("/trading/stop", axum::routing::post(trading_stop))
        .route("/trading/panic", axum::routing::post(trading_panic))
        .route("/settings", get(settings).post(settings_update))
        .route(
            "/exchange/params/schema",
            get(|| async { Json(Value::Object(data::exchange_schema().clone())) }),
        )
        .route(
            "/exchange/params",
            get(exchange_params).post(exchange_params_update),
        )
        .route("/events", get(events))
        .layer(middleware::from_fn_with_state(app.clone(), auth))
        .with_state(app);
    Router::new()
        .route("/health", get(|| async { Json(json!({"status": "ok"})) }))
        .merge(api)
        .fallback(|| async {
            ApiError(
                StatusCode::NOT_FOUND,
                "Этого адреса в Rust-бэкенде пока нет".into(),
            )
        })
        .layer(cors)
}

/// Проверка X-Admin-Token (сравнение без утечки длины по времени не нужно: токен проверяется целиком).
async fn auth(State(app): State<App>, req: Request, next: Next) -> Response {
    if req.method() == Method::OPTIONS
        || app.cfg.token.is_empty()
        || (req.uri().path() == "/metrics" && !app.cfg.metrics_auth)
    {
        return next.run(req).await;
    }
    // X-Admin-Token или Authorization: Bearer (так шлёт токен Prometheus)
    let got = {
        let h = |k: &str| req.headers().get(k).and_then(|v| v.to_str().ok());
        h("x-admin-token")
            .or_else(|| h("authorization").and_then(|v| v.strip_prefix("Bearer ")))
            .unwrap_or("")
            .to_owned()
    };
    if got != app.cfg.token {
        return ApiError(
            StatusCode::UNAUTHORIZED,
            "Неверный или отсутствующий токен".into(),
        )
        .into_response();
    }
    next.run(req).await
}

// ───────────────────────── данные ─────────────────────────

fn control_status_json(bot: &Bot) -> Value {
    let s = bot.snapshot();
    let mut trading = Map::new();
    let mut removable = Map::new();
    for (ex, syms) in &s.selection {
        trading.insert(
            ex.clone(),
            bot.params(ex)
                .get("tradingEnabled")
                .cloned()
                .unwrap_or(Value::Bool(false)),
        );
        removable.insert(
            ex.clone(),
            Value::Object(
                syms.iter()
                    .map(|x| (x.clone(), json!({"можно": true, "причины": []})))
                    .collect(),
            ),
        );
    }
    json!({
        "запущен": bot.running(),
        "выбрано": s.selection,
        "стратегии_монет": s.symbol_strategies,
        "поддерживаемые_биржи": data::exchange_ids(),
        "торговля_включена": trading,
        "убрать": removable,
        "стакан": book_feeds(bot),
        "баны": {},
        "только_выход": {},
    })
}

/// /status в формате Java-бота: признак запуска и по каждой выбранной бирже — подключение, сообщения, символы.
/// «стакан» в /control/status: откуда идёт стакан подключённых бирж (у Rust-бэкенда — только WebSocket, ордеров пока нет).
fn book_feeds(bot: &Bot) -> Value {
    let mut m = Map::new();
    if bot.running() {
        for ex in bot.snapshot().selection.keys() {
            let Some(f) = bot.market.feed(ex).filter(|f| f.unsupported.is_empty()) else {
                continue;
            };
            let feed = if f.connected {
                "websocket"
            } else {
                "нет данных"
            };
            m.insert(
                ex.clone(),
                json!({"feed": feed, "entries": "ордеров пока нет", "exits": "ордеров пока нет",
                "restEntriesBlocked": 0, "restExits": 0}),
            );
        }
    }
    Value::Object(m)
}

/// /exchanges/request-stats в формате Java: режим, рынок и состояние сокета стакана по подключённым биржам.
fn request_stats_json(bot: &Bot) -> Value {
    let mut m = Map::new();
    if bot.running() {
        for ex in bot.snapshot().selection.keys() {
            let Some(f) = bot.market.feed(ex).filter(|f| f.unsupported.is_empty()) else {
                continue;
            };
            let p = bot.params(ex);
            let testnet = p.get("testnet").and_then(Value::as_bool).unwrap_or(false);
            let mode = bot
                .accounts
                .lock()
                .unwrap()
                .get(ex)
                .map_or("PAPER", |a| a.mode());
            m.insert(ex.clone(), json!({
                "mode": mode,
                "modeTitle": match mode {
                    "LIVE" => "реальные ордера (основная сеть)",
                    "TESTNET" => "ордера в тестовую сеть",
                    _ => "бумажная торговля на живом стакане",
                },
                "network": if testnet { "TESTNET" } else { "LIVE" },
                "market": p.get("market").cloned().unwrap_or(Value::Null),
                "marketData": {"ws": {
                    "connected": f.connected, "transport": "tokio-tungstenite", "reconnects": f.reconnects,
                    "lastError": if f.last_error.is_empty() { Value::Null } else { Value::String(f.last_error.clone()) },
                    "bboEnabled": f.bbo_enabled, "bboUpdates": f.bbo_updates, "bboStale": f.stale,
                }},
            }));
        }
    }
    Value::Object(m)
}

async fn request_stats(State(a): State<App>) -> Json<Value> {
    Json(request_stats_json(&a.bot))
}

fn status_json(bot: &Bot) -> Value {
    let mut m = Map::new();
    m.insert("бот_запущен".into(), Value::Bool(bot.running()));
    if bot.running() {
        for ex in bot.snapshot().selection.keys() {
            let f = bot.market.feed(ex).unwrap_or_default();
            let trading = bot
                .params(ex)
                .get("tradingEnabled")
                .cloned()
                .unwrap_or(Value::Bool(false));
            let mut e = json!({
                "торговля_включена": trading,
                "подключение": f.connected,
                "получено_сообщений": f.messages,
                "переподключений": f.reconnects,
                "символы": f.symbols,
                "переподключений_по_тишине": f.silence_reconnects,
                "ошибок_разбора": f.parse_errors,
                "сбросов_перекрещённых": f.crossed_resets,
                "неудач_подряд": f.failures,
                "rest_опрос": f.rest_polling,
                "rest_снимков": f.rest_polls,
            });
            if f.gave_up {
                e["источник_сдался"] = Value::Bool(true);
            }
            let acc_stats = bot
                .accounts
                .lock()
                .unwrap()
                .get(ex)
                .map(|a| (a.stats(), a.error().to_owned()));
            if let Some((stats, err)) = acc_stats {
                if let (Value::Object(m), Value::Object(s)) = (&mut e, stats) {
                    m.extend(s);
                }
                if !err.is_empty() {
                    e["ошибка_счёта"] = Value::String(err);
                }
            }
            if let Some(acc) = bot.account_json(ex) {
                e["тестовый_счёт"] = acc;
            }
            let orph = bot.orphans_json(ex);
            if orph["позиции"] != json!([]) || orph["закрыто"] != json!(0) {
                e["без_стратегии"] = orph;
            }
            if !f.last_error.is_empty() {
                e["ошибка"] = Value::String(f.last_error);
            }
            if !f.rest_error.is_empty() {
                e["ошибка_rest"] = Value::String(f.rest_error);
            }
            if !f.unsupported.is_empty() {
                e["нет_фида"] = Value::String(f.unsupported);
            }
            m.insert(ex.clone(), e);
        }
    }
    Value::Object(m)
}

fn discovery_json(bot: &Bot) -> Value {
    bot.discovery_json()
}

fn topic_json(bot: &Bot, topic: &str) -> Option<Value> {
    Some(match topic {
        "/control/status" => control_status_json(bot),
        "/status" => status_json(bot),
        "/exchanges/request-stats" => request_stats_json(bot),
        "/discovery" => discovery_json(bot),
        _ => return None,
    })
}

// ───────────────────────── обработчики ─────────────────────────

async fn control_status(State(a): State<App>) -> Json<Value> {
    Json(control_status_json(&a.bot))
}

async fn status(State(a): State<App>) -> Json<Value> {
    Json(status_json(&a.bot))
}

async fn discovery_refresh(State(a): State<App>) -> Result<Json<Value>, ApiError> {
    a.bot.discovery_refresh().map_err(bad)?;
    Ok(Json(
        json!({"результат": "ок", "сообщение": "Пересчёт запущен"}),
    ))
}

async fn discovery(State(a): State<App>) -> Json<Value> {
    Json(discovery_json(&a.bot))
}

fn num(q: &HashMap<String, String>, k: &str) -> Result<f64, ApiError> {
    q.get(k).map_or(Ok(0.0), |v| {
        v.trim()
            .parse()
            .map_err(|_| bad(format!("{k}: ожидается число")))
    })
}

/// POST /order — ручной ордер на бумажном счёте: side BUY/SELL, type MARKET/LIMIT, qty, portion или all, price.
async fn order(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    let ex = need(&q, "exchange")?;
    let symbol = need(&q, "symbol")?;
    let side = Side::parse(q.get("side").map_or("BUY", String::as_str))
        .ok_or_else(|| bad("side: BUY или SELL"))?;
    let limit = match q
        .get("type")
        .map_or("MARKET", String::as_str)
        .to_uppercase()
        .as_str()
    {
        "MARKET" => None,
        "LIMIT" => Some(num(&q, "price")?),
        _ => return Err(bad("Неподдерживаемая комбинация параметров")),
    };
    let amount = if q.get("all").is_some_and(|v| v == "true") {
        Amount::All
    } else if num(&q, "portion")? > 0.0 {
        Amount::Portion(num(&q, "portion")?)
    } else {
        Amount::Qty(num(&q, "qty")?)
    };
    let f = a
        .bot
        .place_order(ex, symbol, side, amount, limit)
        .map_err(bad)?;
    Ok(Json(
        json!({"биржа": ex, "orderId": f.order_id, "статус": f.status, "исполнено": f.executed, "средняя_цена": f.avg_price}),
    ))
}

async fn cancel_all(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    let n = a
        .bot
        .cancel_all(need(&q, "exchange")?, need(&q, "symbol")?)
        .map_err(bad)?;
    Ok(ok(format!("Отменено ордеров: {n}")))
}

async fn balances(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        json!({"свободно": a.bot.balances(need(&q, "exchange")?).map_err(bad)?}),
    ))
}

async fn positions(State(a): State<App>, Query(q): Q) -> Json<Value> {
    Json(a.bot.positions_json(target(&q)))
}

async fn positions_close(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        a.bot
            .close_positions(need(&q, "exchange")?, need(&q, "symbol")?)
            .map_err(bad)?,
    ))
}

async fn funding(State(a): State<App>) -> Json<Value> {
    Json(a.bot.funding_json())
}

async fn reset_account(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        a.bot.reset_account(need(&q, "exchange")?).map_err(bad)?,
    ))
}

fn target(q: &HashMap<String, String>) -> &str {
    q.get("exchange").map_or("all", String::as_str)
}

async fn trading_start(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    a.bot.set_trading(target(&q), true).map_err(bad)?;
    Ok(ok("Торговля включена"))
}

async fn trading_stop(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    a.bot.set_trading(target(&q), false).map_err(bad)?;
    Ok(ok("Торговля остановлена"))
}

async fn trading_panic(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    a.bot.panic(target(&q)).map_err(bad)?;
    Ok(ok("Все ордера отменены, торговля остановлена"))
}

async fn strategies(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    let ex = need(&q, "exchange")?;
    Ok(Json(
        json!({"биржа": ex, "стратегии": a.bot.strategies_json(ex).map_err(bad)?}),
    ))
}

async fn arbitrage(State(a): State<App>) -> Json<Value> {
    Json(a.bot.arbitrage_json())
}

async fn market(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    let ex = need(&q, "exchange")?;
    if !a.bot.running() {
        return Err(bad("Соединения не подняты"));
    }
    Ok(Json(a.bot.market.market_json(ex)))
}

/// Параметры из строки запроса без служебных ключей.
fn params_only(q: &HashMap<String, String>, skip: &[&str]) -> Map<String, Value> {
    q.iter()
        .filter(|(k, _)| !skip.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect()
}

async fn select(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    let ex = need(&q, "exchange")?;
    let symbols: Vec<String> = need(&q, "symbols")?
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    let params = params_only(&q, &["exchange", "symbols", "strategy"]);
    a.bot
        .select(
            ex,
            symbols.clone(),
            &params,
            q.get("strategy").map(String::as_str),
        )
        .map_err(bad)?;
    Ok(Json(
        json!({"биржа": ex, "параметры": a.bot.params(ex), "сообщение": format!("Биржа {ex} выбрана с символами {symbols:?}")}),
    ))
}

async fn symbols(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    let ex = need(&q, "exchange")?;
    if let Some(s) = q.get("add") {
        a.bot
            .add_symbol(ex, s, q.get("strategy").map(String::as_str))
            .map_err(bad)?;
    }
    if let Some(s) = q.get("remove") {
        a.bot.remove_symbol(ex, s).map_err(bad)?;
    }
    Ok(Json(
        json!({ex: a.bot.snapshot().selection.get(ex).cloned().unwrap_or_default()}),
    ))
}

/// Вывести монету из торговли. Ордеров и позиций в каркасе нет — монета просто убирается из выбранных.
async fn retire(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    let ex = need(&q, "exchange")?;
    let sym = need(&q, "symbol")?;
    a.bot.remove_symbol(ex, sym).map_err(bad)?;
    Ok(Json(
        json!({"exchange": ex, "symbol": sym.to_uppercase(), "status": "убрана из выбранных"}),
    ))
}

async fn deselect(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    let ex = need(&q, "exchange")?;
    a.bot.deselect(ex).map_err(bad)?;
    Ok(ok(format!("Биржа {ex} убрана из выбора")))
}

async fn start(State(a): State<App>) -> Result<Json<Value>, ApiError> {
    a.bot.start().map_err(bad)?;
    Ok(ok(
        "Соединения подняты: стаканы по WebSocket (ордеров в Rust-бэкенде пока нет)",
    ))
}

async fn stop(State(a): State<App>) -> Json<Value> {
    a.bot.stop();
    ok("Соединения остановлены")
}

fn settings_json(bot: &Bot) -> Value {
    let sch = data::settings_schema();
    json!({"настройки": bot.settings(), "описание": sch["описание"], "фильтры_стратегий": sch["фильтры_стратегий"]})
}

async fn settings(State(a): State<App>) -> Json<Value> {
    Json(settings_json(&a.bot))
}

async fn settings_update(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    a.bot.update_settings(&params_only(&q, &[])).map_err(bad)?;
    Ok(Json(settings_json(&a.bot)))
}

fn exchange_params_json(bot: &Bot, ex: &str) -> Value {
    json!({
        "биржа": ex,
        "выбрана": bot.snapshot().selection.contains_key(ex),
        "testnet_доступен": data::WITH_TESTNET.contains(&ex),
        "параметры": bot.params(ex),
    })
}

async fn exchange_params(State(a): State<App>, Query(q): Q) -> Result<Json<Value>, ApiError> {
    let ex = need(&q, "exchange")?;
    if !data::exchange_ids().iter().any(|x| x == ex) {
        return Err(bad(format!("Неизвестная биржа: {ex}")));
    }
    Ok(Json(exchange_params_json(&a.bot, ex)))
}

async fn exchange_params_update(
    State(a): State<App>,
    Query(q): Q,
) -> Result<Json<Value>, ApiError> {
    let ex = need(&q, "exchange")?;
    a.bot
        .update_params(ex, &params_only(&q, &["exchange"]))
        .map_err(bad)?;
    Ok(Json(exchange_params_json(&a.bot, ex)))
}

/// GET /events?topics=... — Server-Sent Events: по каждой теме сначала текущие данные, потом только изменения
/// (проверка раз в 300 мс). Событие: `{"topic": "...", "data": {...}}`.
async fn events(
    State(a): State<App>,
    Query(q): Q,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let topics: Vec<String> = q
        .get("topics")
        .map(String::as_str)
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|t| EVENT_TOPICS.contains(t))
        .map(str::to_owned)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if topics.is_empty() {
        return Err(bad(
            "topics: укажите темы через запятую, например /control/status,/discovery",
        ));
    }
    let bot = a.bot.clone();
    let st = stream::unfold(
        (
            bot,
            topics,
            HashMap::<String, String>::new(),
            Vec::<Event>::new(),
            true,
        ),
        |(bot, topics, mut last, mut queue, mut first)| async move {
            loop {
                if let Some(e) = queue.pop() {
                    return Some((Ok(e), (bot, topics, last, queue, first)));
                }
                if !first {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                first = false;
                for t in &topics {
                    let Some(v) = topic_json(&bot, t) else {
                        continue;
                    };
                    let s = v.to_string();
                    if last.get(t) == Some(&s) {
                        continue;
                    }
                    last.insert(t.clone(), s);
                    queue.push(Event::default().data(json!({"topic": t, "data": v}).to_string()));
                }
            }
        },
    );
    Ok(Sse::new(st).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn app(token: &str) -> (tempfile::TempDir, Router) {
        let dir = tempfile::tempdir().unwrap();
        let bot = Arc::new(Bot::load(dir.path().join("s.json")));
        (
            dir,
            router(
                bot,
                AdminConfig {
                    token: token.into(),
                    cors_origins: vec![],
                    metrics_auth: false,
                },
            ),
        )
    }

    /// GET текстом (для /metrics).
    async fn text(r: &Router, uri: &str, token: &str) -> (StatusCode, String) {
        let req = axum::http::Request::builder()
            .uri(uri)
            .header("x-admin-token", token)
            .body(Body::empty())
            .unwrap();
        let res = r.clone().oneshot(req).await.unwrap();
        let st = res.status();
        let b = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        (st, String::from_utf8(b.to_vec()).unwrap())
    }

    /// /metrics открыт для Prometheus без токена (METRICS_AUTH не задан), остальное — под токеном.
    #[tokio::test]
    async fn metrics_without_token() {
        let (_d, r) = app("secret");
        let (c, t) = text(&r, "/metrics", "").await;
        assert_eq!(c, StatusCode::OK);
        assert!(t.contains("# TYPE hft_connected gauge"), "{t}");
        assert_eq!(text(&r, "/status", "").await.0, StatusCode::UNAUTHORIZED);
        // токен Prometheus — Authorization: Bearer
        let req = axum::http::Request::builder()
            .uri("/status")
            .header("authorization", "Bearer secret")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            r.clone().oneshot(req).await.unwrap().status(),
            StatusCode::OK
        );
    }

    async fn call(r: &Router, method: &str, uri: &str, token: &str) -> (StatusCode, Value) {
        let req = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header("X-Admin-Token", token)
            .body(Body::empty())
            .unwrap();
        let res = r.clone().oneshot(req).await.unwrap();
        let code = res.status();
        let b = res.into_body().collect().await.unwrap().to_bytes();
        (code, serde_json::from_slice(&b).unwrap_or(Value::Null))
    }

    #[tokio::test]
    async fn token_required() {
        let (_d, r) = app("t");
        assert_eq!(
            call(&r, "GET", "/control/status", "bad").await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(call(&r, "GET", "/health", "").await.0, StatusCode::OK);
        let (c, v) = call(&r, "GET", "/control/status", "t").await;
        assert_eq!(c, StatusCode::OK);
        assert_eq!(v["запущен"], Value::Bool(false));
    }

    #[tokio::test]
    async fn select_and_retire_flow() {
        let (_d, r) = app("t");
        let (c, _) = call(&r, "POST", "/control/select?exchange=binance&symbols=AAAUSDT,BBBUSDT&strategy=spread-capture&scJumpTicks=1", "t").await;
        assert_eq!(c, StatusCode::OK);
        let (_, s) = call(&r, "GET", "/control/status", "t").await;
        assert_eq!(s["выбрано"]["binance"], json!(["AAAUSDT", "BBBUSDT"]));
        assert_eq!(
            s["стратегии_монет"]["binance"]["AAAUSDT"],
            json!(["spread-capture"])
        );
        let (_, p) = call(&r, "GET", "/exchange/params?exchange=binance", "t").await;
        assert_eq!(p["параметры"]["scJumpTicks"], json!(1));
        assert_eq!(p["testnet_доступен"], Value::Bool(true));
        call(
            &r,
            "POST",
            "/control/retire?exchange=binance&symbol=AAAUSDT",
            "t",
        )
        .await;
        let (_, s) = call(&r, "GET", "/control/status", "t").await;
        assert_eq!(s["выбрано"]["binance"], json!(["BBBUSDT"]));
        assert_eq!(
            call(&r, "POST", "/control/start", "t").await.0,
            StatusCode::OK
        );
        assert_eq!(
            call(&r, "GET", "/status", "t").await.1["бот_запущен"],
            Value::Bool(true)
        );
    }

    #[tokio::test]
    async fn errors_in_java_format() {
        let (_d, r) = app("");
        let (c, v) = call(
            &r,
            "POST",
            "/exchange/params?exchange=binance&scJumpTicks=1000",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::BAD_REQUEST);
        assert_eq!(v["результат"], json!("ошибка"));
        assert!(v["сообщение"].as_str().unwrap().contains("scJumpTicks"));
        assert_eq!(
            call(&r, "GET", "/no-such-route", "").await.0,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn events_first_message_has_data() {
        let (_d, r) = app("");
        let req = axum::http::Request::builder()
            .uri("/events?topics=/status,/settings")
            .body(Body::empty())
            .unwrap();
        let res = r.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let mut body = res.into_body();
        let frame = body.frame().await.unwrap().unwrap();
        let text = String::from_utf8(frame.into_data().unwrap().to_vec()).unwrap();
        assert!(
            text.contains("\"topic\":\"/status\"") && text.contains("бот_запущен"),
            "{text}"
        );
        assert_eq!(
            call(&r, "GET", "/events?topics=/settings", "").await.0,
            StatusCode::BAD_REQUEST
        );
    }

    /// Сквозной сценарий бумажной торговли: стакан по WS → включить торговлю → рыночная покупка →
    /// лимитная продажа исполняется, когда цена дошла → паника снимает заявки.
    #[tokio::test]
    async fn paper_trading_flow() {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::Message;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let price = Arc::new(std::sync::Mutex::new(100.0_f64));
        let px = price.clone();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for i in 1..400u64 {
                let p = *px.lock().unwrap();
                let msg = format!(
                    r#"{{"stream":"btcusdt@depth20@100ms","data":{{"lastUpdateId":{i},"bids":[["{}","10"]],"asks":[["{}","10"]]}}}}"#,
                    p - 0.5,
                    p + 0.5
                );
                if ws.send(Message::text(msg)).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = listener.accept().await;
            }
        });
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=binance&symbols=BTCUSDT&market=spot&wsUrl=ws://{addr}/stream&maxPositionQuote=1000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        assert_eq!(
            call(&r, "POST", "/control/start", "").await.0,
            StatusCode::OK
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        // торговля выключена — отказ
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=binance&symbol=BTCUSDT&side=BUY&type=MARKET&qty=1",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::BAD_REQUEST, "{v}");
        assert!(v["сообщение"].as_str().unwrap().contains("выключена"));
        call(&r, "POST", "/trading/start?exchange=binance", "").await;
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=binance&symbol=BTCUSDT&side=BUY&type=MARKET&qty=2",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        assert_eq!(v["статус"], json!("FILLED"));
        assert_eq!(v["средняя_цена"], json!(100.5));
        let (_, b) = call(&r, "GET", "/balances?exchange=binance", "").await;
        assert_eq!(b["свободно"]["BTC"], json!(2.0));
        // лимитная продажа выше рынка ждёт, цена дошла — исполнилась мейкером
        let (_, v) = call(
            &r,
            "POST",
            "/order?exchange=binance&symbol=BTCUSDT&side=SELL&type=LIMIT&qty=1&price=103",
            "",
        )
        .await;
        assert_eq!(v["статус"], json!("NEW"), "{v}");
        *price.lock().unwrap() = 104.0;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let (_, s) = call(&r, "GET", "/status", "").await;
        let acc = &s["binance"]["тестовый_счёт"];
        assert_eq!(acc["openOrders"], json!(0), "{s}");
        assert_eq!(acc["closedTrades"], json!(1));
        assert!(acc["realizedPnl"].as_f64().unwrap() > 0.0);
        assert_eq!(s["binance"]["ордеров_отклонено"], json!(1));
        // паника: заявки сняты, торговля выключена
        call(
            &r,
            "POST",
            "/order?exchange=binance&symbol=BTCUSDT&side=BUY&type=LIMIT&qty=1&price=50",
            "",
        )
        .await;
        call(&r, "POST", "/trading/panic", "").await;
        let (_, s) = call(&r, "GET", "/status", "").await;
        assert_eq!(s["binance"]["тестовый_счёт"]["openOrders"], json!(0));
        assert_eq!(s["binance"]["торговля_включена"], json!(false));
        // сброс счёта
        let (_, a) = call(&r, "POST", "/paper/reset?exchange=binance", "").await;
        assert_eq!(a["closedTrades"], json!(0));
        call(&r, "POST", "/control/stop", "").await;
    }

    /// «Сбор спреда» через API: цена ходит 100 → 97 → 103 со спредом 2% — стратегия покупает по биду, продаёт по аску
    /// дороже средней покупки и выходит в плюс; /strategies показывает сделки, результат и очередь заявок.
    #[tokio::test]
    async fn spread_capture_flow() {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::Message;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for i in 1..2000u64 {
                let p = [100.0, 97.0, 103.0][((i / 15) % 3) as usize];
                let msg = format!(
                    r#"{{"stream":"xyzusdt@depth20@100ms","data":{{"lastUpdateId":{i},"bids":[["{}","100"]],"asks":[["{}","100"]]}}}}"#,
                    p - 1.0,
                    p + 1.0
                );
                if ws.send(Message::text(msg)).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = listener.accept().await;
            }
        });
        // REST биржи: правила пары и оборот минутных свечей
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let rest = Router::new()
            .route("/api/v3/exchangeInfo", get(|| async { Json(json!({"symbols": [{"symbol": "XYZUSDT", "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01"}, {"filterType": "LOT_SIZE", "stepSize": "0.0001", "minQty": "0.0001"},
                {"filterType": "NOTIONAL", "minNotional": "1"}]}]})) }))
            .route("/api/v3/klines", get(move || async move {
                Json(json!((0..6).map(|i| json!([0, "1", "1", "1", "1", "1", now - 60_000 * (6 - i), "500", 5])).collect::<Vec<_>>()))
            }));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=binance&symbols=XYZUSDT&market=spot&wsUrl=ws://{addr}/stream&restUrl=http://{raddr}&strategy=spread-capture\
            &scJumpTicks=1&scMaxJumpsPerMin=600&scMinTradeMinutes=4&scMinMinuteVolumeMultiple=2&scRequoteMs=200&scStopLossPercent=50&scMaxDropPercent=0&scMinBookLevels=0&scMinDepthMultiple=0&scMaxTopChangesPerSec=0&maxOrdersPerMinute=10000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let st = loop {
            let (_, v) = call(&r, "GET", "/strategies?exchange=binance", "").await;
            let sc = v["стратегии"]["spreadCapture"].clone();
            if sc["trades"].as_u64().unwrap_or(0) >= 2 {
                break sc;
            }
            assert!(tokio::time::Instant::now() < deadline, "нет сделок: {v}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        assert!(st["pnl"].as_f64().unwrap() > 0.0, "{st}");
        assert_eq!(st["symbols"][0]["symbol"], json!("XYZUSDT"));
        assert_eq!(st["symbols"][0]["tradeMinutes"], json!("5 из 5"), "{st}");
        assert!(st["symbols"][0]["jumps"].as_u64().unwrap() > 0, "{st}");
        assert!(!st["symbols"][0]["decision"].as_str().unwrap().is_empty());
        // выключили торговлю — заявки сняты, купленное продано
        call(&r, "POST", "/trading/stop", "").await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let (_, s) = call(&r, "GET", "/status", "").await;
        assert_eq!(s["binance"]["тестовый_счёт"]["openOrders"], json!(0), "{s}");
        let (_, b) = call(&r, "GET", "/balances?exchange=binance", "").await;
        assert!(b["свободно"].get("XYZ").is_none(), "{b}");
        call(&r, "POST", "/control/stop", "").await;
    }

    /// Живой режим (testnet) против макета REST Binance: макет проверяет подпись каждого запроса, ведёт балансы и
    /// статусы ордеров. Рыночная покупка учитывается, лимитная продажа стоит, паника снимает её на «бирже».
    /// Межбиржевой круг с реальным переводом: на Bybit дешевле, на Binance дороже, монеты на Binance нет —
    /// покупка на Bybit, сети обеих бирж, адрес депозита Binance, вывод с Bybit по самой дешёвой сети, приход
    /// (рост баланса на Binance) и продажа на Binance.
    #[tokio::test(flavor = "multi_thread")]
    async fn spot_arb_transfer_round_against_mock_exchanges() {
        use axum::extract::RawQuery;
        use axum::http::HeaderMap;
        use futures_util::SinkExt;
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        const BN: &str = "test-secret";
        const BY: &str = "bybit-secret";
        #[derive(Default)]
        struct Ex {
            /// X на Binance (приходит переводом) и USDT
            bn_x: f64,
            bn_usdt: f64,
            by_x: f64,
            by_usdt: f64,
            by_orders: Vec<(String, f64)>,
            withdraw: Option<Value>,
            bn_sold: f64,
            bad_sig: u32,
        }
        let ex = Arc::new(Mutex::new(Ex {
            bn_usdt: 1000.0,
            by_usdt: 1000.0,
            ..Default::default()
        }));
        fn param(q: &str, k: &str) -> Option<String> {
            q.split('&').find_map(|kv| {
                kv.split_once('=')
                    .filter(|(a, _)| *a == k)
                    .map(|(_, v)| v.to_owned())
            })
        }
        fn bn_ok(q: &str, h: &HeaderMap) -> bool {
            let (body, sig) = q.rsplit_once("&signature=").unwrap_or((q, ""));
            h.get("x-mbx-apikey").is_some_and(|k| k == "test-key")
                && crate::signed::sign(BN, body) == sig
        }
        fn by_ok(h: &HeaderMap, payload: &str) -> bool {
            let g = |k: &str| {
                h.get(k)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned()
            };
            g("x-bapi-api-key") == "bybit-key"
                && crate::signed::sign(
                    BY,
                    &format!(
                        "{}bybit-key{}{payload}",
                        g("x-bapi-timestamp"),
                        g("x-bapi-recv-window")
                    ),
                ) == g("x-bapi-sign")
        }
        // Binance (спот, дороже)
        let (e1, e2, e3, e4) = (ex.clone(), ex.clone(), ex.clone(), ex.clone());
        let bn = Router::new()
            .route("/api/v3/time", get(|| async { Json(json!({"serverTime": now_ms()})) }))
            .route("/api/v3/exchangeInfo", get(|| async { Json(json!({"symbols": [{"symbol": "XUSDT", "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01"}, {"filterType": "LOT_SIZE", "stepSize": "0.01", "minQty": "0.01"}]}]})) }))
            .route("/api/v3/klines", get(|| async { Json(json!([])) }))
            .route("/api/v3/account", get(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let mut x = e1.lock().unwrap();
                if !bn_ok(&q.unwrap_or_default(), &h) { x.bad_sig += 1; }
                Json(json!({"balances": [{"asset": "USDT", "free": format!("{}", x.bn_usdt)}, {"asset": "X", "free": format!("{}", x.bn_x)}]}))
            }))
            .route("/sapi/v1/capital/config/getall", get(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                if !bn_ok(&q.unwrap_or_default(), &h) { e2.lock().unwrap().bad_sig += 1; }
                Json(json!([{"coin": "X", "networkList": [
                    {"network": "ETH", "withdrawFee": "0.5", "withdrawMin": "1", "withdrawEnable": true, "depositEnable": true},
                    {"network": "TRX", "withdrawFee": "0.2", "withdrawMin": "1", "withdrawEnable": true, "depositEnable": true}]}]))
            }))
            .route("/sapi/v1/capital/deposit/address", get(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                if !bn_ok(&q, &h) { e3.lock().unwrap().bad_sig += 1; }
                assert_eq!(param(&q, "network").as_deref(), Some("TRX"));
                Json(json!({"coin": "X", "address": "TXdepositAddr", "tag": ""}))
            }))
            .route("/api/v3/order", axum::routing::post(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let mut x = e4.lock().unwrap();
                if !bn_ok(&q, &h) { x.bad_sig += 1; }
                let qty: f64 = param(&q, "quantity").unwrap().parse().unwrap();
                assert_eq!(param(&q, "side").as_deref(), Some("SELL"));
                x.bn_x -= qty;
                x.bn_usdt += qty * 10.3;
                x.bn_sold += qty;
                Json(json!({"orderId": 77, "status": "FILLED", "executedQty": format!("{qty}"), "cummulativeQuoteQty": format!("{}", qty * 10.3)}))
            }));
        // Bybit (спот, дешевле)
        let (b1, b2, b3, b4, b5) = (ex.clone(), ex.clone(), ex.clone(), ex.clone(), ex.clone());
        let by = Router::new()
            .route("/v5/market/time", get(|| async { Json(json!({"retCode": 0, "time": now_ms()})) }))
            .route("/v5/account/wallet-balance", get(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let mut x = b1.lock().unwrap();
                if !by_ok(&h, &q.unwrap_or_default()) { x.bad_sig += 1; }
                Json(json!({"retCode": 0, "result": {"list": [{"coin": [
                    {"coin": "USDT", "walletBalance": format!("{}", x.by_usdt), "locked": "0"},
                    {"coin": "X", "walletBalance": format!("{}", x.by_x), "locked": "0"}]}]}}))
            }))
            .route("/v5/order/create", axum::routing::post(move |h: HeaderMap, body: String| async move {
                let mut x = b2.lock().unwrap();
                if !by_ok(&h, &body) { x.bad_sig += 1; }
                let b: Value = serde_json::from_str(&body).unwrap();
                assert_eq!(b["side"], "Buy");
                let qty: f64 = b["qty"].as_str().unwrap().parse().unwrap();
                x.by_x += qty;
                x.by_usdt -= qty * 10.0;
                let id = format!("by-{}", x.by_orders.len());
                x.by_orders.push((id.clone(), qty));
                Json(json!({"retCode": 0, "result": {"orderId": id}}))
            }))
            .route("/v5/order/realtime", get(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let x = b3.lock().unwrap();
                let _ = by_ok(&h, &q);
                let id = param(&q, "orderId").unwrap();
                let o = x.by_orders.iter().find(|o| o.0 == id).unwrap();
                Json(json!({"retCode": 0, "result": {"list": [{"orderId": o.0, "orderStatus": "Filled",
                    "cumExecQty": format!("{}", o.1), "cumExecValue": format!("{}", o.1 * 10.0)}]}}))
            }))
            .route("/v5/asset/coin/query-info", get(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                if !by_ok(&h, &q.unwrap_or_default()) { b4.lock().unwrap().bad_sig += 1; }
                Json(json!({"retCode": 0, "result": {"rows": [{"coin": "X", "chains": [
                    {"chain": "ETH", "withdrawFee": "0.3", "withdrawMin": "1", "chainWithdraw": "1", "chainDeposit": "1"},
                    {"chain": "TRX", "withdrawFee": "0.1", "withdrawMin": "1", "chainWithdraw": "1", "chainDeposit": "1"}]}]}}))
            }))
            .route("/v5/asset/withdraw/create", axum::routing::post(move |h: HeaderMap, body: String| async move {
                let mut x = b5.lock().unwrap();
                if !by_ok(&h, &body) { x.bad_sig += 1; }
                let b: Value = serde_json::from_str(&body).unwrap();
                let amount: f64 = b["amount"].as_str().unwrap().parse().unwrap();
                x.by_x -= amount;
                x.bn_x += amount - 0.1; // пришло на Binance за вычетом комиссии сети
                x.withdraw = Some(b);
                Json(json!({"retCode": 0, "result": {"id": "w-1"}}))
            }));
        let mut addrs = Vec::new();
        for router in [bn, by] {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.push(l.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(l, router).await.unwrap() });
        }
        // стаканы: Binance 10.30/10.31, Bybit 9.99/10.00
        let mut feeds = Vec::new();
        for (binance, bid, ask) in [(true, "10.3", "10.31"), (false, "9.99", "10")] {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            feeds.push(l.local_addr().unwrap());
            tokio::spawn(async move {
                let (tcp, _) = l.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
                for i in 1..2000u64 {
                    let m = if binance {
                        format!(
                            r#"{{"stream":"xusdt@depth20@100ms","data":{{"lastUpdateId":{i},"bids":[["{bid}","100"]],"asks":[["{ask}","100"]]}}}}"#
                        )
                    } else {
                        format!(
                            r#"{{"topic":"orderbook.50.XUSDT","type":"snapshot","data":{{"s":"XUSDT","b":[["{bid}","100"]],"a":[["{ask}","100"]],"u":{i},"seq":{i}}}}}"#
                        )
                    };
                    if ws.send(Message::text(m)).await.is_err() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
                loop {
                    let _ = l.accept().await;
                }
            });
        }
        // переопределения окружения для этого теста (потокобезопасно)
        crate::util::set_env("BINANCE_TESTNET_API_KEY", "test-key");
        crate::util::set_env("BINANCE_TESTNET_API_SECRET", BN);
        crate::util::set_env("BYBIT_TESTNET_API_KEY", "bybit-key");
        crate::util::set_env("BYBIT_TESTNET_API_SECRET", BY);
        let (_d, r) = app("");
        for (exn, rest, feed) in [
            ("binance", addrs[0], format!("ws://{}/stream", feeds[0])),
            ("bybit", addrs[1], format!("ws://{}", feeds[1])),
        ] {
            let sel = format!(
                "/control/select?exchange={exn}&symbols=XUSDT&market=spot&testnet=true&wsUrl={feed}&restUrl=http://{rest}&spreadCaptureEnabled=false&statArbEnabled=false&maxPositionQuote=1000"
            );
            assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        }
        let (c, v) = call(&r, "POST", "/settings?spotArbLiveWithdraw=true&spotArbOrderQuote=100&spotArbMaxBookAgeMs=60000&spotArbCooldownMs=600000", "").await;
        assert_eq!(c, StatusCode::OK, "{v}");
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let until = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let (_, a) = call(&r, "GET", "/arbitrage", "").await;
            if a["spotPrice"]["rounds"] == json!(1) {
                assert!(a["spotPrice"]["pnl"].as_f64().unwrap() > 0.5, "{a}");
                break;
            }
            assert!(tokio::time::Instant::now() < until, "круг не закрыт: {a}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        {
            let x = ex.lock().unwrap();
            let w = x.withdraw.clone().expect("вывод был");
            assert_eq!(
                (
                    w["coin"].as_str(),
                    w["chain"].as_str(),
                    w["address"].as_str()
                ),
                (Some("X"), Some("TRX"), Some("TXdepositAddr")),
                "самая дешёвая общая сеть"
            );
            assert_eq!(w["amount"], json!("10"));
            assert!(
                (x.bn_sold - 9.9).abs() < 1e-9,
                "продано пришедшее: {}",
                x.bn_sold
            );
            assert_eq!(x.bad_sig, 0, "все подписи верны");
        }
        call(&r, "POST", "/control/stop", "").await;
    }

    /// Живой спот Aster против макета: каждая подпись EIP-712 проверяется восстановлением адреса агента (signer),
    /// рыночная покупка, поток событий по listenKey, исполнение лимитной заявки из события executionReport.
    #[tokio::test(flavor = "multi_thread")]
    async fn live_aster_spot_against_mock() {
        use axum::extract::RawQuery;
        use futures_util::SinkExt;
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        const KEY: &str = "0x0000000000000000000000000000000000000000000000000000000000000003";
        #[derive(Default)]
        struct Ex {
            usdt: f64,
            x: f64,
            orders: Vec<(u64, String)>,
            bad_sig: u32,
            nonces: Vec<u64>,
        }
        let ex = Arc::new(Mutex::new(Ex {
            usdt: 1000.0,
            ..Default::default()
        }));
        let agent = crate::evm::Wallet::from_hex(KEY).unwrap().address;
        // подпись — по строке до &signature=; signer — адрес агента, user — основной кошелёк
        let check = {
            let agent = agent.clone();
            move |q: &str| -> bool {
                let (msg, sig) = q.rsplit_once("&signature=").unwrap_or((q, ""));
                let p = |k: &str| {
                    msg.split('&')
                        .find_map(|kv| kv.strip_prefix(&format!("{k}=")))
                        .unwrap_or("")
                        .to_owned()
                };
                p("user") == "0xmainwallet"
                    && p("signer") == agent
                    && crate::evm::recover(&crate::aster_api::Client::digest(true, msg), sig)
                        .ok()
                        .as_deref()
                        == Some(agent.as_str())
            }
        };
        fn param(q: &str, k: &str) -> Option<String> {
            q.split('&').find_map(|kv| {
                kv.split_once('=')
                    .filter(|(a, _)| *a == k)
                    .map(|(_, v)| v.to_owned())
            })
        }
        let (c1, c2, c3, c4) = (check.clone(), check.clone(), check.clone(), check.clone());
        let (e1, e2, e3, e4) = (ex.clone(), ex.clone(), ex.clone(), ex.clone());
        let rest = Router::new()
            .route("/api/v3/account", get(move |RawQuery(q): RawQuery| async move {
                let q = q.unwrap_or_default();
                let mut x = e1.lock().unwrap();
                if !c1(&q) { x.bad_sig += 1; }
                x.nonces.push(param(&q, "nonce").unwrap().parse().unwrap());
                Json(json!({"balances": [{"asset": "USDT", "free": format!("{}", x.usdt)}, {"asset": "X", "free": format!("{}", x.x)}]}))
            }))
            .route("/api/v3/listenKey", axum::routing::post(move |RawQuery(q): RawQuery| async move {
                if !c2(&q.unwrap_or_default()) { e2.lock().unwrap().bad_sig += 1; }
                Json(json!({"listenKey": "ak1"}))
            }))
            .route("/api/v3/order", axum::routing::post(move |RawQuery(q): RawQuery| async move {
                let q = q.unwrap_or_default();
                let mut x = e3.lock().unwrap();
                if !c3(&q) { x.bad_sig += 1; }
                let qty: f64 = param(&q, "quantity").unwrap().parse().unwrap();
                let id = 900 + x.orders.len() as u64;
                x.orders.push((id, param(&q, "newClientOrderId").unwrap()));
                if param(&q, "type").as_deref() == Some("MARKET") {
                    x.usdt -= qty * 10.0;
                    x.x += qty;
                    Json(json!({"orderId": id, "status": "FILLED", "executedQty": format!("{qty}"), "cummulativeQuoteQty": format!("{}", qty * 10.0)}))
                } else {
                    Json(json!({"orderId": id, "status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0"}))
                }
            }).get(move |RawQuery(q): RawQuery| async move {
                let q = q.unwrap_or_default();
                if !c4(&q) { e4.lock().unwrap().bad_sig += 1; }
                let id: u64 = param(&q, "orderId").unwrap().parse().unwrap();
                Json(json!({"orderId": id, "status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0"}))
            }));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        let wl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = wl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = wl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for i in 1..500u64 {
                let m = format!(
                    r#"{{"stream":"xusdt@depth20@100ms","data":{{"lastUpdateId":{i},"bids":[["9.99","100"]],"asks":[["10","100"]]}}}}"#
                );
                if ws.send(Message::text(m)).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = wl.accept().await;
            }
        });
        let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let sl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let saddr = sl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = sl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while let Some(e) = ev_rx.recv().await {
                if ws.send(Message::text(e)).await.is_err() {
                    break;
                }
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = sl.accept().await;
            }
        });
        // переопределения окружения для этого теста (потокобезопасно)
        crate::util::set_env("ASTER_STREAM_URL", &format!("ws://{saddr}"));
        crate::util::set_env("ASTER_TESTNET_API_KEY", "0xmainwallet");
        crate::util::set_env("ASTER_TESTNET_API_SECRET", KEY);
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=aster&symbols=XUSDT&market=spot&testnet=true&wsUrl=ws://{waddr}/stream&restUrl=http://{raddr}&spreadCaptureEnabled=false&statArbEnabled=false&maxPositionQuote=1000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=aster", "").await;
            let (_, s) = call(&r, "GET", "/status", "").await;
            if b["свободно"]["USDT"] == json!(1000.0)
                && s["aster"]["поток_событий_счёта"] == json!(true)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "баланс или поток: {b} {s}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=aster&symbol=XUSDT&side=BUY&type=MARKET&qty=2",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        let (_, v) = call(
            &r,
            "POST",
            "/order?exchange=aster&symbol=XUSDT&side=BUY&type=LIMIT&qty=1&price=9.5",
            "",
        )
        .await;
        assert_eq!(v["статус"], json!("NEW"), "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        let (oid, client) = loop {
            if let Some(o) = ex.lock().unwrap().orders.get(1).cloned() {
                break o;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "лимитная заявка не дошла"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        ev_tx.send(json!({"e": "executionReport", "s": "XUSDT", "c": client, "C": "", "S": "BUY", "X": "FILLED", "i": oid, "z": "1", "Z": "9.5"}).to_string()).unwrap();
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            let ac = &a["aster"]["тестовый_счёт"];
            if ac["openOrders"] == json!(0)
                && a["aster"]["событий_счёта"].as_u64().unwrap_or(0) >= 1
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "событие не учтено: {a}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        {
            let x = ex.lock().unwrap();
            assert_eq!(x.bad_sig, 0, "все подписи EIP-712 верны");
            assert!(x.nonces.windows(2).all(|w| w[1] > w[0]), "nonce растёт");
        }
        call(&r, "POST", "/control/stop", "").await;
    }

    /// Живые перпы OKX против макета: подпись OK-ACCESS-SIGN каждого запроса, демо-заголовок, размеры контрактов,
    /// объём ордера в контрактах (монеты / ctVal), вход в приватный поток (подпись login), события orders и positions.
    #[tokio::test(flavor = "multi_thread")]
    async fn live_okx_perp_against_mock() {
        use axum::http::HeaderMap;
        use futures_util::{SinkExt, StreamExt};
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        const SECRET: &str = "okx-secret";
        #[derive(Default)]
        struct Ex {
            orders: Vec<Value>,
            leverage: Vec<Value>,
            bad: u32,
            pos: f64,
        }
        let ex = Arc::new(Mutex::new(Ex::default()));
        // подпись: время + метод + путь_с_query + тело; ключ, passphrase и демо-заголовок
        fn ok(h: &HeaderMap, method: &str, path: &str, body: &str) -> bool {
            let g = |k: &str| {
                h.get(k)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned()
            };
            g("ok-access-key") == "okx-key"
                && g("ok-access-passphrase") == "pass"
                && g("x-simulated-trading") == "1"
                && crate::util::hmac_b64(
                    SECRET,
                    &format!("{}{method}{path}{body}", g("ok-access-timestamp")),
                ) == g("ok-access-sign")
        }
        use axum::extract::OriginalUri;
        let (e1, e2, e3, e4, e5) = (ex.clone(), ex.clone(), ex.clone(), ex.clone(), ex.clone());
        let rest = Router::new()
            .route("/api/v5/public/instruments", get(|| async { Json(json!({"code": "0", "data": [{"instId": "BTC-USDT-SWAP", "ctType": "linear", "ctVal": "0.01", "lotSz": "1"}]})) }))
            .route("/api/v5/public/funding-rate", get(|| async { Json(json!({"code": "0", "data": [{"fundingRate": "0.0001", "fundingTime": "9999999999999"}]})) }))
            .route("/api/v5/account/balance", get(move |h: HeaderMap, u: OriginalUri| async move {
                if !ok(&h, "GET", &u.0.to_string(), "") { e1.lock().unwrap().bad += 1; }
                Json(json!({"code": "0", "data": [{"details": [{"ccy": "USDT", "availBal": "900", "availEq": "1000"}]}]}))
            }))
            .route("/api/v5/account/positions", get(move |h: HeaderMap, u: OriginalUri| async move {
                let mut x = e2.lock().unwrap();
                if !ok(&h, "GET", &u.0.to_string(), "") { x.bad += 1; }
                Json(json!({"code": "0", "data": [{"instId": "BTC-USDT-SWAP", "pos": format!("{}", x.pos), "avgPx": "100", "posSide": "net"}]}))
            }))
            .route("/api/v5/account/set-leverage", axum::routing::post(move |h: HeaderMap, body: String| async move {
                let mut x = e3.lock().unwrap();
                if !ok(&h, "POST", "/api/v5/account/set-leverage", &body) { x.bad += 1; }
                x.leverage.push(serde_json::from_str(&body).unwrap());
                Json(json!({"code": "0", "data": [{}]}))
            }))
            .route("/api/v5/trade/order", axum::routing::post(move |h: HeaderMap, body: String| async move {
                let mut x = e4.lock().unwrap();
                if !ok(&h, "POST", "/api/v5/trade/order", &body) { x.bad += 1; }
                let b: Value = serde_json::from_str(&body).unwrap();
                let id = format!("o{}", x.orders.len());
                let c: f64 = b["sz"].as_str().unwrap().parse().unwrap();
                x.pos += if b["side"] == "sell" { -c } else { c };
                x.orders.push(b);
                Json(json!({"code": "0", "data": [{"ordId": id, "sCode": "0"}]}))
            }).get(move |h: HeaderMap, u: OriginalUri| async move {
                let x = e5.lock().unwrap();
                let _ = ok(&h, "GET", &u.0.to_string(), "");
                let id = u.0.query().unwrap_or("").split('&').find_map(|kv| kv.strip_prefix("ordId=")).unwrap_or("").to_owned();
                let n: usize = id.trim_start_matches('o').parse().unwrap_or(0);
                let c = x.orders.get(n).map(|o| o["sz"].as_str().unwrap().to_owned()).unwrap_or_default();
                Json(json!({"code": "0", "data": [{"instId": "BTC-USDT-SWAP", "ordId": id, "state": "filled", "accFillSz": c, "avgPx": "100"}]}))
            }));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        // публичный стакан SWAP (объём — контракты)
        let wl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = wl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = wl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for i in 1..500u64 {
                let m = format!(
                    r#"{{"arg":{{"channel":"books","instId":"BTC-USDT-SWAP"}},"action":"snapshot","data":[{{"bids":[["99.9","10000","0","1"]],"asks":[["100","10000","0","1"]],"seqId":{i}}}]}}"#
                );
                if ws.send(Message::text(m)).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = wl.accept().await;
            }
        });
        // приватный поток: проверить login, ответить, принять подписку, слать события
        let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let login_ok = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let lo = login_ok.clone();
        let pl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let paddr = pl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = pl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let Some(Ok(Message::Text(t))) = ws.next().await else {
                return;
            };
            let v: Value = serde_json::from_str(&t).unwrap();
            let a = &v["args"][0];
            let good = v["op"] == "login"
                && a["apiKey"] == "okx-key"
                && a["passphrase"] == "pass"
                && crate::util::hmac_b64(
                    SECRET,
                    &format!("{}GET/users/self/verify", a["timestamp"].as_str().unwrap()),
                ) == a["sign"].as_str().unwrap();
            lo.store(good, std::sync::atomic::Ordering::SeqCst);
            ws.send(Message::text(
                json!({"event": "login", "code": if good { "0" } else { "60009" }, "msg": ""})
                    .to_string(),
            ))
            .await
            .unwrap();
            let Some(Ok(Message::Text(t))) = ws.next().await else {
                return;
            };
            let v: Value = serde_json::from_str(&t).unwrap();
            assert_eq!(
                v["args"].as_array().unwrap().len(),
                3,
                "orders, account, positions"
            );
            ws.send(Message::text(
                json!({"event": "subscribe", "arg": {"channel": "orders"}}).to_string(),
            ))
            .await
            .unwrap();
            while let Some(e) = ev_rx.recv().await {
                if ws.send(Message::text(e)).await.is_err() {
                    break;
                }
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = pl.accept().await;
            }
        });
        // переопределения окружения для этого теста (потокобезопасно)
        crate::util::set_env("OKX_WS_PRIVATE_URL", &format!("ws://{paddr}"));
        crate::util::set_env("OKX_TESTNET_API_KEY", "okx-key");
        crate::util::set_env("OKX_TESTNET_API_SECRET", SECRET);
        crate::util::set_env("OKX_TESTNET_PASSPHRASE", "pass");
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=okx&symbols=BTCUSDT&market=perp&leverage=3&testnet=true&wsUrl=ws://{waddr}&restUrl=http://{raddr}&spreadCaptureEnabled=false&statArbEnabled=false&maxPositionQuote=1000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=okx", "").await;
            let (_, s) = call(&r, "GET", "/status", "").await;
            let (_, m) = call(&r, "GET", "/market?exchange=okx", "").await;
            if b["свободно"]["USDT"] == json!(1000.0)
                && s["okx"]["поток_событий_счёта"] == json!(true)
                && m["BTCUSDT"]["лучший_бид"].is_number()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "баланс или поток: {b} {s}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            login_ok.load(std::sync::atomic::Ordering::SeqCst),
            "подпись входа верна"
        );
        // шорт 0.5 BTC = 50 контрактов по 0.01
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=okx&symbol=BTCUSDT&side=SELL&type=MARKET&qty=0.5",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            if a["okx"]["тестовый_счёт"]["positions"][0]["qty"] == json!(-0.5) {
                break;
            }
            assert!(tokio::time::Instant::now() < until, "шорт не учтён: {a}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        {
            let x = ex.lock().unwrap();
            assert_eq!(x.orders[0]["sz"], json!("50"), "объём в контрактах");
            assert_eq!(
                (
                    x.orders[0]["tdMode"].as_str(),
                    x.orders[0]["ordType"].as_str()
                ),
                (Some("cross"), Some("market"))
            );
            assert_eq!(x.leverage[0]["lever"], json!("3"));
        }
        // позиция закрыта на бирже — событие positions
        ev_tx.send(json!({"arg": {"channel": "positions", "instType": "SWAP"}, "data": [{"instId": "BTC-USDT-SWAP", "pos": "0", "avgPx": "", "posSide": "net"}]}).to_string()).unwrap();
        let until = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            if a["okx"]["тестовый_счёт"]["positions"] == json!([]) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "событие не учтено: {a}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(ex.lock().unwrap().bad, 0, "все подписи REST верны");
        call(&r, "POST", "/control/stop", "").await;
    }

    /// Живой спот Gate против макета: подпись SHA512 каждого запроса, рыночная покупка — сумма в котируемой валюте
    /// (IOC), подписки потока с подписью, исполнение лимитной заявки из события spot.orders.
    #[tokio::test(flavor = "multi_thread")]
    async fn live_gate_spot_against_mock() {
        use axum::extract::{OriginalUri, RawQuery};
        use axum::http::HeaderMap;
        use futures_util::{SinkExt, StreamExt};
        use sha2::Digest;
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        const SECRET: &str = "gate-secret";
        #[derive(Default)]
        struct Ex {
            orders: Vec<Value>,
            bad: u32,
            x: f64,
        }
        let ex = Arc::new(Mutex::new(Ex::default()));
        fn ok(h: &HeaderMap, method: &str, path: &str, query: &str, body: &str) -> bool {
            let g = |k: &str| {
                h.get(k)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned()
            };
            let bh = hex::encode(sha2::Sha512::digest(body.as_bytes()));
            g("key") == "gate-key"
                && crate::gate_api::hmac512(
                    SECRET,
                    &format!("{method}\n{path}\n{query}\n{bh}\n{}", g("timestamp")),
                ) == g("sign")
        }
        let (e1, e2, e3) = (ex.clone(), ex.clone(), ex.clone());
        let rest = Router::new()
            .route("/api/v4/spot/accounts", get(move |h: HeaderMap| async move {
                let mut x = e1.lock().unwrap();
                if !ok(&h, "GET", "/api/v4/spot/accounts", "", "") { x.bad += 1; }
                Json(json!([{"currency": "USDT", "available": "1000"}, {"currency": "X", "available": format!("{}", x.x)}]))
            }))
            .route("/api/v4/spot/orders", axum::routing::post(move |h: HeaderMap, body: String| async move {
                let mut x = e2.lock().unwrap();
                if !ok(&h, "POST", "/api/v4/spot/orders", "", &body) { x.bad += 1; }
                let b: Value = serde_json::from_str(&body).unwrap();
                let id = format!("{}", 100 + x.orders.len());
                x.orders.push(b.clone());
                if b["type"] == "market" {
                    let quote: f64 = b["amount"].as_str().unwrap().parse().unwrap();
                    x.x += quote / 10.0;
                    Json(json!({"id": id, "text": b["text"], "status": "closed", "finish_as": "filled", "type": "market", "side": "buy",
                        "amount": b["amount"], "left": "0", "filled_total": format!("{quote}"), "avg_deal_price": "10"}))
                } else {
                    Json(json!({"id": id, "text": b["text"], "status": "open", "type": "limit", "side": b["side"], "amount": b["amount"], "left": b["amount"], "filled_total": "0"}))
                }
            }))
            .route("/api/v4/spot/orders/{id}", get(move |h: HeaderMap, u: OriginalUri, RawQuery(q): RawQuery| async move {
                let q = q.unwrap_or_default();
                let x = e3.lock().unwrap();
                let _ = ok(&h, "GET", u.0.path(), &q, "");
                let id = u.0.path().rsplit('/').next().unwrap().to_owned();
                let n: usize = id.parse::<usize>().unwrap() - 100;
                let o = &x.orders[n];
                Json(json!({"id": id, "text": o["text"], "status": "open", "type": o["type"], "side": o["side"], "amount": o["amount"], "left": o["amount"], "filled_total": "0"}))
            }));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        let wl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = wl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = wl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for i in 1..500u64 {
                let m = format!(
                    r#"{{"channel":"spot.order_book","event":"update","result":{{"t":1,"lastUpdateId":{i},"s":"X_USDT","bids":[["9.99","100"]],"asks":[["10","100"]]}}}}"#
                );
                if ws.send(Message::text(m)).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = wl.accept().await;
            }
        });
        let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let subs_ok = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let so = subs_ok.clone();
        let pl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let paddr = pl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = pl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for _ in 0..2 {
                let Some(Ok(Message::Text(t))) = ws.next().await else {
                    return;
                };
                let v: Value = serde_json::from_str(&t).unwrap();
                let ch = v["channel"].as_str().unwrap().to_owned();
                let good = v["auth"]["KEY"] == "gate-key"
                    && crate::gate_api::hmac512(
                        SECRET,
                        &format!("channel={ch}&event=subscribe&time={}", v["time"]),
                    ) == v["auth"]["SIGN"].as_str().unwrap();
                if good {
                    so.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                ws.send(Message::text(
                    json!({"channel": ch, "event": "subscribe", "result": {"status": "success"}})
                        .to_string(),
                ))
                .await
                .unwrap();
            }
            while let Some(e) = ev_rx.recv().await {
                if ws.send(Message::text(e)).await.is_err() {
                    break;
                }
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = pl.accept().await;
            }
        });
        // переопределения окружения для этого теста (потокобезопасно)
        crate::util::set_env("GATE_WS_PRIVATE_URL", &format!("ws://{paddr}"));
        crate::util::set_env("GATE_TESTNET_API_KEY", "gate-key");
        crate::util::set_env("GATE_TESTNET_API_SECRET", SECRET);
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=gate&symbols=XUSDT&market=spot&testnet=true&wsUrl=ws://{waddr}&restUrl=http://{raddr}&spreadCaptureEnabled=false&statArbEnabled=false&maxPositionQuote=1000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=gate", "").await;
            let (_, s) = call(&r, "GET", "/status", "").await;
            let (_, m) = call(&r, "GET", "/market?exchange=gate", "").await;
            if b["свободно"]["USDT"] == json!(1000.0)
                && s["gate"]["поток_событий_счёта"] == json!(true)
                && m["XUSDT"]["лучший_аск"].is_number()
                && subs_ok.load(std::sync::atomic::Ordering::SeqCst) == 2
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "баланс, поток или стакан: {b} {s} {m}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            subs_ok.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "подписи подписок верны"
        );
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=gate&symbol=XUSDT&side=BUY&type=MARKET&qty=2",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        let (_, v) = call(
            &r,
            "POST",
            "/order?exchange=gate&symbol=XUSDT&side=BUY&type=LIMIT&qty=1&price=9.5",
            "",
        )
        .await;
        assert_eq!(v["статус"], json!("NEW"), "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        let (oid, text) = loop {
            if let Some(o) = ex.lock().unwrap().orders.get(1).cloned() {
                break ("101".to_string(), o["text"].as_str().unwrap().to_owned());
            }
            assert!(
                tokio::time::Instant::now() < until,
                "лимитная заявка не дошла"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        {
            let x = ex.lock().unwrap();
            let m = &x.orders[0];
            assert_eq!(
                (m["type"].as_str(), m["time_in_force"].as_str()),
                (Some("market"), Some("ioc"))
            );
            let quote: f64 = m["amount"].as_str().unwrap().parse().unwrap();
            assert!(
                (quote - 2.0 * 10.0 * 1.005).abs() < 1e-6,
                "покупка суммой в USDT: {quote}"
            );
        }
        ev_tx.send(json!({"channel": "spot.orders", "event": "update", "result": [{"id": oid, "text": text, "status": "closed", "finish_as": "filled",
            "type": "limit", "side": "buy", "amount": "1", "left": "0", "filled_total": "9.5", "avg_deal_price": "9.5"}]}).to_string()).unwrap();
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            if a["gate"]["тестовый_счёт"]["openOrders"] == json!(0)
                && a["gate"]["событий_счёта"].as_u64().unwrap_or(0) >= 1
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "событие не учтено: {a}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(ex.lock().unwrap().bad, 0, "все подписи REST верны");
        call(&r, "POST", "/control/stop", "").await;
    }

    /// Живой спот KuCoin против макета: подпись KC-API-SIGN и passphrase (HMAC), токен bullet-private, подписки после
    /// welcome, исполнение лимитной заявки по событию tradeOrdersV2 (средняя цена — по сделкам match).
    #[tokio::test(flavor = "multi_thread")]
    async fn live_kucoin_spot_against_mock() {
        use axum::extract::OriginalUri;
        use axum::http::HeaderMap;
        use futures_util::{SinkExt, StreamExt};
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        const SECRET: &str = "kc-secret";
        #[derive(Default)]
        struct Ex {
            orders: Vec<Value>,
            bad: u32,
        }
        let ex = Arc::new(Mutex::new(Ex::default()));
        fn ok(h: &HeaderMap, method: &str, path: &str, body: &str) -> bool {
            let g = |k: &str| {
                h.get(k)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned()
            };
            g("kc-api-key") == "kc-key"
                && g("kc-api-key-version") == "2"
                && g("kc-api-passphrase") == crate::util::hmac_b64(SECRET, "kc-pass")
                && crate::util::hmac_b64(
                    SECRET,
                    &format!("{}{method}{path}{body}", g("kc-api-timestamp")),
                ) == g("kc-api-sign")
        }
        let pl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let paddr = pl.local_addr().unwrap();
        let (e1, e2, e3, e4) = (ex.clone(), ex.clone(), ex.clone(), ex.clone());
        let rest = Router::new()
            .route("/api/v1/accounts", get(move |h: HeaderMap, u: OriginalUri| async move {
                if !ok(&h, "GET", &u.0.to_string(), "") { e1.lock().unwrap().bad += 1; }
                Json(json!({"code": "200000", "data": [{"currency": "USDT", "available": "1000", "type": "trade"}]}))
            }))
            .route("/api/v1/bullet-private", axum::routing::post(move |h: HeaderMap| async move {
                if !ok(&h, "POST", "/api/v1/bullet-private", "") { e2.lock().unwrap().bad += 1; }
                Json(json!({"code": "200000", "data": {"token": "t1", "instanceServers": [{"endpoint": format!("ws://{paddr}"), "pingInterval": 18000}]}}))
            }))
            .route("/api/v1/orders", axum::routing::post(move |h: HeaderMap, body: String| async move {
                let mut x = e3.lock().unwrap();
                if !ok(&h, "POST", "/api/v1/orders", &body) { x.bad += 1; }
                let b: Value = serde_json::from_str(&body).unwrap();
                let id = format!("k{}", x.orders.len());
                x.orders.push(b);
                Json(json!({"code": "200000", "data": {"orderId": id}}))
            }))
            .route("/api/v1/orders/{id}", get(move |h: HeaderMap, u: OriginalUri| async move {
                let x = e4.lock().unwrap();
                let _ = ok(&h, "GET", &u.0.to_string(), "");
                let id = u.0.path().rsplit('/').next().unwrap().to_owned();
                let n: usize = id.trim_start_matches('k').parse().unwrap();
                let o = &x.orders[n];
                let market = o["type"] == "market";
                Json(json!({"code": "200000", "data": {"id": id, "isActive": !market, "cancelExist": false,
                    "dealSize": if market { o["size"].clone() } else { json!("0") }, "dealFunds": if market { json!("20") } else { json!("0") }}}))
            }));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        // публичный стакан: свой адрес wsUrl (без токена)
        let wl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = wl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = wl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for i in 1..500u64 {
                let m = format!(
                    r#"{{"type":"message","topic":"/spotMarket/level2Depth50:X-USDT","subject":"level2","data":{{"asks":[["10","100"]],"bids":[["9.99","100"]],"timestamp":{i}}}}}"#
                );
                if ws.send(Message::text(m)).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            loop {
                let _ = wl.accept().await;
            }
        });
        // приватный поток: welcome, подписки → ack, затем события
        let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let topics = Arc::new(Mutex::new(Vec::<String>::new()));
        let tp = topics.clone();
        tokio::spawn(async move {
            let (tcp, _) = pl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            ws.send(Message::text(r#"{"id":"w","type":"welcome"}"#))
                .await
                .unwrap();
            for _ in 0..2 {
                let Some(Ok(Message::Text(t))) = ws.next().await else {
                    return;
                };
                let v: Value = serde_json::from_str(&t).unwrap();
                tp.lock()
                    .unwrap()
                    .push(v["topic"].as_str().unwrap().to_owned());
                ws.send(Message::text(
                    json!({"id": v["id"], "type": "ack"}).to_string(),
                ))
                .await
                .unwrap();
            }
            while let Some(e) = ev_rx.recv().await {
                if ws.send(Message::text(e)).await.is_err() {
                    break;
                }
            }
            loop {
                let _ = pl.accept().await;
            }
        });
        crate::util::set_env("KUCOIN_API_KEY", "kc-key");
        crate::util::set_env("KUCOIN_API_SECRET", SECRET);
        crate::util::set_env("KUCOIN_PASSPHRASE", "kc-pass");
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=kucoin&symbols=XUSDT&market=spot&live=true&wsUrl=ws://{waddr}&restUrl=http://{raddr}&spreadCaptureEnabled=false&statArbEnabled=false&maxPositionQuote=1000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=kucoin", "").await;
            let (_, s) = call(&r, "GET", "/status", "").await;
            let (_, m) = call(&r, "GET", "/market?exchange=kucoin", "").await;
            if b["свободно"]["USDT"] == json!(1000.0)
                && s["kucoin"]["поток_событий_счёта"] == json!(true)
                && m["XUSDT"]["лучший_аск"].is_number()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "баланс, поток или стакан: {b} {s} {m}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            *topics.lock().unwrap(),
            vec![
                "/spotMarket/tradeOrdersV2".to_string(),
                "/account/balance".to_string()
            ]
        );
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=kucoin&symbol=XUSDT&side=BUY&type=MARKET&qty=2",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        let (_, v) = call(
            &r,
            "POST",
            "/order?exchange=kucoin&symbol=XUSDT&side=BUY&type=LIMIT&qty=1&price=9.5",
            "",
        )
        .await;
        assert_eq!(v["статус"], json!("NEW"), "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        let oid = loop {
            if let Some(o) = ex.lock().unwrap().orders.get(1).cloned() {
                assert_eq!(o["clientOid"], json!("hft2"));
                break "k1".to_string();
            }
            assert!(
                tokio::time::Instant::now() < until,
                "лимитная заявка не дошла"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        ev_tx.send(json!({"type": "message", "topic": "/spotMarket/tradeOrdersV2", "subject": "orderChange", "data": {"orderId": oid, "clientOid": "hft2",
            "symbol": "X-USDT", "type": "match", "status": "match", "matchSize": "1", "matchPrice": "9.5", "filledSize": "1", "size": "1"}}).to_string()).unwrap();
        ev_tx.send(json!({"type": "message", "topic": "/spotMarket/tradeOrdersV2", "subject": "orderChange", "data": {"orderId": oid, "clientOid": "hft2",
            "symbol": "X-USDT", "type": "filled", "status": "done", "filledSize": "1", "size": "1"}}).to_string()).unwrap();
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            if a["kucoin"]["тестовый_счёт"].is_null()
                && a["kucoin"]["событий_счёта"].as_u64().unwrap_or(0) >= 2
            {
                break; // LIVE — тестового счёта нет
            }
            assert!(
                tokio::time::Instant::now() < until,
                "событие не учтено: {a}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // рыночная покупка учитывается опросом (раз в 5 с, пока поток работает)
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=kucoin", "").await;
            if b["свободно"]["X"].as_f64().unwrap_or(0.0) >= 3.0 - 1e-9 {
                break;
            }
            assert!(tokio::time::Instant::now() < until, "куплено 2 + 1: {b}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(ex.lock().unwrap().bad, 0, "все подписи верны");
        call(&r, "POST", "/control/stop", "").await;
    }

    /// Живые свопы Uniswap V2 против макета ноды: резервы пула в стакан, approve ровно на нужную сумму, затем
    /// swapTokensForExactTokens с maxIn = getAmountsIn × 1.005; транзакции подписаны (EIP-155), итог — по балансам.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_uniswap_against_mock() {
        use futures_util::{SinkExt, StreamExt};
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        const KEY: &str = "0x4646464646464646464646464646464646464646464646464646464646464646";
        let router = "0x00000000000000000000000000000000000000f0";
        let (tx_x, tx_usdc, pair) = (
            "0x00000000000000000000000000000000000000a1",
            "0x00000000000000000000000000000000000000a2",
            "0x00000000000000000000000000000000000000b1",
        );
        let me = crate::evm::Wallet::from_hex(KEY).unwrap().address;
        #[derive(Default)]
        struct Node {
            bal: std::collections::HashMap<String, u128>,
            allowance: u128,
            pending: Option<(String, String)>,
            nonce: u128,
            sent: Vec<String>,
            bad: u32,
        }
        let node = Arc::new(Mutex::new(Node::default()));
        node.lock()
            .unwrap()
            .bal
            .insert(tx_usdc.into(), 1_000_000_000); // 1000 USDC
        let n1 = node.clone();
        let me2 = me.clone();
        let rpc = Router::new().route(
            "/",
            axum::routing::post(move |Json(b): Json<Value>| async move {
                let mut n = n1.lock().unwrap();
                let p = &b["params"];
                let w = |x: u128| format!("{x:064x}");
                let word = |d: &str, i: usize| {
                    u128::from_str_radix(&d[8 + 64 * i + 32..8 + 64 * (i + 1)], 16).unwrap()
                };
                let r = match b["method"].as_str().unwrap() {
                    "eth_chainId" => json!("0x1"),
                    "eth_gasPrice" => json!("0x3b9aca00"),
                    "eth_estimateGas" => {
                        n.pending = Some((
                            p[0]["to"].as_str().unwrap().into(),
                            p[0]["data"].as_str().unwrap()[2..].into(),
                        ));
                        json!("0x30d40")
                    }
                    "eth_getTransactionCount" => json!(format!("0x{:x}", n.nonce)),
                    "eth_getBalance" => json!("0xde0b6b3a7640000"),
                    "eth_getTransactionReceipt" => json!({"status": "0x1"}),
                    "eth_sendRawTransaction" => {
                        let (to, data) = n.pending.take().unwrap();
                        let w8 = crate::evm::Wallet::from_hex(KEY).unwrap();
                        let want = crate::uniswap_api::sign_tx(
                            &w8,
                            1,
                            n.nonce,
                            1_100_000_000,
                            260_000,
                            &to,
                            0,
                            &hex::decode(&data).unwrap(),
                        )
                        .unwrap();
                        if want != p[0] {
                            n.bad += 1;
                        }
                        n.nonce += 1;
                        match &data[..8] {
                            "095ea7b3" => n.allowance = word(&data, 1),
                            "8803dbee" => {
                                let (out, max_in) = (word(&data, 0), word(&data, 1));
                                let need = out / 1_000_000_000_000 * 10_030 / 1000; // 10.03 USDC за X
                                assert!(need <= max_in && need <= n.allowance);
                                *n.bal
                                    .entry("0x00000000000000000000000000000000000000a1".into())
                                    .or_default() += out;
                                *n.bal
                                    .entry("0x00000000000000000000000000000000000000a2".into())
                                    .or_default() -= need;
                            }
                            _ => n.bad += 1,
                        }
                        n.sent.push(data[..8].to_owned());
                        json!(format!("0x{:064x}", n.nonce))
                    }
                    "eth_call" => {
                        let to = p[0]["to"].as_str().unwrap().to_owned();
                        let d = &p[0]["data"].as_str().unwrap()[2..];
                        match &d[..8] {
                            "70a08231" => {
                                assert!(d.ends_with(&me2[2..]));
                                json!(format!("0x{}", w(*n.bal.get(&to).unwrap_or(&0))))
                            }
                            "dd62ed3e" => json!(format!("0x{}", w(n.allowance))),
                            "1f00ca74" => {
                                let out = word(d, 0);
                                json!(format!(
                                    "0x{}{}{}{}",
                                    w(32),
                                    w(2),
                                    w(out / 1_000_000_000_000 * 10_030 / 1000),
                                    w(out)
                                ))
                            }
                            _ => json!("0x"),
                        }
                    }
                    m => panic!("метод {m}"),
                };
                Json(json!({"jsonrpc": "2.0", "id": b["id"], "result": r}))
            }),
        );
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rpc).await.unwrap() });
        // нода по WS для стакана: резервы пула 1000 X / 10000 USDC
        let wl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = wl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = wl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while let Some(Ok(Message::Text(t))) = ws.next().await {
                let v: Value = serde_json::from_str(t.as_str()).unwrap();
                let r = if v["method"] == "eth_call" {
                    json!(format!(
                        "0x{:064x}{:064x}{:064x}",
                        1000u128 * 10u128.pow(18),
                        10_000u128 * 10u128.pow(6),
                        1
                    ))
                } else {
                    json!("0x1")
                };
                if ws
                    .send(Message::text(
                        json!({"jsonrpc": "2.0", "id": v["id"], "result": r}).to_string(),
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            loop {
                let _ = wl.accept().await;
            }
        });
        crate::util::set_env("UNISWAPV2_API_KEY", &me);
        crate::util::set_env("UNISWAPV2_API_SECRET", KEY);
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=uniswapv2&symbols=XUSDC&live=true&wsUrl=ws://{waddr}&restUrl=http://{raddr}/&uniRouter={router}&uniTokens=X={tx_x}:18;USDC={tx_usdc}:6&uniPools=XUSDC={pair}:true:18:6&spreadCaptureEnabled=false&statArbEnabled=false&maxPositionQuote=1000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=uniswapv2", "").await;
            let (_, m) = call(&r, "GET", "/market?exchange=uniswapv2", "").await;
            if b["свободно"]["USDC"] == json!(1000.0) && m["XUSDC"]["лучший_аск"].is_number()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "баланс или стакан: {b} {m}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=uniswapv2&symbol=XUSDC&side=BUY&type=MARKET&qty=2",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=uniswapv2", "").await;
            if b["свободно"]["X"].as_f64().unwrap_or(0.0) >= 2.0 - 1e-9 {
                break;
            }
            assert!(tokio::time::Instant::now() < until, "своп не учтён: {b}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let n = node.lock().unwrap();
        assert_eq!(n.bad, 0, "все транзакции подписаны верно");
        assert_eq!(n.sent, ["095ea7b3", "8803dbee"]);
        // approve — на maxIn: 20.06 USDC × 1.005
        assert_eq!(n.allowance, 20_160_300);
        assert_eq!(
            n.bal["0x00000000000000000000000000000000000000a2"],
            1_000_000_000 - 20_060_000
        );
    }

    /// Живой спот Upbit за USDT против макета: JWT (HS256, query_hash SHA512) в REST и в рукопожатии потока,
    /// рыночная покупка на сумму, шаг цены лимитного ордера, myOrder по identifier, готовность по {"status":"UP"}.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[allow(clippy::result_large_err)] // колбэк рукопожатия tungstenite
    async fn live_upbit_spot_against_mock() {
        use axum::extract::RawQuery;
        use axum::http::HeaderMap;
        use futures_util::{SinkExt, StreamExt};
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        const SECRET: &str = "up-secret";
        #[derive(Default)]
        struct Ex {
            orders: Vec<Value>,
            bad: u32,
            bought: f64,
        }
        let ex = Arc::new(Mutex::new(Ex::default()));
        let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        // JWT: подпись HMAC-SHA256 и query_hash = SHA512 строки параметров
        fn ok(h: &HeaderMap, query: &str) -> bool {
            use hmac::Mac;
            use sha2::Digest;
            let Some(t) = h
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
            else {
                return false;
            };
            let p: Vec<&str> = t.split('.').collect();
            let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
            mac.update(format!("{}.{}", p[0], p[1]).as_bytes());
            // base64url без выравнивания
            let dec = |s: &str| -> Vec<u8> {
                const A: &[u8] =
                    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
                let (mut out, mut acc, mut bits) = (Vec::new(), 0u32, 0);
                for c in s.bytes() {
                    acc = acc << 6 | A.iter().position(|&x| x == c).unwrap() as u32;
                    bits += 6;
                    if bits >= 8 {
                        bits -= 8;
                        out.push((acc >> bits) as u8);
                    }
                }
                out
            };
            let enc = |b: &[u8]| {
                crate::util::base64(b)
                    .trim_end_matches('=')
                    .replace('+', "-")
                    .replace('/', "_")
            };
            let body: Value = serde_json::from_slice(&dec(p[1])).unwrap();
            let qh = if query.is_empty() {
                Value::Null
            } else {
                json!(hex::encode(sha2::Sha512::digest(query.as_bytes())))
            };
            enc(&mac.finalize().into_bytes()) == p[2]
                && body["access_key"] == "up-key"
                && body["query_hash"] == qh
        }
        let (e1, e2) = (ex.clone(), ex.clone());
        let rest = Router::new()
            .route("/v1/orderbook/instruments", get(|| async { Json(json!([{"market": "USDT-X", "tick_size": "0.001"}])) }))
            .route("/v1/accounts", get(move |h: HeaderMap| async move {
                let mut x = e1.lock().unwrap();
                if !ok(&h, "") { x.bad += 1; }
                Json(json!([{"currency": "USDT", "balance": "1000", "locked": "0"}, {"currency": "X", "balance": format!("{}", x.bought), "locked": "0"}]))
            }))
            .route("/v1/orders", axum::routing::post(move |h: HeaderMap, Json(b): Json<Value>| async move {
                let mut x = e2.lock().unwrap();
                // query_hash — по параметрам в порядке тела
                let q: Vec<String> = b.as_object().unwrap().iter().map(|(k, v)| format!("{k}={}", v.as_str().unwrap())).collect();
                if !ok(&h, &q.join("&")) { x.bad += 1; }
                let n = x.orders.len();
                if b["ord_type"] == "price" {
                    x.bought += 2.0;
                    let _ = ev_tx.send(json!({"type": "myOrder", "code": "USDT-X", "uuid": "u0", "identifier": b["identifier"], "state": "done",
                        "executed_volume": 2, "executed_funds": 20}).to_string());
                }
                x.orders.push(b);
                Json(json!({"uuid": format!("u{n}"), "state": "wait", "executed_volume": "0"}))
            }))
            .route("/v1/order", get(|RawQuery(q): RawQuery| async move {
                let q = q.unwrap_or_default();
                Json(if q == "uuid=u0" {
                    json!({"uuid": "u0", "state": "done", "executed_volume": "2", "executed_funds": "20"})
                } else {
                    json!({"uuid": "u1", "state": "wait", "executed_volume": "0"})
                })
            }));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        let wl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = wl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = wl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for _ in 1..500u64 {
                let m = br#"{"type":"orderbook","code":"USDT-X","timestamp":9,"orderbook_units":[{"ask_price":10,"bid_price":9.99,"ask_size":100,"bid_size":100}]}"#;
                if ws.send(Message::binary(m.to_vec())).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            loop {
                let _ = wl.accept().await;
            }
        });
        let seen = Arc::new(Mutex::new((false, String::new())));
        let sn = seen.clone();
        let sl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let saddr = sl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = sl.accept().await.unwrap();
            let sn2 = sn.clone();
            let cb = move |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                           resp| {
                sn2.lock().unwrap().0 = ok(req.headers(), "");
                Ok(resp)
            };
            let mut ws = tokio_tungstenite::accept_hdr_async(tcp, cb).await.unwrap();
            if let Some(Ok(Message::Text(t))) = ws.next().await {
                sn.lock().unwrap().1 = t.to_string();
            }
            ws.send(Message::binary(br#"{"status":"UP"}"#.to_vec()))
                .await
                .unwrap();
            while let Some(e) = ev_rx.recv().await {
                if ws.send(Message::binary(e.into_bytes())).await.is_err() {
                    break;
                }
            }
            loop {
                let _ = sl.accept().await;
            }
        });
        crate::util::set_env("UPBIT_STREAM_URL", &format!("ws://{saddr}/"));
        crate::util::set_env("UPBIT_API_KEY", "up-key");
        crate::util::set_env("UPBIT_API_SECRET", SECRET);
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=upbit&symbols=XUSDT&live=true&wsUrl=ws://{waddr}&restUrl=http://{raddr}&spreadCaptureEnabled=false&statArbEnabled=false&maxPositionQuote=1000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=upbit", "").await;
            let (_, s) = call(&r, "GET", "/status", "").await;
            let (_, m) = call(&r, "GET", "/market?exchange=upbit", "").await;
            if b["свободно"]["USDT"] == json!(1000.0)
                && s["upbit"]["поток_событий_счёта"] == json!(true)
                && m["XUSDT"]["лучший_аск"].is_number()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "баланс, поток или стакан: {b} {s} {m}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=upbit&symbol=XUSDT&side=BUY&type=MARKET&qty=2",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            if a["upbit"]["событий_счёта"].as_u64().unwrap_or(0) >= 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "событие не учтено: {a}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=upbit&symbol=XUSDT&side=SELL&type=LIMIT&qty=1&price=11.12345",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        while ex.lock().unwrap().orders.len() < 2 {
            assert!(
                tokio::time::Instant::now() < until,
                "лимитный ордер не дошёл"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let x = ex.lock().unwrap();
        assert_eq!(x.bad, 0, "все подписи верны");
        // рыночная покупка — на сумму 2 × 10 × 1.005; лимит — по шагу цены 0.001
        assert_eq!(
            x.orders[0],
            json!({"market": "USDT-X", "side": "bid", "price": "20.1", "ord_type": "price", "identifier": x.orders[0]["identifier"]})
        );
        assert_eq!(
            (
                x.orders[1]["side"].as_str(),
                x.orders[1]["price"].as_str(),
                x.orders[1]["volume"].as_str()
            ),
            (Some("ask"), Some("11.123"), Some("1"))
        );
        let seen = seen.lock().unwrap();
        assert!(seen.0, "JWT в рукопожатии потока");
        assert!(
            seen.1.contains("myOrder") && seen.1.contains("myAsset"),
            "{}",
            seen.1
        );
    }

    /// Живой спот LBank против макета: подпись (MD5 → HMAC-SHA256, служебные поля в заголовках), рыночная покупка на
    /// сумму, ключ подписки, поток orderUpdate по customerID, pong на ping сервера.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_lbank_spot_against_mock() {
        use axum::http::HeaderMap;
        use futures_util::{SinkExt, StreamExt};
        use std::collections::BTreeMap;
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        const SECRET: &str = "lb-secret";
        #[derive(Default)]
        struct Ex {
            orders: Vec<BTreeMap<String, String>>,
            bad: u32,
            bought: f64,
        }
        let ex = Arc::new(Mutex::new(Ex::default()));
        let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        // подпись: тело без sign + служебные поля из заголовков
        fn ok(f: &BTreeMap<String, String>, h: &HeaderMap) -> bool {
            let mut p = f.clone();
            let sig = p.remove("sign").unwrap_or_default();
            for k in ["timestamp", "signature_method", "echostr"] {
                p.insert(
                    k.into(),
                    h.get(k)
                        .map(|v| v.to_str().unwrap().to_owned())
                        .unwrap_or_default(),
                );
            }
            p["api_key"] == "lb-key"
                && p["echostr"].len() == 35
                && crate::lbank_api::sign(SECRET, &p) == sig
        }
        type Form = axum::extract::Form<BTreeMap<String, String>>;
        let (e1, e2, e3) = (ex.clone(), ex.clone(), ex.clone());
        let rest = Router::new()
            .route("/v2/timestamp.do", get(|| async { Json(json!({"result": "true", "data": now_ms(), "error_code": 0})) }))
            .route("/v2/accuracy.do", get(|| async { Json(json!({"result": "true", "error_code": 0, "data": [{"symbol": "x_usdt", "quantityAccuracy": "2", "priceAccuracy": "4"}]})) }))
            .route("/v2/supplement/user_info_account.do", axum::routing::post(move |h: HeaderMap, axum::extract::Form(f): Form| async move {
                let mut x = e1.lock().unwrap();
                if !ok(&f, &h) { x.bad += 1; }
                Json(json!({"result": "true", "error_code": 0, "data": {"balances": [{"asset": "usdt", "free": "1000", "locked": "0"}, {"asset": "x", "free": format!("{}", x.bought), "locked": "0"}]}}))
            }))
            .route("/v2/subscribe/get_key.do", axum::routing::post(move |h: HeaderMap, axum::extract::Form(f): Form| async move {
                if !ok(&f, &h) { e2.lock().unwrap().bad += 1; }
                Json(json!({"result": "true", "error_code": 0, "data": "sk1"}))
            }))
            .route("/v2/supplement/create_order.do", axum::routing::post(move |h: HeaderMap, axum::extract::Form(f): Form| async move {
                let mut x = e3.lock().unwrap();
                if !ok(&f, &h) { x.bad += 1; }
                x.bought += 2.0;
                let _ = ev_tx.send(json!({"type": "orderUpdate", "pair": "x_usdt", "orderUpdate": {"uuid": "o1", "customerID": f["custom_id"],
                    "orderStatus": 2, "accAmt": "2", "avgPrice": "10", "type": "buy_market"}}).to_string());
                x.orders.push(f);
                Json(json!({"result": "true", "error_code": 0, "data": {"order_id": "o1"}}))
            }))
            .route("/v2/supplement/orders_info.do", axum::routing::post(|| async {
                Json(json!({"result": "true", "error_code": 0, "data": {"orderId": "o1", "executedQty": "2", "cummulativeQuoteQty": "20", "status": 2}}))
            }));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        let wl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = wl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = wl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for _ in 1..500u64 {
                let m = r#"{"depth":{"asks":[[10,100]],"bids":[[9.99,100]]},"count":50,"type":"depth","pair":"x_usdt","SERVER":"V2","TS":"2026"}"#;
                if ws.send(Message::text(m)).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            loop {
                let _ = wl.accept().await;
            }
        });
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let sn = seen.clone();
        let sl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let saddr = sl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = sl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for _ in 0..2 {
                if let Some(Ok(Message::Text(t))) = ws.next().await {
                    sn.lock().unwrap().push(t.to_string());
                }
            }
            ws.send(Message::text(r#"{"action":"ping","ping":"p1"}"#))
                .await
                .unwrap();
            if let Some(Ok(Message::Text(t))) = ws.next().await {
                sn.lock().unwrap().push(t.to_string());
            }
            ws.send(Message::text(
                r#"{"action":"subscribe","subscribe":"orderUpdate","status":"success"}"#,
            ))
            .await
            .unwrap();
            while let Some(e) = ev_rx.recv().await {
                if ws.send(Message::text(e)).await.is_err() {
                    break;
                }
            }
            loop {
                let _ = sl.accept().await;
            }
        });
        crate::util::set_env("LBANK_STREAM_URL", &format!("ws://{saddr}/"));
        crate::util::set_env("LBANK_API_KEY", "lb-key");
        crate::util::set_env("LBANK_API_SECRET", SECRET);
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=lbank&symbols=XUSDT&live=true&wsUrl=ws://{waddr}&restUrl=http://{raddr}&spreadCaptureEnabled=false&statArbEnabled=false&maxPositionQuote=1000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=lbank", "").await;
            let (_, s) = call(&r, "GET", "/status", "").await;
            let (_, m) = call(&r, "GET", "/market?exchange=lbank", "").await;
            if b["свободно"]["USDT"] == json!(1000.0)
                && s["lbank"]["поток_событий_счёта"] == json!(true)
                && m["XUSDT"]["лучший_аск"].is_number()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "баланс, поток или стакан: {b} {s} {m}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=lbank&symbol=XUSDT&side=BUY&type=MARKET&qty=2",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            if a["lbank"]["событий_счёта"].as_u64().unwrap_or(0) >= 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "событие не учтено: {a}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let x = ex.lock().unwrap();
        assert_eq!(x.bad, 0, "все подписи верны");
        let o = &x.orders[0];
        // рыночная покупка — на сумму: 2 × аск 10 × 1.005
        assert_eq!(
            (
                o["symbol"].as_str(),
                o["type"].as_str(),
                o["price"].as_str()
            ),
            ("x_usdt", "buy_market", "20.1")
        );
        assert!(o["custom_id"].starts_with("hft") && !o.contains_key("amount"));
        let seen = seen.lock().unwrap();
        assert!(
            seen[0].contains(r#""subscribeKey":"sk1""#) && seen[0].contains("orderUpdate"),
            "{seen:?}"
        );
        assert_eq!(seen[2], r#"{"action":"pong","pong":"p1"}"#);
    }

    /// Живые перпы Hyperliquid против макета: подпись агента (EIP-712 по msgpack действия), плечо, IOC-ордер с cloid,
    /// подписка orderUpdates и позиция из события.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_hyperliquid_perp_against_mock() {
        use futures_util::{SinkExt, StreamExt};
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        const AGENT: &str = "0x0123456789012345678901234567890123456789012345678901234567890123";
        const USER: &str = "0x00000000000000000000000000000000000000aa";
        #[derive(Default)]
        struct Ex {
            actions: Vec<Value>,
            bad: u32,
            pos: f64,
        }
        let ex = Arc::new(Mutex::new(Ex::default()));
        let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (e1, e2) = (ex.clone(), ex.clone());
        let rest = Router::new()
            .route("/info", axum::routing::post(move |Json(b): Json<Value>| async move {
                let x = e1.lock().unwrap();
                Json(match b["type"].as_str().unwrap() {
                    "meta" => json!({"universe": [{"name": "ETH", "szDecimals": 4}, {"name": "BTC", "szDecimals": 5}]}),
                    "clearinghouseState" => {
                        assert_eq!(b["user"], USER);
                        json!({"withdrawable": "1000", "assetPositions": [{"position": {"coin": "BTC", "szi": format!("{}", x.pos), "entryPx": "100"}}]})
                    }
                    "orderStatus" => json!({"order": {"status": "filled", "order": {"origSz": "0.5", "sz": "0", "limitPx": "95"}}}),
                    _ => json!([{"universe": [{"name": "BTC"}]}, [{"funding": "0.00001", "markPx": "100"}]]),
                })
            }))
            .route("/exchange", axum::routing::post(move |Json(b): Json<Value>| async move {
                let mut x = e2.lock().unwrap();
                // подпись детерминирована (RFC 6979): та же, что посчитает биржа для этого действия и nonce
                let w = crate::evm::Wallet::from_hex(AGENT).unwrap();
                let want = crate::hyperliquid_api::sign_action(&w, &b["action"], b["nonce"].as_u64().unwrap(), false);
                if want != b["signature"] {
                    x.bad += 1;
                }
                x.actions.push(b["action"].clone());
                if b["action"]["type"] == "order" {
                    let o = &b["action"]["orders"][0];
                    let q: f64 = o["s"].as_str().unwrap().parse().unwrap();
                    x.pos += if o["b"] == true { q } else { -q };
                    let _ = ev_tx.send(json!({"channel": "orderUpdates", "data": [{"status": "filled",
                        "order": {"oid": 9, "coin": "BTC", "cloid": o["c"], "origSz": o["s"], "sz": "0", "limitPx": o["p"]}}]}).to_string());
                    // IOC исполняется сразу: биржа отвечает объёмом и средней ценой
                    return Json(json!({"status": "ok", "response": {"type": "order", "data": {"statuses": [{"filled": {"oid": 9, "totalSz": o["s"], "avgPx": "99.9"}}]}}}));
                }
                Json(json!({"status": "ok", "response": {"type": "default"}}))
            }));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        let wl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = wl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = wl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for _ in 1..500u64 {
                let m = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"99.9","sz":"100","n":1}],[{"px":"100","sz":"100","n":1}]]}}"#;
                if ws.send(Message::text(m)).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            loop {
                let _ = wl.accept().await;
            }
        });
        let subs = Arc::new(Mutex::new(Vec::<Value>::new()));
        let sb = subs.clone();
        let sl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let saddr = sl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = sl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for _ in 0..2 {
                if let Some(Ok(Message::Text(t))) = ws.next().await {
                    sb.lock()
                        .unwrap()
                        .push(serde_json::from_str(t.as_str()).unwrap());
                }
            }
            ws.send(Message::text(
                r#"{"channel":"subscriptionResponse","data":{}}"#,
            ))
            .await
            .unwrap();
            while let Some(e) = ev_rx.recv().await {
                if ws.send(Message::text(e)).await.is_err() {
                    break;
                }
            }
            loop {
                let _ = sl.accept().await;
            }
        });
        crate::util::set_env("HYPERLIQUID_STREAM_URL", &format!("ws://{saddr}/"));
        crate::util::set_env("HYPERLIQUID_TESTNET_API_KEY", USER);
        crate::util::set_env("HYPERLIQUID_TESTNET_API_SECRET", AGENT);
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=hyperliquid&symbols=BTCUSDC&market=perp&leverage=3&testnet=true&wsUrl=ws://{waddr}&restUrl=http://{raddr}&spreadCaptureEnabled=false&statArbEnabled=false&maxPositionQuote=1000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=hyperliquid", "").await;
            let (_, s) = call(&r, "GET", "/status", "").await;
            let (_, m) = call(&r, "GET", "/market?exchange=hyperliquid", "").await;
            if b["свободно"]["USDC"] == json!(1000.0)
                && s["hyperliquid"]["поток_событий_счёта"] == json!(true)
                && m["BTCUSDC"]["лучший_аск"].is_number()
                && !ex.lock().unwrap().actions.is_empty()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "баланс, поток, стакан или плечо: {b} {s} {m}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=hyperliquid&symbol=BTCUSDC&side=SELL&type=MARKET&qty=0.5",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            if a["hyperliquid"]["тестовый_счёт"]["positions"][0]["qty"] == json!(-0.5) {
                break;
            }
            assert!(tokio::time::Instant::now() < until, "шорт не учтён: {a}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let x = ex.lock().unwrap();
        assert_eq!(x.bad, 0, "подписи не сошлись");
        assert_eq!(
            x.actions[0],
            json!({"type": "updateLeverage", "asset": 1, "isCross": true, "leverage": 3})
        );
        let o = &x.actions.iter().find(|a| a["type"] == "order").unwrap()["orders"][0];
        // рыночная продажа — IOC на 5 % ниже опорной (бид 99.9), у BTC szDecimals 5 → один знак цены
        assert_eq!(
            (
                o["a"].clone(),
                o["b"].clone(),
                o["s"].clone(),
                o["p"].clone()
            ),
            (json!(1), json!(false), json!("0.5"), json!("94.9"))
        );
        assert_eq!(o["t"]["limit"]["tif"], "Ioc");
        assert_eq!(
            subs.lock().unwrap()[0]["subscription"],
            json!({"type": "orderUpdates", "user": USER})
        );
    }

    /// Живые перпы BingX против макета: подпись (параметры по алфавиту), positionSide=BOTH, плечо, поток по listenKey
    /// с кадрами gzip (ORDER_TRADE_UPDATE, ACCOUNT_UPDATE), ответ Pong на пинг сервера.
    #[tokio::test(flavor = "multi_thread")]
    async fn live_bingx_perp_against_mock() {
        use axum::extract::RawQuery;
        use axum::http::HeaderMap;
        use futures_util::{SinkExt, StreamExt};
        use std::io::Write;
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        const SECRET: &str = "bx-secret";
        #[derive(Default)]
        struct Ex {
            orders: Vec<String>,
            leverage: Vec<String>,
            bad: u32,
            pos: f64,
        }
        let ex = Arc::new(Mutex::new(Ex::default()));
        // подпись — по строке до &signature=, параметры по алфавиту
        fn ok(q: &str, h: &HeaderMap) -> bool {
            let (body, sig) = q.rsplit_once("&signature=").unwrap_or((q, ""));
            let keys: Vec<&str> = body
                .split('&')
                .map(|kv| kv.split('=').next().unwrap())
                .collect();
            let mut sorted = keys.clone();
            sorted.sort();
            h.get("x-bx-apikey").is_some_and(|k| k == "bx-key")
                && keys == sorted
                && crate::signed::sign(SECRET, body) == sig
        }
        fn param(q: &str, k: &str) -> Option<String> {
            q.split('&').find_map(|kv| {
                kv.split_once('=')
                    .filter(|(a, _)| *a == k)
                    .map(|(_, v)| v.to_owned())
            })
        }
        let (e1, e2, e3, e4, e5) = (ex.clone(), ex.clone(), ex.clone(), ex.clone(), ex.clone());
        let rest = Router::new()
            .route("/openApi/swap/v2/server/time", get(|| async { Json(json!({"code": 0, "data": {"serverTime": now_ms()}})) }))
            .route("/openApi/swap/v2/user/balance", get(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                if !ok(&q.unwrap_or_default(), &h) { e1.lock().unwrap().bad += 1; }
                Json(json!({"code": 0, "data": {"balance": {"asset": "USDT", "balance": "1000", "availableMargin": "1000"}}}))
            }))
            .route("/openApi/swap/v2/user/positions", get(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let x = e2.lock().unwrap();
                let _ = ok(&q.unwrap_or_default(), &h);
                Json(json!({"code": 0, "data": [{"symbol": "BTC-USDT", "positionSide": "BOTH", "positionAmt": format!("{}", x.pos), "avgPrice": "100"}]}))
            }))
            .route("/openApi/swap/v2/trade/leverage", axum::routing::post(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let mut x = e3.lock().unwrap();
                if !ok(&q, &h) { x.bad += 1; }
                x.leverage.push(format!("{}:{}", param(&q, "side").unwrap(), param(&q, "leverage").unwrap()));
                Json(json!({"code": 0, "data": {}}))
            }))
            .route("/openApi/swap/v2/trade/order", axum::routing::post(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let mut x = e4.lock().unwrap();
                if !ok(&q, &h) { x.bad += 1; }
                let qty: f64 = param(&q, "quantity").unwrap().parse().unwrap();
                x.pos += if param(&q, "side").as_deref() == Some("SELL") { -qty } else { qty };
                x.orders.push(q);
                Json(json!({"code": 0, "data": {"order": {"orderId": 777, "status": "FILLED", "executedQty": format!("{qty}"), "avgPrice": "100"}}}))
            }).get(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let _ = ok(&q.unwrap_or_default(), &h);
                let _ = &e5;
                Json(json!({"code": 0, "data": {"order": {"orderId": 777, "status": "FILLED", "executedQty": "0.5", "avgPrice": "100"}}}))
            }))
            .route("/openApi/user/auth/userDataStream", axum::routing::post(|h: HeaderMap| async move {
                assert_eq!(h.get("x-bx-apikey").unwrap(), "bx-key");
                Json(json!({"listenKey": "bx1"}))
            }));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        let gz = |t: String| {
            let mut g = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            g.write_all(t.as_bytes()).unwrap();
            g.finish().unwrap()
        };
        let wl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = wl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = wl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for _ in 1..500u64 {
                let m = r#"{"code":0,"dataType":"BTC-USDT@depth20@500ms","data":{"bids":[["99.9","100"]],"asks":[["100","100"]]}}"#.to_string();
                if ws.send(Message::binary(gz(m))).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            loop {
                let _ = wl.accept().await;
            }
        });
        // поток: путь ?listenKey=bx1, пинг сервера — ждём Pong, затем события
        let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let pong = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let pg = pong.clone();
        let sl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let saddr = sl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = sl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            ws.send(Message::binary(gz("Ping".into()))).await.unwrap();
            if let Some(Ok(Message::Text(t))) = ws.next().await {
                pg.store(t.as_str() == "Pong", std::sync::atomic::Ordering::SeqCst);
            }
            while let Some(e) = ev_rx.recv().await {
                if ws.send(Message::binary(gz(e))).await.is_err() {
                    break;
                }
            }
            loop {
                let _ = sl.accept().await;
            }
        });
        crate::util::set_env("BINGX_STREAM_URL", &format!("ws://{saddr}/"));
        crate::util::set_env("BINGX_TESTNET_API_KEY", "bx-key");
        crate::util::set_env("BINGX_TESTNET_API_SECRET", SECRET);
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=bingx&symbols=BTCUSDT&market=perp&leverage=4&testnet=true&wsUrl=ws://{waddr}&restUrl=http://{raddr}&spreadCaptureEnabled=false&statArbEnabled=false&maxPositionQuote=1000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=bingx", "").await;
            let (_, s) = call(&r, "GET", "/status", "").await;
            let (_, m) = call(&r, "GET", "/market?exchange=bingx", "").await;
            if b["свободно"]["USDT"] == json!(1000.0)
                && s["bingx"]["поток_событий_счёта"] == json!(true)
                && m["BTCUSDT"]["лучший_аск"].is_number()
                && pong.load(std::sync::atomic::Ordering::SeqCst)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "баланс, поток, стакан или Pong: {b} {s} {m}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=bingx&symbol=BTCUSDT&side=SELL&type=MARKET&qty=0.5",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            if a["bingx"]["тестовый_счёт"]["positions"][0]["qty"] == json!(-0.5) {
                break;
            }
            assert!(tokio::time::Instant::now() < until, "шорт не учтён: {a}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        {
            let x = ex.lock().unwrap();
            assert_eq!(param(&x.orders[0], "positionSide").as_deref(), Some("BOTH"));
            assert!(
                param(&x.orders[0], "clientOrderID")
                    .unwrap()
                    .starts_with("hft")
            );
            assert_eq!(x.leverage, vec!["BOTH:4".to_string()]);
        }
        // позиция закрыта — ACCOUNT_UPDATE (gzip)
        ev_tx.send(json!({"e": "ACCOUNT_UPDATE", "a": {"B": [], "P": [{"s": "BTC-USDT", "pa": "0", "ep": "0", "ps": "BOTH"}]}}).to_string()).unwrap();
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            if a["bingx"]["тестовый_счёт"]["positions"] == json!([]) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "событие не учтено: {a}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(ex.lock().unwrap().bad, 0, "все подписи верны");
        call(&r, "POST", "/control/stop", "").await;
    }

    /// Сторож перпов на бумаге: ручная позиция — без стратегии, видна в /status; цена ушла против неё на 2 % — закрыта.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn orphan_guard_closes_paper_perp_position() {
        use futures_util::SinkExt;
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio_tungstenite::tungstenite::Message;
        let rest = Router::new()
            .route("/fapi/v1/exchangeInfo", get(|| async { Json(json!({"symbols": [{"symbol": "BTCUSDT", "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.1"}, {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"}]}]})) }))
            .route("/fapi/v1/premiumIndex", get(|| async {
                Json(json!({"symbol": "BTCUSDT", "markPrice": "100", "lastFundingRate": "0", "nextFundingTime": now_ms() + 3_600_000}))
            }));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        let up = Arc::new(AtomicBool::new(false));
        let up2 = up.clone();
        let wl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = wl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = wl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for i in 1..2000u64 {
                let (b, a) = if up2.load(Ordering::SeqCst) {
                    ("102.9", "103")
                } else {
                    ("100", "100.1")
                };
                let m = format!(
                    r#"{{"stream":"btcusdt@depth20@100ms","data":{{"lastUpdateId":{i},"bids":[["{b}","10"]],"asks":[["{a}","10"]]}}}}"#
                );
                if ws.send(Message::text(m)).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            loop {
                let _ = wl.accept().await;
            }
        });
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=binance&symbols=BTCUSDT&market=perp&leverage=5&wsUrl=ws://{waddr}/stream&restUrl=http://{raddr}&spreadCaptureEnabled=false&statArbEnabled=false&maxPositionQuote=1000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, m) = call(&r, "GET", "/market?exchange=binance", "").await;
            if m["BTCUSDT"]["лучший_аск"].is_number() {
                break;
            }
            assert!(tokio::time::Instant::now() < until, "стакан: {m}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=binance&symbol=BTCUSDT&side=SELL&type=MARKET&qty=1",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        // шорт от 100 — без стратегии, под защитой; цена в пределах стопа — держится
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, s) = call(&r, "GET", "/status", "").await;
            if s["binance"]["без_стратегии"]["позиции"] == json!(["BTCUSDT"]) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "сирота не замечена: {s}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // позиции и ставки — как в Java (PerpAccount.stats)
        let (_, p) = call(&r, "GET", "/positions?exchange=binance", "").await;
        let x = &p["binance"]["positions"][0];
        assert_eq!(
            (
                x["symbol"].clone(),
                x["side"].clone(),
                x["qty"].clone(),
                x["entryPrice"].clone()
            ),
            (json!("BTCUSDT"), json!("SHORT"), json!(1.0), json!(100.0)),
            "{p}"
        );
        assert_eq!(p["binance"]["leverage"], json!(5.0));
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, f) = call(&r, "GET", "/funding", "").await;
            if f["rates"]["binance"][0]["symbol"] == "BTCUSDT" {
                assert_eq!(f["rates"]["binance"][0]["intervalHours"], json!(8.0));
                break;
            }
            assert!(tokio::time::Instant::now() < until, "ставки нет: {f}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // метрики Prometheus: фид, счёт по котировке, стакан, сирота
        let (_, m) = text(&r, "/metrics", "").await;
        for want in [
            "hft_connected{exchange=\"binance\"} 1",
            "hft_trading_enabled{exchange=\"binance\"} 1",
            "hft_open_positions{exchange=\"binance\"} 1",
            "hft_account_equity_quote{exchange=\"binance\",quote=\"USDT\",mode=\"PAPER\"} ",
            "hft_best_bid{exchange=\"binance\",symbol=\"BTCUSDT\"} 100",
            "hft_orphan_positions{exchange=\"binance\"} 1",
        ] {
            assert!(m.contains(want), "нет «{want}» в\n{m}");
        }
        let (_, e) = call(&r, "GET", "/control/exchanges", "").await;
        assert!(
            e["поддерживаемые"]
                .as_array()
                .unwrap()
                .contains(&json!("binance")),
            "{e}"
        );
        up.store(true, Ordering::SeqCst);
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, s) = call(&r, "GET", "/status", "").await;
            if s["binance"]["тестовый_счёт"]["positions"] == json!([]) {
                assert_eq!(
                    s["binance"]["без_стратегии"],
                    json!({"позиции": [], "закрыто": 1}),
                    "{s}"
                );
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "сирота не закрыта: {s}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // новая позиция — закрыть через админку
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=binance&symbol=BTCUSDT&side=BUY&type=MARKET&qty=0.5",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        let (c, v) = call(
            &r,
            "POST",
            "/positions/close?exchange=binance&symbol=all",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        assert_eq!(
            v["closed"],
            json!([{"symbol": "BTCUSDT", "status": "FILLED", "executedQty": 0.5, "avgPrice": 102.9}]),
            "{v}"
        );
        let (_, v) = call(
            &r,
            "POST",
            "/positions/close?exchange=binance&symbol=BTCUSDT",
            "",
        )
        .await;
        assert_eq!(v["closed"][0]["status"], "позиции нет");
        call(&r, "POST", "/control/stop", "").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn live_perp_flow_against_mock_binance_futures() {
        use axum::extract::RawQuery;
        use axum::http::HeaderMap;
        use futures_util::SinkExt;
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        const SECRET: &str = "test-secret";
        #[derive(Default)]
        struct Ex {
            leverage: Vec<String>,
            /// позиция BTCUSDT
            pos: f64,
            orders: Vec<(u64, String)>,
            bad_sig: u32,
            /// стопы: запросы (POST) и снятые algoId (DELETE)
            stops: Vec<String>,
            stops_cancelled: Vec<String>,
        }
        let ex = Arc::new(Mutex::new(Ex::default()));
        fn param(q: &str, k: &str) -> Option<String> {
            q.split('&').find_map(|kv| {
                kv.split_once('=')
                    .filter(|(a, _)| *a == k)
                    .map(|(_, v)| v.to_owned())
            })
        }
        fn check(q: &str, h: &HeaderMap) -> bool {
            let (body, sig) = q.rsplit_once("&signature=").unwrap_or((q, ""));
            h.get("x-mbx-apikey").is_some_and(|k| k == "test-key")
                && crate::signed::sign(SECRET, body) == sig
        }
        let bad = || {
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"code": -1022, "msg": "Signature"})),
            )
        };
        let (e1, e2, e3, e4) = (ex.clone(), ex.clone(), ex.clone(), ex.clone());
        let (e5, e6) = (ex.clone(), ex.clone());
        let rest = Router::new()
            .route("/fapi/v1/time", get(|| async { Json(json!({"serverTime": now_ms()})) }))
            .route("/fapi/v1/exchangeInfo", get(|| async { Json(json!({"symbols": [{"symbol": "BTCUSDT", "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.1"}, {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"}]}]})) }))
            .route("/fapi/v1/klines", get(|| async { Json(json!([])) }))
            .route("/fapi/v1/premiumIndex", get(|| async {
                Json(json!({"symbol": "BTCUSDT", "markPrice": "100.5", "lastFundingRate": "0.00010000", "nextFundingTime": now_ms() + 3_600_000}))
            }))
            .route("/fapi/v1/listenKey", axum::routing::post(|h: HeaderMap| async move {
                assert_eq!(h.get("x-mbx-apikey").unwrap(), "test-key");
                Json(json!({"listenKey": "lk1"}))
            }))
            .route("/fapi/v1/leverage", axum::routing::post(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let mut x = e1.lock().unwrap();
                if !check(&q, &h) { x.bad_sig += 1; return bad(); }
                x.leverage.push(format!("{}={}", param(&q, "symbol").unwrap(), param(&q, "leverage").unwrap()));
                (StatusCode::OK, Json(json!({"leverage": 5})))
            }))
            .route("/fapi/v2/balance", get(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let mut x = e2.lock().unwrap();
                if !check(&q, &h) { x.bad_sig += 1; return bad(); }
                (StatusCode::OK, Json(json!([{"asset": "USDT", "availableBalance": "1000", "balance": "1000"}])))
            }))
            .route("/fapi/v2/positionRisk", get(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let mut x = e3.lock().unwrap();
                if !check(&q, &h) { x.bad_sig += 1; return bad(); }
                (StatusCode::OK, Json(json!([{"symbol": "BTCUSDT", "positionAmt": format!("{}", x.pos), "entryPrice": "100"}])))
            }))
            .route("/fapi/v1/algoOrder", axum::routing::post(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let mut x = e5.lock().unwrap();
                if !check(&q, &h) { x.bad_sig += 1; }
                x.stops.push(q);
                Json(json!({"algoId": 777}))
            }).delete(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let mut x = e6.lock().unwrap();
                if !check(&q, &h) { x.bad_sig += 1; }
                x.stops_cancelled.push(param(&q, "algoId").unwrap());
                Json(json!({"code": "200"}))
            }))
            .route("/fapi/v1/order", axum::routing::post(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let mut x = e4.lock().unwrap();
                if !check(&q, &h) { x.bad_sig += 1; return bad(); }
                let qty: f64 = param(&q, "quantity").unwrap().parse().unwrap();
                let id = 500 + x.orders.len() as u64;
                x.orders.push((id, param(&q, "newClientOrderId").unwrap()));
                x.pos += if param(&q, "side").as_deref() == Some("SELL") { -qty } else { qty };
                // как у фьючерсов: сумма — cumQuote
                (StatusCode::OK, Json(json!({"orderId": id, "status": "FILLED", "executedQty": format!("{qty}"), "cumQuote": format!("{}", qty * 100.0)})))
            }));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        let wl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = wl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = wl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for i in 1..500u64 {
                let m = format!(
                    r#"{{"stream":"btcusdt@depth20@100ms","data":{{"lastUpdateId":{i},"bids":[["100","10"]],"asks":[["100.1","10"]]}}}}"#
                );
                if ws.send(Message::text(m)).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = wl.accept().await;
            }
        });
        // поток событий перпов по listenKey: путь /lk1, событие ACCOUNT_UPDATE по запросу теста
        let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let path = Arc::new(Mutex::new(String::new()));
        let path2 = path.clone();
        let fl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let faddr = fl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = fl.accept().await.unwrap();
            #[allow(clippy::result_large_err)]
            let cb = |req: &tokio_tungstenite::tungstenite::handshake::server::Request, resp| {
                *path2.lock().unwrap() = req.uri().path().to_owned();
                Ok(resp)
            };
            let mut ws = tokio_tungstenite::accept_hdr_async(tcp, cb).await.unwrap();
            while let Some(e) = ev_rx.recv().await {
                if ws.send(Message::text(e)).await.is_err() {
                    break;
                }
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = fl.accept().await;
            }
        });
        // переопределения окружения для этого теста (потокобезопасно)
        crate::util::set_env("BINANCE_FSTREAM_URL", &format!("ws://{faddr}"));
        crate::util::set_env("BINANCE_TESTNET_API_KEY", "test-key");
        crate::util::set_env("BINANCE_TESTNET_API_SECRET", SECRET);
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=binance&symbols=BTCUSDT&market=perp&leverage=5&testnet=true&wsUrl=ws://{waddr}/stream&restUrl=http://{raddr}&spreadCaptureEnabled=false&maxPositionQuote=1000&exchangeStopLossPercent=2.04"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, s) = call(&r, "GET", "/status", "").await;
            let (_, m) = call(&r, "GET", "/market?exchange=binance", "").await;
            let (_, b) = call(&r, "GET", "/balances?exchange=binance", "").await;
            if b["свободно"]["USDT"] == json!(1000.0)
                && s["binance"]["поток_событий_счёта"] == json!(true)
                && m["BTCUSDT"]["funding_проц"] == json!(0.01)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "баланс, поток или funding: {s} {m} {b}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(*path.lock().unwrap(), "/lk1");
        assert_eq!(ex.lock().unwrap().leverage, vec!["BTCUSDT=5".to_string()]);
        // шорт без монет — на перпах можно
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=binance&symbol=BTCUSDT&side=SELL&type=MARKET&qty=2",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            let ps = &a["binance"]["тестовый_счёт"]["positions"];
            if ps[0]["qty"] == json!(-2.0) {
                assert_eq!(a["binance"]["тестовый_счёт"]["market"], json!("perp"));
                assert_eq!(ps[0]["margin"], json!(40.0), "100 × 2 / 5");
                break;
            }
            assert!(tokio::time::Instant::now() < until, "шорт не учтён: {a}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // стоп на бирже: шорт от 100 — BUY на 2.04 % выше, вверх до шага 0.1 → 102.1, closePosition по маркировке
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            if a["binance"]["тестовый_счёт"]["positions"][0]["stop"] == json!(102.1) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "стоп не поставлен: {a}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        {
            let x = ex.lock().unwrap();
            assert_eq!(x.stops.len(), 1, "стоп ставится один раз: {:?}", x.stops);
            let q = &x.stops[0];
            for (k, v) in [
                ("algoType", "CONDITIONAL"),
                ("side", "BUY"),
                ("type", "STOP_MARKET"),
                ("triggerPrice", "102.1"),
                ("closePosition", "true"),
                ("workingType", "MARK_PRICE"),
            ] {
                assert_eq!(param(q, k).as_deref(), Some(v), "{k} в {q}");
            }
        }
        // позиция закрыта на бирже (например, вручную) — событие ACCOUNT_UPDATE убирает её из учёта
        ev_tx.send(json!({"e": "ACCOUNT_UPDATE", "a": {"B": [], "P": [{"s": "BTCUSDT", "pa": "0", "ep": "0"}]}}).to_string()).unwrap();
        let until = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            if a["binance"]["тестовый_счёт"]["positions"] == json!([]) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "событие не учтено: {a}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // позиции нет — стоп снят
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        while ex.lock().unwrap().stops_cancelled.is_empty() {
            assert!(tokio::time::Instant::now() < until, "стоп не снят");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(ex.lock().unwrap().stops_cancelled, ["777"]);
        // метрики Rust-процесса: запросы к бирже по видам, задержка ордеров, рантайм tokio, процесс
        let (_, m) = text(&r, "/metrics", "").await;
        for want in [
            "hft_requests_total{exchange=\"binance\",kind=\"order\"} ",
            "hft_requests_total{exchange=\"binance\",kind=\"stop\"} ",
            "hft_requests_total{exchange=\"binance\",kind=\"leverage\"} ",
            "hft_order_latency_p99_micros{exchange=\"binance\"} ",
            "hft_tokio_workers ",
            "hft_tokio_alive_tasks ",
            "hft_process_cpu_seconds_total ",
            "hft_process_open_fds ",
        ] {
            assert!(m.contains(want), "нет «{want}» в\n{m}");
        }
        assert_eq!(ex.lock().unwrap().bad_sig, 0, "все подписи верны");
        call(&r, "POST", "/control/stop", "").await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn live_testnet_flow_against_mock_bybit() {
        use axum::http::HeaderMap;
        use futures_util::{SinkExt, StreamExt};
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        const SECRET: &str = "bybit-secret";
        #[derive(Default)]
        struct Ex {
            usdt: f64,
            btc: f64,
            /// номер, orderLinkId, статус, исполнено, сумма
            orders: Vec<(String, String, String, f64, f64)>,
            bad_sig: u32,
        }
        let ex = Arc::new(Mutex::new(Ex {
            usdt: 1000.0,
            ..Default::default()
        }));
        // подпись: время + ключ + recvWindow + (строка запроса | тело)
        fn check(h: &HeaderMap, payload: &str) -> bool {
            let g = |k: &str| {
                h.get(k)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned()
            };
            g("x-bapi-api-key") == "bybit-key"
                && crate::signed::sign(
                    SECRET,
                    &format!(
                        "{}{}{}{payload}",
                        g("x-bapi-timestamp"),
                        "bybit-key",
                        g("x-bapi-recv-window")
                    ),
                ) == g("x-bapi-sign")
        }
        let bad = || Json(json!({"retCode": 10004, "retMsg": "sign"}));
        let (e1, e2, e3) = (ex.clone(), ex.clone(), ex.clone());
        let rest = Router::new()
            .route("/v5/market/time", get(|| async { Json(json!({"retCode": 0, "time": now_ms()})) }))
            .route("/v5/account/wallet-balance", get(move |axum::extract::RawQuery(q): axum::extract::RawQuery, h: HeaderMap| async move {
                let mut x = e1.lock().unwrap();
                if !check(&h, &q.unwrap_or_default()) { x.bad_sig += 1; return bad(); }
                Json(json!({"retCode": 0, "result": {"list": [{"coin": [
                    {"coin": "USDT", "walletBalance": format!("{}", x.usdt), "locked": "0"},
                    {"coin": "BTC", "walletBalance": format!("{}", x.btc), "locked": "0"}]}]}}))
            }))
            .route("/v5/order/create", axum::routing::post(move |h: HeaderMap, body: String| async move {
                let mut x = e2.lock().unwrap();
                if !check(&h, &body) { x.bad_sig += 1; return bad(); }
                let b: Value = serde_json::from_str(&body).unwrap();
                let qty: f64 = b["qty"].as_str().unwrap().parse().unwrap();
                let id = format!("uuid-{}", x.orders.len());
                let link = b["orderLinkId"].as_str().unwrap().to_owned();
                if b["orderType"] == "Market" {
                    assert_eq!(b["marketUnit"], "baseCoin");
                    x.usdt -= qty * 101.0;
                    x.btc += qty;
                    x.orders.push((id.clone(), link, "Filled".into(), qty, qty * 101.0));
                } else {
                    x.orders.push((id.clone(), link, "New".into(), 0.0, 0.0));
                }
                Json(json!({"retCode": 0, "result": {"orderId": id, "orderLinkId": b["orderLinkId"]}}))
            }))
            .route("/v5/order/realtime", get(move |axum::extract::RawQuery(q): axum::extract::RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let x = e3.lock().unwrap();
                if !check(&h, &q) { return bad(); }
                let id = q.split('&').find_map(|kv| kv.strip_prefix("orderId=")).unwrap_or("");
                let list: Vec<Value> = x.orders.iter().filter(|o| o.0 == id).map(|o| json!({"orderId": o.0, "orderLinkId": o.1,
                    "orderStatus": o.2, "cumExecQty": format!("{}", o.3), "cumExecValue": format!("{}", o.4)})).collect();
                Json(json!({"retCode": 0, "result": {"list": list}}))
            }));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        // публичный стакан
        let wl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = wl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = wl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for i in 1..500u64 {
                let m = format!(
                    r#"{{"topic":"orderbook.50.BTCUSDT","type":"snapshot","data":{{"s":"BTCUSDT","b":[["100","10"]],"a":[["101","10"]],"u":{i},"seq":{i}}}}}"#
                );
                if ws.send(Message::text(m)).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = wl.accept().await;
            }
        });
        // приватный поток: проверить вход, подписку, затем слать события
        let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let auth_ok = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let auth_ok2 = auth_ok.clone();
        let pl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let paddr = pl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = pl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let Some(Ok(Message::Text(t))) = ws.next().await else {
                return;
            };
            let v: Value = serde_json::from_str(&t).unwrap();
            let a = &v["args"];
            let ok = v["op"] == "auth"
                && a[0] == "bybit-key"
                && crate::signed::sign(SECRET, &format!("GET/realtime{}", a[1]))
                    == a[2].as_str().unwrap();
            auth_ok2.store(ok, std::sync::atomic::Ordering::SeqCst);
            ws.send(Message::text(
                json!({"op": "auth", "success": ok, "ret_msg": ""}).to_string(),
            ))
            .await
            .unwrap();
            let Some(Ok(Message::Text(t))) = ws.next().await else {
                return;
            };
            let v: Value = serde_json::from_str(&t).unwrap();
            assert_eq!(v["args"], json!(["order", "wallet"]));
            ws.send(Message::text(
                json!({"op": "subscribe", "success": true}).to_string(),
            ))
            .await
            .unwrap();
            while let Some(e) = ev_rx.recv().await {
                if ws.send(Message::text(e)).await.is_err() {
                    break;
                }
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = pl.accept().await;
            }
        });
        // переопределения окружения для этого теста (потокобезопасно)
        crate::util::set_env("BYBIT_WS_PRIVATE_URL", &format!("ws://{paddr}"));
        crate::util::set_env("BYBIT_TESTNET_API_KEY", "bybit-key");
        crate::util::set_env("BYBIT_TESTNET_API_SECRET", SECRET);
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=bybit&symbols=BTCUSDT&market=spot&testnet=true&wsUrl=ws://{waddr}&restUrl=http://{raddr}&spreadCaptureEnabled=false&maxPositionQuote=1000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let (_, rs) = call(&r, "GET", "/exchanges/request-stats", "").await;
        assert_eq!(rs["bybit"]["mode"], json!("TESTNET"), "{rs}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=bybit", "").await;
            if b["свободно"]["USDT"] == json!(1000.0) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "баланс не получен: {b}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=bybit&symbol=BTCUSDT&side=BUY&type=MARKET&qty=1",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=bybit", "").await;
            if b["свободно"]["BTC"] == json!(1.0) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "покупка не учтена: {b}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, s) = call(&r, "GET", "/status", "").await;
            if s["bybit"]["поток_событий_счёта"] == json!(true) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "поток событий не подключён: {s}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            auth_ok.load(std::sync::atomic::Ordering::SeqCst),
            "подпись входа верна"
        );
        let (_, v) = call(
            &r,
            "POST",
            "/order?exchange=bybit&symbol=BTCUSDT&side=BUY&type=LIMIT&qty=1&price=99",
            "",
        )
        .await;
        assert_eq!(v["статус"], json!("NEW"), "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        let (oid, link) = loop {
            if let Some(o) = ex.lock().unwrap().orders.get(1).cloned() {
                break (o.0, o.1);
            }
            assert!(
                tokio::time::Instant::now() < until,
                "лимитная заявка не дошла до биржи"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        // исполнение узнаём из потока (опрос — раз в 5 с, а в мок-REST ордер остаётся New)
        ev_tx.send(json!({"topic": "order", "data": [{"orderId": oid, "orderLinkId": link, "orderStatus": "Filled",
            "cumExecQty": "1", "cumExecValue": "99"}]}).to_string()).unwrap();
        ev_tx.send(json!({"topic": "wallet", "data": [{"coin": [{"coin": "BTC", "walletBalance": "2", "locked": "0"}]}]}).to_string()).unwrap();
        let until = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let (_, a) = call(&r, "GET", "/status", "").await;
            let (_, b) = call(&r, "GET", "/balances?exchange=bybit", "").await;
            if b["свободно"]["BTC"] == json!(2.0)
                && a["bybit"]["тестовый_счёт"]["openOrders"] == json!(0)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "событие не учтено: {b} {a}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(ex.lock().unwrap().bad_sig, 0, "все подписи REST верны");
        call(&r, "POST", "/control/stop", "").await;
    }

    #[tokio::test]
    async fn live_testnet_flow_against_mock_binance() {
        use axum::extract::RawQuery;
        use axum::http::HeaderMap;
        use futures_util::SinkExt;
        use std::sync::Mutex;
        use tokio_tungstenite::tungstenite::Message;
        const SECRET: &str = "test-secret";
        #[derive(Default)]
        struct Ex {
            usdt: f64,
            btc: f64,
            orders: Vec<(u64, String, f64, f64)>,
            bad_sig: u32,
            cancels: u32,
            /// номер ордера на бирже → clientOrderId
            clients: Vec<(u64, String)>,
        }
        let ex = Arc::new(Mutex::new(Ex {
            usdt: 1000.0,
            ..Default::default()
        }));
        fn param(q: &str, k: &str) -> Option<String> {
            q.split('&').find_map(|kv| {
                kv.split_once('=')
                    .filter(|(a, _)| *a == k)
                    .map(|(_, v)| v.to_owned())
            })
        }
        // подпись: всё до &signature=
        fn check(q: &str, h: &HeaderMap) -> bool {
            let (body, sig) = q.rsplit_once("&signature=").unwrap_or((q, ""));
            h.get("x-mbx-apikey").is_some_and(|k| k == "test-key")
                && crate::signed::sign(SECRET, body) == sig
        }
        let (e1, e2, e3, e4) = (ex.clone(), ex.clone(), ex.clone(), ex.clone());
        let rest = Router::new()
            .route("/api/v3/time", get(|| async { Json(json!({"serverTime": now_ms()})) }))
            .route("/api/v3/exchangeInfo", get(|| async { Json(json!({"symbols": [{"symbol": "BTCUSDT", "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.01"}, {"filterType": "LOT_SIZE", "stepSize": "0.0001", "minQty": "0.0001"}]}]})) }))
            .route("/api/v3/klines", get(|| async { Json(json!([])) }))
            .route("/api/v3/account", get(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let mut x = e1.lock().unwrap();
                if !check(&q, &h) { x.bad_sig += 1; return (StatusCode::UNAUTHORIZED, Json(json!({"code": -1022, "msg": "Signature"}))); }
                (StatusCode::OK, Json(json!({"balances": [{"asset": "USDT", "free": format!("{}", x.usdt)}, {"asset": "BTC", "free": format!("{}", x.btc)}]})))
            }))
            .route("/api/v3/order", axum::routing::post(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let mut x = e2.lock().unwrap();
                if !check(&q, &h) { x.bad_sig += 1; return (StatusCode::UNAUTHORIZED, Json(json!({"code": -1022, "msg": "Signature"}))); }
                let qty: f64 = param(&q, "quantity").unwrap().parse().unwrap();
                let id = 1000 + x.orders.len() as u64;
                x.clients.push((id, param(&q, "newClientOrderId").unwrap_or_default()));
                if param(&q, "type").as_deref() == Some("MARKET") {
                    x.usdt -= qty * 101.0;
                    x.btc += qty;
                    x.orders.push((id, "FILLED".into(), qty, qty * 101.0));
                    (StatusCode::OK, Json(json!({"orderId": id, "status": "FILLED", "executedQty": format!("{qty}"), "cummulativeQuoteQty": format!("{}", qty * 101.0)})))
                } else {
                    x.orders.push((id, "NEW".into(), 0.0, 0.0));
                    (StatusCode::OK, Json(json!({"orderId": id, "status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0"})))
                }
            }).get(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let x = e3.lock().unwrap();
                if !check(&q, &h) { return (StatusCode::UNAUTHORIZED, Json(json!({"code": -1022, "msg": "Signature"}))); }
                let id: u64 = param(&q, "orderId").unwrap().parse().unwrap();
                let o = x.orders.iter().find(|o| o.0 == id).unwrap();
                (StatusCode::OK, Json(json!({"orderId": id, "status": o.1, "executedQty": format!("{}", o.2), "cummulativeQuoteQty": format!("{}", o.3)})))
            }).delete(move |RawQuery(q): RawQuery, h: HeaderMap| async move {
                let q = q.unwrap_or_default();
                let mut x = e4.lock().unwrap();
                if !check(&q, &h) { return (StatusCode::UNAUTHORIZED, Json(json!({"code": -1022, "msg": "Signature"}))); }
                let id: u64 = param(&q, "orderId").unwrap().parse().unwrap();
                x.cancels += 1;
                let o = x.orders.iter_mut().find(|o| o.0 == id).unwrap();
                o.1 = "CANCELED".into();
                (StatusCode::OK, Json(json!({"orderId": id, "status": "CANCELED", "executedQty": "0", "cummulativeQuoteQty": "0"})))
            }));
        let rl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raddr = rl.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(rl, rest).await.unwrap() });
        // стакан
        let wl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let waddr = wl.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = wl.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            for i in 1..500u64 {
                let m = format!(
                    r#"{{"stream":"btcusdt@depth20@100ms","data":{{"lastUpdateId":{i},"bids":[["100","10"]],"asks":[["101","10"]]}}}}"#
                );
                if ws.send(Message::text(m)).await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = wl.accept().await;
            }
        });
        // WebSocket API: проверить подпись подписки на события счёта, затем слать события из канала
        let (ev_tx, mut ev_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let sub_ok = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sub_ok2 = sub_ok.clone();
        let al = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let aaddr = al.local_addr().unwrap();
        tokio::spawn(async move {
            use futures_util::StreamExt;
            let (tcp, _) = al.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let Some(Ok(Message::Text(t))) = ws.next().await else {
                return;
            };
            let v: Value = serde_json::from_str(&t).unwrap();
            let p = &v["params"];
            let q = format!(
                "apiKey={}&timestamp={}",
                p["apiKey"].as_str().unwrap(),
                p["timestamp"]
            );
            let ok = v["method"] == "userDataStream.subscribe.signature"
                && p["apiKey"] == "test-key"
                && crate::signed::sign(SECRET, &q) == p["signature"].as_str().unwrap();
            sub_ok2.store(ok, std::sync::atomic::Ordering::SeqCst);
            let reply = json!({"id": v["id"], "status": if ok { 200 } else { 401 }, "result": {"subscriptionId": 0}});
            ws.send(Message::text(reply.to_string())).await.unwrap();
            while let Some(e) = ev_rx.recv().await {
                if ws.send(Message::text(e)).await.is_err() {
                    break;
                }
            }
            // порт держится до конца теста: иначе его получит мок другого теста, а переподключение этого бота попадёт туда
            loop {
                let _ = al.accept().await;
            }
        });
        // переопределения окружения для этого теста (потокобезопасно)
        crate::util::set_env("BINANCE_WS_API_URL", &format!("ws://{aaddr}"));
        crate::util::set_env("BINANCE_TESTNET_API_KEY", "test-key");
        crate::util::set_env("BINANCE_TESTNET_API_SECRET", SECRET);
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=binance&symbols=BTCUSDT&market=spot&testnet=true&wsUrl=ws://{waddr}/stream&restUrl=http://{raddr}&spreadCaptureEnabled=false&maxPositionQuote=1000"
        );
        assert_eq!(call(&r, "POST", &sel, "").await.0, StatusCode::OK);
        call(&r, "POST", "/control/start", "").await;
        call(&r, "POST", "/trading/start", "").await;
        let (_, rs) = call(&r, "GET", "/exchanges/request-stats", "").await;
        assert_eq!(rs["binance"]["mode"], json!("TESTNET"), "{rs}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=binance", "").await;
            if b["свободно"]["USDT"] == json!(1000.0) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "баланс не получен: {b}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (c, v) = call(
            &r,
            "POST",
            "/order?exchange=binance&symbol=BTCUSDT&side=BUY&type=MARKET&qty=1",
            "",
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{v}");
        assert_eq!(v["статус"], json!("NEW"), "ордер ушёл на биржу в фоне");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=binance", "").await;
            if b["свободно"]["BTC"] == json!(1.0) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "покупка не учтена: {b}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // поток событий счёта: лимитная покупка исполнилась — узнаём из события, а не из опроса (он раз в 5 с)
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let (_, s) = call(&r, "GET", "/status", "").await;
            if s["binance"]["поток_событий_счёта"] == json!(true) {
                break;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "поток событий не подключён: {s}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            sub_ok.load(std::sync::atomic::Ordering::SeqCst),
            "подпись подписки верна"
        );
        let (_, v) = call(
            &r,
            "POST",
            "/order?exchange=binance&symbol=BTCUSDT&side=BUY&type=LIMIT&qty=1&price=99",
            "",
        )
        .await;
        assert_eq!(v["статус"], json!("NEW"), "{v}");
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        let (oid, client) = loop {
            if let Some(c) = ex
                .lock()
                .unwrap()
                .clients
                .last()
                .filter(|c| c.1.starts_with("hft"))
                .cloned()
                .filter(|c| c.0 >= 1001)
            {
                break c;
            }
            assert!(
                tokio::time::Instant::now() < until,
                "лимитная заявка не дошла до биржи"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        {
            let mut x = ex.lock().unwrap();
            x.btc += 1.0;
            x.usdt -= 99.0;
        }
        ev_tx.send(json!({"subscriptionId": 0, "event": {"e": "executionReport", "s": "BTCUSDT", "c": client, "C": "",
            "S": "BUY", "X": "FILLED", "i": oid, "z": "1", "Z": "99"}}).to_string()).unwrap();
        let t0 = tokio::time::Instant::now();
        loop {
            let (_, b) = call(&r, "GET", "/balances?exchange=binance", "").await;
            if b["свободно"]["BTC"] == json!(2.0) {
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_millis(1500),
                "исполнение из события не учтено: {b}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let (_, s) = call(&r, "GET", "/status", "").await;
        assert!(s["binance"]["событий_счёта"].as_u64().unwrap() >= 1);
        let (_, v) = call(
            &r,
            "POST",
            "/order?exchange=binance&symbol=BTCUSDT&side=SELL&type=LIMIT&qty=1&price=150",
            "",
        )
        .await;
        assert_eq!(v["статус"], json!("NEW"), "{v}");
        tokio::time::sleep(Duration::from_millis(300)).await;
        call(&r, "POST", "/trading/panic", "").await;
        let until = tokio::time::Instant::now() + Duration::from_secs(15);
        while ex.lock().unwrap().cancels == 0 {
            assert!(
                tokio::time::Instant::now() < until,
                "заявка не снята на бирже"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (_, s) = call(&r, "GET", "/status", "").await;
        assert_eq!(
            s["binance"]["тестовый_счёт"]["mode"],
            json!("TESTNET"),
            "{s}"
        );
        assert_eq!(s["binance"]["ордеров_принято"], json!(3));
        assert_eq!(ex.lock().unwrap().bad_sig, 0, "все запросы подписаны верно");
        call(&r, "POST", "/control/stop", "").await;
    }
}
