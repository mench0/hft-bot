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
        .route("/market", get(market))
        .route("/strategies", get(strategies))
        .route("/order", axum::routing::post(order))
        .route("/cancel-all", axum::routing::post(cancel_all))
        .route("/balances", get(balances))
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
    if req.method() == Method::OPTIONS || app.cfg.token.is_empty() {
        return next.run(req).await;
    }
    let got = req
        .headers()
        .get("x-admin-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
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
            m.insert(ex.clone(), json!({
                "mode": "PAPER",
                "modeTitle": "Rust-бэкенд: только рыночные данные, ордеров пока нет",
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
            });
            if let Some(acc) = bot.account_json(ex) {
                let a = bot.accounts.lock().unwrap();
                if let Some(p) = a.get(ex) {
                    e["kill_switch"] = Value::Bool(p.kill_switch);
                    e["дневной_pnl"] = json!(p.daily_pnl);
                    e["ордеров_принято"] = json!(p.accepted);
                    e["ордеров_отклонено"] = json!(p.rejected);
                    e["последний_отказ"] = Value::String(p.last_reject.clone());
                }
                e["тестовый_счёт"] = acc;
            }
            if !f.last_error.is_empty() {
                e["ошибка"] = Value::String(f.last_error);
            }
            if !f.unsupported.is_empty() {
                e["нет_фида"] = Value::String(f.unsupported);
            }
            m.insert(ex.clone(), e);
        }
    }
    Value::Object(m)
}

fn discovery_json(_bot: &Bot) -> Value {
    json!({
        "status": "Подбор тикеров в Rust-бэкенде ещё не реализован",
        "running": false,
        "refreshSeconds": 0,
        "chosenExchanges": [],
        "availableExchanges": data::exchange_ids().into_iter().filter(|x| x != "uniswapv2").collect::<Vec<_>>(),
    })
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
                },
            ),
        )
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
            call(&r, "GET", "/positions", "").await.0,
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
        });
        let (_d, r) = app("");
        let sel = format!(
            "/control/select?exchange=binance&symbols=XYZUSDT&market=spot&wsUrl=ws://{addr}/stream&strategy=spread-capture\
            &scRequoteMs=200&scStopLossPercent=50&scMaxDropPercent=0&scMinBookLevels=0&scMinDepthMultiple=0&scMaxTopChangesPerSec=0&maxOrdersPerMinute=10000"
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
}
