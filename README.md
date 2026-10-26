# hft-bot — HFT-бот на Rust

Второй бэкенд бота, параллельно Java-версии ([hft-trader](https://github.com/mench0/hft-trader)), которая остаётся как есть.
Админ-API повторяет формат Java-бота, поэтому к нему подключается та же админка ([hft-admin](https://github.com/mench0/hft-admin)):
в шапке админки укажите адрес Rust-бэкенда и его `ADMIN_TOKEN`.

## Что уже есть (каркас)

- HTTP-сервер админки (tokio + axum): токен в заголовке `X-Admin-Token`, CORS, ошибки в формате `{"результат": "ошибка", "сообщение": "..."}`.
- Состояние в `state.json`: выбранные биржи и монеты, привязка монет к стратегиям, параметры бирж, настройки процесса.
  Запись через временный файл — файл не бывает записан наполовину.
- Адреса: `/control/status`, `/control/select`, `/control/symbols`, `/control/retire`, `/control/deselect`,
  `/control/start`, `/control/stop`, `/status`, `/discovery`, `/settings` (GET/POST), `/exchange/params` (GET/POST),
  `/exchange/params/schema`, `/exchanges/catalog`, `/exchanges/request-stats`, `/requests/live`, `/events` (SSE), `/health`.
- Проверка параметров по той же схеме, что в Java (`data/` — ответы Java-бота: каталог бирж и схемы параметров).

## Чего ещё нет

Подключений к биржам (стаканы по WebSocket, ордера), подбора тикеров, стратегий, позиций и балансов.
«Поднять соединения» пока только меняет признак «запущен». Адреса, которых нет, отвечают 404 «Этого адреса в Rust-бэкенде пока нет».

## Запуск

```bash
cargo build --release
ADMIN_TOKEN=секрет ADMIN_PORT=8081 ./target/release/hft-bot
```

| Переменная | По умолчанию | Что задаёт |
|---|---|---|
| `ADMIN_PORT` | 8080 | порт админки (если Java-бот на 8080 — укажите другой) |
| `ADMIN_BIND` | 127.0.0.1 | адрес; наружу не открывайте, используйте SSH-туннель |
| `ADMIN_TOKEN` | — | токен админки; пусто — без проверки |
| `ADMIN_CORS_ORIGINS` | — | адреса страниц админки через запятую; пусто — любые |
| `STATE_FILE` | state.json | файл состояния |
| `RUST_LOG` | info | уровень логов |

## Проверки

```bash
cargo test      # состояние, проверка параметров, адреса админки, поток /events
cargo clippy --all-targets
```
