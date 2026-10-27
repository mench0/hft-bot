# hft-bot — HFT-бот на Rust

Второй бэкенд бота, параллельно Java-версии ([hft-trader](https://github.com/mench0/hft-trader)), которая остаётся
как есть. Админ-API повторяет формат Java-бота, поэтому к нему подключается та же админка
([hft-admin](https://github.com/mench0/hft-admin)): в шапке админки укажите адрес Rust-бэкенда и его `ADMIN_TOKEN`.

> **Как проверено.** Все биржевые части проверены тестами против локальных моков (подписи, форматы запросов и
> ответов взяты из документации бирж и Java-бота). С живыми биржами из среды разработки связи нет — перед реальной
> торговлей каждую биржу нужно прогнать на testnet / малых суммах.

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Структура кода](#структура-кода)
- [Биржи](#биржи)
- [Ордера: WebSocket в первую очередь, REST — запасной путь](#ордера-websocket-в-первую-очередь-rest--запасной-путь)
- [Рыночные данные](#рыночные-данные)
- [Счета: бумага и биржа](#счета-бумага-и-биржа)
- [Риск и защита позиций](#риск-и-защита-позиций)
- [Стратегии](#стратегии)
- [Подбор тикеров, перепроверка, листинги](#подбор-тикеров-перепроверка-листинги)
- [Админ-API](#админ-api)
- [Наблюдение: метрики, запросы, журнал](#наблюдение-метрики-запросы-журнал)
- [Хранение состояния и окружение](#хранение-состояния-и-окружение)
- [Переменные окружения](#переменные-окружения)
- [Проверки](#проверки)
- [Чего нет](#чего-нет)

## Быстрый старт

```bash
cargo build --release
ADMIN_TOKEN=секрет ADMIN_PORT=8081 ./target/release/hft-bot
```

Админка: `http://127.0.0.1:8081/control/status` (заголовок `X-Admin-Token`). Наружу порт не открывайте — только через
SSH-туннель. Дальше в админке: выбрать биржи и монеты → «Поднять соединения» → включить торговлю.

Реальные ордера идут, когда в параметрах биржи `live` или `testnet` (и не `paper`) и заданы ключи биржи
(`<БИРЖА>_API_KEY` / `_SECRET`, для тестовой сети — `<БИРЖА>_TESTNET_API_KEY` / `_SECRET`). Без ключей — бумага.

## Структура кода

```
src/
├── main.rs            запуск: логи, состояние, HTTP-сервер админки
├── server/            админ-API и состояние бота
│   ├── api.rs         маршруты axum в формате Java-бота (+ сквозные тесты против моков бирж)
│   └── state.rs       Bot: выбор бирж и монет, параметры, запуск фидов, счетов и стратегий, движок (шаг 50 мс)
├── exchange/          подписанные API бирж: ордера, балансы, позиции, стопы, приватные потоки, сокеты ордеров
│   ├── binance.rs     Binance спот и USDⓈ-M (REST, ws-api, ws-fapi)
│   ├── bybit.rs  okx.rs  gate.rs  kucoin.rs  bingx.rs  mexc.rs
│   ├── hyperliquid.rs aster.rs  lbank.rs  upbit.rs  uniswap.rs
│   ├── evm.rs         keccak, подпись кошельком, EIP-712 (Aster, Hyperliquid, Uniswap)
│   ├── rest.rs        публичный REST: правила пар, минутные свечи, ставки funding
│   └── transfer.rs    переводы между биржами (сети, адрес депозита, вывод)
├── market/            рыночные данные
│   ├── mod.rs         стаканы всех бирж и состояние фидов
│   ├── book.rs        локальный стакан символа
│   └── feed/          фиды стаканов по WebSocket: общий цикл (mod.rs) и диалект каждой биржи
├── trading/           счета
│   ├── account.rs     общий интерфейс: бумага или живой счёт
│   ├── paper.rs       бумажный учёт (спот и перпы), риск, kill switch
│   ├── live.rs        живой счёт: фоновая задача ордеров, приватный поток, отдельный торговый сокет
│   └── trade_ws.rs    сокет ордеров: запрос по id → ответ, таймаут, «исход неизвестен»
├── strategy/          spread.rs (сбор спреда), spotarb.rs, statarb.rs, perparb.rs, leadlag.rs
├── discovery/         screener.rs (подбор тикеров), signals.rs (листинги, анонсы, Discord)
└── infra/             metrics.rs, ratelimit.rs, journal.rs, store.rs (SQLite), data.rs (справочники), util.rs
data/                  каталог бирж и схемы параметров — ответы Java-бота, чтобы формат совпадал
```

## Биржи

| Биржа | Рынки | Стакан (WS) | Ордера | Приватный поток |
|---|---|---|---|---|
| Binance | спот, USDⓈ-M | `depth20@100ms` + `bookTicker` | **WS**: спот — ws-api, перпы — ws-fapi | спот — ws-api `userDataStream.subscribe`, перпы — listenKey |
| Bybit | спот, linear | `orderbook.50` + `orderbook.1` | **WS**: `/v5/trade` | `/v5/private`: order, wallet, position |
| OKX | спот, SWAP | `books` + `bbo-tbt` | **WS**: приватный сокет (`op order`) | login → orders, account, positions |
| Gate | спот, USDT-перпы | `order_book` + `book_ticker` | **WS**: WS API (`*.order_place` после `*.login`) | spot.orders/balances, futures.orders/positions |
| KuCoin | спот, перпы | `level2Depth50` | **WS**: Pro WS API (`spot.order` / `futures.order`) | bullet-private: tradeOrders, balance, positions |
| Hyperliquid | перпы, спот | `l2Book` | **WS**: тот же сокет (`method post`) | orderUpdates, userFills |
| BingX | спот, перпы | `depth20` (gzip) | REST | listenKey |
| MEXC | спот, перпы | protobuf / `sub.depth.full` | REST | listenKey (protobuf) / `edge` |
| Aster | спот, перпы | как Binance | REST (подпись EIP-712 агента) | listenKey |
| LBank | спот | `depth` | REST (MD5 → HMAC) | ключ подписки |
| Upbit | спот за USDT | `orderbook` | REST (JWT) | `/websocket/v1/private` (JWT) |
| Uniswap V2 | спот (AMM) | нода: `eth_subscribe` Sync + `getReserves` | своп через Router02 | — (итог по квитанции) |

Подробности по биржам (ключи и подписи):

- **Binance** — HMAC-SHA256, поправка на время сервера. **Bybit** — REST v5, единый счёт.
- **OKX** — Base64(HMAC-SHA256) в заголовках OK-ACCESS-*, нужен `OKX_PASSPHRASE`; демо — заголовок
  `x-simulated-trading`; перпы — cross, объём в контрактах (монеты / ctVal).
- **Gate** — hex(HMAC-SHA512); рыночная покупка спота — суммой в котируемой валюте (цена стакана + 0.5 %);
  перпы — контракты со знаком (монеты / quanto_multiplier).
- **KuCoin** — KC-API-SIGN = base64(HMAC-SHA256), passphrase тоже HMAC (ключ v2), нужен `KUCOIN_PASSPHRASE`; перпы —
  целые лоты, CROSS, плечо в каждом ордере. Счёт UTA (`KUCOIN_UTA=true`): спот по сокету — `uta.order` / `uta.cancel`
  с `tradeType=SPOT` (как в Java; схема из документации, не сверена).
- **BingX** — hex(HMAC-SHA256) параметров по алфавиту; перпы — positionSide=BOTH.
- **MEXC** — спот `/api/v3`, перпы contract.mexc.com, сторона по ногам хедж-позиции.
- **Hyperliquid** — EIP-712 «фантомного агента» по keccak(msgpack(action) ‖ nonce ‖ 0x00) (сверено с
  hyperliquid-python-sdk); `HYPERLIQUID_API_KEY` — адрес счёта, `_SECRET` — ключ агента; рыночный — IOC ±5 %, свой номер в cloid.
- **Aster** — `ASTER_API_KEY` — адрес основного кошелька, `_SECRET` — ключ агента; nonce в микросекундах.
- **LBank** — параметры по алфавиту → MD5 → HMAC-SHA256. **Upbit** — JWT HS256 с query_hash; рынки USDT-XXX
  (KRW — только для поиска листингов).
- **Uniswap V2** (и совместимые: PancakeSwap V2, SushiSwap) — своп с защитой цены (`uniSlippagePercent`), approve ровно
  на сумму, EIP-155; `restUrl` — RPC-узел, `uniRouter`, `uniTokens`; ключ `UNISWAPV2_API_SECRET` (горячий кошелёк!).

Свои адреса для проверок и прокси: `BINANCE_WS_API_URL`, `BINANCE_WS_FAPI_URL`, `BINANCE_FSTREAM_URL`,
`BYBIT_WS_PRIVATE_URL`, `BYBIT_WS_TRADE_URL`, `OKX_WS_PRIVATE_URL`, `GATE_WS_PRIVATE_URL`, `KUCOIN_WS_PRIVATE_URL`,
`KUCOIN_WS_TRADE_URL`, `HYPERLIQUID_STREAM_URL`, `ASTER_STREAM_URL`, `BINGX_STREAM_URL`.

## Ордера: WebSocket в первую очередь, REST — запасной путь

Параметр биржи `wsTrade` (по умолчанию `true`). Где биржа умеет торговать по сокету, ордера и отмены идут по нему:

| Биржа | Сокет ордеров | Вход |
|---|---|---|
| Binance спот | тот же ws-api, что события счёта (`order.place`, `order.cancel`) | подпись каждого запроса |
| Binance перпы | отдельный ws-fapi | подпись каждого запроса |
| Bybit | отдельный `/v5/trade` (`order.create`, `order.cancel`) | `auth` |
| OKX | тот же приватный сокет (`order`, `cancel-order`) | `login` потока |
| Gate | тот же приватный сокет, WS API (`spot.order_place` / `futures.order_place`, `*.order_cancel`) | `spot.login` / `futures.login` |
| KuCoin | отдельный `wss://wsapi.kucoin.com/v1/private` (`spot.order` / `futures.order`, `*.cancel`) | подпись в адресе + ответ на welcome |
| Hyperliquid | тот же сокет (`method post`, подписанное действие) | подпись действия |

Правила:

1. **Сокет подключён** — ордер и отмена уходят по нему, ответ ждётся по id до 5 с.
2. **Сокета нет** (не подключился, упал, вход не прошёл) — **новые входы запрещены**
   («сокет ордеров не подключён: вход запрещён, выход — по REST»), а **выход идёт по REST**: продажа спота в пределах
   свободной монеты, уменьшение позиции перпа, рыночный выход (стоп-лосс, сторож, выключение стратегии,
   `/positions/close`).
3. **Запрос ушёл, ответа нет** (сокет упал после отправки или тишина 5 с) — повторять вслепую нельзя: ордер ищется по
   REST по своему номеру (`hft<номер>`, OKX `clOrdId`, Bybit `orderLinkId`, Gate `t-hft<номер>`, KuCoin `clientOid`,
   Hyperliquid `cloid`), до трёх попыток; не нашёлся — ордер считается отклонённым.
4. **Отмена по сокету получила отказ** (уже исполнен или снят) или исход неизвестен — итог узнаётся по REST.
5. **Переподключение** — само, с паузой 0.5 → 30 с (сброс после удачного входа). Общие сокеты (Binance спот, OKX, Gate,
   Hyperliquid) переподключаются вместе с приватным потоком; отдельные (Binance перпы, Bybit, KuCoin) — своей задачей.

На биржах без торговли по сокету (BingX, MEXC, Aster, LBank, Upbit, Uniswap) и при `wsTrade=false` всё идёт по REST
и вход не блокируется. Состояние — в `/exchanges/request-stats` → `orders`
(`transport`, `ws.connected`, `calls`, `restFallbacks`, `unknownOutcomes`, `entriesBlocked`; в админке — «Сокет
ордеров»), тексты входов и выходов — в `/control/status` → `стакан`.

## Рыночные данные

- Общий цикл фида (`market/feed/mod.rs`): подписка, пинги, переподключение с паузой `wsReconnectBaseMs` (500 мс) → 30 с;
  устаревшие обновления отбрасываются по номеру.
- Устойчивость (параметры биржи, как в Java): тишина дольше `wsStaleMs` (30 с, Uniswap — 120 с) — переподключение;
  `wsMaxParseErrors` (5) ошибок подряд и `wsMaxCrossedBooks` (200) перекрещённых стаканов подряд — тоже;
  `feedMaxFailures` (15) неудач подряд — источник «сдаётся»: заявки снимаются, стратегии на паузе, попытки продолжаются.
- **Стакан — только по WebSocket**, REST-снимков стакана нет. Пока сокет стакана не подключён (ещё не подключился,
  оборвался, переподключается), **новые входы запрещены** («сокет стакана не подключён: вход запрещён, выход
  разрешён»), а **выход разрешён** — по последнему известному стакану: продажа спота в пределах свободной монеты,
  уменьшение позиции перпа, рыночный выход стопа и сторожа. Сам ордер выхода идёт по сокету ордеров, а если его
  нет — по REST. Если стакан старше 2 с (или его нет), перед рыночным выходом берётся свежая лучшая цена
  по REST — одним запросом, только для этого ордера и только там, где рыночный ордер строится от цены: Gate спот
  (покупка суммой), Hyperliquid (IOC ±5 %), Upbit и LBank (покупка суммой). У остальных бирж рыночному ордеру цена
  не нужна. Запрос не удался — выход идёт по последнему стакану. Это действует и для стратегий, и для ручных ордеров, на бумаге и на бирже. В `/control/status` →
  `стакан`: `feed` (`websocket` / `нет данных`), тексты входов и выходов, `restEntriesBlocked`, `restExits`.
  Сокет вернулся — входы снова разрешены.
- Funding перпов — REST бирж (раз в 30 с; Hyperliquid — `metaAndAssetCtxs`, раз в час). Правила пар Binance
  (`exchangeInfo`) — при подключении, минутные свечи — раз в минуту.
- Монету можно добавить и убрать на ходу. `/market?exchange=`, `/status`, блоки «стакан» и сокет — как у Java.

## Счета: бумага и биржа

- **Бумага** по живому стакану: рыночные ордера проходят по уровням, лимитные ждут и исполняются мейкером. Комиссии из
  каталога бирж, средняя цена, реализованный результат, сделки, тестовый счёт. Ордера округляются по правилам пары.
- **Перпы** (`market=perp`, `leverage`): лонг и шорт, изолированная маржа, funding по ставке биржи, ликвидация при
  марже ниже 0.5 % объёма. На бирже плечо выставляется при старте, позиции сверяются раз в 5 с и по событиям счёта.
- **Живой счёт**: ордер проходит те же проверки, что бумажный, плюс баланс, и уходит в фоновой задаче — движок сеть не
  ждёт. Пока приватный поток работает, статусы приходят событиями (опрос раз в 5 с для подстраховки), без него — опрос
  раз в 0.5 с; балансы — раз в 5 с. Режим (`LIVE` / `TESTNET` / `PAPER`) — в `/exchanges/request-stats`.
- **Переводы между биржами** (спот) — для спот-арбитража: Binance (SAPI), Bybit (v5 asset), OKX (asset), Gate (wallet),
  KuCoin (v3). Сети сопоставляются по общему имени (TRC20 = TRX, ARBEVM = Arbitrum One = ARBITRUM…), выбирается самая
  дешёвая общая. OKX и KuCoin выводят со счёта фондирования: перед выводом монета переводится туда с торгового, а
  пришедший депозит каждые 10 с переносится обратно на торговый. Получатель получает сумму минус комиссия сети
  (у OKX комиссия берётся сверху — сумма вывода уменьшается на неё; у KuCoin — `feeDeductType=EXTERNAL`).

## Риск и защита позиций

- Как в Java: торговля на бирже включена, размер ордера `maxPositionQuote`, проскальзывание `maxSlippagePercent`,
  `maxOrdersPerMinute`, возраст стакана `maxDataAgeMs`, дневной убыток `maxDailyLossQuote` → kill switch (сброс в
  полночь UTC и при включении торговли).
- **Стоп на бирже** (`exchangeStopLossPercent`, перпы): reduceOnly-стоп на весь объём по цене маркировки; новый стоп
  ставится до снятия старого, неудача — повтор через 30 с. Binance, Bybit, OKX, Gate, KuCoin, BingX, Aster,
  Hyperliquid; на остальных — только стоп-лосс бота.
- **Сторож позиций без стратегии** (`guardOrphans`): чужие и оставшиеся позиции перпов — под стоп-лосс
  `orphanStopLossPercent` и таймаут `orphanMaxHoldMinutes`; закрытие рыночным выходом даже при выключенной торговле.

## Стратегии

- **Сбор спреда** (`sc*`): лимитная покупка по биду и продажа по аску не дешевле средней + две комиссии мейкера;
  фильтры стакана, падения цены, частой смены цены, лимита купленного; перебивание на `scJumpTicks`; стоп-лосс и таймаут.
- **Спот – спот между биржами** (`spotArb*`): одновременная продажа на A и покупка на B; без монеты на A —
  покупка на B, вывод по самой дешёвой общей сети, продажа на A (реальный вывод только при `spotArbLiveWithdraw=true`).
  **Выравнивание котируемой валюты** (`spotArbRebalance`, по умолчанию выключено): раз в `spotArbRebalanceMinutes`,
  когда кругов нет, если у самой богатой биржи свободной USDT (или другой котировки) больше, чем у самой бедной, на
  `spotArbRebalanceMinQuote` — половина разницы переводится по самой дешёвой сети (комиссия сети ≤ 1 % суммы).
- **Статистический арбитраж** (`statArb*`): β, z-score спреда ln A − β·ln B, корреляция; вход по |z| ≥ entry,
  выход по exit, стоп, таймаут.
- **Арбитраж цен перпов** (`perpArb*`): шорт на дорогой бирже, лонг на дешёвой; ноги выравниваются.
- **Lead-lag** (`leadLag*`): ведомая биржа отстала от ведущей больше комиссий — вход на ведомой, выход — догнала,
  стоп, таймаут.
- Монеты, занятые одной стратегией, другая не трогает. Состояние — `/strategies?exchange=` и `/arbitrage`.

## Подбор тикеров, перепроверка, листинги

- **Сервер бота** (`serverRegion`: tokyo, singapore, hongkong, europe, us, other; как Java ExchangeRegions): биржа
  рядом — подходят все стратегии, соседний регион Азии — только статарб, далеко — ничего; Uniswap — по RPC-ноде.
  Подбор помечает таких кандидатов «не подходит» с причиной, `/discovery` → `location`, каталог → `recommendation`
  (`yourServer`, `distance`, `forYou`).
- **Скринер** (`/discovery`, пересчёт `POST /discovery/refresh` или раз в `discoveryRefreshSec`): сводки спота
  (Binance, MEXC, Bybit, OKX, Gate, Upbit, LBank, KuCoin, BingX) и перпов со ставками funding. Профили: сбор спреда
  (+ стакан и минутные свечи), спот–спот, lead-lag, перп–перп, статарб (корреляция и бэктест z-score). Пороги —
  `discoveryFilters` {биржа: {стратегия: {параметр: значение}}}.
- **Перепроверка монет** (`POST /trading/recheck`, после каждого пересчёта): монета больше не подходит — входы
  запрещены, при `tradeRecheckRetire` заявки снимаются, позиция закрывается, монета убирается.
- **Листинги и Discord** (`listing*`, `announcePollSec`): новые пары по биржам, анонсы Bybit, OKX, KuCoin, push-фид
  Upbit; алерты в вебхук `listingDiscordUrl`. `GET /signals`, `POST /alerts/test?channel=listing`.

## Админ-API

Токен — заголовок `X-Admin-Token` (или `Authorization: Bearer`), CORS, ошибки — `{"результат": "ошибка", "сообщение": …}`.

| Группа | Адреса |
|---|---|
| Управление | `/control/status`, `/control/select`, `/control/symbols`, `/control/retire`, `/control/deselect`, `/control/start`, `/control/stop`, `/control/exchanges` |
| Параметры | `/settings`, `/exchange/params`, `/exchange/params/schema`, `/exchanges/catalog`, `/env`, `/env/reveal` |
| Торговля | `/order`, `/cancel-all`, `/balances`, `/paper/reset`, `/trading/start\|stop\|panic`, `/trading/recheck`, `/positions`, `/positions/close`, `/funding` |
| Данные | `/status`, `/market`, `/strategies`, `/arbitrage`, `/discovery`, `/signals`, `/events` (SSE), `/health` |
| Наблюдение | `/metrics`, `/exchanges/request-stats`, `/requests/live`, `/requests/stream` (SSE) |

Адреса, которых нет, отвечают 404 «Этого адреса в Rust-бэкенде пока нет».

## Наблюдение: метрики, запросы, журнал

- **`/metrics`** (Prometheus, без токена, пока не задан `METRICS_AUTH=true`): фиды, риск, счёт, сделки, стопы, стаканы,
  запросы к биржам по видам и задержки (в т. ч. `order_ws` / `cancel_ws`), лимиты частоты, рантайм tokio, процесс.
  Дашборды Grafana — в `hft-admin/grafana`.
- **Лимиты частоты** (`infra/ratelimit.rs`, как Java RateLimits): вёдра жетонов на биржу (доля `rateLimitSafety`);
  ответ «слишком часто» — пауза всех запросов к бирже (бан виден в `/control/status` → `баны`). Заголовки остатка
  подтягивают счёт к счёту биржи (как Java syncFromHeaders): Binance/Aster `X-MBX-USED-WEIGHT-1M`,
  `X-MBX-ORDER-COUNT-10S/1D`; Bybit `X-Bapi-Limit-Status`, Gate `X-Gate-RateLimit-Requests-Remain`, KuCoin
  `gw-ratelimit-remaining` — остаток ≤ 2 → пауза до сброса (не дольше 60 с).
- **Журнал** (`debugRequests`): `logs/actions.log` — действия в админке, ордера, ответы биржи, отмены;
  `logs/requests.log` — каждый запрос к бирже. Папка — `LOG_DIR`.

## Хранение состояния и окружение

- **SQLite** (`STATE_DB`, по умолчанию `data/state.db`, WAL): таблица `app_state`, состояние — JSON под ключом `state`.
  Прежний `state.json` переносится при первом запуске; `STATE_FILE` без `STATE_DB` — JSON-файл.
- **`.env`** (путь — `ENV_FILE`) важнее окружения процесса и перечитывается при изменении; `GET/POST /env` — просмотр
  (секреты скрыты) и запись, `GET /env/reveal` — значение целиком (каждый просмотр — в лог).

## Переменные окружения

| Переменная | По умолчанию | Что задаёт |
|---|---|---|
| `ADMIN_PORT` | 8080 | порт админки (если Java-бот на 8080 — укажите другой) |
| `ADMIN_BIND` | 127.0.0.1 | адрес; наружу не открывайте, используйте SSH-туннель |
| `ADMIN_TOKEN` | — | токен админки; пусто — без проверки |
| `ADMIN_CORS_ORIGINS` | — | адреса страниц админки через запятую; пусто — любые |
| `METRICS_AUTH` | false | требовать токен для `/metrics` |
| `STATE_DB` | data/state.db | база SQLite состояния |
| `STATE_FILE` | — | JSON-файл состояния вместо SQLite |
| `ENV_FILE` | .env | файл переменных |
| `LOG_DIR` | logs | журнал отладки |
| `RUST_LOG` | info | уровень логов |
| `<БИРЖА>_API_KEY` / `_SECRET` | — | ключи биржи (`_TESTNET_` — тестовая сеть); OKX и KuCoin — ещё `_PASSPHRASE` |

## Проверки

```bash
cargo test                  # 142 теста: модули и сквозные сценарии против моков бирж
cargo clippy --all-targets
```

Что покрыто сквозными тестами против моков:
- ордера по сокету: Binance спот (ордера и отмена, обрыв → вход отклонён, выход по REST), OKX и Hyperliquid (ордер
  по приватному сокету), Gate (вход `spot.login`, WS API, ack и ответ), Bybit и Binance ws-fapi (вход, подпись, отказ,
  обрыв), KuCoin (подпись в адресе, ответ на welcome, ордер, обрыв и переподключение, UTA);
- исход неизвестен: сокет упал сразу после отправки — ордер не повторяется, а находится по REST по `orderLinkId`;
- стакан по REST: `feed` = `rest`, вход отклонён, счётчик `restEntriesBlocked`; выход проходит (модульный тест счёта);
- переводы OKX (на фондирование → вывод за вычетом комиссии → обратно на торговый) и Gate; разбор сетей всех бирж;
- выравнивание котируемой валюты (бумага), `serverRegion`, заголовки остатка лимитов.

## Чего нет

- Живой проверки на биржах (только моки, см. выше). Особенно стоит проверить на малых суммах переводы OKX, Gate и
  KuCoin (форматы взяты из документации) и KuCoin UTA.
- Торговли по сокету у BingX, MEXC, Aster, LBank, Upbit, Uniswap (у бирж её нет — REST).
- Переводов с BingX, MEXC, Aster, LBank, Upbit, Hyperliquid, Uniswap и с перп-счетов.
- Mean reversion (выключен и в Java), «аптиков/прострелов» (не нужны).
