//! `hft-bot fetch` — бесплатная история бирж в те же таблицы, что и `export` (trades, bbo):
//! Binance — data.binance.vision (zip с CSV), Bybit — public.bybit.com (csv.gz). Это архивы-файлы, а не API бирж:
//! ключи не нужны, вес запросов и лимиты торгового API не расходуются.
//!
//! Бережно, чтобы не получить бан: по одному файлу за раз, пауза между запросами (--pause-ms, по умолчанию 1500),
//! не больше --max-files файлов за запуск (100), уже скачанное и отсутствующее (404) не запрашивается повторно
//! (отметки в out/.fetched). Ответ 429/418/403 — скачивание сразу останавливается до следующего запуска;
//! ошибки сервера и сети — до 3 повторов с паузой 5, 15, 45 с.

use super::export::Tables;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Источник: имя, биржа в таблицах, таблица, адрес файла по (основа, символ, день).
pub struct Source {
    pub name: &'static str,
    ex: &'static str,
    table: &'static str,
    url: fn(&str, &str, &str) -> String,
}

pub const SOURCES: [Source; 5] = [
    Source {
        name: "binance-spot-trades",
        ex: "binance",
        table: "trades",
        url: |b, s, d| format!("{b}/data/spot/daily/aggTrades/{s}/{s}-aggTrades-{d}.zip"),
    },
    Source {
        name: "binance-perp-trades",
        ex: "binance",
        table: "trades",
        url: |b, s, d| format!("{b}/data/futures/um/daily/aggTrades/{s}/{s}-aggTrades-{d}.zip"),
    },
    Source {
        name: "binance-perp-bbo",
        ex: "binance",
        table: "bbo",
        url: |b, s, d| format!("{b}/data/futures/um/daily/bookTicker/{s}/{s}-bookTicker-{d}.zip"),
    },
    Source {
        name: "bybit-perp-trades",
        ex: "bybit",
        table: "trades",
        url: |b, s, d| format!("{b}/trading/{s}/{s}{d}.csv.gz"),
    },
    Source {
        name: "bybit-spot-trades",
        ex: "bybit",
        table: "trades",
        url: |b, s, d| format!("{b}/spot/{s}/{s}_{d}.csv.gz"),
    },
];

fn default_base(source: &str) -> &'static str {
    if source.starts_with("bybit") {
        "https://public.bybit.com"
    } else {
        "https://data.binance.vision"
    }
}

/// Время в мс из секунд, мс, мкс или нс.
fn to_ms(t: f64) -> u64 {
    if t < 1e11 {
        (t * 1000.0).round() as u64
    } else if t < 1e14 {
        t as u64
    } else if t < 1e17 {
        (t / 1e3) as u64
    } else {
        (t / 1e6) as u64
    }
}

/// CSV (с заголовком или без, как aggTrades спота Binance) → строки таблицы источника.
fn convert(src: &Source, sym: &str, text: &str, t: &mut Tables) -> std::io::Result<u64> {
    let mut cols: Vec<String> = Vec::new();
    let mut n = 0;
    for (i, line) in text.lines().enumerate() {
        let v: Vec<&str> = line.trim().split(',').map(str::trim).collect();
        if v.len() < 2 {
            continue;
        }
        if i == 0 && v[0].parse::<f64>().is_err() {
            cols = v.iter().map(|c| c.to_lowercase()).collect();
            continue;
        }
        // колонка по имени; без заголовка — по месту (aggTrades Binance: id, цена, объём, первая, последняя, время,
        // покупатель мейкер; bookTicker: id, бид, объём, аск, объём, время)
        let col = |names: &[&str], pos: usize| -> Option<&str> {
            let i = if cols.is_empty() {
                pos
            } else {
                cols.iter().position(|c| names.contains(&c.as_str()))?
            };
            v.get(i).copied()
        };
        let num = |names: &[&str], pos: usize| col(names, pos)?.parse::<f64>().ok();
        let row = if src.table == "bbo" {
            let (Some(b), Some(bq), Some(a), Some(aq), Some(ts)) = (
                num(&["best_bid_price"], 1),
                num(&["best_bid_qty"], 2),
                num(&["best_ask_price"], 3),
                num(&["best_ask_qty"], 4),
                num(&["transaction_time", "event_time"], 5),
            ) else {
                continue;
            };
            let ms = to_ms(ts);
            format!("{},{},{sym},{ms},{b},{bq},{a},{aq}", ms * 1_000_000, src.ex)
        } else {
            let (Some(px), Some(q), Some(ts)) = (
                num(&["price"], 1),
                num(&["quantity", "qty", "size", "volume"], 2),
                num(&["transact_time", "timestamp", "time"], 5),
            ) else {
                continue;
            };
            // агрессор: is_buyer_maker (Binance; покупатель мейкер — продавал агрессор) или side (Bybit)
            let buy = if cols.is_empty() || cols.iter().any(|c| c == "is_buyer_maker") {
                !col(&["is_buyer_maker"], 6).is_some_and(|m| m.eq_ignore_ascii_case("true"))
            } else {
                col(&["side"], 0).is_some_and(|s| s.eq_ignore_ascii_case("buy"))
            };
            let ms = to_ms(ts);
            format!("{},{},{sym},{ms},{px},{q},{buy}", ms * 1_000_000, src.ex)
        };
        t.row(src.table, &row)?;
        n += 1;
    }
    Ok(n)
}

/// Чем закончилась загрузка файла.
#[derive(Debug, PartialEq)]
pub enum Got {
    Rows(u64),
    /// Файла нет (404): биржа его не выложила.
    Missing,
    /// Уже скачан или отмечен отсутствующим.
    Skipped,
}

/// Ошибка загрузки: сервер ограничил скачивание (429/418/403 — дальше не качаем) или другая.
#[derive(Debug, PartialEq)]
pub enum Fail {
    Limited(String),
    Other(String),
}

pub struct Fetcher {
    http: reqwest::Client,
    pub pause: Duration,
    pub retry_base: Duration,
    out: PathBuf,
    base: Option<String>,
}

impl Fetcher {
    pub fn new(out: &Path, base: Option<String>) -> Fetcher {
        Fetcher {
            http: reqwest::Client::builder()
                .user_agent("hft-bot-fetch/1 (history download)")
                .timeout(Duration::from_secs(300))
                .build()
                .expect("http"),
            pause: Duration::from_millis(1500),
            retry_base: Duration::from_secs(5),
            out: out.to_owned(),
            base,
        }
    }

    fn mark(&self, source: &str, sym: &str, day: &str) -> PathBuf {
        self.out
            .join(".fetched")
            .join(format!("{source}-{sym}-{day}"))
    }

    fn note(&self, path: &Path, text: &str) -> Result<(), Fail> {
        let e = |e: std::io::Error| Fail::Other(e.to_string());
        std::fs::create_dir_all(path.parent().unwrap()).map_err(e)?;
        std::fs::write(path, text).map_err(e)
    }

    /// Скачать с повторами: тело файла, None — 404.
    async fn download(&self, url: &str) -> Result<Option<Vec<u8>>, Fail> {
        let mut wait = self.retry_base;
        let mut last = String::new();
        for attempt in 0..4 {
            if attempt > 0 {
                tokio::time::sleep(wait).await;
                wait *= 3;
            }
            match self.http.get(url).send().await {
                Ok(resp) => {
                    let st = resp.status().as_u16();
                    if st == 404 {
                        return Ok(None);
                    }
                    if matches!(st, 403 | 418 | 429) {
                        return Err(Fail::Limited(format!("{url}: HTTP {st}")));
                    }
                    if !resp.status().is_success() {
                        last = format!("{url}: HTTP {st}");
                        continue;
                    }
                    match resp.bytes().await {
                        Ok(b) => return Ok(Some(b.to_vec())),
                        Err(e) => last = format!("{url}: {e}"),
                    }
                }
                Err(e) => last = format!("{url}: {e}"),
            }
        }
        Err(Fail::Other(last))
    }

    /// Скачать и разобрать один файл (источник, символ, день).
    pub async fn one(&self, source: &str, sym: &str, day: &str) -> Result<Got, Fail> {
        let src = SOURCES
            .iter()
            .find(|s| s.name == source)
            .ok_or_else(|| Fail::Other(format!("неизвестный источник {source}")))?;
        let mark = self.mark(source, sym, day);
        let missing = mark.with_extension("missing");
        if mark.exists() || missing.exists() {
            return Ok(Got::Skipped);
        }
        let base = self.base.as_deref().unwrap_or(default_base(source));
        let url = (src.url)(base.trim_end_matches('/'), sym, day);
        let Some(body) = self.download(&url).await? else {
            self.note(&missing, "")?;
            return Ok(Got::Missing);
        };
        let bad = |what: &str, e: String| Fail::Other(format!("{url}: {what}: {e}"));
        // распаковка: zip (Binance) или gzip (Bybit)
        let mut raw = Vec::new();
        if url.ends_with(".zip") {
            let mut z = zip::ZipArchive::new(std::io::Cursor::new(&body[..]))
                .map_err(|e| bad("zip", e.to_string()))?;
            let mut f = z.by_index(0).map_err(|e| bad("zip", e.to_string()))?;
            f.read_to_end(&mut raw)
                .map_err(|e| bad("zip", e.to_string()))?;
        } else {
            flate2::read::GzDecoder::new(&body[..])
                .read_to_end(&mut raw)
                .map_err(|e| bad("gzip", e.to_string()))?;
        }
        let mut t = Tables::new(&self.out, day, &format!("{source}-{sym}"));
        let rows = convert(src, sym, &String::from_utf8_lossy(&raw), &mut t)
            .map_err(|e| bad("запись", e.to_string()))?;
        t.finish().map_err(|e| bad("запись", e.to_string()))?;
        self.note(&mark, &rows.to_string())?;
        Ok(Got::Rows(rows))
    }
}

/// Дни от from до to включительно (ГГГГ-ММ-ДД).
fn days(from: &str, to: &str) -> Result<Vec<String>, String> {
    let parse = |d: &str| -> Result<u64, String> {
        let v: Vec<i64> = d.split('-').filter_map(|x| x.parse().ok()).collect();
        let [y, m, dd] = v[..] else {
            return Err(format!("дата {d}: нужен ГГГГ-ММ-ДД"));
        };
        // дни от эпохи (алгоритм Хиннанта, обратный iso_ms)
        let y = if m <= 2 { y - 1 } else { y };
        let era = y.div_euclid(400);
        let yoe = y - era * 400;
        let doy = (153 * ((m + 9) % 12) + 2) / 5 + dd - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        Ok((era * 146_097 + doe - 719_468) as u64)
    };
    let (a, b) = (parse(from)?, parse(to)?);
    if b < a || b - a > 366 {
        return Err("--to раньше --from или больше года".into());
    }
    Ok((a..=b)
        .map(|d| crate::infra::util::iso_ms(d * 86_400_000)[..10].to_owned())
        .collect())
}

/// `hft-bot fetch --source <имя> --symbols BTCUSDT,ETHUSDT --from ГГГГ-ММ-ДД [--to ГГГГ-ММ-ДД] [--out data/export]
/// [--pause-ms 1500] [--max-files 100] [--base адрес-зеркала]`.
pub async fn cli(args: &[String]) -> Result<(), String> {
    let get = |k: &str| {
        args.iter()
            .position(|a| a == k)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let names: Vec<&str> = SOURCES.iter().map(|s| s.name).collect();
    let source = get("--source").ok_or(format!("укажите --source: {}", names.join(", ")))?;
    if !names.contains(&source.as_str()) {
        return Err(format!(
            "источник {source} не знаю; есть: {}",
            names.join(", ")
        ));
    }
    let symbols: Vec<String> = get("--symbols")
        .ok_or("укажите --symbols BTCUSDT,ETHUSDT")?
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();
    let from = get("--from").ok_or("укажите --from ГГГГ-ММ-ДД")?;
    let to = get("--to").unwrap_or_else(|| from.clone());
    let out = get("--out").unwrap_or_else(|| "data/export".into());
    let max: usize = get("--max-files")
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let mut f = Fetcher::new(Path::new(&out), get("--base"));
    f.pause = Duration::from_millis(
        get("--pause-ms")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1500)
            .max(200),
    );
    let mut done = 0;
    for day in days(&from, &to)? {
        for sym in &symbols {
            if done >= max {
                println!(
                    "достигнут --max-files {max}: остальное — следующим запуском (скачанное не повторяется)"
                );
                return Ok(());
            }
            match f.one(&source, sym, &day).await {
                Ok(Got::Skipped) => continue,
                Ok(Got::Missing) => println!("{day} {sym}: файла нет у биржи"),
                Ok(Got::Rows(n)) => println!("{day} {sym}: {n} строк"),
                Err(Fail::Limited(e)) => {
                    return Err(format!(
                        "сервер ограничил скачивание ({e}) — остановлено, чтобы не получить бан; повторите позже, \
                         можно с большим --pause-ms"
                    ));
                }
                Err(Fail::Other(e)) => println!("{day} {sym}: ошибка: {e}"),
            }
            done += 1;
            tokio::time::sleep(f.pause).await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, http::StatusCode, routing::get};
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn gz(text: &str) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        e.write_all(text.as_bytes()).unwrap();
        e.finish().unwrap()
    }

    fn zipped(name: &str, text: &str) -> Vec<u8> {
        let mut w = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        w.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        w.write_all(text.as_bytes()).unwrap();
        w.finish().unwrap().into_inner()
    }

    fn read_gz(p: &Path) -> String {
        let mut s = String::new();
        flate2::read::GzDecoder::new(std::fs::File::open(p).unwrap())
            .read_to_string(&mut s)
            .unwrap();
        s
    }

    #[test]
    fn days_and_times() {
        assert_eq!(
            days("2024-02-28", "2024-03-01").unwrap(),
            ["2024-02-28", "2024-02-29", "2024-03-01"]
        );
        assert!(days("2024-03-01", "2024-02-01").is_err());
        assert_eq!(to_ms(1_700_000_000.5), 1_700_000_000_500);
        assert_eq!(
            to_ms(1_700_000_000_123_456.0),
            1_700_000_000_123,
            "мкс спота Binance с 2025 г."
        );
    }

    #[tokio::test]
    async fn downloads_politely_and_stops_on_limit() {
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let spot = zipped(
            "BTCUSDT-aggTrades-2024-01-02.csv",
            "1,42000.5,0.01,1,1,1704153600123456,true,true\n2,42001,0.2,2,2,1704153600200000,false,true\n",
        );
        let perp = zipped(
            "x.csv",
            "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n1,100,3,1,1,1704153600000,false\n",
        );
        let bybit = gz("timestamp,symbol,side,size,price\n1704153600.25,BTCUSDT,Sell,0.5,42000\n");
        let app = Router::new()
            .route(
                "/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01-02.zip",
                get(move || async move { spot }),
            )
            .route(
                "/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01-02.zip",
                get(move || async move { perp }),
            )
            .route(
                "/trading/BTCUSDT/BTCUSDT2024-01-02.csv.gz",
                get(move || async move { bybit }),
            )
            .route(
                "/trading/ETHUSDT/ETHUSDT2024-01-02.csv.gz",
                get(|| async { StatusCode::TOO_MANY_REQUESTS }),
            )
            .layer(axum::middleware::from_fn(
                move |req, next: axum::middleware::Next| {
                    h.fetch_add(1, Ordering::SeqCst);
                    async move { next.run(req).await }
                },
            ));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
        let dir = tempfile::tempdir().unwrap();
        let f = Fetcher::new(dir.path(), Some(base));
        let day = "2024-01-02";
        assert_eq!(
            f.one("binance-spot-trades", "BTCUSDT", day).await,
            Ok(Got::Rows(2))
        );
        let t = read_gz(
            &dir.path()
                .join("trades/date=2024-01-02/part-binance-spot-trades-BTCUSDT.csv.gz"),
        );
        assert_eq!(
            t.lines().collect::<Vec<_>>(),
            [
                "ns,ex,sym,ts_ex,px,qty,buy",
                "1704153600123000000,binance,BTCUSDT,1704153600123,42000.5,0.01,false",
                "1704153600200000000,binance,BTCUSDT,1704153600200,42001,0.2,true"
            ]
        );
        assert_eq!(
            f.one("binance-perp-trades", "BTCUSDT", day).await,
            Ok(Got::Rows(1))
        );
        assert_eq!(
            f.one("bybit-perp-trades", "BTCUSDT", day).await,
            Ok(Got::Rows(1))
        );
        let b = read_gz(
            &dir.path()
                .join("trades/date=2024-01-02/part-bybit-perp-trades-BTCUSDT.csv.gz"),
        );
        assert!(b.contains("1704153600250,42000,0.5,false"), "{b}");
        let n = hits.load(Ordering::SeqCst);
        // скачанное и отсутствующее повторно не запрашивается
        assert_eq!(
            f.one("binance-spot-trades", "BTCUSDT", day).await,
            Ok(Got::Skipped)
        );
        assert_eq!(
            f.one("binance-perp-bbo", "BTCUSDT", day).await,
            Ok(Got::Missing)
        );
        assert_eq!(
            f.one("binance-perp-bbo", "BTCUSDT", day).await,
            Ok(Got::Skipped)
        );
        assert_eq!(hits.load(Ordering::SeqCst), n + 1);
        // 429 — сразу стоп, без повторов
        assert!(matches!(
            f.one("bybit-perp-trades", "ETHUSDT", day).await,
            Err(Fail::Limited(_))
        ));
        assert_eq!(hits.load(Ordering::SeqCst), n + 2);
    }
}
