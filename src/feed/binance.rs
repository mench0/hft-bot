//! Binance (спот и USDⓈ-M перпы): комбинированные потоки `/stream?streams=` — частичный стакан `depth20@100ms`
//! и лучшие цены `bookTicker`. Номера обновлений общие, поэтому устаревшее не применяется.

use super::Backoff;
use crate::market::Market;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::Message;

/// Адрес по умолчанию.
pub fn default_url(perp: bool, testnet: bool) -> &'static str {
    match (perp, testnet) {
        (false, false) => "wss://stream.binance.com:9443/stream",
        (false, true) => "wss://stream.testnet.binance.vision/stream",
        (true, false) => "wss://fstream.binance.com/stream",
        (true, true) => "wss://fstream.binancefuture.com/stream",
    }
}

/// Адрес с подпиской на потоки символов.
pub fn stream_url(base: &str, symbols: &[String], book_ticker: bool) -> String {
    let mut streams = Vec::new();
    for s in symbols {
        let l = s.to_lowercase();
        streams.push(format!("{l}@depth20@100ms"));
        if book_ticker {
            streams.push(format!("{l}@bookTicker"));
        }
    }
    format!(
        "{}?streams={}",
        base.trim_end_matches('/'),
        streams.join("/")
    )
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    stream: &'a str,
    #[serde(borrow)]
    data: &'a serde_json::value::RawValue,
}

/// Частичный стакан: у спота `lastUpdateId`, у перпов `u`; уровни — строки ["цена", "объём"].
#[derive(Deserialize)]
struct Depth<'a> {
    #[serde(rename = "lastUpdateId", alias = "u", default)]
    id: u64,
    #[serde(borrow, alias = "b")]
    bids: Vec<[&'a str; 2]>,
    #[serde(borrow, alias = "a")]
    asks: Vec<[&'a str; 2]>,
}

#[derive(Deserialize)]
struct Ticker<'a> {
    #[serde(default)]
    u: u64,
    #[serde(borrow)]
    b: &'a str,
    #[serde(borrow, rename = "B")]
    bq: &'a str,
    #[serde(borrow)]
    a: &'a str,
    #[serde(borrow, rename = "A")]
    aq: &'a str,
}

fn levels(raw: &[[&str; 2]]) -> Vec<(f64, f64)> {
    raw.iter()
        .filter_map(|l| Some((l[0].parse().ok()?, l[1].parse().ok()?)))
        .collect()
}

/// Разобрать сообщение и применить к стакану; false — не стакан (подтверждение и т.п.) или ошибка разбора.
pub fn apply(market: &Market, ex: &str, text: &str) -> bool {
    let Ok(env) = serde_json::from_str::<Envelope>(text) else {
        return false;
    };
    let Some((sym, kind)) = env.stream.split_once('@') else {
        return false;
    };
    let sym = sym.to_uppercase();
    if kind.starts_with("depth") {
        let Ok(d) = serde_json::from_str::<Depth>(env.data.get()) else {
            return false;
        };
        let (b, a) = (levels(&d.bids), levels(&d.asks));
        market.with_book(ex, &sym, |book| book.apply_snapshot(b, a, d.id));
        true
    } else if kind == "bookTicker" {
        let Ok(t) = serde_json::from_str::<Ticker>(env.data.get()) else {
            return false;
        };
        let (Ok(b), Ok(bq), Ok(a), Ok(aq)) = (t.b.parse(), t.bq.parse(), t.a.parse(), t.aq.parse())
        else {
            return false;
        };
        let fresh = market.with_book(ex, &sym, |book| book.apply_best(b, bq, a, aq, t.u));
        market.with_feed(ex, |f| {
            if fresh {
                f.bbo_updates += 1
            } else {
                f.stale += 1
            }
        });
        true
    } else {
        false
    }
}

/// Держать соединение: читать сообщения, при обрыве переподключаться. Задача снимается abort() при остановке.
pub async fn run(market: Arc<Market>, ex: String, url: String) {
    let mut backoff = Backoff::default();
    loop {
        match tokio_tungstenite::connect_async(url.as_str()).await {
            Ok((mut ws, _)) => {
                tracing::info!("[{ex}] WebSocket подключён");
                market.with_feed(&ex, |f| {
                    f.connected = true;
                    f.last_error.clear();
                });
                backoff.reset();
                while let Some(msg) = ws.next().await {
                    match msg {
                        Ok(Message::Text(t)) => {
                            market.with_feed(&ex, |f| f.messages += 1);
                            apply(&market, &ex, &t);
                        }
                        Ok(Message::Ping(p)) => {
                            let _ = ws.send(Message::Pong(p)).await;
                        }
                        Ok(Message::Close(c)) => {
                            market.with_feed(&ex, |f| {
                                f.last_error = format!("закрыто биржей: {c:?}")
                            });
                            break;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            market.with_feed(&ex, |f| f.last_error = e.to_string());
                            break;
                        }
                    }
                }
            }
            Err(e) => market.with_feed(&ex, |f| f.last_error = e.to_string()),
        }
        let pause = backoff.next();
        let err = market.with_feed(&ex, |f| {
            f.connected = false;
            f.reconnects += 1;
            f.last_error.clone()
        });
        tracing::warn!(
            "[{ex}] WebSocket: {err} — переподключение через {} мс",
            pause.as_millis()
        );
        tokio::time::sleep(pause).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[test]
    fn url_and_parse() {
        let u = stream_url("wss://x/stream", &["BTCUSDT".into()], true);
        assert_eq!(
            u,
            "wss://x/stream?streams=btcusdt@depth20@100ms/btcusdt@bookTicker"
        );
        let m = Market::default();
        assert!(apply(
            &m,
            "binance",
            r#"{"stream":"btcusdt@depth20@100ms","data":{"lastUpdateId":5,"bids":[["100.0","2"]],"asks":[["101.0","3"]]}}"#
        ));
        assert!(apply(
            &m,
            "binance",
            r#"{"stream":"btcusdt@bookTicker","data":{"u":6,"s":"BTCUSDT","b":"100.5","B":"1","a":"100.9","A":"4"}}"#
        ));
        let b = m.book("binance", "BTCUSDT").unwrap();
        assert_eq!(b.best_bid(), Some((100.5, 1.0)));
        assert_eq!(b.best_ask(), Some((100.9, 4.0)));
        // перпы: depthUpdate с полями b/a и номером u
        assert!(apply(
            &m,
            "binance",
            r#"{"stream":"ethusdt@depth20@100ms","data":{"e":"depthUpdate","u":9,"b":[["10","1"]],"a":[["11","1"]]}}"#
        ));
        assert!(m.book("binance", "ETHUSDT").unwrap().is_ready());
        assert!(!apply(&m, "binance", r#"{"result":null,"id":1}"#));
    }

    /// Живое соединение с локальным WS-сервером: снимок и лучшие цены доходят, после обрыва — переподключение.
    #[tokio::test]
    async fn connects_and_reconnects() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for round in 0..2u64 {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
                let id = 10 + round;
                ws.send(Message::text(format!(r#"{{"stream":"btcusdt@depth20@100ms","data":{{"lastUpdateId":{id},"bids":[["100","1"]],"asks":[["101","1"]]}}}}"#))).await.unwrap();
                if round == 0 {
                    ws.close(None).await.ok();
                } else {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        });
        let m = Arc::new(Market::default());
        m.with_feed("binance", |f| f.symbols = vec!["BTCUSDT".into()]);
        let url = stream_url(&format!("ws://{addr}/stream"), &["BTCUSDT".into()], true);
        let task = tokio::spawn(run(m.clone(), "binance".into(), url));
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let f = m.feed("binance").unwrap();
            if f.reconnects >= 1 && f.connected && f.messages >= 2 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "нет переподключения: {f:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(m.book("binance", "BTCUSDT").unwrap().is_ready());
        assert!(
            m.market_json("binance")["BTCUSDT"]["середина"]
                .as_f64()
                .unwrap()
                == 100.5
        );
        task.abort();
    }
}
