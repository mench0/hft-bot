//! Сокет ордеров (как WsRpcChannel в Java): запрос по WebSocket с id и ответ по нему же. Ордера, отмены и плечо идут
//! по сокету, пока он подключён; нет сокета — REST. Ответа не дождались (сокет упал после отправки, тишина 5 с) —
//! исход неизвестен: повторять вслепую нельзя, вызывающий узнаёт итог по REST по своему номеру ордера.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Итог запроса по сокету.
#[derive(Debug)]
pub enum Call {
    /// Сокет не подключён — запрос не ушёл (можно по REST).
    NotReady,
    /// Запрос ушёл, ответа нет — исход неизвестен.
    Unknown(String),
    /// Ответ биржи: данные или ошибка.
    Reply(Result<Value, String>),
}

#[derive(Default, Debug)]
pub struct TradeLink {
    out: Mutex<Option<mpsc::UnboundedSender<String>>>,
    pending: Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>,
    seq: AtomicU64,
    /// Запросов по сокету; ушли в REST, потому что сокета не было; исход неизвестен.
    pub calls: AtomicU64,
    pub fallbacks: AtomicU64,
    pub unknown: AtomicU64,
    /// Когда сокет подключился (мс); 0 — не подключён.
    pub since: AtomicU64,
}

impl TradeLink {
    /// Сокет подключён и вошёл (можно слать запросы).
    pub fn ready(&self) -> bool {
        self.out.lock().unwrap().is_some()
    }

    /// Сокет вошёл — запросы пойдут в него.
    pub fn attach(&self, tx: mpsc::UnboundedSender<String>) {
        *self.out.lock().unwrap() = Some(tx);
        self.since
            .store(crate::infra::util::now_ms(), Ordering::Relaxed);
    }

    /// Сокет упал: новые запросы — в REST, ждущие ответа — исход неизвестен.
    pub fn detach(&self) {
        *self.out.lock().unwrap() = None;
        self.since.store(0, Ordering::Relaxed);
        self.pending.lock().unwrap().clear();
    }

    pub fn next_id(&self, prefix: &str) -> String {
        format!("{prefix}{}", self.seq.fetch_add(1, Ordering::Relaxed) + 1)
    }

    /// Отправить запрос с id и ждать ответ (до timeout).
    pub async fn call(&self, id: &str, msg: String, timeout: Duration) -> Call {
        let (s, r) = oneshot::channel();
        self.pending.lock().unwrap().insert(id.to_owned(), s);
        let sent = self
            .out
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|t| t.send(msg).is_ok());
        if !sent {
            self.pending.lock().unwrap().remove(id);
            self.fallbacks.fetch_add(1, Ordering::Relaxed);
            return Call::NotReady;
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
        match tokio::time::timeout(timeout, r).await {
            Ok(Ok(v)) => Call::Reply(v),
            Ok(Err(_)) => {
                self.unknown.fetch_add(1, Ordering::Relaxed);
                Call::Unknown("сокет ордеров закрылся до ответа".into())
            }
            Err(_) => {
                self.pending.lock().unwrap().remove(id);
                self.unknown.fetch_add(1, Ordering::Relaxed);
                Call::Unknown(format!(
                    "нет ответа по сокету ордеров за {} с",
                    timeout.as_secs()
                ))
            }
        }
    }

    /// Ответ по id пришёл; false — его никто не ждёт.
    pub fn resolve(&self, id: &str, r: Result<Value, String>) -> bool {
        match self.pending.lock().unwrap().remove(id) {
            Some(s) => s.send(r).is_ok(),
            None => false,
        }
    }

    pub fn stats(&self) -> Value {
        serde_json::json!({"connected": self.ready(), "since": self.since.load(Ordering::Relaxed),
            "calls": self.calls.load(Ordering::Relaxed), "restFallbacks": self.fallbacks.load(Ordering::Relaxed),
            "unknownOutcomes": self.unknown.load(Ordering::Relaxed)})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn call_reply_unknown_and_not_ready() {
        let l = std::sync::Arc::new(TradeLink::default());
        assert!(matches!(
            l.call("a", "x".into(), Duration::from_millis(50)).await,
            Call::NotReady
        ));
        let (tx, mut rx) = mpsc::unbounded_channel();
        l.attach(tx);
        let l2 = l.clone();
        tokio::spawn(async move {
            let m = rx.recv().await.unwrap();
            assert_eq!(m, "msg");
            l2.resolve("t1", Ok(json!({"ok": 1})));
            let _ = rx.recv().await; // второй — без ответа
        });
        assert!(
            matches!(l.call("t1", "msg".into(), Duration::from_secs(1)).await, Call::Reply(Ok(v)) if v["ok"] == 1)
        );
        assert!(matches!(
            l.call("t2", "m2".into(), Duration::from_millis(100)).await,
            Call::Unknown(_)
        ));
        l.detach();
        assert!(!l.ready());
        assert_eq!(
            (
                l.calls.load(Ordering::Relaxed),
                l.unknown.load(Ordering::Relaxed),
                l.fallbacks.load(Ordering::Relaxed)
            ),
            (2, 1, 1)
        );
    }
}
