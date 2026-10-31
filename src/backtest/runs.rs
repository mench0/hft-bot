//! Журнал прогонов бэктеста (SQLite, `BACKTEST_DB`, по умолчанию data/backtests.db): что запускали и что вышло —
//! для сравнения вариантов между собой и с прошлыми днями.

use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::path::PathBuf;

fn path() -> PathBuf {
    crate::infra::util::env("BACKTEST_DB")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "data/backtests.db".into())
        .into()
}

fn open() -> rusqlite::Result<Connection> {
    let p = path();
    if let Some(d) = p.parent() {
        let _ = std::fs::create_dir_all(d);
    }
    let c = Connection::open(p)?;
    c.execute_batch(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL,
           kind TEXT NOT NULL, request TEXT NOT NULL, result TEXT NOT NULL);",
    )?;
    Ok(c)
}

/// Сохранить прогон: вид (backtest, sweep, validate), запрос, итог. Номер прогона или None при ошибке базы.
pub fn save(kind: &str, request: &Value, result: &Value) -> Option<i64> {
    let c = open()
        .map_err(|e| tracing::warn!("журнал бэктестов: {e}"))
        .ok()?;
    c.execute(
        "INSERT INTO runs (at, kind, request, result) VALUES (?1, ?2, ?3, ?4)",
        params![
            crate::infra::util::now_ms() as i64,
            kind,
            request.to_string(),
            result.to_string()
        ],
    )
    .map_err(|e| tracing::warn!("журнал бэктестов: {e}"))
    .ok()?;
    Some(c.last_insert_rowid())
}

/// Последние прогоны (без полного итога): номер, время, вид, запрос, главные цифры.
pub fn list(limit: usize) -> Vec<Value> {
    let Ok(c) = open() else { return Vec::new() };
    let Ok(mut st) =
        c.prepare("SELECT id, at, kind, request, result FROM runs ORDER BY id DESC LIMIT ?1")
    else {
        return Vec::new();
    };
    st.query_map([limit as i64], |r| {
        let (id, at, kind, req, res): (i64, i64, String, String, String) =
            (r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?);
        let res: Value = serde_json::from_str(&res).unwrap_or_default();
        let best = res["rows"].get(0).cloned();
        let main = res.get("sim").unwrap_or(&res);
        Ok(json!({"id": id, "at": at, "kind": kind, "request": serde_json::from_str::<Value>(&req).unwrap_or_default(),
            "pnl": best.as_ref().map_or(main["pnl"].clone(), |b| b["pnl"].clone()),
            "fills": best.as_ref().map_or(main["fills"].clone(), |b| b["fills"].clone()),
            "variants": res["variants"], "best": best.map(|b| b["params"].clone()), "error": res["error"]}))
    })
    .map(|it| it.flatten().collect())
    .unwrap_or_default()
}

/// Полный итог прогона.
pub fn get(id: i64) -> Option<Value> {
    let c = open().ok()?;
    let (at, kind, req, res): (i64, String, String, String) = c
        .query_row(
            "SELECT at, kind, request, result FROM runs WHERE id = ?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .ok()?;
    Some(
        json!({"id": id, "at": at, "kind": kind, "request": serde_json::from_str::<Value>(&req).ok()?,
        "result": serde_json::from_str::<Value>(&res).ok()?}),
    )
}
