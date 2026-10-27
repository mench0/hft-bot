//! Хранилище настроек на SQLite (как Java SqliteStateStore): одна таблица key-value
//! `app_state(key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER NOT NULL)`, всё состояние бота (выбор бирж и
//! монет, параметры, настройки) — JSON одной строкой под ключом `state`. WAL: базу можно читать `sqlite3` параллельно с ботом.
//! Запись — только по действиям в админке, не на каждом тике. Путь — STATE_DB (по умолчанию data/state.db); при первом
//! запуске на SQLite прежний state.json (STATE_FILE) переносится в базу.

use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

const KEY: &str = "state";

/// Состояние хранится в SQLite (файл .db, .sqlite или .sqlite3).
pub fn is_sqlite(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("db" | "sqlite" | "sqlite3")
    )
}

fn open(path: &Path) -> rusqlite::Result<Connection> {
    if let Some(d) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        let _ = std::fs::create_dir_all(d);
    }
    let c = Connection::open(path)?;
    c.busy_timeout(std::time::Duration::from_secs(5))?;
    // WAL — читатели не блокируют писателя
    let _: String = c.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
    c.execute(
        "CREATE TABLE IF NOT EXISTS app_state (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER NOT NULL)",
        [],
    )?;
    Ok(c)
}

/// Сохранённое состояние (JSON); None — записи нет или база не открылась.
pub fn load(path: &Path) -> Option<String> {
    let r = open(path).and_then(|c| {
        c.query_row("SELECT value FROM app_state WHERE key = ?1", [KEY], |r| {
            r.get(0)
        })
        .optional()
    });
    match r {
        Ok(Some(v)) => {
            tracing::info!("Состояние загружено из {}", path.display());
            Some(v)
        }
        Ok(None) => {
            tracing::info!(
                "В {} ещё нет сохранённого состояния — стартуем с настроек по умолчанию",
                path.display()
            );
            None
        }
        Err(e) => {
            tracing::error!("Не удалось открыть {}: {e}", path.display());
            None
        }
    }
}

/// UPSERT одной строкой (атомарность — у SQLite).
pub fn save(path: &Path, json: &str) {
    let r = open(path).and_then(|c| {
        c.execute(
            "INSERT INTO app_state (key, value, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            params![KEY, json, crate::infra::util::now_ms() as i64],
        )
    });
    if let Err(e) = r {
        tracing::error!("Не удалось сохранить состояние в {}: {e}", path.display());
    }
}

/// Копия нечитаемой записи под ключом state_backup_<время>, чтобы настройки по умолчанию её не затёрли.
pub fn backup(path: &Path) {
    let key = format!("{KEY}_backup_{}", crate::infra::util::now_ms());
    let r = open(path).and_then(|c| {
        c.execute("INSERT INTO app_state (key, value, updated_at) SELECT ?1, value, updated_at FROM app_state WHERE key = ?2", params![key, KEY])
    });
    match r {
        Ok(_) => tracing::warn!("Прежнее состояние сохранено под ключом {key}"),
        Err(e) => tracing::error!("Резервная копия состояния не сделана: {e}"),
    }
}

/// Перенести state.json в пустую базу (первый запуск на SQLite); файл переименовывается в *.migrated.
pub fn migrate_json(db: &Path, json_file: &Path) {
    if !json_file.is_file() || load(db).is_some() {
        return;
    }
    match std::fs::read_to_string(json_file) {
        Ok(j) if serde_json::from_str::<serde_json::Value>(&j).is_ok() => {
            save(db, &j);
            let _ = std::fs::rename(json_file, json_file.with_extension("json.migrated"));
            tracing::warn!(
                "Состояние перенесено из {} в {}",
                json_file.display(),
                db.display()
            );
        }
        _ => tracing::warn!("{} не перенесён: не читается как JSON", json_file.display()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_backup_and_migration() {
        let d = tempfile::tempdir().unwrap();
        let db = d.path().join("data/state.db");
        assert!(is_sqlite(&db) && !is_sqlite(&d.path().join("state.json")));
        assert_eq!(load(&db), None);
        let j = d.path().join("state.json");
        std::fs::write(&j, r#"{"a":1}"#).unwrap();
        migrate_json(&db, &j);
        assert_eq!(load(&db).as_deref(), Some(r#"{"a":1}"#));
        assert!(!j.exists() && d.path().join("state.json.migrated").exists());
        save(&db, r#"{"a":2}"#);
        assert_eq!(load(&db).as_deref(), Some(r#"{"a":2}"#));
        backup(&db);
        let c = Connection::open(&db).unwrap();
        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM app_state WHERE key LIKE 'state_backup_%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }
}
