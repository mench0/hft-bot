//! Мелочи без внешних зависимостей: base64 и время в ISO 8601.

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Base64 со знаками = в конце.
pub fn base64(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for c in data.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= c.len() {
                out.push(B64[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// HMAC-SHA256 в base64.
pub fn hmac_b64(secret: &str, msg: &str) -> String {
    use hmac::{Hmac, Mac};
    let mut m = Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC принимает ключ любой длины");
    m.update(msg.as_bytes());
    base64(&m.finalize().into_bytes())
}

/// Миллисекунды эпохи в 2020-12-08T09:08:57.715Z.
pub fn iso_ms(ms: u64) -> String {
    let (days, rem) = ((ms / 86_400_000) as i64, ms % 86_400_000);
    // гражданская дата из числа дней (алгоритм Хиннанта)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000
    )
}

/// Переопределения переменных окружения для тестов (тесты идут параллельно, а `set_var` при одновременном чтении
/// окружения из других потоков небезопасен).
static ENV_OVERRIDES: std::sync::Mutex<Option<std::collections::HashMap<String, String>>> =
    std::sync::Mutex::new(None);

fn override_of(key: &str) -> Option<String> {
    ENV_OVERRIDES
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|m| m.get(key))
        .cloned()
}

/// Переменная окружения: переопределения тестов, затем файл .env (он важнее окружения процесса, как в Java:
/// правка .env побеждает старый export), затем окружение процесса.
pub fn env(key: &str) -> Result<String, std::env::VarError> {
    if let Some(v) = override_of(key) {
        return Ok(v);
    }
    if let Some(v) = dotenv::vars().get(key).filter(|v| !v.trim().is_empty()) {
        return Ok(v.trim().to_owned());
    }
    std::env::var(key)
}

/// Файл .env (путь — ENV_FILE, по умолчанию .env в рабочей папке): чтение с перечитыванием по времени изменения и запись.
pub mod dotenv {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::time::SystemTime;

    /// (время изменения при чтении, переменные).
    /// Прочитанный файл: время изменения, путь, переменные.
    type Loaded = (Option<SystemTime>, PathBuf, BTreeMap<String, String>);
    static CACHE: Mutex<Option<Loaded>> = Mutex::new(None);

    pub fn path() -> PathBuf {
        let p = super::override_of("ENV_FILE")
            .or_else(|| std::env::var("ENV_FILE").ok())
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| ".env".into());
        let p = PathBuf::from(p);
        std::path::absolute(&p).unwrap_or(p)
    }

    pub fn exists() -> bool {
        path().is_file()
    }

    /// Переменные из файла (перечитывается, если изменился, появился или исчез).
    pub fn vars() -> BTreeMap<String, String> {
        let p = path();
        let mtime = std::fs::metadata(&p).and_then(|m| m.modified()).ok();
        let mut c = CACHE.lock().unwrap();
        if let Some((t, cp, v)) = c.as_ref()
            && *t == mtime
            && *cp == p
        {
            return v.clone();
        }
        let v = std::fs::read_to_string(&p)
            .map(|t| parse(&t))
            .unwrap_or_default();
        *c = Some((mtime, p, v.clone()));
        v
    }

    /// Разобрать текст .env: «КЛЮЧ=значение», export, комментарии, кавычки вокруг значения.
    pub fn parse(text: &str) -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        for raw in text.lines() {
            let mut line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some(l) = line.strip_prefix("export ") {
                line = l.trim();
            }
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            let (k, mut v) = (k.trim(), v.trim());
            if k.is_empty() {
                continue;
            }
            if v.len() >= 2
                && ((v.starts_with('"') && v.ends_with('"'))
                    || (v.starts_with('\'') && v.ends_with('\'')))
            {
                v = &v[1..v.len() - 1];
            }
            out.insert(k.to_owned(), v.to_owned());
        }
        out
    }

    /// Записать переменные: существующие строки меняются на месте, новые дописываются, пустое значение удаляет строку.
    /// Пишется атомарно (временный файл + переименование), права 600.
    pub fn set(updates: &BTreeMap<String, String>) -> Result<(), String> {
        for (k, v) in updates {
            if k.is_empty()
                || !k.chars().next().is_some_and(|c| c.is_ascii_uppercase())
                || !k
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
            {
                return Err(format!("Недопустимое имя переменной: {k}"));
            }
            if v.contains('\n') || v.contains('\r') {
                return Err("Значение не может содержать перевод строки".into());
            }
        }
        let p = path();
        let mut lines: Vec<String> = std::fs::read_to_string(&p)
            .map(|t| t.lines().map(str::to_owned).collect())
            .unwrap_or_default();
        let mut left = updates.clone();
        lines.retain_mut(|line| {
            let l = line.trim();
            let l = l.strip_prefix("export ").map_or(l, str::trim);
            let Some((k, _)) = l.split_once('=').filter(|_| !l.starts_with('#')) else {
                return true;
            };
            let Some(v) = left.remove(k.trim()) else {
                return true;
            };
            if v.trim().is_empty() {
                return false;
            }
            *line = format!("{}={}", k.trim(), v.trim());
            true
        });
        for (k, v) in left.into_iter().filter(|(_, v)| !v.trim().is_empty()) {
            lines.push(format!("{k}={}", v.trim()));
        }
        if let Some(d) = p.parent() {
            std::fs::create_dir_all(d).map_err(|e| e.to_string())?;
        }
        let tmp = p.with_extension("tmp");
        let mut text = lines.join("\n");
        text.push('\n');
        std::fs::write(&tmp, text).map_err(|e| e.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
        }
        std::fs::rename(&tmp, &p).map_err(|e| e.to_string())?;
        *CACHE.lock().unwrap() = None; // перечитать при следующем чтении, даже если время совпало
        tracing::info!(
            "Обновлены переменные в {}: {:?}",
            p.display(),
            updates.keys().collect::<Vec<_>>()
        );
        Ok(())
    }
}

/// Задать переменную для тестов (без изменения окружения процесса).
#[cfg(test)]
pub fn set_env(key: &str, value: &str) {
    ENV_OVERRIDES
        .lock()
        .unwrap()
        .get_or_insert_with(Default::default)
        .insert(key.to_owned(), value.to_owned());
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b64_iso_hmac() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foobar"), "Zm9vYmFy");
        assert_eq!(iso_ms(1_607_418_537_715), "2020-12-08T09:08:57.715Z");
        assert_eq!(iso_ms(951_782_400_000), "2000-02-29T00:00:00.000Z");
        // RFC 4231, случай 2
        assert_eq!(
            hmac_b64("Jefe", "what do ya want for nothing?"),
            "W9zBRr9gdU5qBCQmCJV1x1oAPwidJzmDnexYuWTsOEM="
        );
    }
}
