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

/// Переменная окружения (сначала — переопределения тестов).
pub fn env(key: &str) -> Result<String, std::env::VarError> {
    if let Some(v) = ENV_OVERRIDES
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|m| m.get(key))
    {
        return Ok(v.clone());
    }
    std::env::var(key)
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
