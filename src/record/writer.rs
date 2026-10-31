//! Поток записи: пачки из очереди → format::Encoder → zstd → `dir/ГГГГ-ММ-ДД/ЧЧ.bin.zst` (UTC), новый файл каждый
//! час, при смене папки и после выключения. Сжатое сбрасывается на диск раз в секунду — файл читается и пока пишется.

use super::event::Rec;
use super::format::{Encoder, header};
use super::{Config, Shared};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Уровень сжатия zstd: быстрый, отдельное ядро справляется с десятками тысяч событий в секунду.
const LEVEL: i32 = 3;
/// Событий в пачке, не больше.
const BATCH: usize = 4096;

#[derive(Default)]
pub struct Stats {
    pub written: AtomicU64,
    /// Сжатых байт записано на диск.
    pub bytes: Arc<AtomicU64>,
    pub file: Mutex<String>,
    pub error: Mutex<String>,
}

/// Считает сжатые байты.
struct Counting {
    inner: BufWriter<File>,
    n: Arc<AtomicU64>,
}

impl Write for Counting {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        let k = self.inner.write(b)?;
        self.n.fetch_add(k as u64, Relaxed);
        Ok(k)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct Open {
    key: (PathBuf, String),
    enc: zstd::Encoder<'static, Counting>,
    coder: Encoder,
}

pub(super) fn run(rx: Receiver<Rec>, shared: &'static OnceLock<Shared>) {
    let s = loop {
        match shared.get() {
            Some(s) => break s,
            None => std::thread::sleep(Duration::from_millis(5)),
        }
    };
    run_with(rx, &|| s.cfg.lock().unwrap().clone(), &s.stats);
}

/// Час события: ("ГГГГ-ММ-ДД", "ЧЧ").
fn hour(ts_ns: u64) -> (String, String) {
    let iso = crate::infra::util::iso_ms(ts_ns / 1_000_000);
    (iso[..10].to_owned(), iso[11..13].to_owned())
}

fn open(dir: &Path, day: &str, hh: &str, stats: &Stats) -> std::io::Result<Open> {
    let d = dir.join(day);
    std::fs::create_dir_all(&d)?;
    // после перезапуска в тот же час — новый файл рядом, а не дописывание в чужой поток zstd
    let mut path = d.join(format!("{hh}.bin.zst"));
    let mut k = 1;
    while path.exists() {
        path = d.join(format!("{hh}-{k}.bin.zst"));
        k += 1;
    }
    let file = File::create(&path)?;
    let mut enc = zstd::Encoder::new(
        Counting {
            inner: BufWriter::with_capacity(1 << 16, file),
            n: stats.bytes.clone(),
        },
        LEVEL,
    )?;
    enc.write_all(&header())?;
    *stats.file.lock().unwrap() = path.display().to_string();
    Ok(Open {
        key: (dir.to_owned(), format!("{day}/{hh}")),
        enc,
        coder: Encoder::default(),
    })
}

fn close(cur: &mut Option<Open>, stats: &Stats) {
    if let Some(o) = cur.take()
        && let Err(e) = o.enc.finish().and_then(|mut w| w.flush())
    {
        *stats.error.lock().unwrap() = format!("закрытие файла: {e}");
    }
}

/// Удалить папки дней (ГГГГ-ММ-ДД) старше keep_days; чужие папки не трогаются.
pub(super) fn cleanup(dir: &Path, keep_days: u64) {
    let cutoff = crate::infra::util::iso_ms(
        crate::infra::util::now_ms().saturating_sub(keep_days * 86_400_000),
    )[..10]
        .to_owned();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let b = name.as_bytes();
        let is_day = b.len() == 10
            && b[4] == b'-'
            && b[7] == b'-'
            && b.iter()
                .enumerate()
                .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit());
        if is_day && name < cutoff && e.path().is_dir() {
            match std::fs::remove_dir_all(e.path()) {
                Ok(()) => tracing::info!("Запись: удалён старый день {name}"),
                Err(err) => tracing::warn!("Запись: не удалось удалить {name}: {err}"),
            }
        }
    }
}

pub(super) fn run_with(rx: Receiver<Rec>, cfg: &dyn Fn() -> Config, stats: &Stats) {
    let mut cur: Option<Open> = None;
    let mut batch: Vec<Rec> = Vec::with_capacity(BATCH);
    let mut buf: Vec<u8> = Vec::with_capacity(1 << 20);
    let mut flushed = Instant::now();
    loop {
        let done = match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(r) => {
                batch.push(r);
                while batch.len() < BATCH {
                    match rx.try_recv() {
                        Ok(r) => batch.push(r),
                        Err(_) => break,
                    }
                }
                false
            }
            Err(RecvTimeoutError::Timeout) => false,
            Err(RecvTimeoutError::Disconnected) => true,
        };
        let c = cfg();
        if !c.enabled {
            close(&mut cur, stats);
            batch.clear();
        }
        for r in batch.drain(..) {
            let (day, hh) = hour(r.ts);
            let key = (c.dir.clone(), format!("{day}/{hh}"));
            if cur.as_ref().is_none_or(|o| o.key != key) {
                close(&mut cur, stats);
                cleanup(&c.dir, c.keep_days);
                match open(&c.dir, &day, &hh, stats) {
                    Ok(o) => {
                        stats.error.lock().unwrap().clear();
                        cur = Some(o)
                    }
                    Err(e) => {
                        *stats.error.lock().unwrap() = format!("{}: {e}", c.dir.display());
                        continue;
                    }
                }
            }
            let Some(o) = cur.as_mut() else { continue };
            buf.clear();
            o.coder.encode(&mut buf, &r);
            match o.enc.write_all(&buf) {
                Ok(()) => {
                    stats.written.fetch_add(1, Relaxed);
                }
                Err(e) => *stats.error.lock().unwrap() = format!("запись: {e}"),
            }
        }
        if flushed.elapsed() >= Duration::from_secs(1) {
            flushed = Instant::now();
            if let Some(o) = cur.as_mut() {
                let _ = o.enc.flush();
            }
        }
        if done {
            close(&mut cur, stats);
            return;
        }
    }
}
