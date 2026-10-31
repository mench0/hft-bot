//! Чтение файлов записи (для выгрузки и бэктестов): распаковка zstd и разбор кадров. Файл, который ещё пишется,
//! читается до последнего целого кадра.

use super::event::{Event, Rec};
use super::format::decode;
use serde_json::{Value, json};
use std::io::Read;
use std::path::Path;

pub fn read_file(path: &Path) -> Result<Vec<Rec>, String> {
    let f = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut d = zstd::Decoder::new(f).map_err(|e| e.to_string())?;
    let mut data = Vec::new();
    // незаконченный поток zstd (файл пишется) — ошибка в конце, прочитанное годится
    let mut chunk = [0u8; 1 << 16];
    loop {
        match d.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => data.extend_from_slice(&chunk[..n]),
        }
    }
    decode(&data)
}

/// Событие строкой JSON (для `hft-bot record-cat`).
pub fn to_json(r: &Rec) -> Value {
    let t = crate::infra::util::iso_ms(r.ts / 1_000_000);
    match &r.ev {
        Event::Bbo {
            ex,
            sym,
            bid,
            bid_qty,
            ask,
            ask_qty,
        } => {
            json!({"t": t, "ns": r.ts, "type": "bbo", "ex": ex.as_str(), "sym": sym.as_str(), "bid": bid, "bidQty": bid_qty, "ask": ask, "askQty": ask_qty})
        }
        Event::Depth {
            ex,
            sym,
            bids,
            asks,
        } => {
            json!({"t": t, "ns": r.ts, "type": "depth", "ex": ex.as_str(), "sym": sym.as_str(), "bids": bids, "asks": asks})
        }
        Event::Order {
            ex,
            sym,
            id,
            side,
            kind,
            qty,
            price,
            status,
            executed,
            avg,
        } => {
            json!({"t": t, "ns": r.ts, "type": "order", "ex": ex.as_str(), "sym": sym.as_str(), "id": id, "side": side,
            "kind": kind, "qty": num(*qty), "price": num(*price), "status": status, "executed": executed, "avg": avg})
        }
        Event::Fill {
            ex,
            sym,
            id,
            side,
            qty,
            price,
            fee,
        } => {
            json!({"t": t, "ns": r.ts, "type": "fill", "ex": ex.as_str(), "sym": sym.as_str(), "id": id, "side": side, "qty": qty, "price": price, "fee": fee})
        }
        Event::Cancel { ex, sym, id, n } => {
            json!({"t": t, "ns": r.ts, "type": "cancel", "ex": ex.as_str(), "sym": sym.as_str(), "id": id, "n": n})
        }
        Event::FeedError { ex } => {
            json!({"t": t, "ns": r.ts, "type": "feedError", "ex": ex.as_str()})
        }
    }
}

fn num(v: f64) -> Value {
    if v.is_finite() { json!(v) } else { Value::Null }
}

/// `hft-bot record-cat <файл>...` — события JSON-строками в stdout.
pub fn cat(paths: &[String]) -> Result<(), String> {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    for p in paths {
        for r in read_file(Path::new(p))? {
            if writeln!(out, "{}", to_json(&r)).is_err() {
                return Ok(()); // закрыли трубу (| head)
            }
        }
    }
    Ok(())
}
