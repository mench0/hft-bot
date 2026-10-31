//! Формат файла записи (до сжатия zstd): заголовок `HFTR` + версия, затем кадры «тег, время, поля», числа — little endian.
//! Имена (биржи, символы) — номерами: первое появление имени в файле предваряется кадром NAME. Каждый файл
//! самодостаточен (своя таблица имён), поэтому читается с начала независимо от других.

use super::event::{Event, Name, Rec};
use std::collections::HashMap;

pub const MAGIC: &[u8; 4] = b"HFTR";
pub const VERSION: u16 = 2;

const NAME: u8 = 0;
const BBO: u8 = 1;
const DEPTH: u8 = 2;
const ORDER: u8 = 3;
const FILL: u8 = 4;
const CANCEL: u8 = 5;
const FEED_ERROR: u8 = 6;
const TRADE: u8 = 7;

pub fn header() -> Vec<u8> {
    let mut b = MAGIC.to_vec();
    b.extend_from_slice(&VERSION.to_le_bytes());
    b
}

/// Кодировщик одного файла: помнит выданные номера имён.
#[derive(Default)]
pub struct Encoder {
    names: HashMap<Name, u16>,
}

impl Encoder {
    fn name(&mut self, out: &mut Vec<u8>, n: &Name) -> u16 {
        if let Some(id) = self.names.get(n) {
            return *id;
        }
        let id = self.names.len() as u16;
        let s = n.as_str().as_bytes();
        out.push(NAME);
        out.extend_from_slice(&id.to_le_bytes());
        out.push(s.len() as u8);
        out.extend_from_slice(s);
        self.names.insert(*n, id);
        id
    }

    /// Дописать событие в out.
    pub fn encode(&mut self, out: &mut Vec<u8>, r: &Rec) {
        fn f(out: &mut Vec<u8>, v: f64) {
            out.extend_from_slice(&v.to_le_bytes());
        }
        let (ex, sym) = match &r.ev {
            Event::Bbo { ex, sym, .. }
            | Event::Depth { ex, sym, .. }
            | Event::Trade { ex, sym, .. }
            | Event::Order { ex, sym, .. }
            | Event::Fill { ex, sym, .. }
            | Event::Cancel { ex, sym, .. } => (self.name(out, ex), Some(self.name(out, sym))),
            Event::FeedError { ex } => (self.name(out, ex), None),
        };
        let tag = match r.ev {
            Event::Bbo { .. } => BBO,
            Event::Depth { .. } => DEPTH,
            Event::Trade { .. } => TRADE,
            Event::Order { .. } => ORDER,
            Event::Fill { .. } => FILL,
            Event::Cancel { .. } => CANCEL,
            Event::FeedError { .. } => FEED_ERROR,
        };
        out.push(tag);
        out.extend_from_slice(&r.ts.to_le_bytes());
        out.extend_from_slice(&ex.to_le_bytes());
        if let Some(s) = sym {
            out.extend_from_slice(&s.to_le_bytes());
        }
        match &r.ev {
            Event::Bbo {
                ts_ex,
                bid,
                bid_qty,
                ask,
                ask_qty,
                ..
            } => {
                out.extend_from_slice(&ts_ex.to_le_bytes());
                for v in [*bid, *bid_qty, *ask, *ask_qty] {
                    f(out, v)
                }
            }
            Event::Depth {
                ts_ex, bids, asks, ..
            } => {
                out.extend_from_slice(&ts_ex.to_le_bytes());
                out.extend_from_slice(&(bids.len() as u16).to_le_bytes());
                out.extend_from_slice(&(asks.len() as u16).to_le_bytes());
                for (p, q) in bids.iter().chain(asks) {
                    f(out, *p);
                    f(out, *q);
                }
            }
            Event::Trade {
                ts_ex,
                px,
                qty,
                buy,
                ..
            } => {
                out.extend_from_slice(&ts_ex.to_le_bytes());
                f(out, *px);
                f(out, *qty);
                out.push(*buy as u8);
            }
            Event::Order {
                id,
                side,
                kind,
                qty,
                price,
                status,
                executed,
                avg,
                ..
            } => {
                out.extend_from_slice(&id.to_le_bytes());
                out.extend_from_slice(&[*side, *kind, *status]);
                for v in [*qty, *price, *executed, *avg] {
                    f(out, v)
                }
            }
            Event::Fill {
                id,
                side,
                qty,
                price,
                fee,
                ..
            } => {
                out.extend_from_slice(&id.to_le_bytes());
                out.push(*side);
                for v in [*qty, *price, *fee] {
                    f(out, v)
                }
            }
            Event::Cancel { id, n, .. } => {
                out.extend_from_slice(&id.to_le_bytes());
                out.extend_from_slice(&n.to_le_bytes());
            }
            Event::FeedError { .. } => {}
        }
    }
}

/// Разбор распакованного файла: события по порядку; обрезанный хвост (файл ещё пишется) — просто конец.
pub fn decode(data: &[u8]) -> Result<Vec<Rec>, String> {
    if data.len() < 6 || &data[..4] != MAGIC {
        return Err("не файл записи HFTR".into());
    }
    let v = u16::from_le_bytes([data[4], data[5]]);
    if v != VERSION {
        return Err(format!("версия формата {v} не поддерживается"));
    }
    let mut c = Cursor { d: data, p: 6 };
    let mut names: Vec<Name> = Vec::new();
    let mut out = Vec::new();
    while c.p < data.len() {
        match c.frame(&mut names) {
            Some(Some(r)) => out.push(r),
            Some(None) => {}
            None => break,
        }
    }
    Ok(out)
}

struct Cursor<'a> {
    d: &'a [u8],
    p: usize,
}

impl Cursor<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        let s = self.d.get(self.p..self.p + n)?;
        self.p += n;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.take(2)?.try_into().ok()?))
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
    fn f64(&mut self) -> Option<f64> {
        Some(f64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    /// Кадр: Some(Some) — событие, Some(None) — служебный (имя), None — данные кончились.
    fn frame(&mut self, names: &mut Vec<Name>) -> Option<Option<Rec>> {
        let tag = self.u8()?;
        if tag == NAME {
            let id = self.u16()? as usize;
            let len = self.u8()? as usize;
            let s = std::str::from_utf8(self.take(len)?).ok()?;
            if names.len() <= id {
                names.resize(id + 1, Name::new(""));
            }
            names[id] = Name::new(s);
            return Some(None);
        }
        let ts = self.u64()?;
        let ex = *names.get(self.u16()? as usize)?;
        let ev = if tag == FEED_ERROR {
            Event::FeedError { ex }
        } else {
            let sym = *names.get(self.u16()? as usize)?;
            match tag {
                BBO => Event::Bbo {
                    ex,
                    sym,
                    ts_ex: self.u64()?,
                    bid: self.f64()?,
                    bid_qty: self.f64()?,
                    ask: self.f64()?,
                    ask_qty: self.f64()?,
                },
                DEPTH => {
                    let ts_ex = self.u64()?;
                    let (nb, na) = (self.u16()? as usize, self.u16()? as usize);
                    let mut lv = Vec::with_capacity(nb + na);
                    for _ in 0..nb + na {
                        lv.push((self.f64()?, self.f64()?));
                    }
                    let asks = lv.split_off(nb);
                    Event::Depth {
                        ex,
                        sym,
                        ts_ex,
                        bids: lv,
                        asks,
                    }
                }
                TRADE => Event::Trade {
                    ex,
                    sym,
                    ts_ex: self.u64()?,
                    px: self.f64()?,
                    qty: self.f64()?,
                    buy: self.u8()? == 1,
                },
                ORDER => {
                    let id = self.u64()?;
                    let (side, kind, status) = (self.u8()?, self.u8()?, self.u8()?);
                    Event::Order {
                        ex,
                        sym,
                        id,
                        side,
                        kind,
                        status,
                        qty: self.f64()?,
                        price: self.f64()?,
                        executed: self.f64()?,
                        avg: self.f64()?,
                    }
                }
                FILL => Event::Fill {
                    ex,
                    sym,
                    id: self.u64()?,
                    side: self.u8()?,
                    qty: self.f64()?,
                    price: self.f64()?,
                    fee: self.f64()?,
                },
                CANCEL => Event::Cancel {
                    ex,
                    sym,
                    id: self.u64()?,
                    n: self.u32()?,
                },
                _ => return None,
            }
        };
        Some(Some(Rec { ts, ev }))
    }
}
