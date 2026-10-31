//! `hft-bot export` — файлы записи за день в таблицы CSV (gzip) для DuckDB, Jupyter и бэктестов:
//! `out/<таблица>/date=ГГГГ-ММ-ДД/part-<источник>.csv.gz`, таблицы bbo, trades, depth, orders, fills, cancels, gaps.
//! Колонки у своих и скачанных (fetch) данных одинаковые. В Parquet — одной командой DuckDB (см. README).

use super::event::{Event, Rec};
use flate2::{Compression, write::GzEncoder};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Таблица и её заголовок.
pub const TABLES: [(&str, &str); 7] = [
    ("bbo", "ns,ex,sym,ts_ex,bid,bid_qty,ask,ask_qty"),
    ("trades", "ns,ex,sym,ts_ex,px,qty,buy"),
    ("depth", "ns,ex,sym,ts_ex,side,level,px,qty"),
    (
        "orders",
        "ns,ex,sym,id,side,kind,qty,price,status,executed,avg",
    ),
    ("fills", "ns,ex,sym,id,side,qty,price,fee"),
    ("cancels", "ns,ex,sym,id,n"),
    ("gaps", "ns,ex"),
];

/// Писатели таблиц одного дня: открываются при первой строке.
pub struct Tables {
    out: PathBuf,
    day: String,
    part: String,
    files: BTreeMap<&'static str, GzEncoder<std::io::BufWriter<std::fs::File>>>,
    pub rows: BTreeMap<&'static str, u64>,
}

fn f(v: f64) -> String {
    if v.is_finite() {
        v.to_string()
    } else {
        String::new()
    }
}

impl Tables {
    pub fn new(out: &Path, day: &str, part: &str) -> Tables {
        Tables {
            out: out.to_owned(),
            day: day.to_owned(),
            part: part.to_owned(),
            files: BTreeMap::new(),
            rows: BTreeMap::new(),
        }
    }

    pub fn row(&mut self, table: &'static str, line: &str) -> std::io::Result<()> {
        if !self.files.contains_key(table) {
            let dir = self.out.join(table).join(format!("date={}", self.day));
            std::fs::create_dir_all(&dir)?;
            let file = std::fs::File::create(dir.join(format!("part-{}.csv.gz", self.part)))?;
            let mut gz = GzEncoder::new(std::io::BufWriter::new(file), Compression::fast());
            let head = TABLES.iter().find(|t| t.0 == table).map_or("", |t| t.1);
            writeln!(gz, "{head}")?;
            self.files.insert(table, gz);
        }
        *self.rows.entry(table).or_default() += 1;
        writeln!(self.files.get_mut(table).unwrap(), "{line}")
    }

    /// Событие записи — в свою таблицу.
    pub fn event(&mut self, r: &Rec) -> std::io::Result<()> {
        let ns = r.ts;
        match &r.ev {
            Event::Bbo {
                ex,
                sym,
                ts_ex,
                bid,
                bid_qty,
                ask,
                ask_qty,
            } => self.row(
                "bbo",
                &format!(
                    "{ns},{},{},{ts_ex},{},{},{},{}",
                    ex.as_str(),
                    sym.as_str(),
                    f(*bid),
                    f(*bid_qty),
                    f(*ask),
                    f(*ask_qty)
                ),
            ),
            Event::Trade {
                ex,
                sym,
                ts_ex,
                px,
                qty,
                buy,
            } => self.row(
                "trades",
                &format!(
                    "{ns},{},{},{ts_ex},{},{},{buy}",
                    ex.as_str(),
                    sym.as_str(),
                    f(*px),
                    f(*qty)
                ),
            ),
            Event::Depth {
                ex,
                sym,
                ts_ex,
                bids,
                asks,
            } => {
                for (side, lv) in [("bid", bids), ("ask", asks)] {
                    for (i, (p, q)) in lv.iter().enumerate() {
                        self.row(
                            "depth",
                            &format!(
                                "{ns},{},{},{ts_ex},{side},{i},{},{}",
                                ex.as_str(),
                                sym.as_str(),
                                f(*p),
                                f(*q)
                            ),
                        )?;
                    }
                }
                Ok(())
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
            } => self.row(
                "orders",
                &format!(
                    "{ns},{},{},{id},{side},{kind},{},{},{status},{},{}",
                    ex.as_str(),
                    sym.as_str(),
                    f(*qty),
                    f(*price),
                    f(*executed),
                    f(*avg)
                ),
            ),
            Event::Fill {
                ex,
                sym,
                id,
                side,
                qty,
                price,
                fee,
            } => self.row(
                "fills",
                &format!(
                    "{ns},{},{},{id},{side},{},{},{}",
                    ex.as_str(),
                    sym.as_str(),
                    f(*qty),
                    f(*price),
                    f(*fee)
                ),
            ),
            Event::Cancel { ex, sym, id, n } => self.row(
                "cancels",
                &format!("{ns},{},{},{id},{n}", ex.as_str(), sym.as_str()),
            ),
            Event::FeedError { ex } => self.row("gaps", &format!("{ns},{}", ex.as_str())),
        }
    }

    pub fn finish(self) -> std::io::Result<BTreeMap<&'static str, u64>> {
        for (_, gz) in self.files {
            gz.finish()?.flush()?;
        }
        Ok(self.rows)
    }
}

/// Выгрузить день: все файлы `dir/день/*.bin.zst` по порядку → таблицы в out (часть «rec»).
pub fn export_day(
    dir: &Path,
    day: &str,
    out: &Path,
) -> Result<BTreeMap<&'static str, u64>, String> {
    let d = dir.join(day);
    let mut files: Vec<PathBuf> = std::fs::read_dir(&d)
        .map_err(|e| format!("{}: {e}", d.display()))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.to_string_lossy().ends_with(".bin.zst"))
        .collect();
    files.sort();
    let mut t = Tables::new(out, day, "rec");
    for p in &files {
        for r in super::reader::read_file(p)? {
            t.event(&r).map_err(|e| e.to_string())?;
        }
    }
    t.finish().map_err(|e| e.to_string())
}

/// `hft-bot export --day ГГГГ-ММ-ДД [--dir data/rec] [--out data/export]`.
pub fn cli(args: &[String]) -> Result<(), String> {
    let get = |k: &str| {
        args.iter()
            .position(|a| a == k)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let day = get("--day").ok_or("укажите --day ГГГГ-ММ-ДД")?;
    let dir = get("--dir").unwrap_or_else(|| "data/rec".into());
    let out = get("--out").unwrap_or_else(|| "data/export".into());
    let rows = export_day(Path::new(&dir), &day, Path::new(&out))?;
    for (t, n) in rows {
        println!("{t}: {n} строк");
    }
    println!("готово: {out}/<таблица>/date={day}/part-rec.csv.gz");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::event::{Event, Name, Rec};
    use std::io::Read;

    #[test]
    fn day_files_to_tables() {
        let dir = tempfile::tempdir().unwrap();
        let day = "2026-01-02";
        let mut out = super::super::format::header();
        let mut e = super::super::format::Encoder::default();
        let n = Name::new;
        for (ts, ev) in [
            (
                1,
                Event::Trade {
                    ex: n("okx"),
                    sym: n("BTCUSDT"),
                    ts_ex: 9,
                    px: 100.5,
                    qty: 0.1,
                    buy: true,
                },
            ),
            (
                2,
                Event::Depth {
                    ex: n("okx"),
                    sym: n("BTCUSDT"),
                    ts_ex: 0,
                    bids: vec![(100.0, 1.0)],
                    asks: vec![(101.0, 2.0), (102.0, 3.0)],
                },
            ),
            (
                3,
                Event::Order {
                    ex: n("okx"),
                    sym: n("BTCUSDT"),
                    id: 5,
                    side: 1,
                    kind: 2,
                    qty: f64::NAN,
                    price: f64::NAN,
                    status: 3,
                    executed: 1.0,
                    avg: 100.6,
                },
            ),
        ] {
            e.encode(&mut out, &Rec { ts, ev });
        }
        let d = dir.path().join("rec").join(day);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("10.bin.zst"), zstd::encode_all(&out[..], 3).unwrap()).unwrap();
        let rows = super::export_day(&dir.path().join("rec"), day, &dir.path().join("x")).unwrap();
        assert_eq!((rows["trades"], rows["depth"], rows["orders"]), (1, 3, 1));
        let read = |t: &str| {
            let mut s = String::new();
            flate2::read::GzDecoder::new(
                std::fs::File::open(dir.path().join(format!("x/{t}/date={day}/part-rec.csv.gz")))
                    .unwrap(),
            )
            .read_to_string(&mut s)
            .unwrap();
            s
        };
        assert_eq!(
            read("trades"),
            "ns,ex,sym,ts_ex,px,qty,buy\n1,okx,BTCUSDT,9,100.5,0.1,true\n"
        );
        assert!(read("depth").ends_with("2,okx,BTCUSDT,0,ask,1,102,3\n"));
        assert!(
            read("orders").contains("3,okx,BTCUSDT,5,1,2,,,3,1,100.6"),
            "{}",
            read("orders")
        );
    }
}
