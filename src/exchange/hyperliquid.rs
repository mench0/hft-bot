//! Реальные ордера Hyperliquid (как в Java HyperliquidRestClient): действия на POST /exchange подписываются
//! кошельком-агентом без HMAC и ключей API. Хеш действия — keccak(msgpack(action) ‖ nonce (8 байт) ‖ 0x00 — без vault)
//! = connectionId; «фантомный агент» Agent{string source, bytes32 connectionId} (source "a" — основная сеть, "b" — тестовая)
//! подписывается EIP-712 с доменом Exchange / "1" / chainId 1337 / нулевой контракт. Порядок ключей в msgpack важен.
//! HYPERLIQUID_API_KEY — адрес основного счёта (0x…), HYPERLIQUID_API_SECRET — приватный ключ агента (или самого счёта).
//! Перпы: актив — индекс монеты в meta, спот — 10000 + индекс пары в spotMeta; рыночный ордер — IOC с ценой ±5 % от
//! опорной; цена — до 5 значащих цифр и не более 6 (спот 8) − szDecimals знаков, объём — szDecimals знаков.
//! Свой номер ордера идёт в cloid (0x + 32 hex), по нему же разбираются события orderUpdates и userFills потока.

use crate::exchange::binance::{Keys, OrderState};
use crate::exchange::evm::{Wallet, domain, keccak, typed_digest};
use crate::infra::util::now_ms;
use crate::trading::live::{Live, StreamMsg};
use crate::trading::paper::{Side, split};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Отступ рыночного IOC-ордера от опорной цены.
const MARKET_BAND: f64 = 0.05;
/// Метка в старших байтах cloid: «hft».
const CLOID_TAG: u128 = 0x6866_7400 << 96;

pub fn rest_url(testnet: bool) -> &'static str {
    if testnet {
        "https://api.hyperliquid-testnet.xyz"
    } else {
        "https://api.hyperliquid.xyz"
    }
}

pub fn stream_url(testnet: bool) -> String {
    crate::infra::util::env("HYPERLIQUID_STREAM_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| crate::market::feed::hyperliquid::default_url(testnet).into())
}

/// MessagePack значения JSON: map в порядке ключей, целые — кратчайшим форматом; дробных чисел в действиях нет.
pub fn msgpack(v: &Value, o: &mut Vec<u8>) {
    fn len(o: &mut Vec<u8>, n: usize, fix: u8, fix_max: usize, t16: u8, t32: u8) {
        if n <= fix_max {
            o.push(fix | n as u8);
        } else if n <= 0xffff {
            o.push(t16);
            o.extend((n as u16).to_be_bytes());
        } else {
            o.push(t32);
            o.extend((n as u32).to_be_bytes());
        }
    }
    match v {
        Value::Null => o.push(0xc0),
        Value::Bool(b) => o.push(if *b { 0xc3 } else { 0xc2 }),
        Value::Number(n) => {
            if let Some(x) = n.as_u64() {
                match x {
                    0..=0x7f => o.push(x as u8),
                    0x80..=0xff => o.extend([0xcc, x as u8]),
                    0x100..=0xffff => {
                        o.push(0xcd);
                        o.extend((x as u16).to_be_bytes())
                    }
                    0x1_0000..=0xffff_ffff => {
                        o.push(0xce);
                        o.extend((x as u32).to_be_bytes())
                    }
                    _ => {
                        o.push(0xcf);
                        o.extend(x.to_be_bytes())
                    }
                }
            } else if let Some(x) = n.as_i64() {
                if x >= -32 {
                    o.push(x as u8);
                } else {
                    o.push(0xd3);
                    o.extend(x.to_be_bytes());
                }
            } else {
                o.push(0xcb);
                o.extend(n.as_f64().unwrap_or(0.0).to_be_bytes());
            }
        }
        Value::String(s) => {
            let b = s.as_bytes();
            if b.len() <= 31 {
                o.push(0xa0 | b.len() as u8);
            } else if b.len() <= 0xff {
                o.extend([0xd9, b.len() as u8]);
            } else {
                len(o, b.len(), 0, 0, 0xda, 0xdb);
            }
            o.extend(b);
        }
        Value::Array(a) => {
            len(o, a.len(), 0x90, 15, 0xdc, 0xdd);
            a.iter().for_each(|x| msgpack(x, o));
        }
        Value::Object(m) => {
            len(o, m.len(), 0x80, 15, 0xde, 0xdf);
            for (k, x) in m {
                msgpack(&Value::String(k.clone()), o);
                msgpack(x, o);
            }
        }
    }
}

/// Подпись действия: {r, s, v}.
pub fn sign_action(w: &Wallet, action: &Value, nonce: u64, mainnet: bool) -> Value {
    let mut b = Vec::new();
    msgpack(action, &mut b);
    b.extend(nonce.to_be_bytes());
    b.push(0);
    let conn = keccak(&b);
    let mut s = Vec::with_capacity(96);
    s.extend(keccak(b"Agent(string source,bytes32 connectionId)"));
    s.extend(keccak(if mainnet { b"a" } else { b"b" }));
    s.extend(conn);
    let d = typed_digest(&domain("Exchange", "1", 1337, [0; 20]), &keccak(&s));
    let (r, s, v) = w.sign(&d);
    json!({"r": format!("0x{}", hex::encode(r)), "s": format!("0x{}", hex::encode(s)), "v": v})
}

/// Цена по правилам биржи: ≤5 значащих цифр, ≤ max_dec − sz_dec знаков; от 100000 — целая.
pub fn format_price(px: f64, sz_dec: u32, max_dec: u32) -> String {
    let scale = max_dec.saturating_sub(sz_dec) as i32;
    let d = if px >= 100_000.0 {
        0
    } else {
        (5 - (px.abs().log10().floor() as i32 + 1)).clamp(0, scale)
    };
    crate::exchange::binance::dec(format!("{px:.*}", d as usize).parse().unwrap_or(px))
}

fn size(qty: f64, sz_dec: u32) -> String {
    let m = 10f64.powi(sz_dec as i32);
    crate::exchange::binance::dec(((qty * m) + 1e-9).floor() / m)
}

pub fn cloid(local: u64) -> String {
    format!("0x{:032x}", CLOID_TAG | local as u128)
}

fn local_of(c: &str) -> Option<u64> {
    let x = u128::from_str_radix(c.strip_prefix("0x")?, 16).ok()?;
    (x >> 96 == CLOID_TAG >> 96).then_some(x as u64)
}

fn num(v: &Value) -> f64 {
    v.as_str()
        .and_then(|s| s.parse().ok())
        .or_else(|| v.as_f64())
        .unwrap_or(0.0)
}

/// Монета на бирже, номер актива, знаков объёма.
#[derive(Clone, Debug, PartialEq)]
pub struct Asset {
    pub coin: String,
    pub index: u64,
    pub sz_dec: u32,
}

pub struct Client {
    base: String,
    pub perp: bool,
    mainnet: bool,
    user: String,
    wallet: Wallet,
    symbols: Vec<String>,
    assets: Mutex<HashMap<String, Asset>>,
    /// Исполнения по номеру ордера биржи (объём, сумма) — из userFills.
    fills: Mutex<HashMap<String, (f64, f64)>>,
    nonce: AtomicU64,
    http: reqwest::Client,
}

impl Client {
    pub fn new(
        base: &str,
        keys: Keys,
        perp: bool,
        testnet: bool,
        symbols: &[String],
    ) -> Result<Client, String> {
        Ok(Client {
            base: base.trim_end_matches('/').to_owned(),
            perp,
            mainnet: !testnet,
            user: keys.key.to_lowercase(),
            wallet: Wallet::from_hex(&keys.secret)?,
            symbols: symbols.iter().map(|s| s.to_uppercase()).collect(),
            assets: Mutex::default(),
            fills: Mutex::default(),
            nonce: AtomicU64::new(0),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP-клиент"),
        })
    }

    /// Лучшие бид и аск по REST (/info l2Book) — свежая цена для выхода, когда сокет стакана лежит.
    pub async fn top(&self, symbol: &str) -> Result<(f64, f64), String> {
        let a = self.asset(symbol)?;
        let v = self.info(json!({"type": "l2Book", "coin": a.coin})).await?;
        let px = |side: usize| {
            v["levels"][side][0]["px"]
                .as_str()
                .and_then(|p| p.parse::<f64>().ok())
        };
        match (px(0), px(1)) {
            (Some(b), Some(a)) => Ok((b, a)),
            _ => Err(format!("Hyperliquid: пустой стакан {symbol}")),
        }
    }

    async fn info(&self, body: Value) -> Result<Value, String> {
        let r = self
            .http
            .post(format!("{}/info", self.base))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let st = r.status();
        let v: Value = r.json().await.map_err(|e| format!("HTTP {st}: {e}"))?;
        if !st.is_success() {
            return Err(format!("Hyperliquid HTTP {st}: {v}"));
        }
        Ok(v)
    }

    /// Подписанное действие (тело /exchange и запроса post по сокету).
    fn signed(&self, action: Value) -> Value {
        // nonce — миллисекунды, строго растёт
        let n = self
            .nonce
            .fetch_max(now_ms(), Ordering::SeqCst)
            .max(now_ms())
            + 1;
        self.nonce.fetch_max(n, Ordering::SeqCst);
        let sig = sign_action(&self.wallet, &action, n, self.mainnet);
        json!({"action": action, "nonce": n, "signature": sig, "vaultAddress": null})
    }

    async fn exchange(&self, action: Value) -> Result<Value, String> {
        let body = self.signed(action);
        let r = self
            .http
            .post(format!("{}/exchange", self.base))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let st = r.status();
        let v: Value = r.json().await.map_err(|e| format!("HTTP {st}: {e}"))?;
        if !st.is_success() || v["status"] == "err" {
            return Err(format!(
                "Hyperliquid: {}",
                v["response"]
                    .as_str()
                    .map_or_else(|| v.to_string(), str::to_owned)
            ));
        }
        if let Some(e) = v["response"]["data"]["statuses"][0]["error"].as_str() {
            return Err(format!("Hyperliquid: {e}"));
        }
        Ok(v)
    }

    /// Активы символов из meta (перпы) или spotMeta (спот).
    pub fn map_assets(&self, meta: &Value) {
        let mut m = self.assets.lock().unwrap();
        if self.perp {
            for (i, u) in meta["universe"]
                .as_array()
                .into_iter()
                .flatten()
                .enumerate()
            {
                let Some(coin) = u["name"].as_str() else {
                    continue;
                };
                for s in self
                    .symbols
                    .iter()
                    .filter(|s| split(s).is_some_and(|(b, _)| b == coin.to_uppercase()))
                {
                    m.insert(
                        s.clone(),
                        Asset {
                            coin: coin.into(),
                            index: i as u64,
                            sz_dec: u["szDecimals"].as_u64().unwrap_or(4) as u32,
                        },
                    );
                }
            }
            return;
        }
        let tokens: HashMap<u64, (String, u32)> = meta["tokens"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|t| {
                Some((
                    t["index"].as_u64()?,
                    (
                        t["name"].as_str()?.to_uppercase(),
                        t["szDecimals"].as_u64().unwrap_or(2) as u32,
                    ),
                ))
            })
            .collect();
        for u in meta["universe"].as_array().into_iter().flatten() {
            let (Some(name), Some(b), Some(q), Some(i)) = (
                u["name"].as_str(),
                u["tokens"][0].as_u64(),
                u["tokens"][1].as_u64(),
                u["index"].as_u64(),
            ) else {
                continue;
            };
            let (Some(b), Some(q)) = (tokens.get(&b), tokens.get(&q)) else {
                continue;
            };
            let sym = format!("{}{}", b.0, q.0);
            if self.symbols.contains(&sym) {
                m.insert(
                    sym,
                    Asset {
                        coin: name.into(),
                        index: 10_000 + i,
                        sz_dec: b.1,
                    },
                );
            }
        }
    }

    fn asset(&self, symbol: &str) -> Result<Asset, String> {
        self.assets
            .lock()
            .unwrap()
            .get(&symbol.to_uppercase())
            .cloned()
            .ok_or_else(|| format!("Hyperliquid: нет актива {symbol} в meta"))
    }

    fn symbol_of(&self, coin: &str) -> Option<String> {
        self.assets
            .lock()
            .unwrap()
            .iter()
            .find(|(_, a)| a.coin == coin)
            .map(|(s, _)| s.clone())
    }

    pub async fn sync_time(&self) -> Result<(), String> {
        let meta = self
            .info(json!({"type": if self.perp { "meta" } else { "spotMeta" }}))
            .await?;
        self.map_assets(&meta);
        Ok(())
    }

    pub fn place_req(
        &self,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
        ref_px: f64,
    ) -> Result<PlaceReq, String> {
        let a = self.asset(symbol)?;
        let buy = side == Side::Buy;
        let (px, tif) = match price {
            Some(p) => (p, "Gtc"),
            None if ref_px > 0.0 => (
                ref_px
                    * if buy {
                        1.0 + MARKET_BAND
                    } else {
                        1.0 - MARKET_BAND
                    },
                "Ioc",
            ),
            None => return Err("Hyperliquid: нет опорной цены для рыночного ордера".into()),
        };
        let sz = size(qty, a.sz_dec);
        if sz.parse::<f64>().unwrap_or(0.0) <= 0.0 {
            return Err(format!("Hyperliquid: объём {qty} округлился до нуля"));
        }
        let order = json!({"a": a.index, "b": buy, "p": format_price(px, a.sz_dec, if self.perp { 6 } else { 8 }), "s": sz,
            "r": false, "t": {"limit": {"tif": tif}}, "c": cloid(local)});
        Ok(PlaceReq {
            action: json!({"type": "order", "orders": [order], "grouping": "na"}),
            sz,
            ioc: tif == "Ioc",
            qty,
        })
    }

    pub async fn place(
        &self,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
        ref_px: f64,
    ) -> Result<OrderState, String> {
        let r = self.place_req(local, symbol, side, qty, price, ref_px)?;
        let v = self.exchange(r.action.clone()).await?;
        r.result(&v)
    }

    /// Ордер по сокету: запрос post с подписанным действием (id — число).
    pub fn ws_place(&self, id: u64, r: &PlaceReq) -> String {
        json!({"method": "post", "id": id, "request": {"type": "action", "payload": self.signed(r.action.clone())}}).to_string()
    }

    pub fn ws_cancel(&self, id: u64, symbol: &str, oid: &str) -> Result<String, String> {
        let a = self.asset(symbol)?;
        let o: u64 = oid.parse().map_err(|_| format!("номер ордера {oid}"))?;
        Ok(json!({"method": "post", "id": id, "request": {"type": "action",
            "payload": self.signed(json!({"type": "cancel", "cancels": [{"a": a.index, "o": o}]}))}}).to_string())
    }

    /// Ордер по своему номеру cloid (исход запроса по сокету неизвестен).
    pub async fn query_client(&self, _symbol: &str, local: u64) -> Result<OrderState, String> {
        let v = self
            .info(json!({"type": "orderStatus", "user": self.user, "oid": cloid(local)}))
            .await?;
        let w = &v["order"];
        if w.is_null() {
            return Err("Hyperliquid: ордер не найден".into());
        }
        Ok(self.state(
            &w["order"]["oid"].to_string(),
            &w["order"],
            w["status"].as_str().unwrap_or(""),
        ))
    }
}

/// Запрос ордера: действие и что нужно, чтобы понять ответ.
pub struct PlaceReq {
    pub action: Value,
    sz: String,
    ioc: bool,
    qty: f64,
}

impl PlaceReq {
    /// Ответ на ордер (тело ответа /exchange или payload ответа сокета).
    pub fn result(&self, v: &Value) -> Result<OrderState, String> {
        if v["status"] == "err" {
            return Err(format!(
                "Hyperliquid: {}",
                v["response"]
                    .as_str()
                    .map_or_else(|| v.to_string(), str::to_owned)
            ));
        }
        let (sz, qty) = (&self.sz, self.qty);
        let tif = if self.ioc { "Ioc" } else { "Gtc" };
        let st = &v["response"]["data"]["statuses"][0];
        if let Some(f) = st.get("filled") {
            let (ex, avg) = (num(&f["totalSz"]), num(&f["avgPx"]));
            let req: f64 = sz.parse().unwrap_or(qty);
            let status = if ex >= req * 0.9999 {
                "FILLED"
            } else if tif == "Ioc" {
                "CANCELED"
            } else {
                "PARTIALLY_FILLED"
            };
            return Ok(OrderState {
                id: f["oid"].to_string(),
                status: status.into(),
                executed: ex,
                quote: ex * avg,
            });
        }
        if let Some(r) = st.get("resting") {
            return Ok(OrderState {
                id: r["oid"].to_string(),
                status: "NEW".into(),
                executed: 0.0,
                quote: 0.0,
            });
        }
        Err(format!("Hyperliquid: неожиданный ответ на ордер: {v}"))
    }
}

impl Client {
    /// Состояние ордера: исполнено = origSz − sz; сумма — по исполнениям потока, иначе по цене ордера.
    fn state(&self, oid: &str, o: &Value, status: &str) -> OrderState {
        let (orig, left) = (num(&o["origSz"]), num(&o["sz"]));
        let mut ex = (orig - left).max(0.0);
        let status = match status {
            "open" if ex > 0.0 => "PARTIALLY_FILLED",
            "open" => "NEW",
            "filled" => {
                ex = orig;
                "FILLED"
            }
            "rejected" => "REJECTED",
            _ => "CANCELED",
        };
        let quote = match self.fills.lock().unwrap().get(oid) {
            Some((q, s)) if (*q - ex).abs() < 1e-9 => *s,
            _ => ex * num(&o["limitPx"]),
        };
        OrderState {
            id: oid.into(),
            status: status.into(),
            executed: ex,
            quote,
        }
    }

    pub async fn query(&self, _symbol: &str, id: &str) -> Result<OrderState, String> {
        let oid: u64 = id.parse().map_err(|_| format!("номер ордера {id}"))?;
        let v = self
            .info(json!({"type": "orderStatus", "user": self.user, "oid": oid}))
            .await?;
        let w = &v["order"];
        if w.is_null() {
            return Err(format!("Hyperliquid: ордер {id} не найден"));
        }
        Ok(self.state(id, &w["order"], w["status"].as_str().unwrap_or("")))
    }

    pub async fn cancel(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        let a = self.asset(symbol)?;
        let oid: u64 = id.parse().map_err(|_| format!("номер ордера {id}"))?;
        let _ = self
            .exchange(json!({"type": "cancel", "cancels": [{"a": a.index, "o": oid}]}))
            .await;
        self.query(symbol, id).await
    }

    pub async fn balances(&self) -> Result<BTreeMap<String, f64>, String> {
        if self.perp {
            let v = self
                .info(json!({"type": "clearinghouseState", "user": self.user}))
                .await?;
            return Ok(BTreeMap::from([(
                "USDC".to_owned(),
                num(&v["withdrawable"]),
            )]));
        }
        let v = self
            .info(json!({"type": "spotClearinghouseState", "user": self.user}))
            .await?;
        Ok(v["balances"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|b| {
                Some((
                    b["coin"].as_str()?.to_uppercase(),
                    (num(&b["total"]) - num(&b["hold"])).max(0.0),
                ))
            })
            .filter(|(_, f)| *f > 0.0)
            .collect())
    }

    pub async fn positions(&self) -> Result<Vec<(String, f64, f64)>, String> {
        let v = self
            .info(json!({"type": "clearinghouseState", "user": self.user}))
            .await?;
        let mut out: Vec<(String, f64, f64)> =
            self.symbols.iter().map(|s| (s.clone(), 0.0, 0.0)).collect();
        for ap in v["assetPositions"].as_array().into_iter().flatten() {
            let p = &ap["position"];
            if let Some(s) = p["coin"].as_str().and_then(|c| self.symbol_of(c))
                && let Some(x) = out.iter_mut().find(|x| x.0 == s)
            {
                (x.1, x.2) = (num(&p["szi"]), num(&p["entryPx"]));
            }
        }
        Ok(out)
    }

    pub async fn set_leverage(&self, symbol: &str, leverage: f64) -> Result<(), String> {
        let a = self.asset(symbol)?;
        self.exchange(json!({"type": "updateLeverage", "asset": a.index, "isCross": true, "leverage": leverage.round().max(1.0) as u64})).await.map(|_| ())
    }

    pub fn hello(&self) -> Vec<String> {
        ["orderUpdates", "userFills"]
            .iter()
            .map(|t| {
                json!({"method": "subscribe", "subscription": {"type": t, "user": self.user}})
                    .to_string()
            })
            .collect()
    }

    pub fn stream_parse(&self, text: &str) -> StreamMsg {
        let Ok(v) = serde_json::from_str::<Value>(text) else {
            return StreamMsg::Ignore;
        };
        let d = &v["data"];
        match v["channel"].as_str().unwrap_or("") {
            "subscriptionResponse" => StreamMsg::Ready,
            "error" => StreamMsg::Fail(format!("Hyperliquid: {d}")),
            "orderUpdates" => {
                let ups: Vec<(u64, OrderState)> = d
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|e| {
                        let o = &e["order"];
                        let local = o["cloid"].as_str().and_then(local_of)?;
                        let oid = o["oid"].to_string();
                        let st = self.state(&oid, o, e["status"].as_str().unwrap_or(""));
                        // исполнение без своих сделок (userFills ещё не пришли): цена ордера — не цена сделки (у IOC
                        // это предел ±5 %), такое событие пропускаем — сумму дадут userFills, итог — ответ или опрос
                        let exact = self
                            .fills
                            .lock()
                            .unwrap()
                            .get(&oid)
                            .is_some_and(|(q, _)| (*q - st.executed).abs() < 1e-9);
                        (st.executed == 0.0 || exact).then_some((local, st))
                    })
                    .collect();
                apply(ups)
            }
            "userFills" if d["isSnapshot"] != true => {
                let mut ups = Vec::new();
                for f in d["fills"].as_array().into_iter().flatten() {
                    let oid = f["oid"].to_string();
                    let (q, s) = {
                        let mut m = self.fills.lock().unwrap();
                        let e = m.entry(oid.clone()).or_default();
                        e.0 += num(&f["sz"]);
                        e.1 += num(&f["sz"]) * num(&f["px"]);
                        *e
                    };
                    if let Some(local) = f["cloid"].as_str().and_then(local_of) {
                        ups.push((
                            local,
                            OrderState {
                                id: oid,
                                status: "PARTIALLY_FILLED".into(),
                                executed: q,
                                quote: s,
                            },
                        ));
                    }
                }
                apply(ups)
            }
            _ => StreamMsg::Ignore,
        }
    }
}

fn apply(ups: Vec<(u64, OrderState)>) -> StreamMsg {
    if ups.is_empty() {
        return StreamMsg::Ignore;
    }
    StreamMsg::Apply(Box::new(move |l: &mut Live| {
        l.stream_events += 1;
        for (local, st) in &ups {
            l.on_state(*local, st);
        }
    }))
}

/// Стоп на бирже (перпы): reduceOnly-стоп по цене маркировки на всю позицию; None — биржа или рынок не поддерживает.
impl Client {
    /// Триггер-ордер tpsl «sl» (рыночный, reduceOnly) на объём позиции; предел проскальзывания — стоп ±5 %.
    pub async fn place_stop(
        &self,
        symbol: &str,
        side: Side,
        qty: f64,
        price: f64,
    ) -> Result<Option<String>, String> {
        if !self.perp {
            return Ok(None);
        }
        let a = self.asset(symbol)?;
        let buy = side == Side::Buy;
        let px = |x: f64| format_price(x, a.sz_dec, 6);
        let order = json!({"a": a.index, "b": buy, "p": px(price * if buy { 1.0 + MARKET_BAND } else { 1.0 - MARKET_BAND }),
            "s": size(qty, a.sz_dec), "r": true, "t": {"trigger": {"isMarket": true, "triggerPx": px(price), "tpsl": "sl"}}});
        let v = self
            .exchange(json!({"type": "order", "orders": [order], "grouping": "na"}))
            .await?;
        let st = &v["response"]["data"]["statuses"][0];
        st["resting"]["oid"]
            .as_u64()
            .map(|o| Some(o.to_string()))
            .ok_or_else(|| format!("Hyperliquid: стоп не принят: {st}"))
    }

    pub async fn cancel_stop(&self, symbol: &str, id: &str) -> Result<(), String> {
        let a = self.asset(symbol)?;
        let oid: u64 = id.parse().map_err(|_| format!("номер стопа {id}"))?;
        self.exchange(json!({"type": "cancel", "cancels": [{"a": a.index, "o": oid}]}))
            .await
            .map(|_| ())
    }
}

/// Ответ на запрос post по сокету: (id, payload действия или ошибка).
pub fn ws_reply(v: &Value) -> Option<(String, Result<Value, String>)> {
    if v["channel"] != "post" {
        return None;
    }
    let d = &v["data"];
    let id = d["id"].as_u64()?.to_string();
    let r = &d["response"];
    Some((
        id,
        if r["type"] == "error" {
            Err(format!(
                "Hyperliquid: {}",
                r["payload"]
                    .as_str()
                    .map_or_else(|| r["payload"].to_string(), str::to_owned)
            ))
        } else {
            Ok(r["payload"].clone())
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msgpack_and_formats() {
        let mut b = Vec::new();
        msgpack(
            &json!({"type": "order", "n": 300, "x": [true, null], "m": -1}),
            &mut b,
        );
        assert_eq!(
            hex::encode(b),
            "84a474797065a56f72646572a16ecd012ca17892c3c0a16dff"
        );
        assert_eq!(format_price(60123.456, 5, 6), "60123");
        assert_eq!(format_price(1.234567, 2, 6), "1.2346");
        assert_eq!(format_price(0.0001234567, 0, 8), "0.00012346");
        assert_eq!(format_price(123456.7, 0, 6), "123457");
        assert_eq!(size(0.123456, 3), "0.123");
        assert_eq!(local_of(&cloid(42)), Some(42));
        assert_eq!(local_of("0x00000000000000000000000000000001"), None);
    }

    #[test]
    fn agent_signature_recovers_to_wallet() {
        let w =
            Wallet::from_hex("0x0123456789012345678901234567890123456789012345678901234567890123")
                .unwrap();
        let a = json!({"type": "dummy", "num": 100000000000u64});
        let sig = sign_action(&w, &a, 0, true);
        // хеш, который проверяет биржа
        let mut b = Vec::new();
        msgpack(&a, &mut b);
        b.extend(0u64.to_be_bytes());
        b.push(0);
        let mut s = keccak(b"Agent(string source,bytes32 connectionId)").to_vec();
        s.extend(keccak(b"a"));
        s.extend(keccak(&b));
        let d = typed_digest(&domain("Exchange", "1", 1337, [0; 20]), &keccak(&s));
        let hex = format!(
            "{}{}{:02x}",
            sig["r"].as_str().unwrap(),
            &sig["s"].as_str().unwrap()[2..],
            sig["v"].as_u64().unwrap()
        );
        assert_eq!(crate::exchange::evm::recover(&d, &hex).unwrap(), w.address);
        // эталон из тестов hyperliquid-python-sdk (там r без ведущего нуля)
        assert_eq!(
            sig["r"],
            "0x053749d5b30552aeb2fca34b530185976545bb22d0b3ce6f62e31be961a59298"
        );
        assert_eq!(
            sig["s"],
            "0x755c40ba9bf05223521753995abb2f73ab3229be8ec921f350cb447e384d8ed8"
        );
        assert_eq!(sig["v"], 27);
    }
}
