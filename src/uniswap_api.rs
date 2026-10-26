//! Реальные свопы Uniswap V2 и совместимых (PancakeSwap V2, SushiSwap) через Router02, как в Java UniswapV2Client.
//! Книги ордеров нет: рыночный ордер и лимитный (как IOC) — своп с защитой по цене: продажа —
//! swapExactTokensForTokens(объём, minOut = getAmountsOut × (1 − проскальзывание)), покупка —
//! swapTokensForExactTokens(объём, maxIn = getAmountsIn × (1 + проскальзывание)); у лимитного — не хуже цены ордера,
//! иначе ордер истекает без транзакции. Перед свопом — approve ровно на нужную сумму, если разрешения не хватает.
//! Транзакции legacy (EIP-155): RLP [nonce, gasPrice ×1.1, gas = оценка ×1.3, to, 0, data, chainId, 0, 0] → keccak →
//! подпись, v = recid + 35 + 2·chainId; итог — по квитанции (status 0x1) и разнице балансов. Отменять нечего.
//! UNISWAPV2_API_KEY — адрес кошелька (метка), UNISWAPV2_API_SECRET — его приватный ключ (отдельный горячий кошелёк!);
//! параметры биржи: restUrl — RPC-узел, uniRouter — адрес Router02, uniTokens — "WETH=0x…:18;USDC=0x…:6",
//! uniSlippagePercent — по умолчанию 0.5. Газ платится в ETH.

use crate::evm::{Wallet, keccak};
use crate::paper::{Side, split};
use crate::signed::OrderState;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub const DEFAULT_RPC: &str = "https://ethereum-rpc.publicnode.com";

// ── RLP и ABI

fn be(x: u128) -> Vec<u8> {
    let b = x.to_be_bytes();
    b[b.iter().position(|&v| v != 0).unwrap_or(16)..].to_vec()
}

fn rlp_bytes(b: &[u8], o: &mut Vec<u8>) {
    if b.len() == 1 && b[0] < 0x80 {
        o.push(b[0]);
    } else {
        rlp_len(b.len(), 0x80, o);
        o.extend(b);
    }
}

fn rlp_len(n: usize, off: u8, o: &mut Vec<u8>) {
    if n < 56 {
        o.push(off + n as u8);
    } else {
        let l = be(n as u128);
        o.push(off + 55 + l.len() as u8);
        o.extend(l);
    }
}

fn rlp_list(items: &[Vec<u8>]) -> Vec<u8> {
    let mut body = Vec::new();
    for i in items {
        rlp_bytes(i, &mut body);
    }
    let mut o = Vec::new();
    rlp_len(body.len(), 0xc0, &mut o);
    o.extend(body);
    o
}

fn addr(a: &str) -> Result<[u8; 20], String> {
    let b = hex::decode(a.trim_start_matches("0x")).map_err(|e| format!("адрес {a}: {e}"))?;
    b.try_into().map_err(|_| format!("адрес {a}: не 20 байт"))
}

/// Подписанная legacy-транзакция EIP-155 (0x…).
#[allow(clippy::too_many_arguments)]
pub fn sign_tx(
    w: &Wallet,
    chain: u64,
    nonce: u128,
    gas_price: u128,
    gas: u128,
    to: &str,
    value: u128,
    data: &[u8],
) -> Result<String, String> {
    let to = addr(to)?.to_vec();
    let mut f = vec![
        be(nonce),
        be(gas_price),
        be(gas),
        to,
        be(value),
        data.to_vec(),
    ];
    let mut pre = f.clone();
    pre.extend([be(chain as u128), vec![], vec![]]);
    let (r, s, v) = w.sign(&keccak(&rlp_list(&pre)));
    let strip = |x: [u8; 32]| x[x.iter().position(|&b| b != 0).unwrap_or(32)..].to_vec();
    f.extend([
        be((v - 27) as u128 + 35 + 2 * chain as u128),
        strip(r),
        strip(s),
    ]);
    Ok(format!("0x{}", hex::encode(rlp_list(&f))))
}

pub enum Arg<'a> {
    U(u128),
    A(&'a str),
    Path(Vec<&'a str>),
}

/// Данные вызова: селектор + аргументы ABI (uint256, address, address[]).
pub fn abi(selector: &str, args: &[Arg]) -> Result<Vec<u8>, String> {
    let word = |x: u128| {
        let mut w = [0u8; 32];
        w[16..].copy_from_slice(&x.to_be_bytes());
        w
    };
    let aw = |a: &str| -> Result<[u8; 32], String> {
        let mut w = [0u8; 32];
        w[12..].copy_from_slice(&addr(a)?);
        Ok(w)
    };
    let mut head = hex::decode(selector).map_err(|e| e.to_string())?;
    let mut tail = Vec::new();
    for a in args {
        match a {
            Arg::U(x) => head.extend(word(*x)),
            Arg::A(s) => head.extend(aw(s)?),
            Arg::Path(p) => {
                head.extend(word((32 * args.len() + tail.len()) as u128));
                tail.extend(word(p.len() as u128));
                for x in p {
                    tail.extend(aw(x)?);
                }
            }
        }
    }
    head.extend(tail);
    Ok(head)
}

fn big(h: &str) -> u128 {
    let h = h.trim_start_matches("0x").trim_start_matches('0');
    if h.len() > 32 {
        u128::MAX
    } else {
        u128::from_str_radix(if h.is_empty() { "0" } else { h }, 16).unwrap_or(0)
    }
}

/// i-е 32-байтное слово ответа.
fn word_at(h: &str, i: usize) -> u128 {
    let h = h.trim_start_matches("0x");
    h.get(64 * i..64 * (i + 1)).map(big).unwrap_or(0)
}

fn units(v: f64, dec: u32) -> u128 {
    (v * 10f64.powi(dec as i32) + 1e-6).floor().max(0.0) as u128
}

fn human(v: u128, dec: u32) -> f64 {
    v as f64 / 10f64.powi(dec as i32)
}

#[derive(Clone)]
struct Token {
    address: String,
    dec: u32,
}

pub struct Client {
    rpc: String,
    wallet: Wallet,
    router: String,
    tokens: HashMap<String, Token>,
    slippage: f64,
    chain: AtomicU64,
    /// Итоги свопов по хешу транзакции (и истёкших ордеров по своему номеру).
    results: Mutex<HashMap<String, OrderState>>,
    seq: AtomicU64,
    http: reqwest::Client,
}

impl Client {
    pub fn new(
        rpc: &str,
        secret: &str,
        router: &str,
        tokens: &str,
        slippage_pct: f64,
    ) -> Result<Client, String> {
        if !router.starts_with("0x") || router.len() != 42 {
            return Err("uniswapv2: задайте параметр uniRouter (адрес Router02)".into());
        }
        let mut t = HashMap::new();
        for item in tokens.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let (k, v) = item
                .split_once('=')
                .ok_or_else(|| format!("uniTokens: нет «=» в {item}"))?;
            let (a, d) = v
                .split_once(':')
                .ok_or_else(|| format!("uniTokens: ожидается 0xадрес:знаков в {item}"))?;
            addr(a)?;
            t.insert(
                k.trim().to_uppercase(),
                Token {
                    address: a.to_lowercase(),
                    dec: d
                        .trim()
                        .parse()
                        .map_err(|_| format!("uniTokens: знаков {d}"))?,
                },
            );
        }
        if t.is_empty() {
            return Err("uniswapv2: задайте параметр uniTokens=\"WETH=0x…:18;USDC=0x…:6\"".into());
        }
        Ok(Client {
            rpc: rpc.to_owned(),
            wallet: Wallet::from_hex(secret)?,
            router: router.to_lowercase(),
            tokens: t,
            slippage: slippage_pct / 100.0,
            chain: AtomicU64::new(0),
            results: Mutex::default(),
            seq: AtomicU64::new(1),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .expect("HTTP-клиент"),
        })
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.seq.fetch_add(1, Ordering::Relaxed);
        let r = self
            .http
            .post(&self.rpc)
            .json(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let st = r.status();
        let v: Value = r.json().await.map_err(|e| format!("HTTP {st}: {e}"))?;
        if let Some(e) = v.get("error").filter(|e| !e.is_null()) {
            return Err(format!(
                "RPC {method}: {}",
                e["message"].as_str().unwrap_or(&e.to_string())
            ));
        }
        Ok(v["result"].clone())
    }

    async fn eth_call(&self, to: &str, data: &[u8]) -> Result<String, String> {
        let r = self
            .call(
                "eth_call",
                json!([{"to": to, "data": format!("0x{}", hex::encode(data))}, "latest"]),
            )
            .await?;
        Ok(r.as_str().unwrap_or("0x").to_owned())
    }

    fn token(&self, a: &str) -> Result<Token, String> {
        self.tokens
            .get(a)
            .cloned()
            .ok_or_else(|| format!("uniswapv2: нет токена {a} в uniTokens"))
    }

    async fn balance_of(&self, t: &Token) -> Result<u128, String> {
        Ok(big(&self
            .eth_call(
                &t.address,
                &abi("70a08231", &[Arg::A(&self.wallet.address)])?,
            )
            .await?))
    }

    pub async fn sync_time(&self) -> Result<(), String> {
        let c = self.call("eth_chainId", json!([])).await?;
        self.chain
            .store(big(c.as_str().unwrap_or("0x1")) as u64, Ordering::Relaxed);
        Ok(())
    }

    async fn send_tx(&self, to: &str, data: &[u8]) -> Result<String, String> {
        let from = self.wallet.address.clone();
        let nonce = big(self
            .call("eth_getTransactionCount", json!([from, "pending"]))
            .await?
            .as_str()
            .unwrap_or("0x0"));
        let gp = big(self
            .call("eth_gasPrice", json!([]))
            .await?
            .as_str()
            .unwrap_or("0x0"))
            * 11
            / 10;
        let hexdata = format!("0x{}", hex::encode(data));
        let gas = self
            .call(
                "eth_estimateGas",
                json!([{"from": from, "to": to, "data": hexdata}]),
            )
            .await
            .map_err(|e| format!("транзакция не пройдёт (оценка газа не удалась): {e}"))?;
        let gas = big(gas.as_str().unwrap_or("0x0")) * 13 / 10;
        if self.chain.load(Ordering::Relaxed) == 0 {
            self.sync_time().await?;
        }
        let raw = sign_tx(
            &self.wallet,
            self.chain.load(Ordering::Relaxed),
            nonce,
            gp,
            gas,
            to,
            0,
            data,
        )?;
        let h = self.call("eth_sendRawTransaction", json!([raw])).await?;
        h.as_str()
            .map(str::to_owned)
            .ok_or_else(|| format!("нет хеша транзакции: {h}"))
    }

    async fn wait_receipt(&self, tx: &str) -> Result<(), String> {
        for _ in 0..180 {
            let r = self.call("eth_getTransactionReceipt", json!([tx])).await?;
            if !r.is_null() {
                return if r["status"] == "0x1" {
                    Ok(())
                } else {
                    Err(format!("транзакция откатилась: {tx}"))
                };
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(format!("транзакция не подтверждена за 90 с: {tx}"))
    }

    pub async fn place(
        &self,
        local: u64,
        symbol: &str,
        side: Side,
        qty: f64,
        price: Option<f64>,
    ) -> Result<OrderState, String> {
        let (b, q) = split(symbol).ok_or_else(|| format!("uniswapv2: символ {symbol}"))?;
        let (base, quote) = (self.token(&b)?, self.token(&q)?);
        let buy = side == Side::Buy;
        let (from, to) = if buy {
            (&quote, &base)
        } else {
            (&base, &quote)
        };
        let path = || Arg::Path(vec![from.address.as_str(), to.address.as_str()]);
        let amount = units(qty, base.dec);
        let expired = || {
            let st = OrderState {
                id: format!("x{local}"),
                status: "EXPIRED".into(),
                executed: 0.0,
                quote: 0.0,
            };
            self.results
                .lock()
                .unwrap()
                .insert(st.id.clone(), st.clone());
            Ok(st)
        };
        // (селектор, первый аргумент, второй, сколько максимум уйдёт)
        let (sel, a1, a2, spend) = if buy {
            let r = self
                .eth_call(&self.router, &abi("1f00ca74", &[Arg::U(amount), path()])?)
                .await?;
            let need = word_at(&r, 2);
            let mut max_in = (need as f64 * (1.0 + self.slippage)).ceil() as u128;
            if let Some(px) = price {
                let cap = units(qty * px, quote.dec);
                if need > cap {
                    return expired();
                }
                max_in = max_in.min(cap);
            }
            ("8803dbee", amount, max_in, max_in)
        } else {
            let r = self
                .eth_call(&self.router, &abi("d06ca61f", &[Arg::U(amount), path()])?)
                .await?;
            let n = word_at(&r, 1) as usize;
            let out = word_at(&r, 1 + n);
            let mut min_out = (out as f64 * (1.0 - self.slippage)).floor() as u128;
            if let Some(px) = price {
                let floor = units(qty * px, quote.dec);
                if out < floor {
                    return expired();
                }
                min_out = min_out.max(floor);
            }
            ("38ed1739", amount, min_out, amount)
        };
        let (b0, q0) = (
            self.balance_of(&base).await?,
            self.balance_of(&quote).await?,
        );
        let allowed = big(&self
            .eth_call(
                &from.address,
                &abi(
                    "dd62ed3e",
                    &[Arg::A(&self.wallet.address), Arg::A(&self.router)],
                )?,
            )
            .await?);
        if allowed < spend {
            tracing::info!("[uniswapv2] approve {} на {spend}", from.address);
            let tx = self
                .send_tx(
                    &from.address,
                    &abi("095ea7b3", &[Arg::A(&self.router), Arg::U(spend)])?,
                )
                .await?;
            self.wait_receipt(&tx).await?;
        }
        let deadline = crate::util::now_ms() / 1000 + 120;
        let data = abi(
            sel,
            &[
                Arg::U(a1),
                Arg::U(a2),
                path(),
                Arg::A(&self.wallet.address),
                Arg::U(deadline as u128),
            ],
        )?;
        let tx = self.send_tx(&self.router, &data).await?;
        self.wait_receipt(&tx).await?;
        let (b1, q1) = (
            self.balance_of(&base).await?,
            self.balance_of(&quote).await?,
        );
        let st = OrderState {
            id: tx.clone(),
            status: "FILLED".into(),
            executed: human(b1.abs_diff(b0), base.dec),
            quote: human(q1.abs_diff(q0), quote.dec),
        };
        tracing::info!(
            "[uniswapv2] своп {side:?} {} {symbol} на {} (tx {tx})",
            st.executed,
            st.quote
        );
        self.results.lock().unwrap().insert(tx, st.clone());
        Ok(st)
    }

    pub async fn query(&self, _symbol: &str, id: &str) -> Result<OrderState, String> {
        self.results
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| format!("uniswapv2: своп {id} не найден"))
    }

    /// Отменять нечего: своп уже подтверждён или отклонён.
    pub async fn cancel(&self, symbol: &str, id: &str) -> Result<OrderState, String> {
        self.query(symbol, id).await
    }

    pub async fn balances(&self) -> Result<BTreeMap<String, f64>, String> {
        let eth = big(self
            .call("eth_getBalance", json!([self.wallet.address, "latest"]))
            .await?
            .as_str()
            .unwrap_or("0x0"));
        let mut out = BTreeMap::from([("ETH".to_owned(), human(eth, 18))]);
        for (k, t) in &self.tokens {
            out.insert(k.clone(), human(self.balance_of(t).await?, t.dec));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eip155_reference_transaction() {
        // пример из EIP-155
        let w =
            Wallet::from_hex("0x4646464646464646464646464646464646464646464646464646464646464646")
                .unwrap();
        let raw = sign_tx(
            &w,
            1,
            9,
            20_000_000_000,
            21000,
            "0x3535353535353535353535353535353535353535",
            1_000_000_000_000_000_000,
            &[],
        )
        .unwrap();
        assert_eq!(
            raw,
            "0xf86c098504a817c800825208943535353535353535353535353535353535353535880de0b6b3a76400008025a028ef61340bd939bc2195fe537567866003e1a15d3c71ff63e1590620aa636276a067cbe9d8997f761aecb703304b3800ccf555c9f3dc64214b297fb1966a3b6d83"
        );
    }

    #[test]
    fn abi_with_path() {
        let a = "0x00000000000000000000000000000000000000aa";
        let d = abi("d06ca61f", &[Arg::U(5), Arg::Path(vec![a, a])]).unwrap();
        let h = hex::encode(&d);
        assert_eq!(&h[..8], "d06ca61f");
        assert_eq!(d.len(), 4 + 32 * 5);
        assert_eq!(word_at(&h[8..], 1), 64); // смещение массива — после двух слов головы
        assert_eq!(word_at(&h[8..], 2), 2);
        assert_eq!(
            hex::encode(keccak(b"getAmountsOut(uint256,address[])"))[..8],
            *"d06ca61f"
        );
        assert_eq!(
            hex::encode(keccak(
                b"swapExactTokensForTokens(uint256,uint256,address[],address,uint256)"
            ))[..8],
            *"38ed1739"
        );
        assert_eq!(
            hex::encode(keccak(
                b"swapTokensForExactTokens(uint256,uint256,address[],address,uint256)"
            ))[..8],
            *"8803dbee"
        );
        assert_eq!(
            hex::encode(keccak(b"getAmountsIn(uint256,address[])"))[..8],
            *"1f00ca74"
        );
    }
}
