//! Подписи кошельком EVM: keccak256, адрес из приватного ключа, подпись хеша (r‖s‖v, v = 27 + recid) и EIP-712.
//! Нужны Aster (Auth v3) и Hyperliquid.

use k256::ecdsa::{RecoveryId, Signature, SigningKey, VerifyingKey};
use sha3::{Digest, Keccak256};

pub fn keccak(data: &[u8]) -> [u8; 32] {
    Keccak256::digest(data).into()
}

/// Кошелёк из приватного ключа (hex, с 0x или без).
pub struct Wallet {
    key: SigningKey,
    /// Адрес 0x… в нижнем регистре.
    pub address: String,
}

impl Wallet {
    pub fn from_hex(hex_key: &str) -> Result<Wallet, String> {
        let raw = hex::decode(hex_key.trim().trim_start_matches("0x"))
            .map_err(|e| format!("приватный ключ: {e}"))?;
        let key = SigningKey::from_slice(&raw).map_err(|e| format!("приватный ключ: {e}"))?;
        let address = address_of(key.verifying_key());
        Ok(Wallet { key, address })
    }

    /// Подпись 32-байтного хеша: (r, s, v).
    pub fn sign(&self, digest: &[u8; 32]) -> ([u8; 32], [u8; 32], u8) {
        let (sig, rec): (Signature, RecoveryId) = self
            .key
            .sign_prehash_recoverable(digest)
            .expect("подпись хеша");
        let b = sig.to_bytes();
        let (mut r, mut s) = ([0u8; 32], [0u8; 32]);
        r.copy_from_slice(&b[..32]);
        s.copy_from_slice(&b[32..]);
        (r, s, 27 + rec.to_byte())
    }

    /// Подпись 0x r‖s‖v.
    pub fn sign_hex(&self, digest: &[u8; 32]) -> String {
        let (r, s, v) = self.sign(digest);
        format!("0x{}{}{v:02x}", hex::encode(r), hex::encode(s))
    }
}

/// Адрес: последние 20 байт keccak открытого ключа (без префикса 04).
pub fn address_of(k: &VerifyingKey) -> String {
    let p = k.to_encoded_point(false);
    format!("0x{}", hex::encode(&keccak(&p.as_bytes()[1..])[12..]))
}

/// Кто подписал хеш (для проверок).
#[cfg(test)]
pub fn recover(digest: &[u8; 32], sig_hex: &str) -> Result<String, String> {
    let b = hex::decode(sig_hex.trim_start_matches("0x")).map_err(|e| e.to_string())?;
    if b.len() != 65 {
        return Err("подпись не 65 байт".into());
    }
    let sig = Signature::from_slice(&b[..64]).map_err(|e| e.to_string())?;
    let rec = RecoveryId::from_byte(b[64].wrapping_sub(27)).ok_or("неверный v")?;
    let k = VerifyingKey::recover_from_prehash(digest, &sig, rec).map_err(|e| e.to_string())?;
    Ok(address_of(&k))
}

/// Число в 32 байта big-endian.
pub fn uint256(v: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[24..].copy_from_slice(&v.to_be_bytes());
    out
}

/// Хеш домена EIP-712 (name, version, chainId, verifyingContract).
pub fn domain(name: &str, version: &str, chain_id: u64, contract: [u8; 20]) -> [u8; 32] {
    let mut b = Vec::with_capacity(160);
    b.extend(keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    ));
    b.extend(keccak(name.as_bytes()));
    b.extend(keccak(version.as_bytes()));
    b.extend(uint256(chain_id));
    b.extend([0u8; 12]);
    b.extend(contract);
    keccak(&b)
}

/// Итоговый хеш EIP-712: keccak(0x1901 ‖ домен ‖ хеш структуры).
pub fn typed_digest(domain: &[u8; 32], struct_hash: &[u8; 32]) -> [u8; 32] {
    let mut b = vec![0x19, 0x01];
    b.extend(domain);
    b.extend(struct_hash);
    keccak(&b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_address_and_roundtrip() {
        // ключ 1 → известный адрес
        let w =
            Wallet::from_hex("0x0000000000000000000000000000000000000000000000000000000000000001")
                .unwrap();
        assert_eq!(w.address, "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf");
        assert_eq!(
            hex::encode(keccak(b"")),
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        );
        let d = typed_digest(
            &domain("AsterSignTransaction", "1", 1666, [0; 20]),
            &keccak(b"x"),
        );
        let sig = w.sign_hex(&d);
        assert_eq!(sig.len(), 132);
        assert_eq!(recover(&d, &sig).unwrap(), w.address);
    }
}
