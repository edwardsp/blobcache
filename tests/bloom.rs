use blobcache::peerindex::Bloom;

fn d(seed: u8) -> [u8; 32] {
    let mut x = [0u8; 32];
    for (i, slot) in x.iter_mut().enumerate() {
        *slot = seed.wrapping_add(i as u8);
    }
    x
}

#[test]
fn new_rejects_under_64_bits_via_min() {
    let b = Bloom::new(0);
    assert_eq!(b.byte_len(), 8 + 8, "0 -> clamped to 64 bits = 1 word");
    let b = Bloom::new(63);
    assert_eq!(b.byte_len(), 8 + 8);
    let b = Bloom::new(64);
    assert_eq!(b.byte_len(), 8 + 8);
}

#[test]
fn insert_then_contains_is_true() {
    let mut b = Bloom::new(1 << 14);
    let key = d(1);
    assert!(!b.contains(&key));
    b.insert(&key);
    assert!(b.contains(&key));
}

#[test]
fn contains_other_keys_likely_false_under_low_load() {
    let mut b = Bloom::new(1 << 16);
    b.insert(&d(1));
    let mut hits = 0usize;
    for s in 2u8..=200 {
        if b.contains(&d(s)) {
            hits += 1;
        }
    }
    assert!(
        hits < 5,
        "FP count {hits} too high for sparse bloom; suggests bit_pos is broken"
    );
}

#[test]
fn to_bytes_from_bytes_roundtrip_preserves_membership() {
    let mut b = Bloom::new(1 << 12);
    let keys: Vec<[u8; 32]> = (0u8..50).map(d).collect();
    for k in &keys {
        b.insert(k);
    }
    let bytes = b.to_bytes();
    let restored = Bloom::from_bytes(&bytes).expect("roundtrip parse");
    for k in &keys {
        assert!(restored.contains(k), "membership lost after roundtrip");
    }
    assert_eq!(restored.to_bytes(), bytes, "roundtrip must be stable");
}

#[test]
fn from_bytes_rejects_short_header() {
    assert!(Bloom::from_bytes(&[]).is_none());
    assert!(Bloom::from_bytes(&[0u8; 7]).is_none());
}

#[test]
fn from_bytes_rejects_under_64_bits() {
    let mut bad = (32u64).to_le_bytes().to_vec();
    bad.extend_from_slice(&[0u8; 8]);
    assert!(
        Bloom::from_bytes(&bad).is_none(),
        "must reject m_bits < 64 to prevent div_ceil(0) and short payload bypass"
    );
}

#[test]
fn from_bytes_rejects_truncated_payload() {
    let mut b = Bloom::new(1 << 12);
    b.insert(&d(7));
    let bytes = b.to_bytes();
    let truncated = &bytes[..bytes.len() - 1];
    assert!(
        Bloom::from_bytes(truncated).is_none(),
        "must reject payload that doesn't equal words*8"
    );
}

#[test]
fn from_bytes_rejects_oversized_payload() {
    let b = Bloom::new(1 << 12);
    let mut bytes = b.to_bytes();
    bytes.push(0u8);
    assert!(Bloom::from_bytes(&bytes).is_none());
}

#[test]
fn byte_len_matches_to_bytes_len() {
    for m in [64usize, 1 << 8, 1 << 12, 1 << 16, (1 << 16) + 1] {
        let b = Bloom::new(m);
        assert_eq!(b.byte_len(), b.to_bytes().len(), "m_bits={m}");
    }
}

#[test]
fn fp_rate_within_theoretical_envelope() {
    let m_bits: usize = 1 << 16;
    let n: usize = 1000;
    let mut b = Bloom::new(m_bits);
    let inserted: Vec<[u8; 32]> = (0..n)
        .map(|i| {
            let mut x = [0u8; 32];
            x[..8].copy_from_slice(&(i as u64).to_le_bytes());
            x
        })
        .collect();
    for k in &inserted {
        b.insert(k);
    }
    let probes: Vec<[u8; 32]> = (0..10_000usize)
        .map(|i| {
            let mut x = [0u8; 32];
            x[..8].copy_from_slice(&((i + 1_000_000) as u64).to_le_bytes());
            x[8] = 0xAA;
            x
        })
        .collect();
    let fps = probes.iter().filter(|k| b.contains(k)).count();
    let fp_rate = fps as f64 / probes.len() as f64;
    assert!(
        fp_rate < 0.05,
        "FP rate {fp_rate:.4} far above expected ~0.001 for k=4, m={m_bits}, n={n}"
    );
}
