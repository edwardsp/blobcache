mod common;

use blobcache::cache::ChunkKey;
use blobcache::peerindex::PeerIndex;
use common::node;

fn k(offset: u64) -> ChunkKey {
    ChunkKey {
        mount: "m".into(),
        blob: "b".into(),
        offset,
    }
}

#[test]
fn hrw_top_with_no_alive_returns_self() {
    let pi = PeerIndex::new("me".into(), 1 << 12);
    let top = pi.hrw_top_id(&k(0), &[]);
    assert_eq!(top, "me");
}

#[test]
fn hrw_top_is_deterministic() {
    let pi = PeerIndex::new("me".into(), 1 << 12);
    let alive = vec![node("a"), node("b"), node("c"), node("d")];
    let key = k(12345);
    let first = pi.hrw_top_id(&key, &alive);
    for _ in 0..50 {
        assert_eq!(pi.hrw_top_id(&key, &alive), first);
    }
}

#[test]
fn hrw_top_invariant_to_alive_order() {
    let pi = PeerIndex::new("me".into(), 1 << 12);
    let key = k(7777);
    let mut peers = vec![node("p1"), node("p2"), node("p3"), node("p4")];
    let canonical = pi.hrw_top_id(&key, &peers);
    peers.reverse();
    assert_eq!(pi.hrw_top_id(&key, &peers), canonical);
    peers.swap(0, 2);
    assert_eq!(pi.hrw_top_id(&key, &peers), canonical);
}

#[test]
fn hrw_top_distributes_keys_across_peers() {
    let pi = PeerIndex::new("me".into(), 1 << 12);
    let alive = vec![node("a"), node("b"), node("c"), node("d")];
    let mut counts = std::collections::HashMap::<String, usize>::new();
    for off in 0..400u64 {
        let owner = pi.hrw_top_id(&k(off), &alive);
        *counts.entry(owner).or_insert(0) += 1;
    }
    assert_eq!(
        counts.len(),
        5,
        "all 4 peers + self should each own >=1 key"
    );
    for (id, n) in &counts {
        assert!(
            *n > 30,
            "peer {id} owns only {n}/400 keys; expected ~80 per node"
        );
    }
}

#[test]
fn rank_candidates_unknown_remote_goes_to_maybe() {
    let pi = PeerIndex::new("me".into(), 1 << 12);
    let alive = vec![node("a"), node("b"), node("c")];
    let cset = pi.rank_candidates(&k(0), &alive, 4, 4);
    assert_eq!(cset.yes.len(), 0);
    assert_eq!(cset.maybe.len(), 3);
}

#[test]
fn rank_candidates_bloom_positive_goes_to_yes() {
    let pi = PeerIndex::new("me".into(), 1 << 12);
    let alive = vec![node("a"), node("b"), node("c")];
    let key = k(99);

    let mut bloom = blobcache::peerindex::Bloom::new(1 << 12);
    bloom.insert(&blobcache::peerindex::key_digest(&key));
    pi.ingest_remote("a", 1, &bloom.to_bytes());
    pi.ingest_remote(
        "c",
        1,
        &blobcache::peerindex::Bloom::new(1 << 12).to_bytes(),
    );

    let cset = pi.rank_candidates(&key, &alive, 4, 4);
    let yes_ids: Vec<_> = cset.yes.iter().map(|n| n.id.clone()).collect();
    let maybe_ids: Vec<_> = cset.maybe.iter().map(|n| n.id.clone()).collect();
    assert_eq!(yes_ids, vec!["a"]);
    assert_eq!(maybe_ids, vec!["b"]);
    assert!(!yes_ids.contains(&"c".to_string()));
    assert!(!maybe_ids.contains(&"c".to_string()));
}

#[test]
fn rank_candidates_respects_max_yes_budget() {
    let pi = PeerIndex::new("me".into(), 1 << 12);
    let alive = vec![node("a"), node("b"), node("c"), node("d")];
    let key = k(123);
    let mut bloom = blobcache::peerindex::Bloom::new(1 << 12);
    bloom.insert(&blobcache::peerindex::key_digest(&key));
    let bytes = bloom.to_bytes();
    for id in &["a", "b", "c", "d"] {
        pi.ingest_remote(id, 1, &bytes);
    }
    let cset = pi.rank_candidates(&key, &alive, 2, 4);
    assert_eq!(cset.yes.len(), 2, "yes capped at max_yes=2");
    assert_eq!(cset.maybe.len(), 0, "all known, none maybe");
}

#[test]
fn rank_candidates_respects_max_maybe_budget() {
    let pi = PeerIndex::new("me".into(), 1 << 12);
    let alive = vec![node("a"), node("b"), node("c"), node("d")];
    let cset = pi.rank_candidates(&k(0), &alive, 4, 2);
    assert_eq!(cset.yes.len(), 0);
    assert_eq!(cset.maybe.len(), 2);
}

#[test]
fn rank_candidates_yes_and_maybe_are_independent_budgets() {
    let pi = PeerIndex::new("me".into(), 1 << 12);
    let alive = vec![node("a"), node("b"), node("c"), node("d")];
    let key = k(42);
    let mut hit = blobcache::peerindex::Bloom::new(1 << 12);
    hit.insert(&blobcache::peerindex::key_digest(&key));
    pi.ingest_remote("a", 1, &hit.to_bytes());
    pi.ingest_remote("b", 1, &hit.to_bytes());
    let cset = pi.rank_candidates(&key, &alive, 1, 1);
    assert_eq!(cset.yes.len(), 1);
    assert_eq!(cset.maybe.len(), 1);
}

#[test]
fn ingest_remote_rejects_malformed_bytes() {
    let pi = PeerIndex::new("me".into(), 1 << 12);
    assert!(!pi.ingest_remote("p", 1, &[]));
    assert!(!pi.ingest_remote("p", 1, &[0u8; 7]));
    let mut bad_m_bits = (32u64).to_le_bytes().to_vec();
    bad_m_bits.extend_from_slice(&[0u8; 8]);
    assert!(!pi.ingest_remote("p", 1, &bad_m_bits));
    assert!(pi.remote_version("p").is_none());
}

#[test]
fn ingest_remote_records_version() {
    let pi = PeerIndex::new("me".into(), 1 << 12);
    let bloom = blobcache::peerindex::Bloom::new(1 << 12);
    assert!(pi.ingest_remote("p", 7, &bloom.to_bytes()));
    assert_eq!(pi.remote_version("p"), Some(7));
}

#[test]
fn drop_remote_removes_peer() {
    let pi = PeerIndex::new("me".into(), 1 << 12);
    let bloom = blobcache::peerindex::Bloom::new(1 << 12);
    pi.ingest_remote("p", 1, &bloom.to_bytes());
    assert!(pi.remote_version("p").is_some());
    pi.drop_remote("p");
    assert!(pi.remote_version("p").is_none());
}

#[test]
fn note_local_insert_advances_version_and_fires_hook() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    let pi = PeerIndex::new("me".into(), 1 << 12);
    let v0 = pi.local_version();

    let last_seen = Arc::new(AtomicU64::new(0));
    let last_seen_clone = last_seen.clone();
    pi.set_on_version_change(move |v| {
        last_seen_clone.store(v, Ordering::SeqCst);
    });

    let digest = blobcache::peerindex::key_digest(&k(0));
    pi.note_local_insert(&digest);
    let v1 = pi.local_version();
    assert!(v1 > v0);
    assert_eq!(last_seen.load(Ordering::SeqCst), v1);
}

#[test]
fn local_snapshot_round_trips_through_ingest_remote() {
    let pi = PeerIndex::new("me".into(), 1 << 12);
    let key = k(555);
    let digest = blobcache::peerindex::key_digest(&key);
    pi.note_local_insert(&digest);
    let (version, bytes) = pi.local_snapshot();

    let other = PeerIndex::new("other".into(), 1 << 12);
    assert!(other.ingest_remote("me", version, &bytes));
    assert_eq!(other.remote_version("me"), Some(version));

    let cset = other.rank_candidates(&key, &[node("me")], 4, 4);
    assert_eq!(
        cset.yes.iter().map(|n| n.id.as_str()).collect::<Vec<_>>(),
        vec!["me"],
        "remote bloom snapshot must round-trip and answer positive on inserted key"
    );
}
