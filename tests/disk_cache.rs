use blobcache::cache::{ChunkKey, DiskCache};
use std::sync::atomic::Ordering;
use tempfile::TempDir;

fn k(offset: u64) -> ChunkKey {
    ChunkKey {
        mount: "m".into(),
        blob: "b".into(),
        offset,
    }
}

fn open_cache(max_bytes: u64) -> (TempDir, std::sync::Arc<DiskCache>) {
    let td = TempDir::new().unwrap();
    let c = DiskCache::open(td.path().to_path_buf(), max_bytes).unwrap();
    (td, c)
}

#[test]
fn open_creates_root_and_starts_empty() {
    let td = TempDir::new().unwrap();
    let inner = td.path().join("does/not/exist/yet");
    let c = DiskCache::open(inner.clone(), 1024).unwrap();
    assert!(inner.is_dir());
    assert_eq!(c.stats.bytes_in_use.load(Ordering::Relaxed), 0);
    assert_eq!(c.live_keys().len(), 0);
}

#[test]
fn open_purges_pre_existing_files() {
    let td = TempDir::new().unwrap();
    std::fs::write(td.path().join("orphan-1"), b"junk").unwrap();
    std::fs::write(td.path().join(".tmp.123.abc"), b"tmp").unwrap();
    let c = DiskCache::open(td.path().to_path_buf(), 1024).unwrap();
    let remaining: Vec<_> = std::fs::read_dir(td.path())
        .unwrap()
        .flatten()
        .filter(|e| e.metadata().map(|m| m.is_file()).unwrap_or(false))
        .collect();
    assert_eq!(remaining.len(), 0, "all stray files purged at startup");
    assert_eq!(c.stats.bytes_in_use.load(Ordering::Relaxed), 0);
}

#[test]
fn insert_then_get_roundtrip() {
    let (_td, c) = open_cache(1024 * 1024);
    let key = k(0);
    let payload = vec![0xAB; 4096];
    c.insert(key.clone(), &payload).unwrap();
    let got = c.try_get(&key).expect("hit");
    assert_eq!(&got[..], &payload[..]);
    assert_eq!(c.stats.hits.load(Ordering::Relaxed), 1);
    assert_eq!(c.stats.inserts.load(Ordering::Relaxed), 1);
    assert_eq!(c.stats.bytes_in_use.load(Ordering::Relaxed), 4096);
}

#[test]
fn try_get_miss_increments_misses() {
    let (_td, c) = open_cache(1024);
    assert!(c.try_get(&k(0)).is_none());
    assert_eq!(c.stats.misses.load(Ordering::Relaxed), 1);
    assert_eq!(c.stats.hits.load(Ordering::Relaxed), 0);
}

#[test]
fn try_get_range_returns_subslice() {
    let (_td, c) = open_cache(1024 * 1024);
    let key = k(0);
    let payload: Vec<u8> = (0..4096u32).map(|i| (i & 0xff) as u8).collect();
    c.insert(key.clone(), &payload).unwrap();
    let slice = c.try_get_range(&key, 100, 200).expect("hit");
    assert_eq!(slice.len(), 200);
    assert_eq!(&slice[..], &payload[100..300]);
}

#[test]
fn try_get_range_zero_len_returns_empty_without_io() {
    let (_td, c) = open_cache(1024);
    let out = c.try_get_range(&k(99), 0, 0).expect("zero-len always Some");
    assert_eq!(out.len(), 0);
}

#[test]
fn try_get_range_overrun_is_miss() {
    let (_td, c) = open_cache(1024 * 1024);
    let key = k(0);
    c.insert(key.clone(), &vec![1u8; 4096]).unwrap();
    assert!(c.try_get_range(&key, 0, 5000).is_none());
    assert!(c.try_get_range(&key, 4000, 200).is_none());
    c.try_get_range(&key, 4000, 96).expect("exact-fit ok");
}

#[test]
fn try_get_into_slice_zero_extra_copy() {
    let (_td, c) = open_cache(1024 * 1024);
    let key = k(0);
    let payload = vec![0x5A; 8192];
    c.insert(key.clone(), &payload).unwrap();
    let mut buf = vec![0u8; 8192];
    let n = c.try_get_into_slice(&key, &mut buf).expect("hit");
    assert_eq!(n, 8192);
    assert_eq!(buf, payload);
}

#[test]
fn try_get_into_slice_rejects_size_mismatch() {
    let (_td, c) = open_cache(1024 * 1024);
    let key = k(0);
    c.insert(key.clone(), &vec![1u8; 4096]).unwrap();
    let mut too_small = vec![0u8; 1024];
    assert!(c.try_get_into_slice(&key, &mut too_small).is_none());
    let mut too_big = vec![0u8; 8192];
    assert!(c.try_get_into_slice(&key, &mut too_big).is_none());
}

#[test]
fn entry_size_returns_payload_len() {
    let (_td, c) = open_cache(1024 * 1024);
    let key = k(0);
    c.insert(key.clone(), &vec![1u8; 4321]).unwrap();
    assert_eq!(c.entry_size(&key), Some(4321));
    assert_eq!(c.entry_size(&k(1)), None);
}

#[test]
fn insert_overwriting_same_key_does_not_double_count() {
    let (_td, c) = open_cache(1024 * 1024);
    let key = k(0);
    c.insert(key.clone(), &vec![1u8; 1000]).unwrap();
    c.insert(key.clone(), &vec![2u8; 2000]).unwrap();
    assert_eq!(c.stats.bytes_in_use.load(Ordering::Relaxed), 2000);
    assert_eq!(c.live_keys().len(), 1);
    let got = c.try_get(&key).unwrap();
    assert_eq!(got.len(), 2000);
    assert!(got.iter().all(|b| *b == 2));
}

#[test]
fn evicts_lru_when_over_max_bytes() {
    let (_td, c) = open_cache(3000);
    c.insert(k(0), &vec![0; 1000]).unwrap();
    c.insert(k(1), &vec![1; 1000]).unwrap();
    c.insert(k(2), &vec![2; 1000]).unwrap();
    assert_eq!(c.live_keys().len(), 3);
    assert_eq!(c.stats.evictions.load(Ordering::Relaxed), 0);
    c.insert(k(3), &vec![3; 1000]).unwrap();
    assert_eq!(
        c.stats.evictions.load(Ordering::Relaxed),
        1,
        "one entry evicted to fit new insert"
    );
    assert!(c.try_get(&k(0)).is_none(), "oldest evicted");
    assert_eq!(c.stats.bytes_in_use.load(Ordering::Relaxed), 3000);
}

#[test]
fn try_get_promotes_lru_position() {
    let (_td, c) = open_cache(3000);
    c.insert(k(0), &vec![0; 1000]).unwrap();
    c.insert(k(1), &vec![1; 1000]).unwrap();
    c.insert(k(2), &vec![2; 1000]).unwrap();
    let _ = c.try_get(&k(0));
    c.insert(k(3), &vec![3; 1000]).unwrap();
    assert!(c.try_get(&k(0)).is_some(), "k(0) was promoted, survives");
    assert!(c.try_get(&k(1)).is_none(), "k(1) was now-oldest, evicted");
}

#[test]
fn try_get_range_promotes_lru() {
    let (_td, c) = open_cache(3000);
    c.insert(k(0), &vec![0; 1000]).unwrap();
    c.insert(k(1), &vec![1; 1000]).unwrap();
    c.insert(k(2), &vec![2; 1000]).unwrap();
    let _ = c.try_get_range(&k(0), 0, 100);
    c.insert(k(3), &vec![3; 1000]).unwrap();
    assert!(c.try_get(&k(0)).is_some());
    assert!(c.try_get(&k(1)).is_none());
}

#[test]
fn live_keys_reflects_inserts_and_evictions() {
    let (_td, c) = open_cache(2000);
    c.insert(k(0), &vec![0; 1000]).unwrap();
    c.insert(k(1), &vec![1; 1000]).unwrap();
    assert_eq!(c.live_keys().len(), 2);
    c.insert(k(2), &vec![2; 1000]).unwrap();
    assert_eq!(c.live_keys().len(), 2, "k(0) evicted");
    let names: std::collections::HashSet<u64> =
        c.live_keys().into_iter().map(|x| x.offset).collect();
    assert_eq!(names, std::collections::HashSet::from([1, 2]));
}

#[test]
fn remove_clears_entry_and_byte_count() {
    let (_td, c) = open_cache(1024 * 1024);
    c.insert(k(0), &vec![0; 1000]).unwrap();
    assert_eq!(c.stats.bytes_in_use.load(Ordering::Relaxed), 1000);
    c.remove(&k(0)).unwrap();
    assert_eq!(c.stats.bytes_in_use.load(Ordering::Relaxed), 0);
    assert!(c.try_get(&k(0)).is_none());
    c.remove(&k(99)).expect("remove of missing key is a no-op");
}

#[test]
fn clear_all_drops_everything_and_resets_bytes() {
    let (td, c) = open_cache(1024 * 1024);
    for i in 0..5 {
        c.insert(k(i), &vec![i as u8; 1000]).unwrap();
    }
    assert_eq!(c.stats.bytes_in_use.load(Ordering::Relaxed), 5000);
    let (files, bytes) = c.clear_all().unwrap();
    assert_eq!(files, 5);
    assert_eq!(bytes, 5000);
    assert_eq!(c.stats.bytes_in_use.load(Ordering::Relaxed), 0);
    assert_eq!(c.live_keys().len(), 0);
    let on_disk: Vec<_> = std::fs::read_dir(td.path())
        .unwrap()
        .flatten()
        .filter(|e| e.metadata().map(|m| m.is_file()).unwrap_or(false))
        .collect();
    assert_eq!(on_disk.len(), 0);
}

#[test]
fn clear_all_sweeps_stray_untracked_files() {
    let (td, c) = open_cache(1024 * 1024);
    c.insert(k(0), &[1; 100]).unwrap();
    std::fs::write(td.path().join("stray-orphan"), b"junk").unwrap();
    let (_, _) = c.clear_all().unwrap();
    let on_disk: Vec<_> = std::fs::read_dir(td.path())
        .unwrap()
        .flatten()
        .filter(|e| e.metadata().map(|m| m.is_file()).unwrap_or(false))
        .collect();
    assert_eq!(on_disk.len(), 0, "stray file must also be swept");
}

#[test]
fn reconcile_drops_entries_whose_files_vanished() {
    let (td, c) = open_cache(1024 * 1024);
    c.insert(k(0), &vec![0; 1000]).unwrap();
    c.insert(k(1), &vec![1; 1000]).unwrap();
    c.insert(k(2), &vec![2; 1000]).unwrap();
    let path1 = td.path().join(k(1).cache_filename());
    std::fs::remove_file(&path1).unwrap();
    let (dropped, reclaimed) = c.reconcile_with_disk();
    assert_eq!(dropped, 1);
    assert_eq!(reclaimed, 1000);
    assert_eq!(c.stats.bytes_in_use.load(Ordering::Relaxed), 2000);
    assert_eq!(c.stats.reconcile_drops.load(Ordering::Relaxed), 1);
    assert!(c.try_get(&k(1)).is_none());
    assert!(c.try_get(&k(0)).is_some());
    assert!(c.try_get(&k(2)).is_some());
}

#[test]
fn reconcile_no_op_when_disk_matches_memory() {
    let (_td, c) = open_cache(1024 * 1024);
    c.insert(k(0), &vec![0; 1000]).unwrap();
    let (dropped, reclaimed) = c.reconcile_with_disk();
    assert_eq!(dropped, 0);
    assert_eq!(reclaimed, 0);
    assert_eq!(c.stats.reconcile_drops.load(Ordering::Relaxed), 0);
}

#[test]
fn insert_under_one_byte_max_evicts_immediately() {
    let (_td, c) = open_cache(0);
    c.insert(k(0), &[0; 100]).unwrap();
    assert!(
        c.try_get(&k(0)).is_none(),
        "max_bytes=0 forces immediate eviction"
    );
    assert_eq!(c.stats.bytes_in_use.load(Ordering::Relaxed), 0);
    assert_eq!(c.stats.evictions.load(Ordering::Relaxed), 1);
}
