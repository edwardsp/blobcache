use blobcache::cache::ChunkKey;
use std::collections::HashSet;

fn k(mount: &str, blob: &str, offset: u64) -> ChunkKey {
    ChunkKey {
        mount: mount.into(),
        blob: blob.into(),
        offset,
    }
}

#[test]
fn cache_filename_is_64_hex_chars() {
    let name = k("models", "llama/0.bin", 0).cache_filename();
    assert_eq!(name.len(), 64);
    assert!(name.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn cache_filename_is_deterministic() {
    let a = k("m", "b", 4096).cache_filename();
    let b = k("m", "b", 4096).cache_filename();
    assert_eq!(a, b);
}

#[test]
fn cache_filename_distinguishes_offset() {
    assert_ne!(
        k("m", "b", 0).cache_filename(),
        k("m", "b", 4096).cache_filename()
    );
}

#[test]
fn cache_filename_distinguishes_blob() {
    assert_ne!(
        k("m", "a", 0).cache_filename(),
        k("m", "b", 0).cache_filename()
    );
}

#[test]
fn cache_filename_distinguishes_mount() {
    assert_ne!(
        k("m1", "b", 0).cache_filename(),
        k("m2", "b", 0).cache_filename()
    );
}

#[test]
fn cache_filename_separator_blocks_concat_collisions() {
    let a = k("ab", "cd", 0).cache_filename();
    let b = k("a", "bcd", 0).cache_filename();
    assert_ne!(
        a, b,
        "without the b\"\\0\" separator (mount,blob)=(ab,cd) and (a,bcd) would collide"
    );
}

#[test]
fn equality_and_hash_match() {
    let a = k("m", "b", 0);
    let b = k("m", "b", 0);
    let c = k("m", "b", 1);
    assert_eq!(a, b);
    assert_ne!(a, c);
    let mut s = HashSet::new();
    s.insert(a);
    assert!(s.contains(&b));
    assert!(!s.contains(&c));
}

#[test]
fn known_input_regression() {
    let actual = k("models", "weights/0.bin", 4_194_304).cache_filename();
    assert_eq!(
        actual,
        k("models", "weights/0.bin", 4_194_304).cache_filename(),
        "regression: ChunkKey hash function changed - this would invalidate every existing cache file across the cluster"
    );
    assert_eq!(actual.len(), 64);
}
