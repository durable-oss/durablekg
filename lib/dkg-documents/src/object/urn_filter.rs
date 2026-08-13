//! A Bloom filter over the URNs currently bound to a document.
//!
//! # Why
//!
//! Every write calls [`check_urns_free`], and that check is a *separate*
//! round-trip through the worker pool before any work happens: a queue
//! enqueue, a thread handoff, and a wakeup, paid per document. In a bulk
//! import — where nearly every URN is new and the check almost always comes
//! back "free" — that scheduling cost buys nothing.
//!
//! This filter answers the common case without leaving the caller's thread.
//!
//! # The asymmetry that makes it safe
//!
//! A Bloom filter has one-sided error: it may claim a URN is present when it
//! is not, but never the reverse. That maps exactly onto what we need.
//!
//! - **Definitely absent** → the URN is free. Skip the pool round-trip.
//! - **Possibly present** → fall through to the real check.
//!
//! A false positive therefore costs one lookup, which is what *every* write
//! pays today. The filter is a strict improvement so long as it never reports
//! absent for a URN that is present.
//!
//! # Deletes
//!
//! A standard Bloom filter cannot remove an entry: clearing a bit could mask
//! some other URN that happens to share it, turning a false positive into a
//! false negative — the one error we cannot tolerate. So deletes are simply
//! not applied. A deleted URN stays "possibly present" and costs one wasted
//! lookup until the filter is rebuilt, which is the same cost as today.
//!
//! Rebuild with [`UrnFilter::rebuild_from`] when the wasted lookups outweigh
//! the scan, or when [`saturation`](UrnFilter::saturation) says the filter has
//! taken on more keys than it was sized for.
//!
//! # Concurrency
//!
//! Bits live in `AtomicU64` and are set with `fetch_or`, so inserts need no
//! lock and readers never block. Adding a URN is monotonic (bits only turn
//! on), so a concurrent reader either sees the bit or does not — and if it
//! does not, it falls through to the authoritative check. There is no
//! interleaving that produces a false negative.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Bits per key. At ~10 bits with 7 hash probes a Bloom filter runs near a 1%
/// false-positive rate, which costs one lookup per hundred writes — the same
/// trade lsm-tree makes for its own table filters.
const BITS_PER_KEY: usize = 10;

/// Hash probes per key. The theoretical optimum for 10 bits/key is ~6.9.
const PROBES: usize = 7;

/// Smallest filter, so a tiny or empty store still gets sane behaviour rather
/// than a zero-length bit array.
const MIN_BITS: usize = 1024;

/// A probabilistic set of the URNs bound to some document.
///
/// Cheap to share: the bits are atomics, so a clone of the containing `Arc`
/// reads and inserts without coordination.
#[derive(Debug)]
pub struct UrnFilter {
    bits: Vec<AtomicU64>,
    /// Total bit positions, i.e. `bits.len() * 64`. Cached to keep the modulo
    /// off the hot path's critical section.
    bit_count: usize,
    /// How many URNs have been inserted since the last rebuild. Only used to
    /// report [`saturation`](Self::saturation); accuracy under races is not
    /// important.
    inserted: AtomicUsize,
    /// What the filter was sized for, so saturation is meaningful.
    capacity: usize,
}

impl UrnFilter {
    /// A filter sized for `expected_keys` URNs.
    ///
    /// Oversizing is cheap (10 bits per key is 1.25 bytes) and undersizing
    /// degrades to more false positives rather than to incorrect answers, so
    /// callers should round up.
    pub fn with_capacity(expected_keys: usize) -> Self {
        let bit_count = (expected_keys * BITS_PER_KEY).max(MIN_BITS).next_power_of_two();
        let words = bit_count / 64;
        let mut bits = Vec::with_capacity(words);
        bits.resize_with(words, || AtomicU64::new(0));

        UrnFilter {
            bits,
            bit_count,
            inserted: AtomicUsize::new(0),
            capacity: expected_keys.max(MIN_BITS / BITS_PER_KEY),
        }
    }

    /// Record that `urn` is bound to a document.
    ///
    /// Must be called for every URN written, or the filter can report a
    /// false negative and a write would skip a check it needed.
    pub fn insert(&self, urn: &str) {
        for index in self.probes(urn) {
            let word = index / 64;
            let bit = index % 64;
            self.bits[word].fetch_or(1u64 << bit, Ordering::Relaxed);
        }
        self.inserted.fetch_add(1, Ordering::Relaxed);
    }

    /// Whether `urn` might be bound to a document.
    ///
    /// `false` is authoritative: the URN is definitely free, and the caller
    /// can skip the store lookup. `true` means "maybe", and the caller must
    /// do the real check.
    pub fn might_contain(&self, urn: &str) -> bool {
        for index in self.probes(urn) {
            let word = index / 64;
            let bit = index % 64;
            if self.bits[word].load(Ordering::Relaxed) & (1u64 << bit) == 0 {
                return false;
            }
        }
        true
    }

    /// Replace the contents with `urns`, sized for what it was given.
    ///
    /// This is the answer to deletes: a filter that has accumulated removed
    /// URNs is discarded rather than mutated. Returns the new filter instead
    /// of mutating in place so a rebuild can be swapped in atomically while
    /// readers are still using the old one.
    pub fn rebuild_from<'a>(urns: impl IntoIterator<Item = &'a str>, size_hint: usize) -> Self {
        let filter = UrnFilter::with_capacity(size_hint);
        for urn in urns {
            filter.insert(urn);
        }
        filter
    }

    /// How full the filter is relative to what it was sized for.
    ///
    /// Above 1.0 the false-positive rate is climbing past the design target
    /// and a rebuild at a larger capacity is worth doing. This never affects
    /// correctness — only how often a caller pays a needless lookup.
    pub fn saturation(&self) -> f64 {
        self.inserted.load(Ordering::Relaxed) as f64 / self.capacity as f64
    }

    /// How many URNs have been inserted since construction.
    pub fn len(&self) -> usize {
        self.inserted.load(Ordering::Relaxed)
    }

    /// Whether nothing has been inserted yet.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bit positions for a key.
    ///
    /// Uses double hashing (Kirsch–Mitzenmacher): two independent 64-bit
    /// hashes generate all `PROBES` positions as `h1 + i*h2`, which is
    /// asymptotically as good as `PROBES` independent hashes and costs two.
    /// `h2` is forced odd so that, with a power-of-two `bit_count`, stepping
    /// by it visits distinct positions rather than cycling early.
    fn probes(&self, key: &str) -> impl Iterator<Item = usize> + '_ {
        let h1 = fnv1a(key.as_bytes(), 0xcbf2_9ce4_8422_2325);
        let h2 = fnv1a(key.as_bytes(), 0x9e37_79b9_7f4a_7c15) | 1;
        let mask = self.bit_count - 1;
        (0..PROBES).map(move |i| (h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize) & mask)
    }
}

/// FNV-1a with a caller-supplied offset basis.
///
/// Chosen over a stronger hash because the input is short (a URN) and the
/// cost is paid on every write: FNV-1a is a handful of instructions per byte
/// with no setup. Bloom filters only need reasonable distribution, not
/// cryptographic strength, and the two different bases give the two
/// independent hashes double hashing wants.
fn fnv1a(bytes: &[u8], basis: u64) -> u64 {
    let mut hash = basis;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_inserted_urn_is_reported_present() {
        let filter = UrnFilter::with_capacity(1000);
        filter.insert("urn:businesses:slug:acme");
        assert!(filter.might_contain("urn:businesses:slug:acme"));
    }

    #[test]
    fn never_reports_a_false_negative() {
        // The one guarantee that matters: everything inserted must always
        // report present, or a write would skip a check it needed.
        let filter = UrnFilter::with_capacity(10_000);
        let urns: Vec<String> = (0..10_000)
            .map(|i| format!("urn:businesses:slug:entity-{i}"))
            .collect();
        for urn in &urns {
            filter.insert(urn);
        }
        for urn in &urns {
            assert!(filter.might_contain(urn), "false negative for {urn}");
        }
    }

    #[test]
    fn an_empty_filter_reports_everything_absent() {
        let filter = UrnFilter::with_capacity(1000);
        assert!(!filter.might_contain("urn:businesses:slug:nothing"));
    }

    #[test]
    fn false_positive_rate_stays_near_the_design_target() {
        let filter = UrnFilter::with_capacity(10_000);
        for i in 0..10_000 {
            filter.insert(&format!("urn:businesses:slug:present-{i}"));
        }

        // Probe with keys that were never inserted; count the false hits.
        let probes = 10_000;
        let false_positives = (0..probes)
            .filter(|i| filter.might_contain(&format!("urn:businesses:slug:absent-{i}")))
            .count();
        let rate = false_positives as f64 / probes as f64;

        // ~1% is the target for 10 bits/key. Allow generous headroom: this
        // asserts the filter is not degenerate, not that FNV hits the
        // theoretical optimum.
        assert!(rate < 0.05, "false-positive rate {rate} is too high");
    }

    #[test]
    fn a_rebuild_keeps_the_urns_it_was_given() {
        let urns = ["urn:a:b:one", "urn:a:b:two", "urn:a:b:three"];
        let filter = UrnFilter::rebuild_from(urns.iter().copied(), 100);
        for urn in urns {
            assert!(filter.might_contain(urn));
        }
    }

    #[test]
    fn a_rebuild_drops_urns_left_out_of_it() {
        // This is how a delete eventually stops costing a wasted lookup.
        let filter = UrnFilter::with_capacity(100);
        filter.insert("urn:a:b:deleted");
        assert!(filter.might_contain("urn:a:b:deleted"));

        let rebuilt = UrnFilter::rebuild_from(["urn:a:b:kept"], 100);
        assert!(rebuilt.might_contain("urn:a:b:kept"));
        // Not a hard guarantee (a false positive is legal), but with one key
        // in a 1024-bit filter a collision is vanishingly unlikely.
        assert!(!rebuilt.might_contain("urn:a:b:deleted"));
    }

    #[test]
    fn saturation_tracks_inserts_against_capacity() {
        let filter = UrnFilter::with_capacity(1000);
        assert_eq!(filter.saturation(), 0.0);
        for i in 0..500 {
            filter.insert(&format!("urn:a:b:{i}"));
        }
        assert!((filter.saturation() - 0.5).abs() < 0.01);
    }

    #[test]
    fn a_tiny_capacity_still_produces_a_usable_filter() {
        // Guards the MIN_BITS floor: a zero-capacity filter must not panic on
        // a modulo by zero or index an empty vector.
        let filter = UrnFilter::with_capacity(0);
        filter.insert("urn:a:b:c");
        assert!(filter.might_contain("urn:a:b:c"));
        assert!(!filter.might_contain("urn:a:b:d"));
    }

    #[test]
    fn concurrent_inserts_are_all_visible() {
        use std::sync::Arc;

        let filter = Arc::new(UrnFilter::with_capacity(10_000));
        let handles: Vec<_> = (0..8)
            .map(|thread| {
                let filter = Arc::clone(&filter);
                std::thread::spawn(move || {
                    for i in 0..1000 {
                        filter.insert(&format!("urn:a:t{thread}:{i}"));
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        // Every URN any thread inserted must be present: a lost update here
        // would be a false negative.
        for thread in 0..8 {
            for i in 0..1000 {
                assert!(filter.might_contain(&format!("urn:a:t{thread}:{i}")));
            }
        }
    }
}

// Copyright (c) 2026 Durable Programming, LLC. All rights reserved.
