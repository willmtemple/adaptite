//! Validates that adaptite's public API is sufficient to build *fine-grained collections*:
//! data structures where observers depend on specific items rather than the whole container.
//!
//! This is the mechanism that keyed list projection (`map_keyed`), deeply-observable structs,
//! and persistent-structure wrappers will build on post-0.1: one lazily-created [`Source`] per
//! addressable part (here, per key), plus one "structure" source for existence and iteration.
//! The test suite doubles as the reference implementation pattern.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use adaptite::{Reactor, Source};
use runite::{queue_macrotask, run};

/// A reactive map with per-key dependency granularity, built on public API only.
struct ReactiveMap<K, V> {
    reactor: Reactor,
    entries: RefCell<HashMap<K, V>>,
    /// Lazily-materialized dependency unit per key. Reading a key (present or absent)
    /// observes its source; writing, inserting, or removing that key triggers it.
    key_sources: RefCell<HashMap<K, Source>>,
    /// Dependency unit for the *shape* of the map: iteration and len observe it; any
    /// insertion or removal triggers it.
    structure: Source,
}

impl<K: Clone + Eq + std::hash::Hash, V: Clone> ReactiveMap<K, V> {
    fn new(reactor: &Reactor) -> Self {
        Self {
            reactor: reactor.clone(),
            entries: RefCell::new(HashMap::new()),
            key_sources: RefCell::new(HashMap::new()),
            structure: reactor.source(),
        }
    }

    fn key_source(&self, key: &K) -> Source {
        self.key_sources
            .borrow_mut()
            .entry(key.clone())
            .or_insert_with(|| self.reactor.source())
            .clone()
    }

    /// Reads one key, depending only on that key.
    fn get(&self, key: &K) -> Option<V> {
        self.key_source(key).observe();
        self.entries.borrow().get(key).cloned()
    }

    /// Reads the number of entries, depending only on the map's shape.
    fn len(&self) -> usize {
        self.structure.observe();
        self.entries.borrow().len()
    }

    fn insert(&self, key: K, value: V) {
        let existed = self
            .entries
            .borrow_mut()
            .insert(key.clone(), value)
            .is_some();
        self.key_source(&key).trigger();
        if !existed {
            self.structure.trigger();
        }
    }

    fn remove(&self, key: &K) {
        if self.entries.borrow_mut().remove(key).is_some() {
            self.key_source(key).trigger();
            self.structure.trigger();
        }
    }

    /// Drops per-key sources that no longer have observers. This is the pattern
    /// `Source::is_observed` exists for: without it, a long-lived collection accumulates one
    /// source per key ever read.
    fn collect_garbage(&self) {
        self.key_sources
            .borrow_mut()
            .retain(|_, source| source.is_observed());
    }

    fn source_count(&self) -> usize {
        self.key_sources.borrow().len()
    }
}

#[test]
fn observers_depend_on_individual_keys_not_the_whole_map() {
    let counts = Rc::new(RefCell::new(HashMap::<&str, usize>::new()));
    let bump = |counts: &Rc<RefCell<HashMap<&str, usize>>>, who: &'static str| {
        *counts.borrow_mut().entry(who).or_default() += 1;
    };

    queue_macrotask({
        let counts = Rc::clone(&counts);
        move || {
            let reactor = Reactor::new();
            let map = Rc::new(ReactiveMap::<String, i64>::new(&reactor));
            map.insert("a".into(), 1);
            map.insert("b".into(), 2);

            // Four observers with four distinct footprints: key a, key b, the (absent) key c,
            // and the shape of the map.
            {
                let map = Rc::clone(&map);
                let counts = Rc::clone(&counts);
                reactor
                    .effect(move || {
                        let _ = map.get(&"a".into());
                        bump(&counts, "a");
                    })
                    .leak()
            };
            {
                let map = Rc::clone(&map);
                let counts = Rc::clone(&counts);
                reactor
                    .effect(move || {
                        let _ = map.get(&"b".into());
                        bump(&counts, "b");
                    })
                    .leak()
            };
            {
                let map = Rc::clone(&map);
                let counts = Rc::clone(&counts);
                reactor
                    .effect(move || {
                        let _ = map.get(&"c".into());
                        bump(&counts, "c");
                    })
                    .leak()
            };
            {
                let map = Rc::clone(&map);
                let counts = Rc::clone(&counts);
                reactor
                    .effect(move || {
                        let _ = map.len();
                        bump(&counts, "len");
                    })
                    .leak()
            };

            reactor.flush_now();
            let baseline = counts.borrow().clone();
            assert!(baseline.values().all(|&count| count == 1));

            // Updating an existing key: only that key's reader re-runs.
            map.insert("b".into(), 20);
            reactor.flush_now();
            assert_eq!(counts.borrow()["a"], 1);
            assert_eq!(counts.borrow()["b"], 2);
            assert_eq!(counts.borrow()["c"], 1);
            assert_eq!(counts.borrow()["len"], 1);

            // Inserting a previously-absent key: its reader AND the shape observer re-run.
            map.insert("c".into(), 3);
            reactor.flush_now();
            assert_eq!(counts.borrow()["a"], 1);
            assert_eq!(counts.borrow()["b"], 2);
            assert_eq!(counts.borrow()["c"], 2);
            assert_eq!(counts.borrow()["len"], 2);

            // Removing a key: its reader and the shape observer re-run.
            map.remove(&"a".into());
            reactor.flush_now();
            assert_eq!(counts.borrow()["a"], 2);
            assert_eq!(counts.borrow()["b"], 2);
            assert_eq!(counts.borrow()["c"], 2);
            assert_eq!(counts.borrow()["len"], 3);
        }
    });

    run();
}

#[test]
fn per_key_sources_are_garbage_collected_once_unobserved() {
    let final_sources = Rc::new(Cell::new(usize::MAX));

    queue_macrotask({
        let final_sources = Rc::clone(&final_sources);
        move || {
            let reactor = Reactor::new();
            let map = Rc::new(ReactiveMap::<u32, u32>::new(&reactor));
            for key in 0..100 {
                map.insert(key, key);
            }

            // One effect reads every key; a second reads only key 0.
            let broad = reactor.effect({
                let map = Rc::clone(&map);
                move || {
                    for key in 0..100 {
                        let _ = map.get(&key);
                    }
                }
            });
            let narrow = reactor.effect({
                let map = Rc::clone(&map);
                move || {
                    let _ = map.get(&0);
                }
            });
            reactor.flush_now();

            assert_eq!(map.source_count(), 100);
            map.collect_garbage();
            assert_eq!(map.source_count(), 100, "all keys are still observed");

            // Disposing the broad reader leaves only key 0 observed.
            broad.dispose();
            map.collect_garbage();
            final_sources.set(map.source_count());

            narrow.leak();
        }
    });

    run();
    assert_eq!(
        final_sources.get(),
        1,
        "sources without observers must be collectable via is_observed"
    );
}
