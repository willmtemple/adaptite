//! Baseline benchmarks for reactive graph propagation.
//!
//! These exercise the pull-based paths (signal writes, memo/thunk reads) without the runite
//! event loop, so they measure graph bookkeeping rather than scheduling.

use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use adaptite::{Memo, Reactor, memo_in, signal_in};

/// Write-then-read on a single signal: the floor for tracking overhead.
fn signal_write_read(c: &mut Criterion) {
    let reactor = Reactor::new();
    let signal = signal_in(&reactor, 0u64);

    c.bench_function("signal_write_read", |b| {
        let mut value = 0u64;
        b.iter(|| {
            value += 1;
            signal.set(black_box(value));
            black_box(signal.get())
        });
    });
}

/// Writing a signal the value it already holds: the suppressed path, which returns before
/// touching the graph and now also reports the discarded write when anyone is listening.
fn signal_write_suppressed(c: &mut Criterion) {
    let reactor = Reactor::new();
    let signal = signal_in(&reactor, 7u64);

    c.bench_function("signal_write_suppressed", |b| {
        b.iter(|| signal.set(black_box(7u64)));
    });
}

/// A linear chain of memos: invalidation and verification walk the full depth.
fn deep_chain(c: &mut Criterion) {
    const DEPTH: usize = 100;

    let reactor = Reactor::new();
    let source = signal_in(&reactor, 0u64);

    let mut tail: Memo<u64> = memo_in(&reactor, {
        let source = source.clone();
        move || source.get() + 1
    });
    for _ in 1..DEPTH {
        let previous = tail.clone();
        tail = memo_in(&reactor, move || previous.get() + 1);
    }

    c.bench_function("deep_chain_100_invalidate_and_pull", |b| {
        let mut value = 0u64;
        b.iter(|| {
            value += 1;
            source.set(black_box(value));
            black_box(tail.get())
        });
    });
}

/// One signal fanned out to many memos, gathered by a single collector.
fn wide_fanout(c: &mut Criterion) {
    const WIDTH: u64 = 100;

    let reactor = Reactor::new();
    let source = signal_in(&reactor, 0u64);

    let layer: Vec<Memo<u64>> = (0..WIDTH)
        .map(|offset| {
            memo_in(&reactor, {
                let source = source.clone();
                move || source.get() + offset
            })
        })
        .collect();
    let collector = memo_in(&reactor, move || layer.iter().map(Memo::get).sum::<u64>());

    c.bench_function("wide_fanout_100_invalidate_and_pull", |b| {
        let mut value = 0u64;
        b.iter(|| {
            value += 1;
            source.set(black_box(value));
            black_box(collector.get())
        });
    });
}

/// Stacked diamonds: the shape that forced redundant recomputation under eager invalidation.
fn layered_diamonds(c: &mut Criterion) {
    const LAYERS: usize = 10;

    let reactor = Reactor::new();
    let source = signal_in(&reactor, 0u64);

    let mut join: Memo<u64> = memo_in(&reactor, {
        let source = source.clone();
        move || source.get()
    });
    for _ in 0..LAYERS {
        let left = memo_in(&reactor, {
            let join = join.clone();
            move || join.get() + 1
        });
        let right = memo_in(&reactor, {
            let join = join.clone();
            move || join.get() * 2
        });
        join = memo_in(&reactor, move || left.get() + right.get());
    }

    c.bench_function("layered_diamonds_10_invalidate_and_pull", |b| {
        let mut value = 0u64;
        b.iter(|| {
            value += 1;
            source.set(black_box(value));
            black_box(join.get())
        });
    });
}

/// Node allocation and teardown, which is where the per-kind gauges and the lifecycle events sit.
fn node_churn(c: &mut Criterion) {
    let reactor = Reactor::new();

    c.bench_function("node_create_and_dispose", |b| {
        b.iter(|| {
            let signal = signal_in(&reactor, black_box(0u64));
            black_box(signal.get());
        });
    });
}

/// Edge churn: an observer whose dependency set is re-recorded on every run.
///
/// This is the hot path the maintained edge counters ride on — `try_observe` runs once per tracked
/// read — so it is the measurement that decides whether "counters are always maintained" stays
/// affordable. Compare against `git stash`-ing the counter calls if the contract is ever in doubt.
fn edge_churn(c: &mut Criterion) {
    const WIDTH: u64 = 32;

    let reactor = Reactor::new();
    let inputs: Vec<_> = (0..WIDTH).map(|i| signal_in(&reactor, i)).collect();
    let sum = memo_in(&reactor, {
        let inputs = inputs.clone();
        move || inputs.iter().map(adaptite::Signal::get).sum::<u64>()
    });

    c.bench_function("edge_churn_32_rerecord", |b| {
        let mut value = 0u64;
        b.iter(|| {
            value += 1;
            inputs[0].set(black_box(value));
            black_box(sum.get())
        });
    });
}

/// The snapshot itself, which must not depend on graph size.
///
/// Run this against a small and a large graph: `graph_stats` is `O(1)` by construction, and a
/// reading that scales with node count means something started walking.
fn graph_stats_snapshot(c: &mut Criterion) {
    const WIDTH: u64 = 1_000;

    let reactor = Reactor::new();
    let source = signal_in(&reactor, 0u64);
    let layer: Vec<Memo<u64>> = (0..WIDTH)
        .map(|offset| {
            let source = source.clone();
            memo_in(&reactor, move || source.get() + offset)
        })
        .collect();
    for memo in &layer {
        black_box(memo.get());
    }

    c.bench_function("graph_stats_1000_nodes", |b| {
        b.iter(|| black_box(reactor.graph_stats()));
    });
}

criterion_group!(
    benches,
    signal_write_read,
    signal_write_suppressed,
    deep_chain,
    wide_fanout,
    layered_diamonds,
    node_churn,
    edge_churn,
    graph_stats_snapshot
);
criterion_main!(benches);
