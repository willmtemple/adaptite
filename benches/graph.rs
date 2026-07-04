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

criterion_group!(
    benches,
    signal_write_read,
    deep_chain,
    wide_fanout,
    layered_diamonds
);
criterion_main!(benches);
