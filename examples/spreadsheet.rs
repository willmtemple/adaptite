//! A four-cell spreadsheet demonstrating what makes fine-grained reactivity fine-grained:
//!
//! - **Formulas are memos.** They recompute only when a cell they actually read changes, and
//!   equal results stop propagation.
//! - **Dependencies are dynamic.** A formula that switches which cells it reads (here, based on
//!   a mode cell) is re-tracked on every computation: edits to cells it no longer reads are
//!   ignored entirely.
//! - **Displays are effects.** The "UI" below re-renders only when the formula's value actually
//!   changed.
//!
//! Run with `cargo run --example spreadsheet`. Watch the `[compute]` lines: they appear only
//! when a formula genuinely needs to recompute.

use std::cell::Cell as Counter;
use std::rc::Rc;
use std::time::Duration;

use adaptite::{effect, memo, signal};
use runite::time::sleep;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Sum,
    Product,
}

#[runite::main]
async fn main() {
    // The sheet: three number cells and a mode cell that selects the formula.
    let a1 = signal(10i64);
    let a2 = signal(20i64);
    let b1 = signal(7i64);
    let mode = signal(Mode::Sum);

    let computes = Rc::new(Counter::new(0usize));

    // RESULT = if mode == Sum { A1 + A2 } else { A1 * B1 }.
    //
    // Dependency tracking happens per computation: while mode is Sum, this memo depends on
    // {mode, a1, a2} and edits to b1 are invisible to it. After switching to Product it depends
    // on {mode, a1, b1} and edits to a2 become invisible instead.
    let result = memo({
        let a1 = a1.clone();
        let a2 = a2.clone();
        let b1 = b1.clone();
        let mode = mode.clone();
        let computes = Rc::clone(&computes);
        move || {
            computes.set(computes.get() + 1);
            let (value, formula) = match mode.get() {
                Mode::Sum => (a1.get() + a2.get(), "A1 + A2"),
                Mode::Product => (a1.get() * b1.get(), "A1 * B1"),
            };
            println!("  [compute] RESULT = {formula} = {value}");
            value
        }
    });

    // The "display": re-renders only when RESULT actually changes.
    effect({
        let result = result.clone();
        move || println!("  [render]  RESULT is {}", result.get())
    })
    .leak();

    let turn = |label: &'static str| async move {
        // A short sleep starts a fresh macrotask turn; all reactive microtask flushes for the
        // previous edits complete before it fires.
        sleep(Duration::from_millis(10)).await;
        println!("{label}");
    };

    println!("initial render (mode = Sum)");

    turn("edit A2 = 32 (a dependency of the Sum formula)").await;
    a2.set(32);

    turn("edit B1 = 9 (NOT a dependency while mode = Sum -- expect silence)").await;
    b1.set(9);

    turn("edit A1 = 10 to the same value (equal write is suppressed -- expect silence)").await;
    a1.set(10);

    turn("switch mode = Product (formula now reads A1 and B1)").await;
    mode.set(Mode::Product);

    turn("edit A2 = 1000 (no longer a dependency -- expect silence)").await;
    a2.set(1000);

    turn("edit B1 = 10 (a dependency again)").await;
    b1.set(10);

    sleep(Duration::from_millis(10)).await;
    println!(
        "done: 6 edits, but the formula computed only {} times",
        computes.get()
    );
}
