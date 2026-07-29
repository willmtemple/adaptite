//! The ambient reactor's diagnostic contract.
//!
//! Creating reactive state while the thread has no default reactor installed produces a fresh,
//! silent graph: reads and writes work perfectly and nothing ever re-renders. That is the failure
//! `Reactor::current` warns about, and these tests pin *when* it warns — the interesting part is
//! the case that must stay quiet as much as the case that must not.
//!
//! Each test runs on a thread it spawns itself. The state under test is thread-local, so relying
//! on the harness to give every test its own thread would make these pass or fail depending on
//! `--test-threads`.

use std::sync::{Arc, Mutex};
use std::thread;

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

use adaptite::{Reactor, signal};

/// Records the `event = "..."` field adaptite tags its structured logs with, plus the level.
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<(Level, String)>>>);

impl Recorder {
    fn events_at(&self, level: Level) -> Vec<String> {
        self.0
            .lock()
            .expect("recorder poisoned")
            .iter()
            .filter(|(recorded, _)| *recorded == level)
            .map(|(_, name)| name.clone())
            .collect()
    }
}

#[derive(Default)]
struct EventName(Option<String>);

impl Visit for EventName {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "event" {
            self.0 = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "event" {
            self.0 = Some(format!("{value:?}").trim_matches('"').to_owned());
        }
    }
}

impl<S: Subscriber> Layer<S> for Recorder {
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let mut name = EventName::default();
        event.record(&mut name);
        if let Some(name) = name.0 {
            self.0
                .lock()
                .expect("recorder poisoned")
                .push((*event.metadata().level(), name));
        }
    }
}

/// Runs `body` on its own thread with a recording subscriber installed.
fn recording<R: Send + 'static>(body: impl FnOnce(&Recorder) -> R + Send + 'static) -> R {
    thread::spawn(move || {
        let recorder = Recorder::default();
        let _guard = tracing_subscriber::registry()
            .with(recorder.clone())
            .set_default();
        body(&recorder)
    })
    .join()
    .expect("test thread panicked")
}

#[test]
fn a_thread_that_never_entered_installs_a_default_quietly() {
    recording(|recorder| {
        // A script, a doctest, or a test body that just wants a graph. Nothing is wrong here, and
        // warning would train everyone to ignore the warning.
        let value = signal(1_u32);
        value.set(2);
        assert_eq!(value.get(), 2);

        assert!(
            recorder.events_at(Level::WARN).is_empty(),
            "a virgin thread must stay quiet, or the diagnostic is noise"
        );
        assert!(
            recorder
                .events_at(Level::DEBUG)
                .contains(&"current_reactor_install".to_owned())
        );
    });
}

#[test]
fn creating_state_after_a_scoped_enter_has_ended_warns() {
    recording(|recorder| {
        // The shape a UI framework has: the reactor is entered around a render or a callback and
        // not held in between. This is exactly the case the 0.2 warning missed, because nothing
        // *expired* — the guard correctly restored "no default", so the install below is a first
        // install on an empty slot rather than a replacement.
        let application = Reactor::new();
        let inside = {
            let _guard = application.enter();
            signal(0_u32).reactor().id()
        };
        assert_eq!(inside, application.id());
        assert!(
            recorder.events_at(Level::WARN).is_empty(),
            "nothing is wrong while the guard is held"
        );

        // A timer, a task, a Drop, or a test body reaching for an ambient constructor after the
        // render finished. It lands on a graph nobody flushes.
        let outside = signal(0_u32).reactor().id();
        assert_ne!(
            outside,
            application.id(),
            "this is the silent failure being diagnosed"
        );
        assert_eq!(
            recorder.events_at(Level::WARN),
            ["current_reactor_reinstall"],
            "and it must be reported exactly once"
        );
    });
}

#[test]
fn holding_the_reactor_for_the_whole_program_never_warns() {
    recording(|recorder| {
        // The contract the warning points at: enter once, for as long as ambient constructors may
        // run. An application that does this is never diagnosed, however much state it creates.
        let application = Reactor::new();
        let _guard = application.enter();

        for _ in 0..8 {
            let value = signal(0_u32);
            assert_eq!(value.reactor().id(), application.id());
        }

        assert!(recorder.events_at(Level::WARN).is_empty());
    });
}

#[test]
fn an_expired_default_still_warns() {
    recording(|recorder| {
        // The case 0.2 already covered, kept: a default installed implicitly, then dropped because
        // nothing held it, then replaced. The new rule is a superset of the old one.
        let first = signal(0_u32).reactor().id();
        let second = signal(0_u32).reactor().id();

        assert_ne!(first, second, "nothing kept the first graph alive");
        assert_eq!(
            recorder.events_at(Level::WARN),
            ["current_reactor_reinstall"]
        );
    });
}
