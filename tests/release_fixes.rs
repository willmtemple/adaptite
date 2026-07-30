//! Gates on the release machinery itself.
//!
//! Two things here are not exercised by any other test because they are not Rust: the
//! GitHub Actions workflows, and the shell helper the runite drift check leans on. Both had
//! defects that every green build was compatible with, so they get pinned here.
//!
//! These read files under `.github/`, which `Cargo.toml`'s `exclude` keeps out of the
//! published `.crate`. When this suite runs from an unpacked package rather than a source
//! checkout there is nothing to check, and each test says so and returns. That skip is
//! narrow on purpose: it triggers on the absence of the *repository*, never on the absence
//! of the thing under test.
//!
//! The shell-helper tests are `#[cfg(unix)]`. The helper is bash, and the only job that runs it
//! is the Linux drift check, so there is nothing for a Windows runner to protect here — and
//! shelling out to `bash` from a Windows test asserts the presence of Git Bash rather than
//! anything about adaptite. The workflow-parsing tests below are not gated and run everywhere.

use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::process::{Command, Stdio};

fn repo_file(relative: &str) -> Option<PathBuf> {
    // `.github/` is the marker for "this is a source checkout". If it is missing we are
    // running from an unpacked .crate; if it is present, the file had better be too.
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    if !root.join(".github").is_dir() {
        return None;
    }
    let path = root.join(relative);
    assert!(
        path.is_file(),
        "{relative} is missing from a source checkout that has .github/"
    );
    Some(path)
}

macro_rules! repo_file_or_skip {
    ($relative:expr) => {
        match repo_file($relative) {
            Some(path) => path,
            None => {
                eprintln!("skipping: no .github/ directory, so this is not a source checkout");
                return;
            }
        }
    };
}

// ---------------------------------------------------------------------------------------
// .github/scripts/latest-stable-version.sh
// ---------------------------------------------------------------------------------------

/// Feed an index file to the helper and return what it prints, or `Err` with stderr.
#[cfg(unix)]
fn latest_stable(script: &Path, index: &str) -> Result<String, String> {
    use std::io::Write as _;

    let mut child = Command::new("bash")
        .arg(script)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("bash should be available");
    child
        .stdin
        .take()
        .expect("stdin was piped")
        .write_all(index.as_bytes())
        .expect("the helper should accept its input");
    let out = child.wait_with_output().expect("the helper should finish");

    let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if out.status.success() {
        Ok(stdout)
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

#[cfg(unix)]
fn index_line(vers: &str, yanked: bool) -> String {
    // Shaped like the real sparse index: one JSON object per line, `vers` and `yanked` among
    // other fields, and no whitespace.
    format!(r#"{{"name":"runite","vers":"{vers}","deps":[],"yanked":{yanked}}}"#)
}

#[cfg(unix)]
fn index(entries: &[(&str, bool)]) -> String {
    entries
        .iter()
        .map(|(v, yanked)| index_line(v, *yanked))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The bug this file exists for: the drift gate used to read the *last line* of the index.
/// The index is ordered by publication, so a patch backported to an older minor lands after
/// a newer minor and gets reported as latest -- failing the gate against a current
/// dependency. crates.io really carries this shape; `time` has 0.1.43 between 0.2.9 and
/// 0.2.10.
#[test]
#[cfg(unix)]
fn a_patch_backported_to_an_older_minor_is_not_the_latest_version() {
    let script = repo_file_or_skip!(".github/scripts/latest-stable-version.sh");

    let published_out_of_order = index(&[
        ("0.1.0", false),
        ("0.2.0", false),
        ("0.3.0", false),
        // Published most recently, but not the highest version.
        ("0.2.1", false),
    ]);

    assert_eq!(
        latest_stable(&script, &published_out_of_order).as_deref(),
        Ok("0.3.0"),
        "the highest stable version is 0.3.0, not whatever was uploaded last"
    );
}

#[test]
#[cfg(unix)]
fn a_prerelease_published_after_a_stable_release_is_not_the_latest_version() {
    let script = repo_file_or_skip!(".github/scripts/latest-stable-version.sh");

    let with_prerelease = index(&[("0.3.0", false), ("0.4.0-alpha.1", false)]);

    assert_eq!(
        latest_stable(&script, &with_prerelease).as_deref(),
        Ok("0.3.0"),
        "a prerelease is not something adaptite can be expected to track"
    );
}

/// Yanked entries stay in the index file forever, so a yanked upload is still the last line.
#[test]
#[cfg(unix)]
fn a_yanked_release_is_not_the_latest_version() {
    let script = repo_file_or_skip!(".github/scripts/latest-stable-version.sh");

    let with_yank = index(&[("0.3.0", false), ("0.4.0", true)]);

    assert_eq!(
        latest_stable(&script, &with_yank).as_deref(),
        Ok("0.3.0"),
        "a yanked 0.4.0 must not be reported as published"
    );
}

/// Lexicographic sorting would say 0.9.0 > 0.10.0.
#[test]
#[cfg(unix)]
fn versions_are_ordered_numerically_not_lexicographically() {
    let script = repo_file_or_skip!(".github/scripts/latest-stable-version.sh");

    let two_digit_minor = index(&[("0.2.0", false), ("0.9.0", false), ("0.10.0", false)]);

    assert_eq!(
        latest_stable(&script, &two_digit_minor).as_deref(),
        Ok("0.10.0")
    );
}

/// Failing loudly beats printing an empty string that the caller then compares against.
#[test]
#[cfg(unix)]
fn an_index_with_no_stable_release_is_an_error_not_an_empty_answer() {
    let script = repo_file_or_skip!(".github/scripts/latest-stable-version.sh");

    let nothing_usable = index(&[("0.4.0-alpha.1", false), ("0.3.0", true)]);

    let err = latest_stable(&script, &nothing_usable)
        .expect_err("no stable unyanked version means the helper must fail");
    assert!(
        err.contains("no stable, unyanked version"),
        "expected an explanatory failure, got: {err}"
    );
}

// ---------------------------------------------------------------------------------------
// .github/workflows/{ci,release}.yml
// ---------------------------------------------------------------------------------------

struct Step {
    advisory: bool,
    run: String,
}

/// A deliberately small reader for the one workflow shape this repo writes: two-space
/// indentation, jobs at depth 1, steps as a `- ` list under `steps:`, and `run:` either
/// inline or as a `|` block. It only needs to find `run:` payloads and whether the step or
/// its job is `continue-on-error`.
fn steps_of(workflow: &str) -> Vec<Step> {
    let mut steps = Vec::new();
    let mut job_advisory = false;
    let mut step_advisory = false;
    let mut block: Option<(usize, String)> = None;

    let indent_of = |line: &str| line.len() - line.trim_start().len();

    fn flush(steps: &mut Vec<Step>, block: &mut Option<(usize, String)>, advisory: bool) {
        if let Some((_, run)) = block.take() {
            steps.push(Step {
                advisory,
                run: run.trim().to_string(),
            });
        }
    }

    for line in workflow.lines() {
        let trimmed = line.trim();
        let indent = indent_of(line);

        // Continue accumulating a `run: |` block until the indentation drops back.
        if let Some((block_indent, run)) = block.as_mut() {
            if trimmed.is_empty() || indent >= *block_indent {
                run.push_str(line.trim_end());
                run.push('\n');
                continue;
            }
            flush(&mut steps, &mut block, step_advisory);
        }

        if indent == 2 && trimmed.ends_with(':') && !trimmed.starts_with('-') {
            job_advisory = false;
            step_advisory = false;
        } else if indent == 4 && trimmed == "continue-on-error: true" {
            job_advisory = true;
        } else if trimmed.starts_with("- ") {
            // A new list item ends the previous step.
            step_advisory = job_advisory;
        }

        if trimmed == "continue-on-error: true" && indent >= 8 {
            step_advisory = true;
        }

        if let Some(rest) = trimmed.strip_prefix("run:").or_else(|| {
            trimmed
                .strip_prefix("- run:")
                .map(|r| r.trim_start_matches(' '))
        }) {
            let rest = rest.trim();
            if rest == "|" || rest == ">" {
                block = Some((indent + 2, String::new()));
            } else {
                steps.push(Step {
                    advisory: step_advisory,
                    run: rest.to_string(),
                });
            }
        }
    }
    flush(&mut steps, &mut block, step_advisory);
    steps
}

fn normalize(run: &str) -> String {
    run.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The release gate must not be weaker than the PR gate.
///
/// It drifted that way once and nothing noticed: release.yml went untouched from before
/// 0.2.0 while 0.3 added release-profile clippy, release-profile tests, benchmark smoke and
/// an MSRV job to ci.yml. A release-only regression -- a binding that is dead only when
/// `debug_assertions` is off -- passed every step release.yml ran, and was caught only by a
/// step it did not run. Since a tag can point at any commit, "CI was green on this commit"
/// is a convention, not an enforced property, and the release job has to stand on its own.
///
/// Advisory (`continue-on-error`) steps in ci.yml are excluded: they are non-blocking there
/// because they depend on the network or on external tooling, and copying them into a
/// release gate would only add noise.
#[test]
fn the_release_gate_runs_every_blocking_ci_gate() {
    let ci = repo_file_or_skip!(".github/workflows/ci.yml");
    let release = repo_file_or_skip!(".github/workflows/release.yml");

    let ci = std::fs::read_to_string(ci).expect("ci.yml should be readable");
    let release = std::fs::read_to_string(release).expect("release.yml should be readable");

    let released: Vec<String> = steps_of(&release)
        .iter()
        .map(|s| normalize(&s.run))
        .collect();

    // `cargo clippy --workspace --all-targets` and the MSRV job's `cargo build --workspace
    // --all-targets --locked` between them compile every target in the release job already,
    // so a bare debug `cargo build --all-targets` would only pay for the artifacts twice.
    // This is the one ci.yml command with no literal counterpart, and it is subsumed rather
    // than skipped. Anything else added to ci.yml must be added to release.yml too.
    let subsumed = ["cargo build --workspace --all-targets"];

    let mut missing = Vec::new();
    for step in steps_of(&ci) {
        if step.advisory {
            continue;
        }
        let command = normalize(&step.run);
        if subsumed.contains(&command.as_str()) {
            continue;
        }
        if !released.contains(&command) {
            missing.push(command);
        }
    }

    assert!(
        missing.is_empty(),
        "ci.yml gates with no counterpart in release.yml -- the tag could publish what the \
         branch would have failed:\n  {}\n\nrelease.yml runs:\n  {}",
        missing.join("\n  "),
        released.join("\n  ")
    );
}

/// Guards the reader above: if it ever stops finding steps, the parity test passes vacuously.
#[test]
fn the_workflow_reader_actually_finds_the_gates_it_compares() {
    let ci = repo_file_or_skip!(".github/workflows/ci.yml");
    let ci = std::fs::read_to_string(ci).expect("ci.yml should be readable");

    let steps = steps_of(&ci);
    let blocking: Vec<String> = steps
        .iter()
        .filter(|s| !s.advisory)
        .map(|s| normalize(&s.run))
        .collect();

    assert!(
        blocking.len() >= 10,
        "expected the reader to find ci.yml's blocking gates, found {}: {blocking:?}",
        blocking.len()
    );
    for expected in [
        "cargo fmt --all --check",
        "cargo clippy --workspace --all-targets -- -D warnings",
        "cargo clippy --release --workspace --all-targets -- -D warnings",
        "cargo doc --workspace --no-deps",
        "cargo test --workspace",
        "cargo test --release --workspace",
        "mise run examples",
        "mise run bench-smoke",
    ] {
        assert!(
            blocking.iter().any(|c| c == expected),
            "the reader failed to find ci.yml's `{expected}` step; found: {blocking:?}"
        );
    }

    // And it must classify the advisory steps as advisory, or the parity test would demand
    // that the release job run network-dependent checks.
    let advisory: Vec<String> = steps
        .iter()
        .filter(|s| s.advisory)
        .map(|s| normalize(&s.run))
        .collect();
    assert!(
        advisory.iter().any(|c| c == "mise run runite-current"),
        "expected the runite drift check to be read as advisory; advisory set: {advisory:?}"
    );
    assert!(
        advisory.iter().any(|c| c == "mise run min-versions"),
        "expected the dependency-floor check to be read as advisory; advisory set: {advisory:?}"
    );
}
