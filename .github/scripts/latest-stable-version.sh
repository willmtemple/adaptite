#!/usr/bin/env bash
#
# Print the highest stable, unyanked version found in a crates.io sparse-index file read on
# stdin (e.g. `curl -sSf https://index.crates.io/ru/ni/runite`).
#
# The index is newline-delimited JSON in *publication* order, and it retains yanked entries.
# So `tail -1` -- the obvious thing, and what `mise run runite-current` used to do -- answers
# "what was uploaded most recently", not "what is the highest version". Three real shapes
# break it:
#
#   * a patch backported to an older minor (crates.io/ti/me/time really does carry 0.1.43
#     between 0.2.9 and 0.2.10),
#   * a prerelease (0.4.0-alpha.1 published after 0.3.0),
#   * a yank (the entry stays in the file with "yanked":true).
#
# Each of those makes a drift gate built on `tail -1` report a *lower* version as latest and
# fail against a dependency that is perfectly current. Filter, then sort by semver instead.
#
# Prints nothing and exits 1 if the input holds no stable, unyanked release.
#
# Tested by tests/release_fixes.rs.

set -euo pipefail

# `-t.` + three numeric keys is a portable semver sort: it does not need GNU `sort -V`, and
# the `grep` above has already guaranteed every line is exactly three numeric components.
latest="$(
  grep -v '"yanked":true' \
    | grep -o '"vers":"[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*"' \
    | cut -d'"' -f4 \
    | sort -t. -k1,1n -k2,2n -k3,3n \
    | tail -1 || true
)"

if [ -z "${latest}" ]; then
  echo "latest-stable-version: no stable, unyanked version in the index input" >&2
  exit 1
fi

printf '%s\n' "${latest}"
