# Roadmap

Feature planning lives in the [GitHub issue tracker](https://github.com/willmtemple/adaptite/issues).

## no_std support

Adaptite is written against `core` + `alloc`, but two things keep it on `std`:

- Thread-local storage: the reactor, owner stack, and untracked-depth state
  use `std::thread_local!`; the `#[thread_local]` attribute that could replace
  it is still nightly-only.
- The runite runtime itself would need to support `no_std` targets first.
