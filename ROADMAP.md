- This module could easily be no*std compatible, but we need thread locals. Currently, those are only available in `std`
  on stable Rust. Nightly has `#[feature(thread_local)]` which we \_WOULD* use if it were stable, and that is the only
  blocker for supporting no_std in adaptite.
