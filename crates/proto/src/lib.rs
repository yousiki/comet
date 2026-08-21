//! zeron-proto — wire types shared by engine, UI, and RPC.
//!
//! Ported from zeron's `packages/control/src/wire.ts` + `packages/harness/src/types.ts`.
//! Token-usage *display* types are excluded by design; the `Usage` agent event is kept as a
//! harness-level passthrough (rate-limit meters), never persisted into docs.

pub mod agent;
pub mod entities;
pub mod motion;
pub mod profile;
pub mod view;

pub use agent::*;
pub use entities::*;
pub use profile::*;
