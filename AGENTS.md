# link-ear agent notes

## Project goal

`link-ear` is a P2P chat and shared listening app. Peers chat, co-manage a music
queue, and try to keep playback state close enough that everyone hears the same
track at roughly the same time.

The networking layer uses libp2p. Peers can meet through a public
relay/rendezvous node, then promote relay-backed routes to direct peer-to-peer
connections when direct addresses become usable so relay traffic stays bounded.

## Main entrypoints

- `src/backend.rs`: main libp2p runtime, protocol message handling, history sync,
  queue sync, voting, playback state sync, relay-to-direct promotion.
- `src/core.rs`: shared protocol and UI-facing data types plus small utilities.
- `src/main.rs`: terminal UI bootstrap and command parsing.
- `src/bin/link-ear-relay.rs`: public relay/rendezvous node plus topology
  dashboard.
- `src-tauri/src/main.rs`: Tauri command bridge into the Rust backend.
- `desktop/src/main.jsx`: React/Vite desktop UI.

## Useful checks

Run the narrowest relevant check first, then broaden if the change crosses
boundaries:

- `cargo test --lib`
- `cargo check`
- `cargo check --bin link-ear-relay`
- `cargo check --manifest-path src-tauri\Cargo.toml`
- `npm.cmd run build`

For relay promotion policy or rendezvous behavior, include
`cargo check --bin link-ear-relay`. For desktop bridge work, include the Tauri
manifest check and the frontend build.

## Repository constraints

- Keep Cargo on the default upstream registry/source behavior. Do not add mirror
  overrides unless the user explicitly asks for that.
- Do not commit generated or dependency outputs. The expected ignored paths are
  `target/`, `src-tauri/target/`, `src-tauri/gen/`, `desktop/dist/`, and
  `node_modules/`.
- Prefer tests around pure protocol/state logic before larger refactors. Good
  first targets are history insertion, queue version ordering, vote thresholds,
  playback position math, address normalization, and Bilibili signing helpers.
- Avoid broad rewrites of `src/backend.rs` until there are enough tests to lock
  current behavior.
