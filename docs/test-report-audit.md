# Test Report Audit

This file maps the real-world test report items to current implementation
evidence. It is not a substitute for `docs/manual-smoke-test.md`; it explains
what is already covered by code/tests and what still needs a real multi-peer
run.

## Implemented With Automated Evidence

| Report item | Current evidence |
| --- | --- |
| iBus/fcitx IME submits partial Chinese text | Desktop composer tracks composition and blocks Enter while composing in `desktop/src/main.jsx`. |
| Same-session playback refresh closes vote modal | `MusicState` keeps playback votes during same-session updates; covered by `same_session_playback_update_keeps_playback_vote`. |
| Vote counts include relay/rendezvous peers | Room peer set excludes rendezvous and includes local peer; covered by `room_peer_set_excludes_rendezvous_nodes_and_includes_local`. |
| Empty playback can start votes | Backend rejects pause/resume/skip/seek when no active playback; stale playback vote tests cover inactive track cases. |
| Queue operation authority is unclear | Enqueue appends directly; move always votes; own remove is direct; other remove votes; seek direct only for requester; pause/resume/skip vote. Rules are documented in `README.md` and `AGENTS.md`. |
| Queue votes disappear while peer is catching up | Future queue-version votes stay active while queue sync is requested; covered by `future_queue_vote_proposal_is_accepted_while_waiting_for_sync` and `passed_future_queue_vote_waits_until_queue_catches_up`. |
| Vote result only applies for proposer | Vote execution uses `item_id` / `session_id` on every receiving peer; covered by `passed_vote_is_executable_by_all_room_peers`. |
| Playback ready includes relay/rendezvous | Expected playback peers come from real room peers only; covered by room peer tests and pending-ready tests. |
| Peer disconnect prevents ready completion | Pending expected/ready sets remove disconnected peers; covered by `pending_playback_starts_after_expected_peer_disconnects`. |
| Track finishes but followers stay paused at the end | Finished playback roles distinguish leader/follower; covered by `finished_playback_role_distinguishes_leader_and_follower`. |
| Direct promotion failure breaks relay delivery | Connection policy keeps relay route after direct failure; covered by `direct_promotion_failure_keeps_relay_route_available`. |
| Direct route drops immediately after relay close | Direct promotion now keeps relay through a handoff grace period and cancels relay close if direct drops; covered by `chat_subscription_schedules_relay_handoff_when_direct_route_exists` and `direct_handoff_keeps_relay_if_direct_drops_before_grace`. |
| Gossipsub readiness falls back to wait after connection churn | Connected-peer unsubscribe events no longer clear readiness until the peer fully disconnects; covered by `unsubscribe_while_connected_does_not_clear_chat_readiness`. |
| Direct fallback is too broad or spoofable | Fallback is reserved for `NoPeersSubscribedToTopic`, targeted sync requests send only to the requested peer, and direct inbound messages use the normal validated `WireMessage` handler. |
| Slow peers fail history sync and cannot retry | History summary considers count or newest timestamp, and failed direct sync clears matching cooldown; covered by `history_summary_newer_uses_count_or_newest_timestamp` and `direct_sync_failure_clears_matching_request_cooldown`. |
| Basic multi-backend chat/history convergence | `local_loopback_smoke_syncs_chat_history_between_two_backends` starts two real backend event loops on loopback, dials one into the other, sends chat, and verifies both history views converge. |
| Dragging queue order does not work | Desktop queue cards support pointer drag plus keyboard move controls that open move vote confirmation. |
| Insert-at-position should be removed | Desktop enqueue form has no position field; backend ignores `position` and always appends; README marks `/insert` as compatibility only. |
| Duplicate display names should be allowed | Name claims are aliases keyed by peer id; covered by `peer_names_allow_duplicate_display_aliases`. |
| Volume control | Desktop local volume command maps to `AudioPlayer::set_volume`; covered by `volume_percent_to_gain_maps_ui_range`. |
| Peer connection overview | `FrontendEvent::Peers` carries UI-only route/direct-promotion snapshots; covered by `peer_views_report_routes_and_direct_promotion_counters`. |
| Chat history cannot scroll | Message list has a scroll viewport with near-bottom auto-follow and latest jump behavior. |
| Play and pause should be one button | Desktop transport has one state-driven play/pause button that maps to existing pause/resume commands. |
| Logs are hard to read | Desktop status log is structured on the frontend with time, level, category, search, and filters. |

## Verification Commands

Last checked after this audit:

```powershell
cargo fmt
cargo test --lib
cargo check
cargo check --bin link-ear-relay
cargo check --manifest-path src-tauri\Cargo.toml
npm.cmd run build
git diff --check
```

Observed result: all commands passed. `cargo test --lib` currently runs 60
tests.

## Still Requires Real Multi-Peer Evidence

These cannot be fully proven inside a single local compile/test run:

- Three-person room with one relay-only peer, one direct-capable peer, and one
  slower peer.
- Actual relay/rendezvous reachability across mixed NATs.
- Real gossipsub delivery versus direct fallback under subscriber churn.
- Audio download and synchronized playback behavior across different machines.
- UI behavior under the users' actual IME/browser/WebView environment.

Use `docs/manual-smoke-test.md` to collect that evidence. Do not mark the
stabilization effort complete until a real run confirms chat, history sync,
queue/vote convergence, playback readiness, direct-promotion failure fallback,
peer overview, status log filtering, duplicate names, and IME input.
