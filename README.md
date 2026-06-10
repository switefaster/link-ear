# link-ear

`link-ear` is a Rust/libp2p peer-to-peer chat and shared listening app. Peers can chat, co-manage a music queue, and keep playback state roughly synchronized through the terminal UI or the Tauri desktop shell.

## Run

Start two clients on the same LAN:

```powershell
cargo run -- --name alice
cargo run -- --name bob
```

Peers on the same network are discovered with mDNS. For remote peers, pass a full multiaddr:

```powershell
cargo run -- --peer /ip4/203.0.113.10/tcp/4001/p2p/12D3KooW...
```
IPv6 multiaddrs are also supported, for example `/ip6/2001:db8::1/tcp/4001/p2p/...`.

Default bind includes both IPv6 and IPv4, and outbound dials try IPv6 addresses first when multiple are present.

To use a circuit relay server, pass its multiaddr. The app dials the relay and requests a relayed listener through `/p2p-circuit`:

```powershell
cargo run -- --relay /ip4/203.0.113.20/tcp/4001/p2p/12D3KooW...
```

The same relay address is also used as a rendezvous server. Each client registers under the current `--topic`, periodically discovers other peers in that topic, dials their relayed addresses, and then tries to upgrade relay-only peers to direct P2P connections when possible.

Run a minimal public relay plus rendezvous node on a reachable host:

```powershell
cargo run --bin link-ear-relay -- --port 4001 --secret-key-seed 0
```

The relay also serves a small topology dashboard on `127.0.0.1:8080` by default:

```text
http://127.0.0.1:8080/
```

Use `--web-addr` to bind it somewhere else, for example on all interfaces:

```powershell
cargo run --bin link-ear-relay -- --port 4001 --web-addr 0.0.0.0:8080
```

Pass `--no-web` to disable the page. The dashboard shows the relay-observed control-plane topology: active relay connections, rendezvous registrations by namespace, advertised peer addresses, and recent relay/rendezvous events. Direct peer-to-peer edges are not visible to the relay unless clients explicitly report them later.

Open TCP and UDP on the chosen port. The relay prints its peer id at startup; clients use a full relay multiaddr:

```powershell
cargo run -- --name alice --relay /ip4/203.0.113.20/tcp/4001/p2p/12D3KooW...
cargo run -- --name bob --relay /ip4/203.0.113.20/tcp/4001/p2p/12D3KooW...
```

If the relay host needs to advertise a specific public address, pass it explicitly:

```powershell
cargo run --bin link-ear-relay -- --external-addr /ip4/203.0.113.20/tcp/4001
```

Useful flags:

```text
--name <NAME>             Display name in chat
--topic <TOPIC>           GossipSub topic, default link-ear.chat.v1
--listen <MULTIADDR>      Add a listen address, can be repeated
--peer <MULTIADDR>        Dial a peer address, can be repeated
--relay <MULTIADDR>       Dial and reserve through a relay, can be repeated
--no-mdns                 Disable LAN discovery
```

Inside the TUI, type a message and press Enter. Press Esc or Ctrl+C to quit.

## Desktop frontend

The Tauri desktop UI is a React/Vite app under `desktop/`. Install and run the frontend preview from the repository root:

```powershell
npm.cmd install
npm.cmd run dev
```

Build the assets Tauri serves from `desktop/dist`:

```powershell
npm.cmd run build
```

Music commands:

```text
/bv <BV_ID> [PART]            Append Bilibili audio to the queue
/insert <INDEX> <BV_ID> [PART] Compatibility alias; backend still appends
/queue or /q                  Show the current track and the next queue items
/skip                         Skip the current track
/remove <INDEX>               Remove a queued track
/move <FROM> <TO>             Move a queued track, guarded by a vote
/pause                        Pause playback
/resume                       Resume playback
/seek <SECONDS>               Seek playback
/vote yes|no, /yes, /no       Respond to the active vote
```

`PART` is 1-based and defaults to `1`.

The desktop queue form only appends. Reordering is handled by move votes.

## Names

Clients announce their `peer_id`, display name, and join timestamp to the chat topic. Display names are only aliases: multiple peers may use the same visible name, and the peer id remains the unique identity. UIs may show a short peer id when a name needs disambiguation.

## History sync

Clients publish a lightweight history summary when a peer connects, when a peer subscribes to the chat topic, and then periodically afterwards. Connection/subscription events also schedule a short burst of follow-up summaries so peers do not have to wait for the periodic sync window. If a client sees another peer with more messages, it requests that peer's history. The peer replies with its retained chat records, and every client merges the response by message ID, keeping the newest `300` messages locally.

Message timestamps use microsecond precision for ordering. Duplicate history records are expected during sync and are silently ignored.

## Relay fallback

Relay-backed routes are kept during a short direct-connection handoff window. If
the direct route drops during that window, the relay route stays available for
chat, sync, playback, and vote messages. Once direct is settled and chat
subscription is ready, relay links may close for that peer.

## Music sync

Music sync uses a shared queue and playback state instead of P2P audio streaming. The peer that starts the next queue item resolves the Bilibili BV through the web API, announces a prepare phase, and every expected peer downloads the audio URL locally before playback starts. The leader broadcasts playback state every second; peers seek when local drift is larger than about `700ms`.

Queue enqueue is immediate and always appends. Moving tracks always opens a vote. Removing your own queued item is direct; removing another peer's queued item opens a vote. The current track requester can seek directly, while other seek requests open a vote. Pause, resume, and skip always open a vote. Majority thresholds count real room peers and exclude relay/rendezvous infrastructure peers.

## Manual smoke test

Use `docs/test-report-audit.md` to see how the real-world test report maps to current implementation and automated coverage. Use `docs/manual-smoke-test.md` for repeatable multi-peer testing. The smoke test covers a three-person room with one relay-only peer, one direct-capable peer, and one slower peer, including chat/history sync, queue votes, playback readiness, direct promotion failure, relay fallback, duplicate display names, IME input, peer overview, and status log checks.
