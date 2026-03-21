# Bug: Stale namespace persists in gossip mesh after publisher disconnect

## Summary

In a full-mesh cluster (dev branch), a namespace announced via gossip survives publisher disconnects and relay restarts. Subscribers receive SUBSCRIBE_OK followed by PUBLISH_DONE status=500 because the relay routes to a gossip peer that has no actual data.

## Environment

- Branch: `dev`
- Cluster: 6 relays in full mesh (`[cluster] connect` lists all peers)
- Publisher: moq-lite-02 client connecting to one relay (the "origin")
- Subscriber: browser via moq-transport-14 / WebTransport

## Steps to Reproduce

1. Start all 6 relays with full-mesh config
2. Start publisher → connects to relay A, announces namespace `test`
3. Verify: subscriber connects to any relay, plays successfully
4. Stop the publisher
5. Stop relay A (the origin)
6. Restart relay A
7. Subscriber connects to relay A → gets SUBSCRIBE_OK then PUBLISH_DONE status=500

The namespace `test` is stuck. Restarting relay A alone does not fix it. Only restarting ALL relays within ~10 seconds clears the stale entry.

## Root Cause Analysis

### How gossip propagates the namespace

When relay A's publisher announces `test`:
1. Relay A stores it locally via `publish_broadcast()` (`rs/moq-lite/src/model/origin.rs:430`)
2. Relay A's gossip connections publish it to peers B-F via `run_remote_once()` (`rs/moq-relay/src/cluster.rs:108`): `.with_publish(self.origin.consume())`
3. Peers B-F each store `test` in their own origin and re-publish to their peers
4. All 6 relays now have `test` in their origin

### How cleanup should work

When the publisher disconnects:
1. The `BroadcastConsumer` on relay A closes
2. The spawned task in `publish_broadcast()` (`origin.rs:444`) awaits `broadcast.closed()`, then calls `remove()` → `unannounce()`
3. The unannounce should propagate to peers

### Why it fails

When relay A restarts:
1. Relay A's local broadcast is gone (process died)
2. Peers B-F still have `test` in their origins (received via gossip)
3. Relay A reconnects to peers via `run_remote_once()` (`cluster.rs:108`)
4. Each peer publishes `test` back to relay A via `.with_publish(self.origin.consume())`
5. Relay A now has `test` again — but it came from gossip, not a real publisher
6. Subscriber connects, relay A finds `test` via gossip, tries to fetch from a peer, peer has no data → PUBLISH_DONE 500

The `broadcast.closed()` cleanup on peers should fire when the gossip connection to relay A drops. But the reconnection happens immediately (backoff starts at 1s in `cluster.rs:94`), and the new connection re-publishes before the old consumer's close task runs.

The `REANNOUNCE_HOLD_DOWN` of 250ms (`origin.rs:15`) is designed for cascading closures, but the reconnect+re-publish race is faster.

### Key code paths

| File | Line | Description |
|------|------|-------------|
| `rs/moq-lite/src/model/origin.rs` | 430 | `publish_broadcast()` — stores broadcast, spawns close watcher |
| `rs/moq-lite/src/model/origin.rs` | 444 | `broadcast.closed().await` — cleanup on consumer close |
| `rs/moq-lite/src/model/origin.rs` | 445 | `root.lock().remove()` → triggers unannounce |
| `rs/moq-lite/src/model/origin.rs` | 15 | `REANNOUNCE_HOLD_DOWN = 250ms` |
| `rs/moq-relay/src/cluster.rs` | 108-122 | `run_remote_once()` — connects to peer, publishes+consumes full origin |
| `rs/moq-relay/src/cluster.rs` | 94 | Backoff starts at 1s, doubles on failure |

### The race condition

```
Time 0:     Relay A dies. Connection to peer B drops.
Time 0+ε:   Peer B's BroadcastConsumer for "test" should start closing
Time 1s:    Peer B reconnects to relay A (backoff=1s)
Time 1s+ε:  Peer B publishes "test" to relay A via .with_publish(self.origin.consume())
Time ???:   Peer B's old consumer close task finally runs → unannounce
            But "test" was already re-published via the new connection
```

The gossip consumer on peer B is a **different** `BroadcastConsumer` than the original publisher's. When peer B's connection to relay A drops, peer B's consumer for the relay A session closes. But `origin.rs` has backup logic — the same namespace from other gossip paths (via peers C-F) keeps the broadcast alive on peer B.

## Possible Fixes

1. **Reconnection delay**: Wait for `REANNOUNCE_HOLD_DOWN` (or longer) after a peer disconnects before re-publishing its namespaces on a new connection. This gives the close task time to propagate.

2. **Hop-aware cleanup**: When a gossip connection drops, immediately unannounce all namespaces that were learned exclusively through that connection (no other path with equal or fewer hops).

3. **Publisher heartbeat**: Require the original publisher to periodically refresh its announce. If no refresh within N seconds, the namespace is unannounced globally.

4. **Admin purge**: Add a relay CLI command or HTTP endpoint to purge a namespace from the mesh.

## Logs

### Subscriber sees repeated PUBLISH_NAMESPACE from every gossip peer, then 500:

```
[MoQT] SUBSCRIBE id=0 track="catalog"
[MoQT] PUBLISH_NAMESPACE id=1 ns="test" — sending OK
[MoQT] SUBSCRIBE_OK id=0 alias=0 track="catalog"
[MoQT] PUBLISH_DONE id=0 status=500
[MoQT] PUBLISH_NAMESPACE id=3 ns="test" — sending OK
[MoQT] PUBLISH_NAMESPACE id=5 ns="test" — sending OK
[MoQT] PUBLISH_NAMESPACE id=7 ns="test" — sending OK
[MoQT] PUBLISH_NAMESPACE id=9 ns="test" — sending OK
...
```

### Edge relay during origin restart — disconnect, reconnect, timeout cycle:

```
WARN conn{id=56}: transport error err=session error: connection error: closed
INFO remote{remote=origin}: connecting to remote url=https://origin/
WARN remote{remote=origin}: QUIC connection failed err=timed out
WARN remote{remote=peer}: transport error err=session error: connection error: closed
INFO remote{remote=peer}: connecting to remote url=https://peer/
WARN remote{remote=peer}: QUIC connection failed err=timed out
```
