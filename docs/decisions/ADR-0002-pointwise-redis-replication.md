## Status

Proposed refinement of the accepted Redis-primary model in ADR-0001. The
product owner selected Redis-first operation with SQLite fallback; this record
defines the write and mirror mechanics for that choice.

## Decision

Use Redis as the primary backend while it is reachable. For every successful
state-changing tool call:

- execute the complete compatibility operation in the private Rust `Store`;
- compare only the affected native scopes with their last published projection;
- send Redis only changed entity records and removed entity keys, together with
  the revision, schema marker, manifest, operation ledger, and idempotency
  marker in one `WATCH`/`MULTI`/`EXEC` transaction;
- do not replace `state:snapshot` in the ordinary write path.

The RESP client sends the transaction command batch in one write/flush and
reads replies in order. This removes a network wait for every queued command
without changing Redis transaction ordering or conflict checks.

The complete SQLite snapshot remains a bounded control-plane transport for
initial attach, schema rebuild, and a Redis-loss recovery boundary. It is not a
per-operation replication format.

Because the native projection does not yet rebuild every compatibility index
and virtual-database detail by itself, the watcher also makes an amortized
restart checkpoint after 256 committed revisions. This is a bounded control-
plane operation; it does not add a snapshot write to each user operation.

## Fallback and recovery

When Redis is unavailable, the coordinator executes and durably writes the
operation to SQLite before acknowledging it. The same operation is retained in
the local outbox. On reconnect, the local fallback image is used as the
recovery base, Redis-native records seed the last-known remote projection, and
the outbox operations are published as pointwise deltas. Redis remains the
priority backend after the recovered transaction commits.

After a Redis commit in the healthy path, the outbox entry is marked
`redis_committed` and is not removed until the background watcher replays that
operation into the SQLite standby. The replay changes only the records touched
by the operation; it does not perform a full Redis/SQLite scan. A bounded
replay error leaves the entry durable for the next watcher attempt.

## Consequences

This makes normal network payload and Redis write work proportional to the
changed records rather than the complete workspace projection. The watcher
performs a small revision read while idle and only runs the bounded outbox
mirror or a full snapshot read when recovery/checkpoint work requires it. The
256-revision checkpoint amortizes compatibility-state backup cost while
keeping a restart's possible replay window bounded.

The compatibility `Store` still supplies the complete 80-tool semantics, so
native Redis records are a durable pointwise projection and not yet an
independent native execution engine. A complete snapshot is therefore still
required at attach/rebuild and at the explicit recovery boundary. Performance
efficacy remains `not_claimed` until the paired real-service baseline records
p50/p95 latency, CPU, Redis wire bytes, SQLite mirror cost, and recovery lag.

## Verification

- Redis unit coverage proves a delta can delete and upsert addressable records
  while leaving the existing full snapshot byte-for-byte unchanged.
- Coordinator coverage proves fallback operations are published after Redis
  recovery and retained operation entries are mirrored into SQLite before
  removal.
- The checkpoint policy keeps the snapshot out of the normal per-operation
  transaction and exercises the full compatibility backup only at the bounded
  256-revision interval.
- The 80-tool route, AppSec, artifact, QA, and PM gates remain unchanged.
