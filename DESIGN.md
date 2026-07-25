# Design notes

An in-memory, single-node `Idempotency-Key` middleware for HTTP POST handlers. Wrap a handler with
`idem.Middleware(h)` and a retried request with the same key runs the handler once; concurrent or
later retries get the same recorded response instead of executing it again.

## How it works

There is one shared `map[string]*entry` guarded by a single mutex. The first request for a key
becomes the **leader**: it inserts an in-progress entry, runs the handler, records the response, and
closes the entry's `done` channel. Any request that finds an existing entry is a **follower**: it
blocks on `done` and then replays the recorded response. A key's fingerprint is
`sha256(method + path + body)`, so reusing a key for a different request is detected.

The only clever part is how followers read the response without locking. The leader writes the
status/headers/body *before* it calls `close(done)`, and followers only read those fields *after*
`<-done`. Go's memory model guarantees a channel close happens-before any receive that observes it,
so the read is safe with no second lock. One writer, one clean handoff.

## Decisions (and the main alternative rejected for each)

**1. A retry that arrives while the first is still running blocks and then replays.**
It waits on the leader's `done` channel, bounded by the caller's request context and a `WaitTimeout`
(then 409). If the leader ends up *failing*, waiters loop and re-execute rather than replaying the
failure.
*Rejected:* return 409 to every concurrent retry immediately. That pushes retry/backoff back onto the
client even when the handler finishes in milliseconds — the exact retry noise this middleware exists
to absorb.

**2. Same key with a different method/path/body is rejected with 422.**
The fingerprint mismatch is caught in `begin` before any work happens.
*Rejected:* run it anyway, or overwrite the stored fingerprint. Either would return a wrong cached
answer for a genuinely different operation and silently hide the client's key-reuse bug. (409 is kept
for the separate "a request with this key is still in progress" case.)

**3. 2xx/3xx/4xx are cached; 5xx and panics are not (the key is freed).**
A 4xx is a deliberate, repeatable answer, so it's cached. A 5xx usually means a transient/infra
failure where a genuine retry is likely to succeed, so we free the key and let the next attempt run.
Panics are recovered only to free the key and are then re-thrown, so the host's own recovery
middleware still sees them.
*Rejected:* cache every outcome including 5xx. Freezing a transient blip for the whole TTL would turn
it into a hard per-key outage. The guarantee is precisely "no duplicate *successful* completions,"
not "the handler body runs at most once ever."

**4. Memory is bounded by an LRU count cap plus a fixed TTL checked lazily.**
`MaxKeys` is a hard ceiling enforced on insert; `TTL` expires stale entries, checked on lookup. There
is no background goroutine. In-progress entries are deliberately kept *out* of the LRU list, so
eviction can never remove a request that is still running.
*Rejected:* a ticker/sweeper goroutine. It adds a lifecycle to manage and a leak risk in tests for no
benefit over the hard cap plus lazy expiry.

**5. A request with no key is rejected with 400.**
Mounting this middleware on a route declares that the route requires an idempotency key.
*Rejected:* pass such requests straight through. Pass-through is the Stripe/IETF convention (the key
is optional), but then a client that simply forgot the header silently loses all retry safety on a
mutating endpoint. A caller that genuinely wants un-keyed access just doesn't wrap that route.

**6. One `sync.Mutex` for the map/list, and a channel close as the "ready" signal.**
The locked sections are all O(1) map/list operations, dwarfed by handler latency.
*Rejected:* an `RWMutex` or a sharded map. Premature optimization here, and sharding also breaks the
single global LRU ordering while adding code to defend.

## Known, deliberate limitations

- The fingerprint is over raw body bytes, so two semantically-equal but differently-serialized bodies
  (e.g. reordered JSON keys) count as different. The query string is not part of it.
- The response body is buffered in memory to enable replay; there is no attempt to replay streaming
  or chunked responses.
- A handler that ignores its context and hangs forever holds its key indefinitely. Waiters are
  bounded by `WaitTimeout` (409), but we never forcibly cancel the leader — `net/http` can't do that
  safely.

## What would change for persistence / multi-node (out of scope)

- The map becomes a shared store (Redis/Postgres). Becoming the leader turns into an atomic
  `SET key NX EX ttl` / `INSERT ... ON CONFLICT DO NOTHING` instead of an in-process mutex.
- The in-memory `done` channel can't cross machines, so followers would poll the store, use pub/sub,
  or simply get a 409 and let the client retry.
- A node can crash mid-handler and never run its cleanup, so the in-progress claim needs its own
  short lease plus a heartbeat to renew it. If the lease lapses while the handler is really still
  alive, you get a second execution — at-least-once unless you add fencing tokens.
- Let the shared store be the single clock; don't compare wall clocks across nodes for TTL.
- Most importantly, this is an HTTP-layer convenience, not a correctness guarantee on its own. A node
  can commit the real write and crash before recording the response, so the actual safety net is a
  `UNIQUE(idempotency_key)` constraint at the data store.

## Request flow

Every branch below is a real branch in `Middleware` / `execute`.

```
incoming POST
│
├─ no Idempotency-Key ....................... 400 Bad Request
├─ key longer than 255 ...................... 400 Bad Request
├─ body over MaxBodyBytes ................... 413 Request Entity Too Large
│
└─ fingerprint = sha256(method + path + body)
   │
   └─ begin(key, fingerprint)
      │
      ├─ key exists, fingerprint differs .... 422 Unprocessable  (same key, different request)
      │
      ├─ key exists  → FOLLOWER: wait on done
      │     ├─ leader succeeded ............. replay stored response (+ Idempotent-Replayed: true)
      │     ├─ leader failed ................ loop back to begin, become the new leader
      │     ├─ WaitTimeout elapsed .......... 409 Conflict  (still processing)
      │     └─ client disconnects ........... return, write nothing
      │
      └─ new key     → LEADER: run the handler
            ├─ status 2xx / 3xx / 4xx ....... complete: cache + return
            ├─ status 5xx ................... fail: free the key + return  (retryable)
            └─ panic ........................ fail: free the key, then re-panic
```

## Entry lifecycle

An entry is created in-progress and leaves in exactly one of two ways.

```
in-progress            done channel OPEN, not in the LRU list (so it can't be evicted)
   │
   ├─ handler succeeds → cached    done CLOSED, ready=true, joins LRU, has expiresAt
   │                                  └─ later: TTL expiry or LRU eviction → removed
   │
   └─ handler fails    → removed   done CLOSED, ready=false, deleted immediately
      (5xx / panic)                   └─ waiters wake, see ready=false, re-execute
```
