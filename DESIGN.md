# celld-ml design

A slop-fork of [denoland/celld](https://github.com/denoland/celld)'s
architecture in OCaml, running as MirageOS unikernels on Solo5 `hvt`.

What carries over from celld:

- **Sans-IO core.** `Celld_logic.on_event : State.t -> now_ms:int -> Event.t -> Effect.t list`
  is the only thing that advances behavioral state. Each unikernel is an effect
  executor: one serial actor feeds events in and runs the effects that come out.
  Every async effect carries an `op` id, and a completion whose op no longer
  matches the cell's phase is ignored. That makes stale completions harmless.
- **Cells.** A cell is a named object (`Class:instance`) with its own state.
  Exactly one node owns a cell at a time, through a conditional write on
  `cells/<cell>/own.json`. Every activation advances the epoch.
- **Bucket as the source of truth.** No consensus and no membership protocol.
  The only coordination primitive is the bucket's conditional write
  (If-None-Match: * / If-Match: etag).
- **Node lease and self-fence.** Each node CAS-renews `nodes/<node>.json`
  every ttl/3. If it can't renew before expiry, it halts.
- **LTX capture.** Each committing request produces an L0 LTX file, which is
  uploaded to `cells/<cell>/ltx/e<epoch>/0000/<min:016x>-<max:016x>.ltx`.
  Each epoch opens with a snapshot (MinTXID = 1). The epoch prefix is the fence.
- **Output gate.** A response that reveals a write is held until that write is
  durable. Read-only responses trail the cell's newest open barrier.
- **Fleet vs bucket proofs.** A fleet proof means every recruited follower
  acked the L0 file over signed peer HTTP (write-all). A bucket proof means
  the upload landed and a re-read of own.json still names this node at this
  epoch.
- **Takeover recovery gate.** Before taking over a cell whose owner's lease
  expired while its log was `open`, the new owner seals that owner's
  followers, uploads their tails into the bucket, and CASes the dead lease's
  log to `sealed`.
- **Signed peer HTTP.** HMAC-SHA256 with a fleet secret created once in
  `fleet/peer-auth.json`.

What does not carry over: V8. JS cells run on QuickJS instead, because a
Solo5 unikernel has no threads and no executable memory for a JIT, and
QuickJS is an interpreter-only, single-threaded engine. Each JS cell gets a
context with Workers-style `fetch`/`Response` and a `ctx.storage` backed by
the cell's SQLite database. SQLite runs inside the unikernel, with an
in-memory VFS whose dirty pages become each commit's LTX file. `Counter:` and `Kv:` cells run native OCaml handlers
over a KV map paged into 4 KiB pages. Starting and Publishing are folded into
Restoring. There are no queues or cron.

## Layout

| path | what | depends on |
|---|---|---|
| `lib/ltx` | `celld_ltx`: LTX encode/decode, CRC-64/GO-ISO, LZ4 block | stdlib only |
| `lib/http` | `celld_http`: pure HTTP/1.1 parse/serialize | stdlib only |
| `lib/http/mirage` | `celld_http.mirage`: server + client over `Tcpip.Stack.V4V6` | lwt, cstruct, tcpip, mirage-flow |
| `lib/bucketc` | `celld_bucketc`: bucket client functor | celld_http.mirage |
| `lib/logic` | `celld_logic`: State/Event/Effect, `on_event` | stdlib only |
| `lib/cell` | `celld_cell`: paged KV store, LTX capture/restore, handlers | celld_ltx |
| `test` | native tests and a deterministic simulator | the pure libs |
| `unikernels/bucket` | S3-ish object store with CAS, block-backed log | http.mirage, mirage-block |
| `lib/blocklog` | `celld_blocklog`: record and superblock framing for the on-disk log | stdlib only |
| `lib/blocklog/mirage` | `celld_blocklog_mirage`: the log over `Mirage_block.S` | lwt, cstruct, mirage-block |
| `unikernels/node` | the celld node: actor + effect executor + follower log on a `log` block device | everything |

Each unikernel dir symlinks `lib -> ../../lib`, and `scripts/build.sh` adds
the local libraries to the generated `dune.build`.

## Bucket wire protocol (port 9000)

- `GET /o/<key>`: 200 with the body and an `ETag: "<etag>"` header, or 404.
  `HEAD` behaves the same.
- `PUT /o/<key>` with an optional `If-None-Match: *` or `If-Match: "<etag>"`:
  200 with an `ETag` on success, 412 when the precondition fails.
- `DELETE /o/<key>` with an optional `If-Match`: 204, 404, or 412.
- `GET /list?prefix=<p>`: 200 with matching keys, sorted and
  newline-separated.
- ETags are opaque strings, unique per write. Keys are raw bytes after
  `/o/`, percent-decoded.

## Peer protocol (internal port 8081)

Every request carries these headers:

- `x-cells-peer-source`
- `x-cells-peer-timestamp` (ms)
- `x-cells-peer-nonce`
- `x-cells-peer-body-sha256`
- `x-cells-peer-signature`, which is
  `hex(hmac_sha256(secret, "cells-peer-request-v1\n5\n{method}\n{path}\n{body_sha256}\n{source}\n{timestamp}\n{nonce}"))`

Requests are rejected outside ±30 s, and nonces are cached for 60 s.

- `POST /peer/forward`: runs a cell request on its owner. The `x-cells-scope`
  header names the cell. `x-cells-method` and `x-cells-path` carry the inner
  request, and the body is the inner body. If this node is not the owner it
  answers 421.
- `POST /peer/log/append`: the body is a binary `CLA1` batch, the same shape
  as celld's. Followers store entries in memory per leader session. The reply
  is `{"ok":true,"end":<seq>}`, or 409 if that session is sealed.
- `POST /peer/log/seal` with `{"session":"..."}`: marks the session sealed and
  returns every entry it holds as a `CLA1` batch.

## Bucket keys

| key | content |
|---|---|
| `cells/<cell>/own.json` | `{"node":"<id or empty>","epoch":N}` |
| `cells/<cell>/ltx/e<epoch:016x>/0000/<min:016x>-<max:016x>.ltx` | LTX |
| `nodes/<node>.json` | `{"node","session","addr","expires_ms","protocol":5,"log":"none|open|recovering|sealed","followers":[["node","addr"],...]}` |
| `fleet/peer-auth.json` | `{"secret":"<64 hex>"}` |
