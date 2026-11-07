# celld-ml

A slop-fork of [denoland/celld](https://github.com/denoland/celld)'s
architecture in OCaml. Each node, and the bucket, runs as a MirageOS
unikernel on Solo5 `hvt` (KVM).

celld runs distributed Durable Objects with no consensus service. A *cell*
is a named object. A conditional write in an object-store bucket makes
exactly one node its owner, and every committed write is captured as an LTX
file. This fork keeps that design and drops the payload: there is no V8,
Workers runtime or SQLite. Cells are native OCaml handlers (`Counter`, `Kv`)
over a paged in-memory KV store. See [DESIGN.md](DESIGN.md) for the mapping.

## How it fits together

Every box is its own Solo5 unikernel. Clients can hit any node; the bucket
is the only shared state.

```mermaid
flowchart LR
  client([client])
  subgraph fleet[nodes]
    n1["n1<br/>:8080 public<br/>:8081 peer"]
    n2["n2"]
    n3["n3"]
  end
  bucket[("bucket :9000<br/>nodes/&lt;node&gt;.json<br/>cells/&lt;cell&gt;/own.json<br/>cells/&lt;cell&gt;/ltx/e&lt;epoch&gt;/...")]
  client -->|"/do/Counter:demo/..."| n2
  n2 -->|"signed /peer/forward"| n1
  n1 -->|"/peer/log/append"| n2
  n1 -->|"/peer/log/append"| n3
  n1 -->|"lease CAS, own.json CAS,<br/>LTX uploads"| bucket
  n2 -.-> bucket
  n3 -.-> bucket
```

Inside a node, one pure function decides and an actor does the I/O:

```mermaid
flowchart LR
  io["actor<br/>(HTTP, bucket, peers, timers)"] -->|event| core["Core.on_event<br/>sans-IO, deterministic"]
  core -->|"eff list"| io
  sim["test/sim"] -->|"same events"| core
```

### A write

The owner commits, ships the LTX entry to its followers, and only answers
once every follower has acked (fleet proof). The upload to the bucket
happens afterwards.

```mermaid
sequenceDiagram
  participant C as client
  participant N2 as n2 (not owner)
  participant N1 as n1 (owner)
  participant F as followers
  participant B as bucket
  C->>N2: POST /do/Counter:demo/increment
  N2->>B: GET own.json, nodes/n1.json
  N2->>N1: signed POST /peer/forward
  N1->>N1: run handler, commit, encode LTX
  Note over N1: output gate holds the response
  N1->>F: /peer/log/append (CLA1 batch)
  F-->>N1: ack
  N1-->>N2: 200 (write proven)
  N2-->>C: 200
  N1-)B: PUT cells/Counter:demo/ltx/e1/... (async)
```

### Takeover after an owner dies

```mermaid
sequenceDiagram
  participant N2 as n2
  participant B as bucket
  participant F as n1's followers
  N2->>B: GET own.json (owner n1, epoch 1)
  N2->>B: GET nodes/n1.json
  alt lease still valid
    Note over N2: n1 still owns the cell, forward or 502
  else lease expired, log open
    N2->>B: CAS nodes/n1.json log=recovering
    N2->>F: /peer/log/seal
    F-->>N2: unuploaded tail
    N2->>B: PUT tail as LTX files
    N2->>B: CAS nodes/n1.json log=sealed
    N2->>B: CAS own.json If-Match (owner n2, epoch 2)
    N2->>B: LIST + GET ltx files, restore
    Note over N2: resident at epoch 2
  end
```

### Cell lifecycle on a node

```mermaid
stateDiagram-v2
  [*] --> Inactive
  Inactive --> Reading_owner: request
  Reading_owner --> Acquiring: unowned, or ours from before
  Reading_owner --> Reading_lease: owned by another node
  Reading_lease --> Remote: lease live (forward)
  Reading_lease --> Recovering_log: lease expired, log open
  Reading_lease --> Acquiring: lease expired, log sealed
  Recovering_log --> Acquiring
  Acquiring --> Reading_owner: CAS rejected or ambiguous
  Acquiring --> Restoring: CAS applied (epoch + 1)
  Restoring --> Resident
  Resident --> Ensuring: idle, shed or draining
  Ensuring --> Cleaning: durable
  Cleaning --> Releasing: runtime stopped
  Releasing --> Inactive: own.json node = ""
  Releasing --> Remote: handed off (shed or drain)
  Remote --> Inactive: lease ends or invalidated
```

A node that cannot renew `nodes/<node>.json` before it expires halts with
exit code 3, so a partitioned owner stops serving before anyone else can
claim its cells.

## What carries over

- **The sans-IO core.** `Celld_logic.Core.on_event : t -> now_ms:int -> event -> eff list`
  is the only thing that changes behavioral state. The unikernel is a serial
  actor that executes the effects and turns their completions into events.
  The deterministic simulator in `test/sim` drives the same function.
- **Ownership by conditional write.** `cells/<cell>/own.json` is claimed with
  `If-None-Match: *` or `If-Match`, and every activation advances the epoch.
  An ambiguous CAS is re-read and never retried blind.
- **Node leases and self-fence.** `nodes/<node>.json` is CAS-renewed every
  ttl/3. A node that can't renew before expiry halts with exit code 3.
- **LTX capture.** The file format follows celld's `ltx-format.md`: LZ4
  pages, CRC-64/GO-ISO and rolling checksums. Files land under
  `cells/<cell>/ltx/e<epoch>/...`. Each epoch opens with a snapshot, and the
  epoch prefix is the fence.
- **Compaction and epoch GC.** Every `--compact-every` commits (default
  64) the owner uploads a level-1 snapshot and deletes the files it
  covers. When a cell is stopped, files from older epochs are deleted.
  Restore reads the newest snapshot of the newest epoch plus what follows
  it.
- **The output gate.** A response that reveals a write is held until the
  write is proven. Reads trail the newest open barrier on their cell.
- **Fleet vs bucket proofs.**
  - A fleet proof is an ack from every recruited follower over signed peer
    HTTP. A follower writes the batch to its own block device before it
    acks, so an acked entry survives the follower restarting.
  - A bucket proof is an upload plus an `own.json` re-read.
- **The takeover recovery gate.** Before taking over a cell from a dead
  owner whose log is open, the new node:
  1. CASes that node's log record to `recovering`;
  2. seals its followers;
  3. uploads their tails;
  4. CASes the record to `sealed`.
- **Rebalance and drain.** Each lease carries the node's resident cell
  count. A node more than one cell above the fair share marks its excess
  cells to move. Each one is handed off when it goes quiet: made durable,
  stopped, released, and then activated on the target (`/peer/activate`).
  Requests that arrive meanwhile wait and follow the cell.
  `POST /.well-known/celld/drain` stops a node from taking ownership and
  hands off every cell. Once its log is in the bucket, the node writes an
  expired lease with no open log and exits 0, so no one has to recover it.
- **Alarms.** `PUT /do/<cell>/__alarm?in_ms=N` (or `?at_ms=T`) sets the
  cell's one alarm, and `GET` or `DELETE` read or clear it. The alarm is a
  key in the cell's own state, so it is captured, replicated and moves with
  the cell. The owner fires it as an ordinary write that runs the class's
  alarm handler (Counter ticks, Kv counts firings). The bucket index
  `alarms/<cell>` lets any node wake a cell that was evicted or whose
  owner died. A fire that finds nothing due is a no-op.
- **Signed peer HTTP.** Requests are HMAC-SHA256-signed with a fleet secret
  that is created once with a conditional write.

The fork leaves out queues, cron, WebSockets and the tunnel. A forwarded request is one signed POST.

## Layout

| path | what |
|---|---|
| `lib/logic` | the core: types, phases, `on_event` |
| `lib/ltx` | the LTX format, CRC-64, and an LZ4 block codec written from the format spec |
| `lib/cell` | the paged KV store, capture, restore planning, and the cell classes |
| `lib/wire` | JSON records, the `CLA1` log batch, and peer signing helpers |
| `lib/http`, `lib/http/mirage`, `lib/bucketc` | HTTP/1.1, plus server and client over mirage-tcpip, plus the bucket client |
| `unikernels/bucket` | an S3-like object store with ETag CAS, persisted to a block device |
| `lib/blocklog`, `lib/blocklog/mirage` | a crash-safe record log on a block device: two halves and a superblock, compacted by rewriting the live records into the other half |
| `unikernels/node` | the node: actor, effect executor, node log, follower log, and peer endpoints |
| `test/*` | native tests and the simulator |

## Run it

Everything runs inside a Lima VM, because Solo5 needs Linux and KVM. On an
M3 or newer Mac, nested virtualization provides `/dev/kvm`.

```bash
limactl create --name=mirage --vm-type=vz --cpus=6 --memory=8 --set='.nestedVirtualization=true | .mounts=[{"location":"~/Github/Personal/celld-ml","writable":true}]' template:ubuntu
```

Inside the VM, install opam, an OCaml 5.3 switch named `mirage`, and the
`mirage`, `dune`, `solo5` and `ocaml-solo5` packages. Then:

```bash
scripts/build.sh bucket && scripts/build.sh node
scripts/fleet.sh up 3                  # bucket at 10.0.0.2, nodes n1..n3 at 10.0.0.11..13
curl -X POST http://10.0.0.11:8080/do/Counter:demo/increment
curl http://10.0.0.12:8080/do/Counter:demo          # forwarded to the owner
curl -X PUT --data-binary hi http://10.0.0.13:8080/do/Kv:cfg/greeting
curl http://10.0.0.11:8080/state
scripts/fleet.sh down
```

`build.sh` patches two vendored dependencies:

- **Lwt:** its optional `runtime_events` support is removed, because it
  needs `fstat`, which Solo5 doesn't provide.
- **mirage-crypto:** a failing timer-entropy self-test is non-fatal instead
  of aborting boot. The arm64 counter is too coarse under nested
  virtualization, so back-to-back reads return the same value.

Lwt is also pinned below 6, which crashes under mirage-solo5.

Native tests:

```bash
dune build @runtest --root . --build-dir _build_all
```

## Observed on the fleet

- **Fleet-acknowledged writes:** 5–20 ms per acknowledged write, with
  forwarding through non-owners working.
- **Takeover with stalled uploads:** n1 was restarted with
  `--upload-delay-ms=600000` and acknowledged 10 writes that only its
  followers held. After a `SIGKILL`, n2 refused to take over while n1's
  lease was still valid. After the lease expired, n2 sealed n1's log on both
  followers, uploaded all 10 entries, claimed epoch 2 and restored the value
  10. No acknowledged write was lost.
- **Every node crashes at once:** with n1's uploads stalled, 10 writes
  were acked by n1 and held only by its followers. All three nodes were
  killed and n2 and n3 restarted. Each replayed its follower log from disk
  (`holding 10 entries`), and n2 recovered n1's log and restored 10. With
  a 256 KB log disk and 400 writes, each follower compacted twice and all
  400 came back.
- **Compaction:** 150 writes left one level-1 snapshot plus 22 files
  instead of 150. After a takeover the new owner restored 150 from them,
  and after it went idle the cell was down to a single file.
- **Rebalance and drain:** n1 restarted holding all 9 cells and spread
  them 3/3/3 within one discovery round. Draining n2 then moved its 3 cells
  to n1 and n3, and n2 exited with code 0. All 9 counters kept their
  values.
- **Alarms:** an alarm fired on its owner's timer, an alarm on a cell
  evicted 5 s earlier woke it at e2 and fired once, and an alarm whose
  owner was killed fired on the node that took over. The `alarms/` index
  was empty afterwards.
- **Partition:** a node cut off from the bucket logged
  `SELF-FENCE: node lease expired` and exited with code 3, and another node
  took over its cell at the next epoch with its state intact.
- **Idle eviction:** an idle cell is made durable, stopped, and its
  ownership released (`node: ""`). The next request reactivates it at
  epoch+1.

## License

Apache-2.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE). The design and the
LTX format come from [celld](https://github.com/denoland/celld) (Apache-2.0).
