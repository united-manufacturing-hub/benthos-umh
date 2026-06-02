# Spec: Atlas Copco Open Protocol Input Plugin

> VSDD Phase 1 design spec. Source of truth for the `open_protocol` Benthos input
> in `benthos-umh`. Field layouts are pinned to the **Atlas Copco Open Protocol
> Specification R2.16** unless stated otherwise.

## Problem

Tightening (screw-driving) controllers — Atlas Copco, Bosch Rexroth Nexo,
Desoutter, Stanley and others — are the quality gate of nearly every assembly
line. Each fastening operation produces a torque/angle result with an OK/NOK
verdict that feeds OEE, traceability and quality systems. These controllers
speak the Atlas Copco **Open Protocol**: an ASCII, TCP, push/subscribe protocol
where the controller is the server and the integrator is a long-lived client.

benthos-umh has no way to read this data. A bridge author who wants tightening
results into the UNS today has nothing to point at. A first-draft plugin exists
(`open_protocol_plugin/`, branch `feature/open-protocol-input`) and works against
two simulators, but it was built before its contract was written down: it makes
delivery, decoding-scope, output-shape and metadata-naming decisions implicitly,
diverges from sibling-plugin conventions (`op_*` vs `opcua_*`), and contains a
latent decoding bug for protocol revisions it does not actually support. The
next maintainer cannot tell which of those choices are deliberate.

This spec fixes the contract first, so the implementation can be rebuilt under
test against a written authority rather than against whatever the simulators
happened to emit.

## What We're Shipping

A Benthos **input** named `open_protocol` that connects to one tightening
controller over TCP, maintains a resilient Open Protocol session, and emits
tightening results into a benthos-umh pipeline ready for the standard
`tag_processor → uns` flow.

- A resilient **session**: login → subscribe → receive → acknowledge → keep-alive.
  Reconnect follows the benthos-umh input idiom — on connection loss `ReadBatch`
  returns `service.ErrNotConnected` and Benthos re-invokes `Connect`, which
  replays login + subscriptions (same pattern as `modbus`/`s7comm`).
- Native decoding of **MID 0061 "Last tightening result", revision 1 only**,
  verified per-telegram against the header revision.
- **Fan-out at the source**: one tightening becomes one message per measurement
  (18 tags), each a clean scalar time series, matching how `opcua` and
  `sensorconnect` emit one message per tag.
- **Source-timestamped output**: the controller's own tightening timestamp is
  parsed (with a configurable timezone) into the `timestamp_ms` metadata key the
  `tag_processor` inherits by default, so every fanned tag from one tightening
  shares one timestamp. The asset-identifying fields (cell/channel/station/
  spindle) are carried as metadata into the topic path, which separates
  simultaneous multi-spindle results; `tightening_id` is itself a tag for exact
  verification.
- **At-least-once delivery**: the controller's MID 0062 acknowledgement for a
  result is sent only after that result has been delivered downstream, via the
  Benthos `AckFunc` (see Delivery & Acknowledgement). The plugin never drops a
  result under back-pressure.
- Raw pass-through for every other MID (alarms, etc.) and for any 0061 that is
  not revision 1, for downstream bloblang decoding.

## What We're NOT Shipping

- **No output / write path.** This is read-only. The plugin never commands the
  controller beyond protocol housekeeping (login, subscribe, ack, keep-alive).
- **No native decoding beyond MID 0061 rev 1.** Alarms (0071), job info,
  multi-spindle (0101) and higher 0061 revisions are passed through raw. Higher
  revisions are append-only supersets, so this is extensible later without
  breaking the rev-1 tags.
- **No per-deployment UNS topic shaping inside the input.** The input attaches
  protocol metadata and a tag name; `location_path`, `data_contract` and topic
  construction remain the bridge's `tag_processor` responsibility.
- **No clock synchronisation** (MID 0080/0081/0082). The controller's clock is
  read as-is from the telegram.
- **No TLS / authentication.** Open Protocol defines none; transport security is
  the network's responsibility.

---

## Architecture & Purity Boundary

The plugin is split into a deterministic, side-effect-free **pure core** and a
thin **effectful shell**. This boundary is the central design decision: it is
what makes the decoding and fan-out logic mutation-testable and the session
logic the only place that needs network fakes.

### Pure core (data in → data out, no I/O)

| Module | Responsibility |
|--------|----------------|
| `header.go` | `ParseHeader([]byte) (Header, error)`, `BuildMessage(mid, rev, data) []byte` |
| `mid.go` | `ParseTelegram(frame) (Telegram, error)`, `Reassembler`, `scanPIDFields` |
| `mid_0061.go` | `ParseLastTightening(Telegram) (LastTightening, error)` — rev 1 |
| `mid_0002.go`, `mid_0004.go` | handshake-reply decoders (MID 0002 login-ack, MID 0004 command-error). MID 0005 command-accepted carries only the accepted MID and is parsed inline in `session.go` |
| `fanout.go` *(new)* | `FanOut(LastTightening) []Tag` — pure mapping of a decoded result to ordered `{Name, Value}` measurement tags |
| `timestamp.go` *(new)* | `ParseControllerTime(string, *time.Location) (time.Time, error)` — parse param-20 wall-clock into an instant |

### Effectful shell (I/O, network, Benthos)

| Module | Responsibility |
|--------|----------------|
| `framer.go` | `FrameReader` — adapts an `io.Reader` into telegram frames (stream boundary) |
| `session.go` | `Connect` (dial → login → confirm subscribes → start keep-alive, bump generation) / `Read` (blocking; returns the next forwardable result + its ack closure, or a connection error) / `Close`; owns the `net.Conn` and the monotonic connection `generation`. **No self-managed reconnect loop** — `ReadBatch` returns `service.ErrNotConnected` and Benthos re-invokes `Connect` (sibling idiom: `modbus.go`, `s7comm.go`) |
| `open_protocol.go` | Benthos registration, config parsing, `Connect`/`ReadBatch`/`Close`, ack wiring, telegram → `service.Message` mapping |

### Boundary interface

`session.go`'s `Read` returns each reassembled, forwardable `Telegram` paired
with a per-result acknowledgement closure (and a connection error when the link
drops). `open_protocol.go` runs each `Telegram` through the pure core (`Decode` →
`FanOut` → message construction), wires the Benthos `AckFunc` to the result's
0062 closure, and returns `service.ErrNotConnected` to Benthos on a `Read`
connection error so Benthos re-invokes `Connect`. Each pure-core function depends only on
its explicit arguments (including a supplied `*time.Location`); none reads an
ambient clock, a socket, a global, or a Benthos type. Loading the timezone
(`time.LoadLocation`) happens in `open_protocol.go` at config-parse time (Edge
#15); only the already-loaded `*time.Location` enters the core.

---

## Delivery & Acknowledgement (at-least-once)

This is the load-bearing contract; it is specified before the per-module
contracts because several modules depend on it.

One received result telegram maps to **exactly one** `service.MessageBatch`
returned from **one** `ReadBatch` call, yielding **exactly one** `AckFunc`:

- A rev-1 0061 → a single batch of **all 18 fanned messages**. The input MUST
  NOT split the fan-out across multiple batches or `ReadBatch` calls; doing so
  would produce more than one `AckFunc` and thus more than one MID 0062 for one
  tightening, which is a protocol violation.
- Any other telegram → a single-message batch.

**When the MID 0062 is sent.** "Delivered" means Benthos has obtained successful
downstream delivery acknowledgement for **every** message in the batch (all 18).
Benthos invokes the batch `AckFunc(ctx, err)` once with the aggregate result:

Benthos guarantees a batch's `AckFunc(ctx, err)` is called *at least once*, not
exactly once — it may fire again (e.g. on a downstream retry), including a second
`err == nil` after an earlier `err != nil`. The closure therefore runs this exact
sequence on **every** invocation:

1. **`err != nil`** (any of the 18 failed downstream): log a warning and return
   without sending. The result stays un-acked so the controller may re-push
   (Edge #21). No guard state is touched.
2. **`err == nil`, stale generation**: re-read the current connection generation
   (below) under `mu`; if it no longer equals the captured `gen`, return without
   sending (Edge #22). No guard state is touched.
3. **`err == nil`, current generation**: acquire the per-result send guard; if a
   0062 has **already been sent** for this result, no-op; otherwise write one MID
   0062 to the captured `conn` (under `writeMu`) and set the sent flag **only on a
   successful write**.

The guard is keyed on *having sent a 0062*, never on *being a subsequent call* —
so an `err != nil` followed by a later `err == nil` still sends the first and only
0062. Use one mechanism (a single `sent bool` under a small mutex), not two.

**Connection binding (the "generation" mechanism).** `Session` holds a monotonic
`generation uint64`, incremented under `mu` inside `Connect` (i.e. on every
successful connect or Benthos-driven re-`Connect`). Each result's ack closure
captures `(gen, conn)` at production time and is **independent
per result**: a re-pushed result after reconnect arrives as a new batch with its
own fresh ack closure; the original stale closure is abandoned, never reused. Step
2 compares generations; step 3 writes to the **captured `conn`**, never the
current one. **Lock order is always `mu` → send-guard mutex → `writeMu`, never
reversed** (the send-guard mutex is taken only in the ack closure, so no other
path can invert it). A 0062 **write
failure** (e.g. the captured conn died mid-reconnect) is a benign no-op — the
result is left un-acked and recovered by the reconnect/re-push path; it does
**not** itself trigger a reconnect (unlike a keep-alive write failure, Edge #25).

**Back-pressure.** Under a slow or stalled consumer Benthos stops calling
`ReadBatch`, so the session stops reading the socket; the prior result's 0062 is
not yet sent (it fires from the `AckFunc` after delivery), and unread telegrams
queue at the TCP/controller level (Edge #16) — never dropped by the plugin. For
controllers that gate the next 0061 on the prior 0062, this back-pressures the
source; controllers that free-run instead drop or overwrite un-acked results per
their own buffer policy (the same controller-dependency as the Durability caveat
below). The connection-scoped keep-alive writer (started in `Connect`) keeps
emitting MID 9999 independently of `ReadBatch`, so the controller does not declare
the link dead during back-pressure.

**Durability caveat.** End-to-end at-least-once across a crash or reconnect
depends on the controller buffering and re-pushing un-acked results, which is
controller-dependent and not guaranteed by the plugin. Documented in user docs.

## Divergences from the first draft

The spec mandates behavior the draft (`feature/open-protocol-input`) does not yet
have. The rebuild MUST make these changes:

| Area | Draft today | Required |
|------|-------------|----------|
| Ack timing | Sends 0062 inside `handle`, **before** forwarding (`session.go`) | Send 0062 from the `AckFunc`, after downstream delivery |
| Back-pressure | `default:` case **drops** on full buffer (at-most-once) | Never drop; Benthos stops calling `ReadBatch`, telegrams queue at TCP level |
| Session lifecycle | `Start`/`Stop` + a background `serveLoop` that reconnects itself | `Connect`/`Read`/`Close`; on loss `ReadBatch` returns `service.ErrNotConnected` and Benthos re-invokes `Connect` (drops the self-managed loop and `max_backoff`) |
| AckFunc | Returns a no-op `func(...) error { return nil }` | Wire it to the connection-generation-bound 0062 send, with a one-shot idempotency guard (AckFunc fires ≥ once) |
| Subscribe | Fire-and-forget (`session.go`), no reply awaited | Await MID 0005/0004 per subscribe within `request_timeout`; fail `Connect` on error/timeout |
| Connection generation | No epoch on `Session` | Add monotonic `generation uint64`, incremented per (re)connect, captured by each ack closure |
| Metadata prefix | `op_mid`, `op_revision`, `op_station_id`, `op_spindle_id`, `op_endpoint` | Rename **all** to `open_protocol_*` |
| Revision guard | Decodes 0061 regardless of header revision | Decode only when normalized header revision == 1; else raw pass-through |
| Login revision | Config default `0` ("controller chooses") | Config default `1`; MID 0001 sends revision `001` |
| Dead-link detection | No read deadline; silent half-open hangs | `read_timeout` read deadline in `Read` → `ErrNotConnected` (Edge #23) |
| Fan-out | Emits one structured object per 0061 | Emit 18 fanned single-value messages as one batch |

---

## Behavioral Contract

### FrameReader (`framer.go`)

- **Preconditions**: wraps a byte stream that may deliver an Open Protocol
  telegram split across arbitrary TCP segments.
- **Postconditions**: `ReadFrame` returns exactly one telegram (header + data),
  NUL terminator stripped; returns `io.EOF` only on a clean frame boundary and a
  non-EOF error on any malformed frame.
- **Invariants**: never returns a frame whose declared length is outside
  `[HeaderLength, 9999]`; never returns bytes belonging to the next telegram;
  a telegram missing its NUL terminator is an error, never a silent truncation.

### Header (`header.go`)

- **Preconditions (`ParseHeader`)**: `buf` is at least `HeaderLength` (20) bytes.
- **Postconditions**: all nine ASCII header fields are parsed; blank revision or
  `"000"` ⇒ revision 1; blank multi-part fields ⇒ `TotalParts = PartNumber = 1`.
- **Invariants (`BuildMessage`)**: output length field equals `len(output) − 1`
  (NUL excluded, per spec); `ParseHeader(BuildMessage(mid, rev, data))` round-trips
  `mid` and `rev` (rev normalised: `rev ≤ 0 ⇒ 1`).

### Reassembler (`mid.go`)

- **Preconditions**: fed `Telegram`s in arrival order.
- **Postconditions**: single-part telegrams pass through unchanged with
  `complete = true`; a multi-part sequence returns the concatenated data on its
  final part with the header normalised to a single part.
- **Invariants**: a multi-part sequence that does not start at part 1, or that
  skips a part, is rejected and its partial state discarded — never silently
  concatenated out of order. A sequence exceeding `maxParts` (16) or whose
  assembled size would exceed `maxAssembledBytes` (64 KiB) is a reassembly error;
  in-flight state is bounded by these ceilings.

### LastTightening decoder (`mid_0061.go`)

- **Preconditions**: `t.Header.MID == 61`. Caller has already confirmed the
  **normalized** `t.Header.Revision == 1` (the decode-time revision guard). Note:
  `ParseHeader` normalizes a blank or `"000"` revision to 1, so a 0061 with a
  blank revision field is treated as rev 1 and decoded.
- **Postconditions**: returns a `LastTightening` with parameters 01–23 parsed at
  the spec R2.16 widths; torque values scaled from hundredths-Nm to Nm; angle
  values whole degrees; `tightening_ok = (param 09 == 1)`.
- **Invariants**: a data field whose parameter-ID sequence does not match the
  rev-1 layout is an error, never a partial/garbage struct.

### FanOut (`fanout.go`)

- **Preconditions**: a successfully decoded `LastTightening`.
- **Postconditions**: returns the 18 tags (see Interface) in a fixed order, each
  carrying its scalar value and type.
- **Invariants**: the set and order of tag names is constant for a given
  revision (rev 1 ⇒ always these 18); FanOut performs no I/O and reads no clock.

### Session (`session.go`)

- **Preconditions**: a reachable `host:port` Open Protocol controller.
- **Postconditions (`Connect`)**: closes any prior connection/keep-alive, then
  returns only after a successful login **and** confirmed initial subscriptions,
  or an error. Each subscribe (MID 0060/0070) awaits its reply within
  `request_timeout`: MID 0005 (command accepted) ⇒ confirmed; MID 0004 (command
  error) or timeout ⇒ `Connect` fails. A result telegram arriving in the window
  also confirms. On success it bumps `generation` and starts a connection-scoped
  keep-alive writer. (Controllers that never acknowledge a subscribe are out of
  scope and documented.) `Connect` is re-runnable — Benthos calls it again after
  a `ReadBatch` connection error.
- **Postconditions (`Read`)**: blocks until the next forwardable telegram is
  reassembled, returning it paired with its 0062 ack closure; returns a
  connection error (surfaced to Benthos as `service.ErrNotConnected`) on
  connection loss or `read_timeout` idle. Keep-alive (9999), command-accepted
  (0005) and command-error (0004) telegrams are consumed inside `Read` and not
  returned.
- **Invariants**: at most one live connection at a time; writes (ack vs
  keep-alive) are serialised under `writeMu` and never interleave; the keep-alive
  writer runs independently of `Read`; a connection idle beyond `read_timeout` or
  whose keep-alive write fails is closed so the next `Read` errors (Edge #23/#25);
  `Close` is idempotent.

### Input (`open_protocol.go`)

- **Preconditions**: valid parsed config; `Connect` succeeded before `ReadBatch`.
- **Postconditions (`ReadBatch`)**: returns exactly one `MessageBatch` per
  received result; for a rev-1 0061 it is the 18 fanned messages sharing one
  `timestamp_ms`, for any other telegram a single raw message. On a `Read`
  connection error it returns `service.ErrNotConnected` (no batch), prompting
  Benthos to re-invoke `Connect`. See **Delivery & Acknowledgement** for the
  batch ⇒ single-`AckFunc` ⇒ single MID 0062 rule and the error / reconnect cases.
- **Invariants**: the controller's 0062 is sent only from the `AckFunc`, never
  before the batch is delivered downstream, and never more than once per result;
  message construction reads no socket.

---

## Interface Definition

### Config spec

| Field | Type | Default | Class | Description |
|-------|------|---------|-------|-------------|
| `endpoint` | string | — | basic | Controller TCP endpoint `host:port` (e.g. `10.0.0.42:4545`) |
| `subscribe` | string list | `[last_tightening]` | basic | Event streams: `last_tightening` (MID 0060/0061/0062), `alarms` (MID 0070/0071/0072) |
| `timezone` | string | `UTC` | advanced | IANA zone (e.g. `Europe/Berlin`) used to interpret the controller's zone-less result timestamp. **Set to the controller's local zone.** |
| `revision` | int | `1` | advanced | MID revision requested at login; sent as the 3-digit revision field of MID 0001 (default ⇒ `001`). Pinned to 1; documented for forward-compat |
| `generic_subscribe` | int list | `[]` | advanced | Extra MIDs via the generic mechanism (MID 0008). Experimental; sent fire-and-forget (not awaited/confirmed in `Connect`, unlike the named subscribes) |
| `keepalive_interval` | duration | `10s` | advanced | MID 9999 cadence |
| `request_timeout` | duration | `5s` | advanced | Handshake-reply timeout |
| `read_timeout` | duration | `30s` | advanced | Max idle in `Read` before the connection is declared dead (→ `ErrNotConnected` → Benthos re-`Connect`). **Must be ≥ 2× `keepalive_interval`** — validated at config load (Edge #26) |

Reconnect backoff is owned by the Benthos input runner (the sibling idiom), so
there is no `reconnect.max_backoff` field.

### Output: rev-1 MID 0061 → 18 fanned tag messages

Each message: `payload = scalar value`; metadata as below. `open_protocol_tag_name`
distinguishes the tags.

**Classification rule:** a field is a **tag** if you would read it back from the
historian to reconstruct or analyze a tightening (it lands as a queryable
`(tag, timestamp, value)` row). A field is **metadata** only if it is
static **asset identity** that defines the topic path, or protocol routing.
Metadata stays in Kafka headers and needs custom UNS→Postgres plumbing to become
queryable — so every per-event datum is a tag.

**Parameter disposition (all 23 decoded PIDs accounted for):** PID 01/02
(cell/channel) → asset metadata; PID 03 (controller name) → metadata; PID 20
(timestamp) → `timestamp_ms` + raw `open_protocol_timestamp` metadata; PID 21
(pset-change timestamp) → raw `open_protocol_pset_change_timestamp` metadata. The
remaining **18** parameters become the 18 tags below.

| `open_protocol_tag_name` | Value type | Source param | Unit / encoding |
|--------------------------|-----------|--------------|-----------------|
| `torque_actual` | float | 15 | Nm (param ÷ 100) |
| `torque_target` | float | 14 | Nm (param ÷ 100) |
| `torque_min` | float | 12 | Nm (param ÷ 100) |
| `torque_max` | float | 13 | Nm (param ÷ 100) |
| `angle_actual` | int | 19 | degrees |
| `angle_target` | int | 18 | degrees |
| `angle_min` | int | 16 | degrees |
| `angle_max` | int | 17 | degrees |
| `torque_status` | int | 10 | 0=low, 1=ok, 2=high |
| `angle_status` | int | 11 | 0=low, 1=ok, 2=high |
| `tightening_ok` | bool | 09 | true when param 09 == 1 |
| `batch_status` | int | 22 | 0=incomplete, 1=complete |
| `tightening_id` | int | 23 | per-event id; correlation / verification key |
| `vin` | string | 04 | workpiece identifier (trimmed); traceability key |
| `job_id` | int | 05 | |
| `pset_number` | int | 06 | |
| `batch_counter` | int | 08 | |
| `batch_size` | int | 07 | |

### Metadata on every fanned tag message

| Key | Source | Notes |
|-----|--------|-------|
| `timestamp_ms` | param 20 + `timezone` | epoch-ms string; **shared** across all 18 tags; `tag_processor` inherits it by default |
| `open_protocol_tag_name` | — | the fanned tag name above |
| `open_protocol_mid` | header | `0061` |
| `open_protocol_revision` | header | `1` today; identifies the schema version |
| `open_protocol_timestamp` | param 20 | raw `YYYY-MM-DD:HH:MM:SS` string, for traceability |
| `open_protocol_pset_change_timestamp` | param 21 | raw `YYYY-MM-DD:HH:MM:SS` string; last Pset-change time |
| `open_protocol_cell_id` | param 01 | asset identity → topic path |
| `open_protocol_channel_id` | param 02 | asset identity → topic path |
| `open_protocol_station_id` | header | asset identity → topic path; often `0`/blank on rev-1 controllers — not a bug |
| `open_protocol_spindle_id` | header | asset identity → topic path; separates simultaneous multi-spindle results |
| `open_protocol_controller_name` | param 03 | static asset identity (trimmed) |
| `open_protocol_endpoint` | config | the controller endpoint |

Correlation: all 18 tags from one tightening share one `timestamp_ms`;
simultaneous results on different spindles are separated by their asset-path
topic (station/spindle); `tightening_id` is available as a tag for exact
verification within a topic.

### Output: any other telegram → one raw message

`payload = raw ASCII data field`; metadata = `open_protocol_mid`,
`open_protocol_revision`, `open_protocol_station_id`, `open_protocol_spindle_id`,
`open_protocol_endpoint`. Applies to non-0061 MIDs **and** to any 0061 whose
header revision ≠ 1.

### Timestamp semantics (verified against spec R2.16)

MID 0061 parameter 20 is *"19 ASCII characters (YYYY-MM-DD:HH:MM:SS)"* — a bare
wall-clock datetime with **no timezone, offset, or zone indicator anywhere in the
protocol** (confirmed by full-text scan of the R2.16 spec; the controller clock
is set zone-lessly via MID 0082). Therefore the zone **must** be supplied by
config to produce a correct instant. `timestamp_ms = ParseControllerTime(param20,
timezone).UnixMilli()`, where `ParseControllerTime` uses
`time.ParseInLocation`. DST edges follow Go's `ParseInLocation` semantics: an
ambiguous wall-clock time (fall-back hour) resolves to the earlier offset; a
nonexistent wall-clock time (spring-forward gap) is normalized forward. These
are sub-second-rare for tightening data and documented rather than special-cased.
The same parsing applies to param 21 (pset-change timestamp); both are also
preserved as raw strings in metadata regardless of parse success.

---

## Edge Cases

| # | Input / Condition | Expected Behavior |
|---|-------------------|-------------------|
| 1 | Telegram split across multiple TCP segments | Reassembled by `FrameReader`; one frame returned |
| 2 | Declared frame length < 20 or > 9999 | Framing error. An Open Protocol stream cannot be resynced mid-stream, so the connection is closed and reconnected (Edge #17 path) |
| 3 | Missing NUL terminator after a frame | Error, not silent truncation |
| 4 | Multi-part telegram arrives out of order / skips a part | Rejected, partial state discarded, warning logged |
| 5 | Multi-part telegram does not start at part 1 | Rejected |
| 6 | Login rejected — MID 0004 code 96 (already connected) | `Connect` returns the `CommandError`; no session |
| 7 | Login rejected — MID 0004 code 97 (unsupported revision) | `Connect` returns the `CommandError`; no session |
| 8 | Unexpected MID during handshake | `Connect` returns an error |
| 9 | 0061 with header revision ≠ 1 | Raw pass-through (single message), warning logged; never decoded with rev-1 layout |
| 10 | 0061 data field whose parameter-ID sequence is malformed | Decode error → raw pass-through, warning logged |
| 11 | `controller_name` / `vin` padded with trailing spaces | Trimmed in metadata |
| 12 | Non-ASCII bytes in a string field | Preserved as-is in metadata (raw), not rejected |
| 13 | param-23 tightening ID width: spec 10 vs some emulators 4 | Final field consumes the remainder; documented divergence, covered by golden tests for both |
| 14 | param 20 not parseable as `YYYY-MM-DD:HH:MM:SS` | `timestamp_ms` omitted; `open_protocol_timestamp` still carries the raw string; tag_processor falls back to ingest time; warning logged |
| 15 | Unknown `timezone` config value | Config-load error (fail fast at startup) |
| 16 | Slow / stalled downstream consumer | Benthos stops calling `ReadBatch`; unread telegrams queue at TCP level, never dropped; keep-alive writer keeps the link up (see Delivery & Acknowledgement) |
| 17 | Connection drops mid-session | `Read` errors → `ReadBatch` returns `service.ErrNotConnected` → Benthos re-invokes `Connect`, replaying login + subscriptions (backoff owned by Benthos) |
| 18 | `Close` called twice / during shutdown | Idempotent; stops the keep-alive writer and closes the connection |
| 19 | Unknown subscription name in `subscribe` | Ignored with a warning; other subscriptions proceed |
| 20 | Controller never pushes a result | Session stays alive on keep-alives; `ReadBatch` blocks until ctx cancel |
| 21 | `AckFunc` invoked with non-nil error (downstream delivery failed) | MID 0062 is **not** sent; result left un-acked so the controller may re-push; warning logged |
| 22 | `AckFunc` fires after the originating connection was replaced by reconnect | 0062 send is a no-op (bound to the connection generation); controller re-pushes on the new session |
| 23 | No telegram received within `read_timeout` while in `Read` (half-open TCP, cable pull) | `Read` returns a connection error → `ErrNotConnected` → Benthos re-`Connect` (Edge #17 path) |
| 24 | Multi-part sequence exceeds `maxParts` (16) or `maxAssembledBytes` (64 KiB) | Reassembly error; partial state discarded; warning logged |
| 25 | Keep-alive (MID 9999) **write** fails | Connection closed so the next `Read` errors → `ErrNotConnected` → Benthos re-`Connect` (distinct from read-idle, Edge #23) |
| 26 | `read_timeout < 2 × keepalive_interval` configured | Config-load error (fail fast); would otherwise reconnect-storm before a keep-alive can arrive |
| 27 | `AckFunc` invoked more than once (Benthos at-least-once) | Guard keyed on *having sent*: the first `err==nil` sends a 0062 (even if it follows an `err!=nil`); any call after a 0062 was sent is a no-op |
| 28 | `AckFunc` fires after `Close` or after a re-`Connect` (late Benthos delivery goroutine) | No-op — a re-`Connect` advanced the generation (generation check suppresses it); after `Close` the captured conn is closed so the 0062 write fails benignly (write-failure path) |

---

## Non-Functional Requirements

- **Delivery guarantee**: at-least-once *to Benthos within a live session*, per
  the **Delivery & Acknowledgement** contract (one batch ⇒ one `AckFunc` ⇒ one
  0062; ack only after full downstream delivery; block, never drop). The
  reconnect-durability caveat is stated there and in the user docs.
- **Performance**: a single controller pushes results at human-cycle rates
  (≤ low tens/sec even multi-spindle). No batching or pooling needed; the cost
  centre is correctness, not throughput.
- **Memory**: one buffered connection + in-flight multi-part reassembly bounded
  by `maxParts` (16) and `maxAssembledBytes` (64 KiB) (Edge #24). No unbounded
  growth.
- **Security**: no credentials, no TLS in-protocol. The plugin issues only
  read/housekeeping MIDs; it never writes Psets, jobs, or controller config.
- **Resilience**: reconnect is Benthos-driven (`ReadBatch` → `ErrNotConnected` →
  re-`Connect`, replaying login + subscriptions); malformed telegrams are dropped
  with a warning and never crash the read path.
- **Convention conformance**: metadata uses the `open_protocol_` prefix
  (matching `opcua_`/`sensorconnect_`/`sparkplug_`); fields follow the
  basic/advanced UI-classification rule; tests use Ginkgo v2 + Gomega.

---

## Verification Strategy

### Provable Properties

| # | Property | Method | Priority |
|---|----------|--------|----------|
| 1 | `ParseHeader(BuildMessage(mid, rev, data))` round-trips mid/rev | property test (rapid) | high |
| 2 | `FrameReader` never emits cross-telegram bytes; honours length+NUL | unit + mutation | critical |
| 3 | Reassembler rejects out-of-order / non-1-start sequences | unit + mutation | high |
| 4 | rev-1 0061 decode reproduces hand-derived R2.16 values (incl. exact consumed length of the widthRest PID-23 field) | golden file = hand-derived expectations | critical |
| 5 | Decode-time revision guard: normalized rev ≠ 1 ⇒ never rev-1-decoded | unit + mutation | critical |
| 6 | FanOut emits exactly the 18 tags, fixed order, correct types | unit + mutation | high |
| 7 | FanOut reads no clock; the one `timestamp_ms` is computed once upstream and copied to all 18 (purity invariant) | unit | high |
| 8 | At-least-once ack: one 0062 only after all-18 delivered (`err==nil`); none on `err!=nil`; no-op on stale connection generation; at most one 0062 even if AckFunc fires twice | session tests over five traces: success / downstream-fail / downstream-fail-then-success (⇒ exactly one 0062) / reconnect-before-ack / double-clean-ack | critical |
| 9 | `ParseControllerTime` is timezone-correct and rejects bad input | unit + property | high |

### Purity Boundary

- **Pure core**: `header.go`, `mid.go`, `mid_0061.go`, `mid_0002.go`,
  `mid_0004.go`, `fanout.go`, `timestamp.go`.
- **Effectful shell**: `framer.go` (io boundary), `session.go`, `open_protocol.go`.
- **Boundary interface**: session yields reassembled `Telegram`s + an ack
  callback; the input maps each via the pure core. No socket, wall-clock, or
  Benthos type crosses into the core.

### Tooling

- **Mutation testing**: `gremlins` on the pure core, target ≥ 95% with a
  documented exclusion list (error-string formatting; the `time.ParseInLocation`
  wrapper in `timestamp.go`, whose zone/DST correctness mutation cannot
  meaningfully test). The widthRest PID-23 logic requires an explicit
  exact-consumed-length assertion to kill boundary mutants (property #4).
- **Static analysis**: `golangci-lint`.
- **Property-based**: `pgregory.net/rapid` for properties 1 and 9. (Property 7 is
  a unit-level purity check, not a randomized property.)
- **Integration**: retain the dockerized GPL emulator test (`//go:build
  integration`, gated by `TEST_OPEN_PROTOCOL`); validates the wire format against
  code we did not write. **Coverage note**: the emulator emits the 4-digit PID-23
  width, so the 10-digit spec-width decode path is exercised only by golden
  files, never against an independent implementation.
- **Golden files**: rev-1 0061 telegrams (spec-width and emulator-width
  variants), decoded and compared field-by-field against hand-derived R2.16
  expected values.

---

## Open Protocol reference (R2.16) — MID 0061 rev-1 parameter layout

Parameter-ID format: each value is preceded by its 2-digit parameter id, fixed
order, fixed widths.

| PID | Field | Width | PID | Field | Width |
|-----|-------|-------|-----|-------|-------|
| 01 | cell id | 4 | 13 | torque max (×100) | 6 |
| 02 | channel id | 2 | 14 | torque target (×100) | 6 |
| 03 | controller name | 25 | 15 | torque actual (×100) | 6 |
| 04 | VIN | 25 | 16 | angle min | 5 |
| 05 | job id | 2 | 17 | angle max | 5 |
| 06 | pset number | 3 | 18 | angle target | 5 |
| 07 | batch size | 4 | 19 | angle actual | 5 |
| 08 | batch counter | 4 | 20 | timestamp | 19 |
| 09 | tightening status | 1 | 21 | pset-change timestamp | 19 |
| 10 | torque status | 1 | 22 | batch status | 1 |
| 11 | angle status | 1 | 23 | tightening id | 10 (spec) / 4 (some emulators) |
| 12 | torque min (×100) | 6 | | | |
