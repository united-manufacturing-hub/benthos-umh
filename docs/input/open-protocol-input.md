# Atlas Copco Open Protocol

This input reads data from industrial tightening controllers that speak the **Atlas Copco Open Protocol** over TCP. Supported vendors include Atlas Copco (Power Focus, MicroTorque, MT Focus, Tensor), Bosch Rexroth Nexo, Desoutter, and Stanley.

Open Protocol is **not polled**. The controller is the TCP server (default port `4545`); this input is the client. It opens a long-lived session and receives pushed events:

1. **Login** — sends MID 0001 and waits for MID 0002 (accepted) or MID 0004 (rejected).
2. **Subscribe** — opens each configured subscription (e.g. MID 0060 for last-tightening results) and awaits confirmation before proceeding.
3. **Receive & acknowledge** — the controller pushes result telegrams (e.g. MID 0061); the input acknowledges each (MID 0062) only after successful downstream delivery.
4. **Keep-alive** — sends MID 9999 at a configurable interval to keep the link alive.
5. **Reconnect** — on connection loss, reconnects automatically and replays login + subscriptions.

This is a **read-only** input. It never writes parameter sets, jobs, or controller configuration.

## Configuration

**Minimal example**

```yaml
input:
  open_protocol:
    endpoint: "10.0.0.42:4545"
    subscribe: [last_tightening]
```

**Full example**

```yaml
input:
  open_protocol:
    endpoint: "10.0.0.42:4545"
    subscribe: [last_tightening, alarms]
    timezone: "Europe/Berlin"
    keepalive_interval: 10s
    read_timeout: 30s
    request_timeout: 5s
    revision: 1
    generic_subscribe: []
```

### Configuration parameters

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | string | **required** | Controller TCP endpoint as `host:port` (e.g. `10.0.0.42:4545`) |
| `subscribe` | string list | `[last_tightening]` | Event streams to subscribe to. `last_tightening` opens MID 0060/0061/0062; `alarms` opens MID 0070/0071/0072 |
| `timezone` | string | `UTC` | IANA timezone used to interpret the controller's zone-less timestamps. **Set to the controller's local zone** (see [Timezone note](#timezone-note)) |
| `read_timeout` | duration | `30s` | Max idle time in `Read` before the connection is declared lost and Benthos reconnects. Must be ≥ 2× `keepalive_interval` |
| `keepalive_interval` | duration | `10s` | How often to send MID 9999 keep-alive telegrams |
| `request_timeout` | duration | `5s` | How long to wait for login/subscribe handshake replies before failing |
| `revision` | int | `1` | MID revision requested at login. Pinned to 1 for forward-compatibility |
| `generic_subscribe` | int list | `[]` | Additional MIDs to subscribe via the generic mechanism (MID 0008). Experimental; validate against your controller |

## Output — fan-out

A single MID 0061 rev-1 tightening result is emitted as **18 messages**, one per measurement tag. Each message carries a scalar payload and shared metadata.

### Tags

All 18 messages from one tightening share the same `timestamp_ms` (derived from the controller's result timestamp).

| `open_protocol_tag_name` | Value type | Unit / encoding |
|--------------------------|-----------|-----------------|
| `torque_actual` | float | Nm |
| `torque_target` | float | Nm |
| `torque_min` | float | Nm |
| `torque_max` | float | Nm |
| `angle_actual` | int | degrees |
| `angle_target` | int | degrees |
| `angle_min` | int | degrees |
| `angle_max` | int | degrees |
| `torque_status` | int | 0 = low, 1 = ok, 2 = high |
| `angle_status` | int | 0 = low, 1 = ok, 2 = high |
| `tightening_ok` | bool | `true` when the result is OK |
| `batch_status` | int | 0 = incomplete, 1 = complete |
| `tightening_id` | int | Per-event ID; traceability/verification key |
| `vin` | string | Workpiece identifier (trimmed); traceability key |
| `job_id` | int | |
| `pset_number` | int | |
| `batch_counter` | int | |
| `batch_size` | int | |

### Metadata

Every fanned-out message carries the following metadata:

| Key | Description |
|-----|-------------|
| `timestamp_ms` | Result timestamp as epoch milliseconds; shared across all 18 tags; inherited by `tag_processor` automatically |
| `open_protocol_tag_name` | The measurement tag name from the table above |
| `open_protocol_mid` | `0061` |
| `open_protocol_revision` | `1` (identifies the schema version) |
| `open_protocol_timestamp` | Raw controller timestamp string (`YYYY-MM-DD:HH:MM:SS`); preserved for traceability |
| `open_protocol_pset_change_timestamp` | Raw string; last parameter-set change time |
| `open_protocol_cell_id` | Cell ID from the data field; asset identity for topic path |
| `open_protocol_channel_id` | Channel ID from the data field; asset identity for topic path |
| `open_protocol_station_id` | Station ID from the telegram header; asset identity for topic path. Often `0` on rev-1 controllers — this is normal, not a bug |
| `open_protocol_spindle_id` | Spindle ID from the telegram header; separates simultaneous multi-spindle results |
| `open_protocol_controller_name` | Controller name (trimmed); static asset identity |
| `open_protocol_endpoint` | Configured controller endpoint |

## Other MIDs and non-rev-1 telegrams

All other received telegrams — alarms (MID 0071), any MID 0061 with revision ≠ 1, or a MID 0061 whose data field cannot be parsed — are emitted as **one raw message** containing the ASCII data field (header stripped, multi-part reassembled). Metadata on raw messages: `open_protocol_mid`, `open_protocol_revision`, `open_protocol_station_id`, `open_protocol_spindle_id`, `open_protocol_endpoint`. No `open_protocol_tag_name` is set.

Use a `switch` processor routing on `@open_protocol_mid` to decode these downstream in bloblang.

## Bridge example — tag_processor → UNS

`timestamp_ms` is inherited by `tag_processor` automatically, so no JavaScript is needed to forward the controller timestamp.

```yaml
input:
  open_protocol:
    endpoint: "10.0.0.42:4545"
    subscribe: [last_tightening]
    timezone: "Europe/Berlin"

pipeline:
  processors:
    - tag_processor:
        defaults: |
          // Only process fanned-out MID 0061 tags.
          if (!msg.meta.open_protocol_tag_name) {
            return null;  // drop raw/alarm messages
          }

          msg.meta.location_path = "acme.berlin.assembly.line1";
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name = msg.meta.open_protocol_tag_name;

          // Optional: add cell/channel to virtual path for multi-spindle setups.
          // msg.meta.virtual_path = "cell" + msg.meta.open_protocol_cell_id;

          // timestamp_ms is already set by the input — tag_processor inherits it.
          return msg;

output:
  uns: {}
```

## Delivery semantics and caveats

**At-least-once within a live session.** MID 0062 (the tightening acknowledgement) is sent to the controller only after Benthos has confirmed successful downstream delivery of all 18 messages. The plugin never drops a result under back-pressure; unread telegrams queue at the TCP/controller level while Benthos is busy.

**Durability across crashes and reconnects.** End-to-end at-least-once across a Benthos crash or reconnect depends on the controller buffering and re-pushing un-acked results. This is controller-dependent and not guaranteed by the plugin. Controllers that gate the next result on the prior acknowledgement provide natural back-pressure; controllers that free-run may drop or overwrite un-acked results per their own buffer policy.

**Reconnect.** On connection loss, Benthos re-invokes `Connect`, which replays login and all subscriptions. The controller re-pushes un-acked results if it buffers them.

## Timezone note

Open Protocol timestamps (MID 0061 parameter 20) are bare wall-clock strings (`YYYY-MM-DD:HH:MM:SS`) with **no timezone or offset** — this is per the Atlas Copco Open Protocol specification R2.16. Without a correct `timezone`, `timestamp_ms` will be computed against UTC and will be wrong for any controller not running on UTC.

Set `timezone` to the IANA zone of the controller's system clock, for example `Europe/Berlin` or `America/New_York`.

## Testing without hardware

A headless emulator is provided for integration testing:

```bash
docker build -t umh-op-emulator open_protocol_plugin/testdata/emulator
docker run --rm -p 4545:4545 umh-op-emulator
```

Point the input at `127.0.0.1:4545`. The emulator pushes a simulated MID 0061 rev-1 result on a timer once subscribed. Integration tests require Docker and `TEST_OPEN_PROTOCOL=true`; see `open_protocol_plugin/integration_test.go`.

## Limitations (v1)

- Only MID 0061 revision 1 is natively decoded; higher revisions and all other MIDs are emitted raw.
- No write path. The input never commands the controller beyond protocol housekeeping.
- No TLS. Open Protocol defines no transport security; use network-level controls.
- No clock synchronisation (MID 0080/0081/0082). The controller's clock is read as-is.
- Link-layer acknowledgement (MID 9997/9998) is not implemented; the session uses application-level acks (MID 0062) and keep-alives.
