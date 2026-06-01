# Atlas Copco Open Protocol

This input reads data from industrial tightening controllers that speak the
**Atlas Copco Open Protocol** over TCP — Atlas Copco (Power Focus, MicroTorque,
MT Focus, Tensor), and many other vendors that implement the same open
specification (Bosch Rexroth Nexo, Desoutter, Stanley, Cleco, ...).

Unlike Modbus or S7, Open Protocol is **not polled**. The controller is the TCP
server (default port `4545`) and this input is the client. It opens a long-lived
session and the controller **pushes** events:

1. **Login** — the input sends MID 0001 and waits for MID 0002 (accepted) or
   MID 0004 (rejected).
2. **Subscribe** — for each configured stream the input opens the matching
   subscription (e.g. MID 0060 for last-tightening results).
3. **Receive & acknowledge** — the controller pushes result telegrams (e.g. MID
   0061) which the input acknowledges (MID 0062). A telegram split across
   several parts is reassembled before it is emitted.
4. **Keep-alive** — the input sends MID 9999 periodically to keep the link up.
5. **Reconnect** — on a dropped connection the input reconnects and replays the
   login + all subscriptions automatically.

## Configuration

```yaml
input:
  open_protocol:
    endpoint: "10.0.0.42:4545"      # controller host:port (required)
    subscribe: [last_tightening]    # streams to subscribe to
    # generic_subscribe: [9999]     # advanced: subscribe arbitrary MIDs by number
    # revision: 0                   # advanced: requested MID revision (0 = controller default)
    # keepalive_interval: 10s       # advanced
    # request_timeout: 5s           # advanced
    # reconnect:
    #   max_backoff: 30s            # advanced
```

### Configuration Parameters

* **endpoint** (required): the controller's TCP endpoint as `host:port`, e.g.
  `10.0.0.42:4545`.
* **subscribe**: event streams to subscribe to. Supported names:
  * `last_tightening` — last tightening result (MID 0060 / 0061 / 0062). This is
    the headline OEE/quality stream.
  * `alarms` — controller alarms (MID 0070 / 0071 / 0072).
* **generic_subscribe** (advanced): a list of additional MID numbers to
  subscribe to via the generic subscription mechanism (MID 0008). Experimental;
  validate against your controller.
* **revision** (advanced): the MID revision requested during login. `0` lets the
  controller choose (treated as revision 1).
* **keepalive_interval** (advanced): how often keep-alive telegrams (MID 9999)
  are sent. Default `10s`.
* **request_timeout** (advanced): how long to wait for the login/handshake reply
  before treating the connection as failed. Default `5s`.
* **reconnect.max_backoff** (advanced): the ceiling for exponential reconnect
  backoff. Default `30s`.

## Output

The input emits **one message per received telegram**:

* **MID 0061 (last tightening result)** is natively decoded into a structured
  JSON payload:

  ```json
  {
    "cell_id": 1, "channel_id": 1,
    "controller_name": "Station-1", "vin": "WBA1234",
    "job_id": 0, "pset_number": 1,
    "batch_size": 5, "batch_counter": 2,
    "tightening_ok": true, "torque_status": 1, "angle_status": 1,
    "torque_min": 47.0, "torque_max": 53.0,
    "torque_target": 50.0, "torque_actual": 50.12,
    "angle_min": 80, "angle_max": 100,
    "angle_target": 90, "angle_actual": 92,
    "timestamp": "2026-06-01:12:00:00",
    "pset_change_time": "2026-06-01:11:00:00",
    "batch_status": 0, "tightening_id": 7
  }
  ```

* **All other MIDs** (alarms, etc.) are emitted as the **raw ASCII data field**
  (header stripped, multi-part reassembled), to be decoded downstream in
  bloblang. This keeps the input thin and lets you support new MIDs in config
  without a new release.

### Metadata

Every message carries routing metadata:

| Field | Description |
|-------|-------------|
| `op_mid` | the 4-digit MID, e.g. `0061` — route on this to pick a decoder |
| `op_revision` | the telegram's MID revision |
| `op_station_id` | the virtual station ID from the header |
| `op_spindle_id` | the spindle ID (for multi-spindle tools) |
| `op_endpoint` | the configured controller endpoint |

Route on `op_mid` with a `switch` processor to apply the right decoding per
message type. See `config/open-protocol-example.yaml` for a full pipeline that
maps results into the UNS.

## Testing without hardware

A headless emulator (the controller side) is provided for development and
integration testing:

```bash
docker build -t umh-op-emulator open_protocol_plugin/testdata/emulator
docker run --rm -p 4545:4545 umh-op-emulator
```

Then point the input at `127.0.0.1:4545`. The emulator pushes a simulated MID
0061 result on a timer once subscribed.

## Limitations (v1)

* Link-layer acknowledgement (MID 9997/9998) is not yet implemented; the session
  uses application-level acks (MID 0062 etc.) and keep-alives.
* Only MID 0061 is natively decoded; all other MIDs are emitted raw.
* TLS and server/listen mode are not supported.
