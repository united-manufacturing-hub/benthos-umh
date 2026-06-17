# Beckhoff ADS (Input)

Input for Beckhoff PLCs using the ADS (Automation Device Specification) protocol. Supports batch reading and change notifications. This input only supports symbols, not direct addresses.

Beckhoff recommends limiting notifications to approximately 500 per connection to avoid overloading the controller. For larger symbol lists, use `readType: interval` instead.

**Metadata outputs**

| Metadata Field | Description |
|---|---|
| `symbol_name` | Sanitized PLC symbol name (dots and special characters replaced with `_`) |
| `data_type` | PLC data type string as reported by the symbol table (e.g. `DINT`, `E_MachineState`, `REAL`). Set after first successful symbol resolution — may be absent on the very first batch after connect. |
| `base_type` | Resolved IEC 61131-3 primitive underlying the symbol (e.g. `DINT` for an INT-aliased enum). Only set when the type resolves to a known primitive. |
| `data_size` | Byte length of the symbol as reported by the PLC (e.g. `4` for DINT, `82` for STRING). |

## Typical Deployment — TwinCAT 3

The most common setup: umh-core running in a VM, connecting to a TwinCAT 3 PLC on the same network. Set `hostIP` to the VM's IP so the PLC registers the correct route entry. `username`/`password` handle route registration automatically on first connect.

```yaml
input:
  ads:
    targetIP: "192.168.1.100"
    targetAMS: "192.168.1.100.1.1"
    runtimePort: 851
    hostIP: "192.168.1.50"                 # VM IP on the PLC network
    username: "Administrator"
    password: "1"
    symbols:
      - "GVL_ProcessData.nMasterCycleCounter"
      - "GVL_ProcessData.fTemperature"
      - "MAIN.bRunning"
```
```yaml
pipeline:
  processors:
    - tag_processor:
        defaults: |-
          msg.meta.location_path = "enterprise.site.area.line";
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name      = msg.meta.symbol_name;
          return msg;
```
```yaml
output:
  uns: {}
```

## Typical Deployment — TwinCAT 2

Same pattern for TwinCAT 2 (runtime port 801, flat dot-prefixed namespace):

```yaml
input:
  ads:
    targetIP: "192.168.1.200"
    targetAMS: "5.3.69.134.1.1"
    runtimePort: 801
    hostIP: "192.168.1.50"                 # VM IP on the PLC network
    username: "Administrator"
    password: "1"
    symbols:
      - ".nMasterCycleCounter"
      - ".fTemperature"
      - ".bRunning"
```
```yaml
pipeline:
  processors:
    - tag_processor:
        defaults: |-
          msg.meta.location_path = "enterprise.site.area.line";
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name      = msg.meta.symbol_name;
          return msg;
```
```yaml
output:
  uns: {}
```

## Minimal Example — TwinCAT 3

TwinCAT 3 with a pre-configured static route on the PLC (no credentials needed):

```yaml
input:
  ads:
    targetIP: "192.168.1.100"
    targetAMS: "192.168.1.100.1.1"
    runtimePort: 851
    symbols:
      - "GVL_ProcessData.nMasterCycleCounter"
      - "MAIN.MyVariable"
```
```yaml
pipeline:
  processors:
    - tag_processor:
        defaults: |-
          msg.meta.location_path = "enterprise.site.area.line";
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name      = msg.meta.symbol_name;
          return msg;
```
```yaml
output:
  uns: {}
```

## Minimal Example — TwinCAT 2

TwinCAT 2 with a pre-configured static route (flat dot-prefixed namespace, runtime port 801):

```yaml
input:
  ads:
    targetIP: "192.168.1.200"
    targetAMS: "5.3.69.134.1.1"
    runtimePort: 801
    symbols:
      - ".nMasterCycleCounter"
      - ".myVariable"
```
```yaml
pipeline:
  processors:
    - tag_processor:
        defaults: |-
          msg.meta.location_path = "enterprise.site.area.line";
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name      = msg.meta.symbol_name;
          return msg;
```
```yaml
output:
  uns: {}
```

## Full Example — TwinCAT 3

All fields with TC3 defaults and comments:

```yaml
input:
  ads:
    # Target connection
    targetIP: "192.168.1.100"              # IP address of the Beckhoff PLC
    targetAMS: "192.168.1.100.1.1"         # AMS net ID of the target runtime
    runtimePort: 851                       # TC3 runtime port (TC2: 801)
    targetPort: 48898                      # TCP port of the PLC ADS gateway
    # Local AMS identity
    hostAMS: "auto"                        # auto = derived from outbound TCP source IP
    hostPort: 0                            # 0 = random per session (recommended)
    hostIP: "192.168.1.50"                 # Docker host IP; auto-detected if empty
    # Route registration — both must be set to activate
    username: "Administrator"
    password: "1"
    # Read mode
    readType: "notification"               # notification | interval
    transmissionMode: "serverOnChange2"    # TC3: auto-falls back to serverOnChange on older firmware
    cycleTime: 100ms                       # how often PLC checks for changes
    maxDelay: 100ms                        # max batching window before PLC sends
    intervalTime: 1s                       # poll interval (readType: interval only)
    # Advanced
    requestTimeout: 5s
    logLevel: "error"
    # Symbols (list last for readability)
    symbols:
      - "GVL_ProcessData.nMasterCycleCounter"
      - "MAIN.MyTrigger:maxDelay=0s:cycleTime=10ms"  # per-symbol overrides
```

## Full Example — TwinCAT 2

All fields with TC2 differences highlighted:

```yaml
input:
  ads:
    # Target connection
    targetIP: "192.168.1.200"              # IP address of the Beckhoff PLC
    targetAMS: "5.3.69.134.1.1"            # AMS net ID of the target runtime
    runtimePort: 801                       # TC2 runtime port
    targetPort: 48898
    # Local AMS identity
    hostAMS: "auto"
    hostPort: 0
    hostIP: "192.168.1.50"                 # Docker host IP; auto-detected if empty
    # Route registration — both must be set to activate
    username: "Administrator"
    password: "1"
    # Read mode
    readType: "notification"
    transmissionMode: "serverOnChange"     # TC2: serverOnChange2 not supported
    cycleTime: 100ms
    maxDelay: 100ms
    intervalTime: 1s                       # readType: interval only
    # Advanced
    requestTimeout: 5s
    logLevel: "error"
    # Symbols — TC2 global variables use dot prefix
    symbols:
      - ".nMasterCycleCounter"
      - ".myTrigger:maxDelay=0s:cycleTime=10ms"
```

## Connection to ADS

When connecting to an ADS device you connect to a router which then routes the traffic to the correct device using the AMS net ID.
Every Beckhoff PLC has an inbuilt router to handle this. The IP to the PLC and the AMS ID is required.
The IP is used to connect to the router, and the AMS ID is needed for the router to forward the packet to the correct device, in this case the PLC runtime.

**Connection flow:**

1. Plugin opens TCP to PLC port 48898 (or configured target port)
2. Plugin sends AMS handshake with source NetID
3. PLC checks route table: "do I have an entry for this NetID?" — if yes, accept
4. All further ADS commands and notifications flow on that same socket

Routes persist on the PLC (survive reboots). The first connection with `username`/`password` creates the route — subsequent connections only need a matching NetID. No reverse connection ever happens.

There are three ways to set up the connection:

1. **TwinCAT Connection Manager**: Use the TwinCAT connection manager locally on the host, scan for the device and add a connection using the correct credentials for the PLC.
2. **Static route on PLC**: Log in to the PLC using the TwinCAT System Manager and add a static route from the PLC to the client. This is the preferred way when using benthos on a Kubernetes cluster since you cannot install the connection manager there. When adding the route, specify the IP and the AMS ID of the UMH instance. Best practice: use the VM's IP as the base and append `.1.1` for the AMS ID (e.g., VM IP `192.168.1.45` → AMS ID `192.168.1.45.1.1`).
3. **Automatic route registration (UDP)**: Use the `username` and `password` config fields to have the plugin automatically register a route on the PLC before connecting. See the [Route Registration](#route-registration) section below. Using this without specifying `hostIP` and `hostAMS` will create a route using the Docker container's internal IP as the NetID. This works, since the NetID is just an identifier for an authorized route. However, setting `hostIP` explicitly is recommended — internal Docker IPs are ephemeral and make it unclear where the route originates when viewed in the PLC's route list.

### Docker and Kubernetes

ADS works from inside Docker containers with default bridge networking — **no `host_network`, no port forwarding, and no open ports are needed**. All ADS traffic (requests, responses, and notifications) flows over a single outbound TCP connection to port 48898. The PLC never initiates connections back to the client; it sends all responses and notifications on the same TCP socket the client opened.

The only requirement is that the `hostAMS` value matches a route registered on the PLC. When running in Docker with bridge networking:
- **`hostIP` should be set** to the Docker host's IP on the PLC network (e.g. `192.168.1.50`). This tells the PLC which IP address to associate with the route. If left empty, it auto-detects the container's bridge IP, which makes route entries on the PLC hard to identify.
- **`hostAMS` can be set explicitly** to `hostIP` + `.1.1` (e.g. `192.168.1.50.1.1`), or left as `auto` — when route registration is configured with `hostIP`, `auto` will correctly derive the AMS NetID from `hostIP` instead of the container's bridge IP.
- **A route must exist on the PLC** for the `hostAMS` NetID. This can be added manually in TwinCAT System Manager, or automatically via the `username`/`password` config fields.
- **`hostPort` is optional** (default 0 = random per session). It is a logical AMS port used in protocol headers, not a network port.

**Option A: Automatic route registration (recommended)**

The plugin registers a route on the PLC automatically via UDP before connecting. No manual PLC configuration needed:

```yaml
input:
  ads:
    targetIP: "192.168.1.100"
    targetAMS: "192.168.1.100.1.1"
    runtimePort: 851
    hostAMS: "auto"                        # derives NetID from hostIP
    hostIP: "192.168.1.50"                 # Docker host IP (required in bridge networking)
    username: "Administrator"              # triggers automatic route registration
    password: "1"
    readType: "notification"
    symbols:
      - "MAIN.MyVariable"
```

You can also set `hostAMS` explicitly if you prefer:

```yaml
    hostAMS: "192.168.1.50.1.1"            # explicit: Docker host IP + .1.1
    hostIP: "192.168.1.50"                 # must match
```

**Option B: Static route on PLC**

If you prefer not to use automatic registration, add a static route on the PLC via TwinCAT System Manager pointing to the Docker host's IP. Then configure `hostAMS` to match — no `username`/`password` needed:

```yaml
input:
  ads:
    targetIP: "192.168.1.100"
    targetAMS: "192.168.1.100.1.1"
    runtimePort: 851
    hostAMS: "192.168.1.50.1.1"            # must match the route on the PLC
    readType: "notification"
    symbols:
      - "MAIN.MyVariable"
```

**Option C: host_network or macvlan**

When the container has a routable IP on the PLC network, `hostAMS: auto` works without `hostIP`:

```yaml
input:
  ads:
    targetIP: "192.168.1.100"
    targetAMS: "192.168.1.100.1.1"
    runtimePort: 851
    hostAMS: "auto"                        # auto-derive from container's real IP
    username: "Administrator"              # optional: auto-register route
    password: "1"
    readType: "notification"
    symbols:
      - "MAIN.MyVariable"
```

## Configuration Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| **targetIP** | Yes | — | IP address of the Beckhoff PLC |
| **targetAMS** | Yes | — | AMS net ID of the target |
| **symbols** | Yes | — | List of symbols to read (see [Symbols Format](#symbols-format) below) |
| **targetPort** | No | `48898` | Port of the target internal gateway |
| **runtimePort** | No | `851` | Runtime port of PLC system. TwinCAT 3: 851, TwinCAT 2: 801 |
| **hostAMS** | No | `auto` | Host AMS net ID. Usually the IP address + `.1.1`. Must match a route on the PLC. `auto` derives it from `hostIP` if set, otherwise from the outbound connection's local IP |
| **hostPort** | No | `0` | AMS source port in protocol headers. `0` uses a random port per session (recommended — avoids notification handle conflicts across sessions). Set a fixed value only in firewalled environments with port allow-lists |
| **readType** | No | `notification` | Read type for the symbols. `interval` polls at `intervalTime`; `notification` uses PLC push updates (see [Interval vs Notification](#interval-vs-notification)). Accepted values: `notification`, `interval` |
| **maxDelay** | No | `100ms` | Maximum time the PLC batches notifications before sending (e.g. `100ms`, `0s`). All changes are still delivered — this controls delivery latency vs network efficiency. `0s` = send immediately. |
| **cycleTime** | No | `100ms` | How often the PLC checks the symbol for changes (e.g. `100ms`, `10ms`, `1s`). Lower = more responsive but more PLC CPU. Values faster than the PLC task cycle are clamped to the task cycle. |
| **intervalTime** | No | `1s` | Interval time between reads (e.g. `1s`, `500ms`). Only used when `readType` is `interval` |
| **requestTimeout** | No | `5s` | Timeout for individual ADS requests (e.g. `5s`, `10s`). Increase for slow PLCs or large symbol tables |
| **transmissionMode** | No | `serverOnChange` | Notification transmission mode. Only applies when `readType` is `notification`. Accepted values: `serverOnChange`, `serverCycle`, `serverOnChange2`, `serverCycle2` (see [Transmission Modes](#transmission-modes)) |
| **logLevel** | No | `error` | go-ads library log level. `error` surfaces transport and ADS errors. `disabled` silences all library logs. `debug`/`trace` add verbose ADS wire details including human-readable error codes. |
| **username** | No | `""` | Username for automatic UDP route registration on the PLC. Both `username` and `password` must be set to activate registration. Requires UDP port 48899 to be reachable (see [Route Registration](#route-registration)) |
| **password** | No | `""` | Password for automatic UDP route registration on the PLC |
| **hostIP** | No | `""` | IP address the PLC associates with the route. Required in Docker bridge networking (set to Docker host's IP). When `hostAMS` is `auto`, the AMS NetID is also derived from this. Auto-detected from outbound connection if empty (only correct with `host_network` or macvlan) |
| **loadSymbols** | No | `false` | Download the full symbol and datatype table from the PLC on connect. Required for subscribing to whole struct or array symbols. May cause brief real-time jitter on the PLC during initial connection; use with care on large programs. See [Struct and Array Symbols](#struct-and-array-symbols) |

### Symbols Format

Symbols are specified as `name[:opt1[:opt2...]]`. Options are either positional integers or `key=value` pairs. Both forms can be mixed.

| Format | maxDelay | cycleTime |
|--------|----------|-----------|
| `MAIN.var` | default | default |
| `MAIN.var:50:100` | 50 | 100 |
| `MAIN.var:50` | 50 | default |
| `MAIN.var::100` | default | 100 |
| `MAIN.var:cycleTime=100` | default | 100 |
| `MAIN.var:maxDelay=50` | 50 | default |
| `MAIN.var:50:cycleTime=100` | 50 (positional) | 100 (key) |
| `MAIN.var:30:maxDelay=50` | 50 (key overrides positional 30) | default |
| `MAIN.var:maxDelay=50:cycleTime=100` | 50 | 100 |

**Rules:**
- Positional integers fill `maxDelay` then `cycleTime` in order
- An empty slot (`::`) reserves the position but keeps the default — use to skip `maxDelay` and set only `cycleTime`
- `key=value` options override by name and do not consume a positional slot
- A keyed option always wins over a positional option for the same field
- Invalid or omitted values fall back to the plugin-level `maxDelay`/`cycleTime` defaults

**Examples:**
- `MAIN.MYBOOL` — uses plugin-level defaults for both
- `MAIN.MYTRIGGER:0:10` — 0ms max delay, 10ms cycle time
- `MAIN.MYSENSOR::10` — default max delay, 10ms cycle time
- `.superDuperInt` — global variable (TC2, must start with `.`)

**TwinCAT 3** uses GVL-prefixed symbols: `GVL_ProcessData.nCounter`, `MAIN.MyVariable`

**TwinCAT 2** uses a flat namespace with dot prefix: `.nCounter`, `.myVariable`. Symbol names are case-insensitive — the PLC accepts any casing, and the plugin always preserves the casing you configured (TC3 returns original casing; TC2 returns uppercase, which the plugin maps back to your configured casing).

## Struct and Array Symbols

Two approaches for reading structured PLC data:

### Option A — Dot-notation (recommended, no extra config)

Subscribe to individual primitive members using their full dot-path. No `loadSymbols` needed.

```yaml
symbols:
  - "MAIN.MachineStatus.Motor1.fSpeed"
  - "MAIN.MachineStatus.Motor1.bRunning"
  - "MAIN.MachineStatus.Pressure.fValue"
```

- Each symbol fires independently on change
- Returns primitive values (`REAL`, `BOOL`, `INT`, etc.)
- Works with `readType: notification` and `readType: interval`
- Scales to large structs without downloading the symbol table

### Option B — Whole struct subscription (requires `loadSymbols: true`)

Subscribe to the struct symbol directly. The plugin downloads the full symbol and datatype table from the PLC on connect, enabling recursive JSON decode.

```yaml
input:
  beckhoff_ads:
    targetIP: "192.168.1.10"
    targetAMS: "5.1.2.3.1.1"
    loadSymbols: true
    symbols:
      - "MAIN.MachineStatus"
```

Output per notification:

```json
{
  "Motor1": {
    "fSpeed": 1450.5,
    "bRunning": true
  },
  "Pressure": {
    "fValue": 3.2
  },
  "sMachineName": "Line1"
}
```

- Single subscription covers all nested fields
- Fires one message per struct change (whole struct value)
- `loadSymbols: true` adds a one-time symbol table download on connect — may cause brief real-time jitter on the PLC
- Use when you want the full struct as a JSON object rather than per-field messages

### Comparison

| | Option A (dot-notation) | Option B (whole struct) |
|---|---|---|
| `loadSymbols` required | No | Yes |
| Output per change | One primitive per field | Whole struct as JSON |
| Symbol table download | No | Yes (on connect) |
| PLC jitter risk | None | Brief on connect |
| Granularity | Per-field | Per-struct |

## Transmission Modes

> **Note:** `transmissionMode` only applies when `readType` is `notification`. When using `readType: interval`, the plugin sends plain ADS Read commands to the PLC at each interval — no notification mechanism is involved, and `transmissionMode` is ignored.

The `transmissionMode` field controls how the PLC's internal notification handler sends updates back to the client. The available modes are:

| Mode | Value | Description |
|------|-------|-------------|
| `serverOnChange` | 4 | **(Default)** The PLC scans for changes at the configured `cycleTime` interval and sends a notification only when the value has changed. This is the most efficient mode for most use cases. |
| `serverCycle` | 3 | The PLC sends the current value at every `cycleTime` interval, regardless of whether the value has changed. Useful when you need a constant data stream or heartbeat. |
| `serverOnChange2` | 6 | Enhanced version of `serverOnChange` available on newer TwinCAT 3 firmware. Supports more efficient internal handling on the PLC side. **Automatically falls back** to `serverOnChange` on older PLCs. |
| `serverCycle2` | 5 | Enhanced version of `serverCycle` available on newer TwinCAT 3 firmware. Same behavior as `serverCycle` but with improved internal efficiency. **Automatically falls back** to `serverCycle` on older PLCs. |

**Choosing a mode:**
- Use `serverOnChange` (default) for event-driven data where you only care about changes
- Use `serverCycle` when you need periodic snapshots regardless of changes
- The `2` variants (`serverOnChange2`, `serverCycle2`) can be used safely on any PLC — the plugin automatically detects older PLCs and falls back to the v1 equivalent

## Explanation of cycleTime and maxDelay

**cycleTime** controls how often the PLC checks the variable:
- `serverCycle` mode: PLC sends a notification every `cycleTime` regardless of value change
- `serverOnChange` mode: PLC checks the value every `cycleTime` and sends a notification only if it changed

**maxDelay** controls how long the PLC can buffer notifications before sending:
- The PLC collects notification events and sends them in a batch when `maxDelay` expires
- This is a network optimization: fewer packets, multiple notifications bundled in one AMS packet

**Practical example** — `cycleTime: 10ms`, `maxDelay: 100ms`, mode `serverOnChange`:
1. PLC checks variable every 10ms
2. If value changed, queues a notification
3. Sends queued notifications at most every 100ms (batched)

**Edge cases:**
- `maxDelay: 0s` — send immediately, no batching
- `cycleTime: 0s` — check as fast as the PLC task cycle allows
- `maxDelay` < `cycleTime` — effectively no batching (fires before next check)

Think of it as:
- **cycleTime** = polling interval (sensor sampling rate)
- **maxDelay** = delivery batch window (network efficiency)

**Important:** If a variable changes faster than `cycleTime`, intermediate values are missed:

```
cycleTime = 1000ms, mode = serverOnChange

Time:    0ms    200ms   400ms   600ms   800ms   1000ms
Value:   5  →   10  →   3   →   7   →   2   →   8
PLC checks:  ↑                                    ↑
Notifies: 5                                       8  (missed 10,3,7,2)
```

The PLC only samples at `cycleTime` intervals. Between checks, it is blind — this is not a continuous event stream.

For fast-changing values, set `cycleTime` close to the PLC task cycle time (typically 1–10ms). The trade-off is more CPU load on the PLC and more network traffic. Even at minimum `cycleTime`, there is no guarantee of capturing every value — if a variable changes twice within one PLC scan cycle, the intermediate value is lost. ADS notifications are polling with push delivery, not event capture.

```yaml
input:
  ads:
    transmissionMode: "serverOnChange"     # default, sends only on value change
    # transmissionMode: "serverCycle"      # sends at every cycle regardless of change
    # transmissionMode: "serverOnChange2"  # enhanced, auto-falls back on older PLCs
    # transmissionMode: "serverCycle2"     # enhanced cyclic, auto-falls back on older PLCs
```

## Interval vs Notification

The `interval` and `notification` read types can produce similar-looking results (periodic data), but they work differently under the hood:

- **`interval`**: The client polls the PLC — sends an ADS Read request for each symbol at every `intervalTime` interval. Simple, no PLC notification overhead, and not subject to the ~500-notification limit.
- **`notification` + `serverOnChange`**: The PLC pushes data only when a value changes. Most efficient for event-driven data. Subject to the ~500-notification limit per connection.
- **`notification` + `serverCycle`**: The PLC pushes data at every `cycleTime` interval regardless of changes. Similar result to `interval` but PLC-driven — more precise timing with no request/response overhead per cycle. Subject to the ~500-notification limit.

| Aspect | `interval` | `notification` + `serverOnChange` | `notification` + `serverCycle` |
|--------|-----------|-----------------------------------|-------------------------------|
| Who drives | Client polls | PLC pushes on change | PLC pushes on timer |
| Network per cycle | Request + response | Push only | Push only |
| Sends unchanged values | Yes | No | Yes |
| Timing precision | Subject to network latency | PLC real-time task | PLC real-time task |
| PLC notification limit | No limit | ~500 max | ~500 max |
| Best for | Large symbol lists, simple setup | Event-driven data (most use cases) | Precise periodic sampling |

## Route Registration

The plugin can automatically register a route on the PLC using the Beckhoff UDP route protocol (port 48899). This removes the need to manually add routes in TwinCAT System Manager.

**Activation:** both `username` and `password` must be set.

**How it works:**
1. The TCP connection to port 48898 is established
2. The plugin probes the PLC with a lightweight ADS command to check if a route already exists
3. If the probe succeeds, route registration is skipped (route already present)
4. If the probe fails, the plugin sends a UDP registration packet to port 48899: "Associate AMS NetID X with IP address Y"
5. After registration the TCP connection is re-established (some PLCs close connections from previously-unknown NetIDs)
6. On reconnect after a network loss, the same probe-first logic runs automatically

**Parameters:**
- `username` / `password`: PLC administrator credentials — same as used in TwinCAT System Manager to add routes
- `hostIP`: IP address the PLC associates with this client. In Docker with bridge networking, set to the Docker host's IP on the PLC network. Auto-detected from the outbound connection if empty (only correct with `host_network` or macvlan)

**Network requirements:**
- UDP port 48899 must be reachable on the PLC from the client (for route registration)
- TCP port 48898 must be reachable on the PLC from the client (outbound — works through any NAT)

## Notification Behavior

### First batch completeness

When `readType: notification`, TwinCAT sends an initial sample for every subscribed symbol immediately on registration (all modes except `NoTransmission`). The plugin waits for these initial samples before returning from `Connect`, so the **first `ReadBatch` always returns a complete batch** containing one message per successfully registered symbol. No separate read or warm-up period is needed to get the current state of all symbols.

### Partial registration failures

If a symbol fails to register (unknown name, PLC-side ADS error), the plugin:
- Logs an **error** through the Benthos logger identifying the symbol and reason
- Continues with the remaining symbols — data flows for all successfully registered symbols
- Does **not** trigger a reconnect for partial failures; only a full failure (zero symbols registered) forces a reconnect

This means a misconfigured symbol name is surfaced immediately in logs without blocking data from the other symbols.

### Interval read — empty batches during reconnect

When `readType: interval`, the first one or two batches after a reconnect may be **empty or partial** while the go-ads library re-resolves symbol handles. This is normal — Benthos retries the next poll and subsequent batches are complete. No action is needed; the gap is typically one poll interval (default 1 second).

## Reconnection

The plugin automatically reconnects when the TCP connection is lost (e.g. network cable unplugged, PLC restart). Aggressive TCP keepalive probes detect dead connections within ~13 seconds. On reconnect, the plugin:
1. Re-establishes the TCP connection (retries indefinitely with 5s interval)
2. Reloads the symbol table from the PLC
3. Re-subscribes all notification handles

No manual intervention is needed.

## Output

Each symbol produces a single message with the payload being the value read from the PLC. Use `meta("symbol_name")` in a following benthos bloblang processor to distinguish messages.

## Testing

Tested and verified on hardware:

### CX7000, TwinCAT 3
- Notifications from Docker container with bridge networking (no host_network)
- Automatic UDP route registration from Docker bridge networking
- Static route with explicit hostAMS (no route registration)
- Reconnection after network loss with automatic notification re-subscribe
- Sum/batch commands for read, add notification, and delete notification
- CycleTime verification: 50ms, 200ms, 1000ms intervals match configured values
- Deterministic value verification against PLC cycle counter formulas

### CX1020, TwinCAT 2
- Read batches, add notifications, different cycle times and max delay
- Different datatypes: INT, INT16, UINT, DINT, BOOL, STRUCT, and more
- Automatic fallback from sum commands to individual calls
- Automatic fallback from v2 transmission modes to v1
- Reconnection after network loss with automatic notification re-subscribe
- CycleTime verification and deterministic value verification
