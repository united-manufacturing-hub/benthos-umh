# Configuration

Every setting this input accepts, and two complete examples that use all of them.

## Settings

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| **targetAddress** | Yes | — | IP address (and optional port) of the PLC's ADS gateway, as `ip` or `ip:port`. Port defaults to `48898` if omitted |
| **targetAMS** | No | discovered | AMS net ID of the target runtime. Left empty, the plugin asks the PLC for its own on connect, so it normally does not need setting. Set it only to pin a specific runtime; a mismatch with what the PLC reports is logged as a warning, not an error |
| **unifiedAddress** | No\* | `[]` | Symbols to read, in unified address form (see [Symbols Format](symbols-and-metadata.md#addressing-symbols) below) |
| **symbols** | No\* | `[]` | Symbols to read by PLC symbol name, same format and parsing rules as `unifiedAddress`. Appended after `unifiedAddress` at connect time; use as an alternative when symbol names don't come from a shared unified-address list |
| **loadSymbols** | No | `false` | Download the PLC's symbol and datatype table on connect. Required to read a struct or array as one value, which arrives as nested JSON. A named member (`GVL.stStatus.fValue`) resolves without it. The download can cause brief real-time jitter on the PLC, so it is off by default |
| **runtimePort** | No | `851` | AMS port of the PLC runtime. `851` is the first TwinCAT 3 runtime and `801` the first TwinCAT 2 one, so a TwinCAT 2 PLC has to set this. `0` asks the PLC for its TwinCAT version and uses the first runtime of that version. A later runtime (`811`, `821` on TwinCAT 2; `852`, `853` on TwinCAT 3) always has to be set explicitly |
| **hostAMS** | No | `auto` | Host AMS net ID. Usually the IP address + `.1.1`. Must match a route on the PLC. `auto` derives it from `hostIP` if set, otherwise from the outbound connection's local IP |
| **hostPort** | No | `0` | AMS source port in protocol headers. `0` uses a random port per session (recommended, avoids notification handle conflicts across sessions). Set a fixed value only in firewalled environments with port allow-lists |
| **readType** | No | `notification` | Read type for the symbols. `interval` polls at `intervalTime`; `notification` uses PLC push updates (see [Interval vs Notification](how-it-works.md#notifications-or-polling)). Accepted values: `notification`, `interval` |
| **maxDelay** | No | `100ms` | Maximum time the PLC batches notifications before sending (e.g. `100ms`, `0s`). All changes are still delivered; this controls delivery latency vs network efficiency. `0s` = send immediately. |
| **cycleTime** | No | `100ms` | How often the PLC checks the symbol for changes (e.g. `100ms`, `10ms`, `1s`). Lower = more responsive but more PLC CPU. Values faster than the PLC task cycle are clamped to the task cycle. |
| **intervalTime** | No | `1s` | Interval time between reads (e.g. `1s`, `500ms`). Only used when `readType` is `interval` |
| **requestTimeout** | No | `5s` | Timeout for individual ADS requests (e.g. `5s`, `10s`). Increase for slow PLCs or large symbol tables |
| **transmissionMode** | No | `serverOnChange` | Notification transmission mode. Only applies when `readType` is `notification`. Accepted values: `serverOnChange`, `serverCycle`, `serverOnChange2`, `serverCycle2` (see [Transmission modes](#transmission-modes)) |
| **username** | No | `""` | Username for automatic UDP route registration on the PLC. Both `username` and `password` must be set to activate registration. Requires UDP port 48899 to be reachable (see [Route Registration](networking.md#automatic-route-registration)) |
| **password** | No | `""` | Password for automatic UDP route registration on the PLC |
| **maxReconnectInterval** | No | `30s` | Upper bound on the reconnect backoff, and on the cooldown applied to a connection that keeps flapping. Every reconnect costs the PLC an accepted socket, which the default protects; lower it for a device that resets on a timer anyway and where samples matter more than sockets (see [Reconnection](troubleshooting.md#reconnection)). `0s` keeps the library default |
| **routeActivationTimeout** | No | `10s` | How long to wait, after registering a route, for the PLC's AMS router to start serving it. The router acknowledges the registration before the entry is necessarily live, and until it is, requests are dropped with no reply. Raise it for a PLC or router under heavy load. `0s` keeps the library default |
| **notificationSilenceTimeout** | No | `10s` | How long the plugin's [connection heartbeat](how-it-works.md#the-connection-heartbeat) may be silent before the subscriptions are treated as dead and re-registered. Converted internally into a number of missed beats. Only applies when `readType` is `notification`. `0s` keeps the library default |
| **heartbeatRecovery** | No | `immediate` | What to do when notification delivery goes silent; see [Silent notification delivery](troubleshooting.md#silent-notification-delivery). Accepted values: `immediate`, `confirm`, `rebuild` |
| **hostIP** | No | `""` | IP address the PLC associates with the route. It must be **the address the PLC sees this client as**, which behind a NATing gateway is not the client's own address. See [PLC behind a NATing gateway](networking.md#plc-behind-a-nating-gateway-or-subnet-router). Required in Docker bridge networking on TwinCAT 2, where replies are routed via this address (set to Docker host's IP); optional on TwinCAT 3. When `hostAMS` is `auto`, the AMS NetID is also derived from this. Auto-detected from outbound connection if empty (only correct with `host_network` or macvlan) |

\* At least one of `unifiedAddress` or `symbols` must be non-empty; the config is rejected otherwise.

ADS library log verbosity follows the pipeline log level (`logger.level`); there is no separate setting.

## Transmission modes

> **Note:** `transmissionMode` only applies when `readType` is `notification`. When using `readType: interval`, the plugin sends plain ADS Read commands to the PLC at each interval, no notification mechanism is involved, and `transmissionMode` is ignored.

The `transmissionMode` field controls how the PLC's internal notification handler sends updates back to the client. The available modes are:

| Mode | Value | Description |
|------|-------|-------------|
| `serverOnChange` | 4 | Default. The PLC checks the value every `cycleTime` and sends a notification only when it changed. The check runs in the target's default task. |
| `serverCycle` | 3 | The PLC sends the current value every `cycleTime`, whether or not it changed. Use for a steady stream or a heartbeat. |
| `serverOnChange2` | 6 | `OnChangeInContext`: the same check, bound to the task that owns the variable instead of the default task, which makes the timing task-synchronous. Falls back to `serverOnChange` for any symbol whose ContextMask is 0 |
| `serverCycle2` | 5 | `CyclicInContext`: as `serverCycle`, bound to the task that owns the variable. Falls back to `serverCycle` for any symbol whose ContextMask is 0 |

**Choosing a mode:**
- Use `serverOnChange` (default) for event-driven data where only changes matter
- Use `serverCycle` for periodic snapshots regardless of changes
- The `2` variants are safe to set on any PLC, because a symbol with ContextMask 0 falls back to the
  v1 equivalent. They only change behaviour for variables local to a PROGRAM POU in a multi-task
  project; a GVL variable or a single-task project always has ContextMask 0. Beckhoff describes
  these as [`CyclicInContext` and `OnChangeInContext`](https://infosys.beckhoff.com/content/1033/tcadsnetref/7313078411.html) and advises against them in the default
  case

## Full example: TwinCAT 3

All fields with TC3 defaults and comments:

```yaml
input:
  ads:
    # Target connection
    targetAddress: "192.168.1.100:48898"   # PLC IP[:port]; port defaults to 48898
    targetAMS: ""                          # empty = ask the PLC for its own NetID
    runtimePort: 851                       # TC3 runtime port (TC2: 801)
    # Local AMS identity
    hostAMS: "auto"                        # auto = derived from hostIP, else the TCP source IP
    hostPort: 0                            # 0 = random per session (recommended)
    hostIP: "192.168.1.50"                 # the address the PLC sees this client as
    # Route registration: both must be set to activate
    username: "Administrator"
    password: "1"
    # Read mode
    readType: "notification"               # notification | interval
    transmissionMode: "serverOnChange"     # default; the 2 variants need a task-bound symbol
    cycleTime: 100ms                       # how often the PLC checks for changes
    maxDelay: 100ms                        # max batching window before the PLC sends
    intervalTime: 1s                       # poll interval (readType: interval only)
    # Symbols
    loadSymbols: true                      # download the symbol table; needed for structs and arrays
    # Timeouts and recovery
    requestTimeout: 5s                     # per-request timeout; raise on slow links
    maxReconnectInterval: 30s              # cap on the reconnect backoff and flap cooldown
    routeActivationTimeout: 10s            # wait for a new route to start being served
    notificationSilenceTimeout: 10s        # silence tolerated before subscriptions count as dead
    heartbeatRecovery: "immediate"         # immediate | confirm | rebuild
    # Symbols to read (list last for readability)
    unifiedAddress:
      - "GVL_ProcessData.nMasterCycleCounter"
      - "GVL_ProcessData.stMachineStatus.stMotor1.fSpeed"
      - "GVL_ProcessData.anCounters[0]"
      - "MAIN.MyTrigger:maxDelay=0s:cycleTime=10ms"  # per-symbol overrides
```

## Full example: TwinCAT 2

All fields with TC2 differences highlighted:

```yaml
input:
  ads:
    # Target connection
    targetAddress: "192.168.1.200:48898"   # PLC IP[:port]; port defaults to 48898
    targetAMS: ""                          # empty = ask the PLC for its own NetID
    runtimePort: 801                       # TC2 runtime port
    # Local AMS identity
    hostAMS: "auto"
    hostPort: 0
    hostIP: "192.168.1.50"                 # the address the PLC sees this client as
    # Route registration, both must be set to activate
    username: "Administrator"
    password: "1"
    # Read mode
    readType: "notification"
    transmissionMode: "serverOnChange"     # the v2 modes would fall back to this anyway
    cycleTime: 100ms
    maxDelay: 100ms
    intervalTime: 1s                       # readType: interval only
    # Symbols
    loadSymbols: true
    # Timeouts and recovery
    requestTimeout: 5s
    maxReconnectInterval: 30s
    routeActivationTimeout: 10s
    notificationSilenceTimeout: 10s
    heartbeatRecovery: "immediate"         # immediate | confirm | rebuild
    # Symbols to read. TC2 globals use a dot prefix, program variables an uppercase prefix
    unifiedAddress:
      - ".nMasterCycleCounter"
      - ".stMachineStatus.stMotor1.fSpeed"
      - "PRG_DIAGNOSTICS.nInt"
      - ".myTrigger:maxDelay=0s:cycleTime=10ms"
```

## Supported hardware

Verified on the following controllers:

| Controller | TwinCAT | Runtime port |
|---|---|---|
| CX7000 (TC/RTOS, ARMv7) | 3.1.4026 | 851 |
| CX8190 | 3.1.4024 | 851 |
| CX1020 | 2.10.1320, 2.10.1341, 2.11.2234 | 801 |

Each device reports its own model, TwinCAT version and NetID over UDP 48899 before any route
exists. That is why `targetAMS` can be left empty, and what `runtimePort: 0` uses to pick the
first runtime of the reported version.

### TwinCAT 3 (4024 and 4026)
- Notifications from a Docker container with bridge networking (no `host_network`, no published ports)
- Automatic UDP route registration from Docker bridge networking
- Static route with explicit `hostAMS` and no route registration
- Reconnection after network loss, with automatic notification re-subscribe and symbol reload
- Sum/batch commands for read, add notification, and delete notification
- `cycleTime` verification: 50ms, 200ms and 1000ms intervals match the configured values
- Deterministic value verification against the PLC's own cycle-counter formulas
- Recovery from a route whose `Address` the PLC could not reach

### TwinCAT 2 (2.10 and 2.11)
- Read batches, notifications, and a range of `cycleTime` / `maxDelay` combinations
- Datatypes: BOOL, SINT, INT, DINT, UDINT, REAL, LREAL, STRING, TIME, DATE, DT, TOD, enums, structs and arrays
- Automatic fallback from sum commands to individual calls, and from `SumReadEx2` to `SumReadEx`
- Automatic fallback from v2 transmission modes to v1
- Reconnection after network loss with automatic notification re-subscribe
- `cycleTime` verification and deterministic value verification
- TC2 symbol naming: GVL globals with a leading dot, program locals in uppercase

## Migrating from the community plugin

This input replaces the community-supported `ads` plugin. The input name is unchanged, so an
existing configuration keeps loading, but several field names changed, and a config using the old
ones fails to start:

| Community plugin | This input |
|---|---|
| `targetIP` + `targetPort` | `targetAddress`, as `ip` or `ip:port` |
| `routeUsername` | `username` |
| `routePassword` | `password` |
| `routeHostAddress` | `hostIP` |
| `logLevel` | removed; the pipeline's `logger.level` applies |
| `symbols` | still accepted; `unifiedAddress` is the preferred name |
| `targetAMS` | unchanged, and now optional: left empty, the plugin asks the PLC for its NetID |
| `runtimePort` | unchanged, and still `851` by default, so a TwinCAT 2 PLC still needs `801` |
| `hostAMS`, `hostPort`, `readType`, `transmissionMode`, `cycleTime`, `maxDelay`, `intervalTime`, `loadSymbols` | unchanged |

Metadata field names changed too: `symbol_name` is now `ads_symbol_name`, so a `tag_processor`
carrying `msg.meta.tag_name = msg.meta.symbol_name` needs updating. See
[Symbols and metadata](symbols-and-metadata.md#metadata-outputs) for the full set.
