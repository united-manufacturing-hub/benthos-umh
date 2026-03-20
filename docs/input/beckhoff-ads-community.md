# Beckhoff ADS (community)

Input for Beckhoff's ADS protocol. Supports batch reading and notifications. Beckhoff recommends limiting notifications to approximately 500 to avoid overloading the controller.\
This input only supports symbols and not direct addresses.

This plugin is community supported only. If you encounter any issues, check out the [original repository](https://github.com/RuneRoven/benthosADS) for more information, or ask around in our Discord.

```yaml
input:
  ads:
    cycleTime: 1000                   # Optional, default: 1000
    hostAMS: auto                     # Optional, default: auto
    intervalTime: 1000                # Optional, default: 1000
    logLevel: disabled                # Optional, default: disabled
    maxDelay: 100                     # Optional, default: 100
    readType: notification            # Optional, default: notification
    routeHostAddress: 192.168.1.123   # Optional, auto detected. Usually required when using docker.
    routePassword: "1"                # Optional, default: "". Required if using automatic UDP route registration on the PLC
    routeUsername: Administrator      # Optional, default: "". Required if using automatic UDP route registration on the PLC
    runtimePort: 801                  # Optional, default: 801 for old TwinCAT 2
    symbols:
      - MAIN.MYTRIGGER:0:10           # variable in the main program with 0ms max delay and 10ms cycle time
      - MAIN.myInt                    # variable in the main program, uses default maxDelay and cycleTime
      - ".superDuperInt"              # global variable (must start with `.`)
    targetAMS: 5.3.12.111.1.1         # Required, AMS net ID of the target.
    targetIP: 192.168.3.70            # Required, IP address of the Beckhoff PLC
    targetPort: 48898                 # Optional, default: 48898
    transmissionMode: serverOnChange  # Optional, default: serverOnChange
    upperCase: true                   # Optional, default: true
```
```yaml
pipeline:
  processors:
    - tag_processor:
        defaults: |-
          // Minimal example
          msg.meta.location_path = "beckhoff.twincat.plc";
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name      = msg.meta.symbol_name;
          // tag_processor now auto-creates msg.meta.umh_topic
          return msg;
```
```yaml
output:
  uns: {}
```

## Connection to ADS
When connecting to an ADS device you connect to a router which then routes the traffic to the correct device using the AMS net ID.
There are basically 3 ways for setting up the connection:

1. **TwinCAT Connection Manager**: Use the TwinCAT connection manager locally on the host, scan for the device and add a connection using the correct credentials for the PLC.
2. **Static route on PLC**: Log in to the PLC using the TwinCAT system manager and add a static route from the PLC to the client. This is the preferred way when using benthos on a Kubernetes cluster since you have no good way of installing the connection manager.
3. **Automatic route registration (UDP)**: Use the `routeUsername` and `routePassword` config fields to have the plugin automatically register a route on the PLC before connecting. See the [Route Registration](#route-registration) section below.

### Docker and Kubernetes

ADS works from inside Docker containers with default bridge networking — **no `host_network`, no port forwarding, and no open ports are needed**. All ADS traffic (requests, responses, and notifications) flows over a single outbound TCP connection to port 48898. The PLC never initiates connections back to the client; it sends all responses and notifications on the same TCP socket the client opened.

The only requirement is that the `hostAMS` value matches a route registered on the PLC. When running in Docker with bridge networking:
- **`routeHostAddress` must be set** to the Docker host's IP on the PLC network (e.g. `192.168.1.50`). This tells the PLC which IP address to associate with the route. If left empty, it auto-detects the container's bridge IP which is not routable from the PLC.
- **`hostAMS` can be set explicitly** to `routeHostAddress` + `.1.1` (e.g. `192.168.1.50.1.1`), or left as `auto` — when route registration is configured with `routeHostAddress`, `auto` will correctly derive the AMS NetID from `routeHostAddress` instead of the container's bridge IP.
- **A route must exist on the PLC** for the `hostAMS` NetID. This can be added manually in TwinCAT System Manager, or automatically via the `routeUsername`/`routePassword` config fields.
- **`hostPort` is optional** (default 10500). It is a logical AMS port used in protocol headers, not a network port. Any value works.

**Option A: Automatic route registration (recommended)**

The plugin registers a route on the PLC automatically via UDP before connecting. No manual PLC configuration needed:

```yaml
input:
  ads:
    targetIP: '192.168.1.100'
    targetAMS: '192.168.1.100.1.1'
    runtimePort: 851
    hostAMS: 'auto'                      # Derives AMS NetID from routeHostAddress
    routeUsername: 'Administrator'        # Triggers automatic route registration
    routePassword: '1'
    routeHostAddress: '192.168.1.50'     # Docker HOST IP (required in bridge networking)
    readType: 'notification'
    symbols:
      - "MAIN.MyVariable"
```

You can also set `hostAMS` explicitly if you prefer:

```yaml
    hostAMS: '192.168.1.50.1.1'          # Explicit: Docker HOST IP + .1.1
    routeHostAddress: '192.168.1.50'     # Must match
```

**Option B: Static route on PLC**

If you prefer not to use automatic registration, add a static route on the PLC via TwinCAT System Manager pointing to the Docker host's IP. Then configure `hostAMS` to match — no `routeUsername`/`routePassword` needed:

```yaml
input:
  ads:
    targetIP: '192.168.1.100'
    targetAMS: '192.168.1.100.1.1'
    runtimePort: 851
    hostAMS: '192.168.1.50.1.1'         # Must match the route on the PLC
    readType: 'notification'
    symbols:
      - "MAIN.MyVariable"
```

**Option C: host_network or macvlan**

When the container has a routable IP on the PLC network, `hostAMS: auto` works without `routeHostAddress`:

```yaml
input:
  ads:
    targetIP: '192.168.1.100'
    targetAMS: '192.168.1.100.1.1'
    runtimePort: 851
    hostAMS: 'auto'                     # Auto-derive from container's real IP
    routeUsername: 'Administrator'       # Optional: auto-register route
    routePassword: '1'
    readType: 'notification'
    symbols:
      - "MAIN.MyVariable"
```

#### Configuration Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| **targetIP** | Yes | — | IP address of the Beckhoff PLC |
| **targetAMS** | Yes | — | AMS net ID of the target |
| **symbols** | Yes | — | List of symbols to read from (see [Symbols Format](#symbols-format) below) |
| **targetPort** | No | `48898` | Port of the target internal gateway |
| **runtimePort** | No | `801` | Runtime port of PLC system, 800–899. TwinCAT 2 uses 800–850 (usually 801), TwinCAT 3 uses 851–899 (usually 851) |
| **hostAMS** | No | `auto` | Host AMS net ID. Usually the IP address + `.1.1`. Must match a route on the PLC. `auto` derives it from `routeHostAddress` if set, otherwise from the outbound connection's local IP |
| **hostPort** | No | `10500` | AMS source port used in protocol headers. This is a logical port, not a network port. Any arbitrary value works |
| **readType** | No | `notification` | Read type for the symbols. `interval` polls at `intervalTime`; `notification` uses PLC push updates (see [Interval vs Notification](#interval-vs-notification)) |
| **maxDelay** | No | `100` | Default max delay for sending notifications in ms. Maximum time after value change before PLC must send the notification |
| **cycleTime** | No | `1000` | Default cycle time for notification handler in ms. How often the PLC scans for changes. Use a low value for triggers that are only true/false for 1 PLC cycle |
| **intervalTime** | No | `1000` | Interval time between reads in ms (only used when `readType` is `interval`) |
| **requestTimeout** | No | `5000` | Timeout for individual ADS requests in ms. Increase for slow PLCs or large symbol tables |
| **transmissionMode** | No | `serverOnChange` | Notification transmission mode. Only applies when `readType` is `notification`. Options: `serverOnChange`, `serverCycle`, `serverOnChange2`, `serverCycle2` (see [Transmission Modes](#transmission-modes)) |
| **upperCase** | No | `true` | Convert symbol names to all uppercase. Often necessary for TwinCAT 2 PLCs |
| **logLevel** | No | `disabled` | Log level for ADS connection (`disabled`, `error`, `warn`, `info`, `debug`, `trace`). At `debug`/`trace`, ADS error codes show human-readable descriptions |
| **routeUsername** | No | `""` | Username for automatic UDP route registration on the PLC. If set, a route is registered before connecting (see [Route Registration](#route-registration)) |
| **routePassword** | No | `""` | Password for automatic UDP route registration on the PLC |
| **routeHostAddress** | No | `""` | IP address the PLC associates with the route. Required in Docker bridge networking (set to Docker host's IP). When `hostAMS` is `auto`, the AMS NetID is also derived from this. Auto-detected from outbound connection if empty (only correct with `host_network` or macvlan) |

##### Symbols Format

Symbols are specified in the format `function.variable:maxDelay:cycleTime`:
- `MAIN.MYBOOL` — variable in the main program, uses default maxDelay and cycleTime
- `MAIN.MYTRIGGER:0:10` — variable in the main program with 0ms max delay and 10ms cycle time
- `.superDuperInt` — global variable (must start with `.`)


#### Transmission Modes

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

```yaml
input:
  ads:
    transmissionMode: 'serverOnChange'   # default, sends only on value change
    # transmissionMode: 'serverCycle'    # sends at every cycle regardless of change
    # transmissionMode: 'serverOnChange2' # enhanced, auto-falls back on older PLCs
    # transmissionMode: 'serverCycle2'    # enhanced cyclic, auto-falls back on older PLCs
```

#### Interval vs Notification

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

#### Route Registration

The plugin can automatically register a route on the PLC using the Beckhoff UDP route protocol (port 48899). This removes the need to manually add routes in the TwinCAT System Manager.

**How it works:**
1. Before establishing the TCP connection, the plugin sends a UDP route registration packet to port 48899 on the PLC
2. The packet tells the PLC: "Associate AMS NetID X with IP address Y"
3. The PLC adds this as a runtime route (may not be visible in TwinCAT System Manager)
4. The normal ADS TCP connection is then established over port 48898

Setting `routeUsername` activates automatic route registration. If route registration fails (e.g. UDP response lost due to NAT), the plugin logs a warning and still attempts the TCP connection — the route may have been created despite the missing response.

**Parameters:**
- `routeUsername` / `routePassword`: PLC administrator credentials. Same as used in TwinCAT System Manager to add routes
- `routeHostAddress`: The IP address the PLC associates with this client. In Docker with bridge networking, this must be set to the Docker host's IP on the PLC network. When `hostAMS` is `auto`, the AMS NetID is derived from this address. Auto-detected if empty (only correct with `host_network` or macvlan)

**Network requirements:**
- UDP port 48899 must be reachable on the PLC from the client (for route registration)
- TCP port 48898 must be reachable on the PLC from the client (outbound — works through any NAT)

#### Reconnection

The plugin automatically reconnects when the TCP connection is lost (e.g. network cable unplugged, PLC restart). Aggressive TCP keepalive probes detect dead connections within ~13 seconds. On reconnect, the plugin:
1. Re-establishes the TCP connection (retries indefinitely with 5s interval)
2. Reloads the symbol table from the PLC
3. Re-subscribes all notification handles

No manual intervention is needed.

#### Output

This outputs for each address a single message with the payload being the value that was read. To distinguish messages, you can use meta("symbol_name") in a following benthos bloblang processor.

## Testing

Tested and verified:
### CX7000, TwinCAT 3
- Notifications from Docker container with bridge networking (no host_network)
- Automatic UDP route registration from Docker bridge networking
- Static route with explicit hostAMS (no route registration)
- Reconnection after network loss with automatic notification re-subscribe
- Sum/batch commands for read, add notification, and delete notification

### CX1020, TwinCAT 2
- Read batches, Add notifications, different cycle times and max delay
- Different datatypes, INT, INT16, UINT, DINT, BOOL, STRUCT, and more
- Automatic fallback from sum commands to individual calls
- Automatic fallback from v2 transmission modes to v1
- Reconnection after network loss with automatic notification re-subscribe
