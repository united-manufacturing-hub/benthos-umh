# How it works

Background that makes the settings make sense. None of it is required reading to get data flowing;
[Quick start](quick-start.md) is enough for that.

## How ADS works

Four ideas cover almost everything you need to configure this input.

**A router, not a device.** Every TwinCAT system runs an AMS router, and you always talk to the
router rather than to the PLC program directly. It forwards each request to one of the devices behind it
(the PLC runtime, the I/O layer, the logger), selected by a port number. `851` is the first PLC
runtime on TwinCAT 3, `801` on TwinCAT 2.

**Two addresses, not one.** The IP address gets you to the router; an **AMS NetID** (six numbers,
usually written like an IP with `.1.1` appended) identifies the system at either end. Your request
carries both your own NetID and the target's. The plugin asks the PLC for its NetID on connect, so
normally only the IP needs configuring.

**A route authorises you.** The PLC answers only clients it has a route for: an entry pairing your
NetID with the address it expects you to come from. Routes live on the PLC, survive reboots, and
can be created by the plugin itself (`username` + `password`) or by hand in TwinCAT. If a request
arrives with no matching route, the connection is accepted and then ignored. That is the most
common cause of an input that looks connected but reads nothing. See
[How a route is matched](#how-a-route-is-matched).

**One connection, two ways to read.** Everything runs over a single outbound TCP connection to port
48898, and normally no inbound port is needed. On that connection you either poll (`readType: interval`) or
subscribe and let the PLC push values when they change (`readType: notification`, the default).
Use `interval` when the symbol list is long enough to pass the PLC's notification limit, and
`notification` when values change less often than you would otherwise poll. Symbols are addressed by name, so `GVL_ProcessData.nMasterCycleCounter`
is all you need; the PLC resolves it to a handle for you.

## Getting the PLC to accept this client

A route has to exist on the PLC before any data flows, and there are three ways to create one: the
TwinCAT Connection Manager on a Windows host, a static route added in the TwinCAT System Manager or
XAE, or automatic registration by the plugin itself from `username` and `password`. Automatic
registration also needs `hostIP`, because that becomes the address stored in the route.
[Networking](networking.md) has the procedure for each, and
[How a route is matched](#how-a-route-is-matched) explains what the PLC checks.

## How a route is matched

An AMS route pairs an AMS NetID with an address, and the runtime uses both. The NetID selects the route, and it must equal the source NetID the client advertises. The address is checked against the **source IP the runtime observes**. If they disagree, the TCP connection stays up and the requests are silently dropped: no error, no reply. Replies to requests that do pass come back over the connection the client opened.

```mermaid
flowchart LR
  C["Client<br/>NetID 192.168.1.50.1.1<br/>IP 192.168.1.50"]
  subgraph PLC["PLC"]
    R["AMS router"]
    T["Route<br/>NetId 192.168.1.50.1.1<br/>Address 192.168.1.50"]
    Q{"Address = source IP<br/>the PLC sees?"}
  end
  OK["Reply on the client's<br/>own connection"]
  NO["Request dropped,<br/>no reply, connection stays up"]
  C -->|"request on TCP 48898"| R
  R -->|"look up by source NetID"| T
  T --> Q
  Q -->|yes| OK
  Q -->|no| NO
  OK --> C
```

`NetId` only has to match what the client advertises (`hostAMS`) and need not resemble the address.
`Address` must equal the source IP the PLC observes. Note *observes*, not *can reach*.

Replies and notifications normally arrive over the connection the client opened, so the input works
from a Docker container with no inbound path and no listening ports.

A device can instead answer only on a connection it opens back to the client on TCP `48898`. The
plugin accepts that connection and reads the replies from it, so there is nothing to configure. Two
things still have to hold: `48898` must be free on the client host, and the PLC must be able to
reach it. If the port is already taken, the plugin logs a warning that names it, and that device
cannot be read.

Routes are bidirectional by default. Leave that alone: with `Unidirectional` ticked, the runtime
stops accepting ADS calls from the system at the far end of the route, which is this client, so it
refuses the plugin's own requests.

A wrong `Address` produces a session that connects and registers successfully and then fails,
usually as repeated request timeouts rather than a connection error.

Automatic registration derives both fields from `hostIP` (`Address` = `hostIP`, `NetId` = `hostIP`
+ `.1.1`). Firmwares differ in what they store when `hostIP` is left empty. Some record the source
address they observe, others the value the client advertises, so set `hostIP` explicitly or add
the route manually.

## Notifications or polling

The `interval` and `notification` read types can produce similar-looking results (periodic data), but they work differently under the hood:

- **`interval`**: The client polls the PLC every `intervalTime`, reading the whole symbol list in one
  sum command. No PLC notification overhead, and the 550-notification limit does not apply.
- **`notification` + `serverOnChange`**: The PLC pushes data only when a value changes. Sends nothing while the value is unchanged. Counts against the 550-notification limit.
- **`notification` + `serverCycle`**: The PLC pushes data at every `cycleTime` interval regardless of changes. Similar result to `interval` but PLC-driven: more precise timing, with no request/response overhead per cycle. Counts against the 550-notification limit.

| Aspect | `interval` | `notification` + `serverOnChange` | `notification` + `serverCycle` |
|--------|-----------|-----------------------------------|-------------------------------|
| Who drives | Client polls | PLC pushes on change | PLC pushes on timer |
| Network per cycle | Request + response | Push only | Push only |
| Sends unchanged values | Yes | No | Yes |
| Timing precision | Subject to network latency | PLC real-time task | PLC real-time task |
| PLC notification limit | No limit | 550 per device | 550 per device |
| Best for | Large symbol lists, simple setup | Event-driven data (most use cases) | Precise periodic sampling |

## Choosing cycleTime and maxDelay

**cycleTime** controls how often the PLC checks the variable:
- `serverCycle` mode: PLC sends a notification every `cycleTime` regardless of value change
- `serverOnChange` mode: PLC checks the value every `cycleTime` and sends a notification only if it changed

**maxDelay** controls how long the PLC can buffer notifications before sending:
- The PLC collects notification events and sends them in a batch when `maxDelay` expires
- This is a network optimization: fewer packets, multiple notifications bundled in one AMS packet

**Practical example**, `cycleTime: 10ms`, `maxDelay: 100ms`, mode `serverOnChange`:
1. PLC checks variable every 10ms
2. If value changed, queues a notification
3. Sends queued notifications at most every 100ms (batched)

**Edge cases:**
- `maxDelay: 0s`: send immediately, no batching
- `cycleTime: 0s`: check as fast as the PLC task cycle allows
- `maxDelay` < `cycleTime`: effectively no batching (fires before next check)

Think of it as:
- **cycleTime** = polling interval (sensor sampling rate)
- **maxDelay** = delivery batch window (network efficiency)

**Important:** If a variable changes faster than `cycleTime`, intermediate values are missed:

```text
cycleTime = 1000ms, mode = serverOnChange

Time:    0ms    200ms   400ms   600ms   800ms   1000ms
Value:   5  →   10  →   3   →   7   →   2   →   8
PLC checks:  ↑                                    ↑
Notifies: 5                                       8  (missed 10,3,7,2)
```

The PLC only samples at `cycleTime` intervals. Between checks it is blind. This is not a continuous event stream.

For fast-changing values, set `cycleTime` close to the PLC task cycle time (typically 1–10ms). The trade-off is more CPU load on the PLC and more network traffic. Even at minimum `cycleTime`, there is no guarantee of capturing every value: if a variable changes twice within one PLC scan cycle, the intermediate value is lost. ADS notifications are polling with push delivery, not event capture.

Which of the four transmission modes applies, and what the `2` variants change, is in
[Transmission modes](configuration.md#transmission-modes).

## What to expect from notifications

### First batch completeness

When `readType: notification`, TwinCAT sends an initial sample for every subscribed symbol immediately on registration (all modes except `NoTransmission`). The plugin waits for these initial samples before returning from `Connect`, so the **first `ReadBatch` always returns a complete batch** containing one message per successfully registered symbol. No separate read or warm-up period is needed to get the current state of all symbols.

### The connection heartbeat

A runtime restart or a CONFIG toggle can empty the PLC's notification table without dropping the
TCP connection, which would otherwise leave the input connected and silent. So with
`readType: notification` the plugin subscribes to one extra cyclic notification of its own, on the
PLC's symbol-version group, and treats those beats as the sign the runtime is still serving
notifications. It arrives every 2 s by default, carries one byte, and counts as one of the PLC's
550 notifications.

`notificationSilenceTimeout` sets how much silence is tolerated, and is converted into a number of
beats: 10 s against a 2 s cycle means five missed beats mark the subscriptions dead. What happens
then is `heartbeatRecovery`, see
[Silent notification delivery](troubleshooting.md#silent-notification-delivery). A PLC that refuses
the subscription is logged as a warning and still serves data; only the ability to notice a silent
death is lost.

`readType: interval` has no heartbeat, and needs none: a runtime that stops answering fails the
next read.

### Partial registration failures

If a symbol fails to register (unknown name, PLC-side ADS error), the plugin:
- Logs an **error** through the Benthos logger identifying the symbol and reason
- Continues with the remaining symbols, so data still flows for every symbol that did register
- Does **not** trigger a reconnect for partial failures; only a full failure (zero symbols registered) forces a reconnect

This means a misconfigured symbol name is surfaced immediately in logs without blocking data from the other symbols.

### Interval read: empty batches during reconnect

When `readType: interval`, the first one or two batches after a reconnect may be **empty or partial** while symbol handles are re-resolved. This is normal, Benthos retries the next poll and subsequent batches are complete. No action is needed; the gap is typically one poll interval (default 1 second).
