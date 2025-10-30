# Bug: OPC UA browse deadlocks at 100k nodes due to channel buffer limit

**Parent Issue:** ENG-3799 (OPC UA deadband filtering)
**Discovered:** 2025-10-30 during deadband scale testing
**Status:** New Bug - Unrelated to deadband feature
**Priority:** High - Blocks large-scale OPC UA deployments
**Labels:** bug, opcua, performance, scalability

## Problem

When browsing 100k+ OPC UA nodes, the system deadlocks with:
- Channel stuck at exactly 100,000 tags
- Browse goroutine blocked (trying to send node 100,001)
- Zero subscriptions created
- No forward progress (stuck for 5+ minutes)

## Root Cause

`MaxTagsToBrowse = 100_000` hardcoded constant in `opcua_plugin/core_browse.go:31` creates a channel buffer of exactly 100k:

```go
// opcua_plugin/core_browse.go:31
const MaxTagsToBrowse = 100_000

// Used throughout:
nodeChan := make(chan NodeDef, MaxTagsToBrowse)
opcuaBrowserChan := make(chan BrowseDetails, MaxTagsToBrowse)
```

When browse finds 100k+ nodes:
1. Browse goroutine fills channel to 100,000 capacity
2. Browse goroutine blocks trying to send node #100,001
3. Subscription creation doesn't drain the channel (or drains too slowly)
4. **Deadlock** - no progress possible

## Evidence

### Logs showing deadlock
```
time="2025-10-30T15:44:49" msg="Please note that browsing large node trees can take some time"
time="2025-10-30T15:44:59" msg="Amount of found opcua tags currently in channel: 61778, (1 active browse goroutines)"
time="2025-10-30T15:45:09" msg="Amount of found opcua tags currently in channel: 100000, (1 active browse goroutines)"
time="2025-10-30T15:45:19" msg="Amount of found opcua tags currently in channel: 100000, (1 active browse goroutines)"
time="2025-10-30T15:45:29" msg="Amount of found opcua tags currently in channel: 100000, (1 active browse goroutines)"
...
[Stuck at 100,000 for 5+ minutes with no change]
```

### OPC-PLC server status
```bash
$ docker logs opc-plc 2>&1 | grep subscriptions
# Open/total subscriptions: 0 | 0
```

Zero subscriptions created despite benthos being "connected".

### Process status
```bash
$ ps aux | grep benthos
jeremytheocharis 34734   0.0  1.1 412411216 202928   ??  S     3:44PM   0:30.52 ./tmp/bin/benthos
```

Process alive (202MB memory) but making no forward progress.

## Impact

- **Blocker for 100k+ node deployments**
- Works fine at 1k (✅) and 10k (✅) scales
- Does NOT affect deadband filtering feature (PR #223)
- Deadband feature works perfectly at supported scales

## Test Results

| Scale | Nodes | Duration | Messages | Throughput | Subscriptions | Result |
|-------|-------|----------|----------|------------|---------------|--------|
| Phase 1 | 1,000 | 90s | 53,697 | ~596/sec | 1,030 | ✅ SUCCESS |
| Phase 2 | 10,000 | 180s | 826,479 | ~4,591/sec | 10,026 | ✅ SUCCESS |
| Phase 3 | 100,000 | N/A | 0 | 0/sec | 0 | ❌ DEADLOCK |

**Linear scaling observed up to 10k nodes**, then complete failure at 100k.

## Reproduction Steps

```bash
# 1. Start OPC-PLC with 100k nodes
docker run -d --name opc-plc -p 50000:50000 \
  mcr.microsoft.com/iotedge/opc-plc:2.12.29 \
  --pn=50000 --sn=50000 --sr=10 --fn=50000 --fr=1 \
  --unsecuretransport

# 2. Create benthos config
cat > config.yaml <<EOF
input:
  opcua:
    endpoint: "opc.tcp://localhost:50000"
    nodeIDs: ["ns=3;s=OpcPlc"]
    subscribeEnabled: true
    securityMode: "None"
    securityPolicy: "None"
output:
  drop: {}
logger:
  level: INFO
EOF

# 3. Run benthos and observe deadlock
./benthos run config.yaml

# Expected: Channel fills to 100k then deadlocks
# Observed: No subscriptions created, browse goroutine stuck
```

## Proposed Solutions

### Option 1: Make MaxTagsToBrowse configurable
```go
// Add to config struct
MaxTagsToBrowse int `yaml:"maxTagsToBrowse" default:"100000"`

// Use in channel creation
nodeChan := make(chan NodeDef, g.MaxTagsToBrowse)
```

**Pros:** Simple, backwards compatible, allows power users to scale
**Cons:** Users must understand their limits, still has hard cap

### Option 2: Implement unbounded channel with backpressure
```go
// Use unbounded channel with worker pool
nodeChan := make(chan NodeDef)  // Unbounded

// Implement backpressure via semaphore
sem := make(chan struct{}, MaxConcurrentSubscriptions)
```

**Pros:** No hard limit, automatic flow control
**Cons:** More complex, potential memory issues if subscriptions are slow

### Option 3: Start subscription creation during browse
```go
// Don't wait for browse to complete
// Start draining channel while browse is still running
go createSubscriptions(nodeChan)  // Runs concurrently with browse
go browseNodes(nodeChan)           // Producer
```

**Pros:** Solves deadlock, better resource utilization
**Cons:** More complex goroutine coordination, need proper shutdown

### Recommendation: Option 3 + Option 1

1. **Immediate fix:** Start subscription creation concurrently with browse (Option 3)
2. **Long-term:** Make `MaxTagsToBrowse` configurable (Option 1)

This gives both correctness (no deadlock) and flexibility (user control).

## Code Locations

- **Constant definition:** `opcua_plugin/core_browse.go:31`
- **Channel creation:**
  - `opcua_plugin/core_browse_frontend.go:60-62`
  - `opcua_plugin/read_discover.go:38-42`
  - `opcua_plugin/read.go:276`

## Related Issues

- Parent: ENG-3799 (OPC UA deadband filtering)
- Related to historical 10k node limit discussions

## Testing Notes

- Phase 1 (1k) and Phase 2 (10k) tests confirm deadband feature works correctly
- PR #223 is valid and ready for merge (bug is separate)
- 100k scale bug discovered during testing but not caused by deadband changes
- OPC-PLC needs `--unsecuretransport` flag for SecurityMode=None connections
