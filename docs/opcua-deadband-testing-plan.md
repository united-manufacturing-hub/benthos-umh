# OPC UA Deadband Testing Plan with opc-plc

## Overview

This document provides a comprehensive testing strategy for validating Fix 3 Task 4 (Server compatibility validation with 100k tags) using the Microsoft opc-plc simulator. The goal is to verify that the OPC UA deadband filtering implementation works correctly at scale and measure performance impact.

**Testing objectives:**
1. Validate server compatibility with DataChangeFilter at 100k+ nodes
2. Measure notification reduction effectiveness (target: 50-70%)
3. Verify no StatusCode errors in MonitoredItemCreateRequest responses
4. Measure CPU/memory usage and browse/subscribe performance
5. Confirm filtering is server-side (not client-side fallback)

## opc-plc Server Setup

### Docker Configuration

The opc-plc server is Microsoft's official OPC UA test server optimized for IoT Edge scenarios.

**Docker image:** `mcr.microsoft.com/iotedge/opc-plc:2.12.29`

**Port mappings:**
- `50000`: OPC UA endpoint (opc.tcp://localhost:50000)
- `8080`: HTTP metrics/health endpoint

### Node Generation for 100k Tags

opc-plc generates nodes dynamically based on command-line parameters. For 100k node testing, use a mix of fast and slow nodes:

**Recommended configuration:**
```bash
docker run -d \
  --name opc-plc-100k \
  --hostname localhost \
  -p 50000:50000 \
  -p 8080:8080 \
  mcr.microsoft.com/iotedge/opc-plc:2.12.29 \
  --pn=50000 \
  --autoaccept \
  --sph \
  --sn=50000 --sr=10 --st=double --str=true \
  --fn=50000 --fr=1 --ft=double \
  --certdnsnames=localhost \
  --plchostname=localhost \
  --unsecuretransport \
  --maxsessioncount=100 \
  --maxsubscriptioncount=100
```

**Parameter explanation:**

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `--pn=50000` | 50000 | Server port |
| `--autoaccept` | - | Auto-accept client certificates (for testing) |
| `--sph` | - | Show PLC hostname in logs |
| `--sn=50000` | 50000 | Number of slow-changing nodes |
| `--sr=10` | 10 | Slow nodes update every 10 seconds |
| `--st=double` | double | Slow nodes are float64 values |
| `--str=true` | true | Enable randomization (triggers deadband) |
| `--fn=50000` | 50000 | Number of fast-changing nodes |
| `--fr=1` | 1 | Fast nodes update every 1 second |
| `--ft=double` | double | Fast nodes are float64 values |
| `--certdnsnames=localhost` | localhost | Certificate DNS name |
| `--plchostname=localhost` | localhost | PLC hostname |
| `--unsecuretransport` | - | Allow connections without encryption (testing only) |
| `--maxsessioncount=100` | 100 | Max parallel sessions |
| `--maxsubscriptioncount=100` | 100 | Max subscriptions per session |

**Why this configuration:**
- 50k slow nodes (10s interval) + 50k fast nodes (1s interval) = 100k total tags
- Randomization (`--str=true`) ensures value changes trigger deadband evaluation
- Double datatype allows testing both absolute and percent deadband
- Fast nodes generate high notification rate for measuring deadband effectiveness

### Node Structure

opc-plc generates nodes under this hierarchy:

```
Root (i=84)
└── Objects (i=85)
    └── OpcPlc (ns=3;s=OpcPlc)
        ├── Telemetry
        │   ├── Basic (ns=3;s=Basic)
        │   │   ├── AlternatingBoolean
        │   │   ├── RandomSignedInt32
        │   │   ├── RandomUnsignedInt32
        │   │   └── StepUp
        │   ├── Fast (ns=3;s=Fast)
        │   │   ├── FastUInt1
        │   │   ├── FastUInt2
        │   │   ├── ...
        │   │   └── FastUInt[N] (N = --fn value)
        │   ├── Slow (ns=3;s=Slow)
        │   │   ├── SlowUInt1
        │   │   ├── SlowUInt2
        │   │   ├── ...
        │   │   └── SlowUInt[N] (N = --sn value)
        │   ├── Anomaly (ns=3;s=Anomaly)
        │   └── Special (ns=3;s=Special)
        └── ...
```

**Namespaces:**
- `ns=0`: OPC UA standard namespace
- `ns=1`: Server application namespace
- `ns=2`: OPC Foundation namespace
- `ns=3`: opc-plc application namespace (where telemetry nodes live)

**Root node for browsing:** `ns=3;s=OpcPlc`

### Alternative Configuration: Focused Testing

For faster iteration during development, test with smaller node counts first:

```bash
docker run -d \
  --name opc-plc-1k \
  --hostname localhost \
  -p 50000:50000 \
  -p 8080:8080 \
  mcr.microsoft.com/iotedge/opc-plc:2.12.29 \
  --pn=50000 \
  --autoaccept \
  --sph \
  --sn=500 --sr=10 --st=double --str=true \
  --fn=500 --fr=1 --ft=double \
  --certdnsnames=localhost \
  --plchostname=localhost \
  --unsecuretransport
```

**1k nodes (500 slow + 500 fast):**
- Faster browse time (~10-30 seconds)
- Easier to debug issues
- Still demonstrates deadband effectiveness
- Use for rapid development/debugging

**Scale up progression:**
- 1k nodes → validate basic functionality
- 10k nodes → validate filtering effectiveness
- 100k nodes → validate server compatibility and performance

## Benthos Configuration

### Test Configuration YAML

Create `config/opcua-deadband-100k-test.yaml`:

```yaml
input:
  opcua:
    endpoint: "opc.tcp://localhost:50000"

    # Start with root node for full browse
    nodeIDs:
      - "ns=3;s=OpcPlc"

    # Enable subscription for DataChangeFilter testing
    subscribeEnabled: true

    # Deadband configuration (CHANGE THESE FOR EACH TEST SCENARIO)
    deadbandType: "absolute"  # or "percent" or "none"
    deadbandValue: 0.5        # absolute: 0.5 units, percent: 0.5%

    # Queue size (1 = only latest value)
    queueSize: 1

    # Sampling interval (1000ms = 1 second)
    samplingInterval: 1000

    # Security (optional, testing only)
    securityMode: "None"
    securityPolicy: "None"

    # Username/password (if needed)
    username: ""
    password: ""

# Output to file for analysis
output:
  file:
    path: "./opcua-deadband-test-${DEADBAND_TYPE}-${DEADBAND_VALUE}.ndjson"
    codec: lines

# Logging configuration
logger:
  level: INFO
  add_timestamp: true
  json_format: false
  static_fields:
    '@service': "opcua-deadband-test"

# Metrics for monitoring
metrics:
  prometheus:
    push_url: ""
    push_interval: "10s"
```

### Running Tests

**1. Start opc-plc server:**
```bash
# For 100k nodes
docker run -d --name opc-plc-100k --hostname localhost -p 50000:50000 -p 8080:8080 \
  mcr.microsoft.com/iotedge/opc-plc:2.12.29 \
  --pn=50000 --autoaccept --sph \
  --sn=50000 --sr=10 --st=double --str=true \
  --fn=50000 --fr=1 --ft=double \
  --certdnsnames=localhost --plchostname=localhost --unsecuretransport

# Wait for server startup (30 seconds)
sleep 30

# Verify server is running
docker logs opc-plc-100k | grep "OPC UA Server started"
```

**2. Run benthos test:**
```bash
# Baseline: No deadband
export DEADBAND_TYPE=none
export DEADBAND_VALUE=0
./benthos run config/opcua-deadband-100k-test.yaml

# Test 1: Absolute deadband
export DEADBAND_TYPE=absolute
export DEADBAND_VALUE=0.5
./benthos run config/opcua-deadband-100k-test.yaml

# Test 2: Percent deadband
export DEADBAND_TYPE=percent
export DEADBAND_VALUE=2.0
./benthos run config/opcua-deadband-100k-test.yaml
```

**3. Monitor performance:**
```bash
# CPU/Memory usage
docker stats opc-plc-100k

# Benthos metrics (if prometheus enabled)
curl http://localhost:4195/stats

# opc-plc health endpoint
curl http://localhost:8080/health
```

## Testing Scenarios

### Scenario 1: Baseline (No Deadband)

**Goal:** Measure notification rate without filtering to establish baseline.

**Configuration:**
```yaml
deadbandType: "none"
deadbandValue: 0
```

**Expected behavior:**
- All value changes generate notifications
- High notification rate (one notification per node update)
- Establishes maximum throughput for comparison

**Metrics to collect:**
- Notifications per second
- Network bandwidth (MB/s)
- Browse time (seconds)
- Subscribe time (seconds)
- CPU usage (%)
- Memory usage (MB)

**Success criteria:**
- ✅ Browse completes in <5 minutes (100k nodes)
- ✅ Subscribe completes without errors
- ✅ Notifications received for all node updates
- ✅ CPU usage remains <80%
- ✅ Memory stable (no leaks)

**Expected results:**
For 100k nodes (50k fast@1s + 50k slow@10s):
- Fast nodes: 50,000 notifications/sec
- Slow nodes: 5,000 notifications/sec (50k / 10s)
- Total: ~55,000 notifications/sec baseline

### Scenario 2: Absolute Deadband

**Goal:** Validate absolute deadband filtering reduces notification rate.

**Configuration:**
```yaml
deadbandType: "absolute"
deadbandValue: 0.5  # Only report changes >0.5 units
```

**Test procedure:**
1. Configure absolute deadband (0.5 units)
2. Browse and subscribe to ns=3;s=Fast (50k fast nodes)
3. Monitor notifications for 60 seconds
4. Calculate notification reduction percentage

**Expected behavior:**
- Only value changes >0.5 units trigger notifications
- Randomized values should still trigger notifications (randomization range spans >0.5)
- Steady-state values (no change or small changes) filtered out

**Metrics to collect:**
- Notifications per second (compared to baseline)
- Notification reduction percentage
- CPU usage (should be lower than baseline)
- Memory usage (should be stable)

**Success criteria:**
- ✅ Notification rate reduced by 30-70% (depends on randomization range)
- ✅ No StatusCode errors in MonitoredItemCreateResult
- ✅ CPU usage lower than baseline
- ✅ Memory stable (no leaks)
- ✅ Notifications only for changes exceeding 0.5 units (verify with sample data)

**Expected results:**
- Notification rate: 15,000-35,000 notifications/sec (50-70% reduction)
- StatusCodes: All Good (0x00000000)
- FilterResult: Populated with actual deadband values

### Scenario 3: Percent Deadband

**Goal:** Validate percent deadband filtering with EURange metadata.

**Configuration:**
```yaml
deadbandType: "percent"
deadbandValue: 2.0  # Only report changes >2% of EURange
```

**Note:** opc-plc may not set EURange on all nodes. If EURange not available, percent deadband falls back to absolute:
- 2% of 0-100 range = 2 units (if EURange = [0, 100])
- Without EURange, behaves like absolute deadband

**Test procedure:**
1. Configure percent deadband (2.0%)
2. Browse and subscribe to ns=3;s=Fast
3. Inspect MonitoredItemCreateResult for FilterResult
4. Verify calculated thresholds match expected percentages

**Expected behavior:**
- Server calculates absolute threshold: `threshold = (EUHigh - EULow) * (deadbandValue / 100)`
- Example: For EURange [0, 100] and deadbandValue=2.0, threshold = 2.0 units
- Only value changes exceeding calculated threshold trigger notifications

**Metrics to collect:**
- Calculated thresholds from FilterResult
- Notification rate (compared to absolute deadband)
- EURange metadata presence (check if opc-plc provides it)

**Success criteria:**
- ✅ FilterResult shows calculated thresholds
- ✅ Notification rate similar to absolute deadband (if EURange consistent)
- ✅ No StatusCode errors
- ✅ Thresholds match expected calculation (EURange * deadbandValue / 100)

**Expected results:**
- If EURange available: Notification rate depends on value range
- If EURange missing: Behaves like absolute deadband with same threshold

### Scenario 4: Server Compatibility Validation

**Goal:** Verify opc-plc supports DataChangeFilter and doesn't reject filter parameters.

**Configuration:**
```yaml
deadbandType: "absolute"
deadbandValue: 0.5
```

**Test procedure:**
1. Enable DEBUG logging in benthos
2. Subscribe with absolute deadband filter
3. Capture MonitoredItemCreateRequest/Response messages
4. Inspect StatusCode and FilterResult in response

**Expected behavior:**
- Server accepts DataChangeFilter in MonitoredItemCreateRequest
- MonitoredItemCreateResult.StatusCode = Good (0x00000000)
- FilterResult contains actual filter parameters used by server
- Filtering occurs server-side (not client-side fallback)

**What to check:**
```go
// In MonitoredItemCreateResult:
StatusCode == ua.StatusOK  // 0x00000000
FilterResult != nil        // Server processed filter
FilterResult.DeadbandType  // Should match request (absolute=1)
FilterResult.DeadbandValue // Should match request (0.5)
```

**Validation checklist:**
- [ ] StatusCode = Good (0x00000000) for all monitored items
- [ ] FilterResult populated (not null)
- [ ] FilterResult.DeadbandType matches request
- [ ] FilterResult.DeadbandValue matches request
- [ ] No "BadFilterNotAllowed" errors
- [ ] No "BadMonitoredItemFilterUnsupported" errors

**Success criteria:**
- ✅ All 100k nodes accept DataChangeFilter
- ✅ No StatusCode errors
- ✅ FilterResult confirms server-side filtering
- ✅ Notification rate reduced (proves server honors filter)

**Failure indicators:**
- ❌ StatusCode = BadFilterNotAllowed (0x80440000)
- ❌ StatusCode = BadMonitoredItemFilterUnsupported (0x80450000)
- ❌ FilterResult is null (server rejected filter)
- ❌ Notification rate unchanged (server ignores filter)

### Scenario 5: Performance Impact Measurement

**Goal:** Measure CPU/memory overhead of deadband filtering at 100k scale.

**Test matrix:**

| Test | Deadband Type | Nodes | Duration | Metrics |
|------|---------------|-------|----------|---------|
| 5a   | None          | 100k  | 5 min    | Baseline CPU/Memory |
| 5b   | Absolute      | 100k  | 5 min    | Filtered CPU/Memory |
| 5c   | Percent       | 100k  | 5 min    | Filtered CPU/Memory |

**Monitoring commands:**
```bash
# Start monitoring
docker stats opc-plc-100k --no-stream --format "{{.CPUPerc}},{{.MemUsage}}" > metrics.csv &

# Run test
./benthos run config/opcua-deadband-100k-test.yaml

# Stop monitoring
killall docker
```

**Expected results:**

| Scenario | CPU Usage | Memory Usage | Notification Rate |
|----------|-----------|--------------|-------------------|
| Baseline (None) | 40-60% | 500-800 MB | 55,000/sec |
| Absolute Deadband | 30-50% | 500-800 MB | 15,000-35,000/sec |
| Percent Deadband | 30-50% | 500-800 MB | 15,000-35,000/sec |

**Success criteria:**
- ✅ CPU usage acceptable (<80%) during operation
- ✅ Memory stable (no leaks over 5 minutes)
- ✅ Deadband reduces CPU load (less notification processing)
- ✅ Browse time <5 minutes (100k nodes)
- ✅ Subscribe time <3 minutes (100k nodes)

### Scenario 6: Edge Case Testing

**Goal:** Test edge cases and error conditions.

**Test cases:**

**6a. Invalid deadband values:**
```yaml
deadbandType: "absolute"
deadbandValue: -1.0  # Negative value (invalid)
```
Expected: Configuration error or server rejection (BadInvalidArgument)

**6b. Very small deadband:**
```yaml
deadbandType: "absolute"
deadbandValue: 0.0001  # Very small threshold
```
Expected: High notification rate (most changes exceed threshold)

**6c. Very large deadband:**
```yaml
deadbandType: "absolute"
deadbandValue: 1000000.0  # Very large threshold
```
Expected: Very low notification rate (few changes exceed threshold)

**6d. Percent deadband without EURange:**
```yaml
deadbandType: "percent"
deadbandValue: 2.0
# Node doesn't have EURange metadata
```
Expected: Fallback to absolute deadband (threshold = 2.0 units)

**6e. Rapid subscribe/unsubscribe:**
```bash
# Subscribe and immediately disconnect
timeout 5s ./benthos run config/opcua-deadband-100k-test.yaml
# Repeat 10 times to check for memory leaks
```
Expected: Clean subscription cleanup, no memory leaks

## Metrics Collection

### Performance Metrics

**Browse metrics:**
- Total nodes discovered: 100,000 (target)
- Browse time: <5 minutes (acceptable)
- Browse errors: 0 (required)

**Subscribe metrics:**
- Nodes subscribed: 100,000 (target)
- Subscribe time: <3 minutes (acceptable)
- MonitoredItem errors: 0 (required)
- StatusCode errors: 0 (required)

**Runtime metrics:**
- Notification rate (notifications/sec):
  - Baseline (no deadband): 55,000/sec
  - With deadband: 15,000-35,000/sec (50-70% reduction)
- CPU usage (%):
  - Server: <80%
  - Client: <60%
- Memory usage (MB):
  - Server: 500-1000 MB
  - Client: 200-500 MB
- Network bandwidth (MB/s):
  - Baseline: ~20-30 MB/s
  - With deadband: ~7-15 MB/s

### Functional Metrics

**Filter acceptance:**
- Nodes with filter accepted: 100,000 / 100,000 (100%)
- StatusCode = Good: 100,000 / 100,000 (100%)
- FilterResult populated: 100,000 / 100,000 (100%)

**Deadband effectiveness:**
- Notification reduction (%): 50-70% (target)
- False positives (notifications below threshold): <1%
- False negatives (missed notifications above threshold): 0%

**Accuracy validation:**
Sample 100 random notifications and verify:
- Absolute deadband: |newValue - previousValue| >= deadbandValue
- Percent deadband: |newValue - previousValue| >= (EURange * deadbandValue / 100)

## Validation Criteria

### Success Criteria

**Functional requirements:**
- ✅ Browse completes in <5 minutes (100k nodes)
- ✅ Subscribe completes without errors (100k nodes)
- ✅ Deadband reduces notifications by 50-70%
- ✅ No StatusCode errors in MonitoredItemCreateResult
- ✅ FilterResult confirms server-side filtering
- ✅ Notification accuracy: <1% error rate

**Performance requirements:**
- ✅ CPU usage acceptable during operation (<80%)
- ✅ Memory stable (no leaks over 5+ minutes)
- ✅ Network bandwidth reduced proportionally with notifications

**Compatibility requirements:**
- ✅ opc-plc accepts DataChangeFilter (StatusCode = Good)
- ✅ opc-plc supports both absolute and percent deadband
- ✅ opc-plc provides FilterResult in response

### Failure Indicators

**Critical failures (block release):**
- ❌ Server rejects filter (BadFilterNotAllowed)
- ❌ No notification reduction (server ignores filter)
- ❌ Browse timeout or failure (can't discover 100k nodes)
- ❌ Memory leak during operation
- ❌ CPU usage >95% sustained

**Non-critical issues (investigate but don't block):**
- ⚠️ Browse time >5 minutes (slow but functional)
- ⚠️ Notification reduction <50% (less effective but working)
- ⚠️ Some nodes missing EURange (percent deadband fallback)
- ⚠️ CPU usage 80-95% (high but acceptable)

## Troubleshooting

### Common Issues

**1. opc-plc not generating 100k nodes**

Symptoms:
- Browse finds fewer nodes than expected
- Node count doesn't match --sn + --fn parameters

Solution:
```bash
# Check container logs
docker logs opc-plc-100k | grep "nodes initialized"

# Verify command-line arguments
docker inspect opc-plc-100k | grep -A 10 "Args"

# Try smaller node count first (1k) to validate config
docker run -d --name opc-plc-1k --hostname localhost -p 50000:50000 \
  mcr.microsoft.com/iotedge/opc-plc:2.12.29 \
  --pn=50000 --autoaccept --sn=500 --fn=500 --st=double --ft=double
```

**2. Connection refused**

Symptoms:
- benthos cannot connect to opc.tcp://localhost:50000
- "connection refused" error

Solution:
```bash
# Verify port mapping
docker ps | grep opc-plc

# Check if server is listening
netstat -an | grep 50000

# Try SecurityMode: None (eliminate certificate issues)
securityMode: "None"
securityPolicy: "None"

# Check container health
curl http://localhost:8080/health
```

**3. Browse timeout**

Symptoms:
- Browse operation takes >10 minutes or times out
- "context deadline exceeded" errors

Solution:
```bash
# Increase browse timeout in benthos config
# (Note: May require code change to expose timeout config)

# Try browsing smaller subtrees
nodeIDs:
  - "ns=3;s=Fast"  # Only fast nodes (50k)

# Check server resource limits
docker stats opc-plc-100k

# Reduce node count for faster iteration
--sn=10000 --fn=10000  # 20k total instead of 100k
```

**4. Server ignores deadband**

Symptoms:
- Notification rate unchanged with deadband enabled
- FilterResult is null in MonitoredItemCreateResult

Solution:
```bash
# Enable DEBUG logging
logger:
  level: DEBUG

# Check FilterResult in logs
grep "FilterResult" benthos.log

# Verify server capabilities
# Browse to Server.ServerCapabilities.OperationLimits
# Check MaxMonitoredItemsPerCall, etc.

# Try different deadband values
deadbandValue: 1.0  # Larger threshold = easier to observe
```

**5. Memory leak during operation**

Symptoms:
- Memory usage increases continuously
- System becomes unresponsive after extended run

Solution:
```bash
# Monitor memory with docker stats
docker stats opc-plc-100k --no-stream

# Check for subscription cleanup issues
# Stop and restart benthos, verify memory returns to baseline

# Enable Go profiling (benthos)
make serve-pprof
# Browse to http://localhost:6060/debug/pprof/heap

# Try smaller node count to isolate issue
--sn=1000 --fn=1000  # If problem disappears, it's scale-related
```

**6. StatusCode errors**

Symptoms:
- MonitoredItemCreateResult contains StatusCode != Good
- Some nodes fail to subscribe

Solution:
```bash
# Capture StatusCodes from benthos logs
grep "StatusCode" benthos.log | grep -v "Good"

# Common errors:
# - BadMonitoredItemIdInvalid: Wrong NodeID format
# - BadNodeIdUnknown: Node doesn't exist
# - BadFilterNotAllowed: Server doesn't support deadband

# Check which nodes are failing
# Add logging in CreateMonitoredItems() to print failed NodeIDs

# Verify node structure with UaExpert or similar tool
```

## Test Execution Checklist

**Pre-test setup:**
- [ ] Build benthos binary: `make target`
- [ ] Create test config: `config/opcua-deadband-100k-test.yaml`
- [ ] Start opc-plc container with 100k nodes
- [ ] Verify server running: `curl http://localhost:8080/health`
- [ ] Verify port accessible: `telnet localhost 50000`

**Scenario 1: Baseline (No Deadband)**
- [ ] Configure deadbandType="none"
- [ ] Run benthos for 60 seconds
- [ ] Collect notification rate (notifications/sec)
- [ ] Collect CPU/memory usage
- [ ] Verify browse completed (<5 min)
- [ ] Verify subscribe completed (<3 min)

**Scenario 2: Absolute Deadband**
- [ ] Configure deadbandType="absolute", deadbandValue=0.5
- [ ] Run benthos for 60 seconds
- [ ] Collect notification rate (expect 50-70% reduction)
- [ ] Verify StatusCodes = Good (all 100k nodes)
- [ ] Verify FilterResult populated
- [ ] Spot-check notification accuracy (sample 100)

**Scenario 3: Percent Deadband**
- [ ] Configure deadbandType="percent", deadbandValue=2.0
- [ ] Run benthos for 60 seconds
- [ ] Collect notification rate
- [ ] Check FilterResult for calculated thresholds
- [ ] Verify EURange metadata presence (if applicable)
- [ ] Compare results with absolute deadband

**Scenario 4: Server Compatibility**
- [ ] Enable DEBUG logging
- [ ] Subscribe with deadband filter
- [ ] Capture MonitoredItemCreateRequest/Response
- [ ] Verify StatusCode = Good for all items
- [ ] Verify FilterResult confirms server-side filtering
- [ ] Confirm notification rate reduced (proves filtering active)

**Scenario 5: Performance Impact**
- [ ] Run 5-minute test with no deadband (baseline)
- [ ] Run 5-minute test with absolute deadband
- [ ] Run 5-minute test with percent deadband
- [ ] Compare CPU/memory/bandwidth across scenarios
- [ ] Verify memory stable (no leaks)

**Scenario 6: Edge Cases**
- [ ] Test invalid deadband value (-1.0)
- [ ] Test very small deadband (0.0001)
- [ ] Test very large deadband (1000000.0)
- [ ] Test percent deadband without EURange
- [ ] Test rapid subscribe/unsubscribe (10 iterations)
- [ ] Verify clean cleanup and no memory leaks

**Post-test analysis:**
- [ ] Calculate notification reduction percentages
- [ ] Generate performance comparison graphs
- [ ] Document any failures or unexpected behavior
- [ ] Update this document with actual results

## Results Documentation Template

After running tests, document results in this format:

```markdown
## Test Results: [Date] - [Benthos Version] - [opc-plc Version]

### Environment
- OS: [macOS/Linux/Windows]
- Docker version: [version]
- opc-plc version: mcr.microsoft.com/iotedge/opc-plc:2.12.29
- Benthos version: [version from git describe]
- Node configuration: 50k slow (10s) + 50k fast (1s) = 100k total

### Scenario 1: Baseline (No Deadband)
- Browse time: [X] seconds
- Subscribe time: [Y] seconds
- Notification rate: [Z] notifications/sec
- CPU usage: [%]
- Memory usage: [MB]

### Scenario 2: Absolute Deadband (0.5 units)
- Notification rate: [Z] notifications/sec
- Reduction: [%] compared to baseline
- StatusCode errors: [N] / 100,000
- FilterResult populated: [N] / 100,000
- CPU usage: [%]
- Memory usage: [MB]
- Sample accuracy: [%] (100 random samples)

### Scenario 3: Percent Deadband (2.0%)
- Notification rate: [Z] notifications/sec
- Reduction: [%] compared to baseline
- EURange metadata: [present/missing]
- FilterResult thresholds: [values]
- StatusCode errors: [N] / 100,000

### Scenario 4: Server Compatibility
- Filter acceptance: [N] / 100,000 nodes
- StatusCode = Good: [N] / 100,000 nodes
- FilterResult present: [N] / 100,000 nodes
- Server-side filtering: [confirmed/not confirmed]

### Scenario 5: Performance Impact
- CPU (no deadband): [%]
- CPU (absolute deadband): [%]
- CPU (percent deadband): [%]
- Memory leak detected: [yes/no]
- Network bandwidth reduction: [%]

### Scenario 6: Edge Cases
- Invalid value handling: [pass/fail]
- Very small threshold: [pass/fail]
- Very large threshold: [pass/fail]
- Missing EURange fallback: [pass/fail]
- Rapid subscribe/unsubscribe: [pass/fail]

### Overall Assessment
- ✅ / ❌ Browse performance acceptable
- ✅ / ❌ Subscribe performance acceptable
- ✅ / ❌ Deadband effectiveness demonstrated
- ✅ / ❌ Server compatibility confirmed
- ✅ / ❌ Resource usage acceptable
- ✅ / ❌ No memory leaks detected

### Issues Encountered
1. [Issue description]
   - Symptoms: [what happened]
   - Solution: [how it was resolved]
2. [...]

### Recommendations
- [Recommendation 1]
- [Recommendation 2]
```

## References

### OPC UA Specification
- OPC UA Part 4: Services (DataChangeFilter definition)
- OPC UA Part 8: Data Access (EURange, deadband semantics)

### opc-plc Documentation
- GitHub: https://github.com/Azure-Samples/iot-edge-opc-plc
- Docker Hub: mcr.microsoft.com/iotedge/opc-plc
- Microsoft Learn: https://learn.microsoft.com/en-us/samples/azure-samples/iot-edge-opc-plc/azure-iot-sample-opc-ua-server/

### benthos-umh OPC UA
- OPC UA input documentation: `docs/input/opc-ua-input.md`
- Test files: `opcua_plugin/opcua_opc-plc_test.go`
- Docker Compose: `tests/docker-compose.yaml`

### Related Skills
- OPC UA Certificate Troubleshooting: `~/umh-git/superpowers-skills/skills/protocols/opcua/SKILL.md`
- Scale Game: `~/umh-git/superpowers-skills/skills/problem-solving/scale-game/SKILL.md`
- Test-Driven Development: `~/umh-git/superpowers-skills/skills/testing/test-driven-development/SKILL.md`

## Appendix: Command Reference

### Quick Commands

**Start opc-plc (100k nodes):**
```bash
docker run -d --name opc-plc-100k --hostname localhost -p 50000:50000 -p 8080:8080 \
  mcr.microsoft.com/iotedge/opc-plc:2.12.29 \
  --pn=50000 --autoaccept --sph \
  --sn=50000 --sr=10 --st=double --str=true \
  --fn=50000 --fr=1 --ft=double \
  --certdnsnames=localhost --plchostname=localhost --unsecuretransport
```

**Start opc-plc (1k nodes, fast iteration):**
```bash
docker run -d --name opc-plc-1k --hostname localhost -p 50000:50000 -p 8080:8080 \
  mcr.microsoft.com/iotedge/opc-plc:2.12.29 \
  --pn=50000 --autoaccept --sph \
  --sn=500 --sr=10 --st=double --str=true \
  --fn=500 --fr=1 --ft=double \
  --certdnsnames=localhost --plchostname=localhost --unsecuretransport
```

**Stop and clean up:**
```bash
docker stop opc-plc-100k && docker rm opc-plc-100k
docker stop opc-plc-1k && docker rm opc-plc-1k
```

**Run benthos test:**
```bash
# Build benthos
make target

# Run baseline test
export DEADBAND_TYPE=none
export DEADBAND_VALUE=0
./benthos run config/opcua-deadband-100k-test.yaml

# Run absolute deadband test
export DEADBAND_TYPE=absolute
export DEADBAND_VALUE=0.5
./benthos run config/opcua-deadband-100k-test.yaml

# Run percent deadband test
export DEADBAND_TYPE=percent
export DEADBAND_VALUE=2.0
./benthos run config/opcua-deadband-100k-test.yaml
```

**Monitor resources:**
```bash
# Docker stats
docker stats opc-plc-100k --no-stream

# Server health
curl http://localhost:8080/health

# Benthos metrics (if enabled)
curl http://localhost:4195/stats
```

**Analyze results:**
```bash
# Count total notifications
wc -l opcua-deadband-test-none-0.ndjson
wc -l opcua-deadband-test-absolute-0.5.ndjson
wc -l opcua-deadband-test-percent-2.0.ndjson

# Calculate reduction
echo "scale=2; (1 - $(wc -l < opcua-deadband-test-absolute-0.5.ndjson) / $(wc -l < opcua-deadband-test-none-0.ndjson)) * 100" | bc

# Sample notifications for accuracy check
head -n 100 opcua-deadband-test-absolute-0.5.ndjson | jq '.payload.value'
```
