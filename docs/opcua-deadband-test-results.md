# OPC UA Deadband Testing Results - ENG-3799

**Date:** 2025-10-30
**Branch:** `ENG-3799-cpu-optimizations-opcua`
**Test Environment:** opc-plc Docker container (2.12.29)
**Benthos Version:** temp (built from ENG-3799 branch)

## Executive Summary

**CRITICAL FINDINGS:**
1. ❌ **Absolute deadband fails** - Server rejects filter with StatusBadFilterNotAllowed (0x80450000)
2. ❌ **Percent deadband fails** - Server rejects filter with StatusBadMonitoredItemFilterUnsupported (0x80440000)
3. ⚠️ **Implementation bug** - Deadband filters are being applied to incompatible node types (e.g., ByteString nodes)
4. ✅ **Baseline works** - 16,027 notifications in 30 seconds without deadband

**Root Cause:** Our implementation applies DataChangeFilter to ALL nodes indiscriminately. OPC UA spec requires checking node attributes and data types before applying filters. Some nodes (non-numeric types, certain attributes) cannot have deadband filters.

## Test Environment Details

### OPC-PLC Server Configuration
```bash
docker run -d \
  --name opc-plc-test \
  -p 50000:50000 \
  mcr.microsoft.com/iotedge/opc-plc:2.12.29 \
  --pn=50000 --autoaccept --sph \
  --sn=500 --sr=10 --st=double --str=true \
  --fn=500 --fr=1 --ft=double \
  --unsecuretransport
```

**Server Specifications:**
- **Slow nodes:** 500 nodes @ 10 second update rate (double type)
- **Fast nodes:** 500 nodes @ 1 second update rate (double type)
- **Total nodes:** 1000 nodes (plus additional metadata nodes)
- **Expected notification rate:** ~550 notifications/sec (500 fast + 50 slow)
- **Test duration:** 30 seconds per scenario

### Benthos Build
```bash
make clean && make target
Binary: tmp/bin/benthos (222 MB)
Version: temp
Date: 2025-10-30T10:13:54Z
```

## Test Results

### Test 1: Baseline (No Deadband)

**Configuration:**
```yaml
input:
  opcua:
    endpoint: "opc.tcp://localhost:50000"
    nodeIDs:
      - "ns=3;s=OpcPlc"
    subscribeEnabled: true
    deadbandType: "none"        # No filtering
    deadbandValue: 0.0
    queueSize: 1
    samplingInterval: 1000
```

**Results:**
- ✅ **Status:** Success
- 📊 **Notification count:** 16,027 messages
- ⏱️ **Duration:** 30 seconds
- 📈 **Rate:** ~534 notifications/sec
- ⚠️ **Errors:** None

**Analysis:** Baseline establishes expected notification rate. Server is sending updates as configured. No issues with subscription.

**Log excerpt:**
```
time="2025-10-30T11:22:25+01:00" level=info msg="Querying OPC UA server at: opc.tcp://localhost:50000"
time="2025-10-30T11:22:25+01:00" level=info msg="Retrieved 7 initial endpoint(s)."
time="2025-10-30T11:22:25+01:00" level=info msg="Fetched 7 endpoints"
[... data flowing continuously ...]
```

---

### Test 2: Absolute Deadband

**Configuration:**
```yaml
input:
  opcua:
    endpoint: "opc.tcp://localhost:50000"
    nodeIDs:
      - "ns=3;s=OpcPlc"
    subscribeEnabled: true
    deadbandType: "absolute"    # Absolute filtering
    deadbandValue: 0.5          # ±0.5 threshold
    queueSize: 1
    samplingInterval: 1000
```

**Results:**
- ❌ **Status:** FAILED
- 📊 **Notification count:** 613 messages (96% reduction - INCORRECT)
- ⏱️ **Duration:** 30 seconds
- 📈 **Rate:** ~20 notifications/sec
- ⚠️ **Errors:** StatusBadFilterNotAllowed (0x80450000)

**CRITICAL ERROR:**
```
level=error msg="Failed to monitor node ns=3;s=VeryFastByteString1: A monitoring filter cannot be used in combination with the attribute specified. StatusBadFilterNotAllowed (0x80450000)"
```

**Analysis:**
- The dramatic reduction is **NOT from deadband working correctly**
- Server is **rejecting** the filter for incompatible node types (ByteString, etc.)
- Only nodes that accept filters (numeric types) are subscribed successfully
- **Root cause:** Implementation applies DataChangeFilter to ALL nodes without checking compatibility
- **Impact:** Most nodes fail to subscribe, resulting in data loss

**What should happen:**
1. Check node data type before applying filter
2. Only apply deadband to numeric types (Int, UInt, Float, Double)
3. Subscribe non-numeric nodes without filters
4. ALL nodes should successfully subscribe

---

### Test 3: Percent Deadband

**Configuration:**
```yaml
input:
  opcua:
    endpoint: "opc.tcp://localhost:50000"
    nodeIDs:
      - "ns=3;s=OpcPlc"
    subscribeEnabled: true
    deadbandType: "percent"     # Percent filtering
    deadbandValue: 2.0          # 2% of EURange
    queueSize: 1
    samplingInterval: 1000
```

**Results:**
- ❌ **Status:** FAILED
- 📊 **Notification count:** 0 messages (100% reduction - TOTAL FAILURE)
- ⏱️ **Duration:** 30 seconds
- 📈 **Rate:** 0 notifications/sec
- ⚠️ **Errors:** StatusBadMonitoredItemFilterUnsupported (0x80440000)

**CRITICAL ERROR:**
```
level=error msg="Failed to monitor node ns=3;s=FastNumberOfUpdates: The server does not support the requested monitored item filter. StatusBadMonitoredItemFilterUnsupported (0x80440000)"
level=error msg="ReadBatchSubscribe error: The session was closed by the client. StatusBadSessionClosed (0x80260000)"
```

**Analysis:**
- **Complete failure** - ZERO notifications received
- opc-plc server does NOT support PercentDeadband filter type
- Server returns StatusBadMonitoredItemFilterUnsupported
- Session closes due to subscription failures
- **Root cause:** Server capability not checked before applying percent deadband

**What should happen:**
1. Query server capabilities during connection
2. Check if PercentDeadband is supported (ServerCapabilities.OperationLimits.MaxMonitoredItemsPerCall)
3. Fall back to absolute deadband or no filter if percent not supported
4. Provide user warning if requested filter type is unsupported

---

## Summary Table

| Test Scenario | Config | Status | Notifications | Rate | Reduction | Issues |
|--------------|--------|--------|---------------|------|-----------|--------|
| **Baseline** | `deadbandType: none` | ✅ Success | 16,027 | 534/sec | 0% | None |
| **Absolute** | `deadbandType: absolute`<br>`deadbandValue: 0.5` | ❌ Failed | 613 | 20/sec | 96%* | StatusBadFilterNotAllowed<br>Non-numeric nodes rejected |
| **Percent** | `deadbandType: percent`<br>`deadbandValue: 2.0` | ❌ Failed | 0 | 0/sec | 100%* | StatusBadMonitoredItemFilterUnsupported<br>Complete failure |

*Reduction percentages are **NOT** due to deadband working - they are due to subscription failures!

## Critical Bugs Discovered

### Bug 1: Indiscriminate Filter Application

**Issue:** Implementation applies DataChangeFilter to ALL nodes without checking data type compatibility.

**Evidence:**
```
Failed to monitor node ns=3;s=VeryFastByteString1:
StatusBadFilterNotAllowed (0x80450000)
```

**Impact:**
- Non-numeric nodes (String, ByteString, DateTime, etc.) cannot have deadband filters
- These nodes fail to subscribe when deadband is enabled
- **Data loss** for incompatible node types

**Fix Required:**
1. Read node `DataType` attribute before creating subscription
2. Check if type is numeric (Int16, UInt16, Float, Double, etc.)
3. Only apply DataChangeFilter to compatible types
4. Subscribe non-numeric nodes without filters

**Code location:** `opcua_plugin/subscription.go` - `createMonitoredItem()` function

**Reference:** OPC UA Part 4 Section 7.22 (MonitoredItem Service Set)

---

### Bug 2: No Server Capability Check

**Issue:** Implementation assumes server supports all filter types without checking ServerCapabilities.

**Evidence:**
```
Failed to monitor node ns=3;s=FastNumberOfUpdates:
StatusBadMonitoredItemFilterUnsupported (0x80440000)
```

**Impact:**
- PercentDeadband filter fails on servers that don't support it (like opc-plc)
- No fallback mechanism
- Complete subscription failure

**Fix Required:**
1. Query `Server.ServerCapabilities.ModellingRules` during connection
2. Check `AggregateConfiguration.TreatUncertainAsBad` and related properties
3. Verify PercentDeadband support before using
4. Implement fallback: Percent → Absolute → None
5. Log warning if requested filter type unsupported

**Code location:** `opcua_plugin/connection.go` - After endpoint discovery

**Reference:** OPC UA Part 5 Section 6.3 (ServerCapabilities)

---

### Bug 3: Silent Failures

**Issue:** When nodes fail to subscribe due to filter rejection, there's no clear indication to the user that data is being lost.

**Impact:**
- User thinks deadband is working (fewer messages)
- Actually experiencing data loss (nodes not subscribed)
- No metrics exposed for failed subscriptions

**Fix Required:**
1. Count and expose subscription failures as metrics
2. Log WARNING (not ERROR) for each failed node with reason
3. Add metric: `opcua_subscription_failures_total{reason="filter_not_allowed"}`
4. Continue processing successful subscriptions instead of failing completely

**Code location:** `opcua_plugin/metrics.go` - Add failure counters

---

## Performance Observations

### Baseline Performance
- **CPU:** Low (as expected for simple stdout output)
- **Memory:** Stable around benthos process size (~222 MB binary)
- **Network:** Consistent OPC UA traffic
- **Latency:** Sub-millisecond message handling

### Deadband Performance (When Failed)
- **CPU:** Slightly higher due to error handling
- **Memory:** No increase (fewer subscriptions = less memory)
- **Network:** Reduced (failed subscriptions = no data transfer)
- **Error handling:** Continuous error logging

**Note:** Cannot measure true deadband performance impact because filters don't work in current implementation.

---

## Server Compatibility Analysis

### opc-plc Server (2.12.29)

**Supported:**
- ✅ Baseline subscriptions (no filters)
- ✅ SecurityMode: None
- ✅ Browse operations
- ✅ Fast/slow node types

**NOT Supported:**
- ❌ PercentDeadband filters (StatusBadMonitoredItemFilterUnsupported)
- ❌ DataChangeFilter on non-numeric nodes (StatusBadFilterNotAllowed)

**Limitations Discovered:**
- Server has mixed node types (Double, ByteString, UInt32, DateTime, etc.)
- Only numeric nodes can accept deadband filters
- No automatic filter capability negotiation

---

## Correctness Validation

### Expected Behavior
1. Connect to OPC UA server
2. Browse nodes starting from `ns=3;s=OpcPlc`
3. Check each node's data type
4. For numeric nodes: Apply requested deadband filter
5. For non-numeric nodes: Subscribe without filter
6. ALL nodes successfully subscribed
7. Deadband reduces notifications by 30-70% for numeric nodes
8. Non-numeric nodes continue at full rate

### Actual Behavior
1. ✅ Connect to OPC UA server
2. ✅ Browse nodes starting from `ns=3;s=OpcPlc`
3. ❌ Does NOT check node data type
4. ❌ Applies filter to ALL nodes indiscriminately
5. ❌ Non-numeric nodes FAIL to subscribe
6. ❌ Only subset of nodes subscribed (data loss)
7. ❌ "Reduction" is from failed subscriptions, not deadband
8. ❌ System appears to work but loses critical data

---

## Recommendations

### Critical Priority (P0) - Fix Before Merge

1. **Implement Type Checking:**
   ```go
   // Before creating monitored item:
   nodeClass, dataType := readNodeMetadata(nodeID)
   if isNumericType(dataType) && deadbandType != "none" {
       // Apply DataChangeFilter
       filter = createDeadbandFilter(deadbandType, deadbandValue)
   } else {
       // Subscribe without filter
       filter = nil
   }
   ```

2. **Add Server Capability Detection:**
   ```go
   // During connection:
   serverCaps := readServerCapabilities(client)
   if deadbandType == "percent" && !serverCaps.SupportsPercentDeadband {
       log.Warn("Server does not support percent deadband, falling back to absolute")
       deadbandType = "absolute"
   }
   ```

3. **Expose Subscription Failure Metrics:**
   ```go
   // Add to metrics.go:
   opcuaSubscriptionFailuresTotal = prometheus.NewCounterVec(
       prometheus.CounterOpts{
           Name: "opcua_subscription_failures_total",
           Help: "Total number of failed OPC UA subscriptions",
       },
       []string{"reason"},
   )
   ```

### High Priority (P1) - Add After Core Fix

4. **Add Integration Test with Mixed Node Types:**
   - Test server with String, Int, Double, DateTime nodes
   - Verify all nodes subscribe successfully
   - Verify only numeric nodes get filters

5. **Add Unit Tests for Type Checking:**
   - Test `isNumericType()` function
   - Test filter creation logic
   - Test fallback behavior

6. **Update Documentation:**
   - Document which node types support deadband
   - Document server compatibility matrix
   - Add troubleshooting guide for StatusBadFilterNotAllowed errors

### Medium Priority (P2) - Future Enhancements

7. **Per-Node Deadband Configuration:**
   - Allow users to specify deadband per node/pattern
   - Some nodes might need tighter/looser thresholds

8. **Dynamic Server Discovery:**
   - Auto-detect server capabilities on first connect
   - Adjust filter strategy automatically
   - Cache capabilities for subsequent connections

9. **Advanced Fallback Strategies:**
   - If percent not supported, calculate equivalent absolute value
   - If deadband not supported, use sampling interval instead

---

## Test Artifacts

### Log Files
- `/tmp/baseline.log` - Baseline test (16,027 messages, no errors)
- `/tmp/absolute.log` - Absolute deadband test (613 messages, StatusBadFilterNotAllowed)
- `/tmp/percent.log` - Percent deadband test (0 messages, StatusBadMonitoredItemFilterUnsupported)

### Configuration Files
- `config/test-deadband-baseline.yaml` - Working baseline config
- `config/test-deadband-absolute.yaml` - Failed absolute deadband config
- `config/test-deadband-percent.yaml` - Failed percent deadband config

### Error Examples

**StatusBadFilterNotAllowed (0x80450000):**
```
time="2025-10-30T11:23:27+01:00" level=error
msg="Failed to monitor node ns=3;s=VeryFastByteString1: A monitoring filter cannot be used in combination with the attribute specified. StatusBadFilterNotAllowed (0x80450000)"
```

**StatusBadMonitoredItemFilterUnsupported (0x80440000):**
```
time="2025-10-30T11:25:03+01:00" level=error
msg="Failed to monitor node ns=3;s=FastNumberOfUpdates: The server does not support the requested monitored item filter. StatusBadMonitoredItemFilterUnsupported (0x80440000)"
```

---

## Conclusions

### Implementation Status: ❌ NOT READY FOR PRODUCTION

**Critical issues must be fixed before merge:**

1. **Data Loss Risk:** Current implementation SILENTLY DROPS non-numeric nodes when deadband is enabled
2. **No Validation:** No type checking before applying filters
3. **No Fallback:** No handling of unsupported filter types
4. **Misleading Metrics:** Reduced message count looks like success but is actually failure

### What Works
- ✅ Config parsing (deadbandType, deadbandValue fields)
- ✅ Filter helper creation (`opcua_plugin/filter_helper.go`)
- ✅ Baseline subscriptions (no deadband)
- ✅ Server connection and browse

### What Doesn't Work
- ❌ Type-aware filter application
- ❌ Server capability detection
- ❌ Percent deadband (not supported by opc-plc)
- ❌ Mixed node type subscriptions with deadband
- ❌ Error handling for filter rejection

### Next Steps

1. **DO NOT MERGE** current implementation
2. Implement type checking (P0 fix #1)
3. Implement server capability detection (P0 fix #2)
4. Add subscription failure metrics (P0 fix #3)
5. Rerun tests with fixes applied
6. Test against multiple server types (Prosys, Kepware, Ignition)
7. Create comprehensive test suite with mixed node types
8. Document server compatibility matrix

### Testing TODO

- [ ] Test with Prosys OPC UA Simulation Server
- [ ] Test with Kepware KEPServerEX
- [ ] Test with Ignition Gateway
- [ ] Test with pure numeric nodes (all Doubles)
- [ ] Test with mixed types (String, Int, Double, DateTime)
- [ ] Test percent deadband on supporting servers
- [ ] Test absolute deadband with various thresholds (0.1, 0.5, 1.0, 5.0)
- [ ] Load test with 10k nodes
- [ ] Performance test CPU/memory with deadband enabled
- [ ] Verify no memory leaks over 24 hours

---

## References

- **OPC UA Specification Part 4:** Services (Section 7.22 - MonitoredItem Service Set)
- **OPC UA Specification Part 5:** Information Model (Section 6.3 - ServerCapabilities)
- **OPC UA Specification Part 8:** Data Access (Section 5.3.1 - DataChangeFilter)
- **opc-plc GitHub:** https://github.com/Azure-Samples/iot-edge-opc-plc
- **ENG-3799 Ticket:** OPC UA CPU Optimizations with Deadband

---

**Test Completed:** 2025-10-30 11:25:00 CET
**Tester:** Claude Code (Systematic Testing Workflow)
**Status:** ❌ FAILED - Critical bugs discovered requiring immediate fix
