# Test Plan: tag_processor CPU Analysis (10k Nodes)

**Status:** 📋 Ready to Execute
**Date:** 2025-10-30
**Related:** PR #223 performance validation
**Previous Test:** `pr223-test-results.md` (log processor baseline)

## Objective

Measure **realistic production CPU usage** with `tag_processor` instead of test logging to:

1. Identify if tag_processor is CPU-intensive in production workloads
2. Get accurate production performance baseline (without test logging overhead)
3. Compare against previous test (log processor: 27.4% CPU overhead)
4. Validate system performance for production deployment

## Hypothesis

**Current bottleneck analysis shows:**
- Logging dominates: 27.4% CPU (test artifact)
- System is NOT CPU-bound: 6.92% utilization, 93% idle
- Tag processor overhead: Unknown

**Expected outcome:**
- Tag processor should have **lower overhead** than logging (< 10% CPU)
- System should handle **10,000+ msg/sec** with realistic pipeline
- CPU utilization should remain **< 20%** for production headroom

## Configuration Changes

### 1. Replace log Processor with tag_processor

**Before (test-deadband-10k.yaml):**
```yaml
pipeline:
  processors:
    - log:
        level: INFO
        message: "Message ${! count(\"messages\") }: ${! json(\"value\") }"
```

**After (test-tag-processor-10k.yaml):**
```yaml
pipeline:
  processors:
    - tag_processor:
        # Standard UMH tag processing
        mode: "standard"  # or whatever production mode is used
```

### 2. Change Output to stdout/file

**Before:**
```yaml
output:
  drop: {}
```

**After (Option A - stdout):**
```yaml
output:
  stdout:
    codec: lines
```

**After (Option B - file for throughput measurement):**
```yaml
output:
  file:
    path: /tmp/tag-processor-output.jsonl
    codec: lines
```

**Recommendation:** Use **Option B (file)** to avoid terminal rendering overhead and enable accurate message counting.

### 3. Keep OPC UA Settings Unchanged

```yaml
input:
  opcua:
    endpoint: "opc.tcp://localhost:50000"
    nodeIDs: ["ns=3;s=OpcPlc"]
    subscribeEnabled: true
    securityMode: "None"
    securityPolicy: "None"
    # deadband defaults (absolute, 0.0)
```

### 4. Keep HTTP/pprof Endpoint

```yaml
http:
  enabled: true
  address: "0.0.0.0:4197"
  root_path: /benthos
  debug_endpoints: true
```

## Complete Test Configuration

**File:** `config/test-tag-processor-10k.yaml`

```yaml
input:
  opcua:
    endpoint: "opc.tcp://localhost:50000"
    nodeIDs: ["ns=3;s=OpcPlc"]
    subscribeEnabled: true
    securityMode: "None"
    securityPolicy: "None"

pipeline:
  processors:
    - tag_processor:
        mode: "standard"

output:
  file:
    path: /tmp/tag-processor-output.jsonl
    codec: lines

logger:
  level: WARN  # Reduce logging overhead
  add_timestamp: true

http:
  enabled: true
  address: "0.0.0.0:4197"
  root_path: /benthos
  debug_endpoints: true
```

## Test Execution Steps

### Phase 1: Setup (5 minutes)

1. **Verify OPC-PLC is running with 10k nodes:**
   ```bash
   docker ps | grep opc-plc
   # Should show: --sn=5000 --sr=10 --fn=5000 --fr=1
   ```

2. **Create test configuration:**
   ```bash
   # Create config/test-tag-processor-10k.yaml (content above)
   cd /Users/jeremytheocharis/umh-git/benthos-umh-fix3/
   ```

3. **Clear previous output file:**
   ```bash
   rm -f /tmp/tag-processor-output.jsonl
   ```

### Phase 2: Test Execution (3 minutes)

1. **Start benthos with tag_processor config:**
   ```bash
   ./target/bin/benthos -c config/test-tag-processor-10k.yaml \
     > /tmp/tag-processor-test.log 2>&1 &

   BENTHOS_PID=$!
   echo "Benthos PID: $BENTHOS_PID"
   ```

2. **Wait for subscriptions to complete (60s):**
   ```bash
   sleep 60
   tail -20 /tmp/tag-processor-test.log | grep -i subscription
   ```

3. **Capture CPU profile (30s):**
   ```bash
   curl -s 'http://localhost:4197/debug/pprof/profile?seconds=30' \
     > /tmp/pprof-cpu-tag-processor.prof

   echo "CPU profile captured: $(ls -lh /tmp/pprof-cpu-tag-processor.prof)"
   ```

4. **Capture heap profile:**
   ```bash
   curl -s 'http://localhost:4197/debug/pprof/heap' \
     > /tmp/pprof-heap-tag-processor.prof

   echo "Heap profile captured: $(ls -lh /tmp/pprof-heap-tag-processor.prof)"
   ```

5. **Let test run for 60 more seconds:**
   ```bash
   sleep 60
   ```

6. **Stop benthos:**
   ```bash
   kill $BENTHOS_PID
   wait $BENTHOS_PID 2>/dev/null
   ```

### Phase 3: Metrics Collection (2 minutes)

1. **Count messages processed:**
   ```bash
   wc -l /tmp/tag-processor-output.jsonl
   ```

2. **Calculate throughput:**
   ```bash
   # Total test duration: ~150 seconds (60s warmup + 30s profile + 60s run)
   # Messages / 150 = msg/sec
   ```

3. **Check final log for subscription counts:**
   ```bash
   tail -50 /tmp/tag-processor-test.log | grep -i subscription
   ```

## Metrics to Capture

### 1. CPU Profile Analysis

```bash
# Top CPU consumers
go tool pprof -top /tmp/pprof-cpu-tag-processor.prof | head -20

# tag_processor overhead
go tool pprof -top /tmp/pprof-cpu-tag-processor.prof | grep tag_processor

# Compare against previous test
echo "=== Previous Test (log processor) ==="
go tool pprof -top /tmp/pprof-cpu-10k.prof | grep -E "(log|sanitize|ReplaceAllString)"

echo "=== Current Test (tag_processor) ==="
go tool pprof -top /tmp/pprof-cpu-tag-processor.prof | grep tag_processor
```

### 2. System Utilization

```bash
# Total CPU utilization
go tool pprof -top /tmp/pprof-cpu-tag-processor.prof | head -1

# Cumulative percentages by category
go tool pprof -top /tmp/pprof-cpu-tag-processor.prof | \
  awk '/syscall/ {syscall+=$4} /logrus/ {log+=$4} /tag_processor/ {tag+=$4}
       END {print "Syscalls:", syscall "%"; print "Logging:", log "%"; print "Tag Processor:", tag "%"}'
```

### 3. Memory Profile Analysis

```bash
# Total allocations
go tool pprof -alloc_space -top /tmp/pprof-heap-tag-processor.prof | head -1

# tag_processor allocations
go tool pprof -alloc_space -top /tmp/pprof-heap-tag-processor.prof | grep tag_processor

# Compare OPC UA receive allocations
echo "=== Previous Test ==="
go tool pprof -alloc_space -top /tmp/pprof-heap-10k.prof | grep Receive

echo "=== Current Test ==="
go tool pprof -alloc_space -top /tmp/pprof-heap-tag-processor.prof | grep Receive
```

### 4. Throughput Comparison

| Test | Processor | Duration | Messages | Throughput | CPU % | Result |
|------|-----------|----------|----------|------------|-------|--------|
| Baseline | log | 155s | 694,889 | 4,483/sec | 6.92% | Previous |
| tag_processor | tag_processor | 150s | TBD | TBD | TBD | **This test** |

## Expected Outcomes

### Success Criteria

✅ **tag_processor overhead < 10% CPU**
✅ **Throughput ≥ 4,000 msg/sec** (similar to baseline)
✅ **Total CPU utilization < 20%** (production headroom)
✅ **No errors or warnings in logs**
✅ **All 10,026 subscriptions created successfully**

### Comparison Matrix

| Metric | log Processor (Baseline) | tag_processor (Expected) | Delta |
|--------|--------------------------|--------------------------|-------|
| **CPU Utilization** | 6.92% | 8-15% | +1-8% |
| **Logging Overhead** | 27.4% | < 5% | -22% |
| **tag_processor Overhead** | N/A | < 10% | +10% |
| **Throughput** | 4,483 msg/sec | 4,000-5,000 msg/sec | ±10% |
| **Memory Allocations** | 3,820MB | 3,500-4,000MB | ±10% |

### Red Flags (Abort Test If)

❌ **CPU utilization > 50%** - tag_processor is too expensive
❌ **Throughput < 3,000 msg/sec** - significant performance regression
❌ **Errors in logs** - configuration or compatibility issue
❌ **Subscriptions < 10,000** - OPC UA connection problem

## Analysis Checklist

After test completion, analyze:

- [ ] **tag_processor CPU overhead:** What % of total CPU time?
- [ ] **Compare against logging overhead:** Is tag_processor more/less expensive?
- [ ] **Identify new hotspots:** Any unexpected CPU consumers?
- [ ] **Memory impact:** Does tag_processor allocate significantly more memory?
- [ ] **Throughput validation:** Can system maintain 4k+ msg/sec?
- [ ] **Production readiness:** Is < 20% CPU achieved for scaling headroom?

## Follow-up Actions

Based on test results:

### If tag_processor < 10% CPU:
✅ **System is production-ready**
✅ **Document realistic performance baseline**
✅ **Proceed with PR #223 merge recommendation**

### If tag_processor 10-20% CPU:
⚠️ **Profile tag_processor internals**
⚠️ **Identify optimization opportunities**
✅ **Still acceptable for production, but monitor**

### If tag_processor > 20% CPU:
❌ **Deep dive into tag_processor implementation**
❌ **Consider optimization or refactoring**
❌ **May need to address before production deployment**

## Deliverables

1. **Test results document:** `docs/plans/tag-processor-results-10k.md`
2. **CPU profile:** `/tmp/pprof-cpu-tag-processor.prof`
3. **Heap profile:** `/tmp/pprof-heap-tag-processor.prof`
4. **Test log:** `/tmp/tag-processor-test.log`
5. **Output data:** `/tmp/tag-processor-output.jsonl` (for validation)
6. **Performance comparison:** Update `pr223-test-results.md` with tag_processor findings

## Timeline

| Phase | Duration | Tasks |
|-------|----------|-------|
| Setup | 5 min | Verify OPC-PLC, create config, clear logs |
| Execution | 3 min | Start benthos, capture profiles, stop |
| Collection | 2 min | Count messages, calculate throughput |
| Analysis | 15 min | Analyze profiles, compare against baseline |
| Documentation | 10 min | Create results document |
| **Total** | **35 min** | Complete test and analysis |

## Notes

- **tag_processor mode:** Need to confirm correct mode for production (check existing configs)
- **Output format:** File output chosen to avoid terminal rendering overhead
- **Logger level:** Set to WARN to minimize logging in test
- **Profile duration:** 30s captures enough samples without excessive overhead
- **Comparison fairness:** Both tests use same OPC-PLC config (10k nodes, same update rates)

## Next Steps After This Test

1. **Execute this test plan** (35 minutes)
2. **Spawn subagent for gopcua memory optimization investigation** (Idea 2)
3. **Update PR #223 description** with comprehensive performance data
4. **Final merge decision** based on both test results
