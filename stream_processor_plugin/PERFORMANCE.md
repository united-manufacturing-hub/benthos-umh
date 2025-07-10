# Stream Processor Plugin Performance Documentation

## Performance Summary

**Current Performance**: ~36,000 messages/second  
**Target Performance**: >100,000 messages/second  
**Performance Gap**: 64% improvement needed

## Benchmark Results

All benchmarks were run on:
- **Platform**: macOS (darwin/arm64)
- **CPU**: Apple M3 Pro
- **Go Version**: As specified in go.mod
- **Test Duration**: 3 seconds per benchmark

### Message Processing Performance

| Scenario | Latency (ns/op) | Throughput | Notes |
|----------|----------------|------------|-------|
| Static Mapping | 27,637 ns | ~36,180 msg/sec | JavaScript evaluation of constants |
| Dynamic Mapping | 27,468 ns | ~36,405 msg/sec | JavaScript with variable context |
| Mixed Mappings | 27,963 ns | ~35,770 msg/sec | Combination of static + dynamic |
| **High Throughput** | **27,822 ns** | **~35,943 msg/sec** | **Baseline performance** |

### Fast-Path Performance (Message Skipping)

| Scenario | Latency (ns/op) | Throughput | Efficiency |
|----------|----------------|------------|------------|
| Missing Metadata | 36.85 ns | ~27M msg/sec | 99.87% faster than processing |
| Invalid JSON | 304.2 ns | ~3.3M msg/sec | 98.9% faster than processing |
| Unknown Topic | 561.9 ns | ~1.8M msg/sec | 98.0% faster than processing |

### JavaScript Expression Performance

| Expression Type | Latency (ns/op) | Relative Performance |
|----------------|----------------|---------------------|
| Simple Expression (`press + 1`) | 12,603 ns | Baseline |
| Complex Math (`Math.sqrt(press * press + tF * tF) / 2.0`) | 789.3 ns | 16x faster |
| String Concatenation | 788.8 ns | 16x faster |
| Conditional (`press > 20 ? 'high' : 'normal'`) | 12,147 ns | Similar to simple |
| Math Operations (`Math.pow(press, 2) + Math.abs(tF - 32) * 1.8`) | 800.9 ns | 16x faster |

**Observation**: Complex expressions without variable access are significantly faster, suggesting overhead in variable context setup.

### Variable Resolution Performance

| Scenario | Latency (ns/op) | Notes |
|----------|----------------|-------|
| First Variable | 30,775 ns | Initial state setup overhead |
| Subsequent Variables | 16,477 ns | 47% faster with existing state |
| Variable Overwrite | 28,913 ns | Similar to first variable |

### Batch Processing Scalability

| Batch Size | Total Latency | Per-Message Latency | Scalability |
|------------|---------------|-------------------|-------------|
| 1 | 27,362 ns | 27,362 ns | Baseline |
| 10 | 207,100 ns | 20,710 ns | 24% improvement |
| 100 | 2,187,756 ns | 21,878 ns | 20% improvement |
| 1000 | 23,706,790 ns | 23,707 ns | 13% improvement |

**Observation**: Batch processing provides diminishing returns, suggesting per-message overhead dominates.

### Memory Usage Analysis

| Metric | Per Message | Notes |
|--------|-------------|-------|
| **Memory Allocated** | **37.6 KB** | High memory usage per message |
| **Allocations** | **512 allocs/op** | High allocation count |
| State Accumulation | Linear growth | No memory leaks detected |

## Performance Bottlenecks Identified

### 1. High Per-Message Overhead (Primary Bottleneck)
- **Current**: ~27-28 μs per message
- **Target**: ~10 μs per message (for 100k msg/sec)
- **Impact**: 3x performance improvement needed

### 2. Memory Allocations (Secondary Bottleneck)
- **Current**: 512 allocations per message (37.6 KB)
- **Impact**: Significant GC pressure
- **Areas**: Message creation, JSON marshaling, metadata handling

### 3. JavaScript Engine Overhead
- Simple expressions have unexpectedly high overhead (12+ μs)
- Variable context setup appears expensive
- Complex expressions perform better relatively

### 4. Metadata Processing
- Each message requires metadata extraction and copying
- Topic resolution and validation overhead

## Performance Optimization Opportunities

### High Impact (Est. 40-60% improvement)
1. **Reduce Memory Allocations**
   - Object pooling for common structures
   - Reuse buffers for JSON marshaling
   - Optimize metadata handling

2. **JavaScript Engine Optimization**
   - Cache compiled expressions more aggressively
   - Optimize variable context creation
   - Consider alternative JS engines or direct evaluation

### Medium Impact (Est. 20-30% improvement)
3. **Message Processing Pipeline**
   - Reduce unnecessary JSON parsing rounds
   - Optimize topic matching algorithms
   - Streamline output message creation

4. **State Management**
   - Optimize variable storage structures
   - Reduce locking overhead with lock-free approaches

### Low Impact (Est. 5-15% improvement)
5. **Batch Processing**
   - Already provides some benefit
   - Focus on reducing per-message overhead instead

## Performance Testing Methodology

### Benchmark Scenarios
- **Static Mapping**: Constant expressions only
- **Dynamic Mapping**: Variable-dependent expressions  
- **Mixed Mappings**: Realistic combination
- **Error Conditions**: Invalid messages (fast-path validation)
- **JavaScript Complexity**: Various expression types
- **Memory Patterns**: Allocation behavior under load

### Metrics Collected
- **Latency**: Nanoseconds per operation
- **Throughput**: Messages per second
- **Memory**: Bytes allocated per operation
- **Allocations**: Number of allocations per operation

### Test Environment
- Isolated benchmarks with minimal external factors
- Multiple runs for statistical significance
- Realistic workload distributions (60% pressure, 30% temperature, 10% status)

## Recommendations

### Immediate Actions
1. **Profile Memory Usage**: Use `go tool pprof` to identify allocation hotspots
2. **Analyze JavaScript Overhead**: Profile expression evaluation performance
3. **Optimize Message Creation**: Reduce allocations in output generation

### Performance Targets
- **Phase 1**: Achieve 60k msg/sec (67% improvement)
- **Phase 2**: Achieve 80k msg/sec (33% additional improvement)  
- **Phase 3**: Achieve 100k+ msg/sec (25% additional improvement)

### Success Metrics
- Throughput: >100,000 messages/second
- Latency: <10 μs per message average
- Memory: <20 KB allocated per message
- Allocations: <200 allocations per message

---

*Last Updated*: Based on benchmark results from stream_processor_plugin  
*Next Review*: After performance optimization implementation 