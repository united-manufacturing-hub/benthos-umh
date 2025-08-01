# Topic Browser Processor

{% hint style="info" %}
**Internal UMH Component**: This processor is designed for internal UMH system use and enables the Topic Browser feature in the Management Console. It transforms raw UMH messages into structured data for real-time topic exploration and debugging.
{% endhint %}

The Topic Browser Processor extracts hierarchical topic information and event data from UMH messages, providing the data foundation for the Topic Browser interface. It enables users to explore the Unified Namespace in real-time, search topics by metadata, and inspect both current values and historical event streams.

## Business Logic Overview

### What the Topic Browser Shows Users

The Topic Browser transforms the raw stream of UMH messages into an organized, searchable interface:

1. **Hierarchical Topic Tree**: Displays the ISA-95 hierarchy (enterprise → site → area → line → etc.) with real-time population as new topics appear
2. **Live Topic Discovery**: Shows new topics the moment they start publishing data, with visual indicators for recently active topics  
3. **Metadata Search & Filtering**: Enables searching by headers like `unit=°C`, `manufacturer=Siemens`, or `plc_address` without scanning live message traffic
4. **Real-Time Value Display**: Shows the latest value for each topic with timestamp and data quality indicators
5. **Event History**: Provides a scrollable timeline of recent values for debugging and trend analysis
6. **Topic Debugging**: Displays raw message headers, routing history, and timing information for troubleshooting

### User Journey Through Topic Browser

| Phase | User Action | UI Response | Underlying Data |
|-------|-------------|-------------|-----------------|
| **Discovery** | Opens Topic Browser | Tree auto-populates with live topics | `uns_map` contains all active topics with metadata |
| **Search** | Types "temperature" | Tree filters, shows matching topics | Search runs against cached `TopicInfo.metadata` |
| **Inspection** | Clicks a topic | Shows latest value + sparkline | Latest `EventTableEntry` + recent event history |
| **Debugging** | Expands "Headers" section | Shows all metadata for topic | `TopicInfo.metadata` merged from recent messages |
| **Troubleshooting** | Views "Message History" | Timeline of recent values | Ring buffer of `EventTableEntry` objects |

## Message Processing Contract

### Processing Architecture

The processor implements a **ring buffer + delayed ACK** architecture:

#### Ring Buffer Strategy
- **Per-topic buffers**: Each topic maintains a ring buffer of latest events
- **Buffer size**: Configurable (default: 10 events per topic per interval)
- **Overflow handling**: Automatic overwrite of oldest events when buffer full
- **Rate limiting**: Prevents memory exhaustion during startup topic replay scenarios
- **Emission interval**: Configurable timer-based emission (default: 1 second)

#### Delayed ACK Pattern
- **Buffering**: Messages are buffered until emission interval elapses
- **In-place ACK**: Buffered messages are ACKed in-place when emission succeeds (not forwarded downstream)
- **Single emission**: Only the protobuf bundle is forwarded downstream, original messages are consumed
- **Failure handling**: Emission failure prevents ACK (messages will be retried)
- **Memory safety**: Buffer size limits protect against unbounded growth

### Input Requirements

The processor expects UMH messages with:
- **umh_topic metadata**: Topic hierarchy string (e.g., `umh.v1.enterprise.plant1._historian.temperature`)
- **Valid JSON payload**: Either UMH-Core time-series format or relational data
- **Kafka headers**: Optional metadata for topic enrichment (unit, manufacturer, etc.)

### Output Emission Rules

The processor follows a strict emission contract that optimizes network traffic:

#### Critical: Only Protobuf Bundle is Forwarded
- **Single output**: Only the compressed protobuf bundle (STARTSTARTSTART format) is sent downstream
- **Original messages**: Input UMH messages are ACKed but **NOT forwarded** downstream
- **No duplication**: You will never see both the protobuf bundle AND the original messages in output
- **Clean pipeline**: Downstream consumers only receive the structured protobuf data

#### UNS Map Emission
- **When emitted**: Always emitted with complete topic tree in every emission interval
- **What's included**: The ENTIRE current topic tree (all topics and their cumulative metadata)
- **Why entire tree**: Stateless consumption - downstream gets complete state each time
- **No change detection**: Always emits full tree for complete state consistency

#### Events Emission  
- **What's included**: ALL successfully processed messages from the current batch
- **No synthetic data**: Processor never fabricates additional events
- **Failed messages**: Logged and counted but don't appear in output

#### Possible Output Scenarios
1. **Both uns_map + events**: Complete topic tree plus ring-buffered events (most common)
2. **Events only**: Ring-buffered events with previously known topic tree
3. **Topics only**: Complete topic tree without events (edge case)
4. **No output**: No messages processed since last emission interval

#### Message-Driven Behavior (Important)
- **Emission trigger**: Emissions ONLY occur when messages are actively being processed
- **No heartbeats**: No timer-based heartbeats if no messages arrive
- **Low-traffic impact**: Low-traffic UNS scenarios may experience extended delays between emissions
- **Downstream considerations**: Consumers should expect gaps in emission timing during quiet periods
- **Design rationale**: Processor is message-driven, not time-driven for resource efficiency

### Data Structures

#### TopicInfo (Topic Metadata)
```protobuf
message TopicInfo {
  string level0 = 1;                    // Enterprise/root level (mandatory)
  repeated string location_sublevels = 2; // [site, area, line, workcell, ...] 
  string data_contract = 3;             // "_historian", "_analytics", etc.
  optional string virtual_path = 4;     // Non-physical grouping (axis.x.position)
  string name = 5;                      // Final segment (temperature, pressure)
  map<string,string> metadata = 6;     // Aggregated headers (unit, manufacturer, etc.)
}
```

**Business Logic**: 
- Represents the "where" and "what" of each signal in the system
- Metadata accumulated from all messages for complete topic state
- Always includes cumulative metadata (all keys ever seen for the topic)
- Updated with latest values using last-write-wins strategy

#### EventTableEntry (Message Data)
```protobuf
message EventTableEntry {
  string uns_tree_id = 1;              // xxHash of TopicInfo for efficient joins
  oneof payload {
    TimeSeriesPayload ts = 10;         // Scalar value + timestamp
    RelationalPayload rel = 11;        // Full JSON document
  }
  EventKafka raw_kafka_msg = 5;        // Original headers + payload for debugging
  repeated string bridged_by = 6;      // Benthos routing history
  uint64 produced_at_ms = 7;           // Kafka write timestamp
}
```

**Business Logic**:
- Represents individual message events with timestamps
- `uns_tree_id` links back to topic hierarchy without repeating strings
- Raw message preserved for debugging network issues or data quality problems

### Topic Hierarchy Processing

The processor parses UMH topic strings into structured hierarchies:

```
umh.v1.enterprise.site.area.line._historian.axis.x.position
        ↓
Level0: "enterprise"
LocationSublevels: ["site", "area", "line"] 
DataContract: "_historian"
VirtualPath: "axis.x"
Name: "position"
```

**Business Rules**:
- level0 is mandatory
- Location sublevels are dynamic (can be 0 to N levels)
- Data contract must start with underscore and cannot be the final segment  
- Virtual path is optional logical grouping
- Name is always the final segment

## Performance Optimizations

### Data Serialization Strategy

The processor uses **direct protobuf serialization** for all output:

#### Serialization Approach
- **Strategy**: Direct protobuf serialization without compression
- **No threshold**: All payloads are processed using the same format
- **Implementation**: Direct binary serialization for efficiency
- **Detection**: Downstream consumers parse protobuf directly

#### Performance Benefits
- **Reduced CPU overhead**: Eliminates compression/decompression cycles
- **Simplified processing**: Direct protobuf parsing without decompression
- **Lower memory usage**: No compression buffer allocations
- **Faster processing**: Optimized for small to medium payloads common in UMH

### LRU Cache Optimization

Topic metadata caching prevents unnecessary re-transmission:

#### Cache Strategy
- **Key**: UNS Tree ID (xxHash of topic hierarchy)  
- **Value**: Merged headers from recent messages
- **Size**: Configurable (default 50,000 entries)
- **Eviction**: Least Recently Used

#### Metadata Accumulation Logic
```
New message → Extract headers → Hash topic hierarchy → Cache lookup
                                                           ↓
Cache miss: Store headers in cache + update full topic map
Cache hit: Merge headers (last-write-wins) → Update cache + full topic map
                                                           ↓
                            Full topic map always emitted with complete state
```

**Result**: Cumulative metadata ensures complete topic state is always available downstream

### Protobuf Schema Design

Efficient binary serialization with forward/backward compatibility:

#### Space Optimizations
- Hash-based topic references instead of repeated strings
- Oneof payload fields to avoid null checks
- Varint encoding for timestamps and counters
- Optional fields for sparse data

#### Network Format
```
STARTSTARTSTART
<hex-encoded-protobuf-data>
ENDDATAENDDATAENDDATA  
<unix-timestamp-ms>
ENDENDENDEND
```

**Purpose**: Enables efficient parsing and latency measurement in downstream systems

## Configuration

### Basic Configuration
```yaml
processors:
  - topic_browser:
      lru_size: 50000                              # Cache size (default: 50,000 entries)
      emit_interval: "1s"                          # Base emit interval - CPU-aware controller adapts this (default: 1s)
      max_events_per_topic_per_interval: 1        # Ring buffer size per topic - burst protection (default: 1)
      max_buffer_size: 10000                     # Safety limit for total buffered messages (default: 10,000)
```

## CPU-Aware Adaptive Behavior

The topic browser processor automatically adapts its emit intervals based on CPU load and payload patterns **without requiring any configuration**. This intelligent resource management ensures optimal performance while preventing CPU saturation.

### Algorithm Overview

- **CPU Monitoring**: Samples CPU usage every 200ms using `syscall.Getrusage`
- **EMA Smoothing**: Applies exponential moving average (α=0.2) to prevent oscillation
- **Payload Awareness**: Adjusts intervals based on message volume patterns
- **Adaptive Range**: Emit intervals dynamically adjust between 1s-15s
- **Gradual Changes**: Maximum 2s adjustment per cycle for stability

### Behavior by Load Pattern

| Scenario | CPU Load | Payload Size | Adaptive Interval | Result |
|----------|----------|--------------|-------------------|---------|
| High traffic burst | >90% | Large (>50KB) | 1s (minimum) | Fast emission, better compression |
| Normal operation | 70-90% | Medium (1-10KB) | 4-8s | Balanced performance |
| Low traffic | <70% | Small (<1KB) | 8-15s | CPU conservation |
| CPU saturation | >90% | Any | Increases gradually | Prevents overload |

### Exposed Metrics

The processor exposes additional Prometheus metrics for operational visibility:

- `cpu_load_percent`: Current CPU usage percentage (0-100)
- `active_emit_interval_seconds`: Current adaptive emit interval in milliseconds
- `active_topics_count`: Number of active topics being tracked

### Configuration Parameters

| Parameter | Default | Purpose | Tuning Guidance |
|-----------|---------|---------|-----------------|
| `lru_size` | 50,000 | LRU cache size for cumulative metadata storage | Adjust based on topic cardinality |
| `emit_interval` | 1s | Base emit interval for CPU-aware adaptation | Used as starting point for adaptive algorithm |
| `max_events_per_topic_per_interval` | 1 | Ring buffer size per topic (burst protection) | Increased from 10→1 for maximum burst protection |
| `max_buffer_size` | 100,000 | Safety limit for total buffered messages | Set based on available memory |

### Sizing Guidelines
| Environment | Topics | LRU Size | Ring Buffer | Memory Usage |
|-------------|--------|----------|-------------|--------------|
| Small plant | <1,000 | 5,000 | 10 | ~5MB |
| Medium plant | 1,000-10,000 | 25,000 | 10 | ~25MB |
| Large enterprise | 10,000-100,000 | 100,000 | 15 | ~100MB |
| Very large | >100,000 | 250,000 | 20 | ~250MB |

## Integration Points

### Upstream Data Sources
- **uns-input plugin**: Provides umh_topic metadata and Kafka timestamps
- **tag-processor**: Generates structured UMH messages with metadata headers
- **Bridge plugins**: Add routing and device metadata to headers

### Downstream Consumers
- **stdout output**: Delivers formatted messages to umh-core  
- **FSM (Finite State Machine)**: Maintains canonical topic tree and event history
- **Management Console**: Renders Topic Browser interface from FSM data

### Data Flow
```
[OPC UA/Modbus] → [Bridge] → [tag-processor] → [topic-browser] → [stdout] → [umh-core FSM] → [UI]
                     ↓            ↓               ↓                           
                  Headers    UMH Format    Structured Data              
```

## Edge Cases & Error Handling

### Message Processing Failures
- **Invalid topic format**: Message skipped, error logged, failure metric incremented
- **Missing umh_topic**: Message skipped with detailed logging
- **Malformed JSON**: Message skipped, error logged, processing continues
- **Duplicate timestamps**: Last-write-wins for headers, all events preserved

### Cache Management
- **Cache eviction**: Evicted topics re-emitted on next access (acceptable trade-off)
- **Memory pressure**: LRU automatically manages memory within configured bounds
- **Thread safety**: Mutex protection prevents race conditions in concurrent processing

### Network & Serialization
- **Protobuf failures**: Return error immediately (no partial emission)
- **Serialization failures**: Return error immediately (no partial data emission)
- **Large payloads**: Direct protobuf serialization handles up to multi-megabyte bundles efficiently

### Ring Buffer Edge Cases
- **Buffer overflow**: Oldest events automatically discarded when ring buffer full
- **Burst traffic**: Startup topic replay scenarios handled gracefully via ring buffer limits
- **Memory safety**: Ring buffer size limits prevent unbounded memory growth
- **Event ordering**: Ring buffer maintains chronological order within each topic

## Troubleshooting

### Common Issues

**No topics appearing in browser**:
- Check umh_topic metadata is present in messages
- Verify topic format follows UMH conventions (umh.v1....)
- Ensure messages reach the processor (check input metrics)

**Seeing duplicate messages in output**:
- This should NOT happen - if you see both protobuf bundles AND original messages, there's a configuration issue
- The processor should only emit protobuf bundles (STARTSTARTSTART format)
- Original UMH messages are consumed and ACKed, not forwarded
- Check for multiple processors or incorrect pipeline configuration

**Performance degradation**:
- Monitor LRU cache hit rate (should be >90%)
- Check for extremely high topic cardinality  
- Verify ring buffer isn't overflowing excessively (check events_overwritten metric)
- Ensure emission intervals aren't too frequent for your traffic volume

**Memory usage growth**:
- Reduce lru_size if memory constrained
- Lower max_events_per_topic_per_interval to reduce ring buffer memory
- Reduce max_buffer_size to limit total buffered messages
- Check for topic metadata churn causing cache misses
- Monitor for header proliferation per topic

**Emission delays**:
- Verify messages are actively flowing (processor is message-driven)
- Check emit_interval setting vs required latency
- Monitor for processing bottlenecks causing buffer delays

### Metrics to Monitor
- `messages_processed`: Successfully processed messages (should increase steadily)
- `messages_failed`: Failed processing attempts (should remain low)
- `events_overwritten`: Ring buffer overflow events (monitor for excessive values)
- `total_events_emitted`: Total events sent downstream (for throughput monitoring)
- `ring_buffer_utilization`: Ring buffer usage patterns (currently tracked but not actively used)
- `flush_duration`: Time taken for buffer flush operations (performance monitoring)
- `emission_size`: Size of emitted protobuf bundles (network usage monitoring)

**Additional Metrics Available**:
- Ring buffer utilization: Monitor per-topic buffer usage patterns
- Downstream latency: Monitor time from message receipt to UI display 