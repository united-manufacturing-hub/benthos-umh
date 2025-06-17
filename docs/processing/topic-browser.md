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

### Input Requirements

The processor expects UMH messages with:
- **umh_topic metadata**: Topic hierarchy string (e.g., `umh.v1.enterprise.plant1._historian.temperature`)
- **Valid JSON payload**: Either UMH-Core time-series format or relational data
- **Kafka headers**: Optional metadata for topic enrichment (unit, manufacturer, etc.)

### Output Emission Rules

The processor follows a strict emission contract that optimizes network traffic:

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
- Metadata aggregated from all messages for search/filter functionality
- Changes infrequently (only when new headers appear or values change)

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
- Level0 (enterprise) is mandatory
- Location sublevels are dynamic (can be 0 to N levels)
- Data contract must start with underscore and cannot be the final segment  
- Virtual path is optional logical grouping
- Name is always the final segment

## Performance Optimizations

### LZ4 Compression Strategy

The processor uses conditional LZ4 compression to minimize network traffic:

#### Compression Decision Algorithm
- **Threshold**: 1024 bytes of protobuf data
- **Below threshold**: Send uncompressed (LZ4 frame overhead > benefit)
- **Above threshold**: Apply LZ4 level 0 (fastest compression)

#### Performance Characteristics
| Bundle Size | Compression Ratio | Processing Time | Bytes Saved |
|-------------|-------------------|-----------------|-------------|
| 100 bytes | No compression | ~450ns | 0 |
| 94KB | 84.8% reduction | ~506μs | 79KB |
| 5MB | 84.6% reduction | ~14.8ms | 4MB |

**Why LZ4 Level 0**: Optimized for latency over compression ratio in real-time industrial systems

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
<hex-encoded-protobuf-or-lz4-data>
ENDDATAENDDATENDDATA  
<unix-timestamp-ms>
ENDENDENDEND
```

**Purpose**: Enables efficient parsing and latency measurement in downstream systems

## Configuration

### Basic Configuration
```yaml
processors:
  - topic_browser:
      lru_size: 50000  # Cache size (adjust based on topic cardinality)
```

### Sizing Guidelines
| Environment | Topics | Recommended LRU Size | Memory Usage |
|-------------|--------|---------------------|--------------|
| Small plant | <1,000 | 5,000 | ~5MB |
| Medium plant | 1,000-10,000 | 25,000 | ~25MB |
| Large enterprise | 10,000-100,000 | 100,000 | ~100MB |
| Very large | >100,000 | 250,000 | ~250MB |

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
- **Malformed JSON**: Graceful fallback, error logged, processing continues
- **Duplicate timestamps**: Last-write-wins for headers, all events preserved

### Cache Management
- **Cache eviction**: Evicted topics re-emitted on next access (acceptable trade-off)
- **Memory pressure**: LRU automatically manages memory within configured bounds
- **Thread safety**: Mutex protection prevents race conditions in concurrent processing

### Network & Serialization
- **Protobuf failures**: Return error immediately (no partial emission)
- **Compression failures**: Return error (prevents data corruption)
- **Large payloads**: Automatic LZ4 compression handles up to multi-megabyte bundles

## Troubleshooting

### Common Issues

**No topics appearing in browser**:
- Check umh_topic metadata is present in messages
- Verify topic format follows UMH conventions (umh.v1....)
- Ensure messages reach the processor (check input metrics)

**Performance degradation**:
- Monitor LRU cache hit rate (should be >90%)
- Check for extremely high topic cardinality  
- Verify LZ4 compression is activating for large bundles

**Memory usage growth**:
- Reduce lru_size if memory constrained
- Check for topic metadata churn causing cache misses
- Monitor for header proliferation per topic

### Metrics to Monitor
- `messages_processed`: Successfully processed messages (should increase steadily)
- `messages_failed`: Failed processing attempts (should remain low)
- Cache hit rate: Not directly exposed but can be inferred from traffic patterns
- Downstream latency: Monitor time from message receipt to UI display 