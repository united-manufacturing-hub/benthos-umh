# UNS Bundle Decoder

This tool properly decodes the UNS Bundle protobuf messages produced by the topic browser processor, which CyberChef has trouble parsing correctly.

## Problem

CyberChef struggles with the protobuf format because:

1. **Complex nested structures**: The UnsBundle contains multiple nested oneOf fields and repeated structures
2. **LZ4 block compression**: All messages are LZ4 block-compressed (optimized for memory efficiency), but CyberChef may not detect this properly
3. **Batched data**: Multiple events are batched together, making manual parsing confusing

## Solution

Use the provided Go decoder which understands the protobuf schema and handles optimized LZ4 block decompression automatically.

## Usage

1. **Build the decoder:**
   ```bash
   go build decode_bundle.go
   ```

2. **Run with hex data:**
   ```bash
   # Direct hex string
   ./decode_bundle "0a70080112..."
   
   # Or from the wrapped format
   ./decode_bundle "STARTSTARTSTART
   0a70080112...
   ENDDATAENDDATENDDATA
   1750091269206
   ENDENDENEND"
   ```

3. **Run with file:**
   ```bash
   ./decode_bundle data.txt
   ```

## Output Format

The decoder will show:

- **Topics**: Metadata about each topic (only included when changed)
  - Tree ID (hash of topic info)
  - Level0 (root hierarchy level)
  - Location Sublevels (physical hierarchy)
  - Virtual Path (logical grouping)
  - Name (tag/signal name)
  - Data Contract (e.g., "_historian")
  - Metadata (additional headers)

- **Events**: Individual data points
  - UNS Tree ID (links to topic)
  - Type (Time Series or Relational)
  - Value and timestamp
  - Raw Kafka message payload
  - Processing breadcrumbs

- **Summary**: Count of topics and events

## Example Output

```
=== UNS Bundle Contents ===

📍 Topics (1):
  Tree ID: 60d25cb40cb329f5
    Level0: enterprise
    Location Sublevels: [plant1 machiningArea cnc-line cnc5 plc123]
    Virtual Path: axis.y
    Name: value
    Data Contract: _historian

📊 Events (50):
  Event #1:
    UNS Tree ID: 60d25cb40cb329f5
    Type: Relational
    JSON Data:
      {
        "timestamp_ms": 1750091269206,
        "value": "hello world"
      }
    Kafka Message Payload: {"timestamp_ms":1750091269206,"value":"hello world"}
    Bridged By: [umh_core]
    Produced At: 1750091269206 ms

📋 Summary: 1 topics, 50 events
```

## Understanding the Data Structure

The data you were seeing in CyberChef represents:

1. **Batched messages**: Multiple events processed together for efficiency
2. **Topic deduplication**: Topic metadata is only sent when it changes (using LRU cache)
3. **Block compression**: All bundles use optimized LZ4 block compression for memory efficiency and reduced network traffic
4. **Hierarchical structure**: UMH's ISA-95 based hierarchy (enterprise → plant → area → line → device)

This is normal behavior for the UMH topic browser processor - it's designed to minimize traffic by batching events and only sending topic changes when necessary. 