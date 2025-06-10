# Downsampler

The Downsampler processor reduces time-series data volume by filtering out insignificant changes using configurable algorithms. It integrates seamlessly with UMH's data pipeline after the tag_processor to compress historian data while preserving significant trends and changes.

Use the `downsampler` to reduce storage and transmission costs for high-frequency time-series data while maintaining data quality. This processor supports both UMH-core time-series format and UMH classic _historian format, automatically passing through non-historian messages unchanged.

## Supported Data Formats

The downsampler processes both UMH time-series data formats:

### UMH-Core Time-Series Format
Single "value" field with timestamp following the "one tag, one message, one topic" principle.  
📖 **Format details**: [UMH-Core Payload Formats](https://docs.umh.app/usage/unified-namespace/payload-formats)

### UMH Classic _historian Format  
Multiple fields in one JSON object with shared timestamp, identified by `data_contract: "_historian"`.  
📖 **Format details**: [UMH Classic Historian Data Contract](https://umh.docs.umh.app/docs/datacontracts/historian/)

For classic format messages, the downsampler processes each non-timestamp field as a separate data point with **per-key filtering**:
- Each metric field is evaluated independently against its specific threshold
- Keys that don't meet their threshold are **removed** from the message
- Keys that meet their threshold are **kept** in the message  
- Only if **all** measurement keys are dropped is the entire message dropped
- Nested values are not supported yet (only flat key-value pairs)

**Key Features**

- **Dual Format Support**: Processes both UMH-core and classic _historian formats
- **Selective Processing**: Processes UMH-Core time-series messages and UMH Classic messages with `data_contract` = `_historian`
- **Deadband Algorithm**: Filters out changes smaller than a configured threshold
- **Max Interval Support**: Forces periodic output to prevent "stale" data appearance
- **Data Type Handling**: Supports numeric, boolean, and string values
- **Fail-Safe Design**: Passes messages through unchanged on errors
- **Metadata Annotation**: Tags processed messages for downstream transparency

**Pipeline Integration**

The downsampler is designed to be placed **after the tag_processor** in UMH pipelines:

```
[Data Source] → tag_processor → downsampler → [Output]
```

The tag_processor ensures data is in the correct UMH format with required metadata (`umh_topic`, `data_contract`, etc.) before downsampling is applied.

## Configuration

```yaml
pipeline:
  processors:
    - tag_processor:
        # ... tag processor configuration
    - downsampler:
        algorithm: "deadband"        # Algorithm to use
        threshold: 0.5               # Default threshold
        max_interval: "60s"          # Optional: force output interval
        # Optional: Topic-specific thresholds
        topic_thresholds:
          - pattern: "*.temperature"   # Pattern matching
            threshold: 0.5
          - topic: "exact.topic.name"  # Exact matching
            threshold: 1.0
```

**Configuration Fields:**

* `algorithm`: Downsampling algorithm to use
  * Currently supported: `"deadband"`
  * Future: `"swinging_door"` (Swinging Door Trending)
* `threshold`: **Default** threshold for determining significant changes
  * For deadband: minimum absolute change required to keep a data point
  * Must be non-negative (negative values treated as 0)
  * Used when no topic-specific threshold matches
* `max_interval`: *(optional)* Maximum time interval before forcing output
  * Only applies to deadband algorithm
  * Prevents data from appearing "stale" during slow changes
  * Format: duration string (e.g., "60s", "5m", "1h")
* `topic_thresholds`: *(optional)* List of topic-specific threshold configurations
  * Each entry must have either `pattern` **or** `topic` (mutually exclusive)
  * **`pattern`**: Wildcard pattern using `*` (e.g., `"*.temperature"`, `"plant1.*.pressure"`)
  * **`topic`**: Exact topic match for precise control
  * **`threshold`**: Threshold value for this pattern/topic
  * **Priority**: Exact topic matches take precedence over patterns
  * **Fallback**: Uses default `threshold` if no matches found

### Topic-Specific Thresholds

The `topic_thresholds` feature allows different thresholds for different metric types, essential when dealing with sensors measuring different physical quantities:

**Pattern Matching Examples:**
```yaml
topic_thresholds:
  - pattern: "*.temperature"      # Matches any topic ending in "temperature" 
    threshold: 0.5                # 0.5°C threshold
  - pattern: "*.pressure"         # Matches any topic ending in "pressure"
    threshold: 50.0               # 50 Pa threshold  
  - pattern: "plant1.*.vibration" # Matches vibration sensors in plant1
    threshold: 0.01               # 0.01 mm/s threshold
```

**Exact Topic Matching Examples:**
```yaml
topic_thresholds:
  - topic: "umh.v1.plant1.line1._historian.temperature"
    threshold: 0.3                # Tight control for critical sensor
  - topic: "umh.v1.plant2.line1._historian.temperature"  
    threshold: 0.8                # Looser control for non-critical sensor
```

**Combined Configuration:**
```yaml
downsampler:
  algorithm: "deadband"
  threshold: 1.0                  # Default fallback
  topic_thresholds:
    # Exact matches (highest priority)
    - topic: "umh.v1.plant1.line1._historian.temperature"
      threshold: 0.2              # Very sensitive critical sensor
    
    # Pattern matches
    - pattern: "*.temperature"
      threshold: 0.5              # General temperature sensors
    - pattern: "*.pressure"
      threshold: 50.0             # Pressure sensors
    - pattern: "*.flow_rate"
      threshold: 2.0              # Flow rate sensors (L/min)
    - pattern: "*.speed"
      threshold: 10.0             # Speed/RPM sensors
```

**Matching Priority:**
1. **Exact topic match** (highest priority)
2. **First matching pattern** (order matters)
3. **Default threshold** (fallback)

## Algorithms

### Deadband Algorithm

The deadband algorithm filters out changes smaller than a configured threshold by comparing each new value to the last output value for the same time series.

**How it works:**

1. **First Point**: Always kept (establishes baseline)
2. **Subsequent Points**: Compare to last kept value
   - If `|current_value - last_output| >= threshold` → **Keep**
   - If `|current_value - last_output| < threshold` → **Filter out**
3. **Max Interval**: If configured, forces output after time limit regardless of threshold

**Error Bounds**: The algorithm guarantees that the maximum error never exceeds the configured threshold. When a change finally exceeds the threshold, the error resets to zero.

**Example Behavior** (threshold = 0.5):

| Time | Input Value | Last Output | Change (Δ) | Decision | Reason |
|------|-------------|-------------|------------|----------|---------|
| 00:00 | 10.0 | *none* | – | **Keep** | First point |
| 00:01 | 10.3 | 10.0 | 0.3 | Filter | 0.3 < 0.5 |
| 00:02 | 10.6 | 10.0 | 0.6 | **Keep** | 0.6 ≥ 0.5 |
| 00:03 | 11.1 | 10.6 | 0.5 | **Keep** | 0.5 ≥ 0.5 |
| 00:04 | 11.0 | 11.1 | 0.1 | Filter | 0.1 < 0.5 |

### Data Type Handling

The downsampler handles different data types appropriately:

**Numeric Values** (int, float):
- Direct threshold comparison using absolute difference
- Preserves precision and type information

**Boolean Values**:
- Converted to 0/1 for comparison (false=0, true=1)
- Boolean changes always have Δ=1, so threshold should be ≤1 to capture state changes
- Any boolean state change is typically kept regardless of threshold

**String Values**:
- Compared for exact equality
- Different strings are always kept (treated as significant change)
- Identical strings are filtered out (no change)

**Complex Values**:
- Non-historian messages with complex payloads pass through unchanged
- Historian messages should contain simple values per UMH data model

## Message Flow and Metadata

**Input Requirements:**
- Messages must have `data_contract` metadata
- Historian messages must have `umh_topic` metadata
- Historian payloads must contain `timestamp_ms` and a value field

**Processing Logic:**

**Message Detection:**
1. **Time-series messages**: Must have `data_contract: "_historian"` metadata
2. **UMH-Core Format**: Detected by presence of "value" field in structured payload  
3. **UMH Classic Format**: Detected by multiple fields (no "value" field) in structured payload
4. **Non-time-series messages**: Pass through unchanged (no processing)

> **Note**: Currently both UMH-Core and UMH Classic formats require the `data_contract: "_historian"` metadata. This differs from the pure UMH-Core specification which should auto-detect based on payload structure alone. Future versions may support auto-detection for UMH-Core format.

**UMH-Core Format Processing:**
1. Extract series ID from topic, timestamp, and value
2. Apply downsampling algorithm with configured threshold
3. If kept → add `downsampled_by` metadata and forward
4. If filtered → drop message (log at debug level)

**UMH Classic Format Processing:**
1. Process each non-timestamp field individually:
   - Create series ID: `{umh_topic}.{field_name}`
   - Apply downsampling algorithm per field
   - Keep fields that meet threshold, remove fields that don't
2. If any fields kept → create new message with filtered data
3. If no fields kept → drop entire message
4. Add `downsampled_by` metadata indicating filtering statistics

**Output Annotation:**
Processed messages receive a `downsampled_by` metadata field:

**UMH-core Format:**
```
downsampled_by: "deadband(threshold=0.500)"
downsampled_by: "deadband(threshold=0.500,max_interval=1m0s)"
```

**UMH Classic Format (per-key filtering):**
```
downsampled_by: "deadband(filtered_3_of_5_keys)"
downsampled_by: "deadband(filtered_2_of_4_keys)"
```

This allows downstream systems to identify and understand the compression applied, including how many fields were filtered from multi-field messages.

## Per-Key Filtering (UMH Classic Format)

When processing UMH classic _historian messages with multiple fields, the downsampler applies filtering at the **field level** rather than the **message level**. This provides fine-grained control over which metrics are stored while preserving related measurements that change significantly.

**Example Behavior:**

Input message:
```json
{
  "temperature": 25.2,
  "pressure": 1001.5, 
  "humidity": 60.8,
  "status": "RUNNING",
  "timestamp_ms": 1733904005000
}
```

With topic-specific thresholds:
```yaml
topic_thresholds:
  - pattern: "*.temperature"
    threshold: 0.5     # 0.5°C sensitivity
  - pattern: "*.pressure"
    threshold: 5.0     # 5.0 Pa sensitivity
  - pattern: "*.humidity"
    threshold: 1.0     # 1.0% sensitivity
```

**Filtering Decision (assuming previous values: temp=25.0, pressure=1000.0, humidity=60.0, status="RUNNING"):**

| Field | Current | Previous | Change (Δ) | Threshold | Keep? | Reason |
|-------|---------|----------|------------|-----------|-------|---------|
| temperature | 25.2 | 25.0 | 0.2°C | 0.5°C | ❌ Drop | 0.2 < 0.5 |
| pressure | 1001.5 | 1000.0 | 1.5 Pa | 5.0 Pa | ❌ Drop | 1.5 < 5.0 |
| humidity | 60.8 | 60.0 | 0.8% | 1.0% | ❌ Drop | 0.8 < 1.0 |
| status | "RUNNING" | "RUNNING" | N/A | N/A | ❌ Drop | String unchanged |

**Result**: Entire message dropped (no fields remain after filtering)

**Alternative Scenario** (humidity changes to 61.5%):

| Field | Current | Previous | Change (Δ) | Threshold | Keep? | Reason |
|-------|---------|----------|------------|-----------|-------|---------|
| temperature | 25.2 | 25.0 | 0.2°C | 0.5°C | ❌ Drop | 0.2 < 0.5 |
| pressure | 1001.5 | 1000.0 | 1.5 Pa | 5.0 Pa | ❌ Drop | 1.5 < 5.0 |
| humidity | 61.5 | 60.0 | 1.5% | 1.0% | ✅ Keep | 1.5 ≥ 1.0 |
| status | "RUNNING" | "RUNNING" | N/A | N/A | ❌ Drop | String unchanged |

**Output message:**
```json
{
  "humidity": 61.5,
  "timestamp_ms": 1733904005000
}
```

**Series State Management:**
Each field creates its own series state with unique identifiers:
- `umh.v1.plant1.line1._historian.temperature`
- `umh.v1.plant1.line1._historian.pressure`  
- `umh.v1.plant1.line1._historian.humidity`
- `umh.v1.plant1.line1._historian.status`

This allows independent threshold tracking and state management per metric.

**Limitations:**
- **No Nested Values**: Only flat key-value pairs are supported
- **Timestamp Preservation**: `timestamp_ms` is always kept
- **Meta Field Exclusion**: `meta` fields are ignored if present

## Use Cases and Benefits

**High-Frequency Sensors**: Reduce data volume from sensors that report minor fluctuations
- Temperature sensors with ±0.1°C noise → set threshold to 0.2°C
- Pressure readings with small variations → filter sub-significant changes

**Storage Cost Reduction**: Decrease database storage requirements while preserving trends
- 1000 Hz data → potentially 10x reduction with proper threshold
- Maintain data quality for analysis and visualization

**Network Bandwidth**: Reduce data transmission in bandwidth-constrained environments
- Edge computing scenarios with limited connectivity
- Cost optimization for cloud data ingestion

**Regulatory Compliance**: Configure max_interval to ensure audit trail completeness
- Force periodic recording even during stable periods
- Maintain time-based data density requirements

## Configuration Examples

**Basic Temperature Monitoring:**
```yaml
downsampler:
  algorithm: "deadband"
  threshold: 0.5  # 0.5°C minimum change
```

**Multi-Metric Configuration with Topic-Specific Thresholds:**
```yaml
downsampler:
  algorithm: "deadband"
  threshold: 0.1                  # Default fallback threshold
  max_interval: "5m"             # Force output every 5 minutes
  topic_thresholds:
    - pattern: "*.temperature"
      threshold: 0.5              # 0.5°C for temperature sensors
    - pattern: "*.pressure" 
      threshold: 50.0             # 50 Pa for pressure sensors
    - pattern: "*.vibration"
      threshold: 0.01             # 0.01 mm/s for vibration sensors
    - pattern: "*.speed"
      threshold: 10.0             # 10 RPM for speed sensors
```

**Plant-Specific Configuration:**
```yaml
downsampler:
  algorithm: "deadband"
  threshold: 1.0                  # Default threshold
  topic_thresholds:
    # Critical production line - tight control
    - topic: "umh.v1.plant1.production.line1._historian.temperature"
      threshold: 0.2
    - topic: "umh.v1.plant1.production.line1._historian.pressure"
      threshold: 25.0
    
    # General temperature sensors - normal control  
    - pattern: "*.temperature"
      threshold: 0.5
    
    # Quality control sensors - very sensitive
    - pattern: "*.quality.*"
      threshold: 0.05
```

**High-Resolution with Periodic Backup:**
```yaml
downsampler:
  algorithm: "deadband"
  threshold: 0.1
  max_interval: "5m"  # Ensure point every 5 minutes
```

**Boolean State Monitoring:**
```yaml
downsampler:
  algorithm: "deadband"
  threshold: 0.5  # Capture all boolean changes (Δ=1 > 0.5)
```

**Conservative Setting for Critical Data:**
```yaml
downsampler:
  algorithm: "deadband"
  threshold: 0.01
  max_interval: "30s"  # Very sensitive with frequent backup
```

## Error Handling and Resilience

The downsampler follows a **fail-open** design philosophy:

**Error Conditions:**
- Missing `umh_topic` metadata → pass through unchanged
- Missing `timestamp_ms` in payload → pass through unchanged  
- Missing value field → pass through unchanged
- Invalid data types → attempt conversion, pass through on failure
- Algorithm errors → pass through unchanged

**Logging:**
- Filtered messages logged at DEBUG level
- Errors logged at ERROR level with context
- First-time series processing logged at INFO level

**Metrics:**
- `messages_processed`: Successfully downsampled messages
- `messages_filtered`: Messages filtered out by algorithm
- `messages_passed_through`: Non-historian or error messages
- `messages_errored`: Messages with processing errors