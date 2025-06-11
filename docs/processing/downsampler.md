# Downsampler

The Downsampler processor reduces time-series data volume by filtering out insignificant changes using configurable algorithms. It integrates seamlessly with UMH's data pipeline after the tag_processor to compress historian data while preserving significant trends and changes.

Use the `downsampler` to reduce storage and transmission costs for high-frequency time-series data while maintaining data quality. This processor supports both UMH-core time-series format and UMH classic _historian format, automatically passing through non-historian messages unchanged.

## Key Features

- **Dual Algorithm Support**: Deadband and Swinging Door Trending (SDT) algorithms
- **Flexible Configuration**: Defaults + overrides pattern for easy multi-signal management
- **Pattern Matching**: Wildcard and regex patterns for topic-specific configuration
- **Data Type Handling**: Intelligent handling of numeric, boolean, and string values
- **Per-Key Filtering**: Independent processing of each field in multi-field messages
- **Industrial Best Practices**: Built-in parameter recommendations for various signal types
- **High Compression**: Achieve 90-99%+ data reduction while preserving signal fidelity
- **Fail-Safe Design**: Passes messages through unchanged on errors

## Supported Data Formats

The downsampler processes both UMH time-series data formats:

### UMH-Core Time-Series Format
Single "value" field with timestamp following the "one tag, one message, one topic" principle.  
📖 **Format details**: [UMH-Core Payload Formats](https://docs.umh.app/usage/unified-namespace/payload-formats)

### UMH Classic _historian Format  
Multiple fields in one JSON object with shared timestamp, identified by `data_contract: "_historian"`.  
📖 **Format details**: [UMH Classic Historian Data Contract](https://umh.docs.umh.app/docs/datacontracts/historian/)

For classic format messages, the downsampler processes each non-timestamp field as a separate data point with **per-key filtering**:
- Each metric field is evaluated independently against its specific configuration
- Keys that don't meet their threshold are **removed** from the message
- Keys that meet their threshold are **kept** in the message  
- Only if **all** measurement keys are dropped is the entire message dropped

## Configuration

The downsampler uses a **defaults + overrides** configuration pattern for flexible multi-signal management with **algorithm-specific sections**:

```yaml
pipeline:
  processors:
    - tag_processor:
        # ... tag processor configuration
    - downsampler:
        default:                     # Default parameters for all signals
          deadband:                  # Deadband algorithm parameters
            threshold: 0.2           # 2× typical sensor noise
            max_time: 1h             # Auditor heartbeat interval
          swinging_door:             # Swinging door algorithm parameters
            threshold: 0.2           # Same threshold concept as deadband
            min_time: 5s             # Noise reduction for fast processes
            max_time: 1h             # Auditor heartbeat interval
        overrides:                   # Signal-specific overrides
          - pattern: "*.temperature"
            deadband:
              threshold: 0.1         # Fine-grained for temperature (±0.05°C noise)
            swinging_door:
              threshold: 0.1         # Consistent threshold naming
          - pattern: ".+_counter"
            swinging_door:
              threshold: 0           # No compression for counters
          - pattern: "^temp_"
            deadband:
              threshold: 0.05        # Very sensitive temp sensors
```

### Configuration Structure

**Algorithm Selection:**
The algorithm is automatically determined by which configuration section contains values:
- If only `deadband` parameters are configured → uses deadband algorithm
- If only `swinging_door` parameters are configured → uses swinging door algorithm
- If both sections have parameters → defaults to deadband, but overrides can specify different algorithms per pattern

**Default Parameters:**
- `default.deadband.threshold`: Default threshold for significant changes (2× sensor noise)
- `default.deadband.max_time`: Default maximum time before forced output (auditor heartbeat)
- `default.swinging_door.threshold`: Default threshold for envelope deviation (same concept as deadband)
- `default.swinging_door.min_time`: Default minimum time between points (noise reduction)
- `default.swinging_door.max_time`: Default maximum time before forced output (auditor heartbeat)

**Override Patterns:**
- `overrides[].pattern`: Regex pattern to match topics (e.g., `"*.temperature"`, `".+_counter"`, `"^temp_"`)
- `overrides[].topic`: Exact topic match (mutually exclusive with pattern)
- Override any default parameter with signal-specific values
- Each override can specify different algorithms by including different config sections

### Algorithm Selection Examples

```yaml
# Pure deadband configuration
downsampler:
  default:
    deadband:
      threshold: 0.2           # 2× sensor noise
      max_time: 1h             # Auditor heartbeat
  overrides:
    - pattern: "*.temperature"
      deadband:
        threshold: 0.1         # Fine-grained temperature monitoring

# Pure swinging door configuration  
downsampler:
  default:
    swinging_door:
      threshold: 0.2           # Same threshold concept
      min_time: 5s             # Process-aware noise reduction
      max_time: 1h             # Auditor heartbeat
  overrides:
    - pattern: "*.temperature"
      swinging_door:
        threshold: 0.1         # Consistent threshold naming

# Mixed algorithm configuration
downsampler:
  default:
    deadband:                  # Default algorithm
      threshold: 0.2           # Standard noise threshold
      max_time: 1h             # Auditor compliance
  overrides:
    - pattern: "*.temperature"
      deadband:                # Override with deadband
        threshold: 0.1         # Temperature precision
    - pattern: "*.vibration"
      swinging_door:           # Override with swinging door
        threshold: 0.01        # Sensitive vibration monitoring
        min_time: 1s           # High-frequency vibration data
```

### Pattern Matching Examples

```yaml
overrides:
  # Wildcard patterns (using filepath.Match)
  - pattern: "*.temperature"        # Any topic ending in "temperature"
    deadband:
      threshold: 0.1                # Fine temperature control
  - pattern: "plant1.*"             # Any topic starting with "plant1."
    swinging_door:
      threshold: 0.05               # High precision for plant1
  
  # Regex patterns (for complex matching)
  - pattern: ".+_counter"           # Any topic ending in "_counter"
    swinging_door:
      threshold: 0                  # No compression for counters
  - pattern: "^temp_"               # Any topic starting with "temp_"
    deadband:
      threshold: 0.05               # Very sensitive temperature sensors
  - pattern: "(vibration|accelerometer)"  # Multiple alternatives
    swinging_door:
      threshold: 0.01               # Sensitive vibration monitoring
      min_time: 1s                  # High-frequency vibration data
  
  # Exact topic matching
  - topic: "umh.v1.plant1.line1._historian.critical_temperature"
    deadband:
      threshold: 0.02               # Very sensitive critical sensor
```

## Algorithms

### Deadband Algorithm

The deadband algorithm filters out changes smaller than a configured threshold by comparing each new value to the last output value. It's ideal for **simple noise filtering** and **discrete signal monitoring**.

**Parameters:**
- `threshold`: Minimum absolute change required to keep a data point *(equivalent to noise floor)*
- `max_time`: Maximum time before forcing output regardless of threshold *(auditor heartbeat)*

**How it works:**
1. **First Point**: Always kept (establishes baseline)
2. **Subsequent Points**: Compare to last kept value
   - If `|current_value - last_output| >= threshold` → **Keep**
   - If `|current_value - last_output| < threshold` → **Filter out**
3. **Max Time**: Forces output after time limit regardless of threshold (ensures auditor requirements)

**Example** (threshold = 0.5):
| Time | Input | Last Output | Change (Δ) | Decision | Reason |
|------|-------|-------------|------------|----------|---------|
| 00:00 | 10.0 | *none* | – | **Keep** | First point |
| 00:01 | 10.3 | 10.0 | 0.3 | Filter | 0.3 < 0.5 |
| 00:02 | 10.6 | 10.0 | 0.6 | **Keep** | 0.6 ≥ 0.5 |

**Tuning Recommendations:**
- **Threshold**: Set to **2× sensor noise level** (e.g., ±0.1 noise → threshold = 0.2)
- **Max Time**: Default **1 hour** for auditor compliance (periodic heartbeat)
- **Use Cases**: Boolean signals, simple analog filtering, discrete state monitoring

### Swinging Door Trending (SDT) Algorithm

The SDT algorithm maintains an "envelope" or "corridor" around the trend line from the last stored point, storing new points only when they would exceed the envelope boundaries. It provides **superior compression** for **continuous analog signals**.

**Parameters:**
- `threshold`: Maximum vertical deviation from the trend line *(same concept as deadband threshold)*
- `min_time`: Minimum time between stored points *(noise reduction for high-frequency processes)*
- `max_time`: Maximum time before forcing output *(auditor heartbeat, same as deadband)*

**How it works:**
1. **Start Segment**: First point establishes the base of a new segment
2. **Envelope Calculation**: For each new point, calculate upper and lower trend lines from base point
3. **Boundary Check**: If the point falls outside the envelope (±threshold), end current segment
4. **Segment End**: Store the previous point as segment end, start new segment from there
5. **Min Time Filter**: Points arriving faster than `min_time` are buffered/evaluated but not immediately stored

**Benefits over Deadband:**
- **Better Trend Preservation**: Maintains slope information, not just magnitude
- **Higher Compression**: Can achieve 95-99% reduction vs 90-95% for deadband
- **Temporal Awareness**: Considers time progression in compression decisions
- **Process-Aware**: `min_time` prevents false triggers from processes that can't change rapidly

**Tuning Recommendations:**
- **Threshold**: Set to **2× sensor noise level** (same principle as deadband)
- **Min Time**: Set to **fastest meaningful process change time** (0 = disabled, 5s typical)
- **Max Time**: Default **1 hour** for auditor compliance (periodic heartbeat)
- **Use Cases**: Continuous analog processes, temperature control, flow rates, pressure monitoring

**Out-of-Order & Bulk Processing:**
Both algorithms process messages **in timestamp order**. If messages arrive out-of-order:
- **Small delays** (seconds): Buffered and reordered automatically  
- **Large delays** (minutes): May cause envelope reset in SDT or threshold recalculation
- **Bulk arrivals**: Each message processed sequentially maintaining state integrity

**Auditor Requirements:**
The `max_time` parameter ensures **regulatory compliance** by forcing periodic data storage regardless of signal changes:
- **Heartbeat Function**: Guarantees data points at least every `max_time` interval
- **Audit Trail**: Proves system was operational even during stable periods
- **Compliance**: Meets FDA 21 CFR Part 11, GxP, and similar requirements for continuous monitoring

## Late Arrival Policy (`late_policy`)

### Why Late Arrival Handling is Critical

Real-world Kafka topics occasionally deliver messages whose timestamps are older than previously processed messages. This creates **out-of-order (OoO) arrival** scenarios that can break compression algorithms:

- **Deadband and Swinging Door algorithms assume strictly increasing timestamps** per time series
- **Feeding an out-of-order sample into the algorithm** would either silently violate error guarantees or corrupt slope calculations
- **Database connectors may create duplicates** if late data overwrites existing compressed points

The late arrival policy defines what happens **when `timestamp < last_processed_timestamp`**.

### Policy Options

| Policy | Behavior | Use Case | Metadata Added |
|--------|----------|----------|----------------|
| **`passthrough`** (default) | Skip compression, forward unchanged | Standard live ingest: nothing lost, minimal compression impact | `late_oos=true` |
| **`drop`** | Log warning and discard message | High-rate sensors where stale data is worthless | None (dropped) |

### Configuration Example

```yaml
pipeline:
  processors:
    - downsampler:
        default:
          deadband:
            threshold: 0.2
            max_time: 1h
          late_policy:
            late_policy: passthrough    # Default behavior
        overrides:
          - pattern: "*.alarm"
            late_policy:
              late_policy: drop        # Drop late alarms - time sensitive
          - pattern: "historical_.*"
            late_policy:
              late_policy: passthrough # Allow historical reprocessing
```