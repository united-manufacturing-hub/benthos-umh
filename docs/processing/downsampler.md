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

The downsampler uses a **defaults + overrides** configuration pattern for flexible multi-signal management:

```yaml
pipeline:
  processors:
    - tag_processor:
        # ... tag processor configuration
    - downsampler:
        algorithm: "deadband"        # Algorithm: "deadband" or "swinging_door"
        default:                     # Default parameters for all signals
          threshold: 2.0             # Default deadband threshold
          max_interval: 30s          # Default maximum time interval
          comp_dev: 0.5              # Default swinging door deviation
          comp_min_time: 5s          # Default minimum time interval
          comp_max_time: 1h          # Default maximum time interval
        overrides:                   # Signal-specific overrides
          - pattern: "*.temperature"
            threshold: 0.5           # Fine-grained for temperature
            comp_dev: 0.25
          - pattern: ".+_counter"
            comp_dev: 0              # No compression for counters
          - pattern: "^temp_"
            threshold: 0.25          # Very sensitive temp sensors
```

### Configuration Structure

**Algorithm Selection:**
- `algorithm`: Choose between `"deadband"` or `"swinging_door"`

**Default Parameters:**
- `default.threshold`: Default deadband threshold for significant changes
- `default.max_interval`: Default maximum time before forced output (deadband only)
- `default.comp_dev`: Default compression deviation for swinging door algorithm
- `default.comp_min_time`: Default minimum time between points (swinging door only)
- `default.comp_max_time`: Default maximum time before forced output (swinging door only)

**Override Patterns:**
- `overrides[].pattern`: Regex pattern to match topics (e.g., `"*.temperature"`, `".+_counter"`, `"^temp_"`)
- `overrides[].topic`: Exact topic match (mutually exclusive with pattern)
- Override any default parameter with signal-specific values

### Pattern Matching Examples

```yaml
overrides:
  # Wildcard patterns (using filepath.Match)
  - pattern: "*.temperature"        # Any topic ending in "temperature"
    threshold: 0.5
  - pattern: "plant1.*"             # Any topic starting with "plant1."
    comp_dev: 0.25
  
  # Regex patterns (for complex matching)
  - pattern: ".+_counter"           # Any topic ending in "_counter"
    comp_dev: 0                     # No compression for counters
  - pattern: "^temp_"               # Any topic starting with "temp_"
    threshold: 0.1
  - pattern: "(vibration|accelerometer)"  # Multiple alternatives
    comp_dev: 0.01
  
  # Exact topic matching
  - topic: "umh.v1.plant1.line1._historian.critical_temperature"
    threshold: 0.1                  # Very sensitive critical sensor
```

## Algorithms

### Deadband Algorithm

The deadband algorithm filters out changes smaller than a configured threshold by comparing each new value to the last output value.

**Parameters:**
- `threshold`: Minimum absolute change required to keep a data point
- `max_interval`: Maximum time before forcing output regardless of threshold

**How it works:**
1. **First Point**: Always kept (establishes baseline)
2. **Subsequent Points**: Compare to last kept value
   - If `|current_value - last_output| >= threshold` → **Keep**
   - If `|current_value - last_output| < threshold` → **Filter out**
3. **Max Interval**: Forces output after time limit regardless of threshold

**Example** (threshold = 0.5):
| Time | Input | Last Output | Change (Δ) | Decision | Reason |
|------|-------|-------------|------------|----------|---------|
| 00:00 | 10.0 | *none* | – | **Keep** | First point |
| 00:01 | 10.3 | 10.0 | 0.3 | Filter | 0.3 < 0.5 |
| 00:02 | 10.6 | 10.0 | 0.6 | **Keep** | 0.6 ≥ 0.5 |

### Swinging Door Trending (SDT) Algorithm

The SDT algorithm maintains an "envelope" or "corridor" around the trend line from the last stored point, storing new points only when they would exceed the envelope boundaries.

**Parameters:**
- `comp_dev`: Maximum vertical deviation from the trend line (compression deviation)
- `comp_min_time`: Minimum time between stored points (noise filtering)
- `comp_max_time`: Maximum time before forcing output (prevents stale data)

**How it works:**
1. **Start Segment**: First point establishes the base of a new segment
2. **Envelope Calculation**: For each new point, calculate upper and lower trend lines from base point
3. **Boundary Check**: If the point falls outside the envelope (±comp_dev), end current segment
4. **Segment End**: Store the previous point as segment end, start new segment from there

**Benefits over Deadband:**
- **Better Trend Preservation**: Maintains slope information, not just magnitude
- **Higher Compression**: Can achieve better compression ratios while preserving signal shape
- **Temporal Awareness**: Considers time progression in compression decisions

## Signal Type Recommendations

Based on industrial best practices, here are recommended starting parameters:

### Analog Process Variables

**Temperature, Pressure, Flow, Level Sensors:**

```yaml
default:
  threshold: 2.0          # Start with ~0.2% of span
  comp_dev: 0.5           # ~0.5% of span for SDT
  max_interval: 8h        # Force periodic archival
  comp_max_time: 8h
overrides:
  - pattern: "*.temperature"
    threshold: 0.5        # ±0.5°C for temperature
    comp_dev: 0.25        # Tight control for temperature
  - pattern: "*.pressure"
    threshold: 50.0       # ±50 Pa for pressure
    comp_dev: 25.0
  - pattern: "*.flow_rate"
    threshold: 2.0        # ±2 L/min for flow
    comp_dev: 1.0
```

**Guidelines:**
- Start with **0.1-0.5% of instrument span**
- Set threshold **≈ instrument noise level**
- Use SDT `comp_dev` about **2× deadband threshold**
- Consider instrument precision and process criticality

### Discrete Signals

**Boolean States, Digital I/O:**

```yaml
default:
  threshold: 0.5          # Catch all boolean changes (0→1 = Δ1.0)
  comp_dev: 0             # No SDT compression for discrete
overrides:
  - pattern: "*.status"
    threshold: 0          # Capture every state change
  - pattern: "*.alarm"
    threshold: 0          # Never miss alarms
  - pattern: "*.enabled"
    threshold: 0          # Track all enable/disable events
```

**Key Points:**
- **Capture every state change** - use threshold ≤ 0.5 for boolean signals
- SDT not typically used for discrete signals
- Boolean changes always have magnitude 1.0 (false=0, true=1)

### Counters and Totals

**Production Counters, Flow Totals, Accumulating Values:**

```yaml
default:
  threshold: 1.0          # Minimum meaningful increment
  comp_dev: 0.5           # Very tight SDT compression
overrides:
  - pattern: ".+_counter"
    threshold: 1.0        # Log every count change
    comp_dev: 0           # Disable SDT to preserve all increments
  - pattern: "*.total"
    threshold: 5.0        # Allow small totalizer variations
    comp_dev: 2.0         # Minimal compression for totals
```

**Critical Considerations:**
- **Avoid losing increments** - ensure threshold captures meaningful counts
- For critical counts, consider **disabling compression** entirely
- Monitor compression effectiveness to ensure totals remain accurate

## Tuning Guidelines

### Starting Parameters

**Conservative Defaults (High Fidelity):**
```yaml
default:
  threshold: 0.1          # Very sensitive - captures small changes
  comp_dev: 0.05          # Tight SDT envelope
  max_interval: 1h        # Frequent periodic updates
  comp_max_time: 1h
```

**Aggressive Compression (High Efficiency):**
```yaml
default:
  threshold: 1.0          # Less sensitive - filters minor changes
  comp_dev: 0.5           # Wider SDT envelope
  max_interval: 8h        # Infrequent periodic updates
  comp_max_time: 8h
```

### Iterative Tuning Process

1. **Start Conservative**: Begin with small thresholds (0.1-0.5% of span)
2. **Monitor Compression**: Check data volume reduction (aim for 90-99%)
3. **Verify Fidelity**: Compare compressed vs. raw signals for key events
4. **Adjust Gradually**: Increase thresholds if too noisy, decrease if missing events
5. **Document Settings**: Record final parameters and rationale

### Percentage of Span Formula

For instruments with known ranges:

```yaml
# Example: 0-100°C temperature sensor
# Target: 0.2% of span = 0.2°C threshold
overrides:
  - pattern: "*.temperature"
    threshold: 0.2        # 0.2% of 100°C span
    comp_dev: 0.1         # Half the threshold for SDT
```

### Noise-Based Tuning

For noisy signals:

```yaml
# Example: Sensor with ±0.1 unit noise
# Set threshold just above noise level
overrides:
  - pattern: "*.noisy_sensor"
    threshold: 0.15       # Slightly above ±0.1 noise
    comp_dev: 0.075       # Half threshold for SDT
```

## Configuration Examples

### Basic Multi-Signal Plant

```yaml
downsampler:
  algorithm: "deadband"
  default:
    threshold: 1.0
    max_interval: 4h
  overrides:
    - pattern: "*.temperature"
      threshold: 0.5      # ±0.5°C sensitivity
    - pattern: "*.pressure"
      threshold: 50.0     # ±50 Pa sensitivity
    - pattern: "*.humidity"
      threshold: 1.0      # ±1% RH sensitivity
    - pattern: "*.status"
      threshold: 0        # Capture all state changes
```

### Advanced SDT Configuration

```yaml
downsampler:
  algorithm: "swinging_door"
  default:
    comp_dev: 0.5
    comp_min_time: 5s
    comp_max_time: 8h
  overrides:
    - pattern: "*.temperature"
      comp_dev: 0.25      # Tight temperature control
      comp_max_time: 2h   # More frequent temperature updates
    - pattern: ".+_counter"
      comp_dev: 0         # No compression for counters
    - pattern: "*.vibration"
      comp_dev: 0.01      # Very sensitive vibration monitoring
      comp_min_time: 1s   # Allow rapid vibration changes
```

### Production Line Monitoring

```yaml
downsampler:
  algorithm: "deadband"
  default:
    threshold: 1.0
    max_interval: 6h
  overrides:
    # Critical production parameters
    - pattern: "production.line1.*"
      threshold: 0.1      # High sensitivity for main line
    - pattern: "quality.*"
      threshold: 0.05     # Very sensitive quality metrics
    
    # Utility monitoring
    - pattern: "utility.*"
      threshold: 2.0      # Less sensitive for utilities
    
    # State monitoring
    - pattern: "*.running"
      threshold: 0        # Never miss state changes
    - pattern: "*.alarm"
      threshold: 0        # Never miss alarms
```

## Testing and Validation

### Built-in Examples

The downsampler includes comprehensive test configurations:

```bash
# Test deadband algorithm with classic format
make test-benthos-downsampler-classic

# Test deadband algorithm with multi-signal generation
make test-benthos-downsampler

# Test swinging door algorithm
make test-benthos-downsampler-swinging-door
```

### Validation Checklist

**Data Fidelity:**
- [ ] Critical events (alarms, state changes) are always captured
- [ ] Trend direction and magnitude are preserved
- [ ] No significant process changes are filtered out

**Compression Effectiveness:**
- [ ] Data volume reduced by 90-99% where appropriate
- [ ] Storage and bandwidth costs reduced
- [ ] Processing performance improved

**Signal-Specific Verification:**
- [ ] Boolean signals capture every state change
- [ ] Counter increments are not lost
- [ ] Analog signals filter noise but preserve trends
- [ ] Critical sensors use appropriate thresholds

## Error Handling and Resilience

The downsampler follows a **fail-open** design philosophy:

**Error Conditions:**
- Invalid configuration → use safe defaults
- Missing metadata → pass through unchanged
- Algorithm errors → pass through unchanged
- Invalid data types → attempt conversion, pass through on failure

**Logging and Debugging:**
- Filtered messages logged at DEBUG level
- Configuration parsing logged at INFO level
- Errors logged at ERROR level with context
- Per-algorithm debug messages for tuning

**Performance Monitoring:**
- Track compression ratios per signal type
- Monitor processing latency
- Alert on unexpected pass-through rates

## Advanced Topics

### Multi-Tenant Configuration

```yaml
downsampler:
  algorithm: "deadband"
  default:
    threshold: 1.0
  overrides:
    # Tenant-specific configurations
    - pattern: "tenant1.*"
      threshold: 0.1      # High-precision tenant
    - pattern: "tenant2.*"
      threshold: 2.0      # Cost-optimized tenant
```

### Algorithm Comparison

| Aspect | Deadband | Swinging Door |
|--------|----------|---------------|
| **Compression** | Good (90-95%) | Better (95-99%) |
| **Trend Preservation** | Magnitude only | Magnitude + slope |
| **Configuration** | Simple (threshold) | Complex (deviation + times) |
| **CPU Usage** | Low | Medium |
| **Best For** | Simple filtering | Complex signal shapes |

### Performance Considerations

**Memory Usage:**
- Each unique topic maintains algorithm state
- SDT requires more state than deadband
- Consider memory limits with thousands of signals

**Processing Overhead:**
- Deadband: O(1) per point
- SDT: O(1) per point, but higher constant factor
- Pattern matching: O(n) where n = number of override patterns

**Scalability:**
- Tested with 10,000+ concurrent time series
- State cleanup on inactivity prevents memory leaks
- Consider horizontal scaling for extreme loads