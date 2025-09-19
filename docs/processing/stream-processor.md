# Stream Processor

The Stream Processor collects timeseries data from multiple UNS sources, maintains state for variable mappings, and generates transformed messages using JavaScript expressions.

## When to Use This Processor

Use Stream Processor when you need to combine data from multiple UNS sources. Example: Calculate pump efficiency from separate flow and temperature sensors.

**Required input format**: UMH timeseries (`value` + `timestamp_ms`)

## Configuration

```yaml
processors:
  - stream_processor:
      mode: "timeseries"              # Only "timeseries" supported
      output_topic: "umh.v1.enterprise.site.area"  # Base output topic
      model:
        name: "pump"                  # Model name for data contract
        version: "v1"                 # Model version
      sources:                        # Map variables to UNS topics
        press: "umh.v1.enterprise.site.area._raw.pressure"
        temp: "umh.v1.enterprise.site.area._raw.temperature"
        run: "umh.v1.enterprise.site.area._raw.running"
      mapping:                        # JavaScript expressions
        # Static (no dependencies)
        serialNumber: '"SN-P42-008"'
        # Dynamic (with dependencies)
        pressure: "press + 4.00001"              # Depends on 'press'
        temperatureC: "(temp - 32) * 5/9"        # Depends on 'temp'
        motor:
          rpm: "press / 4"                       # Nested, depends on 'press'
        status: 'run ? "active" : "inactive"'    # Depends on 'run'
        efficiency: "(press / temp) * 100"       # Depends on both
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `mode` | string | Yes | Only `"timeseries"` supported |
| `output_topic` | string | Yes | Base UMH topic for output |
| `model.name` | string | Yes | Model name for data contract |
| `model.version` | string | Yes | Model version |
| `sources` | map | Yes | Variable name → UNS topic |
| `mapping` | object | No | JavaScript expressions |

## How It Works

### Dependency Tracking

- **Static mappings**: Expressions without variables evaluate on every message (e.g., `"SN-12345"`)
- **Dynamic mappings**: Expressions with variables evaluate when those variables update (e.g., `press * 2`)
- **Multiple dependencies**: Wait for all variables before evaluating (e.g., `press / temp`)

### JavaScript Expression Engine

The Stream Processor uses the same underlying [goja](https://github.com/dop251/goja) JavaScript engine as the [Node-RED JavaScript Processor](node-red-javascript-processor.md), but with different configurations optimized for different use cases:

**Stream Processor JavaScript:**
- Evaluates single expressions (not full functions)
- Pre-compiles at startup
- Auto-detects variable dependencies
- Blocks dangerous operations (eval, require, process)

**Comparison with Node-RED JavaScript Processor:**
- Node-RED JS runs full JavaScript functions with `return` statements
- Stream Processor evaluates expressions directly (no function wrapper needed)
- Both use security sandboxing to prevent dangerous operations
- Node-RED JS allows console.log() for debugging, Stream Processor blocks it
- Stream Processor adds expression pre-compilation and dependency analysis

**Security Features (shared with Node-RED JS):**
- Maximum call stack size limits to prevent stack overflow
- Blocked dangerous globals (eval, Function, require, process, setTimeout)
- Safe built-in objects available (Math, JSON, Date, String, Number)
- Execution within a sandboxed environment

### State Management

- Stores latest value for each configured source
- Values persist until processor restarts
- Enables calculations across async data sources

## Message Processing

### Input Requirements

1. **Format**: `{"value": 25.5, "timestamp_ms": 1647890123456}`
2. **Metadata**: Must have `umh_topic` field
3. **Topic Match**: Must match a configured source

### Processing Steps

1. Match `umh_topic` to configured sources
2. Store value in state (e.g., `press = 25.5`)
3. Evaluate static mappings
4. Evaluate dynamic mappings with satisfied dependencies
5. Generate output messages

### Output Topics

Format: `<output_topic>.<data_contract>[.<virtual_path>].<name>`

- `output_topic`: From configuration
- `data_contract`: `_<model_name>_<model_version>`
- `virtual_path`: For nested mappings
- `name`: Field name

Examples:
- `umh.v1.enterprise.site.area._pump_v1.pressure`
- `umh.v1.enterprise.site.area._pump_v1.motor.rpm` (nested)

## JavaScript Expressions

### Allowed Operations

```javascript
// Math
Math.PI * 2
Math.sqrt(press * press + temp * temp)

// Arithmetic
press * 2 + 1
(temp - 32) * 5/9

// Conditionals
press > 100 ? 'high' : 'normal'
run ? 'active' : 'inactive'

// Strings
"Pump-" + press
`Temperature: ${temp}°C`

// JSON
JSON.stringify({pressure: press, temp: temp})

// Date
Date.now()
```

### Blocked Operations

- `eval()` - No dynamic code
- `Function()` - No function constructors
- `require()` - No module imports
- `process` - No process access
- `setTimeout/setInterval` - No async
- `console` - No console access

## Examples

### Sensor Calibration

```yaml
stream_processor:
  mode: "timeseries"
  output_topic: "umh.v1.plant1.line5.cell3"
  model:
    name: "sensor_calibrated"
    version: "v1"
  sources:
    raw_temp: "umh.v1.plant1.line5.cell3._raw.temperature"
    raw_press: "umh.v1.plant1.line5.cell3._raw.pressure"
  mapping:
    temperature: "raw_temp + 0.5"  # Offset correction
    pressure: "raw_press * 1.02"   # Scale correction
```

### Machine Status

```yaml
stream_processor:
  mode: "timeseries"
  output_topic: "umh.v1.factory.assembly"
  model:
    name: "machine_status"
    version: "v2"
  sources:
    speed: "umh.v1.factory.assembly._raw.conveyor_speed"
    temp: "umh.v1.factory.assembly._raw.motor_temp"
    running: "umh.v1.factory.assembly._raw.is_running"
  mapping:
    status: |
      running ? (temp > 80 ? "warning" : "running") : "stopped"
    alert: "temp > 90"
    efficiency: "running ? (speed / 100) * 100 : 0"
```

### Boiler Efficiency

```yaml
stream_processor:
  mode: "timeseries"
  output_topic: "umh.v1.site.boiler"
  model:
    name: "efficiency_calc"
    version: "v1"
  sources:
    flow_in: "umh.v1.site.boiler._raw.input_flow"
    flow_out: "umh.v1.site.boiler._raw.output_flow"
    temp_in: "umh.v1.site.boiler._raw.input_temp"
    temp_out: "umh.v1.site.boiler._raw.output_temp"
  mapping:
    flow_diff: "flow_out - flow_in"
    temp_diff: "temp_out - temp_in"
    thermal_efficiency: |
      (flow_out * temp_out - flow_in * temp_in) / (flow_in * temp_in) * 100
    heat_transfer: "(flow_out - flow_in) * (temp_out - temp_in) * 500"
```

### Nested Metrics

```yaml
stream_processor:
  mode: "timeseries"
  output_topic: "umh.v1.plant.packaging"
  model:
    name: "machine_metrics"
    version: "v3"
  sources:
    speed: "umh.v1.plant.packaging._raw.belt_speed"
    count: "umh.v1.plant.packaging._raw.package_count"
    weight: "umh.v1.plant.packaging._raw.total_weight"
  mapping:
    performance:
      speed: "speed"
      throughput: "count / 60"  # packages/min
    quality:
      avg_weight: "weight / count"
      deviation: "Math.abs((weight / count) - 1.5)"  # from target
```

Output topics:
- `umh.v1.plant.packaging._machine_metrics_v3.performance.speed`
- `umh.v1.plant.packaging._machine_metrics_v3.performance.throughput`
- `umh.v1.plant.packaging._machine_metrics_v3.quality.avg_weight`
- `umh.v1.plant.packaging._machine_metrics_v3.quality.deviation`

## Processing Behavior

### Static vs Dynamic Evaluation

**Static** (every message): `"SN-12345"`, `Date.now()`, `Math.PI`

**Dynamic** (when dependencies update): `press * 2`, `press / temp`, `run ? "active" : "stopped"`

### Metadata Preservation

- `umh_topic`: Replaced with output topic
- All other metadata: Passed through

### Error Handling

- **Invalid JSON**: "Invalid timeseries format from umh.v1.plant.sensor - expected {value, timestamp_ms}. Skipping message."
- **Missing Dependencies**: "Cannot evaluate 'efficiency' - waiting for 'temp' (last value: 65.2 from 30s ago)"
- **JavaScript Error**: "Failed to evaluate 'motor.rpm': Cannot divide by zero (press=0). Using last valid value: 1250."
- **Missing umh_topic**: "Message missing umh_topic metadata. Check input configuration."

## Troubleshooting

### No Output Messages

**Check umh_topic exists:**
```bash
# Verify metadata in your pipeline
echo '{"value": 42}' | benthos -c your_config.yaml
# Should show: Message missing umh_topic metadata
```

**Verify topic matches source:**
```yaml
sources:
  press: "umh.v1.plant._raw.pressure"  # Must match exactly
```

### Missing Outputs

**Check dependencies:**
- `efficiency: "press / temp"` needs both variables
- Look for "waiting for 'temp'" in logs

**Test expressions:**
```javascript
// Test in browser console first
var press = 100, temp = 50;
console.log(press / temp);  // Should be 2
```

### Performance Issues

- Simplify expressions: `press * 2` instead of complex math
- Check message rate: >1000 msg/s may need optimization
- Monitor memory if storing many variables

### Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| "undefined is not a function" | Using blocked function | Use allowed operations only |
| "variable not defined" | Typo in variable name | Check sources configuration |
| "unexpected token" | JavaScript syntax error | Test in browser console |
| "Cannot read null" | Missing null check | Add: `temp ? temp * 2 : 0` |