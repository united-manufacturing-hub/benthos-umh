# Stream Processor Plugin

The Stream Processor Plugin transforms incoming UMH timeseries messages using configurable JavaScript expressions and variable mappings.

## Features

- **UNS Message Processing**: Validates and processes UMH timeseries format messages
- **Variable Resolution**: Maps incoming topics to configured source variables
- **JavaScript Expression Evaluation**: Supports dynamic calculations using Goja runtime
- **Static & Dynamic Mappings**: Efficiently handles both constant values and variable-dependent calculations
- **Metadata Preservation**: Automatically preserves message metadata from input to output messages
- **Error Handling**: Fail-open policy ensures processing continues despite individual mapping failures

## Metadata Preservation

The plugin automatically preserves all metadata from the original input message to the output messages, with the following behavior:

- **`umh_topic`**: Overridden with the new output topic format: `<output_topic>.<data_contract>.<virtual_path>`
- **All other metadata**: Preserved as-is from the original message

This ensures that important context such as correlation IDs, trace IDs, source system information, and routing metadata flows through the processing pipeline.

### Example

Input message metadata:
```
umh_topic: "umh.v1.corpA.plant-A.aawd._raw.press"
correlation_id: "test-correlation-123"
source_system: "test-system"
priority: "high"
```

Output message metadata:
```
umh_topic: "umh.v1.corpA.plant-A.aawd._pump_v1.pressure"
correlation_id: "test-correlation-123"
source_system: "test-system"
priority: "high"
```

## Configuration

The plugin supports timeseries mode with the following configuration structure:

```yaml
mode: "timeseries"
output_topic: "umh.v1.corpA.plant-A.aawd"
model:
  name: "pump"
  version: "v1"
sources:
  press: "umh.v1.corpA.plant-A.aawd._raw.press"
  tF: "umh.v1.corpA.plant-A.aawd._raw.tempF"
  run: "umh.v1.corpA.plant-A.aawd._raw.run"
mapping:
  pressure: "press + 4.00001"
  temperature: "tF * 9 / 5 + 32"
  motor:
    rpm: "press / 4"
  serialNumber: '"SN-P42-008"'
  status: 'run ? "active" : "inactive"'
```

## JavaScript Engine

The plugin uses the Goja JavaScript runtime for expression evaluation, supporting:

- Mathematical operations
- Conditional expressions
- String manipulation
- Built-in JavaScript functions (Math, Date, etc.)
- Variable references from configured sources

## Testing

The plugin includes comprehensive test coverage:

- Unit tests for configuration validation
- Integration tests for end-to-end message processing
- Metadata preservation tests
- JavaScript expression evaluation tests
- Error handling and edge case tests

Run tests with:
```bash
go test -v .
``` 