# ENG-3099 Notes: Real-World Data Model Context

## Overview

This document provides context on the actual data models that will be transformed into the JSON schemas used by our dual validation system in the `uns_output` plugin.

## Data Model Structure Examples

### Example 1: Simple Sensor Data

**Source Data Model:**
```yaml
dataModels:
  - name: sensor-data
    version:
      1:
        description: Initial version
        structure:
          value:
            payloadType: number
            type: timeseries
      2:
        description: Extended version with timestamp
        structure:
          value:
            payloadType: number
            type: timeseries
          timestamp:
            payloadType: string
            type: timeseries
          metadata:
            _model: sensor-metadata
```

**Transformed to Our Schema:**

**Version 1 Schema:**
```json
{
   "virtual_path": ["value"],
   "fields": {
      "value": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      }
   }
}
```

**Version 2 Schema:**
```json
{
   "virtual_path": ["value", "timestamp", "metadata"],
   "fields": {
      "value": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "timestamp": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "string"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "metadata": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "object"}
         },
         "required": ["timestamp_ms", "value"]
      }
   }
}
```

### Example 2: Complex Nested Model

**Source Data Model:**
```yaml
dataModels:
  - name: complex-model
    version:
      1:
        description: Complex nested data model
        structure:
          sensor:
            payloadType: string
            type: timeseries
            subfields:
              temp_reading:
                payloadType: number
                type: timeseries
              temp_unit:
                payloadType: string
                type: timeseries
                _model: temperature
          metadata:
            _model: device-info
```

**Transformed to Our Schema:**
> **Note:** The `metadata` field is not included in the transformed schema because it only has `_model: device-info` (a reference to another schema) without `type: timeseries`. Only fields with `type: timeseries` are included in the virtual_path and fields for direct validation.
```json
{
   "virtual_path": ["sensor", "sensor.temp_reading", "sensor.temp_unit"],
   "fields": {
      "sensor": {
         "type": "object", 
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "string"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "sensor.temp_reading": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"}, 
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "sensor.temp_unit": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "string"}
         },
         "required": ["timestamp_ms", "value"]  
      }
   }
}
```

## Key Mapping Rules

### 1. Virtual Path Generation

**Simple Structure:**
- `value` → `["value"]`

**Nested Structure:**
- `sensor` + `sensor.temp_reading` + `sensor.temp_unit` → `["sensor", "sensor.temp_reading", "sensor.temp_unit"]`

### 2. Payload Type Mapping

| Data Model `payloadType` | JSON Schema `type` |
|-------------------------|-------------------|
| `number` | `"number"` |
| `string` | `"string"` |
| `boolean` | `"boolean"` |

### 3. Timeseries Standard Structure

All fields with `type: timeseries` get transformed to:
```json
{
   "type": "object",
   "properties": {
      "timestamp_ms": {"type": "number"},
      "value": {"type": "[payloadType]"}
   },
   "required": ["timestamp_ms", "value"]
}
```

## Validation Flow Examples

### Example 1: Valid Message

**UNS Topic:** `umh.v1.enterprise.site.area._historian.sensor.temp_reading`
**Tag Name Extracted:** `sensor.temp_reading`
**Contract:** `_historian`

**Validation Steps:**
1. ✅ Check if `sensor.temp_reading` exists in `virtual_path`
2. ✅ Validate payload against `fields["sensor.temp_reading"]` schema

**Payload:**
```json
{
   "timestamp_ms": 1680000000000,
   "value": 23.5
}
```

**Result:** ✅ Passes both tag name and payload validation

### Example 2: Invalid Tag Name

**UNS Topic:** `umh.v1.enterprise.site.area._historian.unknown_sensor`
**Tag Name Extracted:** `unknown_sensor`

**Validation Steps:**
1. ❌ `unknown_sensor` not found in `virtual_path: ["sensor", "sensor.temp_reading", "sensor.temp_unit"]`

**Result:** ❌ Rejected with "tag 'unknown_sensor' not allowed for contract '_historian'. Allowed tags: [sensor, sensor.temp_reading, sensor.temp_unit]"

### Example 3: Invalid Payload

**UNS Topic:** `umh.v1.enterprise.site.area._historian.sensor.temp_reading`
**Tag Name:** `sensor.temp_reading` ✅
**Payload:**
```json
{
   "timestamp_ms": "not_a_number",
   "value": 23.5
}
```

**Validation Steps:**
1. ✅ Tag name validation passes
2. ❌ Payload validation fails: `timestamp_ms` expected number, got string

**Result:** ❌ Rejected with "payload validation failed for tag 'sensor.temp_reading': timestamp_ms: expected number, got string"

## Version Handling

**Data Model Versions:**
- `sensor-data` v1 vs v2 → Different virtual paths and field schemas
- Versioned contracts: `_historianv1`, `_historianv2`

**Schema Registry Mapping:**
- `_historian` (latest) → Most recent data model version
- `_historianv1` → Specific version with locked schema (immutable)

## Benefits of This Approach

### 1. **Hierarchical Tag Control**
- Prevents unauthorized nested tags like `sensor.unauthorized_field`
- Supports complex device hierarchies

### 2. **Consistent Timeseries Structure**
- All data follows `{timestamp_ms, value}` pattern
- Type safety on `value` field based on `payloadType`

### 3. **Model Evolution Support**
- Version 1: Simple `value` field
- Version 2: Added `timestamp` and `metadata` fields
- Versioned contracts ensure backward compatibility

### 4. **Cross-Model References**
- `_model: sensor-metadata` → Links to other schema definitions
- Allows complex validation across related data structures

## Implementation Notes

### Schema Transformation (Outside This PR)
The transformation from YAML data models to JSON schemas happens in a separate pipeline/service and is **not part of ENG-3099**. Our validation system only consumes the final JSON schemas.

### Tag Name Extraction
Our `extractTagNameFromUNSTopic()` function handles both simple and hierarchical tags:
- `umh.v1.site._historian.value` → `value`
- `umh.v1.site._historian.sensor.temp_reading` → `sensor.temp_reading`

### Performance Considerations
- Virtual path lookups use hash maps for O(1) tag name validation
- Compiled JSON schemas cached per tag for fast payload validation
- Hierarchical tags don't impact performance vs simple tags

## Real-World Usage Scenarios

### Industrial IoT Device
```yaml
# Device data model
structure:
  motor:
    payloadType: string
    type: timeseries
    subfields:
      rpm:
        payloadType: number
        type: timeseries
      temperature:
        payloadType: number  
        type: timeseries
      status:
        payloadType: string
        type: timeseries
```

**Generated Virtual Path:** `["motor", "motor.rpm", "motor.temperature", "motor.status"]`

**Generated JSON Schema:**
```json
{
   "virtual_path": ["motor", "motor.rpm", "motor.temperature", "motor.status"],
   "fields": {
      "motor": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "string"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "motor.rpm": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "motor.temperature": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "motor.status": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "string"}
         },
         "required": ["timestamp_ms", "value"]
      }
   }
}
```

**Valid UNS Topics:** 
- `umh.v1.factory.line1.machine5._motor_controller.motor`
- `umh.v1.factory.line1.machine5._motor_controller.motor.rpm`
- `umh.v1.factory.line1.machine5._motor_controller.motor.temperature`
- `umh.v1.factory.line1.machine5._motor_controller.motor.status`

### Multi-Sensor Array
```yaml
# Sensor array data model  
structure:
  sensors:
    payloadType: string
    type: timeseries
    subfields:
      temp_01:
        payloadType: number
        type: timeseries
      temp_02:
        payloadType: number
        type: timeseries
      humidity:
        payloadType: number
        type: timeseries
```

**Generated Virtual Path:** `["sensors", "sensors.temp_01", "sensors.temp_02", "sensors.humidity"]`

**Generated JSON Schema:**
```json
{
   "virtual_path": ["sensors", "sensors.temp_01", "sensors.temp_02", "sensors.humidity"],
   "fields": {
      "sensors": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "string"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "sensors.temp_01": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "sensors.temp_02": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "sensors.humidity": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      }
   }
}
```

**Valid UNS Topics:**
- `umh.v1.building.floor2.room201._environmental.sensors`
- `umh.v1.building.floor2.room201._environmental.sensors.temp_01`
- `umh.v1.building.floor2.room201._environmental.sensors.humidity`

This rich data model structure provides the foundation for comprehensive validation while maintaining the flexibility needed for complex industrial and IoT use cases. 