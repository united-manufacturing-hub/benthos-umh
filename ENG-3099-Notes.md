# ENG-3099 Notes: Real-World Data Model Context

## Overview

This document provides context on the actual data models that will be transformed into the JSON schemas used by our dual validation system in the `uns_output` plugin.

## Data Model Structure Examples

### Example 1: Simple Sensor Data

**Source Data Model:**
```yaml
sensor-data:
  description: "Basic sensor data model"
  versions:
    v1:
      root:
        value:
          _type: timeseries-number
    v2:
      root:
        value:
          _type: timeseries-number
        timestamp:
          _type: timeseries-string
        metadata:
          _refModel: "sensor-metadata:v1"
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

### Example 2: Complex Pump Model

**Source Data Model:**
```yaml
pump:
  description: "pump from vendor ABC"
  versions:
    v1:
      root:
        count:
          _type: timeseries-number
        vibration:
          x-axis:
            _type: timeseries-number
            _description: "vibration measurement on x-axis" 
          y-axis:
            _type: timeseries-number
            _unit: "mm/s"
          z-axis:
            _type: timeseries-number
        motor:
          _refModel: "motor:v1"
        acceleration:
          x:
            _type: timeseries-number
          y: 
            _type: timeseries-number
        serialNumber:
          _type: timeseries-string
```

**Transformed to Our Schema:**
> **Note:** The `motor` field is not included in the transformed schema because it only has `_refModel: "motor:v1"` (a reference to another model) without a `_type: timeseries-*`. Only fields with `_type: timeseries-*` are included in the virtual_path and fields for direct validation.
```json
{
   "virtual_path": ["count", "vibration.x-axis", "vibration.y-axis", "vibration.z-axis", "acceleration.x", "acceleration.y", "serialNumber"],
   "fields": {
      "count": {
         "type": "object", 
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "vibration.x-axis": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"}, 
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "vibration.y-axis": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "vibration.z-axis": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "acceleration.x": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "acceleration.y": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "serialNumber": {
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
- `vibration.x-axis` + `vibration.y-axis` + `vibration.z-axis` → `["vibration.x-axis", "vibration.y-axis", "vibration.z-axis"]`

### 2. Payload Type Mapping

| Data Model `_type` | JSON Schema `type` |
|-------------------|-------------------|
| `timeseries-number` | `"number"` |
| `timeseries-string` | `"string"` |
| `timeseries-boolean` | `"boolean"` |

### 3. Timeseries Standard Structure

All fields with `_type: timeseries-*` get transformed to:
```json
{
   "type": "object",
   "properties": {
      "timestamp_ms": {"type": "number"},
      "value": {"type": "[extracted_type]"}
   },
   "required": ["timestamp_ms", "value"]
}
```

### 4. Field Inclusion Rules

**Included in virtual_path:**
- ✅ Fields with `_type: timeseries-*`
- ✅ Nested fields like `vibration.x-axis` when parent has timeseries children

**Excluded from virtual_path:**
- ❌ Fields with only `_refModel` (references to other models)
- ❌ Fields with only metadata (`_description`, `_unit`, etc.) without `_type`

## Validation Flow Examples

### Example 1: Valid Message

**UNS Topic:** `umh.v1.enterprise.site.area._pump_data.vibration.x-axis`
**Tag Name Extracted:** `vibration.x-axis`
**Contract:** `_pump_data`

**Validation Steps:**
1. ✅ Check if `vibration.x-axis` exists in `virtual_path`
2. ✅ Validate payload against `fields["vibration.x-axis"]` schema

**Payload:**
```json
{
   "timestamp_ms": 1680000000000,
   "value": 23.5
}
```

**Result:** ✅ Passes both tag name and payload validation

### Example 2: Invalid Tag Name

**UNS Topic:** `umh.v1.enterprise.site.area._pump_data.unknown_field`
**Tag Name Extracted:** `unknown_field`

**Validation Steps:**
1. ❌ `unknown_field` not found in `virtual_path: ["count", "vibration.x-axis", "vibration.y-axis", "vibration.z-axis", "acceleration.x", "acceleration.y", "serialNumber"]`

**Result:** ❌ Rejected with "tag 'unknown_field' not allowed for contract '_pump_data'. Allowed tags: [count, vibration.x-axis, vibration.y-axis, vibration.z-axis, acceleration.x, acceleration.y, serialNumber]"

### Example 3: Invalid Payload

**UNS Topic:** `umh.v1.enterprise.site.area._pump_data.serialNumber`
**Tag Name:** `serialNumber` ✅
**Payload:**
```json
{
   "timestamp_ms": "not_a_number",
   "value": "ABC123"
}
```

**Validation Steps:**
1. ✅ Tag name validation passes
2. ❌ Payload validation fails: `timestamp_ms` expected number, got string

**Result:** ❌ Rejected with "payload validation failed for tag 'serialNumber': timestamp_ms: expected number, got string"

## Version Handling

**Data Model Versions:**
- `sensor-data` v1 vs v2 → Different virtual paths and field schemas
- `pump` v1 vs v2 → Evolution from basic to advanced monitoring
- Versioned contracts: `_pump_datav1`, `_pump_datav2`

**Schema Registry Mapping:**
- `_pump_data` (latest) → Most recent pump model version
- `_pump_datav1` → Specific version with locked schema (immutable)

## Benefits of This Approach

### 1. **Hierarchical Tag Control**
- Prevents unauthorized nested tags like `vibration.unauthorized_axis`
- Supports complex device hierarchies like `temperature.indoor` and `temperature.outdoor`

### 2. **Consistent Timeseries Structure**
- All data follows `{timestamp_ms, value}` pattern
- Type safety on `value` field based on `_type` (timeseries-number, timeseries-string, etc.)

### 3. **Model Evolution Support**
- Version v1: Basic monitoring fields
- Version v2: Enhanced with additional diagnostics and metadata
- Versioned contracts ensure backward compatibility

### 4. **Cross-Model References**
- `_refModel: "motor:v1"` → Links to other schema definitions
- Allows complex validation across related data structures

### 5. **Rich Metadata Support**
- `_description`, `_unit`, and other metadata enhance field understanding
- Metadata is preserved but doesn't affect validation logic

## Implementation Notes

### Schema Transformation (Outside This PR)
The transformation from YAML data models to JSON schemas happens in a separate pipeline/service and is **not part of ENG-3099**. Our validation system only consumes the final JSON schemas.

### Tag Name Extraction
Our `extractTagNameFromUNSTopic()` function handles both simple and hierarchical tags:
- `umh.v1.site._pump_data.count` → `count`
- `umh.v1.site._pump_data.vibration.x-axis` → `vibration.x-axis`
- `umh.v1.site._environmental.temperature.outdoor` → `temperature.outdoor`

### Performance Considerations
- Virtual path lookups use hash maps for O(1) tag name validation
- Compiled JSON schemas cached per tag for fast payload validation
- Hierarchical tags don't impact performance vs simple tags

## Real-World Usage Scenarios

### Industrial Motor Device
```yaml
motor:
  description: "Industrial motor with diagnostics"
  versions:
    v1:
      root:
        rpm:
          _type: timeseries-number
          _unit: "rpm"
        temperature:
          _type: timeseries-number
          _unit: "°C"
        status:
          _type: timeseries-string
          _description: "Motor status: running, stopped, error"
        vibration:
          level:
            _type: timeseries-number
            _unit: "mm/s"
```

**Generated Virtual Path:** `["rpm", "temperature", "status", "vibration.level"]`

**Generated JSON Schema:**
```json
{
   "virtual_path": ["rpm", "temperature", "status", "vibration.level"],
   "fields": {
      "rpm": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "temperature": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "status": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "string"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "vibration.level": {
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
- `umh.v1.factory.line1.machine5._motor_controller.rpm`
- `umh.v1.factory.line1.machine5._motor_controller.temperature`
- `umh.v1.factory.line1.machine5._motor_controller.status`
- `umh.v1.factory.line1.machine5._motor_controller.vibration.level`

### Environmental Sensors
```yaml
environmental:
  description: "Environmental monitoring sensors"
  versions:
    v1:
      root:
        temperature:
          outdoor:
            _type: timeseries-number
            _unit: "°C"
          indoor:
            _type: timeseries-number
            _unit: "°C"
        humidity:
          _type: timeseries-number
          _unit: "%"
        pressure:
          _type: timeseries-number
          _unit: "hPa"
```

**Generated Virtual Path:** `["temperature.outdoor", "temperature.indoor", "humidity", "pressure"]`

**Generated JSON Schema:**
```json
{
   "virtual_path": ["temperature.outdoor", "temperature.indoor", "humidity", "pressure"],
   "fields": {
      "temperature.outdoor": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "temperature.indoor": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "humidity": {
         "type": "object",
         "properties": {
            "timestamp_ms": {"type": "number"},
            "value": {"type": "number"}
         },
         "required": ["timestamp_ms", "value"]
      },
      "pressure": {
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
- `umh.v1.building.floor2.room201._environmental.temperature.outdoor`
- `umh.v1.building.floor2.room201._environmental.temperature.indoor`
- `umh.v1.building.floor2.room201._environmental.humidity`
- `umh.v1.building.floor2.room201._environmental.pressure`

This rich data model structure provides the foundation for comprehensive validation while maintaining the flexibility needed for complex industrial and IoT use cases. 