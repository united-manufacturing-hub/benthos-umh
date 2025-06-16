# Tag Processor

The Tag Processor is designed to prepare incoming data for the UMH data model. It processes messages through three configurable stages: defaults, conditional transformations, and advanced processing, all using a Node-RED style JavaScript environment.

Use the `tag_processor` compared to the `nodered_js` when you are processing tags or time series data and converting them to the UMH data model within the `_historian` data contract. This processor is optimized for handling structured time series data, automatically formats messages, and generates appropriate metadata.

**Message Formatting Behavior**

The processor automatically formats different input types into a consistent structure with a "value" field:

1. **Simple Values (numbers, strings, booleans)**\
   Input:

```json
42
```

Output:

```json
{
  "value": 42
}
```

Input:

```json
"test string"
```

Output:

```json
{
  "value": "test string"
}
```

Input:

```json
true
```

Output:

```json
{
  "value": true
}
```

2. **Arrays** (converted to string representation)\
   Input:

```json
["a", "b", "c"]
```

Output:

```json
{
  "value": "[a b c]"
}
```

3. **Objects** (preserved as JSON objects)\
   Input:

```json
{
  "key1": "value1",
  "key2": 42
}
```

Output:

```json
{
  "value": "{\"key1\": \"value1\",\"key2\": 42}"
}
```

4. **Numbers** (preserved as numbers)\
   Input:

```json
23.5
```

Output:

```json
{
  "value": 23.5
}
```

Input:

```json
42
```

Output:

```json
{
  "value": 42
}
```

This consistent formatting ensures that:

- All messages have a "value" field
- Simple types (numbers, strings, booleans) are preserved as-is
- Complex types (arrays, objects) are converted to their string representations
- Numbers are always preserved as numeric types (integers or floats)

**Configuration**

```yaml
pipeline:
  processors:
    - tag_processor:
        defaults: |

          // Set default location hierarchy and datacontract
          msg.meta.location_path = "enterprise.plant1.machiningArea.cnc-line.cnc5.plc123";
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name = "value";
          msg.payload = msg.payload; //does not modify the payload
          return msg;
        conditions:
          - if: msg.meta.opcua_node_id === "ns=1;i=2245"
            then: |
              // Set path hierarchy and tag name for specific OPC UA node
              msg.meta.virtual_path = "axis.x.position";
              msg.meta.tag_name = "actual";
              return msg;
        advancedProcessing: |
          // Optional advanced message processing
          // Example: double numeric values
          msg.payload = parseFloat(msg.payload) * 2;
          return msg;
```

**Processing Stages**

1. **Defaults**
   - Sets initial metadata values
   - Runs first on every message
   - Must return a message object
2. **Conditions**
   - List of conditional transformations
   - Each condition has an `if` expression and a `then` code block
   - Runs after defaults
   - Must return a message object
3. **Advanced Processing**
   - Optional final processing stage
   - Can modify both metadata and payload
   - Must return a message object

**Metadata Fields**

The processor uses the following metadata fields:

**Required Fields:**

- `location_path`: Hierarchical location path in dot notation (e.g., "enterprise.site.area.line.workcell.plc123")
- `data_contract`: Data schema identifier (e.g., "\_historian", "\_analytics")
- `tag_name`: Name of the tag/variable (e.g., "temperature", "pressure")

**Optional Fields:**

- `virtual_path`: Logical, non-physical grouping path in dot notation (e.g., "axis.x.position")

**Generated Fields:**

- `umh_topic`: Automatically generated from the above fields in the format:

  ```
  umh.v1.<location_path>.<data_contract>.<virtual_path>.<tag_name>
  ```

  Empty or undefined fields are skipped, and dots are normalized.

**Message Structure**

Messages in the Tag Processor follow the Node-RED style format:

```javascript
{
  payload: {
    // The message content - can be a simple value or complex object
    "temperature": 23.5,
    "timestamp_ms": 1733903611000
  },
  meta: {
    // Required fields
    location_path: "enterprise.site.area.line.workcell.plc123",  // Hierarchical location path
    data_contract: "_historian",                                 // Data schema identifier
    tag_name: "temperature",                                     // Name of the tag/variable

    // Optional fields
    virtual_path: "axis.x.position",                            // Logical grouping path

    // Generated field (by processor)
    umh_topic: "umh.v1.enterprise.site.area.line.workcell.plc123._historian.axis.x.position.temperature",

    // Input-specific fields (e.g., from OPC UA)
    opcua_node_id: "ns=1;i=2245",
    opcua_tag_name: "temperature_sensor_1",
    opcua_tag_group: "sensors.temperature",
    opcua_tag_path: "sensors.temperature",
    opcua_tag_type: "number",
    opcua_source_timestamp: "2024-03-12T10:00:00Z",
    opcua_server_timestamp: "2024-03-12T10:00:00.001Z",
    opcua_attr_nodeid: "ns=1;i=2245",
    opcua_attr_nodeclass: "Variable",
    opcua_attr_browsename: "Temperature",
    opcua_attr_description: "Temperature Sensor 1",
    opcua_attr_accesslevel: "CurrentRead",
    opcua_attr_datatype: "Double"
  }
}
```

**Examples**

1. **Basic Defaults Processing**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.plant1.machiningArea.cnc-line.cnc5.plc123";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "actual";
    return msg;
```

Input:

```json
23.5
```

Output:

```json
{
  "actual": 23.5,
  "timestamp_ms": 1733903611000
}
```

UMH Topic: `umh.v1.enterprise.plant1.machiningArea.cnc-line.cnc5.plc123._historian.actual`

2. **OPC UA Node ID Based Processing**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.plant1.machiningArea.cnc-line.cnc5.plc123";
    msg.meta.data_contract = "_historian";
    return msg;
  conditions:
    - if: msg.meta.opcua_attr_nodeid === "ns=1;i=2245"
      then: |
        msg.meta.virtual_path = "axis.x.position";
        msg.meta.tag_name = "actual";
        return msg;
```

Input with metadata `opcua_attr_nodeid: "ns=1;i=2245"`:

```json
23.5
```

Output:

```json
{
  "actual": 23.5,
  "timestamp_ms": 1733903611000
}
```

UMH Topic: `umh.v1.enterprise.plant1.machiningArea.cnc-line.cnc5.plc123._historian.axis.x.position.actual`

3. **Moving Folder Structures in Virtual Path**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.plant1";
    msg.meta.data_contract = "_historian";
    msg.meta.virtual_path = msg.meta.opcua_tag_path;
    msg.meta.tag_name = msg.meta.opcua_tag_name;
    return msg;
  conditions:
    # Move the entire DataAccess_AnalogType folder and its children into axis.x
    - if: msg.meta.opcua_tag_path && msg.meta.opcua_tag_path.includes("DataAccess_AnalogType")
      then: |
        msg.meta.location_path += ".area1.machining_line.cnc5.plc123";
        msg.meta.virtual_path = "axis.x." + msg.meta.opcua_tag_path;
        return msg;
```

Input messages with OPC UA tags:

```javascript
// Original tag paths from OPC UA:
// DataAccess_AnalogType
// DataAccess_AnalogType.EURange
// DataAccess_AnalogType.Min
// DataAccess_AnalogType.Max
```

Output UMH topics will be:

```
umh.v1.enterprise.plant1.area1.machining_line.cnc5.plc123._historian.axis.x.DataAccess_AnalogType
umh.v1.enterprise.plant1.area1.machining_line.cnc5.plc123._historian.axis.x.DataAccess_AnalogType.EURange
umh.v1.enterprise.plant1.area1.machining_line.cnc5.plc123._historian.axis.x.DataAccess_AnalogType.Min
umh.v1.enterprise.plant1.area1.machining_line.cnc5.plc123._historian.axis.x.DataAccess_AnalogType.Max
```

This example shows how to:

- Match an entire folder structure using `includes("DataAccess_AnalogType")`
- Move all matching nodes into a new virtual path prefix (`axis.x`)
- Preserve the original folder hierarchy under the new location
- Apply consistent location path for the entire folder structure

4. **Advanced Processing with getLastPayload**

getLastPayload is a function that returns the last payload of a message that was avaialble in Kafka. Remember that you will get the full payload, and might still need to extract the value you need.

**This is not yet implemented, but will be available in the future.**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.site.area.line.workcell";
    msg.meta.data_contract = "_analytics";
    msg.meta.virtual_path = "work_order";
    return msg;
  advancedProcessing: |
    msg.payload = {
      "work_order_id": msg.payload.work_order_id,
      "work_order_start_time": umh.getLastPayload("enterprise.site.area.line.workcell._historian.workorder.work_order_start_time").work_order_start_time,
      "work_order_end_time": umh.getLastPayload("enterprise.site.area.line.workcell._historian.workorder.work_order_end_time").work_order_end_time
    };
    return msg;
```

Input:

```json
{
  "work_order_id": "WO123"
}
```

Output:

```json
{
  "work_order_id": "WO123",
  "work_order_start_time": "2024-03-12T10:00:00Z",
  "work_order_end_time": "2024-03-12T18:00:00Z"
}
```

UMH Topic: `umh.v1.enterprise.site.area.line.workcell._analytics.work_order`

4. **Dropping Messages Based on Value**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    return msg;
  advancedProcessing: |
    if (msg.payload < 0) {
      // Drop negative values
      return null;
    }
    return msg;
```

Input:

```json
-10
```

Output: Message is dropped (no output)

Input:

```json
10
```

Output:

```json
{
  "temperature": 10,
  "timestamp_ms": 1733903611000
}
```

UMH Topic: `umh.v1.enterprise._historian.temperature`

5. **Duplicating Messages for Different Data Contracts**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise";
    msg.meta.data_contract = "_historian";
    msg.meta.tag_name = "temperature";
    return msg;
  conditions:
    - if: true
      then: |
        msg.meta.location_path += ".production";
        return msg;
  advancedProcessing: |
    // Create two versions of the message:
    // 1. Original value for historian
    // 2. Doubled value for custom
    let doubledValue = msg.payload * 2;

    msg1 = {
      payload: msg.payload,
      meta: { ...msg.meta, data_contract: "_historian" }
    };

    msg2 = {
      payload: doubledValue,
      meta: { ...msg.meta, data_contract: "_custom", tag_name: msg.meta.tag_name + "_doubled" }
    };

    return [msg1, msg2];
```

Input:

```json
23.5
```

Output 1 (Historian):

```json
{
  "temperature": 23.5,
  "timestamp_ms": 1733903611000
}
```

UMH Topic: `umh.v1.enterprise.production._historian.temperature`

Output 2 (custom):

```json
{
  "temperature_doubled": 47,
  "timestamp_ms": 1733903611000
}
```

UMH Topic: `umh.v1.enterprise.production._custom.temperature_doubled`

6. **Processing Full MQTT Message Payload**

```yaml
tag_processor:
  defaults: |
    msg.meta.location_path = "enterprise.area._workorder";
    msg.meta.data_contract = "_workorder";
    msg.meta.virtual_path = "new";
    msg.meta.tag_name = "maintenance";
    return msg;
```

Input:

```json
{
  "maintenanceSchedule": {
    "eventType": "ScheduledMaintenance",
    "eventId": "SM-20240717-025",
    "timestamp": "2024-07-17T13:00:00Z",
    "equipmentId": "InjectionMoldingMachine5",
    "equipmentName": "Engel Victory 120",
    "scheduledDate": "2024-07-22",
    "maintenanceType": "Preventive",
    "description": "Inspection and cleaning of injection unit and mold.",
    "maintenanceDuration": "6 hours",
    "assignedTo": {
      "employeeId": "EMP-5005",
      "name": "Hans Becker"
    },
    "status": "Scheduled",
    "partsRequired": [
      {
        "partId": "NOZZLE-015",
        "description": "Injection Nozzle",
        "quantity": 1
      }
    ],
    "notes": "Replace worn nozzle to prevent defects."
  }
}
```

Output:

```json
{
  "maintenance": {
    "maintenanceSchedule": {
      "eventType": "ScheduledMaintenance",
      "eventId": "SM-20240717-025",
      "timestamp": "2024-07-17T13:00:00Z",
      "equipmentId": "InjectionMoldingMachine5",
      "equipmentName": "Engel Victory 120",
      "scheduledDate": "2024-07-22",
      "maintenanceType": "Preventive",
      "description": "Inspection and cleaning of injection unit and mold.",
      "maintenanceDuration": "6 hours",
      "assignedTo": {
        "employeeId": "EMP-5005",
        "name": "Hans Becker"
      },
      "status": "Scheduled",
      "partsRequired": [
        {
          "partId": "NOZZLE-015",
          "description": "Injection Nozzle",
          "quantity": 1
        }
      ],
      "notes": "Replace worn nozzle to prevent defects."
    }
  },
  "timestamp_ms": 1733903611000
}
```

UMH Topic: `umh.v1.enterprise.area._workorder.maintenance`

**Note:** In the `tag_processor`, the resulting payload will always include `timestamp_ms` and one additional key corresponding to the `tag_name`. If you need to fully control the resulting payload structure, consider using the `nodered_js` processor instead. You can set the topic and payload manually, as shown below:

```yaml
pipeline:
  processors:
    - nodered_js:
        code: |
          // set kafka topic manually
          msg.meta.umh_topic = "umh.v1.enterprise.site.area._workorder.new"

          // only take two fields from the payload
          msg.payload = {
            "maintenanceSchedule": {
              "eventType": msg.payload.maintenanceSchedule.eventType,
              "description": msg.payload.maintenanceSchedule.description
            }
          }
          return msg;
```
