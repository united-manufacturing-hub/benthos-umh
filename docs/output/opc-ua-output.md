# OPC UA (Output)

The **OPC UA output** plugin writes data into an OPC UA server (e.g., a PLC). This plugin supports optional read-back (handshake) to confirm the write and dynamic configuration through interpolated fields.

> **Data Transformations**\
> It is recommended to perform JSON-to-field transformations _before_ this plugin (e.g., via Node-RED JavaScript or [Bloblang](https://www.benthos.dev/docs/guides/bloblang/about)). That way, you feed the final fields directly to this plugin without extra logic here.

## Dynamic Configuration Support

The OPC UA output plugin supports interpolated strings for the following fields:
- `nodeId` - Dynamically determine which OPC UA node to write to based on message content

This enables use cases such as:
- Routing writes to different nodes based on message content
- Building flexible, reusable configurations

***

**Basic Configuration**

```yaml
output:
  opcua:
    # endpoint, securityMode, securityPolicy, serverCertificateFingerprint, clientCertificate
    # see OPC UA Input for more information

    # Static configuration example
    nodeMappings:
      - nodeId: "ns=2;s=MySetpoint"
        valueFrom: "setpoint"   # The JSON field to write
        dataType: "Int32"       # OPC UA data type (required)
      - nodeId: "ns=2;s=MyEnableFlag"
        valueFrom: "enable_flag"
        dataType: "Boolean"     # OPC UA data type (required)

    handshake:
      enabled: true               # enable read-back
      readbackTimeoutMs: 2000     # how long to wait for the new value to appear

      maxWriteAttempts: 3         # how many times to retry if the write fails
      timeBetweenRetriesMs: 1000  # time (ms) between write retries
```

**Supported Data Types**

The OPC UA output plugin supports the following commonly used data types for writing to OPC UA servers:

* `Boolean`: True/false values
* `Byte`: 8-bit unsigned integer (0 to 255)
* `SByte`: 8-bit signed integer (-128 to 127)
* `Int16`: 16-bit signed integer
* `UInt16`: 16-bit unsigned integer
* `Int32`: 32-bit signed integer
* `UInt32`: 32-bit unsigned integer
* `Int64`: 64-bit signed integer
* `UInt64`: 64-bit unsigned integer
* `Float`: 32-bit floating-point number
* `Double`: 64-bit floating-point number
* `String`: UTF-8 encoded string
* `DateTime`: Date and time values

**Note:** When selecting a data type, ensure it matches the expected type on the OPC UA server. Mismatched types may cause write operations to fail or data to be interpreted incorrectly.

**Fields:**

| Field                              | Description                                                                                                                                                                                                                                                                                                                                                                              |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **nodeMappings** (array)           | <p>List of nodes to write to, each with:<br>- <code>nodeId</code>: The OPC UA Node ID, e.g., <code>"ns=2;s=MyVariable"</code><br>- <code>valueFrom</code>: The JSON field name (string) in your message containing the final value.<br>- <code>dataType</code>: The OPC UA data type for the value (e.g., <code>"Boolean"</code>, <code>"Int32"</code>, <code>"Double"</code>, etc.)</p> |
| **handshake**                      | (Optional) A sub-config for read-back validation.                                                                                                                                                                                                                                                                                                                                        |
| **handshake.enabled**              | Default `true` (for safe writes). If `true`, the plugin attempts to read the node(s) after writing. If the new value doesn’t match, the write is considered failed.                                                                                                                                                                                                                      |
| **handshake.readbackTimeoutMs**    | How long to wait for the server to show the updated value. If it times out, the plugin fails (Benthos will not ACK the message).                                                                                                                                                                                                                                                         |
| **handshake.maxWriteAttempts**     | Number of write attempts if the server fails (default 1).                                                                                                                                                                                                                                                                                                                                |
| **handshake.timeBetweenRetriesMs** | Delay between write attempts (default 1000 ms).                                                                                                                                                                                                                                                                                                                                          |

***

**Usage Example**

**Incoming Message** (e.g., from a Benthos input or pipeline):

```json
{
  "setpoint": 123,
  "enable_flag": true
}
```

**Plugin Configuration**:

```yaml
output:
  opcua:
    endpoint: "opc.tcp://192.168.0.10:4840"
    nodeMappings:
      - nodeId: "ns=2;s=MySetpoint"
        valueFrom: "setpoint"
        dataType: "Int32"
      - nodeId: "ns=2;s=MyEnableFlag"
        valueFrom: "enable_flag"
        dataType: "Boolean"

    handshake:
      enabled: true
      readbackTimeoutMs: 2000
      maxWriteAttempts: 3
      timeBetweenRetriesMs: 1000
```

**Behavior**:

1. The plugin connects to the OPC UA server at `opc.tcp://192.168.0.10:4840`.
2. It **writes** the field `setpoint=123` to node `ns=2;s=MySetpoint` and `enable_flag=true` to `ns=2;s=MyEnableFlag`.
3. Immediately after, it **reads** these nodes back:
   * If the server now reports `MySetpoint=123` and `MyEnableFlag=true`, the write is considered successful.
   * If the read times out after 2 s or shows a different value, the plugin fails the write. Benthos will _not_ ACK the message upstream, so it can be retried or routed to an error output.
4. Assuming success, the message is **acknowledged** and removed from the pipeline.

If, for example, the `ns=2;s=MyEnableFlag` node is read-only or the server rejects the update, the read-back will fail, causing the plugin to retry up to 3 times (`maxWriteAttempts`). If all attempts fail, Benthos escalates the failure.

***

**Handshake & Acknowledgment**

By default, the plugin **reads back** each node it wrote to confirm the new value appears. This ensures:

1. **Benthos Message ACK**: If the read-back fails or times out, the output plugin fails. Benthos will _not_ acknowledge the message upstream, and you can configure fallback or retry strategies.
2. **Consistent Setpoints**: If the OPC UA server discards or modifies the value, you’ll see an immediate error.

> **Disable** the handshake by setting `handshake.enabled: false` if you prefer no read-back check (faster, but less safe).

**Example**: If you disable the handshake:

```yaml
handshake:
  enabled: false
```

The plugin will write to the OPC UA server but **not** confirm. It will “succeed” as soon as the write request is sent.

***

## Dynamic Configuration Example

The OPC UA output plugin supports dynamic node IDs using interpolated fields:

```yaml
output:
  opcua:
    endpoint: "opc.tcp://localhost:4840"

    # Dynamic node mapping based on message content
    nodeMappings:
      - nodeId: "${! json(\"target_node\") }"     # Node ID from message field
        valueFrom: "value"
        dataType: "String"
```

**Example with message routing:**

```yaml
pipeline:
  processors:
    - mapping: |
        # Route to different nodes based on tag type
        root.target_node = match this.tag_type {
          "color" => "ns=2;s=[default]/PaintRoom1/Spraytan1/Color",
          "speed" => "ns=2;s=[default]/PaintRoom1/Spraytan1/Speed",
          "temp"  => "ns=2;s=[default]/PaintRoom1/Spraytan1/Temperature"
        }
        root.value = this.tag_value

output:
  opcua:
    endpoint: "opc.tcp://plc.factory.local:4840"
    nodeMappings:
      - nodeId: "${! json(\"target_node\") }"
        valueFrom: "value"
        dataType: "Double"
```

This enables building reusable configurations that adapt to different environments and message types without hardcoding node paths.

***

**Implementation Details & Future Outlook**

For many industrial use cases, you might need more than just writing a value and reading it back:

1. **De-duplication**: If you re-send the same “command” multiple times, do you want the PLC to ignore duplicates?
   * _Now_: Implement a unique command ID (UUID) in your message and let the PLC store/ignore duplicates. Or handle it in your Benthos pipeline (e.g., a “dedupe” processor).
   * _Future_: We may add a built-in “ActionUUID” handshake, which compares a known ID in another read node.
2. **Time-Window Checks**: Only accept a command if it arrives before a certain expiration.
   * _Now_: Use a preceding nodered\_js processor or Bloblang to drop the message if `timestamp_now - msg.timestamp > threshold`.
   * _Future_: We might add plugin-level config like `rejectOlderThanMs` if demand arises.
3. **Separate Acknowledgment Node**: Some PLCs use a separate ack node (e.g., `CommandAck`) that signals the command was _processed_.
   * _Now_: Implement in the PLC + a custom “double read” with a second plugin instance (or a separate input that waits for the ack).
   * _Future_: We may add an advanced handshake config that reads a different node (rather than the same node) and checks for a specific “ACK” value.

With Benthos, the “at least once” acknowledgment ensures that if writing fails, the message can be retried or routed. This plugin’s minimal default handshake (read-back from the same node) is a strong start for safer OPC UA setpoints, and we’ll grow it over time if more advanced scenarios are needed.

