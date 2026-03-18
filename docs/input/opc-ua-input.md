# OPC UA (Input)

The plugin is designed to browse and subscribe to all child nodes within a folder for each configured NodeID, provided that the NodeID represents a folder. It features a recursion depth of up to 10 levels, enabling thorough exploration of nested folder structures. The browsing specifically targets nodes organized under the OPC UA 'Organizes' relationship type, intentionally excluding nodes under 'HasProperty' and 'HasComponent' relationships. Additionally, the plugin does not browse Objects represented by red, blue, or green cube icons in UAExpert.

Subscriptions are selectively managed, with tags having a DataType of null being excluded from subscription. Also, by default, the plugin does not subscribe to the properties of a tag, such as minimum and maximum values.

**Datatypes**

The plugin has been rigorously tested with an array of datatypes, both as single values and as arrays. The following datatypes have been verified for compatibility:

* `Boolean`
* `Byte`
* `DateTime`
* `Double`
* `Enumeration`
* `ExpandedNodeId`
* `Float`
* `Guid`
* `Int16`
* `Int32`
* `Int64`
* `Integer`
* `LocalizedText`
* `NodeId`
* `Number`
* `QualifiedName`
* `SByte`
* `StatusCode`
* `String`
* `UInt16`
* `UInt32`
* `UInt64`
* `UInteger`
* `ByteArray`
* `ByteString`
* `Duration`
* `LocaleId`
* `UtcTime`
* `Variant`
* `XmlElement`

There are specific datatypes which are currently not supported by the plugin:

* Two-dimensional arrays
* Variant arrays (Arrays with multiple different datatypes)

**UA Extension Objects** (custom vendor structs/UDTs) are automatically detected and skipped with a warning log. Since the browse phase discovers individual struct member nodes separately, the data within Extension Objects is still accessible by subscribing to those member nodes directly. If the Extension Object type is registered and decodable by the OPC UA library, it will be serialized as JSON.

**Authentication and Security**

In benthos-umh, we design security and authentication to be as robust as possible while maintaining flexibility. The software automates the process of selecting the highest level of security offered by an OPC-UA server for the selected Authentication Method, but the user can specify their own Security Policy / Security Mode if they want (see below at Configuration options)

**Supported Authentication Methods**

* **Anonymous**: No extra information is needed. The connection uses the highest security level available for anonymous connections.
* **Username and Password**: Specify the username and password in the configuration. The client opts for the highest security level that supports these credentials.
* **Certificate (Future Release)**: Certificate-based authentication is planned for future releases.

**Metadata outputs**

The plugin provides metadata for each message, that can be used to create a topic for the output, as shown in the example above. The metadata can also be used to create a unique identifier for each message, which is useful for deduplication.

| Metadata                 | Description                                                                                                                                          |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `opcua_tag_name`         | The sanitized ID of the Node that sent the message. This is always unique between nodes                                                              |
| `opcua_tag_path`         | A dot-separated path to the tag, created by joining the BrowseNames.                                                                                 |
| `opcua_tag_group`        | Other name for `opcua_tag_path`                                                                                                                      |
| `opcua_tag_type`         | The data type of the node optimized for benthos, which can be either a number, string or bool. For the original one, check out `opcua_attr_datatype` |
| `opcua_source_timestamp` | The SourceTimestamp of the OPC UA node                                                                                                               |
| `opcua_server_timestamp` | The ServerTimestamp of the OPC UA node                                                                                                               |
| `opcua_attr_nodeid`      | The NodeID attribute of the Node as a string                                                                                                         |
| `opcua_attr_nodeclass`   | The NodeClass attribute of the Node as a string                                                                                                      |
| `opcua_attr_browsename`  | The BrowseName attribute of the Node as a string                                                                                                     |
| `opcua_attr_description` | The Description attribute of the Node as a string                                                                                                    |
| `opcua_attr_accesslevel` | The AccessLevel attribute of the Node as a string                                                                                                    |
| `opcua_attr_datatype`    | The DataType attribute of the Node as a string                                                                                                       |
| `opcua_attr_statuscode`  | The OPC UA quality/status code for the data value (e.g., "Good", "BadNodeIdUnknown"). Indicates the reliability of the value.                       |

Taking as example the following OPC-UA structure:

```
Root
└── ns=2;s=FolderNode
    ├── ns=2;s=Tag1
    ├── ns=2;s=Tag2
    └── ns=2;s=SubFolder
        ├── ns=2;s=Tag3
        └── ns=2;s=Tag4
```

Subscribing to `ns=2;s=FolderNode` would result in the following metadata:

| `opcua_tag_name` | `opcua_tag_group`      |
| ---------------- | ---------------------- |
| `Tag1`           | `FolderNode`           |
| `Tag2`           | `FolderNode`           |
| `Tag3`           | `FolderNode.SubFolder` |
| `Tag4`           | `FolderNode.SubFolder` |

**Configuration Options**

The following options can be specified in the `benthos.yaml` configuration file:

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
    username: 'your-username'  # optional (default: unset)
    password: 'your-password'  # optional (default: unset)
    insecure: false | true # DEPRECATED, see below
    securityMode: None | SignAndEncrypt # optional (default: unset)
    securityPolicy: None | Basic128Rsa15 | Basic256 | Basic256Sha256  # optional (default: unset)
    serverCertificateFingerprint: 'sha3-fingerprint-of-cert' # optional (default: unset)
    clientCertificate: 'your-fixed-base64-encoded-certificate' # optional (default: unset)
    userCertificate: 'base64-encoded-user-PEM-certificate' # optional (default: unset)
    userPrivateKey: 'base64-encoded-user-PEM-private-key' # optional (default: unset)
    subscribeEnabled: false | true # optional (default: false)
    useHeartbeat: false | true # optional (default: false)
    pollRate: 1000 # optional (default: 1000) The rate in milliseconds at which to poll the OPC UA server when not using subscriptions
    autoReconnect: false | true # optional (default: false)
    reconnectIntervalInSeconds: 5 # optional (default: 5) The rate in seconds at which to reconnect to the OPC UA server when the connection is lost
    # Advanced options - only modify if you understand your OPC UA server's behavior
    queueSize: 10 # optional (default: 10) Number of subscription notifications to buffer
    samplingInterval: 0.0 # optional (default: 0.0) Sampling interval in milliseconds for subscriptions
```

**Endpoint**

You can specify the endpoint in the configuration file. Node endpoints are automatically discovered and selected based on the authentication method.

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
```

**Node IDs**

You can specify the node IDs in the configuration file (currently only namespaced node IDs are supported):

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
```

**Username and Password**

If you want to use username and password authentication, you can specify them in the configuration file:

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
    username: 'your-username'
    password: 'your-password'
```

**User Certificate and Private Key**

* **Keys**: `userCertificate`, `userPrivateKey`
* **Description**: Credentials for User certificate-based authentication.
* `userCertificate`: Base64-encoded certificate in either PEM (.pem) or DER (.der) format.
* `userPrivateKey`: Base64-encoded private key in PEM (.pem) format only.
* Certificate-based authentication provides stronger security than username/password for high-security environments.
* Proper protection of the private key and certificate validation on both client and server are essential.
* **Configuration Example**:

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
    securityMode: SignAndEncrypt
    securityPolicy: Basic256Sha256
    userCertificate: 'base64-encoded certificate (.pem or .der)'
    userPrivateKey: 'base64-encoded private key (.pem only)'
```

**Security Options**

> To ensure a fully secure connection, you must explicitly configure all of the following security options. However, if these settings seem overwhelming, you can leave them unspecified. In that case, **benthos-umh** will automatically scan for and connect to available endpoints until it succeeds—and then it will log the recommended security settings for your future configuration.

OPC UA supports various security modes and security policies. These options define how messages are signed or encrypted and which cryptographic algorithms are used. In the configuration, you can specify the following:

* **Security Mode**: Defines the level of security applied to messages.
  * **Key**: `securityMode`
  * **Values**:
    * **None**: No security is applied; messages are neither signed nor encrypted.
    * **Sign**: Messages are signed for integrity and authenticity but not encrypted.
    * **SignAndEncrypt**: The highest level of security where messages are both signed and encrypted.
* **Security Policy**: Specifies the cryptographic algorithms used for signing/encrypting messages.
  * **Key**: `securityPolicy`
  * **Values**:
    * **None**: No security applied.
    * **Basic128Rsa15** (**deprecated**): Insecure due to SHA-1. Often disabled on servers by default.
    * **Basic256** (**deprecated**): Insecure due to SHA-1. Often disabled on servers by default.
    * **Basic256Sha256**: Recommended. Uses SHA-256 and provides stronger security.
* **Server Certificate Fingerprint**:
  * **Key**: `serverCertificateFingerprint`
  * **Description**: A SHA3-512 hash of the server’s certificate, used to verify you are connecting to the correct server.
  * If you specify this field, the client will verify that the server’s certificate matches the given fingerprint. If there’s a mismatch, the connection is rejected.
  * If omitted while **still using encryption** (`Sign` or `SignAndEncrypt`), the client will attempt to connect and then **log** the server’s actual fingerprint. You can copy that fingerprint into your config to be certain you’re connecting to the intended server.
  * In future releases, omitting the fingerprint may become a warning or block deployment in certain environments.
* **Client Certificate**:
  * **Key**: `clientCertificate`
  * **Description**: A Base64‐encoded PEM bundle (certificate + private key).
  * When using encryption (`Sign` or `SignAndEncrypt`), the client must present a certificate to the server. If you **do not** provide one, the system **auto‐generates** a random certificate at startup.
  * The auto‐generated certificate is logged in Base64 so you can copy/paste it into your configuration. This allows the server to trust the same client certificate across restarts instead of generating a new one each time.
  * Whenever a certificate is created, and the OPC UA server's settings do not allow automatic acceptance of client certificates, you will need to manually trust the client certificate in the server's settings. The client's name will be displayed, enabling you to uniquely identify it in the certificate list.

If you want to connect with security options, you will at least have to provide the following sample:

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
    securityMode: SignAndEncrypt
    securityPolicy: Basic256Sha256
    serverCertificateFingerprint: 'sha3-fingerprint-of-cert'
    clientCertificate: 'your-fixed-base64-encoded-certificate' # optional but recommended
```

**Insecure Mode**

This is now deprecated. By default, benthos-umh will now connect via SignAndEncrypt and Basic256Sha256 and if this fails it will fall back to insecure mode.

**Pull and Subscribe Methods**

Benthos-umh supports two modes of operation: pull and subscribe. In pull mode, it pulls all nodes every second, regardless of changes. In subscribe mode, it only sends data when there's a change in value, reducing unnecessary data transfer.

| Method    | Advantages                                                                                                                                                                                                                                         | Disadvantages                                                                                                                         |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Pull      | <p>- Provides real-time data visibility, e.g., in MQTT Explorer.<br>- Clearly differentiates between 'no data received' and 'value did not change' scenarios, which can be crucial for documentation and proving the OPC-UA client's activity.</p> | - Results in higher data throughput as it pulls all nodes at the configured poll rate (default: every second), regardless of changes. |
| Subscribe | - Data is sent only when there's a change in value, reducing unnecessary data transfer.                                                                                                                                                            | - Less visibility into real-time data status, and it's harder to differentiate between no data and unchanged values.                  |

```yaml
input:
  opcua:
    endpoint: 'opc.tcp://localhost:46010'
    nodeIDs: ['ns=2;s=IoTSensors']
    subscribeEnabled: true
```

**UseHeartbeat**

If you are unsure if the OPC UA server is actually sending new data, you can enable `useHeartbeat` by setting it to true. It will automatically subscribe to the OPC UA server time, and will re-connect automatically if it does not receive an update within 10 seconds.

```yaml
input:
  opcua:
    useHeartbeat: true
```

**Browse Hierarchical References (Option until version 0.5.2)**

**NOTE**: This property is removed in version 0.6.0 and made as a standard way to browse OPCUA nodes. From version 0.6.0 onwards, opcua\_plugin will browse all nodes with Hierarchical References.

The plugin offers an option to browse OPCUA nodes by following Hierarchical References. By default, this feature is disabled (`false`), which means the plugin will only browse a limited subset of reference types, including:

* `HasComponent`
* `Organizes`
* `FolderType`
* `HasNotifier`

When set to `true`, the plugin will explore a broader range of node references. For a deeper understanding of the different reference types, refer to the [Standard References Type documentation](https://qiyuqi.gitbooks.io/opc-ua/content/Part3/Chapter7.html).

**Recommendation**: Enable this option (`browseHierarchicalReferences: true`) for more comprehensive node discovery.

```yaml
input:
  opcua:
    browseHierarchicalReferences: true
```

**Auto Reconnect**

If the connection is lost, the plugin will automatically reconnect to the OPC UA server. This is useful if the OPC UA server is unstable or if the network is unstable.

```yaml
input:
  opcua:
    autoReconnect: true
```

**Reconnect Interval**

The interval in seconds at which to reconnect to the OPC UA server when the connection is lost. This is only used if `autoReconnect` is set to true.

```yaml
input:
  opcua:
    reconnectIntervalInSeconds: 5
```

**Advanced Configuration Options**

> **WARNING**: The following options are for advanced users only. Modifying these settings without understanding your OPC UA server's behavior and limitations can lead to performance issues, memory problems, or connection failures. Setting those configuration options, does **not** mean that the server will respect that settings. **Leave these at their default values unless you have specific performance requirements and understand the implications.**

**Queue Size**

The `queueSize` parameter controls how many subscription notifications are buffered internally before being processed.

* **Key**: `queueSize`
* **Default**: `10`
* **Description**: This parameter determines the internal buffer size for handling subscription notifications from the OPC UA server. A larger queue can handle burst notifications but uses more memory.

**Risks of incorrect configuration**:
- **Too small**: May cause notification loss during high-frequency periods
- **Too large**: Excessive memory usage that could impact system performance

```yaml
input:
  opcua:
    queueSize: 20  # Only increase if you experience notification loss
```

**Sampling Interval**

The `samplingInterval` parameter controls how frequently the OPC UA server samples the underlying data source for subscription notifications.

* **Key**: `samplingInterval`
* **Default**: `0.0` (fastest possible sampling)
* **Unit**: Milliseconds
* **Description**: Defines the rate at which the server samples the data source. A value of `0.0` means "as fast as possible" according to the server's capabilities. Higher values reduce sampling frequency.

**Server behavior dependency**:
- Some servers ignore this parameter and use their own internal sampling rates
- The actual sampling rate depends on the server's implementation and capabilities

```yaml
input:
  opcua:
    samplingInterval: 1000.0  # Sample every 1 second instead of as fast as possible
```

## Server Profiles and Performance Tuning

Server profiles automatically optimize OPC UA connection parameters based on detected server type. The system queries the server's manufacturer and product information to select the best profile.

### What Are Server Profiles?

Profiles auto-optimize OPC UA connection parameters based on detected server type. The system queries the server's manufacturer and product information (ServerInfo nodes) to select appropriate tuning.

**Two performance parameters:**

1. **Workers (Browse Phase)**: Concurrent goroutines discovering the OPC UA node tree
   - Example: 10,000 nodes with 5 workers ≈ 2,000 Browse calls per worker vs 10,000 sequential calls

2. **Batch Size (Subscribe Phase)**: Nodes per CreateMonitoredItems call
   - Larger batches = faster subscription setup
   - Too large = server rejection or performance degradation

### Available Profiles

Profiles are automatically detected based on server manufacturer/product information:
- **Auto**: Unknown servers (conservative defaults)
- **High-Performance**: Manual override for known high-capacity infrastructure
- **Ignition**: Inductive Automation Ignition Gateway
- **Kepware**: PTC Kepware KEPServerEX
- **S7-1200**: Siemens S7-1200 PLCs (hardware limit: 1000 monitored items)
- **S7-1500**: Siemens S7-1500 PLCs (hardware limit: 10000 monitored items)
- **Prosys**: Prosys Simulation Server

**Profile values** are defined in [`opcua_plugin/server_profiles.go`](../../opcua_plugin/server_profiles.go) with vendor documentation citations.

**Key insight**: Profile values are production-safe limits from vendor docs and real-world testing, NOT server-reported theoretical maximums. Example: S7-1200 reports `MaxMonitoredItemsPerCall=1000` but profile uses 100 (values >200 cause 50× performance degradation per [Siemens docs](https://cache.industry.siemens.com/dl/files/846/109755846/att_1163306/v4/109755846_TIA_Portal_OPC_UA_system_limits.pdf)).

### Performance Impact

Examples of profile-specific optimizations:

- **S7-1200**: Batch size limited to 100 (server reports 1000, but >200 causes 50× degradation - [Siemens docs](https://cache.industry.siemens.com/dl/files/846/109755846/att_1163306/v4/109755846_TIA_Portal_OPC_UA_system_limits.pdf))
- **Ignition/Kepware**: Batch size of 1000 enables 10× faster subscription setup compared to conservative 100. But limited browse worker amount as Eclipse Milo, the OPC UA library that Ignition uses, has a limit on concurrent operations per OPC UA session fo by default 64.
- **Prosys**: Batch size of 800 prevents simulation server unresponsiveness with large node counts

### Dynamic Worker Scaling

During the Browse phase, the system automatically adjusts worker concurrency based on measured server response time. This optimization balances performance (faster browsing with more workers) against server load (preventing overload).

**How it works:**

1. **Measurement**: The system samples response times from 5 consecutive Browse operations
2. **Target latency**: 250ms per Browse request (default)
3. **Scaling logic**:
   - If average response > 250ms → reduce workers by 1 (down to profile's MinWorkers)
   - If average response < 250ms → increase workers by 1 (up to profile's MaxWorkers)
   - If average response ≈ 250ms → no adjustment

**Bounds enforcement**: Worker count always respects the ServerProfile's MinWorkers and MaxWorkers limits. These limits are **global across ALL NodeIDs** being browsed - if you configure 3 NodeIDs with MaxWorkers=20, all 3 NodeIDs share a single pool of 20 workers (not 20 workers per NodeID = 60 total). The system cannot scale beyond profile-defined hardware constraints.

**Why this matters:**

- **Performance**: Automatically finds optimal worker count for each server's capability
- **Safety**: Prevents server overload by reducing workers when response times increase
- **Gradual adaptation**: Fine-grained control with ±1 worker adjustments provides smoother scaling
- **Adaptability**: Adjusts to changing server conditions during long Browse operations
- **Global resource management**: MaxWorkers controls total system concurrency regardless of NodeID count, preventing server overload when browsing multiple large folders simultaneously

Example: An S7-1500 profile with MaxWorkers=50 and **2 NodeIDs** might start with 10 workers **shared across both browse operations**. If Browse responses average 100ms (< 250ms target), the shared worker pool gradually increases to 50 maximum **for all browse operations combined**: 10 → 11 → 12 → 13. If responses slow to 400ms, workers reduce **across all active browse operations**: 13 → 12 → 11 to maintain stability.

### Manual Profile Override

Override auto-detection by setting profile explicitly:

```yaml
input:
  opcua:
    endpoint: "opc.tcp://10.0.0.1:4840"
    profile: "high-performance"  # Override auto-detection
```

Use case: Force high-performance profile when server manufacturer string doesn't match known vendors but infrastructure is validated for aggressive batching.

## Metrics

### opcua_subscription_failures_total

Counter tracking OPC UA subscription failures by reason and node ID.

**Labels:**
- `reason`: Failure classification (`filter_not_allowed`, `filter_unsupported`, `node_id_unknown`, `node_id_invalid`, `other`)
- `node_id`: OPC UA NodeID that failed to subscribe

**Common Failure Reasons:**

| Reason | Description | Resolution |
|--------|-------------|------------|
| `filter_not_allowed` | Server rejected deadband filter | Only numeric types (Int, UInt, Float, Double) support deadband |
| `filter_unsupported` | Server doesn't support filters | Disable deadband: `deadbandType: none` |
| `node_id_unknown` | NodeID doesn't exist | Verify NodeID configuration |
| `node_id_invalid` | NodeID syntax error | Fix NodeID string format |
| `other` | Other subscription errors | Check server logs |
