# Ethernet/IP


The plugin is designed to read data from configured tags within Rockwell Automation PLCs that support the CIP protocol over Ethernet/IP, such as **ControlLogix**, **CompactLogix**, and **Micro820**. It uses the [`gologix`](https://github.com/danomagnum/gologix) driver — a native Go implementation modeled after pylogix — to establish and maintain communication with the controller.

Currently, the plugin behaves similarly to the `s7comm` plugin: you must explicitly configure the **tag names** and **data types** you want to read. Automatic browsing or discovery of tags is **not yet implemented**. As such, you should already know the tag names and structures from the PLC program (e.g., via Studio 5000 or Connected Components Workbench).

> ⚠️ Support is limited to modern controllers that use **CIP over Ethernet/IP**. Legacy PLCs such as **PLC-5**, **SLC-500**, or **MicroLogix** models using **PCCC** are *not supported*.

In future versions, support for **browsing** and **listing available tags** directly from the controller may be added to improve usability and reduce manual configuration.
**Datatypes**

The plugin is being tested with multiple datatypes, therefore the following datatypes are verified for compatibility:

* `Boolean`
* `Byte`
* `Int8`
* `Int16`
* `Int32`
* `Int64`
* `UInt16`
* `UInt32`
* `UInt64`
* `Float32`
* `Float64`
* `String`
* `Array of Byte`
* `Array of Int8`
* `Array of Int16`
* `Array of Int32`
* `Array of Int64`
* `Array of Uint16`
* `Array of Uint32`
* `Array of Uint64`
* `Array of Float32`
* `Array of Float64`
* `Array of String`

**Metadata outputs**

The plugin provides metadata for each message, that can be used to create a topic for the output, as shown in the example above. The metadata can also be used to create a unique identifier for each message, which is useful for deduplication.

| Metadata       | Description                                                                                                          |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| `eip_tag_name` | The Name of the Tag that sent the message. If an alias was specified it will replace the original naming of the tag. |
| `eip_tag_path` | A dot-separated path to the tag, which is usually used for TagSets and Tags.                                         |
| `eip_tag_type` | The data type of the node optimized for benthos, which can be either a number, string or bool.                       |

**Configuration Options**

The following options can be specified in the `benthos.yaml` configuration file:

```yaml
input:
  ethernetip:
    endpoint: '127.0.0.1:44818'
    pollRate: 1000 # optional (default: 1000) The rate in milliseconds at which to poll the EthernetIP plc
    listAllTags: false | true # (currently not supported)
    useMultiRead: false | true # (currently not supported)
    attributes:
      - path: "1-1-1" # specify the path like `Class-Instance-Attribute`
        type: "int16"
        alias: "vendorID" # optional (default: unset)
    tags:
      - name: "tagName"
        type: "bool"
        alias: "tagAlias" # optional (default: unset)
        length: 1 # optional (default: 1) you only need to set this for arrays
```

**Endpoint**

You can specify the endpoint in the configuration file. Node endpoints are automatically discovered and selected based on the authentication method.

```yaml
input:
  ethernetip:
    endpoint: '127.0.0.1:44818'
    tags:
      - name: "tagName"
        type: "bool"
```

**Attributes**

You can specify the Attributes in the configuration file:

```yaml
input:
  ethernetip:
    endpoint: '127.0.0.1:44818'
    attributes:
      - path: "1-1-1" # specify the path like `Class-Instance-Attribute`
        type: "int16"
        alias: "vendorID" # optional (default: unset)
```

* **Key**: `path`
* **Description**: The `path` always consists of this pattern: "Class-Instance-Attribute". Path to the specific attribute you want to read. Attributes are usually used for some device specific information, therefore you have to check your device's manual to check whether a specific attribute exists.
* **Example**: `1-1-1`
* **Key**: `type`
* **Description**: The `type` specifies the attributes type, which has to be set correctly otherwise you will receive an error. You can also get this from your device's manual.
* **Example**: `int16`
* **Key**: `alias`
* **Description**: The `alias` is an option to store your data into a specific name, so you can later easilier access this attribute.
* **Example**: `testAlias`

**Tags**

You can specify the Tags in the configuration file:

```yaml
input:
  ethernetip:
    endpoint: '127.0.0.1:44818'
    tags:
      - name: "testInt16" # specify the path like `Class-Instance-Attribute`
        type: "int16"
        alias: "counter" # optional (default: unset)
```

* **Key**: `name`
* **Description**: The `name` is basically the name of the tag you want to read data from. This is specified in your device's software and you have to know the name of that.
* **Example**: `testInt16`
* **Key**: `type`
* **Description**: The `type` specifies the tags type, which has to be set correctly otherwise you will receive an error. You can also get this from your device's software.
* **Example**: `int16`
* **Key**: `alias`
* **Description**: The `alias` is an option to store your data into a specific name, so you can later easilier access this tags data.
* **Example**: `testAlias`
* **Key**: `length` (only if you read from a `type` e.g. `arrayofint16`)
* **Description**: The `length` is an option to set the length of the data you want to read. This is only needed when reading arrays. Otherwise you will receive an error here.
* **Example**: 3

**Important:** The functionality of reading tag-sets in specific is not yet implemented and will need further investigation.

**List all tags (not yet implemented)**

This is not yet implemented and currently not set to a specific timeline.

**MultiRead (not yet implemented)**

This is not yet implemented and currently not set to a specific timeline, but will improve reading time out of your plc if it supports this service.
