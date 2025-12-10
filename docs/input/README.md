# Input

This section covers Benthos input plugins for collecting data from various industrial systems and protocols. Input plugins allow you to ingest data from PLCs, sensors, and other industrial devices into your Benthos data pipelines.

## Available Input Plugins

- **[Sparkplug B Input](sparkplug-b-input.md)** - Ingests data from MQTT brokers using the Sparkplug B specification. Acts as a Host (Secondary or Primary) to consume Sparkplug B messages from Edge Nodes and converts them to UMH-Core format with automatic hierarchy mapping.

- **[OPC UA Input](opc-ua-input.md)** - Connects to OPC UA servers to browse and subscribe to nodes. Supports various data types, authentication methods, and provides comprehensive metadata for each message.

- **[Modbus](modbus.md)** - Communicates with Modbus devices supporting coils, discrete inputs, holding registers, and input registers. Features register optimization, device-specific workarounds, and extensive configuration options.

- **[ifm IO-Link Master / SensorConnect](ifm-io-link-master-sensorconnect.md)** - Integrates with ifm electronic's IO-Link Master devices (AL1350, AL1352) and EIO404 Bluetooth mesh stations. Processes sensor data using IODD files for human-readable output.

- **[Beckhoff ADS (Community)](beckhoff-ads-community.md)** - Community-supported plugin for Beckhoff's ADS protocol. Supports batch reading, notifications, and symbol-based communication with Beckhoff PLCs.

- **[Siemens S7](siemens-s7.md)** - Connects to Siemens S7 PLCs via the native S7 protocol, enabling cyclic or on-demand reads of data blocks, inputs/outputs, and markers with configurable polling interval.

- **[Ethernet/IP](ethernet-ip.md)** - Interfaces with Rockwell and other Ethernet/IP compatible controllers, mainly supporting the CIP protocol.

- **[UNS Input](uns-input.md)** - Connects to UMH Core's Unified Namespace to consume and filter messages using regex patterns against UMH topic keys.

- **[More Input Plugins](https://docs.redpanda.com/redpanda-connect/components/inputs/about/)** - Additional built-in input plugins available in Benthos/Redpanda Connect for various data sources.

## Choosing the Right Input Plugin

- Use **Sparkplug B Input** when consuming data from existing Sparkplug B Edge Nodes or integrating with Sparkplug B-enabled devices
- Use **OPC UA Input** for modern industrial systems that support the OPC UA standard
- Use **Modbus** for legacy industrial devices and PLCs that communicate via Modbus protocol
- Use **ifm IO-Link Master** when working with ifm electronic's IO-Link infrastructure and sensors
- Use **Beckhoff ADS** specifically for Beckhoff PLC systems and TwinCAT environments
- Use **Siemens S7** for industrial Siemens devices that communicate via Siemens protocol
- Use **Ethernet/IP** for CompactLogix, ControlLogix, Micro800er series, that communicate via CIP protocol
- Use **UNS Input** when working within UMH Core to consume and filter messages from the Unified Namespace
- Explore **additional input plugins** for other protocols like HTTP, MQTT, databases, file systems, and more
