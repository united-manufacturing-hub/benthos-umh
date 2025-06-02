# Output

This section covers Benthos output plugins for writing data to various industrial systems and external destinations. Output plugins allow you to send processed data from your Benthos pipelines to PLCs, databases, message brokers, and other systems.

## Available Output Plugins

- **[OPC UA Output](opc-ua-output.md)** - Writes data to OPC UA servers with optional read-back confirmation (handshake). Supports multiple data types and provides safe setpoint operations for industrial control systems.

- **[More Output Plugins](https://docs.redpanda.com/redpanda-connect/components/outputs/about/)** - Additional built-in output plugins available in Benthos/Redpanda Connect for various destinations.

## Choosing the Right Output Plugin

- Use **OPC UA Output** when writing setpoints, commands, or data back to OPC UA-enabled industrial systems with confirmation requirements
- Explore **additional output plugins** for databases (PostgreSQL, MySQL, InfluxDB), message brokers (MQTT, Kafka), cloud services (AWS, Azure, GCP), file systems, HTTP APIs, and more

