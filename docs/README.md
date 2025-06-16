# Introduction

benthos-umh is a specialized extension of Benthos (now known as Redpanda Connect) developed by the [United Manufacturing Hub (UMH)](https://www.umh.app). Tailored for the manufacturing industry, benthos-umh integrates additionally manufacturing protocols such as OPC UA, Siemens S7, and Modbus.

Learn more by visiting our [Protocol Converter product page](https://www.umh.app/product/protocol-converter). For comprehensive technical documentation and configuration details, please continue reading below.

# Table of contents

* [Introduction](README.md)
* [Input](input/README.md)
  * [OPC UA (Input)](input/opc-ua-input.md)
  * [Modbus](input/modbus.md)
  * [ifm IO-Link Master / "sensorconnect"](input/ifm-io-link-master-sensorconnect.md)
  * [Beckhoff ADS (community)](input/beckhoff-ads-community.md)
  * [Siemens S7](input/siemens-s7.md)
  * [Ethernet/IP](input/ethernet-ip.md)
  * [More](https://docs.redpanda.com/redpanda-connect/components/inputs/about/)
* [Processing](processing/README.md)
  * [Tag Processor](processing/tag-processor.md)
  * [Node-RED JavaScript Processor](processing/node-red-javascript-processor.md)
  * [More](https://docs.redpanda.com/redpanda-connect/components/processors/about/)
* [Output](output/README.md)
  * [OPC UA (Output)](output/opc-ua-output.md)
  * [UNS (Output)](output/uns-output.md)
  * [More](https://docs.redpanda.com/redpanda-connect/components/outputs/about/)
