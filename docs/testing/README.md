# Testing

We execute automated tests and verify that benthos-umh works against various targets. All tests are started with `make test`, but might require environment parameters in order to not be skipped.

Some of these tests are executed with a local GitHub runner called "hercules", which is connected to an isolated testing network.

## Target: WAGO PFC100 (OPC UA)

 - Model number: 750-8101 PFC100 CS 2ETH
 - Firmware: 03.10.08(22)
 - OPC-UA-Server Version: 1.3.1

Requires:

```bash
TEST_WAGO_ENDPOINT_URI="opc.tcp://your_wago_endpoint_uri:port"
TEST_WAGO_USERNAME="your_wago_username"
TEST_WAGO_PASSWORD="your_wago_password"
```

## Target: Microsoft OPC UA Simulator (OPC UA)

- Docker tag: mcr.microsoft.com/iotedge/opc-plc:2.9.11

Requires:
```bash
TEST_OPCUA_SIMULATOR="opc.tcp://localhost:50000"
```

## Target: Prosys OPC UA Simulator (OPC UA)

- Version: 5.4.6-148

Requires:
```bash
TEST_PROSYS_ENDPOINT_URI="opc.tcp://your_prosys_endpoint:port"
```

This requires additional to have the simulator setup somewhere (e.g., locally on your PC) and pointing the test towards it. This is not included in any CI and must be run manually.

## Target: Siemens S7-1200 (OPC UA)

- Model number: SIMATIC S7-1200 (6ES7211-1AE40-0XB0)
- Firmware: v4.4

Requires:
```bash
TEST_S7_ENDPOINT_URI="opc.tcp://your_s7_endpoint_uri:port"
```

## Target: Unit Tests (OPC UA)

Requires:
```bash
TEST_OPCUA_UNITTEST=true
```

## Target: Siemens S7-1200 (S7comm)

- Model number: SIMATIC S7-1200 (6ES7211-1AE40-0XB0)
- Firmware: v4.4

Requires:
```bash
TEST_S7_TCPDEVICE="your_s7_ip:port"
TEST_S7_RACK=0
TEST_S7_SLOT=1
```
The rack and slotnumbers are just an example. Ensure to pick the matching ones for your test-setup.

## Target: Unit Tests (S7comm)

Requires:
```bash
TEST_S7COMM_UNITTEST=true
```
