# Quick start

In this quick start we set up a TwinCAT 3 PLC as a data source in a Benthos pipeline. By the end
of this section you will know how to:

1. Find the symbols you want to read
2. Write the input configuration
3. Add a tag processor
4. Send the values somewhere
5. Check that values are arriving

Before you start you need:

- A Beckhoff PLC reachable from this host
- An administrator user on that PLC
- The PLC's IP address

You end with a Benthos config file that you run with `benthos -c config.yaml`. The whole thing
takes about 15 minutes.

Step 2 writes a route entry to the PLC, so point this at a PLC you are allowed to configure.

The same five steps work for TwinCAT 2, with different symbol names and a different runtime port.
There is a complete TwinCAT 2 example at the end of this page.

## 1. Find the symbols you want

Open the PLC project in TwinCAT and note the full name of each variable, for example
`GVL_ProcessData.nMasterCycleCounter`. Global variables live under their GVL name; program
variables under the program name.

## 2. Write the input

> **Warning:** `username` and `password` below make the plugin register a route on the PLC. That
> writes a persistent entry to the device's route table, which stays until someone removes it on the
> device. See [Getting a route onto the PLC](networking.md#getting-a-route-onto-the-plc).

`targetAMS` is discovered from the PLC, so you do not need its AMS NetID. `runtimePort` is not: it
defaults to `851`, the first TwinCAT 3 runtime, so a TwinCAT 2 PLC has to set `801`. `hostIP` is the
address the PLC will see this client as, which is usually the VM's IP on the PLC network:

```yaml
input:
  ads:
    targetAddress: "192.168.1.100"
    hostIP: "192.168.1.50"
    username: "Administrator"              # the PLC's own login, for route registration
    password: "1"                           # this PLC's password, not a default to copy
    unifiedAddress:
      - "GVL_ProcessData.nMasterCycleCounter"
      - "GVL_ProcessData.fGlobalReal"
```

## 3. Add a tag processor

Each message carries the symbol name in `ads_symbol_name`, which is what the tag name should
usually be:

```yaml
pipeline:
  processors:
    - tag_processor:
        defaults: |-
          msg.meta.location_path = "enterprise.site.area.line";
          msg.meta.data_contract = "_historian";
          msg.meta.tag_name      = msg.meta.ads_symbol_name;
          return msg;
```

## 4. Send the values somewhere

`stdout` prints each value as it arrives, which is enough to prove the PLC side works:

```yaml
output:
  stdout: {}
```

To write to the Unified Namespace instead, swap in the `uns` output, which needs the embedded
Redpanda that ships with UMH Core:

```yaml
output:
  uns: {}
```

## 5. Check the values arrive

On the first connect the plugin registers a route on the PLC, subscribes to each symbol, and waits
for one value from each before reporting itself connected. In the logs you should see, in order:

```text
registering route               (only the first time, or after the PLC forgets the route)
route registration successful
Connected to PLC
Registering notifications succeeded for 2/2 symbols
Input type ads is now active
```

Two different registrations appear there. The route registration writes an entry to the PLC's route
table and happens once. The notification registration subscribes to each symbol and happens on
every connect.

Values then print one per line. The `tag_processor` step has already set each message's topic to
`umh.v1.enterprise.site.area.line._historian.<symbol>`, which is what the `uns` output uses as the
message key.

If the log stops after `Connected to PLC`, or symbols register but no values arrive, go to
[Troubleshooting](troubleshooting.md). The cause is most often a wrong `hostIP`.

## The same config on TwinCAT 2

Global variables carry a leading dot, program variables an uppercase program prefix:

```yaml
input:
  ads:
    targetAddress: "192.168.1.200"
    runtimePort: 801                    # first TwinCAT 2 runtime; the default 851 is TwinCAT 3
    hostIP: "192.168.1.50"
    username: "Administrator"
    password: "1"
    unifiedAddress:
      - ".nMasterCycleCounter"
      - "PRG_DIAGNOSTICS.nInt"
```

## Next

- Reading structs, arrays or strings: [Symbols and metadata](symbols-and-metadata.md)
- Running in a container, or across a VPN: [Networking](networking.md)
- Tuning how often values arrive: [Configuration](configuration.md)
