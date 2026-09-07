# Quick start

Goal: values from one TwinCAT 3 PLC flowing into the UNS, in five steps. The same steps work for
TwinCAT 2 — only the symbol names differ, and there is a TwinCAT 2 example at the end.

You need the PLC's IP address and an administrator user on the PLC.

## 1. Find the symbols you want

Open the PLC project in TwinCAT and note the full name of each variable, for example
`GVL_ProcessData.nMasterCycleCounter`. Global variables live under their GVL name; program
variables under the program name.

## 2. Write the input

`targetAMS` and `runtimePort` are discovered from the PLC, so the address is all that is required.
`hostIP` is the address the PLC will see this client as — the VM's IP on the PLC network:

```yaml
input:
  ads:
    targetAddress: "192.168.1.100"
    hostIP: "192.168.1.50"
    username: "Administrator"
    password: "1"
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

## 4. Send it to the UNS

```yaml
output:
  uns: {}
```

## 5. Check it worked

On the first connect the plugin registers a route on the PLC, subscribes to each symbol, and waits
for one value from each before reporting healthy. In the logs you should see, in order:

```
Registering route on PLC        (only the first time, or after the PLC forgets the route)
Connected to PLC
Registering notifications succeeded for 2/2 symbols
Input type ads is now active
```

Then values appear in the Topic Browser under
`umh.v1.enterprise.site.area.line._historian.<symbol>`.

If the log stops after `Connected to PLC`, or symbols register but no values arrive, go to
[Troubleshooting](troubleshooting.md) — the cause is often wrong `hostIP`.

## The same thing on TwinCAT 2

Global variables carry a leading dot, program variables an uppercase program prefix:

```yaml
input:
  ads:
    targetAddress: "192.168.1.200"
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
