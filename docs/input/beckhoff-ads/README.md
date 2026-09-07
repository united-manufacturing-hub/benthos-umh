# Beckhoff ADS (Input)

Reads variables from Beckhoff PLCs over ADS, by symbol name, either as change notifications pushed
by the PLC or as interval polling. Works with TwinCAT 2 and TwinCAT 3, from a VM or a container,
and can register its own route on the PLC.

This input reads **symbols only** — named variables such as `GVL_ProcessData.nMasterCycleCounter`.
Direct addresses (`%MB100`) are not supported.

## Where to go

| Page | For |
|---|---|
| [Quick start](quick-start.md) | Getting first values out of a PLC, step by step |
| [How it works](how-it-works.md) | ADS, AMS NetIDs and routes — read this once and the settings explain themselves |
| [Configuration](configuration.md) | Every setting, with complete examples |
| [Symbols and metadata](symbols-and-metadata.md) | How to address symbols, and what each message carries |
| [Networking](networking.md) | Containers, VPNs, NAT, and creating routes by hand |
| [Troubleshooting](troubleshooting.md) | Symptoms, causes, and what the log lines mean |

## Prerequisites

Have these ready before you start. Everything else has a working default.

| | What you need | How to check |
|---|---|---|
| **PLC address** | The IP address of the PLC, reachable from wherever this runs | `ping <plc-ip>` |
| **ADS port open** | TCP `48898` to the PLC | `nc -z -w3 <plc-ip> 48898` |
| **Route port open** | UDP `48899`, only if the plugin should register its own route | Blocked UDP means creating the route by hand instead |
| **PLC credentials** | An administrator user on the PLC, only for automatic route registration | The same login you would use in TwinCAT to add a route |
| **This client's address** | The address the PLC will see this client as — needed for `hostIP` | Usually the VM's IP on the PLC network. Behind a container boundary, VPN or NAT it is something else: see [Networking](networking.md) |
| **Symbol names** | The full name of each variable, e.g. `GVL_ProcessData.nMasterCycleCounter` | Read them from the PLC project in TwinCAT |
| **A running PLC runtime** | The runtime must be in RUN, not CONFIG | A stopped runtime accepts the connection and serves nothing |

Two limits worth knowing up front:

- Beckhoff recommends keeping notifications to roughly **500 per connection**. Above that, use
  `readType: interval`.
- A PLC serves **one connection per client address**. A second client from the same address —
  another bridge, an engineering station behind the same NAT gateway — displaces the first.
