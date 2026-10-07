---
paths:
  - "opcua_plugin/**"
  - "docs/input/opc-ua-input.md"
  - "docs/output/opc-ua-output.md"
---

# OPC UA plugin

## Browsing

Browse follows all hierarchical references (`id.HierarchicalReferences`: `Organizes`, `HasComponent`, `HasProperty`, …) and returns Object and Variable nodes only (`opcua_plugin/core_browse_global_pool.go`).

## Server profiles

The plugin detects the server vendor and sets the Browse worker count and the Subscribe batch size from a profile. Profile values are production-safe limits from vendor documentation, not the maximums a server reports. For example, the S7-1200 profile uses 100, not 1000.

- `opcua_plugin/server_profiles.go`: profile definitions and `DetectServerProfile()`.
- `opcua_plugin/core_browse_global_pool.go`: the Browse worker pool (`GlobalWorkerPool`).
- `opcua_plugin/read_discover.go`: Subscribe batching.

To add a profile, define it in `server_profiles.go`, add detection in `DetectServerProfile()`, and validate it in `init()`. User docs: `docs/input/opc-ua-input.md`, section "Server Profiles and Performance Tuning".
