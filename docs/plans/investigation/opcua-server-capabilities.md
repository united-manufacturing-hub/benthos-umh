# OPC UA ServerCapabilities Programmatic Discovery

**Investigation Date:** 2025-11-05
**Context:** Investigating whether to query OPC UA server capabilities dynamically instead of hardcoding values in ServerProfile objects
**Status:** Research Complete

---

## Executive Summary

**What can be queried:** OPC UA servers expose 12 standardized operation limits through the `ServerCapabilities.OperationLimits` node, including `MaxNodesPerBrowse`, `MaxMonitoredItemsPerCall`, and other service-specific limits.

**What should be used:** **Profile-based limits with optional dynamic querying for validation**. Hardcoded profiles are more reliable because:
- Not all servers implement OperationLimits (especially embedded PLCs)
- Server-reported values may be zero (unlimited/unknown)
- No additional connection overhead
- Fail-fast validation before subscription

**Key recommendation:** Keep current ServerProfile approach, add optional OperationLimits query for logging/validation, do NOT use as primary source for limits.

---

## ServerCapabilities Node

### Definition and Location

**Node ID:** `ns=0;i=2268` (ServerCapabilities)
**OPC UA Specification:** Part 5 (Information Model), Section 6.3.2 - ServerCapabilitiesType
**Status:** **Mandatory** - All OPC UA servers must expose this node
**Parent:** Server object (`ns=0;i=2253`)

**Address Space Path:**
```
Root (ns=0;i=84)
  └─ Objects (ns=0;i=85)
      └─ Server (ns=0;i=2253)
          └─ ServerCapabilities (ns=0;i=2268)
              ├─ OperationLimits (ns=0;i=11704)
              ├─ ServerProfileArray
              ├─ LocaleIdArray
              ├─ MinSupportedSampleRate
              ├─ MaxBrowseContinuationPoints
              ├─ MaxArrayLength
              ├─ MaxStringLength
              └─ MaxByteStringLength
```

### Available Information

The ServerCapabilities object exposes:

1. **ServerProfileArray** - List of supported OPC UA profiles (e.g., "Standard UA Server", "Embedded UA Server")
2. **SoftwareCertificates** - Signed certificates identifying server capabilities
3. **LocaleIdArray** - Supported localization languages
4. **MinSupportedSampleRate** - Minimum sampling rate for subscriptions (ms)
5. **MaxBrowseContinuationPoints** - Maximum parallel browse operations per session
6. **MaxArrayLength** - Maximum array size in variables
7. **MaxStringLength** - Maximum string length in bytes
8. **MaxByteStringLength** - Maximum ByteString length
9. **OperationLimits** - Service-specific operation limits (detailed below)

**Relevance to benthos-umh:**
- `MinSupportedSampleRate` → Affects `SamplingInterval` configuration
- `MaxBrowseContinuationPoints` → Affects parallel browse workers
- `OperationLimits.MaxNodesPerBrowse` → Affects `MaxBatchSize` for browse operations
- `OperationLimits.MaxMonitoredItemsPerCall` → Affects `MaxBatchSize` for monitoring

---

## OperationLimits Node

### Definition and Location

**Node ID:** `ns=0;i=11704` (OperationLimits)
**OPC UA Specification:** Part 5 (Information Model), Section 6.3.11 - OperationLimitsType
**Status:** **Optional** - Servers MAY expose this node, but it's not mandatory
**Parent:** ServerCapabilities (`ns=0;i=2268`)
**Type:** FolderType (ObjectType)

**Purpose:** Provides entry point to access server-imposed limits on various OPC UA service operations. Helps clients avoid "Bad_TooManyOperations" errors by querying limits before making service calls.

### Queryable Limits

All properties are **UInt32** data type and **optional**. If not present or set to zero, assume no limit or server default applies.

#### MaxNodesPerRead
- **Node ID:** `ns=0;i=11705`
- **Data Type:** UInt32
- **Purpose:** Maximum size of `nodesToRead` array in Read service call
- **Relevance to benthos-umh:** Affects batch reading during polling mode (when `subscribeEnabled=false`)
- **Default if not present:** Server-dependent, typically 1000-10000 for VM servers, 100-1000 for PLCs

#### MaxNodesPerHistoryReadData
- **Node ID:** `ns=0;i=11706`
- **Data Type:** UInt32
- **Purpose:** Maximum size of `nodesToRead` array in HistoryRead service (RAW, PROCESSED, MODIFIED, ATTIME)
- **Relevance to benthos-umh:** Not currently used (benthos-umh doesn't implement historical data reading)
- **Default if not present:** Server-dependent

#### MaxNodesPerHistoryReadEvents
- **Node ID:** `ns=0;i=11707`
- **Data Type:** UInt32
- **Purpose:** Maximum size of `nodesToRead` array in HistoryRead service (EVENTS)
- **Relevance to benthos-umh:** Not currently used (no event history support)
- **Default if not present:** Server-dependent

#### MaxNodesPerWrite
- **Node ID:** `ns=0;i=11708`
- **Data Type:** UInt32
- **Purpose:** Maximum size of `nodesToWrite` array in Write service call
- **Relevance to benthos-umh:** Affects OPC UA output plugin batch writes (future enhancement)
- **Default if not present:** Server-dependent, typically matches MaxNodesPerRead

#### MaxNodesPerHistoryUpdateData
- **Node ID:** `ns=0;i=11709`
- **Data Type:** UInt32
- **Purpose:** Maximum size of `historyUpdateDetails` array in HistoryUpdate service
- **Relevance to benthos-umh:** Not currently used
- **Default if not present:** Server-dependent

#### MaxNodesPerHistoryUpdateEvents
- **Node ID:** `ns=0;i=11710`
- **Data Type:** UInt32
- **Purpose:** Maximum size of `historyUpdateDetails` array in HistoryUpdate service (events)
- **Relevance to benthos-umh:** Not currently used
- **Default if not present:** Server-dependent

#### MaxNodesPerMethodCall
- **Node ID:** `ns=0;i=11711`
- **Data Type:** UInt32
- **Purpose:** Maximum size of `methodsToCall` array in Call service
- **Relevance to benthos-umh:** Not currently used (no method call support)
- **Default if not present:** Server-dependent

#### MaxNodesPerBrowse
- **Node ID:** `ns=0;i=11712`
- **Data Type:** UInt32
- **Purpose:** Maximum size of `nodesToBrowse` array in Browse service, OR `continuationPoints` array in BrowseNext service
- **Relevance to benthos-umh:** **CRITICAL** - Affects browse batch size during node discovery. Current `MaxBatchSize` in profiles should not exceed this value.
- **Default if not present:** OPC UA spec recommends 1000, but PLCs may only support 100-500
- **Impact:** If exceeded, server returns `Bad_TooManyOperations` (0x80E30000)

#### MaxNodesPerRegisterNodes
- **Node ID:** `ns=0;i=11713`
- **Data Type:** UInt32
- **Purpose:** Maximum size of `nodesToRegister` and `nodesToUnregister` arrays in RegisterNodes/UnregisterNodes services
- **Relevance to benthos-umh:** Not currently used (benthos-umh doesn't register nodes)
- **Default if not present:** Server-dependent

#### MaxNodesPerTranslateBrowsePathsToNodeIds
- **Node ID:** `ns=0;i=11714`
- **Data Type:** UInt32
- **Purpose:** Maximum size of `browsePaths` array in TranslateBrowsePathsToNodeIds service
- **Relevance to benthos-umh:** Not currently used (direct NodeID access only)
- **Default if not present:** Server-dependent

#### MaxNodesPerNodeManagement
- **Node ID:** `ns=0;i=11715`
- **Data Type:** UInt32
- **Purpose:** Maximum size for AddNodes, AddReferences, DeleteNodes, DeleteReferences service arrays
- **Relevance to benthos-umh:** Not applicable (read-only client)
- **Default if not present:** Server-dependent

#### MaxMonitoredItemsPerCall
- **Node ID:** `ns=0;i=11574` ⚠️ (Different namespace range!)
- **Data Type:** UInt32
- **Purpose:** Maximum size for CreateMonitoredItems, ModifyMonitoredItems, SetMonitoringMode, SetTriggering arrays
- **Relevance to benthos-umh:** **CRITICAL** - Affects `MonitorBatched()` batch size when `subscribeEnabled=true`. This is the most important limit for benthos-umh.
- **Default if not present:** OPC UA spec recommends 1000, but S7-1200 = 100, S7-1500 = 500
- **Impact:** If exceeded, subscription fails with OPC UA status code (e.g., `StatusBadTooManyMonitoredItems`)

---

## How to Query Programmatically

### Using gopcua Library

The gopcua library doesn't provide a built-in helper for ServerCapabilities, but you can query nodes directly using the standard Read service.

#### Option 1: Direct Property Read (Simple)

Query individual limit properties by NodeID:

```go
package opcua_plugin

import (
    "context"
    "fmt"

    "github.com/gopcua/opcua"
    "github.com/gopcua/opcua/ua"
)

// QueryableLimit represents a single operation limit
type QueryableLimit struct {
    Name      string
    NodeID    *ua.NodeID
    Value     uint32
    Available bool
}

// ServerCapabilitiesQueried holds dynamically queried server capabilities
type ServerCapabilitiesQueried struct {
    MaxNodesPerRead                      *QueryableLimit
    MaxNodesPerBrowse                    *QueryableLimit
    MaxMonitoredItemsPerCall             *QueryableLimit
    MaxNodesPerWrite                     *QueryableLimit
    MinSupportedSampleRate               float64
    MaxBrowseContinuationPoints          uint32
}

// QueryServerOperationLimits reads OperationLimits from server
func (g *OPCUAInput) QueryServerOperationLimits(ctx context.Context) (*ServerCapabilitiesQueried, error) {
    caps := &ServerCapabilitiesQueried{
        MaxNodesPerRead:          &QueryableLimit{Name: "MaxNodesPerRead", NodeID: ua.NewNumericNodeID(0, 11705)},
        MaxNodesPerBrowse:        &QueryableLimit{Name: "MaxNodesPerBrowse", NodeID: ua.NewNumericNodeID(0, 11712)},
        MaxMonitoredItemsPerCall: &QueryableLimit{Name: "MaxMonitoredItemsPerCall", NodeID: ua.NewNumericNodeID(0, 11574)},
        MaxNodesPerWrite:         &QueryableLimit{Name: "MaxNodesPerWrite", NodeID: ua.NewNumericNodeID(0, 11708)},
    }

    // Build read request for all limits
    nodesToRead := []*ua.ReadValueID{
        {NodeID: caps.MaxNodesPerRead.NodeID, AttributeID: ua.AttributeIDValue},
        {NodeID: caps.MaxNodesPerBrowse.NodeID, AttributeID: ua.AttributeIDValue},
        {NodeID: caps.MaxMonitoredItemsPerCall.NodeID, AttributeID: ua.AttributeIDValue},
        {NodeID: caps.MaxNodesPerWrite.NodeID, AttributeID: ua.AttributeIDValue},

        // Also query non-OperationLimits capabilities
        {NodeID: ua.NewNumericNodeID(0, 2272), AttributeID: ua.AttributeIDValue}, // MinSupportedSampleRate
        {NodeID: ua.NewNumericNodeID(0, 2735), AttributeID: ua.AttributeIDValue}, // MaxBrowseContinuationPoints
    }

    req := &ua.ReadRequest{
        NodesToRead: nodesToRead,
    }

    resp, err := g.Client.Read(ctx, req)
    if err != nil {
        return nil, fmt.Errorf("failed to read ServerCapabilities: %w", err)
    }

    // Parse MaxNodesPerRead
    if len(resp.Results) > 0 && resp.Results[0].Status == ua.StatusOK {
        if val, ok := resp.Results[0].Value.Value().(uint32); ok {
            caps.MaxNodesPerRead.Value = val
            caps.MaxNodesPerRead.Available = true
        }
    }

    // Parse MaxNodesPerBrowse
    if len(resp.Results) > 1 && resp.Results[1].Status == ua.StatusOK {
        if val, ok := resp.Results[1].Value.Value().(uint32); ok {
            caps.MaxNodesPerBrowse.Value = val
            caps.MaxNodesPerBrowse.Available = true
        }
    }

    // Parse MaxMonitoredItemsPerCall
    if len(resp.Results) > 2 && resp.Results[2].Status == ua.StatusOK {
        if val, ok := resp.Results[2].Value.Value().(uint32); ok {
            caps.MaxMonitoredItemsPerCall.Value = val
            caps.MaxMonitoredItemsPerCall.Available = true
        }
    }

    // Parse MaxNodesPerWrite
    if len(resp.Results) > 3 && resp.Results[3].Status == ua.StatusOK {
        if val, ok := resp.Results[3].Value.Value().(uint32); ok {
            caps.MaxNodesPerWrite.Value = val
            caps.MaxNodesPerWrite.Available = true
        }
    }

    // Parse MinSupportedSampleRate
    if len(resp.Results) > 4 && resp.Results[4].Status == ua.StatusOK {
        if val, ok := resp.Results[4].Value.Value().(float64); ok {
            caps.MinSupportedSampleRate = val
        }
    }

    // Parse MaxBrowseContinuationPoints
    if len(resp.Results) > 5 && resp.Results[5].Status == ua.StatusOK {
        if val, ok := resp.Results[5].Value.Value().(uint32); ok {
            caps.MaxBrowseContinuationPoints = val
        }
    }

    return caps, nil
}
```

#### Option 2: Browse OperationLimits Children (Discovery)

Query all available limits dynamically by browsing the OperationLimits node:

```go
// DiscoverServerOperationLimits browses OperationLimits node and reads all children
func (g *OPCUAInput) DiscoverServerOperationLimits(ctx context.Context) (map[string]uint32, error) {
    operationLimitsNodeID := ua.NewNumericNodeID(0, 11704)

    // Browse OperationLimits children
    node := g.Client.Node(operationLimitsNodeID)
    refs, err := node.References(ctx,
        ua.NewNumericNodeID(0, 46), // HasProperty reference type
        ua.BrowseDirectionForward,
        ua.NodeClassVariable,
        true, // IncludeSubtypes
    )
    if err != nil {
        return nil, fmt.Errorf("failed to browse OperationLimits: %w", err)
    }

    limits := make(map[string]uint32)

    // Read each property value
    for _, ref := range refs {
        limitNode := g.Client.Node(ref.NodeID.NodeID)
        val, err := limitNode.Value(ctx)
        if err != nil {
            g.Log.Warnf("Failed to read %s: %v", ref.BrowseName.Name, err)
            continue
        }

        if uintVal, ok := val.Value().(uint32); ok {
            limits[ref.BrowseName.Name] = uintVal
            g.Log.Debugf("OperationLimit: %s = %d", ref.BrowseName.Name, uintVal)
        }
    }

    return limits, nil
}
```

### Error Handling

**Critical:** Always handle missing or unsupported OperationLimits gracefully.

```go
func (g *OPCUAInput) QueryOperationLimitsSafe(ctx context.Context) {
    caps, err := g.QueryServerOperationLimits(ctx)
    if err != nil {
        // OperationLimits not supported by server (common for embedded PLCs)
        g.Log.Infof("Server does not expose OperationLimits (common for PLCs): %v", err)
        return
    }

    // Log discovered limits
    if caps.MaxNodesPerBrowse.Available {
        g.Log.Infof("Server reports MaxNodesPerBrowse: %d", caps.MaxNodesPerBrowse.Value)

        // Compare with profile limit
        if g.ServerProfile.MaxBatchSize > int(caps.MaxNodesPerBrowse.Value) {
            g.Log.Warnf(
                "Profile MaxBatchSize (%d) exceeds server's MaxNodesPerBrowse (%d). Server limit will be respected.",
                g.ServerProfile.MaxBatchSize,
                caps.MaxNodesPerBrowse.Value,
            )
        }
    } else {
        g.Log.Infof("Server does not expose MaxNodesPerBrowse, using profile default: %d", g.ServerProfile.MaxBatchSize)
    }

    if caps.MaxMonitoredItemsPerCall.Available {
        g.Log.Infof("Server reports MaxMonitoredItemsPerCall: %d", caps.MaxMonitoredItemsPerCall.Value)

        // Validate against profile
        if caps.MaxMonitoredItemsPerCall.Value == 0 {
            g.Log.Infof("Server reports MaxMonitoredItemsPerCall=0 (unlimited or unknown)")
        } else if len(g.NodeList) > int(caps.MaxMonitoredItemsPerCall.Value) {
            g.Log.Errorf(
                "Cannot monitor %d nodes: server MaxMonitoredItemsPerCall limit is %d",
                len(g.NodeList),
                caps.MaxMonitoredItemsPerCall.Value,
            )
        }
    }
}
```

**Handle these scenarios:**

1. **OperationLimits node missing** (Status: `Bad_NodeIdUnknown`)
   - Fallback: Use profile defaults
   - Action: Log informational message

2. **Individual limit property missing** (Status: `Bad_NodeIdUnknown` for child node)
   - Fallback: Use profile default for that specific limit
   - Action: Log debug message

3. **Limit value is zero**
   - Interpretation: Unlimited OR unknown (ambiguous in OPC UA spec)
   - Fallback: Use profile default (safer assumption)
   - Action: Log warning about ambiguous zero value

4. **Server returns `Bad_AttributeIdInvalid`**
   - Cause: Node exists but doesn't support Value attribute (rare)
   - Fallback: Use profile default
   - Action: Log warning

---

## Real-World Server Support

### Research Methodology

This assessment is based on:
- Official vendor documentation
- OPC UA server implementation manuals
- Forum discussions from OPC Foundation and vendor forums
- Open-source OPC UA server implementations (Eclipse Milo, open62541)

**Note:** No live server testing was performed. Real-world behavior may vary by firmware version.

### Server Compatibility Matrix

| Server | ServerCapabilities (i=2268) | OperationLimits (i=11704) | Specific Limits Exposed | Notes |
|--------|----------------------------|---------------------------|-------------------------|-------|
| **Ignition Gateway** | ✅ Yes | ✅ Yes | MaxNodesPerRead, MaxNodesPerBrowse, MaxMonitoredItemsPerCall, MaxNodesPerWrite, MaxNodesPerMethodCall | Based on Eclipse Milo implementation. All limits configurable via server config. Default: MaxMonitoredItemsPerCall=1000 |
| **Kepware KEPServerEX** | ✅ Yes | ⚠️ Partial | MaxArrayLength, MaxStringLength, MaxByteStringLength | Documentation unclear on OperationLimits support. Likely exposes ServerCapabilities but not full OperationLimits. Defaults: Memory-bound |
| **Siemens S7-1200** | ✅ Yes | ❌ No | None (hardware limits not exposed) | ServerCapabilities exists for profile information, but OperationLimits NOT implemented. Limits are firmware-based: 1000 total monitored items, 10 sessions |
| **Siemens S7-1500** | ✅ Yes | ❌ No | None (hardware limits not exposed) | Same as S7-1200. Limits are firmware-based: 10,000 total monitored items (V3.1+), 32 sessions |
| **Prosys Simulation Server** | ✅ Yes | ✅ Yes | All standard limits | Full OPC UA compliance. Exposes all OperationLimits. Defaults: MaxMonitoredItemsPerCall=1000, MaxNodesPerBrowse=1000 |
| **open62541** | ✅ Yes | ⚠️ Configurable | Depends on build config | Open-source server. OperationLimits can be enabled/disabled at compile time. Not guaranteed in all builds |

**Legend:**
- ✅ Yes = Fully implemented and reliable
- ⚠️ Partial = Implemented but incomplete or version-dependent
- ❌ No = Not implemented (use profile defaults)

### Detailed Server Analysis

#### Ignition Gateway (Eclipse Milo)

**OperationLimits Support:** ✅ **Full**

**Evidence:**
- Eclipse Milo source code (`OpcUaNamespace.java`) implements OperationLimits node with all properties
- Limits are configurable via `OpcUaServerConfigLimits` object
- Default values pulled from server configuration

**Available Limits:**
- MaxNodesPerRead: Configurable (default 1000)
- MaxNodesPerBrowse: Configurable (default 1000)
- MaxMonitoredItemsPerCall: Configurable (default 1000)
- MaxNodesPerWrite: Configurable (default 1000)
- MaxNodesPerMethodCall: Configurable (default 1000)
- MaxNodesPerHistoryReadData: Configurable
- MaxNodesPerHistoryReadEvents: Configurable

**Querying Strategy:**
- ✅ Query OperationLimits during connection
- ✅ Use queried values for validation
- ⚠️ Zero values mean "unlimited" in Milo (don't fallback to profile)

#### Kepware KEPServerEX

**OperationLimits Support:** ⚠️ **Partial/Unknown**

**Evidence:**
- Documentation mentions ServerCapabilities support
- Explicit OperationLimits documentation not found
- Likely exposes basic ServerCapabilities properties (MaxArrayLength, MaxStringLength)

**Available Limits:**
- MaxArrayLength: Likely exposed
- MaxStringLength: Likely exposed
- MaxBrowseContinuationPoints: Likely exposed
- MaxMonitoredItemsPerCall: **Unknown** (documentation unclear)
- MaxNodesPerBrowse: **Unknown**

**Querying Strategy:**
- ✅ Query OperationLimits, but expect failures
- ⚠️ Fallback to profile limits immediately if node not found
- ⚠️ Kepware is memory-bound, not hard-limited (profile assumes unlimited)

#### Siemens S7-1200 PLC

**OperationLimits Support:** ❌ **Not Implemented**

**Evidence:**
- Siemens SIMATIC S7-1200 OPC UA Server manual (Firmware V4.4+)
- Hardware limits are firmware-enforced, NOT exposed via OPC UA address space
- Forum reports confirm OperationLimits node missing

**Hardware Limits (from manual):**
- Max Sessions: 10
- Max Monitored Items: **1,000** (total across all sessions)
- Max Subscriptions: 10
- Max Browse Continuations: Unknown (likely 1-2)

**Available in ServerCapabilities:**
- ServerProfileArray: Yes (exposes "Embedded UA Server" profile)
- LocaleIdArray: Yes
- MaxArrayLength: Possibly
- OperationLimits: **NO**

**Querying Strategy:**
- ❌ Do NOT query OperationLimits (will fail)
- ✅ Use profile defaults exclusively
- ✅ Detect S7-1200 via ServerInfo.ProductName and apply hardcoded limits

#### Siemens S7-1500 PLC

**OperationLimits Support:** ❌ **Not Implemented**

**Evidence:**
- Similar to S7-1200 (same OPC UA server implementation)
- Hardware limits are firmware-enforced

**Hardware Limits (from manual):**
- Max Sessions: 32
- Max Monitored Items: **10,000** (total across all sessions, CPU 1511-1513, Firmware V3.1+)
- Max Subscriptions: 32
- Max Browse Continuations: Unknown

**Querying Strategy:**
- Same as S7-1200 (do NOT query, use profile)

#### Prosys Simulation Server

**OperationLimits Support:** ✅ **Full**

**Evidence:**
- Prosys is a reference implementation designed for OPC UA testing
- Fully spec-compliant
- All OperationLimits nodes present and functional

**Available Limits:**
- All 12 OperationLimits properties exposed
- Default values: 1000 for most limits
- Configurable via server settings

**Querying Strategy:**
- ✅ Query OperationLimits reliably
- ✅ Use queried values
- ⚠️ Still validate against profile (Prosys is simulation, real workloads differ)

### Summary: What Happens When Server Doesn't Support OperationLimits?

**Common Scenarios:**

1. **OperationLimits node missing** (Siemens PLCs)
   - Read request returns `Bad_NodeIdUnknown` (0x80340000)
   - Client must catch this error and fallback to profile defaults
   - **No impact on operation** if handled gracefully

2. **OperationLimits node exists but properties are zero**
   - Ambiguous: Could mean "unlimited" OR "not configured"
   - OPC UA spec doesn't mandate interpretation
   - **Safe fallback:** Treat zero as "unknown" and use profile defaults

3. **Server doesn't enforce reported limits**
   - Rare but possible (buggy implementations)
   - Server may allow operations beyond stated limits
   - **Risk:** Client relies on wrong limit, operation unexpectedly succeeds
   - **Mitigation:** Use profile limits as conservative floor

**Fallback Strategy Recommendations:**

```
IF OperationLimits.MaxNodesPerBrowse query succeeds:
    IF value > 0:
        actual_limit = MIN(profile.MaxBatchSize, queried_value)
    ELSE: # Zero value
        actual_limit = profile.MaxBatchSize  # Treat zero as unknown
ELSE: # Query failed
    actual_limit = profile.MaxBatchSize  # Server doesn't expose limits
```

---

## Integration Recommendations

### When to Query Capabilities

**Recommended Timing:** During connection setup, AFTER successful authentication, BEFORE browsing nodes

**Rationale:**
- One-time overhead per connection (amortized across thousands of reads)
- Early detection of server limits prevents mid-operation failures
- Results can be logged for troubleshooting

**Implementation Location:**

```go
// opcua_plugin/core_connection.go

func (c *OPCUAConnection) Connect(ctx context.Context) error {
    // ... existing connection logic ...

    if err := c.Client.Connect(ctx); err != nil {
        return err
    }

    // Query server info (existing)
    serverInfo, err := c.queryServerInfo(ctx)
    if err != nil {
        return fmt.Errorf("failed to query server info: %w", err)
    }

    // Detect server profile (existing)
    profile := DetectServerProfile(serverInfo)

    // NEW: Query OperationLimits for validation/logging
    queriedCaps, err := c.QueryServerOperationLimits(ctx)
    if err != nil {
        c.Log.Infof("Server does not expose OperationLimits: %v (using profile defaults)", err)
    } else {
        c.LogDiscoveredCapabilities(queriedCaps, profile)
        c.ValidateProfileAgainstServer(queriedCaps, profile)
    }

    return nil
}
```

### Profile Override Strategy

**Core Principle:** Profile defaults are PRIMARY, queried values are VALIDATION/WARNING only.

**DO NOT directly override profile values.** Instead, use queried values for:
1. **Logging** - Show user what server reports vs. profile defaults
2. **Validation** - Warn if profile exceeds server limits
3. **Debugging** - Help diagnose "TooManyOperations" errors

**Pseudocode:**

```go
// LogDiscoveredCapabilities logs comparison between profile and server
func (c *OPCUAConnection) LogDiscoveredCapabilities(queried *ServerCapabilitiesQueried, profile ServerProfile) {
    c.Log.Infof("=== Server Capabilities Discovery ===")
    c.Log.Infof("Profile: %s", profile.Name)

    if queried.MaxNodesPerBrowse.Available {
        c.Log.Infof("  MaxNodesPerBrowse: Server=%d, Profile=%d",
            queried.MaxNodesPerBrowse.Value, profile.MaxBatchSize)
    } else {
        c.Log.Infof("  MaxNodesPerBrowse: Not exposed (using profile default %d)", profile.MaxBatchSize)
    }

    if queried.MaxMonitoredItemsPerCall.Available {
        c.Log.Infof("  MaxMonitoredItemsPerCall: Server=%d, Profile=%d",
            queried.MaxMonitoredItemsPerCall.Value, profile.MaxBatchSize)
    } else {
        c.Log.Infof("  MaxMonitoredItemsPerCall: Not exposed (using profile default %d)", profile.MaxBatchSize)
    }
}

// ValidateProfileAgainstServer warns if profile exceeds server limits
func (c *OPCUAConnection) ValidateProfileAgainstServer(queried *ServerCapabilitiesQueried, profile ServerProfile) {
    if queried.MaxNodesPerBrowse.Available && queried.MaxNodesPerBrowse.Value > 0 {
        if profile.MaxBatchSize > int(queried.MaxNodesPerBrowse.Value) {
            c.Log.Warnf(
                "Profile MaxBatchSize (%d) exceeds server's MaxNodesPerBrowse (%d). "+
                "Consider using a different profile or server may return errors.",
                profile.MaxBatchSize, queried.MaxNodesPerBrowse.Value,
            )
        }
    }

    if queried.MaxMonitoredItemsPerCall.Available && queried.MaxMonitoredItemsPerCall.Value > 0 {
        if profile.MaxBatchSize > int(queried.MaxMonitoredItemsPerCall.Value) {
            c.Log.Warnf(
                "Profile MaxBatchSize (%d) exceeds server's MaxMonitoredItemsPerCall (%d). "+
                "Subscription may fail if too many nodes selected.",
                profile.MaxBatchSize, queried.MaxMonitoredItemsPerCall.Value,
            )
        }
    }
}
```

### Trust vs Hardcode Decision Matrix

| Scenario | Use Queried Value | Use Profile Default | Rationale |
|----------|------------------|---------------------|-----------|
| **Server reports limit=0** | ❌ | ✅ | Zero is ambiguous (unlimited OR unknown). Profile defaults are safer. |
| **S7-1200/1500 (OperationLimits missing)** | ❌ | ✅ | Hardware limits are firmware-enforced but not exposed. Trust manufacturer docs. |
| **Ignition/Milo reports limit > 0** | ⚠️ Log only | ✅ | Use profile, but log if profile exceeds server limit (warn user). |
| **Prosys reports limit > 0** | ⚠️ Log only | ✅ | Simulation server, not representative of production load. Use profile. |
| **Kepware (OperationLimits missing)** | ❌ | ✅ | Memory-bound server, no hard limits. Profile assumes unlimited. |
| **Unknown server reports limit > 0** | ⚠️ Log only | ✅ | Unknown vendor reliability. Conservative profile is safer. |
| **Query fails (node missing)** | ❌ | ✅ | Server doesn't implement OperationLimits. Always use profile. |

**Key Insight:** Always use profile defaults for actual operation limits. Use queried values ONLY for logging and validation warnings.

### Performance Considerations

**Cost of Querying:**
- Single Read request with ~6 NodeIDs
- Typical response time: 10-50ms on LAN, 50-200ms on WAN
- Negligible compared to browsing thousands of nodes (seconds to minutes)

**Caching Strategy:**
- Query once per connection
- Store results in `OPCUAInput` struct
- Re-query on reconnection (server may have restarted with different config)
- Do NOT cache across process restarts (server firmware may change)

**When to Skip Queries:**
- **S7-1200/1500 detected:** Skip OperationLimits query (known to fail)
- **Fast connection requirement:** Skip if connection time is critical
- **Embedded systems:** Skip if client runs on resource-constrained hardware

**Implementation:**

```go
// opcua_plugin/core_connection.go

// SkipServerCapabilitiesQuery returns true if querying would fail or waste time
func (c *OPCUAConnection) SkipServerCapabilitiesQuery(profile ServerProfile) bool {
    // Siemens PLCs don't expose OperationLimits - skip query
    if profile.Name == ProfileS71200 || profile.Name == ProfileS71500 {
        c.Log.Debugf("Skipping OperationLimits query for %s (known unsupported)", profile.Name)
        return true
    }

    // Could add other skip conditions here
    // - User config flag to disable queries
    // - Known problematic server versions

    return false
}
```

---

## Implementation Plan

### Phase 1: Read-Only Discovery (Low Risk, High Value)

**Goal:** Query capabilities, log results, don't change behavior

**Tasks:**
1. Add `QueryServerOperationLimits()` function to `opcua_plugin/core_connection.go`
2. Call during `Connect()` after server profile detection
3. Log discovered capabilities with comparison to profile defaults
4. Add validation warnings if profile exceeds server limits

**Code Changes:**

```go
// opcua_plugin/core_connection.go

type OPCUAConnection struct {
    // ... existing fields ...
    QueriedCapabilities *ServerCapabilitiesQueried
}

func (c *OPCUAConnection) Connect(ctx context.Context) error {
    // ... existing connection logic ...

    // NEW: Query and log server capabilities
    if !c.SkipServerCapabilitiesQuery(c.ServerProfile) {
        queriedCaps, err := c.QueryServerOperationLimits(ctx)
        if err != nil {
            c.Log.Infof("Server does not expose OperationLimits: %v", err)
        } else {
            c.QueriedCapabilities = queriedCaps
            c.LogDiscoveredCapabilities(queriedCaps, c.ServerProfile)
            c.ValidateProfileAgainstServer(queriedCaps, c.ServerProfile)
        }
    }

    return nil
}
```

**Benefits:**
- Zero risk (no behavior change)
- Provides visibility into server capabilities
- Helps validate profile accuracy
- Aids troubleshooting of "TooManyOperations" errors

**Estimated Effort:** 2-3 hours

### Phase 2: Validation Against Profiles (Medium Risk, High Value)

**Goal:** Compare queried values with profile defaults, identify discrepancies

**Tasks:**
1. Add unit tests for profile vs. queried value comparison
2. Collect data from real servers (Ignition, Kepware, Prosys)
3. Update profile defaults if consistent discrepancies found
4. Document any servers with incorrect/misleading OperationLimits

**Metrics to Collect:**
- How often do servers expose OperationLimits? (exposure rate)
- How often do reported limits differ from profile defaults? (delta rate)
- How often are reported limits zero? (ambiguous rate)

**Implementation:**

```go
// opcua_plugin/metrics.go

type ServerCapabilitiesMetrics struct {
    OperationLimitsAvailable     bool
    MaxNodesPerBrowseReported    uint32
    MaxMonitoredItemsReported    uint32
    ProfileMaxBatchSize          int
    LimitExceedsProfile          bool
}

func (m *ServerCapabilitiesMetrics) LogToTelemetry() {
    // Send metrics to observability platform for analysis
    // Track which servers expose limits, which don't
    // Identify systematic profile vs. reality gaps
}
```

**Benefits:**
- Data-driven profile refinement
- Identify servers with buggy OperationLimits implementations
- Build confidence in query reliability

**Estimated Effort:** 4-6 hours (including testing with real servers)

### Phase 3: Dynamic Profile Adjustment (High Risk, Conditional)

**Goal:** Override profile values with queried capabilities WHERE SAFE

**Decision Point:** Only proceed if Phase 2 data shows:
- >80% of tested servers expose OperationLimits
- <5% report zero values (ambiguous)
- <10% report values that cause failures

**Tasks:**
1. Add config flag `use_dynamic_limits: bool` (default `false`)
2. When enabled, use `MIN(profile, queried)` for limits
3. Always fallback to profile if query fails or value is zero
4. Add extensive logging when dynamic limits are applied

**Implementation:**

```go
// opcua_plugin/server_profiles.go

type ServerProfile struct {
    // ... existing fields ...

    // NEW: Runtime-adjusted limits (set during connection)
    ActualMaxBatchSize int  // Effective limit used for operations
}

func (c *OPCUAConnection) ApplyDynamicLimits(config ConfigWithDynamicLimits) {
    // Start with profile default
    c.ServerProfile.ActualMaxBatchSize = c.ServerProfile.MaxBatchSize

    // Only override if explicitly enabled AND queried value is valid
    if config.UseDynamicLimits && c.QueriedCapabilities != nil {
        if c.QueriedCapabilities.MaxNodesPerBrowse.Available &&
           c.QueriedCapabilities.MaxNodesPerBrowse.Value > 0 {

            // Use minimum of profile and server-reported limit
            c.ServerProfile.ActualMaxBatchSize = min(
                c.ServerProfile.MaxBatchSize,
                int(c.QueriedCapabilities.MaxNodesPerBrowse.Value),
            )

            c.Log.Infof("Dynamic limits enabled: Using MaxBatchSize=%d (profile=%d, server=%d)",
                c.ServerProfile.ActualMaxBatchSize,
                c.ServerProfile.MaxBatchSize,
                c.QueriedCapabilities.MaxNodesPerBrowse.Value,
            )
        }
    }
}
```

**Risks:**
- Server reports incorrect limit → operations fail unexpectedly
- Zero value ambiguity → unclear whether to trust or ignore
- Firmware updates change limits → client uses stale cached values

**Mitigation:**
- Feature flag (opt-in, not default)
- Always use `MIN(profile, queried)` (never exceed profile)
- Extensive logging for debugging
- Fallback to profile on any query failure

**Estimated Effort:** 6-8 hours (including safety testing)

---

## Limitations and Caveats

### What Cannot Be Queried

**Total Server-Wide Limits:**
- **Max Total Monitored Items** (e.g., S7-1200's 1000-item global limit)
  - OperationLimits only expose per-call limits, not aggregate totals
  - Must rely on vendor documentation for global caps
  - **Workaround:** Add `MaxTotalMonitoredItems` to ServerProfile (see `monitored-item-limits.md`)

- **Max Concurrent Sessions**
  - Not part of OperationLimits
  - Exposed in ServerDiagnostics (different node tree)
  - Rarely enforced in practice (sessions are cheap)

- **Memory Constraints**
  - Servers don't expose available RAM
  - Limits are soft (degrade performance, not hard failures)

**Performance Characteristics:**
- **Actual Throughput** (messages/second)
  - OperationLimits define API constraints, not performance
  - Real throughput depends on network, CPU, I/O
  - Must be determined through benchmarking

- **Optimal Batch Sizes**
  - MaxNodesPerBrowse is upper bound, not optimal value
  - Smaller batches may perform better (less latency, better concurrency)
  - Requires workload-specific tuning

**Firmware-Specific Behavior:**
- **Deadband Support** (already queried via ServerCapabilities)
- **Sampling Rate Constraints**
  - MinSupportedSampleRate is queryable
  - Actual supported rates vary by node type (not exposed)

### Unreliable Values

**Known Cases Where Servers Report Incorrect Limits:**

1. **Zero Values (Ambiguous)**
   - OPC UA spec: "If not specified or zero, then the Server has no limit"
   - Reality: Some servers use zero to mean "not configured" or "unknown"
   - **Impact:** Cannot distinguish "unlimited" from "unimplemented"
   - **Recommendation:** Treat zero as "unknown", fallback to profile

2. **Overly Optimistic Limits**
   - Server reports MaxMonitoredItemsPerCall=10000 but crashes at 5000
   - Vendor testing was insufficient
   - **Impact:** Client trusts wrong limit, operation fails
   - **Recommendation:** Use profile as conservative floor, never exceed it

3. **Firmware Version Drift**
   - Older firmware reports different limits than newer firmware
   - OperationLimits may not update after firmware upgrade
   - **Impact:** Cached limits become stale
   - **Recommendation:** Query on every connection, don't cache across restarts

4. **Virtualized Servers**
   - VM-hosted servers may report limits based on allocated memory
   - Limits change if VM is resized (not reflected in OperationLimits)
   - **Impact:** Dynamic environment, static limits
   - **Recommendation:** Re-query periodically or on reconnection

### Fallback Strategy

**Three-Tier Fallback Hierarchy:**

```
1. Profile Default (PRIMARY)
   ├─ Source: Hardcoded in ServerProfile based on vendor documentation
   ├─ Reliability: High (tested, conservative)
   └─ Use: Always, as baseline

2. Queried Server Limit (VALIDATION)
   ├─ Source: Dynamic query during connection
   ├─ Reliability: Medium (server-dependent)
   └─ Use: Logging, validation warnings only

3. Runtime Observation (ADAPTIVE)
   ├─ Source: Monitor "TooManyOperations" errors during runtime
   ├─ Reliability: Highest (empirical, real workload)
   └─ Use: Future enhancement - auto-adjust if errors detected
```

**Decision Tree:**

```
START: Need to determine MaxBatchSize for browse operation

Is OperationLimits.MaxNodesPerBrowse available?
├─ NO → Use profile.MaxBatchSize (done)
└─ YES → Is value > 0?
    ├─ NO (value is 0) → Use profile.MaxBatchSize (treat zero as unknown)
    └─ YES → Log comparison:
        └─ "Profile=%d, Server=%d" % (profile.MaxBatchSize, queriedValue)
        └─ If queriedValue < profile.MaxBatchSize:
            └─ WARN: "Profile may exceed server limit"
        └─ Use profile.MaxBatchSize (trust profile, log server value)
```

---

## Code Examples

### Complete Example: Query and Apply Capabilities

```go
package opcua_plugin

import (
    "context"
    "fmt"
    "time"

    "github.com/gopcua/opcua"
    "github.com/gopcua/opcua/ua"
)

// QueryAndValidateServerCapabilities performs full capability discovery
func (c *OPCUAConnection) QueryAndValidateServerCapabilities(ctx context.Context) error {
    c.Log.Infof("Querying server capabilities...")

    // Skip query for known unsupported servers
    if c.SkipServerCapabilitiesQuery(c.ServerProfile) {
        c.Log.Infof("Skipping OperationLimits query (server type: %s)", c.ServerProfile.Name)
        return nil
    }

    // Query with timeout
    queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()

    caps, err := c.QueryServerOperationLimits(queryCtx)
    if err != nil {
        // Non-fatal: Server doesn't expose OperationLimits
        c.Log.Infof("OperationLimits not available: %v", err)
        c.Log.Infof("Using profile defaults for %s", c.ServerProfile.Name)
        return nil
    }

    // Store for later reference
    c.QueriedCapabilities = caps

    // Log discovered capabilities
    c.LogDiscoveredCapabilities(caps, c.ServerProfile)

    // Validate profile against server
    c.ValidateProfileAgainstServer(caps, c.ServerProfile)

    return nil
}

// QueryServerOperationLimits reads OperationLimits from server (implementation from earlier)
func (c *OPCUAConnection) QueryServerOperationLimits(ctx context.Context) (*ServerCapabilitiesQueried, error) {
    // ... (implementation shown in "How to Query Programmatically" section) ...
}

// LogDiscoveredCapabilities logs comparison (implementation from earlier)
func (c *OPCUAConnection) LogDiscoveredCapabilities(queried *ServerCapabilitiesQueried, profile ServerProfile) {
    // ... (implementation shown in "Profile Override Strategy" section) ...
}

// ValidateProfileAgainstServer warns if profile exceeds server (implementation from earlier)
func (c *OPCUAConnection) ValidateProfileAgainstServer(queried *ServerCapabilitiesQueried, profile ServerProfile) {
    // ... (implementation shown in "Profile Override Strategy" section) ...
}

// SkipServerCapabilitiesQuery returns true if querying would fail
func (c *OPCUAConnection) SkipServerCapabilitiesQuery(profile ServerProfile) bool {
    // Siemens PLCs don't expose OperationLimits
    if profile.Name == ProfileS71200 || profile.Name == ProfileS71500 {
        return true
    }

    // Add other skip conditions as needed
    return false
}
```

### Testing Approach

#### Unit Test: Query Handling

```go
// opcua_plugin/core_connection_test.go

var _ = Describe("ServerCapabilities Querying", func() {
    var (
        connection *OPCUAConnection
        mockServer *opcua.Server
    )

    BeforeEach(func() {
        // Setup mock OPC UA server with OperationLimits
        mockServer = SetupMockServer()
        connection = &OPCUAConnection{
            Client: mockServer.Client(),
        }
    })

    Context("when OperationLimits node exists", func() {
        It("should query all limit properties", func() {
            caps, err := connection.QueryServerOperationLimits(context.Background())
            Expect(err).ToNot(HaveOccurred())
            Expect(caps.MaxNodesPerBrowse.Available).To(BeTrue())
            Expect(caps.MaxMonitoredItemsPerCall.Available).To(BeTrue())
        })

        It("should handle zero values gracefully", func() {
            // Mock server returns 0 for MaxNodesPerBrowse
            mockServer.SetOperationLimit("MaxNodesPerBrowse", 0)

            caps, err := connection.QueryServerOperationLimits(context.Background())
            Expect(err).ToNot(HaveOccurred())
            Expect(caps.MaxNodesPerBrowse.Value).To(Equal(uint32(0)))

            // Validation should warn about zero value
            profile := ServerProfile{MaxBatchSize: 100}
            connection.ValidateProfileAgainstServer(caps, profile)
            // Expect log warning about ambiguous zero
        })
    })

    Context("when OperationLimits node missing", func() {
        It("should return error and use profile defaults", func() {
            // Mock server without OperationLimits
            mockServer.RemoveNode(ua.NewNumericNodeID(0, 11704))

            caps, err := connection.QueryServerOperationLimits(context.Background())
            Expect(err).To(HaveOccurred())
            Expect(caps).To(BeNil())

            // Connection should continue with profile defaults
            profile := ServerProfile{MaxBatchSize: 50}
            // Expect no errors, just logged warning
        })
    })

    Context("when individual limit property missing", func() {
        It("should mark that limit as unavailable", func() {
            // Mock server without MaxMonitoredItemsPerCall
            mockServer.RemoveNode(ua.NewNumericNodeID(0, 11574))

            caps, err := connection.QueryServerOperationLimits(context.Background())
            Expect(err).ToNot(HaveOccurred())
            Expect(caps.MaxMonitoredItemsPerCall.Available).To(BeFalse())
            Expect(caps.MaxNodesPerBrowse.Available).To(BeTrue()) // Other limits still work
        })
    })
})
```

#### Integration Test: Real Server Behavior

```go
// opcua_plugin/opcua_integration_test.go

var _ = Describe("ServerCapabilities Integration", func() {
    Context("Prosys Simulation Server", func() {
        It("should expose all OperationLimits", func() {
            Skip("Requires Prosys server running on localhost:53530")

            conn := ConnectToProsysServer()
            defer conn.Close()

            caps, err := conn.QueryServerOperationLimits(context.Background())
            Expect(err).ToNot(HaveOccurred())
            Expect(caps.MaxNodesPerBrowse.Available).To(BeTrue())
            Expect(caps.MaxNodesPerBrowse.Value).To(Equal(uint32(1000)))
            Expect(caps.MaxMonitoredItemsPerCall.Available).To(BeTrue())
            Expect(caps.MaxMonitoredItemsPerCall.Value).To(Equal(uint32(1000)))
        })
    })

    Context("Siemens S7-1200 PLC", func() {
        It("should NOT expose OperationLimits", func() {
            Skip("Requires S7-1200 PLC on network")

            conn := ConnectToS71200()
            defer conn.Close()

            caps, err := conn.QueryServerOperationLimits(context.Background())
            Expect(err).To(HaveOccurred())
            Expect(err.Error()).To(ContainSubstring("Bad_NodeIdUnknown"))

            // Should fallback to profile
            profile := DetectServerProfile(conn.ServerInfo)
            Expect(profile.Name).To(Equal(ProfileS71200))
            Expect(profile.MaxBatchSize).To(Equal(100))
        })
    })
})
```

---

## References

### OPC UA Specification

**Official Documents:**
1. **OPC UA Part 5: Information Model (Version 1.04)**
   - Section 6.3.2: ServerCapabilitiesType definition
   - Section 6.3.11: OperationLimitsType definition
   - URL: https://reference.opcfoundation.org/v104/Core/docs/Part5/6.3.2/

2. **OPC UA Part 4: Services (Version 1.04)**
   - Section 5.10.2.2: Bad_TooManyOperations error code
   - Browse, Read, Write, MonitoredItems service limits
   - URL: https://reference.opcfoundation.org/Core/Part4/

3. **OPC UA Part 6: Mappings (Version 1.04)**
   - Annex A.3: Numeric Node IDs (NodeSet2.xml)
   - Standard node ID assignments for ServerCapabilities
   - URL: https://reference.opcfoundation.org/Core/Part6/v104/docs/A.3

### Vendor Documentation

**Siemens:**
- SIMATIC S7-1200 OPC UA Server Manual (Firmware V4.4+)
  - Max Sessions: 10
  - Max Monitored Items: 1,000 (total)
  - Max Subscriptions: 10

- SIMATIC S7-1500 OPC UA Server Manual (Firmware V3.1+)
  - Max Sessions: 32
  - Max Monitored Items: 10,000 (total)
  - Max Subscriptions: 32

**Kepware:**
- KEPServerEX OPC UA Server Configuration Guide
  - Default Max Sessions: 128 (configurable to 4000)
  - Memory-bound (no hard item limits)

**Inductive Automation:**
- Ignition Gateway OPC UA Server (Eclipse Milo)
  - Based on Eclipse Milo implementation
  - Configurable limits via server settings
  - Default MaxMonitoredItemsPerCall: 1000

**Prosys:**
- Prosys OPC UA Simulation Server Documentation
  - Full OPC UA specification compliance
  - All OperationLimits exposed
  - Default limits: 1000 for most operations

### Open-Source Implementations

**Eclipse Milo:**
- GitHub: https://github.com/eclipse-milo/milo
- File: `OpcUaNamespace.java` - ServerCapabilities implementation
- OperationLimits fully implemented with configurable defaults

**open62541:**
- GitHub: https://github.com/open62541/open62541
- ServerCapabilities configurable at compile time
- OperationLimits may not be enabled in all builds

**gopcua Library:**
- GitHub: https://github.com/gopcua/opcua
- Go package documentation: https://pkg.go.dev/github.com/gopcua/opcua
- Node reading examples: `examples/read/read.go`

### Community Resources

**OPC Foundation Forum:**
- MaxNodesPerRequest limitation discussion
  - URL: https://opcfoundation.org/forum/opc-ua-standard/maxnodesperrequest-limitation/
  - Confirms OperationLimits usage for avoiding "TooManyOperations" errors

**GitHub Issues:**
- python-opcua: OperationLimits not set
  - URL: https://github.com/FreeOpcUa/python-opcua/issues/815
  - Discussion of default OperationLimits values

- node-opcua: MaxNodesPerRead error handling
  - URL: https://github.com/node-opcua/node-opcua/issues/281
  - Real-world examples of limit enforcement

### Related benthos-umh Documents

- `docs/plans/investigation/monitored-item-limits.md` - Investigation of total monitored item limits (server-wide caps)
- `opcua_plugin/server_profiles.go` - Current ServerProfile implementation
- `opcua_plugin/read.go` - Existing ServerCapabilities query (deadband only)

---

## Appendix: Standard Node IDs

### ServerCapabilities Tree (Namespace 0)

| Node Name | Node ID | Data Type | Parent | Description |
|-----------|---------|-----------|--------|-------------|
| **ServerCapabilities** | ns=0;i=2268 | Object | Server (i=2253) | Root capabilities object |
| ServerProfileArray | ns=0;i=2269 | String[] | i=2268 | Supported OPC UA profiles |
| LocaleIdArray | ns=0;i=2271 | String[] | i=2268 | Supported locale IDs |
| MinSupportedSampleRate | ns=0;i=2272 | Double | i=2268 | Minimum sampling rate (ms) |
| MaxBrowseContinuationPoints | ns=0;i=2735 | UInt16 | i=2268 | Max parallel browse operations |
| MaxArrayLength | ns=0;i=11512 | UInt32 | i=2268 | Max array size in variables |
| MaxStringLength | ns=0;i=11513 | UInt32 | i=2268 | Max string length (bytes) |
| MaxByteStringLength | ns=0;i=12910 | UInt32 | i=2268 | Max ByteString length |
| **OperationLimits** | ns=0;i=11704 | Object | i=2268 | Operation limits folder |

### OperationLimits Properties (Namespace 0)

| Property Name | Node ID | Data Type | Description |
|---------------|---------|-----------|-------------|
| **MaxNodesPerRead** | ns=0;i=11705 | UInt32 | Max nodes in Read service array |
| **MaxNodesPerHistoryReadData** | ns=0;i=11706 | UInt32 | Max nodes in HistoryRead (data) |
| **MaxNodesPerHistoryReadEvents** | ns=0;i=11707 | UInt32 | Max nodes in HistoryRead (events) |
| **MaxNodesPerWrite** | ns=0;i=11708 | UInt32 | Max nodes in Write service array |
| **MaxNodesPerHistoryUpdateData** | ns=0;i=11709 | UInt32 | Max nodes in HistoryUpdate (data) |
| **MaxNodesPerHistoryUpdateEvents** | ns=0;i=11710 | UInt32 | Max nodes in HistoryUpdate (events) |
| **MaxNodesPerMethodCall** | ns=0;i=11711 | UInt32 | Max methods in Call service array |
| **MaxNodesPerBrowse** | ns=0;i=11712 | UInt32 | Max nodes in Browse/BrowseNext |
| **MaxNodesPerRegisterNodes** | ns=0;i=11713 | UInt32 | Max nodes in RegisterNodes |
| **MaxNodesPerTranslateBrowsePathsToNodeIds** | ns=0;i=11714 | UInt32 | Max paths in TranslateBrowsePaths |
| **MaxNodesPerNodeManagement** | ns=0;i=11715 | UInt32 | Max nodes in node management services |
| **MaxMonitoredItemsPerCall** | ns=0;i=11574 | UInt32 | Max items in CreateMonitoredItems, ModifyMonitoredItems, SetMonitoringMode, SetTriggering |

**Note on MaxMonitoredItemsPerCall:** The node ID `i=11574` is outside the contiguous OperationLimits range (i=11704-11715). This is expected according to the OPC UA specification.

### Query Example NodeIDs

```go
// Core ServerCapabilities
var (
    NodeID_ServerCapabilities             = ua.NewNumericNodeID(0, 2268)
    NodeID_MinSupportedSampleRate         = ua.NewNumericNodeID(0, 2272)
    NodeID_MaxBrowseContinuationPoints    = ua.NewNumericNodeID(0, 2735)

    // OperationLimits folder
    NodeID_OperationLimits                = ua.NewNumericNodeID(0, 11704)

    // Individual limits (most relevant for benthos-umh)
    NodeID_MaxNodesPerRead                = ua.NewNumericNodeID(0, 11705)
    NodeID_MaxNodesPerBrowse              = ua.NewNumericNodeID(0, 11712)
    NodeID_MaxMonitoredItemsPerCall       = ua.NewNumericNodeID(0, 11574)
    NodeID_MaxNodesPerWrite               = ua.NewNumericNodeID(0, 11708)
)
```

---

## Changelog

**2025-11-05 - Initial Investigation Complete**
- Researched OPC UA Part 5 (Information Model) and Part 4 (Services) specifications
- Identified 12 queryable operation limits under OperationLimits node
- Analyzed vendor support: Ignition (full), Kepware (partial), Siemens PLCs (none)
- Documented gopcua library query patterns and error handling
- **Recommendation:** Use profile-based limits as primary source, query for validation only
- Estimated integration effort: Phase 1 (2-3 hours), Phase 2 (4-6 hours), Phase 3 (6-8 hours)
