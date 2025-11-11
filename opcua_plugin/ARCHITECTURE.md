# OPC UA Plugin Architecture

## Two Code Paths: Production vs. Legacy UI

The OPC UA plugin has **two distinct browse implementations** that are **mutually exclusive**:

### Production Path (Subscribe-based Discovery)
**Used by:** All production benthos-umh flows (DFC configs)
**Entry point:** `read_discover.go::discoverNodes()`
**Worker system:** browse() internal workers (core_browse.go + core_browse_workers.go)
**Controlled by:** ServerProfile.MaxWorkers/MinWorkers
**Purpose:** Discover nodes → Subscribe to changes → Stream to UNS

**Flow:**
```
OPCUAInput.Connect()
  ↓
BrowseAndSubscribeIfNeeded()
  ↓
discoverNodes()  ← Production entry point
  ↓
browse() with internal worker pool
  ↓
MonitorBatched() ← Subscribe phase
  ↓
Read() → Stream messages to Kafka
```

**Key characteristics:**
- Uses ServerProfile-tuned worker pool (5-60 workers depending on server)
- Optimized for bulk subscription setup
- Drains browse results via nodeChan consumer goroutine
- Dynamic worker scaling based on server response time

### Legacy UI Path (Tree Browser)
**Used by:** ManagementConsole v1 BrowseOPCUA UI only
**Entry point:** `core_browse_frontend.go::GetNodeTree()`
**Worker system:** Same browse() internal workers (NOT GlobalWorkerPool)
**Controlled by:** ServerProfile.MaxWorkers (uses Auto profile = 5 workers max)
**Purpose:** Build hierarchical tree structure for UI display

**Flow:**
```
ManagementConsole UI
  ↓
HTTP API request
  ↓
GetNodeTree() ← Legacy UI entry point
  ↓
browse() with internal worker pool (Auto profile)
  ↓
constructNodeHierarchy()
  ↓
Return tree JSON to UI
```

**Key characteristics:**
- Always uses Auto profile (defensive defaults)
- Builds parent-child relationships for tree display
- Returns complete tree structure, not flat node list
- No subscription (one-time browse operation)

## Worker Pool Architecture

### browse() Internal Workers (Production)

**Location:** core_browse.go (browse function) + core_browse_workers.go (ServerMetrics)

**How it works:**
1. browse() creates taskChan and spawns worker goroutines
2. Workers pull NodeTasks from taskChan
3. Each worker browses node children and queues new tasks
4. ServerMetrics adjusts worker count every 5 seconds based on response time
5. Workers exit when taskChan is closed (all tasks complete)

**Tuning:**
- ServerProfile.MaxWorkers sets upper bound (e.g., S7-1200 = 10, Prosys = 60)
- ServerProfile.MinWorkers sets lower bound (e.g., S7-1200 = 3, Prosys = 5)
- Dynamic scaling between min/max based on avg response time vs 250ms target

**Why internal workers:**
- Each browse() call has isolated worker pool (no shared state between operations)
- Allows profile-specific tuning per server type
- Workers automatically clean up when browse completes

### GlobalWorkerPool (NOT USED)

**Status:** Legacy pattern, exists in codebase but NOT used by production or UI

**Why it exists:** Historical artifact from earlier architecture exploration

**Important:** Do NOT conflate GlobalWorkerPool with browse() internal workers. They are completely separate systems. GlobalWorkerPool has no impact on production behavior.

## ServerProfile Controls

ServerProfile settings control **both code paths** (production and legacy UI):

| Setting | Affects | Browse Phase | Subscribe Phase |
|---------|---------|--------------|-----------------|
| MaxWorkers | browse() worker pool | ✅ Yes | ❌ No |
| MinWorkers | browse() worker pool | ✅ Yes | ❌ No |
| MaxBatchSize | MonitorBatched() | ❌ No | ✅ Yes |
| MaxMonitoredItems | Validation | ❌ No | ✅ Yes |

**Key insight:** Production path has TWO phases:
1. **Browse phase:** Uses MaxWorkers/MinWorkers to control parallel browsing
2. **Subscribe phase:** Uses MaxBatchSize to batch CreateMonitoredItems calls

Legacy UI path only has Browse phase (no subscription).

## When to Use Each Path

### Use discoverNodes() (Production Path)
- All DFC configurations
- Production data streaming
- Automated node discovery + subscription
- When ServerProfile tuning matters

### Use GetNodeTree() (Legacy UI Path)
- ONLY from ManagementConsole BrowseOPCUA UI
- One-time tree structure generation
- Manual node exploration by users
- When hierarchical parent-child display needed

### Use browse() directly
- NEVER in production code (use discoverNodes() instead)
- Only in tests via Browse() public wrapper
- Only in GetNodeTree() internal implementation

## Common Misconceptions

### ❌ Wrong: "GlobalWorkerPool should control browse() workers"
**Reality:** browse() uses internal worker pool, not GlobalWorkerPool. GlobalWorkerPool is unused legacy code.

### ❌ Wrong: "GetNodeTree and discoverNodes should share workers"
**Reality:** They use the same browse() implementation with different profiles. No shared worker pool needed.

### ❌ Wrong: "MaxWorkers affects Subscribe phase"
**Reality:** MaxWorkers only affects Browse phase. Subscribe phase uses MaxBatchSize.

### ❌ Wrong: "Frontend code needs different worker implementation"
**Reality:** Frontend uses same browse() workers, just with Auto profile (5 workers max).

## Adding New Features

### If modifying Browse phase:
- Edit core_browse.go (browse() logic) or core_browse_workers.go (ServerMetrics)
- Test with both discoverNodes() and GetNodeTree() code paths
- Verify ServerProfile.MaxWorkers/MinWorkers honored
- Check Browse() test wrapper still works

### If modifying Subscribe phase:
- Edit read_discover.go (MonitorBatched() logic)
- Only affects production path (GetNodeTree doesn't subscribe)
- Verify ServerProfile.MaxBatchSize honored
- Test with large node counts (>1000 nodes)

### If adding new ServerProfile:
- Define in server_profiles.go
- Set MaxWorkers (Browse), MinWorkers (Browse), MaxBatchSize (Subscribe)
- Add detection logic in DetectServerProfile()
- Test with both code paths
