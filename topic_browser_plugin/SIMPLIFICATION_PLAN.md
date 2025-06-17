# Topic Browser Simplification Plan

## Background & Rationale

During the last design session the team reached consensus that several "clever" micro-optimisations, originally added to the **topic_browser** prototype, create **far more moving parts than real-world benefit**.

Key quotes from the call (paraphrased):
* *"I'm mentally struggling … performance code is sprinkled everywhere, nobody sees the real business logic any more."*
* *"We're only optimising a log-file size, not network traffic—just crank the log-rotate limit if it ever hurts."*
* *"Each optimisation forces duplicate handling in FSM **and** Communicator; the chance of state-skew bugs explodes."*

## Implementation Plan

### **Phase 1: Remove LRU/Delta Logic - Always Send Full uns_map** 
*Duration: 1-2 days*

#### **1.1 Remove shouldReportTopic Logic** ⚠️
- [x] **File**: `metadata.go`
- [x] **Action**: Delete `shouldReportTopic` function entirely
- [x] **Impact**: All topics will always be included in uns_map (no more cache comparison)
- [x] **Commit**: "Remove shouldReportTopic logic - no more conditional topic emission"
- [x] **Test**: Run basic tests to ensure topics are always emitted

#### **1.2 Remove LRU Cache Infrastructure** ⚠️
- [ ] **File**: `metadata.go` 
- [ ] **Action**: Remove LRU import, cache initialization, and all cache operations
- [ ] **File**: `topic_browser_plugin.go`
- [ ] **Action**: Remove `topicMetadataCache` and `topicMetadataCacheMutex` fields from struct
- [ ] **Commit**: "Remove LRU cache infrastructure"
- [ ] **Test**: Verify compilation and basic functionality
- [ ] **Note**: Cache still used for cumulative metadata - may keep for now

#### **1.3 Simplify updateTopicMetadata** ⚠️ 
- [x] **File**: `metadata.go`
- [x] **Action**: Remove cache lookup/comparison logic
- [x] **New Logic**: Always add all topics to uns_map bundle
- [x] **Result**: Function becomes simple loop over topics → bundle
- [x] **Commit**: "Simplify updateTopicMetadata - always emit all topics"
- [x] **Test**: Verify topic data is correctly included in bundles

#### **1.4 Update flushBufferAndACK** ⚠️
- [x] **File**: `buffer.go` 
- [x] **Action**: Remove conditional full tree emission
- [x] **New Logic**: **ALWAYS** emit complete `fullTopicMap` in every bundle
- [x] **Rationale**: No more "changed topics" concept - every emission is full state
- [x] **Commit**: "Always emit full topic map in every bundle"
- [x] **Test**: Verify complete topic state is always present

### **Phase 2: Remove Size Check - Always Compress** 
*Duration: 1 day*

#### **2.1 Simplify BundleToProtobufBytesWithCompression** ⚠️
- [x] **File**: `serialization.go` (moved from `proto.go`)
- [x] **Action**: Remove 1024-byte size check 
- [x] **New Logic**: Always apply LZ4 level 0 compression
- [x] **Result**: Single code path - no conditional branches
- [x] **Commit**: "Remove 1024-byte size check - always compress with LZ4"
- [x] **Test**: Verify all outputs are LZ4 compressed

#### **2.2 Rename Compression Functions** 
- [x] **File**: `serialization.go`
- [x] **Action**: Rename to `BundleToProtobufBytes` (remove "WithCompression" suffix)
- [x] **Rationale**: Compression is no longer conditional
- [x] **Commit**: "Rename compression functions - compression is always enabled"
- [x] **Test**: Verify function calls are updated throughout codebase

#### **2.3 Update Decompression Function** ⚠️
- [x] **File**: `serialization.go`
- [x] **Action**: Remove LZ4 magic number check in `ProtobufBytesToBundleWithCompression`
- [x] **New Logic**: Always expect LZ4 format (no fallback to plain protobuf)
- [x] **Commit**: "Remove LZ4 magic number check - always expect compressed input"
- [x] **Test**: Verify decompression works correctly

### **Phase 3: Update Documentation & Comments**
*Duration: 0.5 days*

#### **3.1 Update Function Documentation**
- [ ] **Files**: All modified files
- [ ] **Action**: Remove references to "conditional", "1024 bytes", "LRU", "deltas"
- [ ] **New Descriptions**: "Always full tree", "Always compressed"
- [ ] **Commit**: "Update documentation to reflect simplified behavior"
- [ ] **Test**: Review docs for accuracy

#### **3.2 Update Tokenizer Constants Documentation** 
- [ ] **File**: `serialization.go`, `topic_browser_plugin.go`
- [ ] **Action**: Update STARTSTARTSTART/ENDENDEND comments
- [ ] **New Text**: "FSM always decompresses LZ4 frames"
- [ ] **Commit**: "Update tokenizer documentation for always-compressed format"
- [ ] **Test**: Verify comments are accurate

#### **3.3 Add Simplification Rationale**
- [ ] **File**: `topic_browser_plugin.go` (package documentation)
- [ ] **Action**: Add the provided background explanation as package comment
- [ ] **Content**: Include quotes about complexity vs. benefit trade-offs
- [ ] **Commit**: "Add simplification rationale to package documentation"
- [ ] **Test**: Review package docs

### **Phase 4: Update Tests**
*Duration: 1 day*

#### **4.1 Remove Cache-Related Tests**
- [ ] **File**: `topic_browser_plugin_test.go`
- [ ] **Action**: Remove tests that verify LRU caching behavior
- [ ] **Examples**: Cache hit/miss scenarios, metadata comparison tests
- [ ] **Commit**: "Remove LRU cache-related tests"
- [ ] **Test**: Verify remaining tests pass

#### **4.2 Update Compression Tests**
- [ ] **File**: Any compression-related tests
- [ ] **Action**: Remove small payload tests that expect no compression
- [ ] **New Logic**: All test outputs should be LZ4-compressed
- [ ] **Commit**: "Update tests to expect always-compressed output"
- [ ] **Test**: Verify all compression tests pass

#### **4.3 Update Bundle Content Tests**
- [ ] **Action**: Expect full uns_map in every test output
- [ ] **Verification**: No empty uns_map scenarios (except truly empty inputs)
- [ ] **Commit**: "Update tests to expect full topic map in every bundle"
- [ ] **Test**: Verify bundle content tests pass

### **Phase 5: Update Configuration & Metrics**
*Duration: 0.5 days*

#### **5.1 Remove LRU Configuration**
- [ ] **File**: `topic_browser_plugin.go`
- [ ] **Action**: Remove `lru_size` configuration parameter
- [ ] **Impact**: No breaking change (ignored if present)
- [ ] **Commit**: "Remove lru_size configuration parameter"
- [ ] **Test**: Verify configuration parsing works

#### **5.2 Update Metrics**
- [ ] **File**: `topic_browser_plugin.go`
- [ ] **Action**: Remove cache-related metrics if any exist
- [ ] **Add**: Compression ratio metrics (optional)
- [ ] **Commit**: "Update metrics to reflect simplified behavior"
- [ ] **Test**: Verify metrics are correctly reported

## Implementation Order & Risk Assessment

**Recommended Sequence:**
1. **Phase 2** (Compression) - Lower risk, isolated changes
2. **Phase 1** (LRU removal) - Higher risk, affects core logic
3. **Phase 3** (Documentation) - No risk
4. **Phase 4** (Tests) - Medium risk, needs careful validation
5. **Phase 5** (Config) - Low risk

**Key Risk Mitigation:**
- Test thoroughly with small datasets first
- Verify downstream FSM can handle always-compressed format
- Ensure bundle sizes remain reasonable with always-full uns_map
- Test edge cases (empty inputs, single messages, large batches)

**Expected Benefits After Implementation:**
- ~30% less code complexity (remove cache, conditional logic)
- Single compression code path (easier debugging)
- Stateless FSM processing (always has complete topic tree)
- Predictable output format (always LZ4, always full state)
- Easier testing and observability

**Breaking Changes:**
- Downstream consumers must always decompress LZ4
- Bundle sizes will increase (more topic data per emission)
- FSM must handle larger, more frequent topic updates

## Progress Tracking

- [x] Phase 1 Complete
- [x] Phase 2 Complete  
- [ ] Phase 3 Complete
- [ ] Phase 4 Complete
- [ ] Phase 5 Complete
- [ ] Final Integration Testing
- [ ] Documentation Review 