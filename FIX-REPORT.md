# ENG-5553 step 2 — the fix

Branch: `eng-5553-opc-ua-values-attributed-to-the-wrong-tag-after-a` (base commit `756c2d3`).
All commands run from `/workspace` with `GOTOOLCHAIN=auto` (go.mod wants Go 1.27.4, container has 1.27.1).

## Headline: every target row is green

All of `TestB1`, `TestB2`, `TestO1`, `TestO3`, `TestO4` pass — 5/5 rows, 18/18 subtests. The frozen
test files (`opcua_plugin/read_client_handle_attribution_test.go`,
`opcua_plugin/subscription_fake_test.go`) are untouched (`git status` shows only `read.go`,
`read_discover.go` modified and `subscription_handle_registry.go` added).

## What changed and why

The defect: `MonitorBatched` assigned `ClientHandle: uint32(batchRange.Start + pos)` — a position in
whatever slice it was handed — and the `StatusBadFilterNotAllowed` retry recurses with
`nodes[failedNodeIndex:]`, so positions restart at 0 while earlier items of the same subscription are
still live under those handles. `ReadBatchSubscribe` resolved notifications with
`g.NodeList[handleID]` against the full list, so values published under the wrong node with no error,
and the off-by-one bounds guard (`uint32(len(g.NodeList)) >= handleID`) panicked on a handle equal to
the list length.

The fix is the agreed design: a subscription-owned, synchronized `ClientHandle → NodeDef` registry.
Three files, ~30 lines of production change plus one new file:

1. **`opcua_plugin/subscription_handle_registry.go` (new)** — `subscriptionHandleRegistry`:
   an `RWMutex`-guarded `map[uint32]NodeDef` plus a monotonic `nextHandle` counter. The zero value is
   ready to use. `allocate(node)` registers the node under a fresh handle and returns it;
   `lookup(handle)` resolves a notification; `reset()` drops the map but **never zeroes the
   counter** — a handle from a previous subscription generation must come back *unknown* against the
   next one, never valid-but-pointing-at-a-different-node (row O4).

2. **`opcua_plugin/read_discover.go`** —
   - `MonitorBatched` now builds each request with `ClientHandle: g.handleRegistry.allocate(nodeDef)`
     instead of the batch position. Allocation happens as the request is built, i.e. *before* it is
     sent, so notifications that arrive before the CreateMonitoredItems response (row B2, OPC
     10000-4 §5.13.2.1) already resolve. Because the retry re-slices the node list but the counter
     keeps counting, the retried suffix gets fresh handles and no handle ever identifies two live
     MonitoredItems (rows B1/O1). The loop variable `pos` existed only for the positional handle and
     was removed (`for _, nodeDef := range batch`).
   - `BrowseAndSubscribeIfNeeded` calls `g.handleRegistry.reset()` immediately after
     `g.Client.Subscribe(...)` succeeds — the point where a new subscription generation is created.

3. **`opcua_plugin/read.go`** —
   - New field `handleRegistry subscriptionHandleRegistry` on `OPCUAInput`. (Inserting a doc comment
     splits gofmt's alignment group, so gofmt re-aligned the field names of that struct block — that
     is the whole of the `Subscription`/`ServerInfo`/... re-alignment in the diff.)
   - `ReadBatchSubscribe` resolves `item.ClientHandle` through the registry. An unknown handle is
     dropped with an `Errorf` diagnostic ("Dropping notification with unknown ClientHandle %d ...")
     and never attributed to an arbitrary node, never allowed to panic (rows O3/O4). The
     `uint32(len(g.NodeList)) >= handleID` bounds guard is deleted entirely, per the design — the
     off-by-one is gone because the positional lookup is gone, not because it was corrected.
   - `unsubscribeAndResetHeartbeat` calls `g.handleRegistry.reset()` after `Subscription.Cancel` —
     the teardown point where the subscription generation ends.

Concurrency: allocation happens on the browse goroutine (building later batches / the retry) while
resolution happens on the benthos input goroutine draining `SubNotifyChan`; the `RWMutex` guards
both. `reset` vs `lookup` is equally guarded, so a notification being resolved concurrently with a
teardown/reset sees either the old map or "unknown" — never a torn read.

Not changed, per the boundaries: no migration to `gopcua/monitor`, no subscription-stack change, no
dependency change, no filter-fallback policy change.

## Verification

### Target rows — before (at `756c2d3`, unmodified) and after

Command: `GOTOOLCHAIN=auto go test -count=1 ./opcua_plugin/ -run 'TestB1|TestB2|TestO1|TestO3|TestO4' -v`

Before (RED — all five rows fail; representative top-level lines, failures match
`/workspace/TEST-PASS-REPORT.md` in every detail):

```
--- FAIL: TestB1ClientHandleIdentifiesExactlyOneMonitoredItem (6.01s)
--- FAIL: TestB2EarlyNotificationsAreAttributedCorrectly (0.02s)
--- FAIL: TestO1FilterRejectionFallbackPreservesNodeIdentity (10.04s)
--- FAIL: TestO3UnknownOrOutOfRangeHandleIsDroppedWithDiagnosticNeverFatal (2.01s)
--- FAIL: TestO4HandlesFromOneGenerationDoNotResolveAfterReconnect (2.01s)
FAIL	github.com/united-manufacturing-hub/benthos-umh/opcua_plugin	20.144s
```

After (GREEN — full subtest list, 18/18):

```
--- PASS: TestB1ClientHandleIdentifiesExactlyOneMonitoredItem (6.01s)
    --- PASS: .../accepted_filter_path_assigns_one_handle_per_item (1.00s)
    --- PASS: .../filter_fallback_at_a_nonzero_index_reuses_handles_for_different_nodes (1.00s)
    --- PASS: .../filter_fallback_in_a_later_batch_reuses_handles_for_different_nodes (2.00s)
    --- PASS: .../filter_fallback_at_index_zero_against_a_filterless_server_keeps_handles_unique (1.00s)
    --- PASS: .../rejection_of_only_the_first_node's_filter_re-subscribes_the_survivors_under_duplicate_handles (1.00s)
--- PASS: TestB2EarlyNotificationsAreAttributedCorrectly (0.01s)
    --- PASS: .../accepted_filter_path (0.01s)
    --- PASS: .../filter_fallback_at_a_nonzero_index (0.01s)
--- PASS: TestO1FilterRejectionFallbackPreservesNodeIdentity (10.03s)
    --- PASS: .../rejection_at_a_nonzero_index (1.00s)
    --- PASS: .../rejection_at_index_zero_against_a_filterless_server (1.00s)
    --- PASS: .../rejection_in_a_later_batch (2.01s)
    --- PASS: .../repeated_fallback_across_two_subscription_generations (2.00s)
    --- PASS: .../accepted_filter_path_(no_fallback) (1.00s)
    --- PASS: .../no-filter_profile_path (1.00s)
    --- PASS: .../heartbeat_node_keeps_its_identity_through_the_fallback (1.00s)
    --- PASS: .../heartbeat_node_keeps_its_identity_on_the_accepted_filter_path (1.00s)
--- PASS: TestO3UnknownOrOutOfRangeHandleIsDroppedWithDiagnosticNeverFatal (2.01s)
    --- PASS: .../handle_equal_to_the_node_list_length_must_not_be_fatal (1.00s)
    --- PASS: .../handle_beyond_the_node_list_length_is_dropped_but_must_be_diagnosed (1.00s)
    --- PASS: .../unknown_in-range_handle_the_server_has_no_item_for_must_be_dropped (0.00s)
--- PASS: TestO4HandlesFromOneGenerationDoNotResolveAfterReconnect (2.01s)
ok  	github.com/united-manufacturing-hub/benthos-umh/opcua_plugin	20.091s
```

The scenarios that already passed at `756c2d3` (B1 accepted-filter path, B1 index-zero rejection,
B2 accepted path, O1 index-zero/accepted/no-filter/heartbeat-on-accepted) still pass: the
registry allocates exactly one handle per accepted item on those paths too, so attribution is
unchanged where it was already correct.

### Full-package before/after comparison

Command: `GOTOOLCHAIN=auto go test -count=1 ./opcua_plugin/`

Before (at `756c2d3`):

```
--- FAIL: TestB1ClientHandleIdentifiesExactlyOneMonitoredItem (6.02s)
--- FAIL: TestB2EarlyNotificationsAreAttributedCorrectly (0.02s)
--- FAIL: TestO1FilterRejectionFallbackPreservesNodeIdentity (10.04s)
--- FAIL: TestO3UnknownOrOutOfRangeHandleIsDroppedWithDiagnosticNeverFatal (2.01s)
--- FAIL: TestO4HandlesFromOneGenerationDoNotResolveAfterReconnect (2.01s)
FAIL	github.com/united-manufacturing-hub/benthos-umh/opcua_plugin	21.098s
```

After:

```
ok  	github.com/united-manufacturing-hub/benthos-umh/opcua_plugin	20.823s
```

Also run with the older unit tests enabled (`TEST_OPCUA_UNIT=1 GOTOOLCHAIN=auto go test -count=1
./opcua_plugin/`): before, the same five rows fail (exit 1, ginkgo suite itself 16 Passed / 0
Failed); after, `ok ... 26.072s` (exit 0).

**Pre-existing tests that fail: none, in either direction.** The only failing tests before the fix
were the five new target rows; every other test in the package — including the env-gated ginkgo
suite — passed at `756c2d3` and still passes after the fix. In particular, the older tests around
this area that the step-1 briefing predicted might mirror the deleted positional logic
(`read_discover_recursive_retry_bug_test.go`, `read_discover_capability_filter_test.go`,
`read_discover_trial_logic_test.go`, `read_discover_deadband_test.go`) do not exercise
`ClientHandle` through production at all — they re-implement the batching/decision logic in local
closures — so they were green before and stay green. Nothing had to be left failing, and no older
test required removal in this step.

### go vet

`GOTOOLCHAIN=auto go vet ./opcua_plugin/` reports exactly one line, both before and after my change:

```
opcua_plugin/core_browse_global_pool_test.go:206:7: the cancel function returned by context.WithTimeout should be called, not discarded, to avoid a context leak
```

That is the pre-existing complaint already recorded in `TEST-PASS-REPORT.md`, in a file I did not
touch. All files I added or edited (`read.go`, `read_discover.go`,
`subscription_handle_registry.go`) are vet-clean. `gofmt -l opcua_plugin/` is clean, and
`GOTOOLCHAIN=auto go build ./...` succeeds for the whole repository.

### Race detector — NOT run, cannot run here

`go test -race` requires cgo and the container has no C compiler, so the race run has to happen on
the host. Attempted, for the record:

```
$ CGO_ENABLED=1 GOTOOLCHAIN=auto go test -race -count=1 ./opcua_plugin/ -run 'TestO4'
# runtime/cgo
cgo: C compiler "gcc" not found: exec: "gcc": executable file not found in $PATH
FAIL	github.com/united-manufacturing-hub/benthos-umh/opcua_plugin [build failed]
```

I did not run any race-detector verification and claim nothing from it. The concurrency shape the
registry must survive (browse goroutine allocating in later batches / the retry while the input
goroutine resolves notifications, and reset vs lookup around teardown) is confined to
`subscriptionHandleRegistry`'s three methods, all of which hold `mu` for every read and write of
`handles`/`nextHandle`.

## Things I noticed but deliberately did not touch

- **The ENG-5554 side is still open, as designed.** B1's "rejection of only the first node's filter"
  scenario now passes because the retry allocates *fresh* handles, but the server still ends up with
  the surviving nodes monitored twice (the first request created them per-item before the retry
  re-created them). Duplicate MonitoredItems per node remain; what this fix guarantees is that each
  duplicate carries its own handle and each handle resolves to the right node. De-duplicating the
  retry (e.g. deleting/ignoring already-created items) is a separate decision with its own review.
- **Handles allocated for rejected items stay in the registry.** If a request item is rejected
  (filter or otherwise), its handle was already allocated; the retry allocates a new one. A
  notification for the rejected handle — which a conformant server will not send, since the item
  was never created — would resolve to the same, correct node. Left as is; freeing handles would add
  bookkeeping for a case the protocol says cannot happen.
- **Counter wrap-around.** `nextHandle` is a `uint32`; after 2^32 allocations it would wrap and could
  collide with a still-live handle within one generation. That needs four billion MonitoredItem
  creations without a reconnect; I judged a guard not worth the code, but it is worth knowing the
  registry does not defend against it.
- **`Connect()` still recreates `SubNotifyChan` per connection** (`read.go`), which today
  quarantines stale generation-1 publishes on the old channel. The O4 tests deliberately share the
  channel across generations to model the hazard if that ever changes; the fix makes stale handles
  unresolvable regardless of which channel they arrive on, but I did not change the channel
  lifecycle.
- **`ReadBatchSubscribe`'s `g.NodeList == nil` early-error check** is kept even though the
  positional lookup it protected is gone: it still catches the "never browsed" case cheaply.
- **The 1s post-batch sleep in `MonitorBatched`** still dominates the suite runtime (~20s).
  Untouched.
- **The stale comment referenced by the old code** ("the handle id, which is the position in
  g.Nodelist") was replaced by the registry explanation; no other comments were reworded, and no
  reformatting happened beyond the gofmt-mandated struct-field realignment described above.
- **`metrics.go` `RecordSubscriptionFailure`** and the non-filter rejection path (abort + `Close`)
  are unchanged; only the filter-retry path's handle arithmetic changed.

## Frozen-file integrity

`opcua_plugin/read_client_handle_attribution_test.go` and
`opcua_plugin/subscription_fake_test.go` were not modified. `git status` at end of work:

```
 M opcua_plugin/read.go
 M opcua_plugin/read_discover.go
?? opcua_plugin/subscription_handle_registry.go
```
