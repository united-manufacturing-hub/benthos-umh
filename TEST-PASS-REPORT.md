# ENG-5553 step 1 — subscription seam and ClientHandle attribution tests

Branch: `eng-5553-opc-ua-values-attributed-to-the-wrong-tag-after-a` (cut from `staging` @ `1f78f71`).

Delivered:

| File | Kind | Content |
|---|---|---|
| `opcua_plugin/subscription_api.go` | production (seam) | `SubscriptionAPI` interface with exactly the two methods production calls on the subscription (`Monitor`, `Cancel`), plus the compile-time assertion `var _ SubscriptionAPI = (*opcua.Subscription)(nil)`. |
| `opcua_plugin/read.go` | production (seam, 1 field) | `OPCUAInput.Subscription` retyped from `*opcua.Subscription` to `SubscriptionAPI`. This is the **only** production change; every use site compiled unchanged. |
| `opcua_plugin/subscription_fake_test.go` | test | `fakeSubscription` — plays the OPC UA server's side of CreateMonitoredItems/Publish. Records, for every item it accepts, the `(ClientHandle, NodeID)` pair *as the server sees it*. |
| `opcua_plugin/read_client_handle_attribution_test.go` | test | The row tests for B1, B2, O1, O3, O4. |

## Method (why these tests can move when production changes)

Every test calls the real production functions — `MonitorBatched` (`read_discover.go`), `ReadBatchSubscribe` (`read.go`), `unsubscribeAndResetHeartbeat` (`read.go`) — against a fake that behaves like an OPC UA server per OPC 10000-4: per-item independent processing of CreateMonitoredItems, no enforcement of client-handle uniqueness (that is the *client's* obligation per §7.21), Cancel discarding all items, and notifications that may be queued before the response returns (§5.13.2.1).

The assertion is always the same shape: *a notification carrying handle h must be published under exactly the node the server actually monitors under h* — same node ID, browse name, tag group/tag name/tag path, and payload. Expected handles are never computed in the tests; they are read from the server's record of what it accepted. The earlier tests around this area (`read_discover_recursive_retry_bug_test.go`, and the reproductions on `repro/eng-5553-5554-clienthandle`) reimplemented the batching arithmetic in a local closure and asserted against their own reimplementation; these tests do not.

All commands below run from `/workspace`. Note: `go.mod` wants Go 1.27.4 and the container has 1.27.1, so every command needs `GOTOOLCHAIN=auto`. The suite takes ~20s because production's `MonitorBatched` sleeps 1s after every batch (`read_discover.go`).

---

## B1 — a ClientHandle identifies exactly one MonitoredItem within a subscription — **RED**

Source: OPC 10000-4 §7.21 (`clientHandle`: "Client-supplied id of the MonitoredItem. This id is used in Notifications generated").

Command:

```
GOTOOLCHAIN=auto go test ./opcua_plugin/ -run 'TestB1ClientHandleIdentifiesExactlyOneMonitoredItem' -count=1 -v
```

Real output (subtest statuses; failures shown in full):

```
    --- PASS: TestB1ClientHandleIdentifiesExactlyOneMonitoredItem/accepted_filter_path_assigns_one_handle_per_item (1.00s)
    --- FAIL: TestB1ClientHandleIdentifiesExactlyOneMonitoredItem/filter_fallback_at_a_nonzero_index_reuses_handles_for_different_nodes (1.00s)
        read_client_handle_attribution_test.go:557: B1 (OPC 10000-4 §7.21 ...) [filter fallback at a nonzero index]: 2 handles identify more than one MonitoredItem:
            handle 0 identifies 2 live MonitoredItems: item 1 (handle 0 -> node ns=2;i=1000); item 5 (handle 0 -> node ns=2;i=1002)
            handle 1 identifies 2 live MonitoredItems: item 2 (handle 1 -> node ns=2;i=1001); item 6 (handle 1 -> node ns=2;i=1003)
    --- FAIL: TestB1ClientHandleIdentifiesExactlyOneMonitoredItem/filter_fallback_in_a_later_batch_reuses_handles_for_different_nodes (2.00s)
        read_client_handle_attribution_test.go:574: B1 (OPC 10000-4 §7.21 ...) [filter fallback in a later batch]: 3 handles identify more than one MonitoredItem:
            handle 0 identifies 2 live MonitoredItems: item 1 (handle 0 -> node ns=2;i=1000); item 10 (handle 0 -> node ns=2;i=1007)
            handle 1 identifies 2 live MonitoredItems: item 2 (handle 1 -> node ns=2;i=1001); item 11 (handle 1 -> node ns=2;i=1008)
            handle 2 identifies 2 live MonitoredItems: item 3 (handle 2 -> node ns=2;i=1002); item 12 (handle 2 -> node ns=2;i=1009)
    --- PASS: TestB1ClientHandleIdentifiesExactlyOneMonitoredItem/filter_fallback_at_index_zero_against_a_filterless_server_keeps_handles_unique (1.00s)
    --- FAIL: TestB1ClientHandleIdentifiesExactlyOneMonitoredItem/rejection_of_only_the_first_node's_filter_re-subscribes_the_survivors_under_duplicate_handles (1.00s)
        read_client_handle_attribution_test.go:608: B1 (OPC 10000-4 §7.21 ...) [rejection of only the first node's filter]: 4 handles identify more than one MonitoredItem:
            handle 1 identifies 2 live MonitoredItems: item 1 (handle 1 -> node ns=2;i=1001); item 6 (handle 1 -> node ns=2;i=1001)
            handle 2 identifies 2 live MonitoredItems: item 2 (handle 2 -> node ns=2;i=1002); item 7 (handle 2 -> node ns=2;i=1002)
            handle 3 identifies 2 live MonitoredItems: item 3 (handle 3 -> node ns=2;i=1003); item 8 (handle 3 -> node ns=2;i=1003)
            handle 4 identifies 2 live MonitoredItems: item 4 (handle 4 -> node ns=2;i=1004); item 9 (handle 4 -> node ns=2;i=1004)
--- FAIL: TestB1ClientHandleIdentifiesExactlyOneMonitoredItem (6.01s)
FAIL	github.com/united-manufacturing-hub/benthos-umh/opcua_plugin	6.039s FAIL
```

**Reasoning.** RED, genuinely broken. `MonitorBatched` assigns `ClientHandle: uint32(batchRange.Start + pos)` — a position in the slice it was handed (`read_discover.go`). On `StatusBadFilterNotAllowed` it recurses with `nodes[failedNodeIndex:]` (`read_discover.go`), so positions restart at 0 while the earlier items of the same subscription are still live under the same handles. The server (which per §7.21 does not enforce uniqueness — that is the client's job) ends up with two live MonitoredItems per reused handle, and notifications from either are indistinguishable to the client. Three of five scenarios fail. The two green scenarios are worth keeping: the accepted-filter path, and index-zero rejection against a filterless server (the retry's suffix *is* the full list, so handles are allocated once) — the fix must not break either.

The last failing scenario ("only the first node's filter") shows a second face of the same defect: the server created nodes 1–4 in the first request (per-item processing), and the full-list retry re-created them, so four nodes are monitored twice under duplicate handles — the duplicate-subscription behaviour of the sibling issue (ENG-5554).

---

## B2 — attribution must already be correct for notifications that arrive before the CreateMonitoredItems response — **RED**

Source: OPC 10000-4 §5.13.2.1 ("Clients set the ClientHandle for the MonitoredItem in the CreateMonitoredItems Request and are therefore able to process the Notifications" that arrive before the response does).

Command:

```
GOTOOLCHAIN=auto go test ./opcua_plugin/ -run 'TestB2EarlyNotificationsAreAttributedCorrectly' -count=1 -v
```

Real output (subtest statuses; representative failures shown, 3 of 7 in the failing scenario):

```
    --- PASS: TestB2EarlyNotificationsAreAttributedCorrectly/accepted_filter_path (0.01s)
    --- FAIL: TestB2EarlyNotificationsAreAttributedCorrectly/filter_fallback_at_a_nonzero_index (0.01s)
        read_client_handle_attribution_test.go:713: B2 (OPC 10000-4 §5.13.2.1 ...) [filter fallback at a nonzero index]: 3 of 7 early notifications (delivered before the CreateMonitoredItems response returned) were attributed to the wrong node:
            ... handle 0: the server monitors node ns=2;i=1002 (browse name "sensor_002", tag plant.line/sensor_002) under this handle via item 5 (handle 0 -> node ns=2;i=1002), but the notification's value was published as node ns=2;i=1000 (browse name "sensor_000", tag plant.line/sensor_000, payload "5"); mismatched: node ID: published ns=2;i=1000, expected ns=2;i=1002; browse name: published "sensor_000", expected "sensor_002"; tag name: published "sensor_000", expected "sensor_002"
            ... handle 1: ... published as node ns=2;i=1001 ... expected ns=2;i=1003 ...
            ... handle 2: ... published as node ns=2;i=1002 ... expected ns=2;i=1004 ...
--- FAIL: TestB2EarlyNotificationsAreAttributedCorrectly (0.01s)
FAIL	github.com/united-manufacturing-hub/benthos-umh/opcua_plugin	0.027s FAIL
```

**Reasoning.** RED in the fallback scenario, green on the plain path. The fake queues each accepted item's DataChangeNotification from *inside* `Monitor`, before the CreateMonitoredItems response returns; the test processes them while `MonitorBatched` is still in flight (the production 1s post-batch sleep provides the window; the test records a note if the race is ever lost). On the plain path B2 holds today because `ReadBatchSubscribe` resolves positionally against `NodeList`, which exists before `Monitor` is even called — nothing depends on the response. That green scenario pins behaviour the fix must keep: if the fix rebuilds attribution from the response, early notifications would regress it. In the fallback scenario the early notifications of the retried suffix items carry restarted handles and are misattributed exactly as in O1 — B2 fails there with the same root cause, and it fails *at the moment of arrival*, not merely after the response.

---

## O1 — the filter-rejection fallback preserves node ID, browse name and downstream tag — **RED**

Source: UMH policy (ours, no clause). Failure messages cite the policy and the concrete handle/node facts.

Command:

```
GOTOOLCHAIN=auto go test ./opcua_plugin/ -run 'TestO1FilterRejectionFallbackPreservesNodeIdentity' -count=1 -v
```

Real output (subtest statuses):

```
    --- FAIL: TestO1FilterRejectionFallbackPreservesNodeIdentity/rejection_at_a_nonzero_index (1.00s)
    --- PASS: TestO1FilterRejectionFallbackPreservesNodeIdentity/rejection_at_index_zero_against_a_filterless_server (1.00s)
    --- FAIL: TestO1FilterRejectionFallbackPreservesNodeIdentity/rejection_in_a_later_batch (2.01s)
    --- FAIL: TestO1FilterRejectionFallbackPreservesNodeIdentity/repeated_fallback_across_two_subscription_generations (2.01s)
    --- PASS: TestO1FilterRejectionFallbackPreservesNodeIdentity/accepted_filter_path_(no_fallback) (1.00s)
    --- PASS: TestO1FilterRejectionFallbackPreservesNodeIdentity/no-filter_profile_path (1.01s)
    --- FAIL: TestO1FilterRejectionFallbackPreservesNodeIdentity/heartbeat_node_keeps_its_identity_through_the_fallback (1.00s)
    --- PASS: TestO1FilterRejectionFallbackPreservesNodeIdentity/heartbeat_node_keeps_its_identity_on_the_accepted_filter_path (1.01s)
--- FAIL: TestO1FilterRejectionFallbackPreservesNodeIdentity (10.03s)
FAIL	github.com/united-manufacturing-hub/benthos-umh/opcua_plugin	10.041s FAIL
```

Representative failure (rejection at a nonzero index — 3 of 7 live items misattributed):

```
        read_client_handle_attribution_test.go:749: O1 (UMH policy, ENG-5553: the filter-rejection fallback must preserve each value's node ID, browse name and downstream tag) [rejection at a nonzero index]: 3 of 7 live MonitoredItems were attributed to the wrong node:
            ... handle 0: the server monitors node ns=2;i=1002 (browse name "sensor_002", tag plant.line/sensor_002) under this handle via item 5 (handle 0 -> node ns=2;i=1002), but the notification's value was published as node ns=2;i=1000 (browse name "sensor_000", tag plant.line/sensor_000, payload "5"); mismatched: node ID: published ns=2;i=1000, expected ns=2;i=1002; browse name: published "sensor_000", expected "sensor_002"; tag name: published "sensor_000", expected "sensor_002"
            ... handle 1: the server monitors node ns=2;i=1003 ... published as node ns=2;i=1001 ...
            ... handle 2: the server monitors node ns=2;i=1004 ... published as node ns=2;i=1002 ...
```

Heartbeat failure:

```
        read_client_handle_attribution_test.go:856: O1 (UMH policy, ENG-5553: ...) [heartbeat node through the fallback]: the heartbeat node lost its identity:
            ... the heartbeat node ns=2;i=1004 is monitored under handle 2 (item 7 (handle 2 -> node ns=2;i=1004)), but its value was published as node ns=2;i=1002 with heartbeat marker "" (marker present=false)
```

**Reasoning.** RED — this is the defect itself. The retry recurses with `nodes[failedNodeIndex:]`, positions restart at 0, and `ReadBatchSubscribe` still resolves `g.NodeList[handleID]` against the full list (`read.go`), so values publish under the wrong node ID, browse name and tag with no error and no log line. The later-batch scenario (10 nodes, batch size 5, rejection at index 7) shows it is not only a first-batch problem: the retry's handles 0–2 point at nodes 7–9 while the list resolves them to nodes 0–2. The repeated-fallback scenario fails in *both* generations (fresh connection state re-triggers the fallback after a reconnect). The heartbeat scenario shows the heartbeat node loses both its node identity and its `opcua_heartbeat_message` marker when it lands in the retried suffix.

The three green scenarios (index-zero rejection against a filterless server, accepted filter path, no-filter profile path, plus heartbeat on the accepted path) pin correct behaviour the fix must not break. Index-zero rejection passes today because the retry's suffix *is* the whole list, so positions line up again — worth knowing, because it is the fallback case a fix gets right "by accident" and must keep.

---

## O3 — an unknown or out-of-range handle is dropped with a diagnostic, never fatal — **RED**

Source: UMH policy.

Command:

```
GOTOOLCHAIN=auto go test ./opcua_plugin/ -run 'TestO3UnknownOrOutOfRangeHandleIsDroppedWithDiagnosticNeverFatal' -count=1 -v
```

Real output (all three scenarios fail):

```
    --- FAIL: TestO3UnknownOrOutOfRangeHandleIsDroppedWithDiagnosticNeverFatal/handle_equal_to_the_node_list_length_must_not_be_fatal (1.00s)
        read_client_handle_attribution_test.go:899: O3 (UMH policy, ENG-5553: an unknown or out-of-range ClientHandle must be dropped with a diagnostic, never fatal): a DataChangeNotification carrying handle 5 (the node list has 5 entries, so this handle is out of range) panicked ReadBatchSubscribe: runtime error: index out of range [5] with length 5 - an out-of-range handle must be dropped with a diagnostic, never fatal
    --- FAIL: TestO3UnknownOrOutOfRangeHandleIsDroppedWithDiagnosticNeverFatal/handle_beyond_the_node_list_length_is_dropped_but_must_be_diagnosed (1.00s)
        read_client_handle_attribution_test.go:944: O3 (UMH policy, ENG-5553: ...): a DataChangeNotification carrying out-of-range handle 100 (the node list has 5 entries) was dropped silently - it must be dropped with a diagnostic so operators can tell values are being lost
    --- FAIL: TestO3UnknownOrOutOfRangeHandleIsDroppedWithDiagnosticNeverFatal/unknown_in-range_handle_the_server_has_no_item_for_must_be_dropped (0.00s)
        read_client_handle_attribution_test.go:980: O3 (UMH policy, ENG-5553: ...): the server has no MonitoredItem under handle 2 (it has no live items at all), yet the notification's value was published under node ns=2;i=1002 (browse name "sensor_002") - an unknown handle must be dropped with a diagnostic, never attributed to an arbitrary node
        read_client_handle_attribution_test.go:986: O3 (UMH policy, ENG-5553: ...): a DataChangeNotification carrying unknown handle 2 (the server has no item under it) was dropped silently - it must be dropped with a diagnostic
--- FAIL: TestO3UnknownOrOutOfRangeHandleIsDroppedWithDiagnosticNeverFatal (2.01s)
FAIL	github.com/united-manufacturing-hub/benthos-umh/opcua_plugin	2.028s FAIL
```

**Reasoning.** RED on all three obligations, each for a different reason, all rooted in the same positional lookup:

1. **Fatal**: `read.go` guards with `uint32(len(g.NodeList)) >= handleID` — an off-by-one. A handle equal to the list length passes the guard and `g.NodeList[handleID]` panics. In production that panic would take down the benthos process. The test recovers the panic and reports it as an O3 violation instead of crashing, so the row stays a normal assertion.
2. **Silent drop**: handles beyond the list are dropped (correct) but nothing is logged, so an operator cannot tell values are being lost.
3. **Unknown in-range handle**: a handle the server has no item under resolves to an arbitrary list entry and is published under that node's ID with no error — indistinguishable, in production, from correct data.

The "with a diagnostic" half is asserted only as *some log output after the drop* (via a captured slog logger); the tests deliberately do not pin wording.

---

## O4 — handles from one subscription generation cannot resolve against a later one after a reconnect — **RED**

Source: UMH policy.

Command:

```
GOTOOLCHAIN=auto go test ./opcua_plugin/ -run 'TestO4HandlesFromOneGenerationDoNotResolveAfterReconnect' -count=1 -v
```

Real output:

```
=== RUN   TestO4HandlesFromOneGenerationDoNotResolveAfterReconnect
    read_client_handle_attribution_test.go:1089: O4 (UMH policy, ENG-5553: handles from one subscription generation must not resolve against a later one after a reconnect): stale handles from a previous subscription generation resolved against the new one:
        ... a stale notification from subscription generation 1 (subscription ID 1001) carrying handle 2, which generation 1's server monitored as node ns=2;i=1002, was published after the reconnect as: node ns=3;i=2002 (browse name "valve_002") - a handle from a previous generation must not resolve against the new node list
        ... a late stale notification from subscription generation 1 (subscription ID 1001) carrying handle 3, which generation 1's server monitored as node ns=2;i=1003, panicked ReadBatchSubscribe after the reconnect: runtime error: index out of range [3] with length 3
--- FAIL: TestO4HandlesFromOneGenerationDoNotResolveAfterReconnect (2.01s)
FAIL	github.com/united-manufacturing-hub/benthos-umh/opcua_plugin	2.024s FAIL
```

**Reasoning.** RED, in both variants. The test builds generation 1 (5 nodes, ns=2, filter accepted), snapshots the server's handle→node pairs, then reconnects through the production cleanup path (`unsubscribeAndResetHeartbeat` → `Cancel`), and builds generation 2 (3 nodes, ns=3, fresh `ServerCapabilities`) as a fresh browse of the same server can produce. A stale generation-1 notification carrying handle 2 is then published as a generation-2 node it never belonged to; a late stale notification with handle 3 panics off the same off-by-one as O3.

Honest caveat on reachability, recorded here rather than hidden: production currently recreates `SubNotifyChan` on every `Connect()` (`read.go`), and gopcua routes a subscription's notifications to the channel it was created with, so a stale generation-1 publish normally dies on the old channel. The test deliberately shares the notify channel across generations to model the hazard the obligation names — a stale publish reaching the reader around a reconnect, which becomes reachable the moment the channel is shared across generations or in-flight notifications are re-routed. The two sub-scenarios also give a future fix two honest targets: a drain-on-reconnect passes sub-scenario (a) (stale notification queued before generation 2 is set up), while only generation-scoped handles or a SubscriptionID check pass (b) (late arrival). The fix step owns that choice; this row only pins the obligation.

---

## Rows / scenarios I could not cover, and why

- **O3 "with a diagnostic" wording** — asserted only as "some log output appears after the drop". Pinning exact wording would make the row brittle against any reasonable fix.
- **Race-detector run** — `go test -race` cannot run in this container (no C compiler: `cgo: C compiler "gcc" not found`). The B2 tests' concurrency shape (a reader draining notifications while `MonitorBatched` is in flight) mirrors production's own concurrency; no *new* shared state is introduced by the tests.
- **A real OPC UA server** — all rows are covered against the fake server described above. The fake follows OPC 10000-4 semantics (per-item results, opaque-but-echoed client handles, notification-before-response), but hardware confirmation of the fallback paths remains the domain of the simulator/PLC suites (`opcua_plc_test.go` etc.), which are env-gated and out of scope here.
- **B2 early notifications for the index-zero and later-batch fallback variants** — only the nonzero-index fallback is exercised with early delivery; the index-zero variant is green today (positions realign, see O1) so early delivery would pass trivially, and the later-batch variant shares the nonzero-index root cause. Noted rather than padded with redundant rows.
- **Heartbeat through the later-batch / repeated-fallback variants** — only the nonzero-index fallback; one representative path through the suffix suffices to pin the heartbeat obligation, the other variants exercise the same resolution code.
- **`StatusBadFilterNotAllowed` on the *retry* itself** — unreachable: the retry omits filters entirely (cached `SupportsDataChangeFilter=false`), so the server cannot reject it with a filter error; a non-filter rejection follows the non-trial error path and was not part of the rows.

## What I noticed but did not change

- `read.go` (bounds guard near the `NodeList[handleID]` resolution): `uint32(len(g.NodeList)) >= handleID` is off by one — a handle equal to the list length passes and panics. Pinned by O3; not fixed.
- `read_discover.go` retry recursion: because the server processes CreateMonitoredItems per item, nodes *after* the rejected one in the same request are already live when the retry re-creates them — duplicate MonitoredItems (the ENG-5554 side). Pinned by B1's "rejection of only the first node's filter" scenario; not fixed.
- `MonitorBatched` sleeps a fixed 1s after every batch, which dominates the suite's ~20s runtime. Untouched.
- Pre-existing `go vet` complaint, present before my change and in a file I did not touch: `opcua_plugin/core_browse_global_pool_test.go:206:7: the cancel function returned by context.WithTimeout should be called, not discarded, to avoid a context leak`. `GOTOOLCHAIN=auto go vet ./opcua_plugin/` reports this one line only; all files I added or edited are vet-clean.
- The existing "reproduction" tests (`read_discover_recursive_retry_bug_test.go`, and the ginkgo specs in the internal `opcua_plugin` test package generally) are gated behind `TEST_OPCUA_UNIT` and recompute the batching arithmetic locally; they pass today while the defect exists. Left untouched per instructions — a later step removes them.
- `ReadBatchSubscribe`'s resolution comment says "the handle id, which is the position in g.Nodelist"; the O3/O4 results above show that assumption also has no diagnostic and an off-by-one. Not fixed.
- `Connect()` recreates `SubNotifyChan` per connection; the O4 discussion above explains why the cross-generation hazard is quarantined today and what un-quarantines it.
