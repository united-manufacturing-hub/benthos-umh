# ENG-5553 step 3 — the one-way-door testing bar

Branch: `eng-5553-opc-ua-values-attributed-to-the-wrong-tag-after-a` (base `bcc7b7d`).
All commands run from `/workspace` with `GOTOOLCHAIN=auto` (go.mod wants Go 1.27.4, container has 1.27.1, arm64).

## Headline

**The invariant held under the property test.** 37 generated scenarios (13 deterministic edge shapes + 24 seeded
random ones: arbitrary list length 1–8, arbitrary batch size 1–8, arbitrary rejection sets including zero
rejections, index 0, the last index, and several rejections) plus ~614 fuzz executions found **zero** attribution
violations in the fixed code. No new files touch production; no existing test was modified
(`git status` shows only two new test files):

```
?? opcua_plugin/client_handle_attribution_property_test.go
?? opcua_plugin/client_handle_fault_injection_test.go
```

The frozen files are byte-identical to HEAD (`git diff HEAD --stat -- read_client_handle_attribution_test.go
subscription_fake_test.go subscription_nil_test.go` is empty).

One asked-for fault-injection scenario **reproduced the suspected ENG-7894 deadlock** (recorded, test skipped with
a comment naming ENG-7894 — see §2). Two deliberate mutants **survived** the entire package suite and are named in
§3.

---

## 1. Property-based test

File: `opcua_plugin/client_handle_attribution_property_test.go` (new). Standard library only — no new dependency.

**Invariant:** for an arbitrary node list, an arbitrary batch size and an arbitrary set of filter-rejection
positions, every notification resolves to the node the server monitors under its handle (and a handle identifies
exactly one MonitoredItem).

**Design.** Both permitted styles are used:

- a hand-rolled generator over a seeded `math/rand` (`derivePropertyScenario(seed)`): seed → list length (1–8),
  batch size (1–8), per-index rejection with p=1/3, occasional reject-everything server. Each scenario is a
  **parallel subtest named after the exact scenario** (`seed=14_n=6_b=5_rejected=[2_3_5]...`), so a failure's
  `go test` output names the list length, batch size and rejection set, and the seed alone reproduces it.
- a native `testing.F` target (`FuzzClientHandleAttributionProperty`) whose inputs **are** the scenario
  (`listLength, batchSize uint8, rejectedMask uint64`), so a failing input reported by the fuzzing engine is
  directly readable as the scenario; the test also `t.Logf`s the derived scenario on every exec.

13 deterministic **fixed edge scenarios** guarantee the named corners in every run: zero rejections (single- and
multi-batch), rejection at index 0 (single- and multi-batch), rejection at the last index (single- and
multi-batch), several rejections (single- and multi-batch), batch size 1, batch larger than the list, single-node
list (with and without rejection), and a filterless server rejecting every filtered request.

Every scenario drives the **real production path** (`MonitorBatched` with its trial/fallback logic, then a replayed
notification for every live MonitoredItem through `ReadBatchSubscribe`) using the frozen harness and the frozen
`fakeSubscription` — expected handles are never computed; the server's record is the authority. Three assertions
per scenario: handle uniqueness (B1 shape), end-to-end attribution (O1 shape), and a direct registry check
(lookup of every live handle returns exactly that item's node).

**Real command output** (normal mode; `-short` runs 6 seeds instead of 24 and skips the fuzz corpus — the edge
scenarios still all run):

```
$ GOTOOLCHAIN=auto go test -count=1 ./opcua_plugin/ -run 'TestClientHandleAttributionProperty|FuzzClientHandleAttributionProperty'
ok  github.com/united-manufacturing-hub/benthos-umh/opcua_plugin 22.138s
```

37 scenarios, 37 PASS, 0 FAIL.

**Fuzzing** (the generated scenario space is bounded by the 1s-per-batch production sleep, so exec rates are
seconds, not thousands/sec — reported honestly):

```
$ GOTOOLCHAIN=auto go test -count=1 ./opcua_plugin/ -run '^$' -fuzz '^FuzzClientHandleAttributionProperty$' -fuzztime 150s
fuzz: elapsed: 2m30s, execs: 577 (3/sec), new interesting: 52 (total: 60)
PASS
ok  github.com/united-manufacturing-hub/benthos-umh/opcua_plugin 152.054s
```

577 fuzz execs in 150s plus 37 corpus/subtest scenarios — **no failures**. The fuzzer marked 52 inputs as
"interesting" (coverage-guided, kept in the build cache); none violated the invariant. `fuzz` found no corpus
entries on disk, so nothing was added under `testdata/`.

**Result: no defect found.** The registry-based attribution satisfies the property on every generated shape.

---

## 2. Fault injection

File: `opcua_plugin/client_handle_fault_injection_test.go` (new). All three scenarios drive production code
(`MonitorBatched`, `ReadBatchSubscribe`, `unsubscribeAndResetHeartbeat`, `startBrowsing`,
`BrowseAndSubscribeIfNeeded`) against the frozen fake.

```
$ GOTOOLCHAIN=auto go test -count=1 ./opcua_plugin/ -run 'TestFaultInjection' -v
--- PASS: TestFaultInjectionSubscriptionCancelledAndRebuiltMidStream (2.01s)
--- PASS: TestFaultInjectionMonitoredItemCreationFailsNotBadFilterNotAllowed (0.00s)
--- SKIP: TestFaultInjectionENG7894CloseFromInsideBrowseGoroutineDeadlocks (0.00s)   <- reproduced deadlock, see below
--- PASS: TestFaultInjectionNotificationAfterTeardownBeforeNextGeneration (2.01s)
ok   github.com/united-manufacturing-hub/benthos-umh/opcua_plugin 4.042s
```

### FI-1 — subscription cancelled and rebuilt (reconnect) mid-stream — PASS

Generation 1 streams notifications from inside `Monitor` (delivered before the response returns, OPC 10000-4
§5.13.2.1) while the reader consumes them; the production teardown path (`unsubscribeAndResetHeartbeat`: Cancel +
registry reset) runs mid-stream; the generation is then rebuilt against different nodes sharing the notify
channel. Verified: the first streaming notifications attribute correctly; the still-queued generation-1
notifications become post-teardown arrivals that are **dropped with a diagnostic, never attributed**; generation 2
attributes correctly; a stale generation-1 handle that is positionally valid in generation 2's node list (the exact
ENG-5553 misattribution shape) does not resolve; generation 2 attribution is unaffected after the stale arrival.

### FI-2 — monitored-item creation fails for a non-`StatusBadFilterNotAllowed` reason — PASS (and ENG-7894 reproduced)

The server rejects one node with `StatusBadNodeIDUnknown`. Verified that the failure is **reported, not
swallowed**: `MonitorBatched` returns a non-nil error naming the rejected node; a log line names the node; the
`opcua_subscription_failures_total` counter is incremented (`node_id_unknown` label). Attribution still holds
around the failure: per-item result processing means the server created the items before *and after* the rejected
one in the same request — all of them still attribute correctly through their allocated handles; the registry
never maps any handle to a node it was not allocated for; and the rejected node's own handle (allocated before the
request was sent, per §5.13.2.1, but never created server-side) resolves to the rejected node itself and only to
it — this pins the documented FIX-REPORT decision.

### FI-2b — the same abort path inside the browse goroutine: **ENG-7894 deadlock REPRODUCED**

The suspicion was that `BrowseAndSubscribeIfNeeded` and `MonitorBatched` call `g.Close(ctx)` from inside the
browse goroutine while `Close` waits on that same goroutine. **Nobody had executed that path; this test does, with
100% production code** (`startBrowsing` → `BrowseAndSubscribeIfNeeded` → `g.Close(ctx)`): a never-connected
`*opcua.Client` (Subscribe fails fast with `StatusBadServerNotConnected`) plus a fresh visited-cache so discovery
is skipped. The test ran **without** the skip and failed after its 15s watchdog with this captured goroutine dump
(full run log preserved from the actual execution):

```
goroutine 35 [sync.WaitGroup.Wait]:
    sync.runtime_SemacquireWait(0x0?, 0x0?)
    sync.(*WaitGroup).Wait(0x179189880868)
    github.com/united-manufacturing-hub/benthos-umh/opcua_plugin.(*OPCUAConnection).cleanupBrowsing(0x179189880780)
        /workspace/opcua_plugin/core_connection.go:200 +0x8c
    github.com/united-manufacturing-hub/benthos-umh/opcua_plugin.(*OPCUAConnection).Close(0x179189880780, {...})
        /workspace/opcua_plugin/core_connection.go:225 +0x58
    github.com/united-manufacturing-hub/benthos-umh/opcua_plugin.(*OPCUAInput).BrowseAndSubscribeIfNeeded(0x17918962fc80, {...})
        /workspace/opcua_plugin/read_discover.go:342 +0x608
    github.com/united-manufacturing-hub/benthos-umh/opcua_plugin.(*OPCUAInput).startBrowsing.func1()
        /workspace/opcua_plugin/read.go:339 +0xa0
    created by github.com/united-manufacturing-hub/benthos-umh/opcua_plugin.(*OPCUAInput).startBrowsing in goroutine 34
        /workspace/opcua_plugin/read.go:335 +0xcc
```

That is the deadlock: the browse goroutine (goroutine 35, created by `startBrowsing`) is blocked in
`browseWaitGroup.Wait()` inside `cleanupBrowsing`, called from `Close`, called from the `Subscribe`-failure branch
of `BrowseAndSubscribeIfNeeded` (read_discover.go:342) — **Close waits for the very goroutine that is calling it**.
It never returns; `setBrowseErr` is never reached, so `ReadBatch` never even learns the connection is dead.

Per the task's instruction I did **not** fix it and did **not** hide it behind a production timeout. The test is
committed as `t.Skip` with a comment naming ENG-7894 and pointing at this report; the repro body is kept intact
so removing the Skip verifies a future fix. Two honest notes: (a) the reproduced call site is the
`BrowseAndSubscribeIfNeeded` one — the `MonitorBatched` `g.Close` call sites (Monitor error, nil response,
non-filter rejection) share the identical in-goroutine shape but need a *successful* `Client.Subscribe` to be
reached, which the harness cannot fake (see §3 survivor M14); by construction they deadlock the same way. (b)
The FI-2 test above exercises the abort path itself safely because it calls `MonitorBatched` from the test
goroutine, where `cleanupBrowsing` has no browse goroutine to wait for.

### FI-3 — notification arriving after teardown but before the next generation is built — PASS

Teardown via the production path, then **nothing rebuilt**: every generation-1 handle (all positionally valid
against any future list — the old positional lookup would have attributed them) is dropped with a diagnostic,
never fatal, never published; an out-of-band handle nobody ever allocated is also dropped with a diagnostic. When
generation 2 is finally built, it attributes correctly, and the old handles still do not resolve — including
handle 1 against a 4-entry list, the exact shape that misattributed values before the fix. Overlap disclosure:
this necessarily touches the same obligation as the frozen O4(a)/(b); its distinct angles are the every-handle
sweep, the dropped-with-diagnostic requirement for *each* drop in the teardown window, and the never-rebuilt
window itself.

---

## 3. Mutation testing

**Gremlins could not be installed — plainly:** `github.com/hexops/gremlins` no longer exists on GitHub
(`https://github.com/hexops/gremlins` → 404; the org deleted its public repos) and the Go module proxy has no
cached versions:

```
$ go install github.com/hexops/gremlins@latest
go: module github.com/hexops/gremlins: git ls-remote ... : exit status 128:
    fatal: could not read Username for 'https://github.com': terminal prompts disabled
$ curl -s https://proxy.golang.org/github.com/hexops/gremlins/@v/list
404 - not found: module github.com/hexops/gremlins
```

Fell back to a small set of **deliberate hand-applied mutants on the new code only** — the registry
(`subscription_handle_registry.go`), the handle allocation (the `ClientHandle` line and the post-Subscribe reset
in `read_discover.go`), and the notification resolution (the lookup/drop block and the teardown reset in
`read.go`). Each mutant was applied, the suite run, and the file restored with `git checkout` (working tree is
clean afterwards; frozen files untouched).

**Kill suite** (frozen attribution rows + the new property and fault-injection tests; run with `-short` so the
property generator uses 6 seeds — the 13 fixed edge scenarios always run; survivors were re-checked without
`-short` and against the whole package):

```
$ GOTOOLCHAIN=auto go test -count=1 -short ./opcua_plugin/ \
    -run 'TestB1|TestB2|TestO1|TestO3|TestO4|TestTeardown|TestClientHandleAttributionProperty|TestFaultInjection'
ok  github.com/united-manufacturing-hub/benthos-umh/opcua_plugin 29.203s   (baseline, unmutated)
```

### Mutant table (12 killed, 2 survivors)

| # | Mutant (file: what was changed) | Caught by |
|---|---|---|
| M1 | registry: `allocate` never increments `nextHandle` (every handle 0) | **B1, B2, O1, O4, Property, FI-1, FI-2, FI-3** |
| M2 | registry: `allocate` does not store the node (lookup always unknown) | B2, O1, Property, FI-1, FI-2, FI-3 |
| M3 | registry: `allocate` returns `nextHandle+1` (handle values shifted) | **O3 only** |
| M4 | registry: `lookup` inverts its found flag | B2, O1, O3, O4, Property, FI-1, FI-2, FI-3 |
| M5 | registry: `lookup` returns zero `NodeDef`, `true` (ignores the map) | B2, O1, O3, O4, Property, FI-1, FI-2, FI-3 |
| M6 | registry: `reset` also zeroes `nextHandle` | O4, FI-1, FI-3 |
| M7 | registry: `reset` is a no-op (map kept) | O4, FI-1, FI-3 |
| **M8** | registry: `lookup` takes no lock (unsynchronized map read) | **SURVIVOR — nothing caught it** |
| M9 | allocation: `ClientHandle` reverts to `uint32(batchRange.Start + pos)` (the original ENG-5553 defect) | **B1, B2, O1, Property, FI-1, FI-2, FI-3** |
| M10 | resolution: `handleID := item.ClientHandle - 1` (off-by-one) | B2, O1, O3, Property, FI-1, FI-2, FI-3 |
| M11 | resolution: unknown handle dropped silently (diagnostic removed) | O3, O4, FI-1, FI-3 |
| M12 | resolution: unknown handle attributed to a zero-value node instead of dropped | O3, O4, FI-1, FI-3 |
| M13 | teardown: `unsubscribeAndResetHeartbeat` no longer resets the registry | O4, FI-1, FI-3 |
| **M14** | `BrowseAndSubscribeIfNeeded` no longer resets the registry after `Subscribe` succeeds | **SURVIVOR — nothing caught it** |

### The two survivors, named and explained

**M8 — `subscriptionHandleRegistry.lookup` without the `RWMutex` read lock.** Nothing catches it: not the kill
suite, not three consecutive **full** (non-short) runs of the entire attribution suite, and not the whole package
including the env-gated ginkgo suite (`TEST_OPCUA_UNIT=1`, three `ok` results). The mutex is *behaviorally
invisible* to these tests: the only genuine lookup-vs-allocate overlap (the B2 fallback scenario, where the retry
allocates while the reader resolves early notifications) is too narrow for Go's concurrent-map detection to fire.
The tool that kills this mutant is `-race`, which **cannot run in this container** (see below). This is a real
gap, not a score: the registry's concurrency guard is unverified by any executable test I could run here.
Recommendation: a `-race` run of `./opcua_plugin/` on a host with cgo before this ships.

**M14 — the `g.handleRegistry.reset()` after `Client.Subscribe` in `BrowseAndSubscribeIfNeeded`
(read_discover.go:351).** Nothing catches it because nothing can *reach* it: driving
`BrowseAndSubscribeIfNeeded` past `Subscribe` requires a live `*opcua.Client` session — `Client` is a concrete
type, not a seam like `Subscription` — so no fake-based test can execute the Subscribe-success path (the ENG-7894
repro in §2 reaches only the failure branch). In every test-reachable path the teardown reset (M13, killed by three
tests) has already cleared the registry, making this reset redundant there. It is defense-in-depth for a
"new subscription created without a teardown" production path — dead code from the test suite's point of view.
Either it deserves a test (which needs a client seam, a production change out of scope here) or it should be a
conscious review decision, not an accident.

A coverage-thinness note worth naming: **M3 was caught by exactly one test (O3)**. Only O3 pins the *values*
handles take (it pushes handle = node-list length, which the shift accidentally makes valid); if O3 were ever
relaxed, all handle-value drift becomes invisible to the suite. Conversely B1 never catches resolution mutants
(M2, M4, M5, M10…) — it inspects only server-side uniqueness, which stays correct — the resolution obligations are
carried by B2/O1/O3/O4 and the new tests.

---

## Things found that nobody asked about

1. **ENG-7894 is real, executed, and worse than a suspicion** (§2, FI-2b): `Close` from inside the browse
   goroutine waits on itself. Because the browse goroutine never returns, `setBrowseErr` is never called —
   `ReadBatch` cannot even report the dead connection; the input hangs rather than reconnecting. The reproduced
   path needs nothing exotic: a server declining a subscription (any unconnected/unreachable endpoint) at
   Connect time is enough.
2. **The registry's mutex is test-unverifiable in this container** (survivor M8): `-race` requires cgo and the
   container has no C compiler — attempted for the record:
   ```
   $ CGO_ENABLED=1 GOTOOLCHAIN=auto go test -race -count=1 ./opcua_plugin/ -run 'TestClientHandleAttributionProperty'
   # runtime/cgo
   cgo: C compiler "gcc" not found: exec: "gcc": executable file not found in $PATH
   FAIL    github.com/united-manufacturing-hub/benthos-umh/opcua_plugin [build failed]
   ```
   I ran no race-detector verification and claim nothing from it. Host-side `-race` run recommended.
3. **`OPCUAConnection.Client` is a concrete `*opcua.Client`**, so `BrowseAndSubscribeIfNeeded`'s
   Subscribe-success path (and `MonitorBatched` behind it) is un-fakeable — that is why M14 survives and why the
   ENG-7894 repro covers the `Subscribe`-failure call site rather than `MonitorBatched`'s `g.Close` sites. The step-1
   `SubscriptionAPI` seam made the subscription fakeable; an analogous client seam would make the rest testable.
4. **A mid-flight teardown would nil-panic `MonitorBatched`**: `unsubscribeAndResetHeartbeat` sets
   `g.Subscription = nil`, so a teardown racing an in-flight `MonitorBatched` makes its next
   `g.Subscription.Monitor` call a nil-interface panic on the browse goroutine. Production ordering currently
   protects it (`Close` → `cleanupBrowsing` waits for the browse goroutine *before* `cleanup_func` runs the
   teardown), and FI-1 deliberately tears down during a single-batch generation's post-batch sleep to stay inside
   that ordering. But the protection is a call-order invariant, not a lock — worth knowing before anyone
   reorders `Close`.
5. **The 1s post-batch sleep in `MonitorBatched` caps property-test throughput** (~3 fuzz execs/sec, §1). Not
   changed (production code is frozen for this step), but an injectable batch delay would make future fuzzing
   of this area orders of magnitude more effective.
6. **Suite cost**: the full package now runs ~48s (frozen suite ~20s; property test ~22s standalone; fuzz corpus
   +~10s; FI tests +4s). `-short` runs ~31s (6 generated property scenarios, no fuzz corpus, all 13 edge
   scenarios and all FI tests). `go vet` on the package still reports only the pre-existing
   `core_browse_global_pool_test.go:206` complaint from a file I did not touch; `gofmt -l` is clean;
   `GOTOOLCHAIN=auto go build ./...` succeeds.

Nothing was pushed, no PR opened, no outward comments made; all work returns as this patch (two new test files
plus this report).
