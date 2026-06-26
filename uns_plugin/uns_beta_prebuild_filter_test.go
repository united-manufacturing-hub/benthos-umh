//go:build connect_patched

// Copyright 2025 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package uns_plugin

import (
	"math"
	"runtime"
	"testing"
)

// TestUnsBetaSelectOneAllocBudget is the committed perf gate for the connect
// key_pattern pre-filter (ENG-5105). It drains a select_one firehose (one keep
// record at the tail of an all-drop corpus) through the uns_beta stream against
// a real redpanda broker and asserts the per-record allocation cost stays under
// budget. It only has teeth on the patched build, hence //go:build
// connect_patched: the omit-from-batch branch lives in the patched connect
// reader, so without the patch the budget is blown by the unfiltered build.
//
// Baseline (omit branch absent): ~32 allocs/record. With omit: the firehose's
// non-matching records are dropped before message construction, leaving only a
// per-poll high-water placeholder ferried through the framework. The budget
// guards that the large drop is preserved across connect bumps and refactors.
//
// Measurement: two untimed warmup drains absorb the one-time cold cost — a fresh
// container's first fetch triggers consumer-group rebalance + franz-go
// epoch/offset loads that a single warmup does not always fully absorb (observed
// spiking to ~159/record on the very first drain after a fresh build). Then each
// of measuredRuns steady-state drains is measured independently and the MINIMUM
// is taken: the steady-state floor is the minimum, so a one-off cold/GC spike on
// a single drain does not move it, while a real regression (non-matching records
// rebuilt) inflates EVERY drain so the minimum rises too and the gate still trips.
func TestUnsBetaSelectOneAllocBudget(t *testing.T) {
	const corpus = 100000
	// Steady-state floor on the patched build is well under 1 alloc/record by this
	// MemStats min-of-drains measure (RED, omit branch absent: ~32); the E2E
	// throughput benchmark's amortized -benchmem counts it nearer ~2 for the same
	// scenario — both are an order of magnitude or more below baseline, and the
	// difference is measurement methodology, not the design. Budget 3.0 sits
	// safely above either steady state and far below the ~32 a regression produces.
	const budget = 3.0
	const measuredRuns = 3

	// Pin the firehose depth for this gate, independent of the UNS_BENCH_CORPUS
	// env knob the throughput benchmark uses. Restored after.
	savedCorpus := benchCorpusSize
	benchCorpusSize = corpus
	defer func() { benchCorpusSize = savedCorpus }()

	// testing.Benchmark gives us a *testing.B (which startRedpandaBroker needs
	// and which satisfies the runUnsBetaStream testingT interface). We ignore
	// its iteration accounting and compute allocs ourselves so we can warm up
	// before measuring. It runs the body once (each drain is seconds-long, so
	// b.N stays 1); guard against a future N>1 by only doing setup once.
	var (
		allocsPerRecord float64
		ran             bool
	)
	res := testing.Benchmark(func(b *testing.B) {
		if ran {
			return
		}
		ran = true

		addr := startRedpandaBroker(b)
		ensureTopic(b, addr)
		benchProduce(b, addr, benchCorpus())

		drain := func() {
			group := nextBenchGroup("perf-unsbeta")
			drainOnce(b, benchInputYAML("uns_beta", addr, group, benchTopicsSelectOne), 1)
		}

		drain() // warmup 1: absorbs consumer-group rebalance
		drain() // warmup 2: absorbs the fresh-container epoch/offset-load cold cost

		// Measure each steady-state drain independently and keep the MINIMUM, so a
		// one-off cold/GC spike on a single drain cannot false-fail the gate while
		// a real regression (every drain rebuilds non-matching records) still does.
		allocsPerRecord = math.Inf(1)
		for i := 0; i < measuredRuns; i++ {
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			drain()
			runtime.ReadMemStats(&after)
			if apr := float64(after.Mallocs-before.Mallocs) / float64(corpus); apr < allocsPerRecord {
				allocsPerRecord = apr
			}
		}
	})

	if res.N == 0 || !ran {
		t.Skip("benchmark did not run (redpanda/Docker unavailable)")
	}

	t.Logf("uns_beta select_one: %.4f allocs/record (min over %d steady-state drains of %d records, after 2 warmups)",
		allocsPerRecord, measuredRuns, corpus)
	if allocsPerRecord > budget {
		t.Fatalf("alloc budget exceeded: %.4f allocs/record > %.1f (the connect key_pattern omit branch is the load-bearing reduction; a regression here means non-matching records are being built again)",
			allocsPerRecord, budget)
	}
}
