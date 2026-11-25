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

// TestUnsBetaSelectOneAllocBudget drains a select_one corpus (one matching
// record at the end of 100000 non-matching ones) through uns_beta against a
// redpanda container and checks the allocations per record. Connect's
// key_pattern pre-filter drops the non-matching records before it builds a
// message. If they are built again, the cost rises to about 32 allocations
// per record.
//
// Two warmup drains absorb the consumer-group rebalance and the first fetches
// from a fresh container. The gate takes the minimum of measuredRuns drains:
// a one-off GC spike raises only one drain, a regression raises all of them.
func TestUnsBetaSelectOneAllocBudget(t *testing.T) {
	const corpus = 100000
	// The patched build measures about 2.1 allocations per record.
	const budget = 3.0
	const measuredRuns = 3

	// Skips when Docker is unavailable.
	addr := startRedpandaBroker(t)
	ensureTopic(t, addr)
	benchProduce(t, addr, benchCorpus(corpus))

	drain := func() {
		drainOnce(t, benchInputYAML("uns_beta", addr, nextBenchGroup("perf-unsbeta"), benchTopicsSelectOne), corpus, 1)
	}
	drain()
	drain()

	allocsPerRecord := math.Inf(1)
	for i := 0; i < measuredRuns; i++ {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		drain()
		runtime.ReadMemStats(&after)
		allocsPerRecord = math.Min(allocsPerRecord, float64(after.Mallocs-before.Mallocs)/float64(corpus))
	}

	t.Logf("uns_beta select_one: %.4f allocs/record (min over %d drains of %d records, after 2 warmups)",
		allocsPerRecord, measuredRuns, corpus)
	if allocsPerRecord > budget {
		t.Fatalf("alloc budget exceeded: %.4f allocs/record > %.1f; non-matching records are being built into messages again",
			allocsPerRecord, budget)
	}
}
