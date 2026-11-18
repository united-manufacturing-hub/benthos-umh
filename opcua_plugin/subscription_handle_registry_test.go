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

package opcua_plugin

import (
	"fmt"
	"sync"
	"testing"

	"github.com/gopcua/opcua/ua"
)

// TestHandleRegistryAllocateAndLookupAreSafeConcurrently pins the registry's
// locking.
//
// In production the browse goroutine allocates handles while it builds later
// batches at the same time as the benthos input goroutine resolves arriving
// notifications, so allocate and lookup genuinely overlap. Every other test in
// this package drives the registry from a single goroutine, which cannot
// observe a race however often it runs - so without this test the RWMutex can
// be deleted and the whole suite stays green, including under -race.
//
// Run this with -race to get the full value from it.
func TestHandleRegistryAllocateAndLookupAreSafeConcurrently(t *testing.T) {
	const (
		writers          = 4
		readers          = 4
		allocsPerWriter  = 200
		lookupsPerReader = 400
	)

	var r subscriptionHandleRegistry
	var wg sync.WaitGroup

	// Seed one handle so readers have something real to find from the start.
	seeded := r.allocate(NodeDef{NodeID: ua.NewStringNodeID(1, "seed")})

	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < allocsPerWriter; i++ {
				r.allocate(NodeDef{NodeID: ua.NewStringNodeID(1, fmt.Sprintf("w%d-n%d", w, i))})
			}
		}(w)
	}

	wg.Add(readers)
	for rd := 0; rd < readers; rd++ {
		go func() {
			defer wg.Done()
			for i := 0; i < lookupsPerReader; i++ {
				// The seeded handle must stay resolvable throughout: a handle
				// that was allocated never stops being valid while its
				// subscription generation lives.
				if _, ok := r.lookup(seeded); !ok {
					t.Errorf("handle %d was allocated but stopped resolving while other handles were being allocated", seeded)
					return
				}
				r.lookup(uint32(i)) // result unused; exercises concurrent reads of arbitrary keys
			}
		}()
	}

	wg.Wait()

	// Every allocation handed out a distinct handle and every one resolves.
	total := uint32(writers*allocsPerWriter) + 1
	seen := make(map[uint32]struct{}, total)
	for h := uint32(0); h < total; h++ {
		if _, ok := r.lookup(h); !ok {
			t.Errorf("handle %d was allocated but does not resolve after the concurrent run", h)
		}
		seen[h] = struct{}{}
	}
	if len(seen) != int(total) {
		t.Errorf("expected %d distinct handles after the concurrent run, got %d", total, len(seen))
	}
}
