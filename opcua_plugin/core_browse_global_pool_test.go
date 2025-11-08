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
	"sync"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("GlobalWorkerPool", func() {
	Context("when creating pool with MaxWorkers=20", func() {
		It("should initialize with maxWorkers=20", func() {
			profile := ServerProfile{MaxWorkers: 20, MinWorkers: 5}
			pool := NewGlobalWorkerPool(profile)
			Expect(pool.maxWorkers).To(Equal(20))
		})
	})

	Context("when creating pool with MaxWorkers < InitialWorkers", func() {
		It("should clamp currentWorkers to MaxWorkers", func() {
			profile := ServerProfile{MaxWorkers: 5} // InitialWorkers=10
			pool := NewGlobalWorkerPool(profile)
			Expect(pool.currentWorkers).To(Equal(5)) // Clamped
		})
	})

	Context("with profileAuto (MaxWorkers=5, MinWorkers=1)", func() {
		It("should clamp to 5 workers", func() {
			pool := NewGlobalWorkerPool(profileAuto)
			Expect(pool.currentWorkers).To(Equal(5))
			Expect(pool.maxWorkers).To(Equal(5))
		})
	})

	Context("with profileIgnition (MaxWorkers=20)", func() {
		It("should initialize with 10 workers", func() {
			pool := NewGlobalWorkerPool(profileIgnition)
			Expect(pool.currentWorkers).To(Equal(10)) // Not clamped
		})
	})

	Context("with zero MaxWorkers (unlimited)", func() {
		It("should not clamp", func() {
			profile := ServerProfile{MaxWorkers: 0}
			pool := NewGlobalWorkerPool(profile)
			Expect(pool.currentWorkers).To(Equal(InitialWorkers))
		})
	})

	Context("when MinWorkers > InitialWorkers", func() {
		It("should use MinWorkers as initial", func() {
			profile := ServerProfile{MinWorkers: 15, MaxWorkers: 20}
			pool := NewGlobalWorkerPool(profile)
			Expect(pool.currentWorkers).To(Equal(15))
		})
	})

	Context("when both MinWorkers and MaxWorkers constrain InitialWorkers", func() {
		It("should respect MaxWorkers as hardware limit", func() {
			// MinWorkers=3, InitialWorkers=10, MaxWorkers=8
			// Should clamp to MaxWorkers=8 (hardware limit wins)
			profile := ServerProfile{MinWorkers: 3, MaxWorkers: 8}
			pool := NewGlobalWorkerPool(profile)
			Expect(pool.currentWorkers).To(Equal(8))
		})
	})

	Context("checking all struct fields are initialized", func() {
		It("should initialize taskChan with correct buffer size", func() {
			profile := ServerProfile{MaxWorkers: 10}
			pool := NewGlobalWorkerPool(profile)
			Expect(pool.taskChan).NotTo(BeNil())
			Expect(cap(pool.taskChan)).To(Equal(MaxTagsToBrowse * 2)) // 200k buffer
		})

		It("should initialize workerControls map", func() {
			profile := ServerProfile{MaxWorkers: 10}
			pool := NewGlobalWorkerPool(profile)
			Expect(pool.workerControls).NotTo(BeNil())
		})

		It("should store minWorkers from profile", func() {
			profile := ServerProfile{MinWorkers: 3, MaxWorkers: 10}
			pool := NewGlobalWorkerPool(profile)
			Expect(pool.minWorkers).To(Equal(3))
		})
	})

	Describe("SpawnWorkers", func() {
		Context("when MaxWorkers limit is set", func() {
			It("should not spawn more workers than MaxWorkers", func() {
				profile := ServerProfile{MaxWorkers: 5, MinWorkers: 0}
				pool := NewGlobalWorkerPool(profile)
				// Pool starts with 5 workers (clamped to MaxWorkers)
				// Reset to 0 for this test
				pool.currentWorkers = 0
				pool.workerControls = make(map[uuid.UUID]chan struct{})

				spawned := pool.SpawnWorkers(10)

				Expect(spawned).To(Equal(5)) // Only 5 allowed
				Expect(pool.currentWorkers).To(Equal(5))
				Expect(len(pool.workerControls)).To(Equal(5))
			})

			It("should respect cumulative limit across multiple calls", func() {
				profile := ServerProfile{MaxWorkers: 10, MinWorkers: 0}
				pool := NewGlobalWorkerPool(profile)
				// Reset to 0 for clean test
				pool.currentWorkers = 0
				pool.workerControls = make(map[uuid.UUID]chan struct{})

				// First spawn: should get 5
				spawned1 := pool.SpawnWorkers(5)
				Expect(spawned1).To(Equal(5))
				Expect(pool.currentWorkers).To(Equal(5))

				// Second spawn: should get 5 more
				spawned2 := pool.SpawnWorkers(5)
				Expect(spawned2).To(Equal(5))
				Expect(pool.currentWorkers).To(Equal(10))

				// Third spawn: should get 0 (already at limit)
				spawned3 := pool.SpawnWorkers(5)
				Expect(spawned3).To(Equal(0))
				Expect(pool.currentWorkers).To(Equal(10))
			})
		})

		Context("when MaxWorkers is unlimited (0)", func() {
			It("should spawn all requested workers", func() {
				profile := ServerProfile{MaxWorkers: 0, MinWorkers: 0}
				pool := NewGlobalWorkerPool(profile)
				// Reset to 0 for clean test
				pool.currentWorkers = 0
				pool.workerControls = make(map[uuid.UUID]chan struct{})

				spawned := pool.SpawnWorkers(20)

				Expect(spawned).To(Equal(20))
				Expect(pool.currentWorkers).To(Equal(20))
				Expect(len(pool.workerControls)).To(Equal(20))
			})
		})

		Context("worker registration in workerControls", func() {
			It("should register workers with unique UUIDs", func() {
				profile := ServerProfile{MaxWorkers: 10, MinWorkers: 0}
				pool := NewGlobalWorkerPool(profile)
				// Reset to 0 for clean test
				pool.currentWorkers = 0
				pool.workerControls = make(map[uuid.UUID]chan struct{})

				spawned := pool.SpawnWorkers(3)

				Expect(spawned).To(Equal(3))
				Expect(len(pool.workerControls)).To(Equal(3))

				// Verify each UUID is unique
				seenUUIDs := make(map[string]bool)
				for workerID, controlChan := range pool.workerControls {
					Expect(seenUUIDs[workerID.String()]).To(BeFalse(), "UUID should be unique")
					seenUUIDs[workerID.String()] = true
					Expect(controlChan).NotTo(BeNil(), "Control channel should not be nil")
				}
			})
		})

		Context("thread safety", func() {
			It("should handle concurrent spawning without race conditions", func() {
				profile := ServerProfile{MaxWorkers: 20, MinWorkers: 0}
				pool := NewGlobalWorkerPool(profile)
				// Reset to 0 for clean test
				pool.currentWorkers = 0
				pool.workerControls = make(map[uuid.UUID]chan struct{})

				done := make(chan bool)
				totalSpawned := 0
				var mu sync.Mutex

				// Spawn workers concurrently from 5 goroutines
				for i := 0; i < 5; i++ {
					go func() {
						spawned := pool.SpawnWorkers(2)
						mu.Lock()
						totalSpawned += spawned
						mu.Unlock()
						done <- true
					}()
				}

				// Wait for all goroutines to complete
				for i := 0; i < 5; i++ {
					<-done
				}

				// Verify total spawned matches what was possible
				Expect(totalSpawned).To(Equal(10))
				Expect(pool.currentWorkers).To(Equal(10))
				Expect(len(pool.workerControls)).To(Equal(10))
			})
		})
	})
})
