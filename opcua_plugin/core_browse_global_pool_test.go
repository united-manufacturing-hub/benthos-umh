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
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("GlobalWorkerPool", func() {
	var logger *mockLogger

	BeforeEach(func() {
		logger = &mockLogger{}
	})

	Context("when creating pool with MaxWorkers=20", func() {
		It("should initialize with maxWorkers=20", func() {
			profile := ServerProfile{MaxWorkers: 20, MinWorkers: 5}
			pool := NewGlobalWorkerPool(profile, logger)
			Expect(pool.maxWorkers).To(Equal(20))
		})
	})

	Context("when creating pool with MaxWorkers < InitialWorkers", func() {
		It("should start with 0 workers (caller must spawn)", func() {
			profile := ServerProfile{MaxWorkers: 5} // InitialWorkers=10
			pool := NewGlobalWorkerPool(profile, logger)
			Expect(pool.currentWorkers).To(Equal(0)) // Starts at 0
			Expect(pool.maxWorkers).To(Equal(5))     // Limit stored
		})
	})

	Context("with profileAuto (MaxWorkers=5, MinWorkers=1)", func() {
		It("should store limits but start with 0 workers", func() {
			pool := NewGlobalWorkerPool(profileAuto, logger)
			Expect(pool.currentWorkers).To(Equal(0))
			Expect(pool.maxWorkers).To(Equal(5))
			Expect(pool.minWorkers).To(Equal(1))
		})
	})

	Context("with profileIgnition (MaxWorkers=20)", func() {
		It("should start with 0 workers", func() {
			pool := NewGlobalWorkerPool(profileIgnition, logger)
			Expect(pool.currentWorkers).To(Equal(0))
			Expect(pool.maxWorkers).To(Equal(20))
		})
	})

	Context("with zero MaxWorkers (unlimited)", func() {
		It("should start with 0 workers and no limit", func() {
			profile := ServerProfile{MaxWorkers: 0}
			pool := NewGlobalWorkerPool(profile, logger)
			Expect(pool.currentWorkers).To(Equal(0))
			Expect(pool.maxWorkers).To(Equal(0)) // Unlimited
		})
	})

	Context("when MinWorkers > 0", func() {
		It("should store MinWorkers but not spawn workers", func() {
			profile := ServerProfile{MinWorkers: 15, MaxWorkers: 20}
			pool := NewGlobalWorkerPool(profile, logger)
			Expect(pool.currentWorkers).To(Equal(0))
			Expect(pool.minWorkers).To(Equal(15))
			Expect(pool.maxWorkers).To(Equal(20))
		})
	})

	Context("when both MinWorkers and MaxWorkers are set", func() {
		It("should store both limits", func() {
			profile := ServerProfile{MinWorkers: 3, MaxWorkers: 8}
			pool := NewGlobalWorkerPool(profile, logger)
			Expect(pool.currentWorkers).To(Equal(0))
			Expect(pool.minWorkers).To(Equal(3))
			Expect(pool.maxWorkers).To(Equal(8))
		})
	})

	Context("checking all struct fields are initialized", func() {
		It("should initialize taskChan with correct buffer size", func() {
			profile := ServerProfile{MaxWorkers: 10}
			pool := NewGlobalWorkerPool(profile, logger)
			Expect(pool.taskChan).NotTo(BeNil())
			Expect(cap(pool.taskChan)).To(Equal(MaxTagsToBrowse * 2)) // 200k buffer
		})

		It("should initialize workerControls map", func() {
			profile := ServerProfile{MaxWorkers: 10}
			pool := NewGlobalWorkerPool(profile, logger)
			Expect(pool.workerControls).NotTo(BeNil())
		})

		It("should store minWorkers from profile", func() {
			profile := ServerProfile{MinWorkers: 3, MaxWorkers: 10}
			pool := NewGlobalWorkerPool(profile, logger)
			Expect(pool.minWorkers).To(Equal(3))
		})
	})

	Describe("SubmitTask", func() {
		Context("runtime validation", func() {
			It("should reject non-channel ResultChan types", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)

				// Try to submit with int instead of channel (invalid)
				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					ResultChan: 42, // Invalid: int not channel
				}

				err := pool.SubmitTask(task)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("expected channel"))
			})

			It("should accept valid chan NodeDef", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(1)

				validChan := make(chan NodeDef, 1)
				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					ResultChan: validChan,
				}

				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should accept nil ResultChan", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)

				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					ResultChan: nil, // Nil is valid (fire-and-forget)
				}

				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when pool is running", func() {
			It("should queue and process task successfully", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(3)

				resultChan := make(chan any, 1)
				errChan := make(chan error, 1)
				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					ResultChan: resultChan,
					ErrChan:    errChan,
				}

				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Wait for result (with timeout)
				Eventually(resultChan).Within(time.Second).Should(Receive())
			})

			It("should handle multiple tasks concurrently", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(3)

				numTasks := 10
				resultChans := make([]chan any, numTasks)
				errChans := make([]chan error, numTasks)

				// Submit 10 tasks
				for i := 0; i < numTasks; i++ {
					resultChans[i] = make(chan any, 1)
					errChans[i] = make(chan error, 1)
					task := GlobalPoolTask{
						NodeID:     "ns=2;i=1000",
						ResultChan: resultChans[i],
						ErrChan:    errChans[i],
					}
					err := pool.SubmitTask(task)
					Expect(err).ToNot(HaveOccurred())
				}

				// Verify all results received
				for i := 0; i < numTasks; i++ {
					Eventually(resultChans[i]).Within(time.Second).Should(Receive())
				}
			})

			It("should not block when submitting 1000 tasks (buffer test)", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(5)

				numTasks := 1000
				resultChans := make([]chan any, numTasks)

				start := time.Now()
				// Submit 1000 tasks rapidly - should not block
				for i := 0; i < numTasks; i++ {
					resultChans[i] = make(chan any, 1)
					task := GlobalPoolTask{
						NodeID:     "ns=2;i=1000",
						ResultChan: resultChans[i],
					}
					err := pool.SubmitTask(task)
					Expect(err).ToNot(HaveOccurred())
				}
				duration := time.Since(start)

				// Submission should be instant (buffered channel)
				Expect(duration).To(BeNumerically("<", 100*time.Millisecond))

				// Verify all tasks eventually processed
				for i := 0; i < numTasks; i++ {
					Eventually(resultChans[i]).Within(10 * time.Second).Should(Receive())
				}
			})

			It("should send results to correct channels", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				resultChan1 := make(chan any, 1)
				resultChan2 := make(chan any, 1)

				task1 := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					ResultChan: resultChan1,
				}
				task2 := GlobalPoolTask{
					NodeID:     "ns=2;i=2000",
					ResultChan: resultChan2,
				}

				err := pool.SubmitTask(task1)
				Expect(err).ToNot(HaveOccurred())
				err = pool.SubmitTask(task2)
				Expect(err).ToNot(HaveOccurred())

				// Both channels should receive results
				Eventually(resultChan1).Within(time.Second).Should(Receive())
				Eventually(resultChan2).Within(time.Second).Should(Receive())
			})

			It("should handle nil result channels gracefully", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					ResultChan: nil, // No result channel
					ErrChan:    nil, // No error channel
				}

				// Should not panic or error
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Give worker time to process (no way to verify completion without channel)
				time.Sleep(100 * time.Millisecond)
			})
		})

		Context("when pool is shutdown", func() {
			It("should return error", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)

				// Simulate shutdown by closing taskChan
				close(pool.taskChan)

				task := GlobalPoolTask{NodeID: "ns=2;i=1000"}
				err := pool.SubmitTask(task)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("shutdown"))
			})
		})
	})

	Describe("SpawnWorkers", func() {
		Context("when MaxWorkers limit is set", func() {
			It("should not spawn more workers than MaxWorkers", func() {
				profile := ServerProfile{MaxWorkers: 5, MinWorkers: 0}
				pool := NewGlobalWorkerPool(profile, logger)
				// Pool starts with 0 workers (explicit initialization)

				spawned := pool.SpawnWorkers(10)

				Expect(spawned).To(Equal(5)) // Only 5 allowed due to MaxWorkers limit

				// Verify currentWorkers matches
				pool.mu.Lock()
				actualCount := pool.currentWorkers
				pool.mu.Unlock()
				Expect(actualCount).To(Equal(5))
			})

			It("should respect cumulative limit across multiple calls", func() {
				profile := ServerProfile{MaxWorkers: 10, MinWorkers: 0}
				pool := NewGlobalWorkerPool(profile, logger)
				// Pool starts with 0 workers (explicit initialization)

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
				pool := NewGlobalWorkerPool(profile, logger)
				// Pool starts with 0 workers (explicit initialization)

				spawned := pool.SpawnWorkers(20)

				Expect(spawned).To(Equal(20))
				Expect(pool.currentWorkers).To(Equal(20))
				Expect(len(pool.workerControls)).To(Equal(20))
			})
		})

		Context("worker registration in workerControls", func() {
			It("should register workers with unique UUIDs", func() {
				profile := ServerProfile{MaxWorkers: 10, MinWorkers: 0}
				pool := NewGlobalWorkerPool(profile, logger)
				// Pool starts with 0 workers (explicit initialization)

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
				pool := NewGlobalWorkerPool(profile, logger)
				// Pool starts with 0 workers (explicit initialization)

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

		Context("when worker exits via shutdown signal", func() {
			It("should decrement currentWorkers and remove from workerControls", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				// Pool starts with 0 workers (explicit initialization)

				// Spawn 3 workers
				spawned := pool.SpawnWorkers(3)
				Expect(spawned).To(Equal(3))
				Expect(pool.currentWorkers).To(Equal(3))
				Expect(len(pool.workerControls)).To(Equal(3))

				// Get one worker's control channel
				pool.mu.Lock()
				var targetWorkerID uuid.UUID
				var targetControlChan chan struct{}
				for id, ch := range pool.workerControls {
					targetWorkerID = id
					targetControlChan = ch
					break
				}
				pool.mu.Unlock()

				// Signal shutdown to one worker
				close(targetControlChan)

				// Give worker time to exit and clean up
				Eventually(func() int {
					pool.mu.Lock()
					defer pool.mu.Unlock()
					return pool.currentWorkers
				}).Within(1 * time.Second).Should(Equal(2)) // Should drop from 3 to 2

				// Verify worker removed from map
				pool.mu.Lock()
				_, exists := pool.workerControls[targetWorkerID]
				pool.mu.Unlock()
				Expect(exists).To(BeFalse())
			})
		})
	})

	Describe("Profile Tracking", func() {
		Context("when pool is created", func() {
			It("should store the profile", func() {
				profile := ServerProfile{MaxWorkers: 10, MinWorkers: 2}
				pool := NewGlobalWorkerPool(profile, logger)

				storedProfile := pool.Profile()
				Expect(storedProfile.MaxWorkers).To(Equal(10))
				Expect(storedProfile.MinWorkers).To(Equal(2))
			})
		})

		Context("when multiple pools created with different profiles", func() {
			It("should track profiles independently", func() {
				profile1 := ServerProfile{MaxWorkers: 10, MinWorkers: 2}
				profile2 := ServerProfile{MaxWorkers: 64, MinWorkers: 5}

				pool1 := NewGlobalWorkerPool(profile1, logger)
				pool2 := NewGlobalWorkerPool(profile2, logger)

				Expect(pool1.Profile().MaxWorkers).To(Equal(10))
				Expect(pool2.Profile().MaxWorkers).To(Equal(64))
			})
		})
	})

	Describe("Shutdown", func() {
		Context("when pool has no workers", func() {
			It("should shutdown immediately", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)

				err := pool.Shutdown(time.Second)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when pool has active workers", func() {
			It("should wait for workers to finish current tasks", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(3)

				// Submit a task that will be processed
				resultChan := make(chan any, 1)
				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					ResultChan: resultChan,
				}
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Shutdown should wait for workers to exit
				err = pool.Shutdown(time.Second)
				Expect(err).ToNot(HaveOccurred())

				// Verify all workers exited
				pool.mu.Lock()
				workerCount := pool.currentWorkers
				pool.mu.Unlock()
				Expect(workerCount).To(Equal(0))
			})

			It("should return error if timeout exceeded", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(3)

				// Create tasks with blocking result channels to prevent workers from completing
				blockingChans := make([]chan any, 3)
				for i := 0; i < 3; i++ {
					blockingChans[i] = make(chan any) // Unbuffered - blocks on send
					task := GlobalPoolTask{
						NodeID:     "ns=2;i=1000",
						ResultChan: blockingChans[i],
					}
					err := pool.SubmitTask(task)
					Expect(err).ToNot(HaveOccurred())
				}

				// Give workers time to pick up tasks and block on send
				time.Sleep(100 * time.Millisecond)

				// Shutdown with very short timeout - workers will block trying to send results
				err := pool.Shutdown(1 * time.Millisecond)

				// Should timeout because workers are blocked
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("timeout"))

				// Clean up - unblock workers by reading from channels
				for i := 0; i < 3; i++ {
					select {
					case <-blockingChans[i]:
					case <-time.After(time.Second):
						// Worker might have already exited
					}
				}
			})
		})

		Context("idempotency", func() {
			It("should allow multiple Shutdown calls without error", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				// First shutdown
				err := pool.Shutdown(time.Second)
				Expect(err).ToNot(HaveOccurred())

				// Second shutdown (idempotent)
				err = pool.Shutdown(time.Second)
				Expect(err).ToNot(HaveOccurred())

				// Third shutdown (still idempotent)
				err = pool.Shutdown(time.Second)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("after shutdown", func() {
			It("should reject new task submissions", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				// Shutdown pool
				err := pool.Shutdown(time.Second)
				Expect(err).ToNot(HaveOccurred())

				// Try to submit task after shutdown
				task := GlobalPoolTask{NodeID: "ns=2;i=1000"}
				err = pool.SubmitTask(task)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("shutdown"))
			})

			It("should prevent new worker spawning", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				// Shutdown pool
				err := pool.Shutdown(time.Second)
				Expect(err).ToNot(HaveOccurred())

				// Try to spawn more workers after shutdown
				spawned := pool.SpawnWorkers(3)
				Expect(spawned).To(Equal(0))

				// Verify worker count didn't change
				pool.mu.Lock()
				workerCount := pool.currentWorkers
				pool.mu.Unlock()
				Expect(workerCount).To(Equal(0))
			})
		})

		Context("concurrent shutdown", func() {
			It("should handle multiple goroutines calling Shutdown", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(5)

				// Call Shutdown from 3 goroutines simultaneously
				done := make(chan error, 3)
				for i := 0; i < 3; i++ {
					go func() {
						err := pool.Shutdown(time.Second)
						done <- err
					}()
				}

				// All should complete without error
				for i := 0; i < 3; i++ {
					err := <-done
					Expect(err).ToNot(HaveOccurred())
				}

				// Verify all workers exited
				pool.mu.Lock()
				workerCount := pool.currentWorkers
				pool.mu.Unlock()
				Expect(workerCount).To(Equal(0))
			})
		})

		Context("workers finishing in-flight tasks", func() {
			It("should allow workers to complete current task before exit", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(3)

				// Submit multiple tasks
				numTasks := 5
				resultChans := make([]chan any, numTasks)
				for i := 0; i < numTasks; i++ {
					resultChans[i] = make(chan any, 1)
					task := GlobalPoolTask{
						NodeID:     "ns=2;i=1000",
						ResultChan: resultChans[i],
					}
					err := pool.SubmitTask(task)
					Expect(err).ToNot(HaveOccurred())
				}

				// Shutdown pool
				err := pool.Shutdown(2 * time.Second)
				Expect(err).ToNot(HaveOccurred())

				// Verify all tasks completed
				for i := 0; i < numTasks; i++ {
					Eventually(resultChans[i]).Within(time.Second).Should(Receive())
				}

				// Verify all workers exited
				pool.mu.Lock()
				workerCount := pool.currentWorkers
				pool.mu.Unlock()
				Expect(workerCount).To(Equal(0))
			})
		})
	})

	// Task 2.3: sendTaskResult() helper tests
	Describe("sendTaskResult helper", func() {
		It("should send NodeDef to chan NodeDef", func() {
			profile := ServerProfile{MaxWorkers: 5}
			pool := NewGlobalWorkerPool(profile, logger)

			resultChan := make(chan NodeDef, 1)
			task := GlobalPoolTask{ResultChan: resultChan}
			stubNode := NodeDef{NodeID: &ua.NodeID{}}

			sent := pool.sendTaskResult(task, stubNode, logger)
			Expect(sent).To(BeTrue())

			var result NodeDef
			Eventually(resultChan).Should(Receive(&result))
			Expect(result.NodeID).NotTo(BeNil())
		})

		It("should send NodeDef to chan<- NodeDef (send-only)", func() {
			profile := ServerProfile{MaxWorkers: 5}
			pool := NewGlobalWorkerPool(profile, logger)

			resultChan := make(chan NodeDef, 1)
			var sendOnly chan<- NodeDef = resultChan
			task := GlobalPoolTask{ResultChan: sendOnly}
			stubNode := NodeDef{NodeID: &ua.NodeID{}}

			sent := pool.sendTaskResult(task, stubNode, logger)
			Expect(sent).To(BeTrue())

			var result NodeDef
			Eventually(resultChan).Should(Receive(&result))
		})

		It("should handle backward compat chan any", func() {
			profile := ServerProfile{MaxWorkers: 5}
			pool := NewGlobalWorkerPool(profile, logger)

			resultChan := make(chan any, 1)
			task := GlobalPoolTask{ResultChan: resultChan}
			stubNode := NodeDef{NodeID: &ua.NodeID{}}

			sent := pool.sendTaskResult(task, stubNode, logger)
			Expect(sent).To(BeTrue())

			Eventually(resultChan).Should(Receive())
		})

		It("should return false for unsupported channel type", func() {
			profile := ServerProfile{MaxWorkers: 5}
			pool := NewGlobalWorkerPool(profile, logger)

			unsupportedChan := make(chan string, 1)
			task := GlobalPoolTask{ResultChan: unsupportedChan}
			stubNode := NodeDef{NodeID: &ua.NodeID{}}

			sent := pool.sendTaskResult(task, stubNode, logger)
			Expect(sent).To(BeFalse())
			// Note: Debug logging is tested implicitly - method should not panic
		})

		It("should return false when ResultChan is nil", func() {
			profile := ServerProfile{MaxWorkers: 5}
			pool := NewGlobalWorkerPool(profile, logger)

			task := GlobalPoolTask{ResultChan: nil}
			stubNode := NodeDef{NodeID: &ua.NodeID{}}

			sent := pool.sendTaskResult(task, stubNode, logger)
			Expect(sent).To(BeFalse())
		})
	})

	// Task 2.2: Type system refactoring tests
	Describe("Task 2.2: GlobalPoolTask Type System", func() {
		Context("when ResultChan receives NodeDef", func() {
			It("should allow sending NodeDef through ResultChan", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				// Create typed channel for NodeDef
				resultChan := make(chan NodeDef, 1)

				// This test verifies the type system allows NodeDef results
				// Task 2.2 will fix GlobalPoolTask to enable this pattern
				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					ResultChan: resultChan,
					ErrChan:    nil,
				}

				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Should receive NodeDef result
				var result NodeDef
				Eventually(resultChan).Within(time.Second).Should(Receive(&result))
				Expect(result.NodeID).ToNot(BeNil())
			})
		})
	})

	// Task 3.1: Global Metrics Aggregation
	Describe("GetMetrics", func() {
		Context("when tracking tasks submitted", func() {
			It("should increment tasksSubmitted counter", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)

				// Submit 3 tasks
				pool.SubmitTask(GlobalPoolTask{NodeID: "ns=2;i=1000"})
				pool.SubmitTask(GlobalPoolTask{NodeID: "ns=2;i=1001"})
				pool.SubmitTask(GlobalPoolTask{NodeID: "ns=2;i=1002"})

				metrics := pool.GetMetrics()
				Expect(metrics.TasksSubmitted).To(Equal(uint64(3)))
			})
		})

		Context("when tracking tasks completed", func() {
			It("should increment tasksCompleted counter on success", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(1)

				resultChan := make(chan NodeDef, 1)
				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					ResultChan: resultChan,
				}

				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Wait for task to complete
				Eventually(resultChan).Within(time.Second).Should(Receive())

				// Check metrics
				metrics := pool.GetMetrics()
				Expect(metrics.TasksCompleted).To(Equal(uint64(1)))
			})

			It("should track multiple completed tasks", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				numTasks := 5
				resultChans := make([]chan NodeDef, numTasks)

				// Submit 5 tasks
				for i := 0; i < numTasks; i++ {
					resultChans[i] = make(chan NodeDef, 1)
					task := GlobalPoolTask{
						NodeID:     "ns=2;i=1000",
						ResultChan: resultChans[i],
					}
					err := pool.SubmitTask(task)
					Expect(err).ToNot(HaveOccurred())
				}

				// Wait for all tasks to complete
				for i := 0; i < numTasks; i++ {
					Eventually(resultChans[i]).Within(time.Second).Should(Receive())
				}

				metrics := pool.GetMetrics()
				Expect(metrics.TasksCompleted).To(Equal(uint64(5)))
			})
		})

		Context("when tracking active workers", func() {
			It("should reflect current worker count", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)

				// Initially 0 workers
				metrics := pool.GetMetrics()
				Expect(metrics.ActiveWorkers).To(Equal(0))

				// Spawn 3 workers
				spawned := pool.SpawnWorkers(3)
				Expect(spawned).To(Equal(3))

				metrics = pool.GetMetrics()
				Expect(metrics.ActiveWorkers).To(Equal(3))

				// Spawn 2 more
				spawned = pool.SpawnWorkers(2)
				Expect(spawned).To(Equal(2))

				metrics = pool.GetMetrics()
				Expect(metrics.ActiveWorkers).To(Equal(5))
			})
		})

		Context("when tracking queue depth", func() {
			It("should reflect pending tasks in queue", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				// Don't spawn workers - tasks will queue up

				// Submit 5 tasks without workers to process them
				for i := 0; i < 5; i++ {
					pool.SubmitTask(GlobalPoolTask{NodeID: "ns=2;i=1000"})
				}

				metrics := pool.GetMetrics()
				Expect(metrics.QueueDepth).To(BeNumerically(">=", 5))
			})

			It("should decrease as workers process tasks", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)

				// Submit 10 tasks without workers
				for i := 0; i < 10; i++ {
					pool.SubmitTask(GlobalPoolTask{NodeID: "ns=2;i=1000"})
				}

				initialMetrics := pool.GetMetrics()
				initialDepth := initialMetrics.QueueDepth
				Expect(initialDepth).To(BeNumerically(">=", 10))

				// Spawn workers to drain queue
				pool.SpawnWorkers(3)

				// Queue should eventually drain
				Eventually(func() int {
					metrics := pool.GetMetrics()
					return metrics.QueueDepth
				}).Within(2 * time.Second).Should(BeNumerically("<", initialDepth))
			})
		})

		Context("concurrent access", func() {
			It("should handle concurrent GetMetrics calls without race", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(3)

				// Spawn 10 goroutines reading metrics concurrently
				done := make(chan bool, 10)
				for i := 0; i < 10; i++ {
					go func() {
						for j := 0; j < 100; j++ {
							_ = pool.GetMetrics()
						}
						done <- true
					}()
				}

				// Wait for all goroutines
				for i := 0; i < 10; i++ {
					<-done
				}
			})

			It("should handle concurrent task submission and metrics reads", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(5)

				done := make(chan bool, 20)

				// 10 goroutines submitting tasks
				for i := 0; i < 10; i++ {
					go func() {
						for j := 0; j < 50; j++ {
							pool.SubmitTask(GlobalPoolTask{NodeID: "ns=2;i=1000"})
						}
						done <- true
					}()
				}

				// 10 goroutines reading metrics
				for i := 0; i < 10; i++ {
					go func() {
						for j := 0; j < 50; j++ {
							_ = pool.GetMetrics()
						}
						done <- true
					}()
				}

				// Wait for all goroutines
				for i := 0; i < 20; i++ {
					<-done
				}

				// Final metrics should be consistent
				metrics := pool.GetMetrics()
				Expect(metrics.TasksSubmitted).To(Equal(uint64(500)))
			})
		})

		Context("metrics consistency", func() {
			It("should maintain consistent snapshot across fields", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				// Submit some tasks
				for i := 0; i < 10; i++ {
					pool.SubmitTask(GlobalPoolTask{NodeID: "ns=2;i=1000"})
				}

				// Get metrics - should be consistent snapshot
				metrics := pool.GetMetrics()

				// TasksSubmitted should be >= TasksCompleted (logical invariant)
				Expect(metrics.TasksSubmitted).To(BeNumerically(">=", metrics.TasksCompleted))

				// ActiveWorkers should match what we spawned
				Expect(metrics.ActiveWorkers).To(Equal(2))

				// QueueDepth + TasksCompleted should be <= TasksSubmitted
				total := uint64(metrics.QueueDepth) + metrics.TasksCompleted
				Expect(total).To(BeNumerically("<=", metrics.TasksSubmitted))
			})
		})
	})
})
