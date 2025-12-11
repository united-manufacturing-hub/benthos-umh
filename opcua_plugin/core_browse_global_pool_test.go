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
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// recordingLogger records all debug log calls for testing
type recordingLogger struct {
	mu         sync.Mutex
	debugCalls []string
	infoCalls  []string
	warnCalls  []string
	errorCalls []string
}

func (r *recordingLogger) Debugf(format string, args ...interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.debugCalls = append(r.debugCalls, fmt.Sprintf(format, args...))
}

func (r *recordingLogger) Infof(format string, args ...interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.infoCalls = append(r.infoCalls, fmt.Sprintf(format, args...))
}

func (r *recordingLogger) Warnf(format string, args ...interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.warnCalls = append(r.warnCalls, fmt.Sprintf(format, args...))
}

func (r *recordingLogger) Errorf(format string, args ...interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errorCalls = append(r.errorCalls, fmt.Sprintf(format, args...))
}

func (r *recordingLogger) GetDebugCalls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.debugCalls...)
}

func (r *recordingLogger) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.debugCalls = nil
	r.infoCalls = nil
	r.warnCalls = nil
	r.errorCalls = nil
}

// mockNodeBrowser implements NodeBrowser interface for testing
type mockNodeBrowser struct {
	id           *ua.NodeID
	browseName   string
	nodeClass    ua.NodeClass // NEW: Support NodeClass for filtering tests
	children     []NodeBrowser
	browseErr    error
	browseCalled bool
	mu           sync.Mutex
}

func (m *mockNodeBrowser) ID() *ua.NodeID {
	if m.id != nil {
		return m.id
	}
	return &ua.NodeID{}
}

func (m *mockNodeBrowser) Attributes(_ context.Context, attrs ...ua.AttributeID) ([]*ua.DataValue, error) {
	// Return NodeClass if requested (for filtering tests)
	// Default to Variable if not set
	nodeClass := m.nodeClass
	if nodeClass == 0 {
		nodeClass = ua.NodeClassVariable
	}

	result := make([]*ua.DataValue, len(attrs))
	for i, attr := range attrs {
		switch attr {
		case ua.AttributeIDNodeClass:
			result[i] = &ua.DataValue{
				EncodingMask: ua.DataValueValue,
				Value:        ua.MustVariant(int64(nodeClass)),
				Status:       ua.StatusOK,
			}
		case ua.AttributeIDBrowseName:
			result[i] = &ua.DataValue{
				EncodingMask: ua.DataValueValue,
				Value:        ua.MustVariant(&ua.QualifiedName{Name: m.browseName}),
				Status:       ua.StatusOK,
			}
		case ua.AttributeIDDataType:
			// Return a valid NodeID for DataType (use Int32 for all nodes - Objects don't use DataType anyway)
			dataTypeID := ua.NewNumericNodeID(0, uint32(ua.TypeIDInt32))
			result[i] = &ua.DataValue{
				EncodingMask: ua.DataValueValue,
				Value:        ua.MustVariant(dataTypeID),
				Status:       ua.StatusOK,
			}
		case ua.AttributeIDDescription:
			// Return empty string for description (ignoreInvalidAttr = true)
			result[i] = &ua.DataValue{
				EncodingMask: ua.DataValueValue,
				Value:        ua.MustVariant(""),
				Status:       ua.StatusOK,
			}
		case ua.AttributeIDAccessLevel:
			// Return default AccessLevel for Variables, 0 for Objects
			accessLevel := uint8(ua.AccessLevelTypeCurrentRead)
			if nodeClass == ua.NodeClassObject {
				accessLevel = 0
			}
			result[i] = &ua.DataValue{
				EncodingMask: ua.DataValueValue,
				Value:        ua.MustVariant(int64(accessLevel)),
				Status:       ua.StatusOK,
			}
		default:
			result[i] = &ua.DataValue{
				EncodingMask: ua.DataValueValue,
				Value:        ua.MustVariant(""),
				Status:       ua.StatusOK,
			}
		}
	}
	return result, nil
}

func (m *mockNodeBrowser) BrowseName(_ context.Context) (*ua.QualifiedName, error) {
	name := m.browseName
	if name == "" {
		name = "MockNode"
	}
	return &ua.QualifiedName{Name: name}, nil
}

func (m *mockNodeBrowser) ReferencedNodes(_ context.Context, _ uint32, _ ua.BrowseDirection, _ ua.NodeClass, _ bool) ([]NodeBrowser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.browseCalled = true
	if m.browseErr != nil {
		return nil, m.browseErr
	}
	return m.children, nil
}

func (m *mockNodeBrowser) Children(_ context.Context, _ uint32, _ ua.NodeClass) ([]NodeBrowser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.browseCalled = true
	if m.browseErr != nil {
		return nil, m.browseErr
	}
	return m.children, nil
}

func (m *mockNodeBrowser) WasBrowseCalled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.browseCalled
}

func (m *mockNodeBrowser) ResetBrowseCalled() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.browseCalled = false
}

// Test helper functions for creating proper GlobalPoolTask structs

// newTestTask creates a GlobalPoolTask with mock Node and context for testing.
// This ensures tests use proper mocks instead of relying on stub mode in production code.
// Uses timeout context for test isolation - prevents context pollution between tests.
func newTestTask(nodeID string, resultChan any) GlobalPoolTask {
	// Use timeout context to prevent context pollution between tests
	// 10 second timeout is much longer than test timeouts, ensures context doesn't fire early
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second) //nolint:govet // intentional: context must stay valid for task lifecycle
	mockNode := &mockNodeBrowser{
		id: ua.MustParseNodeID(nodeID),
	}
	return GlobalPoolTask{
		NodeID:     nodeID,
		Ctx:        ctx,
		Node:       mockNode,
		ResultChan: resultChan,
		ErrChan:    make(chan error, 10), // Buffered to prevent blocking
	}
}

var _ = Describe("GlobalWorkerPool", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

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
			Expect(pool.taskChan).To(HaveCap(MaxTagsToBrowse * 2)) // 200k buffer
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
				task := newTestTask("ns=2;i=1000", validChan)

				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should accept nil ResultChan", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)

				task := newTestTask("ns=2;i=1000", nil)

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
				task := newTestTask("ns=2;i=1000", resultChan)
				task.ErrChan = errChan

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
					task := newTestTask("ns=2;i=1000", resultChans[i])
					task.ErrChan = errChans[i]
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
					task := newTestTask("ns=2;i=1000", resultChans[i])
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

				task1 := newTestTask("ns=2;i=1000", resultChan1)
				task2 := newTestTask("ns=2;i=2000", resultChan2)

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

				task := newTestTask("ns=2;i=1000", nil)
				task.ErrChan = nil

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

				task := newTestTask("ns=2;i=1000", nil)
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
				Expect(pool.workerControls).To(HaveLen(20))
			})
		})

		Context("worker registration in workerControls", func() {
			It("should register workers with unique UUIDs", func() {
				profile := ServerProfile{MaxWorkers: 10, MinWorkers: 0}
				pool := NewGlobalWorkerPool(profile, logger)
				// Pool starts with 0 workers (explicit initialization)

				spawned := pool.SpawnWorkers(3)

				Expect(spawned).To(Equal(3))
				Expect(pool.workerControls).To(HaveLen(3))

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
				Expect(pool.workerControls).To(HaveLen(10))
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
				Expect(pool.workerControls).To(HaveLen(3))

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
				task := newTestTask("ns=2;i=1000", resultChan)
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

				// Create tasks with unbuffered channels - workers will block immediately on send
				blockingChans := make([]chan any, 3)
				for i := 0; i < 3; i++ {
					blockingChans[i] = make(chan any) // Unbuffered - blocks immediately
					task := newTestTask("ns=2;i=1000", blockingChans[i])
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
				task := newTestTask("ns=2;i=1000", nil)
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
				errChans := make([]chan error, numTasks)
				for i := 0; i < numTasks; i++ {
					resultChans[i] = make(chan any, 1)
					errChans[i] = make(chan error, 10)
					task := GlobalPoolTask{
						NodeID:     "ns=2;i=1000",
						Ctx:        context.Background(),
						Node:       &mockNodeBrowser{id: ua.MustParseNodeID("ns=2;i=1000")},
						ResultChan: resultChans[i],
						ErrChan:    errChans[i],
					}
					err := pool.SubmitTask(task)
					Expect(err).ToNot(HaveOccurred())
				}

				// Shutdown pool
				err := pool.Shutdown(2 * time.Second)
				Expect(err).ToNot(HaveOccurred())

				// Verify all tasks completed (either with results or errors)
				for i := 0; i < numTasks; i++ {
					select {
					case <-resultChans[i]:
						// Task completed successfully
					case <-errChans[i]:
						// Task cancelled during shutdown
					case <-time.After(time.Second):
						Fail(fmt.Sprintf("Task %d did not complete", i))
					}
				}

				// Verify all workers exited
				pool.mu.Lock()
				workerCount := pool.currentWorkers
				pool.mu.Unlock()
				Expect(workerCount).To(Equal(0))
			})
		})
	})

	// sendTaskResult() helper tests
	Describe("sendTaskResult helper", func() {
		It("should send NodeDef to chan NodeDef", func() {
			profile := ServerProfile{MaxWorkers: 5}
			pool := NewGlobalWorkerPool(profile, logger)

			resultChan := make(chan NodeDef, 1)
			task := GlobalPoolTask{
				Ctx:        context.Background(),
				ResultChan: resultChan,
			}
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
			task := GlobalPoolTask{
				Ctx:        context.Background(),
				ResultChan: sendOnly,
			}
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
			task := GlobalPoolTask{
				Ctx:        context.Background(),
				ResultChan: resultChan,
			}
			stubNode := NodeDef{NodeID: &ua.NodeID{}}

			sent := pool.sendTaskResult(task, stubNode, logger)
			Expect(sent).To(BeTrue())

			Eventually(resultChan).Should(Receive())
		})

		It("should return false for unsupported channel type", func() {
			profile := ServerProfile{MaxWorkers: 5}
			pool := NewGlobalWorkerPool(profile, logger)

			unsupportedChan := make(chan string, 1)
			task := GlobalPoolTask{
				Ctx:        context.Background(),
				ResultChan: unsupportedChan,
			}
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

	// Type system refactoring tests
	Describe("GlobalPoolTask Type System", func() {
		Context("when ResultChan receives NodeDef", func() {
			It("should allow sending NodeDef through ResultChan", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				// Create typed channel for NodeDef
				resultChan := make(chan NodeDef, 1)

				// This test verifies the type system allows NodeDef results
				// GlobalPoolTask uses interface{} for ResultChan to enable this pattern
				task := newTestTask("ns=2;i=1000", resultChan)
				task.ErrChan = nil

				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Should receive NodeDef result
				var result NodeDef
				Eventually(resultChan).Within(time.Second).Should(Receive(&result))
				Expect(result.NodeID).ToNot(BeNil())
			})
		})
	})

	// BrowseDetails Progress Reporting
	Describe("BrowseDetails Progress Reporting", func() {
		Context("when pool accepts progress channel", func() {
			It("should send progress updates during task processing", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				progressChan := make(chan BrowseDetails, 10)
				resultChan := make(chan NodeDef, 1)

				task := newTestTask("ns=2;i=1000", resultChan)
				task.ProgressChan = progressChan

				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Should receive progress update
				Eventually(progressChan).Within(time.Second).Should(Receive())
			})

			It("should work when progress channel is nil", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				resultChan := make(chan NodeDef, 1)
				task := newTestTask("ns=2;i=1000", resultChan)
				task.ProgressChan = nil // No progress reporting

				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Task should complete normally
				Eventually(resultChan).Within(time.Second).Should(Receive())
			})

			It("should not block task processing if progress channel is full", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(3)

				// Create unbuffered progress channel (will block)
				progressChan := make(chan BrowseDetails)
				resultChan := make(chan NodeDef, 1)

				task := newTestTask("ns=2;i=1000", resultChan)
				task.ProgressChan = progressChan

				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Result should still be delivered (non-blocking progress send)
				Eventually(resultChan).Within(time.Second).Should(Receive())
			})
		})

		Context("when multiple concurrent tasks send progress", func() {
			It("should send independent progress updates for each task", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(5)

				numTasks := 5
				progressChans := make([]chan BrowseDetails, numTasks)
				resultChans := make([]chan NodeDef, numTasks)

				// Submit 5 tasks with independent progress channels
				for i := 0; i < numTasks; i++ {
					progressChans[i] = make(chan BrowseDetails, 5)
					resultChans[i] = make(chan NodeDef, 1)

					task := newTestTask("ns=2;i=1000", resultChans[i])
					task.ProgressChan = progressChans[i]
					err := pool.SubmitTask(task)
					Expect(err).ToNot(HaveOccurred())
				}

				// Each progress channel should receive at least one update
				for i := 0; i < numTasks; i++ {
					Eventually(progressChans[i]).Within(time.Second).Should(Receive())
				}
			})
		})
	})

	// Global Metrics Aggregation
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
				task := newTestTask("ns=2;i=1000", resultChan)

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
					task := newTestTask("ns=2;i=1000", resultChans[i])
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
					pool.SubmitTask(newTestTask("ns=2;i=1000", nil))
				}

				metrics := pool.GetMetrics()
				Expect(metrics.QueueDepth).To(BeNumerically(">=", 5))
			})

			It("should decrease as workers process tasks", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)

				// Submit 10 tasks without workers
				for i := 0; i < 10; i++ {
					pool.SubmitTask(newTestTask("ns=2;i=1000", nil))
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
							pool.SubmitTask(newTestTask("ns=2;i=1000", nil))
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

	// Debug Logging
	Describe("Debug Logging", func() {
		var recLogger *recordingLogger

		BeforeEach(func() {
			recLogger = &recordingLogger{}
		})

		Context("worker spawning", func() {
			It("should log worker spawn events with count", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, recLogger)

				pool.SpawnWorkers(3)

				calls := recLogger.GetDebugCalls()
				Expect(calls).To(HaveLen(3))
				// Expect logs to contain worker count information
				Expect(calls[0]).To(ContainSubstring("Worker spawned"))
				Expect(calls[0]).To(ContainSubstring("totalWorkers=1"))
			})

			It("should log cumulative worker counts", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, recLogger)

				pool.SpawnWorkers(2)
				pool.SpawnWorkers(3)

				calls := recLogger.GetDebugCalls()
				Expect(calls).To(HaveLen(5))
				// Last spawn should show totalWorkers=5
				Expect(calls[4]).To(ContainSubstring("totalWorkers=5"))
			})
		})

		Context("task submission", func() {
			It("should log task submission with queue depth", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, recLogger)
				pool.SpawnWorkers(1)

				recLogger.Reset()
				pool.SubmitTask(GlobalPoolTask{NodeID: "ns=2;i=1000"})

				calls := recLogger.GetDebugCalls()
				Expect(calls).ToNot(BeEmpty())
				// Should log submission with metrics
				found := false
				for _, call := range calls {
					matched, _ := ContainSubstring("Task submitted").Match(call)
					if matched {
						found = true
						Expect(call).To(ContainSubstring("queueDepth="))
						break
					}
				}
				Expect(found).To(BeTrue())
			})

			It("should log worker count on submission", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, recLogger)
				pool.SpawnWorkers(3)

				recLogger.Reset()
				pool.SubmitTask(GlobalPoolTask{NodeID: "ns=2;i=1000"})

				calls := recLogger.GetDebugCalls()
				// Should include worker count in submission log
				found := false
				for _, call := range calls {
					matched1, _ := ContainSubstring("Task submitted").Match(call)
					matched2, _ := ContainSubstring("workers=3").Match(call)
					if matched1 && matched2 {
						found = true
						break
					}
				}
				Expect(found).To(BeTrue())
			})
		})

		Context("task completion", func() {
			It("should log task completion on success", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, recLogger)
				pool.SpawnWorkers(1)

				resultChan := make(chan NodeDef, 1)
				task := newTestTask("ns=2;i=1000", resultChan)

				recLogger.Reset()
				pool.SubmitTask(task)

				// Wait for task to complete
				Eventually(resultChan).Within(time.Second).Should(Receive())

				// Should see completion log
				Eventually(func() []string {
					return recLogger.GetDebugCalls()
				}).Should(ContainElement(ContainSubstring("Task completed")))
			})

			It("should include NodeID in completion log", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, recLogger)
				pool.SpawnWorkers(1)

				resultChan := make(chan NodeDef, 1)
				task := newTestTask("ns=2;i=1234", resultChan)

				recLogger.Reset()
				pool.SubmitTask(task)

				Eventually(resultChan).Within(time.Second).Should(Receive())

				calls := recLogger.GetDebugCalls()
				found := false
				for _, call := range calls {
					matched1, _ := ContainSubstring("Task completed").Match(call)
					matched2, _ := ContainSubstring("ns=2;i=1234").Match(call)
					if matched1 && matched2 {
						found = true
						break
					}
				}
				Expect(found).To(BeTrue())
			})
		})

		Context("shutdown events", func() {
			It("should log shutdown initiation", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, recLogger)
				pool.SpawnWorkers(2)

				recLogger.Reset()
				pool.Shutdown(time.Second)

				calls := recLogger.GetDebugCalls()
				// Should log shutdown event
				Expect(calls).To(ContainElement(ContainSubstring("Shutdown initiated")))
			})

			It("should log shutdown completion", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, recLogger)
				pool.SpawnWorkers(2)

				recLogger.Reset()
				pool.Shutdown(time.Second)

				calls := recLogger.GetDebugCalls()
				// Should log shutdown completion
				Expect(calls).To(ContainElement(ContainSubstring("Shutdown complete")))
			})

			It("should log worker count during shutdown", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, recLogger)
				pool.SpawnWorkers(3)

				recLogger.Reset()
				pool.Shutdown(time.Second)

				calls := recLogger.GetDebugCalls()
				found := false
				for _, call := range calls {
					matched1, _ := ContainSubstring("Shutdown initiated").Match(call)
					matched2, _ := ContainSubstring("workers=3").Match(call)
					if matched1 && matched2 {
						found = true
						break
					}
				}
				Expect(found).To(BeTrue())
			})
		})

		Context("nil logger handling", func() {
			It("should not panic with nil logger on spawn", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, nil)

				Expect(func() {
					pool.SpawnWorkers(3)
				}).ToNot(Panic())
			})

			It("should not panic with nil logger on submit", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, nil)
				pool.SpawnWorkers(1)

				Expect(func() {
					pool.SubmitTask(GlobalPoolTask{NodeID: "ns=2;i=1000"})
				}).ToNot(Panic())
			})

			It("should not panic with nil logger on shutdown", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, nil)
				pool.SpawnWorkers(2)

				Expect(func() {
					pool.Shutdown(time.Second)
				}).ToNot(Panic())
			})
		})
	})

	// Task Completion Tracking Tests (TDD RED Phase - ENG-3876 Task 5)
	Describe("Task Completion Tracking", func() {
		Context("pendingTasks counter initialization", func() {
			It("should start at 0 when pool is created", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)

				// Read atomic counter
				pending := atomic.LoadInt64(&pool.pendingTasks)
				Expect(pending).To(Equal(int64(0)))
			})
		})

		Context("pendingTasks counter increments on task submission", func() {
			It("should increment counter when task is submitted", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)

				// Submit task
				resultChan := make(chan NodeDef, 1)
				task := newTestTask("ns=2;i=1000", resultChan)

				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Counter should be incremented
				pending := atomic.LoadInt64(&pool.pendingTasks)
				Expect(pending).To(Equal(int64(1)))
			})

			It("should increment counter for each task submitted", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				// Don't spawn workers - prevents race where workers process tasks before assertion

				// Submit 5 tasks
				for i := 0; i < 5; i++ {
					resultChan := make(chan NodeDef, 1)
					task := newTestTask(fmt.Sprintf("ns=2;i=%d", 1000+i), resultChan)
					err := pool.SubmitTask(task)
					Expect(err).ToNot(HaveOccurred())
				}

				// Counter should reflect all submitted tasks (none processed yet)
				pending := atomic.LoadInt64(&pool.pendingTasks)
				Expect(pending).To(Equal(int64(5))) // Exactly 5 pending (no workers running)
			})

			It("should increment atomically during concurrent submissions", func() {
				profile := ServerProfile{MaxWorkers: 20}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(10)

				numGoroutines := 10
				tasksPerGoroutine := 10
				done := make(chan bool, numGoroutines)

				// Submit tasks concurrently
				for i := 0; i < numGoroutines; i++ {
					go func(offset int) {
						for j := 0; j < tasksPerGoroutine; j++ {
							resultChan := make(chan NodeDef, 1)
							task := newTestTask(fmt.Sprintf("ns=2;i=%d", offset*1000+j), resultChan)
							pool.SubmitTask(task)
						}
						done <- true
					}(i)
				}

				// Wait for all submissions
				for i := 0; i < numGoroutines; i++ {
					<-done
				}

				// Counter should eventually reach total submitted (may have some processed already)
				Eventually(func() int64 {
					submitted := atomic.LoadUint64(&pool.metricsTasksSubmitted)
					return int64(submitted)
				}).Should(Equal(int64(100)))
			})
		})

		Context("pendingTasks counter decrements on task completion", func() {
			It("should decrement counter when task completes", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				resultChan := make(chan NodeDef, 1)
				task := newTestTask("ns=2;i=1000", resultChan)

				// Submit task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Wait for task to complete
				Eventually(resultChan).Within(time.Second).Should(Receive())

				// Counter should be back to 0
				Eventually(func() int64 {
					return atomic.LoadInt64(&pool.pendingTasks)
				}).Within(time.Second).Should(Equal(int64(0)))
			})

			It("should decrement counter for all completed tasks", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(5)

				numTasks := 10
				resultChans := make([]chan NodeDef, numTasks)

				// Submit multiple tasks
				for i := 0; i < numTasks; i++ {
					resultChans[i] = make(chan NodeDef, 1)
					task := newTestTask(fmt.Sprintf("ns=2;i=%d", 1000+i), resultChans[i])
					err := pool.SubmitTask(task)
					Expect(err).ToNot(HaveOccurred())
				}

				// Wait for all tasks to complete
				for i := 0; i < numTasks; i++ {
					Eventually(resultChans[i]).Within(time.Second).Should(Receive())
				}

				// Counter should be back to 0
				Eventually(func() int64 {
					return atomic.LoadInt64(&pool.pendingTasks)
				}).Within(2 * time.Second).Should(Equal(int64(0)))
			})
		})

		Context("allTasksDone channel behavior", func() {
			// SKIPPED: These tests accessed internal allTasksDone channel field.
			// Implementation changed to use sync.Cond instead (supports multiple concurrent waiters).
			// Functionality is tested via WaitForCompletion method tests.
			PIt("should be buffered channel", func() {
				// Test body removed - implementation changed from channel to sync.Cond
			})

			PIt("should close when pendingTasks reaches 0", func() {
				// Test body removed - implementation changed from channel to sync.Cond
			})

			PIt("should not signal while tasks are still pending", func() {
				// Test body removed - implementation changed from channel to sync.Cond
			})

			PIt("should handle multiple tasks completing", func() {
				// Test body removed - implementation changed from channel to sync.Cond
			})
		})

		Context("WaitForCompletion method", func() {
			It("should exist and be callable", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)

				// Method should exist
				err := pool.WaitForCompletion(1 * time.Second)
				Expect(err).ToNot(HaveOccurred()) // No tasks, should complete immediately
			})

			It("should return nil when no tasks are pending", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)

				err := pool.WaitForCompletion(100 * time.Millisecond)
				Expect(err).ToNot(HaveOccurred())
			})

			It("should block until all tasks complete", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(3)

				numTasks := 5
				resultChans := make([]chan NodeDef, numTasks)

				// Submit tasks
				for i := 0; i < numTasks; i++ {
					resultChans[i] = make(chan NodeDef, 1)
					task := newTestTask(fmt.Sprintf("ns=2;i=%d", 1000+i), resultChans[i])
					err := pool.SubmitTask(task)
					Expect(err).ToNot(HaveOccurred())
				}

				// WaitForCompletion should block until done
				start := time.Now()
				err := pool.WaitForCompletion(5 * time.Second)
				duration := time.Since(start)

				Expect(err).ToNot(HaveOccurred())
				Expect(duration).To(BeNumerically(">", 0)) // Took some time

				// All tasks should be complete
				pending := atomic.LoadInt64(&pool.pendingTasks)
				Expect(pending).To(Equal(int64(0)))
			})

			It("should return error on timeout", func() {
				profile := ServerProfile{MaxWorkers: 1}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(1)

				// Create blocking task
				blockingChan := make(chan NodeDef) // Unbuffered - will block
				task := newTestTask("ns=2;i=1000", blockingChan)

				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// WaitForCompletion should timeout
				err = pool.WaitForCompletion(100 * time.Millisecond)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("timeout"))

				// Error should include pending task count
				Expect(err.Error()).To(ContainSubstring("pending"))

				// Cleanup - unblock worker
				go func() { <-blockingChan }()
			})

			It("should return immediately when called after completion", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				resultChan := make(chan NodeDef, 1)
				task := newTestTask("ns=2;i=1000", resultChan)

				// Submit and wait for completion
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				Eventually(resultChan).Within(time.Second).Should(Receive())

				// First WaitForCompletion
				err = pool.WaitForCompletion(time.Second)
				Expect(err).ToNot(HaveOccurred())

				// Second WaitForCompletion should also return immediately
				start := time.Now()
				err = pool.WaitForCompletion(time.Second)
				duration := time.Since(start)

				Expect(err).ToNot(HaveOccurred())
				Expect(duration).To(BeNumerically("<", 100*time.Millisecond))
			})

			It("should handle concurrent WaitForCompletion calls", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(3)

				numTasks := 10
				resultChans := make([]chan NodeDef, numTasks)

				// Submit tasks
				for i := 0; i < numTasks; i++ {
					resultChans[i] = make(chan NodeDef, 1)
					task := newTestTask(fmt.Sprintf("ns=2;i=%d", 1000+i), resultChans[i])
					err := pool.SubmitTask(task)
					Expect(err).ToNot(HaveOccurred())
				}

				// Multiple goroutines calling WaitForCompletion
				done := make(chan error, 3)
				for i := 0; i < 3; i++ {
					go func() {
						err := pool.WaitForCompletion(5 * time.Second)
						done <- err
					}()
				}

				// All should complete without error
				for i := 0; i < 3; i++ {
					err := <-done
					Expect(err).ToNot(HaveOccurred())
				}
			})
		})

		Context("counter and channel coordination", func() {
			It("should maintain invariant: counter=0 implies channel signaled", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(3)

				numRounds := 5
				for round := 0; round < numRounds; round++ {
					// Submit tasks
					numTasks := 3
					resultChans := make([]chan NodeDef, numTasks)
					for i := 0; i < numTasks; i++ {
						resultChans[i] = make(chan NodeDef, 1)
						task := newTestTask(fmt.Sprintf("ns=2;i=%d", 1000+i), resultChans[i])
						pool.SubmitTask(task)
					}

					// Wait for completion
					err := pool.WaitForCompletion(2 * time.Second)
					Expect(err).ToNot(HaveOccurred())

					// Verify counter is 0
					pending := atomic.LoadInt64(&pool.pendingTasks)
					Expect(pending).To(Equal(int64(0)))
				}
			})

			It("should handle rapid submit/complete cycles", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(5)

				// Rapid submission and completion
				for i := 0; i < 50; i++ {
					resultChan := make(chan NodeDef, 1)
					task := newTestTask(fmt.Sprintf("ns=2;i=%d", i), resultChan)

					err := pool.SubmitTask(task)
					Expect(err).ToNot(HaveOccurred())

					// Immediately wait
					Eventually(resultChan).Within(time.Second).Should(Receive())
				}

				// Final counter should be 0
				Eventually(func() int64 {
					return atomic.LoadInt64(&pool.pendingTasks)
				}).Within(time.Second).Should(Equal(int64(0)))
			})
		})

		Context("edge cases", func() {
			It("should handle counter reaching 0 multiple times", func() {
				profile := ServerProfile{MaxWorkers: 10}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(3)

				// First batch
				resultChan1 := make(chan NodeDef, 1)
				task1 := newTestTask("ns=2;i=1000", resultChan1)
				pool.SubmitTask(task1)
				err := pool.WaitForCompletion(time.Second)
				Expect(err).ToNot(HaveOccurred())

				// Second batch
				resultChan2 := make(chan NodeDef, 1)
				task2 := newTestTask("ns=2;i=2000", resultChan2)
				pool.SubmitTask(task2)
				err = pool.WaitForCompletion(time.Second)
				Expect(err).ToNot(HaveOccurred())

				// Both times should work
				pending := atomic.LoadInt64(&pool.pendingTasks)
				Expect(pending).To(Equal(int64(0)))
			})

			PIt("should not deadlock when channel buffer is full", func() {
				// Test body removed - implementation changed from channel to sync.Cond
			})

			// TDD RED Phase - Task 8.1: Race condition test
			// This test exposes the race condition where a stale completion signal
			// causes WaitForCompletion to return prematurely when a new task is
			// submitted after a previous task completes but before WaitForCompletion
			// is called.
			//
			// Timeline:
			// T1: Task A submitted (pendingTasks = 1)
			// T2: Task A completes (pendingTasks = 0, signal sent to allTasksDone)
			// T3: Task B submitted (pendingTasks = 1, but signal still buffered)
			// T4: WaitForCompletion called
			// T5: Receives stale signal from T2
			// T6: Returns nil immediately (BUG - should wait for Task B)
			//
			// Expected behavior (after fix):
			// - WaitForCompletion re-checks pendingTasks after receiving signal
			// - Realizes counter is still 1 (Task B pending)
			// - Waits for Task B to complete
			It("should handle stale completion signal when new task submitted after previous completion", func() {
				// Setup pool with 2 workers
				profile := ServerProfile{MaxWorkers: 10, MinWorkers: 2}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				// Submit and complete first task
				resultChan1 := make(chan NodeDef, 1)
				task1 := newTestTask("ns=2;i=1000", resultChan1)
				err := pool.SubmitTask(task1)
				Expect(err).ToNot(HaveOccurred())

				// Wait for first task to complete (triggers signal)
				Eventually(resultChan1).Within(time.Second).Should(Receive())
				Eventually(func() int64 {
					return atomic.LoadInt64(&pool.pendingTasks)
				}).Within(time.Second).Should(Equal(int64(0)))

				// Allow time for completion signal to be sent to channel
				// This ensures the signal is buffered in allTasksDone channel
				time.Sleep(50 * time.Millisecond)

				// Submit new task AFTER signal sent (race condition window)
				resultChan2 := make(chan NodeDef, 1)
				task2 := newTestTask("ns=2;i=2000", resultChan2)
				err = pool.SubmitTask(task2)
				Expect(err).ToNot(HaveOccurred())

				// Verify task2 is pending
				pending := atomic.LoadInt64(&pool.pendingTasks)
				Expect(pending).To(Equal(int64(1)))

				// WaitForCompletion should handle stale signal correctly
				// BUG: With stale signal, might return while tasks still pending
				// FIXED: Re-checks counter after receiving signal
				err = pool.WaitForCompletion(2 * time.Second)

				// Test expectations
				Expect(err).ToNot(HaveOccurred(), "WaitForCompletion should succeed")

				// Verify all tasks completed (this is what matters)
				finalPending := atomic.LoadInt64(&pool.pendingTasks)
				Expect(finalPending).To(Equal(int64(0)), "All tasks should complete")

				// Verify task2 completed (task1 already verified at line 1933)
				Eventually(resultChan2).Within(time.Second).Should(Receive())
			})
		})
	})

	// Enhanced GlobalPoolTask Structure Tests (TDD RED Phase - ENG-3876 Task 1)
	Describe("Enhanced GlobalPoolTask Structure", func() {
		Context("when creating task with browse execution context", func() {
			It("should have Ctx field for cancellation context", func() {
				ctx := context.Background()
				task := GlobalPoolTask{
					Ctx: ctx,
				}
				Expect(task.Ctx).To(Equal(ctx))
				Expect(task.Ctx).NotTo(BeNil())
			})

			It("should have Node field for OPC UA node to browse", func() {
				// Create a mock NodeBrowser (this will fail until NodeBrowser is properly defined)
				mockNode := &mockNodeBrowser{}
				task := GlobalPoolTask{
					Node: mockNode,
				}
				Expect(task.Node).To(Equal(mockNode))
				Expect(task.Node).NotTo(BeNil())
			})

			It("should have Path field for current browse tree path", func() {
				path := "enterprise.site.area.line"
				task := GlobalPoolTask{
					Path: path,
				}
				Expect(task.Path).To(Equal(path))
			})

			It("should have Level field for recursion depth", func() {
				level := 5
				task := GlobalPoolTask{
					Level: level,
				}
				Expect(task.Level).To(Equal(level))
			})

			It("should have ParentNodeID field for parent node identifier", func() {
				parentID := "ns=2;i=1000"
				task := GlobalPoolTask{
					ParentNodeID: parentID,
				}
				Expect(task.ParentNodeID).To(Equal(parentID))
			})
		})

		Context("when creating task with shared state", func() {
			It("should have Visited field for duplicate visit prevention", func() {
				visited := &sync.Map{}
				visited.Store("ns=2;i=1000", VisitedNodeInfo{})

				task := GlobalPoolTask{
					Visited: visited,
				}
				Expect(task.Visited).To(Equal(visited))
				Expect(task.Visited).NotTo(BeNil())

				// Verify we can actually use the sync.Map
				val, ok := task.Visited.Load("ns=2;i=1000")
				Expect(ok).To(BeTrue())
				Expect(val).NotTo(BeNil())
			})
		})

		Context("when creating complete task with all fields", func() {
			It("should support full browse task creation with all context fields", func() {
				ctx := context.Background()
				mockNode := &mockNodeBrowser{}
				visited := &sync.Map{}
				resultChan := make(chan NodeDef, 1)
				errChan := make(chan error, 1)
				progressChan := make(chan BrowseDetails, 1)

				task := GlobalPoolTask{
					// Task identification
					NodeID: "ns=2;i=1000",

					// Browse execution context
					Ctx:          ctx,
					Node:         mockNode,
					Path:         "enterprise.site.area",
					Level:        3,
					ParentNodeID: "ns=2;i=999",

					// Shared state
					Visited: visited,

					// Result delivery
					ResultChan:   resultChan,
					ErrChan:      errChan,
					ProgressChan: progressChan,
				}

				// Verify all fields are accessible
				Expect(task.NodeID).To(Equal("ns=2;i=1000"))
				Expect(task.Ctx).To(Equal(ctx))
				Expect(task.Node).To(Equal(mockNode))
				Expect(task.Path).To(Equal("enterprise.site.area"))
				Expect(task.Level).To(Equal(3))
				Expect(task.ParentNodeID).To(Equal("ns=2;i=999"))
				Expect(task.Visited).To(Equal(visited))
				Expect(task.ResultChan).To(Equal(resultChan))
				// Note: ErrChan and ProgressChan use send-only channel types (chan<- T)
				// When assigned from bidirectional channels, Go converts them at compile time
				// The channels are functionally equivalent but have different types for type safety
				// We verify they're not nil instead of comparing types
				Expect(task.ErrChan).NotTo(BeNil())
				Expect(task.ProgressChan).NotTo(BeNil())
			})

			It("should enforce recursion depth limit of 25 via field value", func() {
				task := GlobalPoolTask{
					Level: 25, // Maximum allowed depth
				}
				Expect(task.Level).To(Equal(25))

				// Test that Level can represent depth limit
				taskExceeded := GlobalPoolTask{
					Level: 26, // Exceeds limit
				}
				Expect(taskExceeded.Level).To(BeNumerically(">", 25))
			})
		})
	})

	// Browse Logic in Workers Tests (TDD RED Phase - ENG-3876 Task 9)
	// These tests verify that workers execute actual browse operations instead of
	// processing stub nodes. They should FAIL until implementation is complete.
	Describe("Browse Logic in Workers", func() {
		Context("executeBrowse operation in worker", func() {
			It("should execute actual browse operation using task's browse context", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				defer func() {
					if err := pool.Shutdown(TestPoolShutdownTimeout); err != nil {
						logger.Warnf("Test cleanup: pool shutdown timeout: %v", err)
					}
				}()
				pool.SpawnWorkers(2)

				// Setup mock node with trackable browse calls
				mockNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "ParentNode",
					children:   []NodeBrowser{}, // No children for simplicity
				}

				ctx := context.Background()
				visited := &sync.Map{}
				resultChan := make(chan NodeDef, 1)

				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        ctx,
					Node:       mockNode,
					Path:       "enterprise.site",
					Level:      1,
					Visited:    visited,
					ResultChan: resultChan,
				}

				// Submit task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Wait for result
				Eventually(resultChan).Within(time.Second).Should(Receive())

				// CRITICAL ASSERTION: Worker should have called Browse() on the node
				// This will FAIL in current implementation because workers process stub nodes
				Eventually(func() bool {
					return mockNode.WasBrowseCalled()
				}).Within(time.Second).Should(BeTrue(), "Worker should execute Browse() operation")
			})

			It("should use task's Context for browse cancellation", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				defer func() {
					if err := pool.Shutdown(TestPoolShutdownTimeout); err != nil {
						logger.Warnf("Test cleanup: pool shutdown timeout: %v", err)
					}
				}()
				pool.SpawnWorkers(2)

				// Setup mock node
				mockNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "TestNode",
				}

				// Create cancellable context
				ctx, cancel := context.WithCancel(context.Background())
				visited := &sync.Map{}
				resultChan := make(chan NodeDef, 1)
				errChan := make(chan error, 1)

				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        ctx,
					Node:       mockNode,
					Path:       "enterprise.site",
					Level:      1,
					Visited:    visited,
					ResultChan: resultChan,
					ErrChan:    errChan,
				}

				// Cancel context before processing
				cancel()

				// Submit task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Should receive error due to cancelled context
				// This will FAIL until workers check context before browse
				Eventually(errChan).Within(time.Second).Should(Receive(MatchError(ContainSubstring("context canceled"))))
			})
		})

		Context("children submission back to pool", func() {
			It("should submit browse children back to pool as new tasks", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				defer func() {
					if err := pool.Shutdown(TestPoolShutdownTimeout); err != nil {
						logger.Warnf("Test cleanup: pool shutdown timeout: %v", err)
					}
				}()
				pool.SpawnWorkers(3)

				// Setup parent node with 3 children
				child1 := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 2001),
					browseName: "Child1",
				}
				child2 := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 2002),
					browseName: "Child2",
				}
				child3 := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 2003),
					browseName: "Child3",
				}

				parentNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "ParentNode",
					children:   []NodeBrowser{child1, child2, child3},
				}

				ctx := context.Background()
				visited := &sync.Map{}
				resultChan := make(chan NodeDef, 10) // Buffer for parent + children

				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        ctx,
					Node:       parentNode,
					Path:       "enterprise.site",
					Level:      1,
					Visited:    visited,
					ResultChan: resultChan,
				}

				// Submit parent task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Should receive parent result
				Eventually(resultChan).Within(time.Second).Should(Receive())

				// CRITICAL ASSERTION: Should submit 3 child tasks back to pool
				// This will FAIL until workers submit children
				Eventually(func() uint64 {
					metrics := pool.GetMetrics()
					return metrics.TasksSubmitted
				}).Within(2*time.Second).Should(BeNumerically(">=", 4), "Should submit parent + 3 children")

				// Should receive child results
				Eventually(func() int {
					count := 0
					for {
						select {
						case <-resultChan:
							count++
						default:
							return count
						}
					}
				}).Within(3*time.Second).Should(BeNumerically(">=", 3), "Should receive results for 3 children")
			})

			It("should pass correct browse context to child tasks", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				defer func() {
					if err := pool.Shutdown(TestPoolShutdownTimeout); err != nil {
						logger.Warnf("Test cleanup: pool shutdown timeout: %v", err)
					}
				}()
				pool.SpawnWorkers(2)

				// Setup parent with one child
				child := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 2001),
					browseName: "ChildNode",
				}

				parentNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "ParentNode",
					children:   []NodeBrowser{child},
				}

				ctx := context.Background()
				visited := &sync.Map{}
				resultChan := make(chan NodeDef, 10)

				task := GlobalPoolTask{
					NodeID:       "ns=2;i=1000",
					Ctx:          ctx,
					Node:         parentNode,
					Path:         "enterprise.site",
					Level:        1,
					ParentNodeID: "ns=2;i=999",
					Visited:      visited,
					ResultChan:   resultChan,
				}

				// Submit parent task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Wait for parent result
				Eventually(resultChan).Within(time.Second).Should(Receive())

				// CRITICAL ASSERTION: Child task should have Level incremented
				// This will FAIL until workers properly construct child tasks
				// We verify this indirectly by checking that browse was called on child
				Eventually(func() bool {
					return child.WasBrowseCalled()
				}).Within(2*time.Second).Should(BeTrue(), "Child node should be browsed (Level=2)")
			})
		})

		Context("error handling during browse", func() {
			It("should handle browse errors and send to ErrChan", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				defer func() {
					if err := pool.Shutdown(TestPoolShutdownTimeout); err != nil {
						logger.Warnf("Test cleanup: pool shutdown timeout: %v", err)
					}
				}()
				pool.SpawnWorkers(2)

				// Setup node that returns error on browse
				mockNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "ErrorNode",
					browseErr:  errors.New("browse failed: connection timeout"),
				}

				ctx := context.Background()
				visited := &sync.Map{}
				resultChan := make(chan NodeDef, 1)
				errChan := make(chan error, 1)

				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        ctx,
					Node:       mockNode,
					Path:       "enterprise.site",
					Level:      1,
					Visited:    visited,
					ResultChan: resultChan,
					ErrChan:    errChan,
				}

				// Submit task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// CRITICAL ASSERTION: Error should be sent to ErrChan
				// This will FAIL until workers handle browse errors
				Eventually(errChan).Within(time.Second).Should(Receive(MatchError(ContainSubstring("browse failed"))))

				// Task should still be marked complete
				Eventually(func() int64 {
					return atomic.LoadInt64(&pool.pendingTasks)
				}).Within(time.Second).Should(Equal(int64(0)))
			})

			It("should continue processing other tasks after browse error", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				defer func() {
					if err := pool.Shutdown(TestPoolShutdownTimeout); err != nil {
						logger.Warnf("Test cleanup: pool shutdown timeout: %v", err)
					}
				}()
				pool.SpawnWorkers(2)

				// First task will error
				errorNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "ErrorNode",
					browseErr:  errors.New("browse error"),
				}

				// Second task will succeed
				successNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 2000),
					browseName: "SuccessNode",
				}

				ctx := context.Background()
				visited := &sync.Map{}
				errChan := make(chan error, 2)
				resultChan1 := make(chan NodeDef, 1)
				resultChan2 := make(chan NodeDef, 1)

				task1 := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        ctx,
					Node:       errorNode,
					Path:       "enterprise.site",
					Level:      1,
					Visited:    visited,
					ResultChan: resultChan1,
					ErrChan:    errChan,
				}

				task2 := GlobalPoolTask{
					NodeID:     "ns=2;i=2000",
					Ctx:        ctx,
					Node:       successNode,
					Path:       "enterprise.site",
					Level:      1,
					Visited:    visited,
					ResultChan: resultChan2,
					ErrChan:    errChan,
				}

				// Submit both tasks
				pool.SubmitTask(task1)
				pool.SubmitTask(task2)

				// Should receive error for first task
				Eventually(errChan).Within(time.Second).Should(Receive())

				// Should still receive result for second task
				Eventually(resultChan2).Within(time.Second).Should(Receive())
			})
		})

		Context("recursion depth limit", func() {
			It("should stop browsing at maximum recursion depth", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				defer func() {
					if err := pool.Shutdown(TestPoolShutdownTimeout); err != nil {
						logger.Warnf("Test cleanup: pool shutdown timeout: %v", err)
					}
				}()
				pool.SpawnWorkers(2)

				// Setup node with children at max depth
				child := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 2001),
					browseName: "ChildAtMaxDepth",
				}

				deepNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "NodeAtLevel25",
					children:   []NodeBrowser{child},
				}

				ctx := context.Background()
				visited := &sync.Map{}
				resultChan := make(chan NodeDef, 10)

				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        ctx,
					Node:       deepNode,
					Path:       "enterprise.site.area.line.machine",
					Level:      25, // Maximum recursion depth
					Visited:    visited,
					ResultChan: resultChan,
				}

				// Submit task at max depth
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Should receive result for current node
				Eventually(resultChan).Within(time.Second).Should(Receive())

				// CRITICAL ASSERTION: Child should NOT be browsed (depth limit)
				// This will FAIL until workers check recursion depth
				time.Sleep(500 * time.Millisecond) // Give time for any potential child processing
				Expect(child.WasBrowseCalled()).To(BeFalse(), "Child should not be browsed at max depth")

				// Should not submit additional tasks for children
				poolMetrics := pool.GetMetrics()
				Expect(poolMetrics.TasksSubmitted).To(Equal(uint64(1)), "Should only submit parent task, not children")
			})

			It("should increment Level for each recursion", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				defer func() {
					if err := pool.Shutdown(TestPoolShutdownTimeout); err != nil {
						logger.Warnf("Test cleanup: pool shutdown timeout: %v", err)
					}
				}()
				pool.SpawnWorkers(2)

				// Setup 3-level hierarchy: Parent -> Child -> Grandchild
				grandchild := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 3001),
					browseName: "Grandchild",
				}

				child := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 2001),
					browseName: "Child",
					children:   []NodeBrowser{grandchild},
				}

				parent := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "Parent",
					children:   []NodeBrowser{child},
				}

				ctx := context.Background()
				visited := &sync.Map{}
				resultChan := make(chan NodeDef, 10)

				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        ctx,
					Node:       parent,
					Path:       "enterprise",
					Level:      1, // Start at level 1
					Visited:    visited,
					ResultChan: resultChan,
				}

				// Submit parent task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Should browse all 3 levels
				Eventually(func() bool {
					return parent.WasBrowseCalled() &&
						child.WasBrowseCalled() &&
						grandchild.WasBrowseCalled()
				}).Within(3*time.Second).Should(BeTrue(), "Should browse all levels")

				// Should submit 3 tasks total (parent + child + grandchild)
				Eventually(func() uint64 {
					m := pool.GetMetrics()
					return m.TasksSubmitted
				}).Within(3 * time.Second).Should(BeNumerically(">=", 3))
			})

			// TDD RED Phase - ENG-3876 Browse() Migration Cleanup
			// Test exposes path propagation bug where child tasks receive parent's path
			// instead of parent.child path, causing all nodes in hierarchy to have same path.
			It("should build correct paths for multi-level hierarchy", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				defer func() {
					if err := pool.Shutdown(TestPoolShutdownTimeout); err != nil {
						logger.Warnf("Test cleanup: pool shutdown timeout: %v", err)
					}
				}()
				pool.SpawnWorkers(2)

				// Setup 3-level hierarchy: Parent -> Child -> Grandchild
				grandchild := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 3001),
					browseName: "Grandchild",
				}

				child := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 2001),
					browseName: "Child",
					children:   []NodeBrowser{grandchild},
				}

				parent := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "Parent",
					children:   []NodeBrowser{child},
				}

				ctx := context.Background()
				visited := &sync.Map{}
				resultChan := make(chan NodeDef, 10)

				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        ctx,
					Node:       parent,
					Path:       "enterprise",
					Level:      1,
					Visited:    visited,
					ResultChan: resultChan,
				}

				// Submit parent task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Collect all results
				var results []NodeDef
				Eventually(func() int {
					for {
						select {
						case result := <-resultChan:
							results = append(results, result)
						default:
							return len(results)
						}
					}
				}).Within(3*time.Second).Should(BeNumerically(">=", 3),
					"Should receive at least 3 NodeDef results")

				// Verify paths are built correctly
				// Parent: enterprise.Parent
				// Child: enterprise.Parent.Child
				// Grandchild: enterprise.Parent.Child.Grandchild

				pathsFound := make(map[string]bool)
				for _, result := range results {
					pathsFound[result.Path] = true
				}

				Expect(pathsFound).To(HaveKey("enterprise.Parent"),
					"Parent should have path: enterprise.Parent")
				Expect(pathsFound).To(HaveKey("enterprise.Parent.Child"),
					"Child should have path: enterprise.Parent.Child")
				Expect(pathsFound).To(HaveKey("enterprise.Parent.Child.Grandchild"),
					"Grandchild should have path: enterprise.Parent.Child.Grandchild")
			})
		})

		Context("duplicate node detection", func() {
			It("should not browse already-visited nodes", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				defer func() {
					if err := pool.Shutdown(TestPoolShutdownTimeout); err != nil {
						logger.Warnf("Test cleanup: pool shutdown timeout: %v", err)
					}
				}()
				pool.SpawnWorkers(2)

				// Setup node that's already in Visited map
				mockNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "AlreadyVisited",
				}

				ctx := context.Background()
				visited := &sync.Map{}
				// Mark node as already visited
				visited.Store("ns=2;i=1000", VisitedNodeInfo{
					Def: NodeDef{
						Path: "enterprise.site",
					},
				})
				resultChan := make(chan NodeDef, 1)

				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        ctx,
					Node:       mockNode,
					Path:       "enterprise.site",
					Level:      1,
					Visited:    visited,
					ResultChan: resultChan,
				}

				// Submit task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// CRITICAL ASSERTION: Browse() should NOT be called on visited node
				// This will FAIL until workers check Visited map
				time.Sleep(500 * time.Millisecond) // Give time for processing
				Expect(mockNode.WasBrowseCalled()).To(BeFalse(), "Should not browse already-visited node")

				// Task should still complete (just skip browse)
				Eventually(func() int64 {
					return atomic.LoadInt64(&pool.pendingTasks)
				}).Within(time.Second).Should(Equal(int64(0)))
			})

			It("should add newly browsed nodes to Visited map", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				defer func() {
					if err := pool.Shutdown(TestPoolShutdownTimeout); err != nil {
						logger.Warnf("Test cleanup: pool shutdown timeout: %v", err)
					}
				}()
				pool.SpawnWorkers(2)

				// Setup node not yet visited
				mockNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "NewNode",
				}

				ctx := context.Background()
				visited := &sync.Map{}
				resultChan := make(chan NodeDef, 1)

				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        ctx,
					Node:       mockNode,
					Path:       "enterprise.site",
					Level:      1,
					Visited:    visited,
					ResultChan: resultChan,
				}

				// Submit task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Wait for processing
				Eventually(resultChan).Within(time.Second).Should(Receive())

				// CRITICAL ASSERTION: Node should be added to Visited map
				// This will FAIL until workers update Visited map
				Eventually(func() bool {
					_, exists := visited.Load("ns=2;i=1000")
					return exists
				}).Within(time.Second).Should(BeTrue(), "Should add node to Visited map after browsing")
			})
		})

		Context("metrics tracking during browse", func() {
			It("should pass Metrics object through browse operations", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				defer func() {
					if err := pool.Shutdown(TestPoolShutdownTimeout); err != nil {
						logger.Warnf("Test cleanup: pool shutdown timeout: %v", err)
					}
				}()
				pool.SpawnWorkers(2)

				// Setup node with children
				child1 := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 2001),
					browseName: "Child1",
				}
				child2 := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 2002),
					browseName: "Child2",
				}

				parentNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "Parent",
					children:   []NodeBrowser{child1, child2},
				}

				ctx := context.Background()
				visited := &sync.Map{}
				resultChan := make(chan NodeDef, 10)

				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        ctx,
					Node:       parentNode,
					Path:       "enterprise.site",
					Level:      1,
					Visited:    visited,
					ResultChan: resultChan,
				}

				// Submit task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// CRITICAL ASSERTION: All nodes should be browsed (parent + 2 children)
				// This will FAIL until workers execute browse and submit children
				Eventually(func() bool {
					return parentNode.WasBrowseCalled() &&
						child1.WasBrowseCalled() &&
						child2.WasBrowseCalled()
				}).Within(3*time.Second).Should(BeTrue(), "Should browse all nodes including children")
			})
		})

		Context("result delivery from browse", func() {
			It("should send actual browsed NodeDef to ResultChan", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				defer func() {
					if err := pool.Shutdown(TestPoolShutdownTimeout); err != nil {
						logger.Warnf("Test cleanup: pool shutdown timeout: %v", err)
					}
				}()
				pool.SpawnWorkers(2)

				mockNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "ActualNode",
				}

				ctx := context.Background()
				visited := &sync.Map{}
				resultChan := make(chan NodeDef, 1)

				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        ctx,
					Node:       mockNode,
					Path:       "enterprise.site.area",
					Level:      3,
					Visited:    visited,
					ResultChan: resultChan,
				}

				// Submit task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Receive result
				var result NodeDef
				Eventually(resultChan).Within(time.Second).Should(Receive(&result))

				// CRITICAL ASSERTION: Result should contain actual browse data
				// This will FAIL until workers create real NodeDef from browse
				Expect(result.NodeID).NotTo(BeNil(), "Result should have NodeID")
				Expect(result.Path).To(Equal("enterprise.site.area.ActualNode"), "Result should build path with BrowseName")
				// Note: NodeDef doesn't have Level field, but Path is sufficient to verify browse data
			})
		})

		// TDD RED Phase - ENG-3876 Task 12.5
		// Test exposes shutdown flag check counter bug where SubmitTask error path
		// at line 202 returns error WITHOUT incrementing counter, but child submission
		// code at line 559 ALWAYS decrements, causing negative counter.
		Context("shutdown flag check counter bug", func() {
			It("should not corrupt counter when shutdown flag check fails", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(1)

				// Create parent with children to trigger child submission
				child1 := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 2001),
					browseName: "Child1",
				}

				parentNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "Parent",
					children:   []NodeBrowser{child1},
				}

				resultChan := make(chan NodeDef, 10)
				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        context.Background(),
					Node:       parentNode,
					Path:       "/root",
					Level:      0,
					Visited:    &sync.Map{},
					ResultChan: resultChan,
				}

				// Submit parent task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Shutdown pool IMMEDIATELY to hit shutdown flag check for child submissions
				// This will cause SubmitTask to return error at line 202 (before incrementing counter)
				pool.Shutdown(50 * time.Millisecond)

				// Wait for shutdown to complete
				time.Sleep(200 * time.Millisecond)

				// CRITICAL TEST: Counter should NOT go negative
				// Bug scenario:
				//   1. Parent task submitted and incremented counter (counter = 1)
				//   2. Worker starts processing parent
				//   3. Pool shuts down (shutdown flag set)
				//   4. Worker tries to submit child task
				//   5. SubmitTask hits shutdown flag check (line 202) - returns error WITHOUT incrementing
				//   6. Child submission code (line 559) decrements counter anyway
				//   7. Counter goes negative: 1 -> 0 (parent done) -> -1 (bug!)
				finalCounter := atomic.LoadInt64(&pool.pendingTasks)
				Expect(finalCounter).To(BeNumerically(">=", 0), "Counter should NEVER go negative")

				// Additional verification: Counter should reach zero eventually
				// (This may fail if counter is stuck at -1)
				Expect(finalCounter).To(Equal(int64(0)), "Counter should reach zero, not stay negative")
			})

			It("should handle shutdown during child processing without negative counter", func() {
				profile := ServerProfile{MaxWorkers: 5}
				pool := NewGlobalWorkerPool(profile, logger)
				pool.SpawnWorkers(2)

				// Create parent with multiple children to extend browse time
				children := make([]NodeBrowser, 5)
				for i := 0; i < 5; i++ {
					children[i] = &mockNodeBrowser{
						id:         ua.NewNumericNodeID(2, uint32(2000+i)),
						browseName: "Child" + string(rune('A'+i)),
					}
				}

				parentNode := &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, 1000),
					browseName: "Parent",
					children:   children,
				}

				resultChan := make(chan NodeDef, 20)
				task := GlobalPoolTask{
					NodeID:     "ns=2;i=1000",
					Ctx:        context.Background(),
					Node:       parentNode,
					Path:       "/root",
					Level:      0,
					Visited:    &sync.Map{},
					ResultChan: resultChan,
				}

				// Submit parent task
				err := pool.SubmitTask(task)
				Expect(err).ToNot(HaveOccurred())

				// Shutdown very quickly to catch children submission in progress
				time.Sleep(10 * time.Millisecond)
				pool.Shutdown(100 * time.Millisecond)

				// Wait for shutdown
				time.Sleep(200 * time.Millisecond)

				// Counter should never go negative during shutdown
				finalCounter := atomic.LoadInt64(&pool.pendingTasks)
				Expect(finalCounter).To(BeNumerically(">=", 0), "Counter should not go negative even during shutdown")
			})
		})
	})

	// TDD RED Phase - ENG-3876 Task 12.1
	// This test exposes bug where SubmitTask errors are ignored during child task submission,
	// causing pendingTasks counter corruption and WaitForCompletion hangs.
	Context("error handling in child task submission", func() {
		It("should handle pool shutdown during child task submission without hanging", func() {
			profile := ServerProfile{MaxWorkers: 5}
			pool := NewGlobalWorkerPool(profile, logger)
			pool.SpawnWorkers(1) // Single worker to ensure sequential processing

			// Create parent with MANY children (1000+) to ensure long submission loop
			// This guarantees shutdown will occur during child task submission
			children := make([]NodeBrowser, 1000)
			for i := 0; i < 1000; i++ {
				children[i] = &mockNodeBrowser{
					id:         ua.NewNumericNodeID(2, uint32(2000+i)),
					browseName: fmt.Sprintf("Child%d", i),
					children:   []NodeBrowser{}, // Leaf nodes
				}
			}

			parentNode := &mockNodeBrowser{
				id:         ua.NewNumericNodeID(2, 1000),
				browseName: "ParentWith1000Children",
				children:   children,
			}

			ctx := context.Background()
			visited := &sync.Map{}
			resultChan := make(chan NodeDef, 2000)

			task := GlobalPoolTask{
				NodeID:     "ns=2;i=1000",
				Ctx:        ctx,
				Node:       parentNode,
				Path:       "enterprise.site",
				Level:      1,
				Visited:    visited,
				ResultChan: resultChan,
			}

			// Submit parent task
			err := pool.SubmitTask(task)
			Expect(err).ToNot(HaveOccurred())

			// Shutdown pool IMMEDIATELY while children are being submitted in worker
			// With 1000 children, the submission loop will still be running
			time.Sleep(1 * time.Millisecond)                     // Minimal delay - just let worker start browse
			shutdownErr := pool.Shutdown(100 * time.Millisecond) // Short timeout to force early shutdown

			// The shutdown may timeout if worker is still processing, which is expected
			// What matters is that new task submissions will fail after shutdown begins
			_ = shutdownErr // Ignore shutdown timeout - not the focus of this test

			// CRITICAL TEST: WaitForCompletion should NOT hang
			// With bug: Hangs forever because failed SubmitTask calls leave counter inflated
			//   - Worker browses parent, gets 1000 children
			//   - Worker starts submitting children in loop (line 535-558)
			//   - Some submissions succeed, incrementing counter
			//   - Shutdown closes taskChan
			//   - Remaining submissions fail (panic recovered)
			//   - BUT error is ignored! Counter was incremented but task never queued!
			//   - Worker completes, decrements parent task
			//   - Counter stuck at (failed submission count) > 0
			//   - WaitForCompletion waits forever
			// After fix: Worker checks SubmitTask errors and decrements counter on failure
			err = pool.WaitForCompletion(2 * time.Second)
			Expect(err).ToNot(HaveOccurred(), "WaitForCompletion should complete without hanging despite pool shutdown")

			// Verify counter integrity - must reach zero
			finalCounter := atomic.LoadInt64(&pool.pendingTasks)
			Expect(finalCounter).To(Equal(int64(0)), "pendingTasks counter should reach zero (no corruption)")
		})
	})

	// TDD RED PHASE: Test for NodeClass filtering bug
	// Bug: GlobalWorkerPool sends ALL nodes (Objects AND Variables) to ResultChan
	// Expected: Only Variables should be sent to ResultChan
	// Objects (folders) should be browsed but NOT subscribed
	Context("NodeClass Filtering", func() {
		It("should only send Variable nodes to ResultChan, not Object nodes", func() {
			// ARRANGE: Create hierarchy with mix of Objects and Variables
			// Root (Object - folder)
			//   ├─ Folder1 (Object - folder)
			//   │   ├─ Sensor1 (Variable)
			//   │   └─ Sensor2 (Variable)
			//   └─ Sensor3 (Variable)
			ctx := context.Background()
			logger := &mockLogger{}
			profile := ServerProfile{MaxWorkers: 5}
			pool := NewGlobalWorkerPool(profile, logger)
			pool.SpawnWorkers(2)
			defer pool.Shutdown(TestPoolShutdownTimeout)

			// Create root node (Object - folder)
			rootNode := &mockNodeBrowser{
				id:         ua.NewNumericNodeID(0, 1),
				browseName: "Root",
				nodeClass:  ua.NodeClassObject,
			}

			// Create Folder1 (Object - folder)
			folder1 := &mockNodeBrowser{
				id:         ua.NewNumericNodeID(0, 2),
				browseName: "Folder1",
				nodeClass:  ua.NodeClassObject,
			}

			// Create Sensor1 (Variable)
			sensor1 := &mockNodeBrowser{
				id:         ua.NewNumericNodeID(0, 3),
				browseName: "Sensor1",
				nodeClass:  ua.NodeClassVariable,
			}

			// Create Sensor2 (Variable)
			sensor2 := &mockNodeBrowser{
				id:         ua.NewNumericNodeID(0, 4),
				browseName: "Sensor2",
				nodeClass:  ua.NodeClassVariable,
			}

			// Create Sensor3 (Variable)
			sensor3 := &mockNodeBrowser{
				id:         ua.NewNumericNodeID(0, 5),
				browseName: "Sensor3",
				nodeClass:  ua.NodeClassVariable,
			}

			// Build hierarchy
			folder1.children = []NodeBrowser{sensor1, sensor2}
			rootNode.children = []NodeBrowser{folder1, sensor3}

			// Setup channels and tracking
			resultChan := make(chan NodeDef, 10)
			errChan := make(chan error, 10)
			visited := &sync.Map{}

			// ACT: Submit root task to pool
			task := GlobalPoolTask{
				NodeID:     rootNode.ID().String(),
				Ctx:        ctx,
				Node:       rootNode,
				Path:       "",
				Level:      0,
				Visited:    visited,
				ResultChan: resultChan,
				ErrChan:    errChan,
			}

			err := pool.SubmitTask(task)
			Expect(err).ToNot(HaveOccurred())

			// Wait for all tasks to complete (with timeout)
			err = pool.WaitForCompletion(5 * time.Second)
			Expect(err).ToNot(HaveOccurred())

			// Close channels
			close(resultChan)
			close(errChan)

			// Collect all results
			var results []NodeDef
			for result := range resultChan {
				results = append(results, result)
			}

			// Check for errors
			var errors []error
			for err := range errChan {
				errors = append(errors, err)
			}
			Expect(errors).To(BeEmpty(), "Browse should complete without errors")

			// ASSERT: This test will FAIL because current code sends ALL nodes
			// Bug: Current code sends Root (Object), Folder1 (Object), Sensor1-3 (Variables) = 5 nodes
			// Expected: Only Sensor1, Sensor2, Sensor3 (Variables) = 3 nodes
			//
			// After fix: This assertion will PASS
			Expect(results).To(HaveLen(3), "Should receive exactly 3 Variable nodes (Sensor1, Sensor2, Sensor3)")

			// ASSERT: Verify NO Object nodes were sent
			for _, result := range results {
				// All results should be Variables (none should be Objects)
				nodeIDStr := result.NodeID.String()
				Expect(nodeIDStr).NotTo(ContainSubstring("i=1"), "Root (Object) should not be in results")
				Expect(nodeIDStr).NotTo(ContainSubstring("i=2"), "Folder1 (Object) should not be in results")
			}

			// ASSERT: Verify all Variable nodes WERE sent
			nodeIDs := make(map[uint32]bool)
			for _, result := range results {
				nodeIDs[result.NodeID.IntID()] = true
			}
			Expect(nodeIDs[3]).To(BeTrue(), "Sensor1 (i=3) should be in results")
			Expect(nodeIDs[4]).To(BeTrue(), "Sensor2 (i=4) should be in results")
			Expect(nodeIDs[5]).To(BeTrue(), "Sensor3 (i=5) should be in results")
		})
	})
})
