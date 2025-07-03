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

package topic_browser_plugin

import (
	"sync"
	"sync/atomic"
	"time"
)

// Hardcoded constants for CPU-aware controller (as per plan requirements)
const (
	// EMA smoothing factor for bytes tracking (0.2 = responsive but stable)
	emaAlpha = 0.2

	// Emit interval bounds
	minEmitInterval = 1 * time.Second  // Fast emissions for large payloads
	maxEmitInterval = 15 * time.Second // Slow emissions for small payloads

	// CPU target and thresholds
	cpuTargetPercent = 85.0 // Target CPU usage (85%)
	cpuHighThreshold = 90.0 // High CPU threshold (90%)
	cpuLowThreshold  = 70.0 // Low CPU threshold (70%)

	// Payload size thresholds for different emission strategies
	smallPayloadBytes  = 1024  // < 1KB = small payload
	mediumPayloadBytes = 10240 // < 10KB = medium payload
	largePayloadBytes  = 51200 // < 50KB = large payload

	// CPU sampling interval (200ms as per plan)
	cpuSampleInterval = 200 * time.Millisecond

	// Maximum interval change per adjustment (prevents sudden jumps)
	maxIntervalChange = 2 * time.Second
)

// AdaptiveController implements CPU-aware emit interval adaptation.
// This controller uses CPU load and payload size patterns to intelligently
// adjust emit intervals for optimal performance without exceeding CPU limits.
type AdaptiveController struct {
	mu               sync.Mutex
	nextFlush        time.Time                     // Next scheduled flush time
	interval         atomic.Pointer[time.Duration] // Current emit interval (time.Duration)
	emaBytes         float64                       // EMA-tracked payload size
	cpuMeter         *CPUMeter                     // CPU usage monitor
	lastCPUSample    time.Time                     // Last CPU sampling time
	lastPayloadBytes int                           // Last payload size
}

// NewAdaptiveController creates a new CPU-aware adaptive controller.
func NewAdaptiveController(initialInterval time.Duration) *AdaptiveController {
	ctrl := &AdaptiveController{
		cpuMeter: NewCPUMeter(emaAlpha),
		emaBytes: 0.0,
	}

	// Store initial interval atomically
	ctrl.interval.Store(&initialInterval)
	ctrl.nextFlush = time.Now().Add(initialInterval)

	return ctrl
}

// OnBatch is called for each ProcessBatch invocation to update the controller state.
// This is the message-driven algorithm that samples CPU periodically and adapts
// the emit interval based on both CPU load and payload patterns.
func (c *AdaptiveController) OnBatch(payloadBytes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastPayloadBytes = payloadBytes

	now := time.Now()

	// Update EMA for payload size tracking
	if c.emaBytes == 0.0 {
		c.emaBytes = float64(payloadBytes) // Initialize on first call
	} else {
		c.emaBytes = emaAlpha*float64(payloadBytes) + (1-emaAlpha)*c.emaBytes
	}

	// Sample CPU every 200ms (not on every message to avoid overhead)
	if now.Sub(c.lastCPUSample) >= cpuSampleInterval {
		c.lastCPUSample = now
		c.updateInterval()
	}
}

// ShouldFlush returns true if it's time to flush based on the current adaptive interval.
func (c *AdaptiveController) ShouldFlush() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return time.Now().After(c.nextFlush)
}

// OnFlush should be called after a successful flush to update the next flush time.
func (c *AdaptiveController) OnFlush() {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentInterval := *c.interval.Load()
	c.nextFlush = time.Now().Add(currentInterval)
}

// GetCurrentInterval returns the current adaptive emit interval.
func (c *AdaptiveController) GetCurrentInterval() time.Duration {
	return *c.interval.Load()
}

// GetCPUPercent returns the current CPU usage percentage.
func (c *AdaptiveController) GetCPUPercent() float64 {
	return c.cpuMeter.GetCPUPercent()
}

// GetEMABytes returns the current exponential moving average of payload bytes.
func (c *AdaptiveController) GetEMABytes() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.emaBytes
}

// GetLastPayloadBytes returns the last payload size.
func (c *AdaptiveController) GetLastPayloadBytes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastPayloadBytes
}

// updateInterval implements the core adaptive algorithm based on CPU load and payload patterns.
// This is called periodically (every 200ms) to adjust the emit interval.
func (c *AdaptiveController) updateInterval() {
	cpuPercent := c.cpuMeter.GetCPUPercent()
	currentInterval := *c.interval.Load()

	// Determine target interval based on payload size and CPU load
	var targetInterval time.Duration

	// Payload-based base interval (larger payloads = faster emission for better compression)
	baseInterval := c.calculateBaseInterval()

	// CPU-based adjustment
	if cpuPercent > cpuHighThreshold {
		// High CPU: slow down emissions to reduce processing load
		targetInterval = baseInterval + 3*time.Second
	} else if cpuPercent < cpuLowThreshold {
		// Low CPU: speed up emissions for better responsiveness
		targetInterval = baseInterval - 2*time.Second
	} else {
		// Medium CPU: use base interval with minor adjustments
		cpuFactor := (cpuPercent - cpuTargetPercent) / 10.0 // ±1.5 range for ±15% CPU
		adjustment := time.Duration(cpuFactor * float64(time.Second))
		targetInterval = baseInterval + adjustment
	}

	// Clamp to bounds using Go 1.21+ min/max built-ins
	targetInterval = max(minEmitInterval, min(maxEmitInterval, targetInterval))

	// Apply gradual change to prevent oscillation
	newInterval := c.applyGradualChange(currentInterval, targetInterval)

	// Update interval atomically
	c.interval.Store(&newInterval)
}

// calculateBaseInterval determines the base interval based on average payload size.
// Larger payloads get faster intervals to take advantage of better compression ratios.
func (c *AdaptiveController) calculateBaseInterval() time.Duration {
	switch {
	case c.emaBytes < smallPayloadBytes:
		// Small payloads: slower emission (less compression benefit)
		return 8 * time.Second
	case c.emaBytes < mediumPayloadBytes:
		// Medium payloads: moderate emission
		return 4 * time.Second
	case c.emaBytes < largePayloadBytes:
		// Large payloads: faster emission (better compression efficiency)
		return 2 * time.Second
	default:
		// Very large payloads: fastest emission
		return minEmitInterval
	}
}

// applyGradualChange prevents sudden interval changes that could cause system instability.
// Limits the change per adjustment cycle to maxIntervalChange.
func (c *AdaptiveController) applyGradualChange(current, target time.Duration) time.Duration {
	diff := target - current

	if diff > maxIntervalChange {
		return current + maxIntervalChange
	}
	if diff < -maxIntervalChange {
		return current - maxIntervalChange
	}

	return target
}
