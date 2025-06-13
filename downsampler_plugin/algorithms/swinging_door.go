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

package algorithms

import (
	"fmt"
	"math"
	"os"
	"time"
)

// Debug logging helper
func debugLog(format string, args ...interface{}) {
	if os.Getenv("SDT_DEBUG") != "" {
		fmt.Printf("[SDT] "+format+"\n", args...)
	}
}

// -----------------------------------------------------------------------------
// Factory registration
// -----------------------------------------------------------------------------

func init() {
	Register("swinging_door", newSwingingDoor)
}

// newSwingingDoor validates the user-supplied config and returns
// a fully-initialised compressor instance.
func newSwingingDoor(cfg map[string]interface{}) (StreamCompressor, error) {
	const name = "swinging_door"

	// ---- mandatory: threshold -------------------------------------------------
	rawThr, ok := cfg["threshold"]
	if !ok {
		return nil, fmt.Errorf("%s: missing required parameter `threshold`", name)
	}
	thr, ok := rawThr.(float64)
	if !ok {
		return nil, fmt.Errorf("%s: `threshold` must be float64", name)
	}
	if thr < 0 {
		return nil, fmt.Errorf("%s: `threshold` cannot be negative", name)
	}

	// ---- optional: max_time ---------------------------------------
	var maxT time.Duration
	if mt, ok := cfg["max_time"]; ok {
		if v, ok := mt.(time.Duration); ok {
			if v < 0 {
				return nil, fmt.Errorf("%s: invalid max_time: %v", name, v)
			}
			maxT = v
		} else {
			return nil, fmt.Errorf("%s: invalid max_time type: %T", name, mt)
		}
	}

	// ---- optional: min_time ---------------------------------------
	var minT time.Duration
	if mt, ok := cfg["min_time"]; ok {
		if v, ok := mt.(time.Duration); ok {
			if v < 0 {
				return nil, fmt.Errorf("%s: invalid min_time: %v", name, v)
			}
			minT = v
		} else {
			return nil, fmt.Errorf("%s: invalid min_time type: %T", name, mt)
		}
	}

	// ---- validate time constraints relationship ----------------------
	if maxT > 0 && minT > 0 && minT > maxT {
		return nil, fmt.Errorf("%s: min_time (%v) cannot be greater than max_time (%v) - this would deadlock the delta-min gate", name, minT, maxT)
	}

	c := &SwingingDoorAlgorithm{
		threshold: thr,
		maxTime:   maxT,
		minTime:   minT,
	}
	c.openDoor() // initialise slopes
	debugLog("Created SDT algorithm: threshold=%.3f, maxTime=%v, minTime=%v", thr, maxT, minT)
	return c, nil
}

// -----------------------------------------------------------------------------
// Internal data structure
// -----------------------------------------------------------------------------

// SwingingDoorAlgorithm implements the industry-standard "emit-previous"
// Swinging Door Trending (SDT) algorithm used by PI Server, WinCC, and other
// historians for numeric time-series data compression.
//
// SDT maintains upper and lower envelope lines (the "doors") to determine when
// linear interpolation error would exceed the configured threshold.
type SwingingDoorAlgorithm struct {
	// -------- configuration ----------------------------------------------------
	threshold float64
	maxTime   time.Duration // 0 → disabled
	minTime   time.Duration // 0 → disabled

	// -------- state ------------------------------------------------------------
	started      bool   // becomes true after first ingest
	base         Point  // last archived point (B)
	cand         *Point // pending candidate (C)
	slopeMin     float64
	slopeMax     float64
	lastEmitTime time.Time
}

// -----------------------------------------------------------------------------
// StreamCompressor implementation
// -----------------------------------------------------------------------------

// Ingest processes one point and returns 0–1 points to emit immediately.
func (sd *SwingingDoorAlgorithm) Ingest(v float64, ts time.Time) ([]Point, error) {
	// Guard against NaN and Inf values that could poison downstream math
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil, fmt.Errorf("invalid value: NaN and Inf values are not allowed (got %v)", v)
	}

	debugLog("=== INGEST: (%.1f, %s) ===", v, ts.Format("15:04:05"))

	out := make([]Point, 0, 1)

	// ---- first point is always archived -----------------------------------
	if !sd.started {
		sd.started = true
		sd.base = Point{Value: v, Timestamp: ts}
		sd.lastEmitTime = ts
		out = append(out, sd.base)
		debugLog("FIRST POINT: base=(%.1f, %s), EMIT",
			sd.base.Value, sd.base.Timestamp.Format("15:04:05"))
		return out, nil
	}

	debugLog("CURRENT STATE: base=(%.1f, %s), cand=%s, slopes=[%.3f, %.3f]",
		sd.base.Value, sd.base.Timestamp.Format("15:04:05"),
		sd.candString(), sd.slopeMin, sd.slopeMax)

	// ---------------------------------------------------------------
	// 2. heartbeat (comp-max-time) – runs on *every* call after start
	// ---------------------------------------------------------------
	debugLog("HEARTBEAT CHECK: Δt=%v   maxTime=%v", ts.Sub(sd.lastEmitTime), sd.maxTime)
	if sd.maxTime > 0 && ts.Sub(sd.lastEmitTime) >= sd.maxTime {
		p := Point{Value: v, Timestamp: ts}

		// Capture the actual elapsed time before updating lastEmitTime
		actualElapsed := ts.Sub(sd.lastEmitTime)

		sd.base = p
		sd.lastEmitTime = ts
		sd.cand = nil // discard any pending candidate
		sd.openDoor() // reset envelope

		debugLog("HEARTBEAT fired: Δt=%v   maxTime=%v", actualElapsed, sd.maxTime)

		return append(out, p), nil
	}

	// ---- establish candidate if none yet ----------------------------------
	if sd.cand == nil {
		sd.cand = &Point{Value: v, Timestamp: ts}
		sd.closeDoor(*sd.cand)
		debugLog("NO CANDIDATE: set candidate=(%.1f, %s), new slopes=[%.3f, %.3f]",
			sd.cand.Value, sd.cand.Timestamp.Format("15:04:05"), sd.slopeMin, sd.slopeMax)
		return out, nil
	}

	// ---- decide if we must emit the candidate -----------------------------
	emitNeeded := sd.mustEmit(v, ts)
	debugLog("EMIT CHECK: emitNeeded=%t", emitNeeded)

	// ---------- NEW: Delta-Min gate -------------------------------------------
	debugLog("DELTA-MIN gate: Δt=%v   minTime=%v", sd.cand.Timestamp.Sub(sd.lastEmitTime), sd.minTime)
	if emitNeeded && sd.minTime > 0 &&
		sd.cand.Timestamp.Sub(sd.lastEmitTime) < sd.minTime {
		// Not enough time has elapsed → keep sliding candidate
		debugLog("DELTA-MIN gate: hold candidate (Δt=%v < %v)",
			sd.cand.Timestamp.Sub(sd.lastEmitTime), sd.minTime)

		sd.cand = &Point{Value: v, Timestamp: ts}
		sd.openDoor()
		return out, nil // nothing emitted this call
	}

	// ---- emit the candidate if required -----------------------------------
	if emitNeeded {
		emittedPoint := *sd.cand
		out = append(out, emittedPoint)
		sd.base = emittedPoint
		sd.lastEmitTime = emittedPoint.Timestamp

		debugLog("EMIT PREVIOUS: emit candidate=(%.1f, %s)",
			emittedPoint.Value, emittedPoint.Timestamp.Format("15:04:05"))

		// Current sample becomes the new candidate
		sd.cand = &Point{Value: v, Timestamp: ts}
		sd.openDoor()
		return out, nil
	}

	// ---- envelope not broken – tighten door ------------------------------
	debugLog("ENVELOPE OK: update candidate from (%.1f, %s) to (%.1f, %s)",
		sd.cand.Value, sd.cand.Timestamp.Format("15:04:05"),
		v, ts.Format("15:04:05"))
	sd.cand = &Point{Value: v, Timestamp: ts}
	sd.closeDoor(*sd.cand)
	debugLog("TIGHTENED: new slopes=[%.3f, %.3f]", sd.slopeMin, sd.slopeMax)

	return out, nil
}

// Helper to format candidate for debug output
func (sd *SwingingDoorAlgorithm) candString() string {
	if sd.cand == nil {
		return "nil"
	}
	return fmt.Sprintf("(%.1f, %s)", sd.cand.Value, sd.cand.Timestamp.Format("15:04:05"))
}

// Flush emits the final pending candidate exactly once.
func (sd *SwingingDoorAlgorithm) Flush() ([]Point, error) {
	debugLog("=== FLUSH ===")
	if sd.cand == nil {
		debugLog("FLUSH: no candidate to emit")
		return nil, nil
	}
	p := *sd.cand
	sd.cand = nil
	debugLog("FLUSH: emit final candidate=(%.1f, %s)", p.Value, p.Timestamp.Format("15:04:05"))
	return []Point{p}, nil
}

func (sd *SwingingDoorAlgorithm) Reset() {
	debugLog("=== RESET ===")
	*sd = SwingingDoorAlgorithm{
		threshold: sd.threshold,
		maxTime:   sd.maxTime,
		minTime:   sd.minTime,
	}
	sd.openDoor()
}

func (sd *SwingingDoorAlgorithm) Config() string {
	cfg := fmt.Sprintf("swinging_door(threshold=%.3f", sd.threshold)
	if sd.maxTime > 0 {
		cfg += ",max_time=" + sd.maxTime.String()
	}
	if sd.minTime > 0 {
		cfg += ",min_time=" + sd.minTime.String()
	}
	cfg += ")"
	return cfg
}

func (sd *SwingingDoorAlgorithm) Name() string { return "swinging_door" }

// NeedsPreviousPoint returns true because Swinging Door Trending uses emit-previous logic.
func (sd *SwingingDoorAlgorithm) NeedsPreviousPoint() bool {
	return true
}

// -----------------------------------------------------------------------------
// Envelope helpers
// -----------------------------------------------------------------------------

func (sd *SwingingDoorAlgorithm) openDoor() {
	sd.slopeMax = math.Inf(+1)
	sd.slopeMin = math.Inf(-1)
	debugLog("OPEN DOOR: slopes=[%.3f, %.3f]", sd.slopeMin, sd.slopeMax)
}

// closeDoor narrows the envelope using the given point.
func (sd *SwingingDoorAlgorithm) closeDoor(p Point) {
	dx := p.Timestamp.Sub(sd.base.Timestamp).Seconds()
	if dx <= 0 {
		debugLog("CLOSE DOOR: dx=%.3f <= 0, ignoring", dx)
		return // identical timestamp – ignore for envelope maths
	}
	dy := p.Value - sd.base.Value

	upper := (dy + sd.threshold) / dx
	lower := (dy - sd.threshold) / dx

	debugLog("CLOSE DOOR: base=(%.1f, %s), point=(%.1f, %s), dx=%.3f, dy=%.3f",
		sd.base.Value, sd.base.Timestamp.Format("15:04:05"),
		p.Value, p.Timestamp.Format("15:04:05"), dx, dy)
	debugLog("CLOSE DOOR: calculated slopes: upper=%.3f, lower=%.3f", upper, lower)
	debugLog("CLOSE DOOR: before: slopeMax=%.3f, slopeMin=%.3f", sd.slopeMax, sd.slopeMin)

	if upper < sd.slopeMax {
		sd.slopeMax = upper
	}
	if lower > sd.slopeMin {
		sd.slopeMin = lower
	}

	debugLog("CLOSE DOOR: after: slopeMax=%.3f, slopeMin=%.3f, intersection=%t",
		sd.slopeMax, sd.slopeMin, sd.slopeMin > sd.slopeMax)
}

func (sd *SwingingDoorAlgorithm) mustEmit(v float64, ts time.Time) bool {
	// Use geometric envelope approach as the canonical decision mechanism
	// This is the industry standard for SDT and is consistent with closeDoor()

	dx := ts.Sub(sd.base.Timestamp).Seconds()
	if dx == 0 {
		debugLog("MUST EMIT: duplicate timestamp")
		return true // duplicate timestamp
	}

	slope := (v - sd.base.Value) / dx

	// Check envelope intersection - the new point's slope must fit within the envelope
	violation := slope < sd.slopeMin || slope > sd.slopeMax
	debugLog("ENVELOPE CHECK: slope=%.3f, bounds=[%.3f, %.3f], violation=%t",
		slope, sd.slopeMin, sd.slopeMax, violation)

	// Also check if the envelope has collapsed (slopeMin > slopeMax)
	// This happens when the accumulated constraints become impossible to satisfy
	envelopeCollapsed := sd.slopeMin > sd.slopeMax
	debugLog("ENVELOPE COLLAPSED: %t (min=%.3f > max=%.3f)",
		envelopeCollapsed, sd.slopeMin, sd.slopeMax)

	// Additional safety check: verify interpolation error for the candidate
	// This catches edge cases where envelope math might miss significant errors
	if sd.cand != nil && !violation && !envelopeCollapsed {
		baseDx := ts.Sub(sd.base.Timestamp).Seconds()
		candDx := sd.cand.Timestamp.Sub(sd.base.Timestamp).Seconds()

		if baseDx > 0 && candDx > 0 {
			// Calculate interpolation error if we skip the candidate
			slopeBaseToNew := (v - sd.base.Value) / baseDx
			interpolatedAtCand := sd.base.Value + slopeBaseToNew*candDx
			interpolationError := math.Abs(sd.cand.Value - interpolatedAtCand)

			if interpolationError >= sd.threshold {
				debugLog("MUST EMIT: interpolation safety check - error %.3f >= threshold %.3f",
					interpolationError, sd.threshold)
				return true
			}
		}
	}

	if violation || envelopeCollapsed {
		debugLog("MUST EMIT: geometric envelope - violation=%t, collapsed=%t", violation, envelopeCollapsed)
		return true
	}

	debugLog("ENVELOPE OK: point fits within bounds")
	return false
}
