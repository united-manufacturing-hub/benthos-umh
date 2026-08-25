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

package cache

import (
	"errors"
	"fmt"
)

// ErrOldWatermark is when the incoming Watermark is not strictly newer than the stored one.
var ErrOldWatermark = errors.New("dropped write with watermark older/equal to stored")

// ErrMissingValue is when the passed msg has no "value" field.
var ErrMissingValue = errors.New("msg is missing the 'value' field")

// ErrMissingWatermark is when the passed msg has no watermark field ('watermark', 'timestamp_ms', or 'kafka_offset').
var ErrMissingWatermark = errors.New("msg is missing a watermark field: expected one of 'watermark', 'timestamp_ms', 'kafka_offset'")

// ErrMultipleWatermarks is when more than one watermark field is present in the passed msg.
var ErrMultipleWatermarks = errors.New("msg has multiple watermark fields; only one of 'watermark', 'timestamp_ms', 'kafka_offset' may be set")

// ErrWatermarkNotNumeric is when the watermark field is not a number.
var ErrWatermarkNotNumeric = errors.New("watermark field must be numeric")

// StaleWriteError carries both watermarks so callers can log a diagnostic message.
type StaleWriteError struct {
	Key      string
	Incoming int64
	Stored   int64
}

func (e *StaleWriteError) Error() string {
	return fmt.Sprintf("cache.set: dropped write for key %q (incoming watermark %d is older/equal to the last stored %d)", e.Key, e.Incoming, e.Stored)
}

// Is lets errors.Is(err, ErrOldWatermark) match a *StaleWriteError.
func (e *StaleWriteError) Is(target error) bool { return target == ErrOldWatermark }
