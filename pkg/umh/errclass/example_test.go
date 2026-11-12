// Copyright 2026 UMH Systems GmbH
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

package errclass_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/errclass"
)

// A plugin declares its Plugin, IDs, Rules and Classifier once, as package
// variables.
var (
	example = errclass.NewPlugin("example")

	idDeviceUnreachable = example.ID("device_unreachable", errclass.Known)
	idSymbolConfig      = example.ID("symbol_config", errclass.Known)
	idReadGap           = example.ID("read_gap", errclass.Bug)
	idExampleUnknown    = example.ID("unclassified", errclass.Unknown)

	errDeviceUnreachable = errors.New("device unreachable")

	exampleClassifier = &errclass.Classifier{
		Rules: []errclass.Rule{
			{Matches: errclass.Is(errDeviceUnreachable), ID: idDeviceUnreachable},
		},
		Unclassified: idExampleUnknown,
	}
)

// exampleLog returns a Logger that prints each line as its level, message and
// fields, with the fields sorted, so the output below is stable.
func exampleLog() errclass.Logger {
	return exampleClassifier.Logger(service.NewLoggerFromSlog(slog.New(printHandler{})))
}

type printHandler struct{ attrs []slog.Attr }

func (printHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h printHandler) WithGroup(string) slog.Handler          { return h }

func (h printHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return printHandler{attrs: append(slices.Clone(h.attrs), attrs...)}
}

func (h printHandler) Handle(_ context.Context, r slog.Record) error {
	fields := make([]string, 0, len(h.attrs))
	for _, a := range h.attrs {
		fields = append(fields, a.String())
	}
	r.Attrs(func(a slog.Attr) bool {
		fields = append(fields, a.String())
		return true
	})
	slices.Sort(fields)
	fmt.Println(strings.Join(append([]string{r.Level.String(), strconv.Quote(r.Message)}, fields...), " "))
	return nil
}

func Example() {
	log := exampleLog()

	// The call site says what the line means.
	log.Warnf(idSymbolConfig, "Symbol %q not found", "MAIN.speed")

	// The line reports an error, and a rule matches it, so the rule decides.
	err := fmt.Errorf("connect: %w", errDeviceUnreachable)
	log.ErrorErr(err, idReadGap, "Read failed")

	// Info and Debug lines carry no fields.
	log.Info("Connected")

	// The error returned from Connect carries the same ID in its text.
	fmt.Println(exampleClassifier.WithPrefix(err))
	// Output:
	// WARN "Symbol \"MAIN.speed\" not found" error_class=known error_id=example::symbol_config
	// ERROR "Read failed: connect: device unreachable" error_class=known error_id=example::device_unreachable
	// INFO "Connected"
	// [error_id=example::device_unreachable error_class=known] connect: device unreachable
}

// Classify picks the ID in this order: the first rule that matches err, then
// the ID of the call site, then Unclassified.
func ExampleClassifier_Classify() {
	unreachable := fmt.Errorf("connect: %w", errDeviceUnreachable)
	other := errors.New("boom")

	fmt.Println(exampleClassifier.Classify(unreachable, idReadGap))
	fmt.Println(exampleClassifier.Classify(other, idReadGap))
	fmt.Println(exampleClassifier.Classify(other, errclass.ID{}))
	// Output:
	// example::device_unreachable
	// example::read_gap
	// example::unclassified
}

// A line whose error decides its ID, but does not belong in its text, passes
// the result of Classify to Warnf.
func ExampleLogger_Warnf() {
	log := exampleLog()
	skipCause := fmt.Errorf("resolve: %w", errDeviceUnreachable)

	log.Warnf(exampleClassifier.Classify(skipCause, idSymbolConfig), "Symbol %q skipped", "MAIN.speed")
	// Output:
	// WARN "Symbol \"MAIN.speed\" skipped" error_class=known error_id=example::device_unreachable
}

func ExampleClassifier_WithPrefix() {
	err := exampleClassifier.WithPrefix(errors.New("boom"))

	fmt.Println(err)
	// Output:
	// [error_id=example::unclassified error_class=unknown] boom
}
