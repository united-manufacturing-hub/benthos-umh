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

package fieldrule_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/fieldrule"
)

type discardOutput struct{}

func (discardOutput) Connect(context.Context) error                 { return nil }
func (discardOutput) Write(context.Context, *service.Message) error { return nil }
func (discardOutput) Close(context.Context) error                   { return nil }

func lintErrorFor(rule *fieldrule.DurationRule, yamlValue string) error {
	environment := service.NewEnvironment()
	spec := service.NewConfigSpec().Field(rule.Field())
	registerErr := environment.RegisterOutput("probe", spec,
		func(*service.ParsedConfig, *service.Resources) (service.Output, int, error) {
			return discardOutput{}, 1, nil
		})
	Expect(registerErr).NotTo(HaveOccurred())

	return environment.NewStreamBuilder().AddOutputYAML(fmt.Sprintf("probe:\n  interval: %s\n", yamlValue))
}

var _ = Describe("DurationRule", func() {
	wholeSecondsAtLeastOne := fieldrule.Duration("interval").Min(time.Second).WholeSeconds()

	DescribeTable("accepts a Go duration of whole seconds from 1s",
		func(yamlValue string) {
			Expect(lintErrorFor(wholeSecondsAtLeastOne, yamlValue)).NotTo(HaveOccurred())
		},
		Entry("hours", `"168h"`),
		Entry("hours and minutes", `"1h30m"`),
		Entry("fractional hours", `"1.5h"`),
		Entry("milliseconds adding up to whole seconds", `"1000ms"`),
		Entry("leading dot", `".5h"`),
		Entry("plus sign", `"+2h"`),
	)

	DescribeTable("rejects a value the plugin cannot use",
		func(yamlValue string, wantMessage string) {
			Expect(lintErrorFor(wholeSecondsAtLeastOne, yamlValue)).To(MatchError(ContainSubstring(wantMessage)))
		},
		Entry("a quoted number without a unit", `"168"`, "must be a duration with a unit"),
		Entry("a YAML number", `168`, "interval"),
		Entry("days", `"7d"`, "must be a duration with a unit"),
		Entry("an empty string", `""`, "must be a duration with a unit"),
		Entry("below one second", `"500ms"`, "must be at least 1s"),
		Entry("zero", `"0"`, "must be at least 1s"),
		Entry("negative", `"-1h"`, "must be at least 1s"),
		Entry("a fraction of a second", `"1500ms"`, "must be a whole number of seconds"),
	)

	It("accepts an empty value when the rule allows it", func() {
		optional := fieldrule.Duration("interval").AllowEmpty().Min(time.Second)
		Expect(lintErrorFor(optional, `""`)).NotTo(HaveOccurred())
	})

	It("accepts a fraction of a second when whole seconds are not required", func() {
		notNegative := fieldrule.Duration("interval").Min(0)
		Expect(lintErrorFor(notNegative, `"500ms"`)).NotTo(HaveOccurred())
		Expect(lintErrorFor(notNegative, `"-5s"`)).To(MatchError(ContainSubstring("must be at least 0s")))
	})
})
