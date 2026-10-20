// Copyright 2026 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package historian_plugin_test

import (
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"

	tsh "github.com/united-manufacturing-hub/benthos-umh/historian_plugin"
)

const minimalHistorianYAML = "host: h\npassword: p\ndata_contract_name: pump\n"

var durationFields = []string{
	"compress_after",
	"retention",
	"value_chunk_interval",
	"attribute_chunk_interval",
	"write_timeout",
}

var durationSamples = []string{
	`"168h"`, `"1h30m"`, `"1.5h"`, `"1000ms"`, `".5h"`, `"+2h"`, `"0s"`,
	`"168"`, `168`, `"7d"`, `""`, `"500ms"`, `"1500ms"`, `"0"`, `"-1h"`, `"-5s"`, `"1µs1s"`, `"10000000h"`,
}

func lintRejects(configYAML string) bool {
	indented := "  " + strings.ReplaceAll(strings.TrimRight(configYAML, "\n"), "\n", "\n  ")
	lintErr := service.NewEnvironment().NewStreamBuilder().AddOutputYAML("historian:\n" + indented + "\n")
	return lintErr != nil
}

func constructorRejects(configYAML string) bool {
	parsed, parseErr := tsh.HistorianConfig().ParseYAML(configYAML, service.NewEnvironment())
	if parseErr != nil {
		return true
	}
	_, constructErr := tsh.NewHistorianForConfig(parsed)
	return constructErr != nil
}

var _ = Describe("historian duration lint rules", func() {
	It("reject exactly the values the output constructor rejects", func() {
		for _, field := range durationFields {
			for _, sample := range durationSamples {
				configYAML := fmt.Sprintf("%s%s: %s\n", minimalHistorianYAML, field, sample)
				Expect(lintRejects(configYAML)).To(Equal(constructorRejects(configYAML)),
					"lint and constructor disagree on %s: %s", field, sample)
			}
		}
	})

	It("accept the minimal config", func() {
		Expect(lintRejects(minimalHistorianYAML)).To(BeFalse())
	})
})
