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

package opcua_plugin

import (
	"bytes"
	"log/slog"
	"strings"

	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// captureWarnLogger returns a logger writing into a buffer, so tests can
// count the warnings the plugin would log.
func captureWarnLogger() (*service.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	l := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	return l, &buf
}

var _ = Describe("nodesAffectedByTagPathBug", func() {
	It("returns only the nodes whose topic the corrected calculation changes", func() {
		nodes := []NodeDef{
			// Unaffected: the browse name is only the last segment.
			{NodeID: ua.NewNumericNodeID(2, 1), BrowseName: "Tag1", Path: "FolderNode.Tag1"},
			// Affected: the name also appears earlier in the path.
			{NodeID: ua.NewNumericNodeID(2, 2), BrowseName: "1", Path: "DB1.arr.1"},
			// Unaffected: sanitize keeps the last segment (ENG-7956).
			{NodeID: ua.NewNumericNodeID(2, 3), BrowseName: "Pump 1", Path: "Line.Pump_1"},
		}

		affected := nodesAffectedByTagPathBug(nodes)

		Expect(affected).To(HaveLen(1))
		Expect(affected[0].NodeID.String()).To(Equal("ns=2;i=2"))
	})

	It("returns nothing for an empty node list", func() {
		Expect(nodesAffectedByTagPathBug(nil)).To(BeEmpty())
	})
})

var _ = Describe("tag path compatibility warning", func() {
	// affectedNodes builds n nodes whose topic the corrected calculation
	// would change, as a browse of a large address space might find.
	affectedNodes := func(n int) []NodeDef {
		nodes := make([]NodeDef, 0, n)
		for i := 0; i < n; i++ {
			nodes = append(nodes, NodeDef{
				NodeID:     ua.NewNumericNodeID(2, uint32(i+1)),
				BrowseName: "1",
				Path:       "DB1.arr.1",
			})
		}
		return nodes
	}

	It("warns exactly once, naming the flag, when a node is affected", func() {
		log, buf := captureWarnLogger()
		g := &OPCUAInput{OPCUAConnection: &OPCUAConnection{Log: log}}

		g.warnTagPathCompatibility(affectedNodes(5))

		out := buf.String()
		Expect(strings.Count(out, "level=WARN")).To(Equal(1))
		Expect(out).To(ContainSubstring("compatibility.correct_tag_path"))
	})

	It("says nothing when no node is affected", func() {
		log, buf := captureWarnLogger()
		g := &OPCUAInput{OPCUAConnection: &OPCUAConnection{Log: log}}

		g.warnTagPathCompatibility([]NodeDef{
			{NodeID: ua.NewNumericNodeID(2, 1), BrowseName: "Tag1", Path: "FolderNode.Tag1"},
			{NodeID: ua.NewNumericNodeID(2, 2), BrowseName: "xLifeBit", Path: "xLifeBit"},
		})

		Expect(strings.Count(buf.String(), "level=WARN")).To(Equal(0))
	})

	It("says nothing when the flag is on", func() {
		log, buf := captureWarnLogger()
		g := &OPCUAInput{OPCUAConnection: &OPCUAConnection{Log: log}, CorrectTagPath: true}

		g.warnTagPathCompatibility(affectedNodes(2))

		Expect(strings.Count(buf.String(), "level=WARN")).To(Equal(0))
	})
})

var _ = Describe("correct_tag_path compatibility flag", func() {
	const baseYAML = `
endpoint: 'opc.tcp://localhost:4840'
nodeIDs: ['ns=2;s=IoTSensors']
`

	// parseInput builds the input the way the plugin does: real YAML through
	// the OPC UA input's config spec, then the constructor.
	parseInput := func(compatibilityBlock string) *OPCUAInput {
		conf, err := OPCUAConfigSpec.ParseYAML(baseYAML+compatibilityBlock, nil)
		ExpectWithOffset(1, err).NotTo(HaveOccurred())

		input, err := newOPCUAInput(conf, service.MockResources())
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
		return input
	}

	It("stays on the legacy calculation when the compatibility block is absent", func() {
		Expect(parseInput("").CorrectTagPath).To(BeFalse())
	})

	It("stays on the legacy calculation when the flag is false", func() {
		Expect(parseInput("compatibility:\n  correct_tag_path: false\n").CorrectTagPath).To(BeFalse())
	})

	It("selects the corrected calculation when the flag is true", func() {
		Expect(parseInput("compatibility:\n  correct_tag_path: true\n").CorrectTagPath).To(BeTrue())
	})
})

var _ = Describe("compatibility field in the published config schema", func() {
	// OPCUAConfigSpec is a package-level var whose initializer calls
	// compatibility.Field(), and Field() renders its description from the
	// flags declared so far. Go orders package-level initialization by
	// dependency, and OPCUAConfigSpec depends on the compatibility Flags but
	// not on correctTagPath, so nothing in the language guarantees the flag
	// is declared before the spec is built: it works today only because
	// core_browse.go sorts before read.go. If a file rename ever reorders
	// them, the flag silently vanishes from the exported schema — the
	// Management Console shows "This plugin currently has no compatibility
	// flags" — while Parse still accepts the key. This test walks the
	// registered inputs the way cmd/schema-export does and fails if that
	// ever happens.
	It("exports correct_tag_path in the opcua input's schema", func() {
		found := false
		service.GlobalEnvironment().WalkInputs(func(name string, view *service.ConfigView) {
			if name != "opcua" {
				return
			}
			found = true

			js, err := view.FormatJSON()
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
			ExpectWithOffset(1, string(js)).To(ContainSubstring("correct_tag_path"))
		})
		Expect(found).To(BeTrue(), "the opcua input was not found among the registered inputs")
	})
})
