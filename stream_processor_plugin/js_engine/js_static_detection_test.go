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

package js_engine_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/config"
	"github.com/united-manufacturing-hub/benthos-umh/stream_processor_plugin/js_engine"
)

var _ = Describe("Static Detection Edge Cases", func() {
	var detector *js_engine.StaticDetector

	BeforeEach(func() {
		sources := map[string]string{
			"press":   "topic1",
			"tF":      "topic2",
			"r":       "topic3",
			"voltage": "topic4",
			"current": "topic5",
		}
		detector = js_engine.NewStaticDetector(sources)
	})

	Describe("Basic Constants", func() {
		It("should detect string literals", func() {
			testCases := []string{
				`"static string"`,
				`'single quotes'`,
				`"embedded \"quotes\""`,
				`""`, // empty string
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})

		It("should detect numeric literals", func() {
			testCases := []string{
				"42",
				"3.14159",
				"-123",
				"0",
				"1e10",
				"0xFF",   // hex
				"0b1010", // binary
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})

		It("should detect boolean and null literals", func() {
			testCases := []string{
				"true",
				"false",
				"null",
				"undefined",
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})
	})

	Describe("Built-in Objects and Functions", func() {
		It("should detect Date operations as static", func() {
			testCases := []string{
				"Date.now()",
				"new Date()",
				"new Date(2024, 0, 1)",
				"Date.UTC(2024, 0, 1)",
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})

		It("should detect Math operations as static", func() {
			testCases := []string{
				"Math.PI",
				"Math.E",
				"Math.random()",
				"Math.floor(3.7)",
				"Math.max(1, 2, 3)",
				"Math.sin(Math.PI / 2)",
				"Math.pow(2, 8)",
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})

		It("should detect JSON operations as static", func() {
			testCases := []string{
				`JSON.stringify({test: "value"})`,
				`JSON.parse('{"key": "value"}')`,
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})

		It("should detect type constructors as static", func() {
			testCases := []string{
				"Number(42)",
				"String(123)",
				"Boolean(1)",
				"Array(5)",
				"Object()",
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})
	})

	Describe("Simple Variable References", func() {
		It("should detect single source variable references", func() {
			testCases := map[string][]string{
				"press":   {"press"},
				"tF":      {"tF"},
				"r":       {"r"},
				"voltage": {"voltage"},
				"current": {"current"},
			}

			for expr, expectedDeps := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.DynamicMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(ConsistOf(expectedDeps), "Expression: %s", expr)
			}
		})

		It("should ignore unknown variables (treat as static)", func() {
			testCases := []string{
				"unknownVar",
				"someOtherVariable",
				"x",
				"temp", // similar to tF but not exact match
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})
	})

	Describe("Arithmetic Operations", func() {
		It("should detect arithmetic with source variables", func() {
			testCases := map[string][]string{
				"press + 4.00001":   {"press"},
				"tF * 69 / 31":      {"tF"},
				"voltage - 12":      {"voltage"},
				"current * 1000":    {"current"},
				"press + tF":        {"press", "tF"},
				"voltage * current": {"voltage", "current"},
				"press + tF + r":    {"press", "tF", "r"},
				"(press + tF) * r":  {"press", "tF", "r"},
			}

			for expr, expectedDeps := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.DynamicMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(ConsistOf(expectedDeps), "Expression: %s", expr)
			}
		})

		It("should detect static arithmetic operations", func() {
			testCases := []string{
				"42 + 8",
				"3.14 * 2",
				"100 / 5",
				"Math.PI * 2",
				"Math.pow(2, 3) + 1",
				"(5 + 3) * 2",
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})
	})

	Describe("Object and Array Operations", func() {
		It("should detect object property access with variables", func() {
			testCases := map[string][]string{
				"press.value":  {"press"},
				"tF.timestamp": {"tF"},
				"voltage.min":  {"voltage"},
			}

			for expr, expectedDeps := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.DynamicMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(ConsistOf(expectedDeps), "Expression: %s", expr)
			}
		})

		It("should detect computed property access with variables", func() {
			testCases := map[string][]string{
				"press[tF]":        {"press", "tF"},
				"voltage[current]": {"voltage", "current"},
				"press['value']":   {"press"}, // static property name
			}

			for expr, expectedDeps := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.DynamicMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(ConsistOf(expectedDeps), "Expression: %s", expr)
			}
		})

		It("should detect array literals with variables", func() {
			testCases := map[string][]string{
				"[press, tF, r]":      {"press", "tF", "r"},
				"[press, 42, 'test']": {"press"},
				"[1, 2, voltage]":     {"voltage"},
			}

			for expr, expectedDeps := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.DynamicMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(ConsistOf(expectedDeps), "Expression: %s", expr)
			}
		})

		It("should detect static arrays and objects", func() {
			testCases := []string{
				"[1, 2, 3]",
				`["a", "b", "c"]`,
				`{key: "value"}`,
				`{num: 42, bool: true}`,
				`{nested: {prop: "value"}}`,
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})
	})

	Describe("Function Calls", func() {
		It("should detect function calls with variable arguments", func() {
			testCases := map[string][]string{
				"Math.max(press, tF)":        {"press", "tF"},
				"Math.min(voltage, current)": {"voltage", "current"},
				"Math.pow(press, 2)":         {"press"},
				"parseInt(tF)":               {"tF"},
				"parseFloat(voltage)":        {"voltage"},
			}

			for expr, expectedDeps := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.DynamicMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(ConsistOf(expectedDeps), "Expression: %s", expr)
			}
		})

		It("should detect static function calls", func() {
			testCases := []string{
				"Math.max(1, 2, 3)",
				"parseInt('42')",
				"parseFloat('3.14')",
				"String.fromCharCode(65)",
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})
	})

	Describe("Conditional Expressions", func() {
		It("should detect ternary operators with variables", func() {
			testCases := map[string][]string{
				"press > 100 ? press : 0":               {"press"},
				"tF < 0 ? 'cold' : 'hot'":               {"tF"},
				"voltage > current ? voltage : current": {"voltage", "current"},
				"press > tF ? press : tF":               {"press", "tF"},
			}

			for expr, expectedDeps := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.DynamicMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(ConsistOf(expectedDeps), "Expression: %s", expr)
			}
		})

		It("should detect static ternary operators", func() {
			testCases := []string{
				"true ? 'yes' : 'no'",
				"5 > 3 ? 100 : 200",
				"Math.PI > 3 ? 'pi' : 'not pi'",
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})
	})

	Describe("Boolean Logic", func() {
		It("should detect logical operations with variables", func() {
			testCases := map[string][]string{
				"press && tF":           {"press", "tF"},
				"voltage || current":    {"voltage", "current"},
				"!press":                {"press"},
				"press && tF && r":      {"press", "tF", "r"},
				"press > 0 && tF < 100": {"press", "tF"},
			}

			for expr, expectedDeps := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.DynamicMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(ConsistOf(expectedDeps), "Expression: %s", expr)
			}
		})

		It("should detect static logical operations", func() {
			testCases := []string{
				"true && false",
				"1 || 0",
				"!false",
				"5 > 3 && 2 < 4",
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})
	})

	Describe("Complex Mixed Expressions", func() {
		It("should handle complex expressions with multiple variables", func() {
			testCases := map[string][]string{
				"(press + tF) / (voltage - current)":                         {"press", "tF", "voltage", "current"},
				"Math.sqrt(Math.pow(press, 2) + Math.pow(tF, 2))":            {"press", "tF"},
				"press > 100 ? Math.max(tF, voltage) : Math.min(r, current)": {"press", "tF", "voltage", "r", "current"},
				"press * 0.1 + tF * 0.2 + voltage * 0.3":                     {"press", "tF", "voltage"},
			}

			for expr, expectedDeps := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.DynamicMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(ConsistOf(expectedDeps), "Expression: %s", expr)
			}
		})

		It("should handle mixed static and unknown variables", func() {
			testCases := []string{
				"unknownVar + Math.PI",
				"someConst * Date.now()",
				"Math.max(unknownVar, 42)",
			}

			for _, expr := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.StaticMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(BeEmpty(), "Expression: %s", expr)
			}
		})
	})

	Describe("Edge Cases and Error Handling", func() {
		It("should handle empty expressions", func() {
			_, err := detector.AnalyzeMapping("")
			Expect(err).To(HaveOccurred())
		})

		It("should handle invalid JavaScript syntax", func() {
			invalidExpressions := []string{
				"invalid syntax +++",
				"function() { return }",
				"var x = 5;",
				"if (true) { }",
				"for (;;) { }",
				"press +",
				"(press + tF",
				"press + tF)",
			}

			for _, expr := range invalidExpressions {
				_, err := detector.AnalyzeMapping(expr)
				Expect(err).To(HaveOccurred(), "Expression should be invalid: %s", expr)
			}
		})

		It("should handle very long expressions", func() {
			// Build a long but valid expression
			longExpr := "press"
			for i := 0; i < 100; i++ {
				longExpr += " + " + "1"
			}

			analysis, err := detector.AnalyzeMapping(longExpr)
			Expect(err).ToNot(HaveOccurred())
			Expect(analysis.Type).To(Equal(config.DynamicMapping))
			Expect(analysis.Dependencies).To(ConsistOf("press"))
		})

		It("should handle special characters in variable names", func() {
			// Create detector with special variable names
			specialSources := map[string]string{
				"press_1":  "topic1",
				"temp$F":   "topic2",
				"$voltage": "topic3",
				"_current": "topic4",
			}
			specialDetector := js_engine.NewStaticDetector(specialSources)

			testCases := map[string][]string{
				"press_1 + 10":  {"press_1"},
				"temp$F * 2":    {"temp$F"},
				"$voltage / 12": {"$voltage"},
				"_current + 5":  {"_current"},
			}

			for expr, expectedDeps := range testCases {
				analysis, err := specialDetector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.DynamicMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(ConsistOf(expectedDeps), "Expression: %s", expr)
			}
		})
	})

	Describe("Deduplication", func() {
		It("should deduplicate repeated variable references", func() {
			testCases := map[string][]string{
				"press + press":              {"press"},
				"tF * tF * tF":               {"tF"},
				"Math.max(press, press, tF)": {"press", "tF"},
				"press > tF ? press : tF":    {"press", "tF"},
			}

			for expr, expectedDeps := range testCases {
				analysis, err := detector.AnalyzeMapping(expr)
				Expect(err).ToNot(HaveOccurred(), "Expression: %s", expr)
				Expect(analysis.Type).To(Equal(config.DynamicMapping), "Expression: %s", expr)
				Expect(analysis.Dependencies).To(ConsistOf(expectedDeps), "Expression: %s", expr)
			}
		})
	})
})
