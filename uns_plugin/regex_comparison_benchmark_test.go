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

package uns_plugin

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// MultiRegexProcessor represents the current approach: pre-compile all regex separately
type MultiRegexProcessor struct {
	regexes []*regexp.Regexp
}

func NewMultiRegexProcessor(patterns []string) (*MultiRegexProcessor, error) {
	regexes := make([]*regexp.Regexp, len(patterns))
	for i, pattern := range patterns {
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("failed to compile regex pattern '%s': %v", pattern, err)
		}
		regexes[i] = regex
	}
	return &MultiRegexProcessor{regexes: regexes}, nil
}

func (p *MultiRegexProcessor) Matches(key []byte) bool {
	for _, regex := range p.regexes {
		if regex.Match(key) {
			return true // Early return on first match
		}
	}
	return false
}

// CombinedRegexProcessor represents the alternative approach: combine all regex into one
type CombinedRegexProcessor struct {
	regex *regexp.Regexp
}

func NewCombinedRegexProcessor(patterns []string) (*CombinedRegexProcessor, error) {
	// Combine all patterns into one: (?:pattern1)|(?:pattern2)|(?:pattern3)
	escapedPatterns := make([]string, len(patterns))
	for i, pattern := range patterns {
		escapedPatterns[i] = fmt.Sprintf("(?:%s)", pattern)
	}
	combinedPattern := strings.Join(escapedPatterns, "|")

	regex, err := regexp.Compile(combinedPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to compile combined regex pattern: %v", err)
	}

	return &CombinedRegexProcessor{regex: regex}, nil
}

func (p *CombinedRegexProcessor) Matches(key []byte) bool {
	return p.regex.Match(key)
}

// CombinedSortedRegexProcessor represents the alternative approach: combine all regex into one but also presort by complexity (low complexity first)
type CombinedSortedRegexProcessor struct {
	regex *regexp.Regexp
}

func NewCombinedSortedRegexProcessor(patterns []string) (*CombinedSortedRegexProcessor, error) {
	// Sort patterns by complexity
	regexes, err := SortRegexesByComplexity(patterns)
	if err != nil {
		return nil, fmt.Errorf("failed to sort regexes: %v", err)
	}

	// Combine all patterns into one: (?:pattern1)|(?:pattern2)|(?:pattern3)
	escapedPatterns := make([]string, len(patterns))
	for i, pattern := range regexes {
		escapedPatterns[i] = fmt.Sprintf("(?:%s)", pattern.Pattern)
	}
	combinedPattern := strings.Join(escapedPatterns, "|")

	regex, err := regexp.Compile(combinedPattern)
	if err != nil {
		return nil, fmt.Errorf("failed to compile combined regex pattern: %v", err)
	}

	return &CombinedSortedRegexProcessor{regex: regex}, nil
}

func (p *CombinedSortedRegexProcessor) Matches(key []byte) bool {
	return p.regex.Match(key)
}

// RegexComplexity represents a regex pattern with its complexity score
type RegexComplexity struct {
	Pattern    string
	Complexity int
	Compiled   *regexp.Regexp
}

// calculateComplexity estimates regex complexity using various heuristics
func calculateComplexity(pattern string) int {
	complexity := 0

	// Base complexity from length (minor factor)
	complexity += len(pattern) / 10

	// High complexity patterns
	complexity += strings.Count(pattern, ".*") * 50     // Greedy wildcards
	complexity += strings.Count(pattern, ".+") * 40     // Greedy wildcards
	complexity += strings.Count(pattern, "(.*)*") * 100 // Nested quantifiers
	complexity += strings.Count(pattern, "(.+)+") * 100 // Nested quantifiers
	complexity += strings.Count(pattern, "(.*)+") * 100 // Nested quantifiers
	complexity += strings.Count(pattern, "(.*)") * 30   // Capturing groups with wildcards

	// Nested quantifiers patterns
	nestedQuantifiers := regexp.MustCompile(`\([^)]*[*+][^)]*\)[*+]`)
	complexity += len(nestedQuantifiers.FindAllString(pattern, -1)) * 80

	// Alternation complexity (especially with overlap potential)
	complexity += strings.Count(pattern, "|") * 15

	// Lookarounds (expensive operations)
	complexity += strings.Count(pattern, "(?=") * 25  // Positive lookahead
	complexity += strings.Count(pattern, "(?!") * 25  // Negative lookahead
	complexity += strings.Count(pattern, "(?<=") * 25 // Positive lookbehind
	complexity += strings.Count(pattern, "(?<!") * 25 // Negative lookbehind

	// Quantifiers (moderate complexity)
	complexity += strings.Count(pattern, "*") * 5
	complexity += strings.Count(pattern, "+") * 5
	complexity += strings.Count(pattern, "?") * 3
	complexity += strings.Count(pattern, "{") * 8 // Range quantifiers

	// Capturing groups add overhead
	complexity += strings.Count(pattern, "(") * 3

	// Character classes (generally efficient, low penalty)
	complexity += strings.Count(pattern, "[") * 2

	// Positive factors (things that make regex more efficient)
	// Anchors make matching more efficient
	if strings.HasPrefix(pattern, "^") {
		complexity -= 10
	}
	if strings.HasSuffix(pattern, "$") {
		complexity -= 10
	}

	// Literal strings are efficient
	literalChars := regexp.MustCompile(`[a-zA-Z0-9]`)
	literalCount := len(literalChars.FindAllString(pattern, -1))
	complexity -= literalCount / 5

	// Ensure minimum complexity
	if complexity < 1 {
		complexity = 1
	}

	return complexity
}

// SortRegexesByComplexity sorts regexes from least to most complex
func SortRegexesByComplexity(patterns []string) ([]*RegexComplexity, error) {
	regexes := make([]*RegexComplexity, 0, len(patterns))

	for _, pattern := range patterns {
		// Try to compile to ensure it's valid
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern '%s': %w", pattern, err)
		}

		complexity := calculateComplexity(pattern)
		regexes = append(regexes, &RegexComplexity{
			Pattern:    pattern,
			Complexity: complexity,
			Compiled:   compiled,
		})
	}

	// Sort by complexity (ascending)
	sort.Slice(regexes, func(i, j int) bool {
		return regexes[i].Complexity < regexes[j].Complexity
	})

	return regexes, nil
}

// Test data generation
func generateTestKeys(matchingRatio float64) [][]byte {
	// Generate a mix of matching and non-matching keys
	keys := make([][]byte, 0, 1000)

	// Matching keys (will match our test patterns)
	matchingCount := int(float64(1000) * matchingRatio)
	for i := 0; i < matchingCount; i++ {
		switch i % 4 {
		case 0:
			keys = append(keys, []byte(fmt.Sprintf("umh.v1.acme.berlin.assembly.temperature_%d", i)))
		case 1:
			keys = append(keys, []byte(fmt.Sprintf("umh.v1.acme.munich.line%d.speed", i)))
		case 2:
			keys = append(keys, []byte(fmt.Sprintf("umh.v1.example.plant%d.status", i)))
		case 3:
			keys = append(keys, []byte(fmt.Sprintf("umh.v1.factory.production.line%d.temperature", i)))
		}
	}

	// Non-matching keys
	nonMatchingCount := 1000 - matchingCount
	for i := 0; i < nonMatchingCount; i++ {
		switch i % 3 {
		case 0:
			keys = append(keys, []byte(fmt.Sprintf("different.v1.company.%d.data", i)))
		case 1:
			keys = append(keys, []byte(fmt.Sprintf("other.v2.system.%d.metrics", i)))
		case 2:
			keys = append(keys, []byte(fmt.Sprintf("random.topic.%d.info", i)))
		}
	}

	return keys
}

// Test patterns that represent realistic UMH topic filters
var testPatterns = []string{
	"umh\\.v1\\.acme\\.berlin\\.assembly\\.temperature.*",
	"umh\\.v1\\.acme\\.munich\\..*",
	"umh\\.v1\\.example\\..*\\.status",
	"umh\\.v1\\.factory\\.production\\..*\\.temperature",
	"umh\\.v1\\.manufacturing\\..*\\.pressure",
	"umh\\.v1\\.plant\\..*\\.speed",
	"umh\\.v1\\..*\\.energy\\..*",
	"umh\\.v1\\..*\\.quality\\..*",
}

// Benchmark with different matching ratios
func BenchmarkMultiRegex_HighMatch(b *testing.B) {
	processor, err := NewMultiRegexProcessor(testPatterns)
	if err != nil {
		b.Fatal(err)
	}

	keys := generateTestKeys(0.8) // 80% matching

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = processor.Matches(key)
		}
	}
}

func BenchmarkCombinedRegex_HighMatch(b *testing.B) {
	processor, err := NewCombinedRegexProcessor(testPatterns)
	if err != nil {
		b.Fatal(err)
	}

	keys := generateTestKeys(0.8) // 80% matching

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = processor.Matches(key)
		}
	}
}

func BenchmarkCombinedSortedRegex_HighMatch(b *testing.B) {
	processor, err := NewCombinedSortedRegexProcessor(testPatterns)
	if err != nil {
		b.Fatal(err)
	}

	keys := generateTestKeys(0.8) // 80% matching

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = processor.Matches(key)
		}
	}
}

func BenchmarkMultiRegex_LowMatch(b *testing.B) {
	processor, err := NewMultiRegexProcessor(testPatterns)
	if err != nil {
		b.Fatal(err)
	}

	keys := generateTestKeys(0.2) // 20% matching

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = processor.Matches(key)
		}
	}
}

func BenchmarkCombinedRegex_LowMatch(b *testing.B) {
	processor, err := NewCombinedRegexProcessor(testPatterns)
	if err != nil {
		b.Fatal(err)
	}

	keys := generateTestKeys(0.2) // 20% matching

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = processor.Matches(key)
		}
	}
}

func BenchmarkCombinedSortedRegex_LowMatch(b *testing.B) {
	processor, err := NewCombinedSortedRegexProcessor(testPatterns)
	if err != nil {
		b.Fatal(err)
	}

	keys := generateTestKeys(0.2) // 20% matching

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = processor.Matches(key)
		}
	}
}

// Benchmark with different numbers of regex patterns
func BenchmarkMultiRegex_FewPatterns(b *testing.B) {
	patterns := testPatterns[:2] // Just 2 patterns
	processor, err := NewMultiRegexProcessor(patterns)
	if err != nil {
		b.Fatal(err)
	}

	keys := generateTestKeys(0.5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = processor.Matches(key)
		}
	}
}

func BenchmarkCombinedRegex_FewPatterns(b *testing.B) {
	patterns := testPatterns[:2] // Just 2 patterns
	processor, err := NewCombinedRegexProcessor(patterns)
	if err != nil {
		b.Fatal(err)
	}

	keys := generateTestKeys(0.5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = processor.Matches(key)
		}
	}
}

func BenchmarkCombinedSortedRegex_FewPatterns(b *testing.B) {
	patterns := testPatterns[:2] // Just 2 patterns
	processor, err := NewCombinedSortedRegexProcessor(patterns)
	if err != nil {
		b.Fatal(err)
	}

	keys := generateTestKeys(0.5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = processor.Matches(key)
		}
	}
}

func BenchmarkMultiRegex_ManyPatterns(b *testing.B) {
	// Create many patterns for stress testing
	manyPatterns := make([]string, 20)
	for i := 0; i < 20; i++ {
		manyPatterns[i] = fmt.Sprintf("umh\\.v1\\.company%d\\..*", i)
	}

	processor, err := NewMultiRegexProcessor(manyPatterns)
	if err != nil {
		b.Fatal(err)
	}

	keys := generateTestKeys(0.5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = processor.Matches(key)
		}
	}
}

func BenchmarkCombinedRegex_ManyPatterns(b *testing.B) {
	// Create many patterns for stress testing
	manyPatterns := make([]string, 20)
	for i := 0; i < 20; i++ {
		manyPatterns[i] = fmt.Sprintf("umh\\.v1\\.company%d\\..*", i)
	}

	processor, err := NewCombinedRegexProcessor(manyPatterns)
	if err != nil {
		b.Fatal(err)
	}

	keys := generateTestKeys(0.5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = processor.Matches(key)
		}
	}
}

func BenchmarkCombinedSortedRegex_ManyPatterns(b *testing.B) {
	// Create many patterns for stress testing
	manyPatterns := make([]string, 20)
	for i := 0; i < 20; i++ {
		manyPatterns[i] = fmt.Sprintf("umh\\.v1\\.company%d\\..*", i)
	}

	processor, err := NewCombinedSortedRegexProcessor(manyPatterns)
	if err != nil {
		b.Fatal(err)
	}

	keys := generateTestKeys(0.5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, key := range keys {
			_ = processor.Matches(key)
		}
	}
}

// Benchmark the current MessageProcessor implementation for comparison
func BenchmarkCurrentMessageProcessor(b *testing.B) {
	processor, err := NewMessageProcessor(testPatterns, &UnsInputMetrics{}, "bytes")
	if err != nil {
		b.Fatal(err)
	}

	keys := generateTestKeys(0.5)

	// Create mock records
	records := make([]*kgo.Record, len(keys))
	for i, key := range keys {
		records[i] = &kgo.Record{
			Key:   key,
			Value: []byte(`{"value": 123.45}`),
			Topic: "umh.messages",
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, record := range records {
			_ = processor.ProcessRecord(record)
		}
	}
}

// Benchmark regex compilation time
func BenchmarkRegexCompilation_Multi(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := NewMultiRegexProcessor(testPatterns)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRegexCompilation_Combined(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := NewCombinedRegexProcessor(testPatterns)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRegexCompilation_CombinedSorted(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := NewCombinedSortedRegexProcessor(testPatterns)
		if err != nil {
			b.Fatal(err)
		}
	}
}
