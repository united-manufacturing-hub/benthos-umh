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

// Package stream_processor_plugin provides state management for the Benthos stream processor.
//
// STATE MANAGEMENT SYSTEM OVERVIEW:
//
// The state management system is designed to handle timeseries data processing with
// dependency-based evaluation. It solves several key challenges:
//
//  1. THREAD-SAFE VARIABLE STORAGE: Multiple goroutines may process incoming messages
//     simultaneously, so we need thread-safe access to shared variable state.
//
//  2. DEPENDENCY TRACKING: Variables can depend on other variables through JavaScript
//     expressions. We need to track which variables have been updated to determine
//     which mappings should be re-evaluated.
//
//  3. SOURCE TRACEABILITY: Each variable value must be traceable back to its source
//     topic (e.g., "press", "tF", "r") for debugging and validation purposes.
//
//  4. TIMESTAMP TRACKING: For timeseries data, we need to know when each variable
//     was last updated to ensure proper ordering and freshness.
//
//  5. PERFORMANCE OPTIMIZATION: Only re-evaluate mappings when their dependencies
//     have changed, rather than recalculating everything on every message.
//
// WHY VariableValue HAS A SOURCE FIELD:
//
// The Source field in VariableValue serves multiple purposes:
// - DEBUGGING: When a mapping fails, we can trace which source topic provided the problematic data
// - VALIDATION: Ensures variables are coming from expected sources as configured
// - AUDITING: Provides a clear audit trail of data flow through the system
// - MONITORING: Enables monitoring of which sources are active vs. inactive
//
// EXAMPLE FLOW:
// 1. Message arrives with umh_topic="umh.v1.corpA.plant-A.aawd._raw.press"
// 2. StateManager resolves this to variable "press" based on Sources configuration
// 3. Variable stored as VariableValue{Value: 1.23, Source: "press", Timestamp: now}
// 4. Dependent mappings (like "efficiency = press / target") are identified
// 5. Only those mappings are re-evaluated, not all mappings
package stream_processor_plugin

import (
	"fmt"
	"sync"
	"time"
)

// ProcessorState holds the variable state for the processor.
//
// It provides thread-safe storage for variables using a RWMutex to allow
// concurrent reads while ensuring write safety. The Variables map stores
// variable names to their current values with metadata.
//
// This struct handles the low-level storage mechanics, while StateManager
// provides the higher-level coordination and business logic.
type ProcessorState struct {
	Variables map[string]*VariableValue // Thread-safe variable storage
	mutex     sync.RWMutex              // Protects concurrent access to Variables
}

// VariableValue represents a stored variable value with metadata.
//
// Each field serves a specific purpose in the dependency-based evaluation system:
//
// Value: The actual data value received from the source topic (e.g., 1.23, "active", true).
// This is what gets used in JavaScript expressions and calculations.
//
// Timestamp: When this variable was last updated. Critical for timeseries data to:
// - Ensure proper ordering of events
// - Detect stale data that might need refreshing
// - Enable time-based calculations and filtering
// - Support debugging by showing when variables were last seen
//
// Source: The source identifier (e.g., "press", "tF", "r") that provided this value.
// Essential for:
// - DEBUGGING: Trace which input caused a calculation failure
// - VALIDATION: Verify variables come from expected sources
// - MONITORING: Track which sources are active/inactive
// - AUDITING: Maintain clear data lineage through the system
//
// Example: VariableValue{Value: 1.23, Source: "press", Timestamp: 2024-01-15T10:30:00Z}
// means the "press" variable was set to 1.23 at the given timestamp from topic
// "umh.v1.corpA.plant-A.aawd._raw.press".
type VariableValue struct {
	Value     interface{} // The actual data value from the source
	Timestamp time.Time   // When this variable was last updated
	Source    string      // Which source provided this value (for traceability)
}

// NewProcessorState creates a new processor state instance
func NewProcessorState() *ProcessorState {
	return &ProcessorState{
		Variables: make(map[string]*VariableValue),
	}
}

// SetVariable stores a variable value with timestamp and source information
func (ps *ProcessorState) SetVariable(name string, value interface{}, source string) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	ps.Variables[name] = &VariableValue{
		Value:     value,
		Timestamp: time.Now(),
		Source:    source,
	}
}

// GetVariable retrieves a variable value by name
func (ps *ProcessorState) GetVariable(name string) (*VariableValue, bool) {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	value, exists := ps.Variables[name]
	return value, exists
}

// GetVariableValue retrieves just the value portion of a variable
func (ps *ProcessorState) GetVariableValue(name string) (interface{}, bool) {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	if value, exists := ps.Variables[name]; exists {
		return value.Value, true
	}
	return nil, false
}

// HasVariable checks if a variable exists in the state
func (ps *ProcessorState) HasVariable(name string) bool {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	_, exists := ps.Variables[name]
	return exists
}

// GetAllVariables returns a copy of all variables (thread-safe)
func (ps *ProcessorState) GetAllVariables() map[string]*VariableValue {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	result := make(map[string]*VariableValue)
	for name, value := range ps.Variables {
		// Create a copy of the variable value
		result[name] = &VariableValue{
			Value:     value.Value,
			Timestamp: value.Timestamp,
			Source:    value.Source,
		}
	}
	return result
}

// GetVariableNames returns all variable names currently stored
func (ps *ProcessorState) GetVariableNames() []string {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	names := make([]string, 0, len(ps.Variables))
	for name := range ps.Variables {
		names = append(names, name)
	}
	return names
}

// HasAllVariables checks if all specified variables exist in the state
func (ps *ProcessorState) HasAllVariables(names []string) bool {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	for _, name := range names {
		if _, exists := ps.Variables[name]; !exists {
			return false
		}
	}
	return true
}

// GetVariableContext returns a map of variable names to values for JavaScript context
func (ps *ProcessorState) GetVariableContext() map[string]interface{} {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	context := make(map[string]interface{})
	for name, value := range ps.Variables {
		context[name] = value.Value
	}
	return context
}

// FillVariableContext fills a pooled context map with variable values
func (ps *ProcessorState) FillVariableContext(context map[string]interface{}) {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	for name, value := range ps.Variables {
		context[name] = value.Value
	}
}

// ClearVariables removes all variables from the state
func (ps *ProcessorState) ClearVariables() {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	ps.Variables = make(map[string]*VariableValue)
}

// RemoveVariable removes a specific variable from the state
func (ps *ProcessorState) RemoveVariable(name string) bool {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	_, exists := ps.Variables[name]
	if exists {
		delete(ps.Variables, name)
	}
	return exists
}

// GetVariableCount returns the number of variables currently stored
func (ps *ProcessorState) GetVariableCount() int {
	ps.mutex.RLock()
	defer ps.mutex.RUnlock()

	return len(ps.Variables)
}

// StateManager handles source-to-variable resolution and state coordination.
//
// It serves as the central coordinator between incoming UMH topics and the processor's
// internal variable state. The StateManager is responsible for:
//
//  1. TOPIC-TO-VARIABLE RESOLUTION: Converting incoming UMH topics like
//     "umh.v1.corpA.plant-A.aawd._raw.press" to variable names like "press" based on
//     the configured Sources mapping.
//
//  2. DEPENDENCY ANALYSIS: Determining which JavaScript mappings need to be
//     re-evaluated when a particular variable is updated.
//
//  3. EXECUTION PLANNING: Identifying which mappings can be executed based on
//     available variable dependencies.
//
//  4. STATIC MAPPING COORDINATION: Managing mappings that should be emitted
//     on every message regardless of variable updates.
//
// The StateManager acts as a facade over the ProcessorState, providing
// higher-level operations that understand the stream processor's configuration
// and business logic, while the ProcessorState handles the low-level
// thread-safe storage operations.
type StateManager struct {
	state  *ProcessorState
	config *StreamProcessorConfig
}

// NewStateManager creates a new state manager with the given configuration
func NewStateManager(config *StreamProcessorConfig) *StateManager {
	return &StateManager{
		state:  NewProcessorState(),
		config: config,
	}
}

// GetState returns the underlying processor state
func (sm *StateManager) GetState() *ProcessorState {
	return sm.state
}

// ResolveVariableFromTopic determines the variable name from an incoming UMH topic.
//
// This is a core method that enables the dependency-based evaluation system.
// It maps incoming UMH topics to variable names based on the configured Sources.
//
// Example: If Sources config contains {"press": "umh.v1.corpA.plant-A.aawd._raw.press"},
// then topic "umh.v1.corpA.plant-A.aawd._raw.press" resolves to variable "press".
//
// Returns the variable name and true if a mapping exists, empty string and false otherwise.
func (sm *StateManager) ResolveVariableFromTopic(topic string) (string, bool) {
	// Find which source this topic matches
	for variableName, sourceTopic := range sm.config.Sources {
		if topic == sourceTopic {
			return variableName, true
		}
	}
	return "", false
}

// GetDependentMappings returns all mappings that depend on the given variable.
//
// This method is essential for the performance optimization strategy - instead of
// re-evaluating all mappings when any variable changes, we only re-evaluate
// mappings that actually use the changed variable.
//
// Example: If variable "press" is updated, this method returns only mappings
// like "efficiency = press / target" or "quality = press > threshold", not
// unrelated mappings like "temperature_status = tF > 50".
//
// The dependencies are pre-calculated during startup through static analysis
// of the JavaScript expressions.
func (sm *StateManager) GetDependentMappings(variableName string) []MappingInfo {
	var dependentMappings []MappingInfo

	// Check dynamic mappings for dependencies
	for _, mapping := range sm.config.DynamicMappings {
		for _, dependency := range mapping.Dependencies {
			if dependency == variableName {
				dependentMappings = append(dependentMappings, mapping)
				break
			}
		}
	}

	return dependentMappings
}

// GetExecutableMappings returns mappings that can be executed based on current state
func (sm *StateManager) GetExecutableMappings(variableName string) []MappingInfo {
	var executableMappings []MappingInfo

	// Get all mappings that depend on this variable
	dependentMappings := sm.GetDependentMappings(variableName)

	// Filter to only those with all dependencies satisfied
	for _, mapping := range dependentMappings {
		if sm.state.HasAllVariables(mapping.Dependencies) {
			executableMappings = append(executableMappings, mapping)
		}
	}

	return executableMappings
}

// GetStaticMappings returns all static mappings that should be executed
func (sm *StateManager) GetStaticMappings() []MappingInfo {
	var staticMappings []MappingInfo

	for _, mapping := range sm.config.StaticMappings {
		staticMappings = append(staticMappings, mapping)
	}

	return staticMappings
}

// ValidateTopicIsConfiguredSource checks if a topic is configured as a source
func (sm *StateManager) ValidateTopicIsConfiguredSource(topic string) bool {
	for _, sourceTopic := range sm.config.Sources {
		if topic == sourceTopic {
			return true
		}
	}
	return false
}

// GetSourceTopics returns all configured source topics
func (sm *StateManager) GetSourceTopics() []string {
	topics := make([]string, 0, len(sm.config.Sources))
	for _, topic := range sm.config.Sources {
		topics = append(topics, topic)
	}
	return topics
}

// GetVariableInfo returns information about a variable including its source
func (sm *StateManager) GetVariableInfo(variableName string) (string, bool) {
	if sourceTopic, exists := sm.config.Sources[variableName]; exists {
		return sourceTopic, true
	}
	return "", false
}

// String returns a string representation of the state manager
func (sm *StateManager) String() string {
	return fmt.Sprintf("StateManager{variables: %d, sources: %d, static_mappings: %d, dynamic_mappings: %d}",
		sm.state.GetVariableCount(),
		len(sm.config.Sources),
		len(sm.config.StaticMappings),
		len(sm.config.DynamicMappings))
}
