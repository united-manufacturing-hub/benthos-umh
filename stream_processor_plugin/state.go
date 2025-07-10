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

package stream_processor_plugin

import (
	"fmt"
	"time"
)

// Note: ProcessorState and VariableValue are defined in processor.go
// This file extends them with additional functionality

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

// StateManager handles source-to-variable resolution and state coordination
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

// ResolveVariableFromTopic determines the variable name from an incoming UMH topic
func (sm *StateManager) ResolveVariableFromTopic(topic string) (string, bool) {
	// Find which source this topic matches
	for variableName, sourceTopic := range sm.config.Sources {
		if topic == sourceTopic {
			return variableName, true
		}
	}
	return "", false
}

// GetDependentMappings returns all mappings that depend on the given variable
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
