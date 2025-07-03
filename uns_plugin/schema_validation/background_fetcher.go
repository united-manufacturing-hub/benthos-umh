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

package schemavalidation

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxFetchQueueSize = 10               // Maximum number of contracts to fetch
	fetchInterval     = 5 * time.Second  // How often to check for new schemas
	httpTimeout       = 30 * time.Second // HTTP request timeout
)

// BackgroundFetcher handles background schema fetching from the registry
type BackgroundFetcher struct {
	schemaRegistryURL string
	httpClient        *http.Client
	validator         *Validator

	// Queue for contracts to fetch (limited to maxFetchQueueSize)
	fetchQueue      map[string]bool // "contractName-v123" -> true
	fetchQueueMutex sync.RWMutex

	// Control
	stopChan  chan struct{}
	stopped   bool
	stopMutex sync.RWMutex
}

// SchemaRegistrySubject represents a subject from the schema registry
type SchemaRegistrySubject struct {
	Subject string `json:"subject"`
}

// SchemaRegistryVersion represents a version response from the schema registry
type SchemaRegistryVersion struct {
	Version int    `json:"version"`
	ID      int    `json:"id"`
	Schema  string `json:"schema"`
}

// NewBackgroundFetcher creates a new background fetcher
func NewBackgroundFetcher(schemaRegistryURL string, validator *Validator) *BackgroundFetcher {
	return &BackgroundFetcher{
		schemaRegistryURL: schemaRegistryURL,
		httpClient: &http.Client{
			Timeout: httpTimeout,
		},
		validator:  validator,
		fetchQueue: make(map[string]bool),
		stopChan:   make(chan struct{}),
	}
}

// Start begins the background fetching goroutine
func (bf *BackgroundFetcher) Start() {
	go bf.fetchLoop()
}

// Stop stops the background fetching goroutine
func (bf *BackgroundFetcher) Stop() {
	bf.stopMutex.Lock()
	defer bf.stopMutex.Unlock()

	if !bf.stopped {
		close(bf.stopChan)
		bf.stopped = true
	}
}

// QueueSchema adds a contract-version combination to the fetch queue
func (bf *BackgroundFetcher) QueueSchema(contractName string, version uint64) {
	bf.fetchQueueMutex.Lock()
	defer bf.fetchQueueMutex.Unlock()

	// Check if queue is full
	if len(bf.fetchQueue) >= maxFetchQueueSize {
		// Queue is full, ignore this request
		return
	}

	key := fmt.Sprintf("%s-v%d", contractName, version)
	bf.fetchQueue[key] = true
}

// fetchLoop is the main background fetching loop
func (bf *BackgroundFetcher) fetchLoop() {
	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bf.stopChan:
			return
		case <-ticker.C:
			bf.processFetchQueue()
		}
	}
}

// processFetchQueue processes all items in the fetch queue
func (bf *BackgroundFetcher) processFetchQueue() {
	bf.fetchQueueMutex.Lock()
	currentQueue := make(map[string]bool)
	for k, v := range bf.fetchQueue {
		currentQueue[k] = v
	}
	// Clear the fetch queue
	bf.fetchQueue = make(map[string]bool)
	bf.fetchQueueMutex.Unlock()

	if len(currentQueue) == 0 {
		return
	}

	// Get all available subjects from the registry
	subjects, err := bf.getAllSubjects()
	if err != nil {
		// Log error and re-queue all items
		bf.fetchQueueMutex.Lock()
		for k, v := range currentQueue {
			if len(bf.fetchQueue) < maxFetchQueueSize {
				bf.fetchQueue[k] = v
			}
		}
		bf.fetchQueueMutex.Unlock()
		return
	}

	// Process each item in the queue
	for queueKey := range currentQueue {
		bf.processQueueItem(queueKey, subjects)
	}
}

// getAllSubjects fetches all subjects from the schema registry
func (bf *BackgroundFetcher) getAllSubjects() ([]string, error) {
	url := fmt.Sprintf("%s/subjects", bf.schemaRegistryURL)

	resp, err := bf.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch subjects from registry: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema registry returned status %d when fetching subjects", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read subjects response: %w", err)
	}

	var subjects []string
	if err := json.Unmarshal(body, &subjects); err != nil {
		return nil, fmt.Errorf("failed to unmarshal subjects response: %w", err)
	}

	return subjects, nil
}

// processQueueItem processes a single queue item
func (bf *BackgroundFetcher) processQueueItem(queueKey string, subjects []string) {
	// Parse the queue key to extract contract name and version
	contractName, version, err := bf.parseQueueKey(queueKey)
	if err != nil {
		// Invalid queue key, skip
		return
	}

	// Check if the contract exists in the subjects list
	found := false
	for _, subject := range subjects {
		if subject == contractName {
			found = true
			break
		}
	}

	if !found {
		// Contract doesn't exist in registry, don't re-queue
		return
	}

	// Check if the specific version exists
	versionExists, err := bf.checkVersionExists(contractName, version)
	if err != nil {
		// Network error, re-queue the item
		bf.reQueueItem(queueKey)
		return
	}

	if !versionExists {
		// Version doesn't exist, don't re-queue
		return
	}

	// Fetch the schema
	schema, err := bf.fetchSchema(contractName, version)
	if err != nil {
		// Error fetching schema, re-queue the item
		bf.reQueueItem(queueKey)
		return
	}

	// Load the schema into the validator
	if err := bf.validator.LoadSchema(contractName, version, schema); err != nil {
		// Schema loading failed, don't re-queue (likely a schema format issue)
		return
	}

	// Success! Schema loaded, don't re-queue
}

// parseQueueKey parses a queue key like "contractName-v123" into name and version
func (bf *BackgroundFetcher) parseQueueKey(queueKey string) (string, uint64, error) {
	parts := strings.Split(queueKey, "-v")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid queue key format: %s", queueKey)
	}

	contractName := parts[0]
	version, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid version in queue key %s: %w", queueKey, err)
	}

	return contractName, version, nil
}

// checkVersionExists checks if a specific version exists for a contract
func (bf *BackgroundFetcher) checkVersionExists(contractName string, version uint64) (bool, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions/%d", bf.schemaRegistryURL, contractName, version)

	resp, err := bf.httpClient.Get(url)
	if err != nil {
		return false, fmt.Errorf("failed to check version existence: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("schema registry returned status %d when checking version", resp.StatusCode)
	}

	return true, nil
}

// fetchSchema fetches the actual schema content
func (bf *BackgroundFetcher) fetchSchema(contractName string, version uint64) ([]byte, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions/%d", bf.schemaRegistryURL, contractName, version)

	resp, err := bf.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("schema not found for contract %s version %d", contractName, version)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("schema registry returned status %d when fetching schema", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema response: %w", err)
	}

	var versionResp SchemaRegistryVersion
	if err := json.Unmarshal(body, &versionResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema response: %w", err)
	}

	return []byte(versionResp.Schema), nil
}

// reQueueItem re-adds an item to the fetch queue if there's space
func (bf *BackgroundFetcher) reQueueItem(queueKey string) {
	bf.fetchQueueMutex.Lock()
	defer bf.fetchQueueMutex.Unlock()

	if len(bf.fetchQueue) < maxFetchQueueSize {
		bf.fetchQueue[queueKey] = true
	}
}
