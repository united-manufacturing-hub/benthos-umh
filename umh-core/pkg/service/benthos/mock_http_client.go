package benthos

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

// MockHTTPClient is a mock implementation of HTTPClient for testing
type MockHTTPClient struct {
	// ResponseMap maps endpoint paths to their mock responses
	ResponseMap map[string]MockResponse
}

// MockResponse represents a mock HTTP response
type MockResponse struct {
	StatusCode int
	Body       interface{}
	Error      error
	Delay      time.Duration // Simulates response delay for timeout testing
}

// NewMockHTTPClient creates a new MockHTTPClient with default healthy responses
func NewMockHTTPClient() *MockHTTPClient {
	return &MockHTTPClient{
		ResponseMap: map[string]MockResponse{
			"/ping": {
				StatusCode: http.StatusOK,
				Body:       nil,
			},
			"/ready": {
				StatusCode: http.StatusOK,
				Body: readyResponse{
					Statuses: []connStatus{
						{
							Label:     "",
							Path:      "input",
							Connected: true,
						},
						{
							Label:     "",
							Path:      "output",
							Connected: true,
						},
					},
				},
			},
			"/version": {
				StatusCode: http.StatusOK,
				Body: versionResponse{
					Version: "mock-version",
					Built:   "2025-02-24T12:50:06Z",
				},
			},
		},
	}
}

// SetResponse sets a mock response for a specific endpoint
func (m *MockHTTPClient) SetResponse(endpoint string, response MockResponse) {
	m.ResponseMap[endpoint] = response
}

// SetReadyStatus sets a custom ready status for testing different scenarios
func (m *MockHTTPClient) SetReadyStatus(statusCode int, inputConnected, outputConnected bool, err string) {
	statuses := []connStatus{
		{
			Path:      "input",
			Connected: inputConnected,
		},
		{
			Path:      "output",
			Connected: outputConnected,
		},
	}

	m.ResponseMap["/ready"] = MockResponse{
		StatusCode: statusCode,
		Body: readyResponse{
			Error:    err,
			Statuses: statuses,
		},
	}
}

// Do implements the HTTPClient interface
func (m *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	// Extract the path from the URL
	path := req.URL.Path

	// Get the mock response for this path
	mockResp, exists := m.ResponseMap[path]
	if !exists {
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	}

	// If there's a delay configured, simulate it
	if mockResp.Delay > 0 {
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(mockResp.Delay):
		}
	}

	// If there's an error configured, return it
	if mockResp.Error != nil {
		return nil, mockResp.Error
	}

	// Create response body
	var bodyReader io.Reader
	if mockResp.Body != nil {
		bodyBytes, err := json.Marshal(mockResp.Body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(bodyBytes)
	} else {
		bodyReader = strings.NewReader("")
	}

	// Create and return the response
	return &http.Response{
		StatusCode: mockResp.StatusCode,
		Body:       io.NopCloser(bodyReader),
		Header:     make(http.Header),
	}, nil
}
