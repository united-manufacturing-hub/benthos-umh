package integration_test

// Some example "expected" system behavior constraints
const (
	maxAllocBytes        = 128 * 1024 * 1024 // 128 MB max heap usage
	maxErrorCount        = 0                 // zero error policy
	maxStarvedSeconds    = 0                 // zero starved seconds policy
	maxReconcileTime99th = 40.0              // 99th percentile under 40ms
)
