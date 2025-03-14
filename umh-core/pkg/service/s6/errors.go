package s6

import "errors"

var (
	// ErrServiceNotExist indicates the requested service does not exist
	ErrServiceNotExist = errors.New("service does not exist")

	// ErrServiceConfigMapNotFound indicates the service config map was not found
	ErrServiceConfigMapNotFound = errors.New("service config map not found")
)
