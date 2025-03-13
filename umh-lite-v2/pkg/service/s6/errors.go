package s6

import "errors"

var (
	// ErrServiceNotExist indicates that the service directory or run file does not exist
	ErrServiceNotExist = errors.New("service does not exist")
	// ErrServiceConfigMapNotFound indicates that a required config map is missing
	ErrServiceConfigMapNotFound = errors.New("service config map not found")
)
