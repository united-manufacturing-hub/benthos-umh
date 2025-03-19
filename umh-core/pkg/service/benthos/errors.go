package benthos

import "errors"

var (
	// ErrServiceNotExist indicates the requested service does not exist
	ErrServiceNotExist = errors.New("service does not exist")

	// ErrServiceAlreadyExists indicates the requested service already exists
	ErrServiceAlreadyExists = errors.New("service already exists")
)
