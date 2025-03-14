package logger

// This file contains example code for refactoring existing components to use
// standardized logging. These are examples only and are not meant to be used.

// Example of how to refactor S6Service to use standardized logger
/*
package s6

import (
	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/logger"
	"go.uber.org/zap"
)

type DefaultService struct {
	// Existing fields
	logger *zap.SugaredLogger
}

func NewDefaultService() *DefaultService {
	// Get component-specific logger
	log := logger.For(logger.ComponentS6Service)

	service := &DefaultService{
		// Initialize other fields
		logger: log,
	}

	return service
}

// Example of a method using the logger
func (s *DefaultService) CreateService(ctx context.Context, servicePath string) error {
	// Use component logger directly
	s.logger.Debugf("Creating S6 service %s", servicePath)

	// Rest of the method
	return nil
}

// Another example showing how to migrate existing code
// BEFORE:
// zap.S().Debugf("[S6Service] Creating S6 service %s", servicePath)
//
// AFTER:
// s.logger.Debugf("Creating S6 service %s", servicePath)
*/
