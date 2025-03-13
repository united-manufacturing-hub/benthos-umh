package filesystem

import (
	"context"
	"fmt"
	"os"
	"os/exec"
)

// Service provides an interface for filesystem operations
// This allows for easier testing and separation of concerns
type Service interface {
	// EnsureDirectory creates a directory if it doesn't exist
	EnsureDirectory(ctx context.Context, path string) error

	// ReadFile reads a file's contents respecting the context
	ReadFile(ctx context.Context, path string) ([]byte, error)

	// WriteFile writes data to a file respecting the context
	WriteFile(ctx context.Context, path string, data []byte, perm os.FileMode) error

	// FileExists checks if a file exists
	FileExists(ctx context.Context, path string) (bool, error)

	// Remove removes a file or directory
	Remove(ctx context.Context, path string) error

	// RemoveAll removes a directory and all its contents
	RemoveAll(ctx context.Context, path string) error

	// MkdirTemp creates a new temporary directory
	MkdirTemp(ctx context.Context, dir, pattern string) (string, error)

	// Stat returns file info
	Stat(ctx context.Context, path string) (os.FileInfo, error)

	// CreateFile creates a new file with the specified permissions
	CreateFile(ctx context.Context, path string, perm os.FileMode) (*os.File, error)

	// Chmod changes the mode of the named file
	Chmod(ctx context.Context, path string, mode os.FileMode) error

	// ReadDir reads a directory, returning all its directory entries
	ReadDir(ctx context.Context, path string) ([]os.DirEntry, error)

	// ExecuteCommand executes a command with context
	ExecuteCommand(ctx context.Context, name string, args ...string) ([]byte, error)
}

// DefaultService is the default implementation of FileSystemService
type DefaultService struct{}

// NewDefaultService creates a new DefaultFileSystemService
func NewDefaultService() *DefaultService {
	return &DefaultService{}
}

// checkContext checks if the context is done before proceeding with an operation
func (s *DefaultService) checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// EnsureDirectory creates a directory if it doesn't exist
func (s *DefaultService) EnsureDirectory(ctx context.Context, path string) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	return os.MkdirAll(path, 0755)
}

// ReadFile reads a file's contents respecting the context
func (s *DefaultService) ReadFile(ctx context.Context, path string) ([]byte, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

// WriteFile writes data to a file respecting the context
func (s *DefaultService) WriteFile(ctx context.Context, path string, data []byte, perm os.FileMode) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	return os.WriteFile(path, data, perm)
}

// FileExists checks if a file exists
func (s *DefaultService) FileExists(ctx context.Context, path string) (bool, error) {
	if err := s.checkContext(ctx); err != nil {
		return false, err
	}

	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check if file exists: %w", err)
	}
	return true, nil
}

// Remove removes a file or directory
func (s *DefaultService) Remove(ctx context.Context, path string) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	return os.Remove(path)
}

// RemoveAll removes a directory and all its contents
func (s *DefaultService) RemoveAll(ctx context.Context, path string) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	return os.RemoveAll(path)
}

// MkdirTemp creates a new temporary directory
func (s *DefaultService) MkdirTemp(ctx context.Context, dir, pattern string) (string, error) {
	if err := s.checkContext(ctx); err != nil {
		return "", err
	}
	return os.MkdirTemp(dir, pattern)
}

// Stat returns file info
func (s *DefaultService) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}
	return os.Stat(path)
}

// CreateFile creates a new file with the specified permissions
func (s *DefaultService) CreateFile(ctx context.Context, path string, perm os.FileMode) (*os.File, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, perm)
}

// Chmod changes the mode of the named file
func (s *DefaultService) Chmod(ctx context.Context, path string, mode os.FileMode) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	return os.Chmod(path, mode)
}

// ReadDir reads a directory, returning all its directory entries
func (s *DefaultService) ReadDir(ctx context.Context, path string) ([]os.DirEntry, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}
	return os.ReadDir(path)
}

// ExecuteCommand executes a command with context
func (s *DefaultService) ExecuteCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, name, args...)
	return cmd.CombinedOutput()
}
