package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// MockFileSystem is a mock implementation of the filesystem.Service interface for testing
type MockFileSystem struct {
	// EnsureDirectoryFunc mocks the EnsureDirectory method
	EnsureDirectoryFunc func(ctx context.Context, path string) error
	// ReadFileFunc mocks the ReadFile method
	ReadFileFunc func(ctx context.Context, path string) ([]byte, error)
	// WriteFileFunc mocks the WriteFile method
	WriteFileFunc func(ctx context.Context, path string, data []byte, perm os.FileMode) error
	// FileExistsFunc mocks the FileExists method
	FileExistsFunc func(ctx context.Context, path string) (bool, error)
	// RemoveFunc mocks the Remove method
	RemoveFunc func(ctx context.Context, path string) error
	// RemoveAllFunc mocks the RemoveAll method
	RemoveAllFunc func(ctx context.Context, path string) error
	// MkdirTempFunc mocks the MkdirTemp method
	MkdirTempFunc func(ctx context.Context, dir, pattern string) (string, error)
	// StatFunc mocks the Stat method
	StatFunc func(ctx context.Context, path string) (os.FileInfo, error)
	// CreateFileFunc mocks the CreateFile method
	CreateFileFunc func(ctx context.Context, path string, perm os.FileMode) (*os.File, error)
	// ChmodFunc mocks the Chmod method
	ChmodFunc func(ctx context.Context, path string, mode os.FileMode) error
	// ReadDirFunc mocks the ReadDir method
	ReadDirFunc func(ctx context.Context, path string) ([]os.DirEntry, error)
	// ExecuteCommandFunc mocks the ExecuteCommand method
	ExecuteCommandFunc func(ctx context.Context, name string, args ...string) ([]byte, error)
}

func (m *MockFileSystem) EnsureDirectory(ctx context.Context, path string) error {
	if m.EnsureDirectoryFunc != nil {
		return m.EnsureDirectoryFunc(ctx, path)
	}
	return nil
}

func (m *MockFileSystem) ReadFile(ctx context.Context, path string) ([]byte, error) {
	if m.ReadFileFunc != nil {
		return m.ReadFileFunc(ctx, path)
	}
	return []byte{}, nil
}

func (m *MockFileSystem) WriteFile(ctx context.Context, path string, data []byte, perm os.FileMode) error {
	if m.WriteFileFunc != nil {
		return m.WriteFileFunc(ctx, path, data, perm)
	}
	return nil
}

func (m *MockFileSystem) FileExists(ctx context.Context, path string) (bool, error) {
	if m.FileExistsFunc != nil {
		return m.FileExistsFunc(ctx, path)
	}
	return true, nil
}

func (m *MockFileSystem) Remove(ctx context.Context, path string) error {
	if m.RemoveFunc != nil {
		return m.RemoveFunc(ctx, path)
	}
	return nil
}

func (m *MockFileSystem) RemoveAll(ctx context.Context, path string) error {
	if m.RemoveAllFunc != nil {
		return m.RemoveAllFunc(ctx, path)
	}
	return nil
}

func (m *MockFileSystem) MkdirTemp(ctx context.Context, dir, pattern string) (string, error) {
	if m.MkdirTempFunc != nil {
		return m.MkdirTempFunc(ctx, dir, pattern)
	}
	return "", nil
}

func (m *MockFileSystem) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	if m.StatFunc != nil {
		return m.StatFunc(ctx, path)
	}
	return nil, nil
}

func (m *MockFileSystem) CreateFile(ctx context.Context, path string, perm os.FileMode) (*os.File, error) {
	if m.CreateFileFunc != nil {
		return m.CreateFileFunc(ctx, path, perm)
	}
	return nil, nil
}

func (m *MockFileSystem) Chmod(ctx context.Context, path string, mode os.FileMode) error {
	if m.ChmodFunc != nil {
		return m.ChmodFunc(ctx, path, mode)
	}
	return nil
}

func (m *MockFileSystem) ReadDir(ctx context.Context, path string) ([]os.DirEntry, error) {
	if m.ReadDirFunc != nil {
		return m.ReadDirFunc(ctx, path)
	}
	return nil, nil
}

func (m *MockFileSystem) ExecuteCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	if m.ExecuteCommandFunc != nil {
		return m.ExecuteCommandFunc(ctx, name, args...)
	}
	return []byte{}, nil
}

var _ = Describe("ConfigManager", func() {
	var (
		mockFS            *MockFileSystem
		configManager     *FileConfigManager
		testConfigPath    string
		ctx               context.Context
		ctxWithCancelFunc context.CancelFunc
	)

	BeforeEach(func() {
		mockFS = &MockFileSystem{}
		testConfigPath = "/test/config.yaml"

		// Create a context with a timeout for cancellation tests
		ctx = context.Background()
		ctxWithCancelFunc = func() {}
	})

	JustBeforeEach(func() {
		configManager = NewFileConfigManager(testConfigPath)
		configManager.WithFileSystemService(mockFS)
	})

	AfterEach(func() {
		// Clean up resources
		ctxWithCancelFunc()
	})

	Describe("NewFileConfigManager", func() {
		Context("when initialized with a path", func() {
			It("should use the provided path", func() {
				manager := NewFileConfigManager(testConfigPath)
				Expect(manager.configPath).To(Equal(testConfigPath))
			})
		})

		Context("when initialized with an empty path", func() {
			It("should use the default path", func() {
				manager := NewFileConfigManager("")
				Expect(manager.configPath).To(Equal(DefaultConfigPath))
			})
		})
	})

	Describe("GetConfig", func() {
		var (
			validYAML = `services:
- name: service1
  desiredState: running
  s6:
    command: ["/bin/echo", "hello world"]
    env:
      KEY: value
    configFiles:
      file.txt: content
`
			invalidYAML = `services: - invalid: yaml: content`
		)

		Context("when file exists and contains valid YAML", func() {
			BeforeEach(func() {
				mockFS.EnsureDirectoryFunc = func(ctx context.Context, path string) error {
					Expect(path).To(Equal(filepath.Dir(testConfigPath)))
					return nil
				}

				mockFS.FileExistsFunc = func(ctx context.Context, path string) (bool, error) {
					Expect(path).To(Equal(testConfigPath))
					return true, nil
				}

				mockFS.ReadFileFunc = func(ctx context.Context, path string) ([]byte, error) {
					Expect(path).To(Equal(testConfigPath))
					return []byte(validYAML), nil
				}
			})

			It("should return the parsed config", func() {
				config, err := configManager.GetConfig(ctx)
				Expect(err).NotTo(HaveOccurred())

				Expect(config.Services).To(HaveLen(1))
				Expect(config.Services[0].Name).To(Equal("service1"))
				Expect(config.Services[0].DesiredState).To(Equal("running"))
				Expect(config.Services[0].S6ServiceConfig.Command).To(Equal([]string{"/bin/echo", "hello world"}))
				Expect(config.Services[0].S6ServiceConfig.Env).To(HaveKeyWithValue("KEY", "value"))
				Expect(config.Services[0].S6ServiceConfig.ConfigFiles).To(HaveKeyWithValue("file.txt", "content"))
			})
		})

		Context("when file does not exist", func() {
			BeforeEach(func() {
				mockFS.EnsureDirectoryFunc = func(ctx context.Context, path string) error {
					return nil
				}

				mockFS.FileExistsFunc = func(ctx context.Context, path string) (bool, error) {
					return false, nil
				}
			})

			It("should return an empty config", func() {
				config, err := configManager.GetConfig(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(config.Services).To(BeEmpty())
			})
		})

		Context("when file exists but contains invalid YAML", func() {
			BeforeEach(func() {
				mockFS.EnsureDirectoryFunc = func(ctx context.Context, path string) error {
					return nil
				}

				mockFS.FileExistsFunc = func(ctx context.Context, path string) (bool, error) {
					return true, nil
				}

				mockFS.ReadFileFunc = func(ctx context.Context, path string) ([]byte, error) {
					return []byte(invalidYAML), nil
				}
			})

			It("should return an error", func() {
				_, err := configManager.GetConfig(ctx)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to parse config file"))
			})
		})

		Context("when EnsureDirectory fails", func() {
			BeforeEach(func() {
				mockFS.EnsureDirectoryFunc = func(ctx context.Context, path string) error {
					return errors.New("directory creation failed")
				}
			})

			It("should return an error", func() {
				_, err := configManager.GetConfig(ctx)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to create config directory"))
			})
		})

		Context("when FileExists fails", func() {
			BeforeEach(func() {
				mockFS.EnsureDirectoryFunc = func(ctx context.Context, path string) error {
					return nil
				}

				mockFS.FileExistsFunc = func(ctx context.Context, path string) (bool, error) {
					return false, errors.New("file check failed")
				}
			})

			It("should return an error", func() {
				_, err := configManager.GetConfig(ctx)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("file check failed"))
			})
		})

		Context("when ReadFile fails", func() {
			BeforeEach(func() {
				mockFS.EnsureDirectoryFunc = func(ctx context.Context, path string) error {
					return nil
				}

				mockFS.FileExistsFunc = func(ctx context.Context, path string) (bool, error) {
					return true, nil
				}

				mockFS.ReadFileFunc = func(ctx context.Context, path string) ([]byte, error) {
					return nil, errors.New("file read failed")
				}
			})

			It("should return an error", func() {
				_, err := configManager.GetConfig(ctx)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to read config file"))
			})
		})

		Context("when context is canceled", func() {
			BeforeEach(func() {
				// Create a context with cancel function
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(context.Background())
				ctxWithCancelFunc = cancel

				// Set up mock to block and check context
				mockFS.EnsureDirectoryFunc = func(ctx context.Context, path string) error {
					// Cancel the context
					cancel()
					// Wait a bit to ensure the cancellation propagates
					time.Sleep(10 * time.Millisecond)
					// Check if context is canceled
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						return fmt.Errorf("context should have been canceled")
					}
				}
			})

			It("should respect context cancellation", func() {
				_, err := configManager.GetConfig(ctx)
				Expect(err).To(HaveOccurred())
				// Check if the error contains context.Canceled by unwrapping it
				Expect(errors.Is(err, context.Canceled)).To(BeTrue(), "Expected error to wrap context.Canceled")
				// Also verify the error message contains the expected text
				Expect(err.Error()).To(ContainSubstring("context canceled"))
			})
		})
	})
})
