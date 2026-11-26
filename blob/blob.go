// Package blob stores opaque file bytes for a board's attachments.
//
// The board above chooses keys and content types; this package only keeps
// bytes. A store must write a key once and never change what it wrote: an
// attachment's ID names its bytes for good. Memory serves tests and
// development, FS a local board, and a deployment plugs in its own object
// store. A store that can sign direct-upload URLs also implements UploadSigner.
package blob

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrNotFound reports a key with no bytes.
var ErrNotFound = errors.New("blob: not found")

// Store keeps files by key. Every method must be safe for concurrent use.
type Store interface {
	// Put stores data under key. A key already written must not be replaced:
	// the board draws random IDs and never overwrites one.
	Put(ctx context.Context, key string, data []byte, contentType string) error
	// Get returns a key's bytes and content type.
	Get(ctx context.Context, key string) ([]byte, string, error)
	// Stat returns a key's size in bytes.
	Stat(ctx context.Context, key string) (int64, error)
	// Delete removes a key. Deleting a key that isn't there is not an error.
	Delete(ctx context.Context, key string) error
}

// SignOptions describe a direct upload's URL.
type SignOptions struct {
	ContentType string
	MaxBytes    int64
	ExpiresAt   time.Time
}

// SignedUpload is where a client PUTs a direct upload's bytes.
type SignedUpload struct {
	URL     string
	Headers map[string]string
}

// UploadSigner is a store that can hand out direct-upload URLs bound to one
// key, a maximum size, and an expiry. A board without one keeps uploads at the
// request body limit.
type UploadSigner interface {
	SignUpload(ctx context.Context, key string, opts SignOptions) (SignedUpload, error)
}

// Memory keeps files in the process.
type Memory struct {
	mu      sync.Mutex
	objects map[string]object
}

type object struct {
	data        []byte
	contentType string
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{objects: make(map[string]object)}
}

// Put implements Store.
func (m *Memory) Put(_ context.Context, key string, data []byte, contentType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.objects[key]; exists {
		return fmt.Errorf("blob: %s is already written", key)
	}
	m.objects[key] = object{data: append([]byte(nil), data...), contentType: contentType}
	return nil
}

// Get implements Store.
func (m *Memory) Get(_ context.Context, key string) ([]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	found, ok := m.objects[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	return append([]byte(nil), found.data...), found.contentType, nil
}

// Stat implements Store.
func (m *Memory) Stat(_ context.Context, key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	found, ok := m.objects[key]
	if !ok {
		return 0, ErrNotFound
	}
	return int64(len(found.data)), nil
}

// Delete implements Store.
func (m *Memory) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

// FS keeps files under a directory: each key is a path below it, with the
// content type beside it as "<key>.type".
type FS struct {
	mu  sync.RWMutex
	dir string
}

// NewFS returns a store rooted at dir, creating it if it doesn't exist.
func NewFS(dir string) (*FS, error) {
	if dir == "" {
		return nil, fmt.Errorf("blob: an FS store needs a directory")
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(absolute, 0o755); err != nil {
		return nil, err
	}
	return &FS{dir: absolute}, nil
}

// path maps a key to a path under the store's directory, refusing anything
// that could leave it.
func (f *FS) path(key string) (string, error) {
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, `\`) {
		return "", fmt.Errorf("blob: invalid key %q", key)
	}
	cleaned := filepath.Clean(key)
	if cleaned != key || cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("blob: invalid key %q", key)
	}
	for _, part := range strings.Split(cleaned, string(filepath.Separator)) {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("blob: invalid key %q", key)
		}
	}
	return filepath.Join(f.dir, cleaned), nil
}

// Put implements Store.
func (f *FS) Put(_ context.Context, key string, data []byte, contentType string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	path, err := f.path(key)
	if err != nil {
		return err
	}
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("blob: %s is already written", key)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	// Write the type first: a data file with no type is a half-written key.
	if err := writeFile(path+".type", []byte(contentType)); err != nil {
		return err
	}
	return writeFile(path, data)
}

// Get implements Store.
func (f *FS) Get(_ context.Context, key string) ([]byte, string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	path, err := f.path(key)
	if err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	contentType := "application/octet-stream"
	if stored, err := os.ReadFile(path + ".type"); err == nil {
		contentType = string(stored)
	}
	return data, contentType, nil
}

// Stat implements Store.
func (f *FS) Stat(_ context.Context, key string) (int64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	path, err := f.path(key)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// Delete implements Store.
func (f *FS) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	path, err := f.path(key)
	if err != nil {
		return err
	}
	for _, name := range []string{path, path + ".type"} {
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// Keys returns every key in a Memory store, sorted. It is for tests and
// administration; a real store needn't list.
func (m *Memory) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.objects))
	for key := range m.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func writeFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}
