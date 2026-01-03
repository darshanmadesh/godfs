package master

import (
	"errors"
	"sync"
	"time"
)

// Common errors returned by the metadata store.
// In Go, we define errors as package-level variables so callers can check
// for specific error types using errors.Is().
var (
	ErrFileNotFound      = errors.New("file not found")
	ErrFileAlreadyExists = errors.New("file already exists")
)

// FileMeta represents metadata for a single file in the DFS.
// This struct will grow as we add features (chunks, replicas, etc.)
type FileMeta struct {
	Filename   string
	Size       int64
	CreatedAt  time.Time
	ModifiedAt time.Time
	// Future fields:
	// Chunks     []string  // List of chunk IDs
	// Checksum   string    // File integrity hash
}

// MetadataStore defines the interface for metadata operations.
// Using an interface allows us to:
// 1. Swap implementations (in-memory -> database) without changing server code
// 2. Create mock implementations for testing
// 3. Document the contract clearly
type MetadataStore interface {
	// Create adds a new file's metadata. Returns ErrFileAlreadyExists if file exists.
	Create(meta *FileMeta) error

	// Get retrieves metadata for a file. Returns ErrFileNotFound if not found.
	Get(filename string) (*FileMeta, error)

	// Update modifies an existing file's metadata. Returns ErrFileNotFound if not found.
	Update(meta *FileMeta) error

	// Delete removes a file's metadata. Returns ErrFileNotFound if not found.
	Delete(filename string) error

	// List returns all files matching the optional prefix filter.
	// Pass empty string to list all files.
	List(prefix string) ([]*FileMeta, error)

	// Exists checks if a file exists without returning full metadata.
	Exists(filename string) bool
}

// InMemoryMetadataStore implements MetadataStore using an in-memory map.
// This is perfect for Phase 1 learning - simple and fast.
// In production, you'd use a persistent store (etcd, PostgreSQL, etc.)
type InMemoryMetadataStore struct {
	// mu protects concurrent access to the files map.
	// RWMutex allows multiple concurrent readers OR one writer.
	// This is more efficient than a regular Mutex when reads are common.
	mu sync.RWMutex

	// files maps filename -> metadata
	files map[string]*FileMeta
}

// NewInMemoryMetadataStore creates a new in-memory metadata store.
// In Go, constructor functions are named New<Type> by convention.
func NewInMemoryMetadataStore() *InMemoryMetadataStore {
	return &InMemoryMetadataStore{
		files: make(map[string]*FileMeta),
	}
}

// Create adds new file metadata to the store.
func (s *InMemoryMetadataStore) Create(meta *FileMeta) error {
	s.mu.Lock()         // Acquire write lock
	defer s.mu.Unlock() // Release lock when function returns (defer is LIFO)

	// Check if file already exists
	if _, exists := s.files[meta.Filename]; exists {
		return ErrFileAlreadyExists
	}

	// Set timestamps if not provided
	now := time.Now()
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = now
	}
	if meta.ModifiedAt.IsZero() {
		meta.ModifiedAt = now
	}

	// Store a copy to prevent external modification
	// This is defensive programming - the caller can't accidentally
	// modify our internal state after Create() returns
	stored := *meta
	s.files[meta.Filename] = &stored

	return nil
}

// Get retrieves file metadata by filename.
func (s *InMemoryMetadataStore) Get(filename string) (*FileMeta, error) {
	s.mu.RLock()         // Acquire read lock (multiple readers allowed)
	defer s.mu.RUnlock() // Release read lock

	meta, exists := s.files[filename]
	if !exists {
		return nil, ErrFileNotFound
	}

	// Return a copy to prevent external modification of our data
	result := *meta
	return &result, nil
}

// Update modifies existing file metadata.
func (s *InMemoryMetadataStore) Update(meta *FileMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.files[meta.Filename]; !exists {
		return ErrFileNotFound
	}

	// Update modification time
	meta.ModifiedAt = time.Now()

	// Store a copy
	stored := *meta
	s.files[meta.Filename] = &stored

	return nil
}

// Delete removes file metadata from the store.
func (s *InMemoryMetadataStore) Delete(filename string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.files[filename]; !exists {
		return ErrFileNotFound
	}

	delete(s.files, filename)
	return nil
}

// List returns all files matching the prefix filter.
func (s *InMemoryMetadataStore) List(prefix string) ([]*FileMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Pre-allocate slice with estimated capacity for efficiency
	// This avoids repeated allocations as the slice grows
	result := make([]*FileMeta, 0, len(s.files))

	for filename, meta := range s.files {
		// If prefix is empty, include all files
		// Otherwise, check if filename starts with prefix
		if prefix == "" || len(filename) >= len(prefix) && filename[:len(prefix)] == prefix {
			// Return copies to prevent external modification
			copy := *meta
			result = append(result, &copy)
		}
	}

	return result, nil
}

// Exists checks if a file exists in the store.
func (s *InMemoryMetadataStore) Exists(filename string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.files[filename]
	return exists
}

// Compile-time check that InMemoryMetadataStore implements MetadataStore.
// This is a Go idiom - if the implementation is wrong, you get a compile error
// rather than a runtime error.
var _ MetadataStore = (*InMemoryMetadataStore)(nil)
