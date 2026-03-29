// Package registry provides a generic thread-safe key-value registry.
package registry

import (
	"fmt"
	"sync"
)

// Registry is a generic thread-safe map from string keys to values of type T.
type Registry[T any] struct {
	mu    sync.RWMutex
	items map[string]T
	kind  string
}

// New creates a Registry for the given item kind (used in error messages).
func New[T any](kind string) *Registry[T] {
	return &Registry[T]{items: make(map[string]T), kind: kind}
}

// Register adds item under name. Returns an error if name is already registered.
func (r *Registry[T]) Register(name string, item T) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.items[name]; exists {
		return fmt.Errorf("%s %q already registered", r.kind, name)
	}
	r.items[name] = item
	return nil
}

// Unregister removes the item registered under name, if any.
func (r *Registry[T]) Unregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.items, name)
}

// Lookup returns the item registered under name, or an error if not found.
func (r *Registry[T]) Lookup(name string) (T, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	item, ok := r.items[name]
	if !ok {
		var zero T
		return zero, fmt.Errorf("%s %q not found", r.kind, name)
	}
	return item, nil
}
