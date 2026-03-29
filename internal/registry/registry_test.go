package registry_test

import (
	"sync"
	"testing"

	"github.com/GoCodeAlone/workflow-plugin-data-engineering/internal/registry"
)

func TestRegistry_RegisterLookupUnregister(t *testing.T) {
	r := registry.New[string]("item")

	if err := r.Register("a", "hello"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, err := r.Lookup("a")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got != "hello" {
		t.Errorf("Lookup: got %q, want %q", got, "hello")
	}

	r.Unregister("a")

	if _, err := r.Lookup("a"); err == nil {
		t.Error("expected error after Unregister, got nil")
	}
}

func TestRegistry_DuplicateRegister(t *testing.T) {
	r := registry.New[int]("widget")

	if err := r.Register("x", 1); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if err := r.Register("x", 2); err == nil {
		t.Fatal("expected error on duplicate Register, got nil")
	}
}

func TestRegistry_LookupNotFound(t *testing.T) {
	r := registry.New[float64]("number")

	_, err := r.Lookup("missing")
	if err == nil {
		t.Fatal("expected error for missing key, got nil")
	}
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	r := registry.New[int]("counter")

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i%26))
			_ = r.Register(key, i)
			_, _ = r.Lookup(key)
			r.Unregister(key)
		}(i)
	}
	wg.Wait()
}
