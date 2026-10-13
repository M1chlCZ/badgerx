package main

import (
	"path/filepath"
	"testing"
)

func TestRequireIsolatedPaths(t *testing.T) {
	t.Parallel()
	if err := requireIsolatedPaths(filepath.Join(t.TempDir(), "store")); err != nil {
		t.Fatalf("absolute path rejected: %v", err)
	}
	if err := requireIsolatedPaths("/"); err == nil {
		t.Fatal("root path accepted")
	}
	if err := requireIsolatedPaths("store"); err == nil {
		t.Fatal("relative path accepted")
	}
	if err := requireIsolatedPaths("/absolute", "relative"); err == nil {
		t.Fatal("mixed paths accepted")
	}
}
