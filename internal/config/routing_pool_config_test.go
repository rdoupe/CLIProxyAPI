package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRoutingPoolConfigRoundTripsInBothLayouts(t *testing.T) {
	for _, v8 := range []bool{false, true} {
		dir := t.TempDir()
		path := filepath.Join(dir, "config.yaml")
		if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		allow := false
		cfg := &Config{}
		cfg.Routing.Pool = RoutingPoolConfig{Strategy: "subscriptions-first", AllowPaidFallback: &allow}
		if err := SaveConfigPreserveComments(path, cfg, v8); err != nil {
			t.Fatalf("save (v8=%v): %v", v8, err)
		}
		saved, _ := os.ReadFile(path)
		if !strings.Contains(string(saved), "allow-paid-fallback: false") {
			t.Fatalf("v8=%v saved config dropped allow-paid-fallback:\n%s", v8, saved)
		}
		loaded, err := LoadConfig(path)
		if err != nil {
			t.Fatalf("load (v8=%v): %v", v8, err)
		}
		if loaded.Routing.Pool.Strategy != "subscriptions-first" || loaded.Routing.Pool.AllowPaidFallback == nil || *loaded.Routing.Pool.AllowPaidFallback {
			t.Fatalf("v8=%v reloaded pool config = %+v", v8, loaded.Routing.Pool)
		}
	}
}

func TestRoutingPoolConfigDefaultsAreNotWritten(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := SaveConfigPreserveComments(path, &Config{}, false); err != nil {
		t.Fatalf("save: %v", err)
	}
	saved, _ := os.ReadFile(path)
	if strings.Contains(string(saved), "pool:") {
		t.Fatalf("default pool policy must not be written:\n%s", saved)
	}
}
