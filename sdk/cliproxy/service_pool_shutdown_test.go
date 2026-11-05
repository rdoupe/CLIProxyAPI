package cliproxy

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/poolstats"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

type shutdownStatsStore struct {
	mu    sync.Mutex
	saved map[string]poolstats.Bucket
}

func (s *shutdownStatsStore) Load(context.Context, string, *poolstats.Bucket) (bool, error) {
	return false, nil
}

func (s *shutdownStatsStore) Save(_ context.Context, key string, bucket *poolstats.Bucket, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved[key] = *bucket
	return nil
}

func (s *shutdownStatsStore) Persistence() string { return "kv" }

func (s *shutdownStatsStore) requests() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, bucket := range s.saved {
		total += bucket.Totals.Requests
	}
	return total
}

// gatePlugin holds the dispatcher on its first record until released, so the
// next record is still queued when shutdown begins.
type gatePlugin struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *gatePlugin) HandleUsage(context.Context, coreusage.Record) {
	p.once.Do(func() {
		close(p.entered)
		<-p.release
	})
}

// signalingDrainer reports when shutdown starts draining usage.
type signalingDrainer struct {
	*coreusage.Manager
	draining chan struct{}
}

func (d signalingDrainer) StopAndDrain(ctx context.Context) error {
	close(d.draining)
	return d.Manager.StopAndDrain(ctx)
}

func TestFlushPoolStateBeforeKVShutdownPersistsQueuedRecords(t *testing.T) {
	store := &shutdownStatsStore{saved: map[string]poolstats.Bucket{}}
	now := time.Now()
	collector := poolstats.NewCollector(store, func() time.Time { return now })
	usageManager := coreusage.NewManager(16)
	gate := &gatePlugin{entered: make(chan struct{}), release: make(chan struct{})}
	usageManager.Register(gate)
	usageManager.Register(collector)
	usageManager.Start(context.Background())

	record := coreusage.Record{Provider: "claude", Model: "m", AuthIndex: "a", RequestedAt: now}
	usageManager.Publish(context.Background(), record)
	<-gate.entered
	usageManager.Publish(context.Background(), record) // queued behind the gate

	drainer := signalingDrainer{Manager: usageManager, draining: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		flushPoolStateBeforeKVShutdown(context.Background(), drainer, nil, collector)
	}()
	<-drainer.draining // shutdown began before any record reached the collector
	if got := store.requests(); got != 0 {
		t.Fatalf("statistics were flushed before draining usage (saved %d)", got)
	}
	close(gate.release)
	<-done
	if got := store.requests(); got != 2 {
		t.Fatalf("persisted requests = %d, want both queued records saved before the KV store closes", got)
	}
}

func TestUsageStopAndDrainWaitsForQueuedRecords(t *testing.T) {
	usageManager := coreusage.NewManager(16)
	gate := &gatePlugin{entered: make(chan struct{}), release: make(chan struct{})}
	collector := poolstats.NewCollector(&shutdownStatsStore{saved: map[string]poolstats.Bucket{}}, nil)
	usageManager.Register(gate)
	usageManager.Register(collector)
	usageManager.Start(context.Background())
	usageManager.Publish(context.Background(), coreusage.Record{Provider: "claude", Model: "m"})
	<-gate.entered

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if err := usageManager.StopAndDrain(expired); err == nil {
		t.Fatal("StopAndDrain returned while a record was still being delivered")
	}
	close(gate.release)
	if err := usageManager.StopAndDrain(context.Background()); err != nil {
		t.Fatalf("StopAndDrain after release: %v", err)
	}
	if report := collector.Query(context.Background(), time.Hour); report.Totals.Requests != 1 {
		t.Fatalf("delivered requests = %d, want 1", report.Totals.Requests)
	}
}
