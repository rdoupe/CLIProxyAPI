package poolstats

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

type memoryStore struct {
	mu   sync.Mutex
	data map[string][]byte
	mode string
}

func newMemoryStore() *memoryStore { return &memoryStore{data: map[string][]byte{}, mode: "kv"} }

func (s *memoryStore) Load(_ context.Context, key string, out *Bucket) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.data[key]
	if !ok {
		return false, nil
	}
	return true, json.Unmarshal(raw, out)
}

func (s *memoryStore) Save(_ context.Context, key string, bucket *Bucket, _ time.Duration) error {
	raw, err := json.Marshal(bucket)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = raw
	return nil
}

func (s *memoryStore) Persistence() string { return s.mode }

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func record(at time.Time, failed bool, tokens bool) coreusage.Record {
	rec := coreusage.Record{Provider: "Claude", Model: "claude-opus", AuthIndex: "acct-1", RequestedAt: at, Latency: 200 * time.Millisecond, Failed: failed}
	if tokens {
		rec.Detail = coreusage.Detail{InputTokens: 100, OutputTokens: 40, TotalTokens: 140}
	}
	return rec
}

func TestCollector_PersistsAcrossRestartWithoutDoubleCounting(t *testing.T) {
	store := newMemoryStore()
	clk := &clock{now: time.Date(2026, 11, 4, 10, 15, 0, 0, time.UTC)}
	first := NewCollector(store, clk.Now)
	ctx := context.Background()
	first.HandleUsage(ctx, record(clk.now, false, true))
	first.HandleUsage(ctx, record(clk.now, true, false))
	first.RecordFailover("claude", "claude-opus", "acct-2", clk.now)
	if err := first.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// A query after a flush in the same process must not add the persisted copy again.
	if got := first.Query(ctx, 24*time.Hour).Totals.Requests; got != 2 {
		t.Fatalf("same-process requests = %d, want 2", got)
	}

	// Restart in the same hour: new records must add to, not overwrite, saved totals.
	clk.now = clk.now.Add(10 * time.Minute)
	second := NewCollector(store, clk.Now)
	second.HandleUsage(ctx, record(clk.now, false, true))
	if err := second.Flush(ctx); err != nil {
		t.Fatalf("flush after restart: %v", err)
	}
	third := NewCollector(store, clk.Now)
	report := third.Query(ctx, 24*time.Hour)
	if !report.Available || report.Persistence != "kv" {
		t.Fatalf("report availability = %+v", report)
	}
	totals := report.Totals
	if totals.Requests != 3 || totals.Successes != 2 || totals.Failures != 1 || totals.Failovers != 1 {
		t.Fatalf("totals = %+v, want 3 requests / 2 ok / 1 failed / 1 failover", totals)
	}
	if totals.InputTokens == nil || *totals.InputTokens != 200 || totals.OutputTokens == nil || *totals.OutputTokens != 80 {
		t.Fatalf("tokens = %v/%v, want 200/80", totals.InputTokens, totals.OutputTokens)
	}
	if totals.AverageLatencyMs == nil || *totals.AverageLatencyMs != 200 {
		t.Fatalf("latency = %v, want 200", totals.AverageLatencyMs)
	}
	if len(report.Series) != 25 {
		t.Fatalf("series points = %d, want 25 hourly buckets", len(report.Series))
	}
	if len(report.ByProvider) != 1 || report.ByProvider[0].Name != "claude" {
		t.Fatalf("byProvider = %+v", report.ByProvider)
	}
	foundFailoverTarget := false
	for _, account := range report.ByAccount {
		if account.Name == "acct-2" && account.Failovers == 1 {
			foundFailoverTarget = true
		}
	}
	if !foundFailoverTarget {
		t.Fatalf("byAccount = %+v, want failover recorded on acct-2", report.ByAccount)
	}
}

func TestCollector_UnknownValuesStayNull(t *testing.T) {
	clk := &clock{now: time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)}
	c := NewCollector(newMemoryStore(), clk.Now)
	rec := record(clk.now, false, false)
	rec.Latency = 0
	c.HandleUsage(context.Background(), rec)
	report := c.Query(context.Background(), time.Hour)
	if report.Totals.Requests != 1 {
		t.Fatalf("requests = %d", report.Totals.Requests)
	}
	if report.Totals.InputTokens != nil || report.Totals.OutputTokens != nil || report.Totals.AverageLatencyMs != nil {
		t.Fatalf("unknown values must be null: %+v", report.Totals)
	}
	raw, _ := json.Marshal(report)
	for _, want := range []string{`"inputTokens":null`, `"averageLatencyMs":null`} {
		if !contains(string(raw), want) {
			t.Fatalf("json %s missing %s", raw, want)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func TestCollector_BoundsDimensions(t *testing.T) {
	clk := &clock{now: time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)}
	c := NewCollector(newMemoryStore(), clk.Now)
	for i := 0; i < MaxKeysPerDimension+50; i++ {
		rec := record(clk.now, false, false)
		rec.Model = fmt.Sprintf("model-%d", i)
		c.HandleUsage(context.Background(), rec)
	}
	report := c.Query(context.Background(), time.Hour)
	if len(report.ByModel) != MaxKeysPerDimension+1 {
		t.Fatalf("models = %d, want %d plus overflow", len(report.ByModel), MaxKeysPerDimension)
	}
	if report.Totals.Requests != int64(MaxKeysPerDimension+50) {
		t.Fatalf("totals must still count every request: %d", report.Totals.Requests)
	}
}

func TestCollector_MemoryModeReportsPersistence(t *testing.T) {
	store := newMemoryStore()
	store.mode = "memory"
	clk := &clock{now: time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)}
	c := NewCollector(store, clk.Now)
	c.HandleUsage(context.Background(), record(clk.now, false, true))
	if err := c.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(store.data) != 0 {
		t.Fatal("memory mode must not write")
	}
	if got := c.Query(context.Background(), time.Hour); got.Persistence != "memory" || got.Totals.Requests != 1 {
		t.Fatalf("memory report = %+v", got)
	}
}

func TestCollector_PluginDoesNotConsumeSharedStream(t *testing.T) {
	// The collector is one more named plugin on the usage manager: another
	// plugin registered beside it still receives every record.
	manager := coreusage.NewManager(16)
	clk := &clock{now: time.Now()}
	c := NewCollector(newMemoryStore(), clk.Now)
	var mu sync.Mutex
	seen := 0
	done := make(chan struct{}, 4)
	manager.RegisterNamed("pool-stats-test", c)
	manager.RegisterNamed("other-consumer", pluginFunc(func(context.Context, coreusage.Record) {
		mu.Lock()
		seen++
		mu.Unlock()
		done <- struct{}{}
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager.Start(ctx)
	defer manager.Stop()
	manager.Publish(ctx, record(clk.now, false, true))
	manager.Publish(ctx, record(clk.now, false, true))
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("other consumer did not receive records")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if seen != 2 {
		t.Fatalf("other consumer saw %d records, want 2", seen)
	}
}

type pluginFunc func(context.Context, coreusage.Record)

func (f pluginFunc) HandleUsage(ctx context.Context, record coreusage.Record) { f(ctx, record) }

func TestParseRange(t *testing.T) {
	for raw, want := range map[string]time.Duration{"": 24 * time.Hour, "1h": time.Hour, "24h": 24 * time.Hour, "7d": 7 * 24 * time.Hour, "30d": Retention} {
		got, err := ParseRange(raw)
		if err != nil || got != want {
			t.Fatalf("ParseRange(%q) = %v, %v; want %v", raw, got, err, want)
		}
	}
	for _, raw := range []string{"5m", "31d", "abc"} {
		if _, err := ParseRange(raw); err == nil {
			t.Fatalf("ParseRange(%q) should fail", raw)
		}
	}
}
