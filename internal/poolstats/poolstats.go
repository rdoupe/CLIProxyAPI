// Package poolstats keeps bounded, durable request statistics for the account
// pool. It observes the shared usage stream as one more usage plugin, so it
// never consumes or reorders records other consumers (such as the usage queue)
// rely on. Values the providers did not report stay unknown (nil) instead of
// being estimated.
package poolstats

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/kvstore"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

const (
	// BucketSize is the width of one time-series bucket.
	BucketSize = time.Hour
	// Retention bounds how long buckets are kept in memory and in the KV store.
	Retention = 30 * 24 * time.Hour
	// MaxKeysPerDimension bounds provider/model/account maps inside one bucket.
	MaxKeysPerDimension = 256
	// OverflowKey collects entries beyond MaxKeysPerDimension.
	OverflowKey = "(other)"
	// UnknownKey labels records without the dimension value.
	UnknownKey = "(unknown)"

	kvKeyPrefix   = "cliproxy:pool:stats:v1:"
	flushInterval = 30 * time.Second
	pluginName    = "pool-stats"
	bucketSeconds = int64(BucketSize / time.Second)
)

// Store persists stats buckets.
type Store interface {
	Load(ctx context.Context, key string, out *Bucket) (bool, error)
	Save(ctx context.Context, key string, bucket *Bucket, ttl time.Duration) error
	Persistence() string
}

// KVStore persists buckets in the configured KV store (Home KV or local bbolt).
type KVStore struct{}

// Load implements Store.
func (KVStore) Load(ctx context.Context, key string, out *Bucket) (bool, error) {
	_, found, err := kvstore.KVGetJSONRequired(ctx, key, out)
	return found, err
}

// Save implements Store.
func (KVStore) Save(ctx context.Context, key string, bucket *Bucket, ttl time.Duration) error {
	_, err := kvstore.KVSetJSONRequired(ctx, key, bucket, ttl)
	return err
}

// Persistence implements Store.
func (KVStore) Persistence() string {
	if _, kvMode, err := kvstore.CurrentKVClient(); kvMode && err == nil {
		return "kv"
	}
	return "memory"
}

// MemoryStore keeps statistics in this process only (used in Home mode, where
// the Home KV is shared by every node).
type MemoryStore struct{}

// Load implements Store.
func (MemoryStore) Load(context.Context, string, *Bucket) (bool, error) { return false, nil }

// Save implements Store.
func (MemoryStore) Save(context.Context, string, *Bucket, time.Duration) error { return nil }

// Persistence implements Store.
func (MemoryStore) Persistence() string { return "memory" }

// Counts are additive counters for one bucket and dimension value.
type Counts struct {
	Requests       int64 `json:"requests"`
	Successes      int64 `json:"successes"`
	Failures       int64 `json:"failures"`
	Failovers      int64 `json:"failovers"`
	InputTokens    int64 `json:"inputTokens"`
	OutputTokens   int64 `json:"outputTokens"`
	TokenSamples   int64 `json:"tokenSamples"`
	LatencyTotalMs int64 `json:"latencyTotalMs"`
	LatencySamples int64 `json:"latencySamples"`
}

func (c *Counts) add(other Counts) {
	c.Requests += other.Requests
	c.Successes += other.Successes
	c.Failures += other.Failures
	c.Failovers += other.Failovers
	c.InputTokens += other.InputTokens
	c.OutputTokens += other.OutputTokens
	c.TokenSamples += other.TokenSamples
	c.LatencyTotalMs += other.LatencyTotalMs
	c.LatencySamples += other.LatencySamples
}

// Bucket holds one hour of statistics.
type Bucket struct {
	Start     int64              `json:"start"`
	Totals    Counts             `json:"totals"`
	Providers map[string]*Counts `json:"providers,omitempty"`
	Models    map[string]*Counts `json:"models,omitempty"`
	Accounts  map[string]*Counts `json:"accounts,omitempty"`
}

func newBucket(start int64) *Bucket {
	return &Bucket{Start: start, Providers: map[string]*Counts{}, Models: map[string]*Counts{}, Accounts: map[string]*Counts{}}
}

func dimension(dim map[string]*Counts, key string) *Counts {
	key = strings.TrimSpace(key)
	if key == "" {
		key = UnknownKey
	}
	if counts := dim[key]; counts != nil {
		return counts
	}
	limit := MaxKeysPerDimension
	if _, hasOverflow := dim[OverflowKey]; hasOverflow {
		limit++
	}
	if key != OverflowKey && len(dim) >= limit {
		key = OverflowKey
		if counts := dim[key]; counts != nil {
			return counts
		}
	}
	counts := &Counts{}
	dim[key] = counts
	return counts
}

func (b *Bucket) ensureMaps() {
	if b.Providers == nil {
		b.Providers = map[string]*Counts{}
	}
	if b.Models == nil {
		b.Models = map[string]*Counts{}
	}
	if b.Accounts == nil {
		b.Accounts = map[string]*Counts{}
	}
}

func (b *Bucket) apply(provider, model, account string, delta Counts) {
	b.ensureMaps()
	b.Totals.add(delta)
	dimension(b.Providers, provider).add(delta)
	dimension(b.Models, model).add(delta)
	dimension(b.Accounts, account).add(delta)
}

// Collector aggregates usage records into hourly buckets.
type Collector struct {
	mu        sync.Mutex
	store     Store
	now       func() time.Time
	buckets   map[int64]*Bucket
	loaded    map[int64]bool
	dirty     map[int64]bool
	lastFlush time.Time
	started   time.Time
	flushMu   sync.Mutex
}

// NewCollector creates a collector backed by store (KVStore when nil).
func NewCollector(store Store, now func() time.Time) *Collector {
	if store == nil {
		store = KVStore{}
	}
	if now == nil {
		now = time.Now
	}
	start := now()
	return &Collector{store: store, now: now, buckets: map[int64]*Bucket{}, loaded: map[int64]bool{}, dirty: map[int64]bool{}, started: start, lastFlush: start}
}

// SetStore replaces the persistence backend.
func (c *Collector) SetStore(store Store) {
	if c == nil || store == nil {
		return
	}
	c.mu.Lock()
	c.store = store
	c.mu.Unlock()
}

var (
	defaultMu        sync.Mutex
	defaultCollector *Collector
)

// Default returns the process collector, or nil when Register was never called.
func Default() *Collector {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	return defaultCollector
}

// Register installs the process collector as a named usage plugin. It is idempotent.
func Register() *Collector {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	if defaultCollector == nil {
		defaultCollector = NewCollector(nil, nil)
		coreusage.RegisterNamedPlugin(pluginName, defaultCollector)
	}
	return defaultCollector
}

func bucketStart(at time.Time) int64 {
	return at.UTC().Truncate(BucketSize).Unix()
}

func bucketKey(start int64) string {
	return kvKeyPrefix + strconv.FormatInt(start, 10)
}

// bucketLocked returns the bucket for start, creating it if needed. Caller holds c.mu.
func (c *Collector) bucketLocked(start int64) *Bucket {
	bucket := c.buckets[start]
	if bucket == nil {
		bucket = newBucket(start)
		c.buckets[start] = bucket
	}
	c.dirty[start] = true
	return bucket
}

func (c *Collector) pruneLocked(now time.Time) {
	cutoff := bucketStart(now.Add(-Retention))
	for start := range c.buckets {
		if start < cutoff {
			delete(c.buckets, start)
			delete(c.dirty, start)
		}
	}
	for start := range c.loaded {
		if start < cutoff {
			delete(c.loaded, start)
		}
	}
}

// HandleUsage implements coreusage.Plugin. It runs on the usage dispatcher, not on the request path.
func (c *Collector) HandleUsage(ctx context.Context, record coreusage.Record) {
	if c == nil {
		return
	}
	at := record.RequestedAt
	if at.IsZero() {
		at = c.now()
	}
	delta := Counts{Requests: 1}
	if record.Failed {
		delta.Failures = 1
	} else {
		delta.Successes = 1
	}
	if record.Latency > 0 {
		delta.LatencyTotalMs = record.Latency.Milliseconds()
		delta.LatencySamples = 1
	}
	if detail := record.Detail; detail.InputTokens > 0 || detail.OutputTokens > 0 || detail.TotalTokens > 0 {
		delta.InputTokens = detail.InputTokens
		delta.OutputTokens = detail.OutputTokens
		delta.TokenSamples = 1
	}
	model := record.Alias
	if strings.TrimSpace(model) == "" {
		model = record.Model
	}
	c.mu.Lock()
	c.bucketLocked(bucketStart(at)).apply(strings.ToLower(strings.TrimSpace(record.Provider)), model, record.AuthIndex, delta)
	due := c.now().Sub(c.lastFlush) >= flushInterval
	c.mu.Unlock()
	if due {
		if err := c.Flush(ctx); err != nil {
			log.Warnf("pool stats: persist failed: %v", err)
		}
	}
}

// RecordFailover counts a request that moved to another account after a failure.
// accountID is the account the request moved to.
func (c *Collector) RecordFailover(provider, model, accountID string, at time.Time) {
	if c == nil {
		return
	}
	if at.IsZero() {
		at = c.now()
	}
	c.mu.Lock()
	c.bucketLocked(bucketStart(at)).apply(strings.ToLower(strings.TrimSpace(provider)), model, accountID, Counts{Failovers: 1})
	c.mu.Unlock()
}

// Flush persists dirty buckets. A bucket is merged with its persisted copy
// before its first write, so a restart never overwrites earlier counts.
func (c *Collector) Flush(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.flushMu.Lock()
	defer c.flushMu.Unlock()
	c.mu.Lock()
	now := c.now()
	c.pruneLocked(now)
	c.lastFlush = now
	store := c.store
	var unloaded []int64
	for start := range c.dirty {
		if !c.loaded[start] {
			unloaded = append(unloaded, start)
		}
	}
	c.mu.Unlock()
	if store.Persistence() != "kv" {
		// Memory-only mode: keep buckets dirty so they are written once a KV store appears.
		return nil
	}
	firstErr := c.loadStarts(ctx, unloaded)

	c.mu.Lock()
	pending := make([]*Bucket, 0, len(c.dirty))
	for start := range c.dirty {
		if !c.loaded[start] {
			continue
		}
		if bucket := c.buckets[start]; bucket != nil {
			pending = append(pending, cloneBucket(bucket))
		}
		delete(c.dirty, start)
	}
	c.mu.Unlock()
	for _, bucket := range pending {
		ttl := time.Unix(bucket.Start, 0).Add(Retention + BucketSize).Sub(now)
		if ttl <= 0 {
			continue
		}
		if err := store.Save(ctx, bucketKey(bucket.Start), bucket, ttl); err != nil {
			c.mu.Lock()
			c.dirty[bucket.Start] = true
			c.mu.Unlock()
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func cloneBucket(bucket *Bucket) *Bucket {
	out := newBucket(bucket.Start)
	out.Totals = bucket.Totals
	for _, pair := range []struct{ dst, src map[string]*Counts }{{out.Providers, bucket.Providers}, {out.Models, bucket.Models}, {out.Accounts, bucket.Accounts}} {
		for key, value := range pair.src {
			if value != nil {
				copied := *value
				pair.dst[key] = &copied
			}
		}
	}
	return out
}

func mergeBucket(dst, src *Bucket) {
	dst.ensureMaps()
	dst.Totals.add(src.Totals)
	for _, pair := range []struct{ dst, src map[string]*Counts }{{dst.Providers, src.Providers}, {dst.Models, src.Models}, {dst.Accounts, src.Accounts}} {
		for key, value := range pair.src {
			if value != nil {
				dimension(pair.dst, key).add(*value)
			}
		}
	}
}

// ensureLoaded reads persisted buckets for [from, to] that are not in memory yet.
func (c *Collector) ensureLoaded(ctx context.Context, from, to int64) {
	c.mu.Lock()
	var missing []int64
	for start := from; start <= to; start += bucketSeconds {
		if !c.loaded[start] {
			missing = append(missing, start)
		}
	}
	store := c.store
	c.mu.Unlock()
	if len(missing) == 0 || store.Persistence() != "kv" {
		return
	}
	if err := c.loadStarts(ctx, missing); err != nil {
		log.Warnf("pool stats: load buckets failed: %v", err)
	}
}

// loadStarts merges persisted buckets into memory and marks them loaded. Live
// buckets of a not-yet-loaded hour hold only this process's deltas (they are
// never written before being loaded), so adding the persisted copy is exact.
func (c *Collector) loadStarts(ctx context.Context, starts []int64) error {
	c.mu.Lock()
	store := c.store
	c.mu.Unlock()
	var firstErr error
	for _, start := range starts {
		var persisted Bucket
		found, err := store.Load(ctx, bucketKey(start), &persisted)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		c.mu.Lock()
		if !c.loaded[start] {
			c.loaded[start] = true
			if found {
				persisted.Start = start
				if live := c.buckets[start]; live != nil {
					mergeBucket(live, &persisted)
				} else {
					persisted.ensureMaps()
					c.buckets[start] = &persisted
				}
			}
		}
		c.mu.Unlock()
	}
	return firstErr
}

// ParseRange parses a stats range such as "1h", "24h", "7d" or "30d".
func ParseRange(raw string) (time.Duration, error) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return 24 * time.Hour, nil
	}
	var value time.Duration
	if strings.HasSuffix(raw, "d") {
		days, err := strconv.Atoi(strings.TrimSuffix(raw, "d"))
		if err != nil {
			return 0, fmt.Errorf("invalid range %q", raw)
		}
		value = time.Duration(days) * 24 * time.Hour
	} else {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return 0, fmt.Errorf("invalid range %q", raw)
		}
		value = parsed
	}
	if value < BucketSize || value > Retention {
		return 0, fmt.Errorf("range must be between 1h and 30d")
	}
	return value, nil
}

// Totals is the JSON shape of aggregated counters. Unknown values are nil.
type Totals struct {
	Requests         int64    `json:"requests"`
	Successes        int64    `json:"successes"`
	Failures         int64    `json:"failures"`
	Failovers        int64    `json:"failovers"`
	InputTokens      *int64   `json:"inputTokens"`
	OutputTokens     *int64   `json:"outputTokens"`
	AverageLatencyMs *float64 `json:"averageLatencyMs"`
}

func totalsFrom(counts Counts) Totals {
	out := Totals{Requests: counts.Requests, Successes: counts.Successes, Failures: counts.Failures, Failovers: counts.Failovers}
	if counts.TokenSamples > 0 {
		input, output := counts.InputTokens, counts.OutputTokens
		out.InputTokens, out.OutputTokens = &input, &output
	}
	if counts.LatencySamples > 0 {
		average := float64(counts.LatencyTotalMs) / float64(counts.LatencySamples)
		out.AverageLatencyMs = &average
	}
	return out
}

// SeriesPoint is one bucket in the time series.
type SeriesPoint struct {
	At time.Time `json:"at"`
	Totals
}

// Breakdown is one row of a provider, model or account aggregate.
type Breakdown struct {
	Name string `json:"name"`
	Totals
}

// Report is the result of Query.
type Report struct {
	Available     bool          `json:"available"`
	Reason        string        `json:"reason,omitempty"`
	From          *time.Time    `json:"from"`
	To            *time.Time    `json:"to"`
	BucketSeconds int64         `json:"bucketSeconds"`
	Persistence   string        `json:"persistence"`
	ObservedSince *time.Time    `json:"observedSince"`
	Totals        Totals        `json:"totals"`
	Series        []SeriesPoint `json:"series"`
	ByProvider    []Breakdown   `json:"byProvider"`
	ByModel       []Breakdown   `json:"byModel"`
	ByAccount     []Breakdown   `json:"byAccount"`
}

// Unavailable builds a report for a missing collector.
func Unavailable(reason string) Report {
	return Report{Available: false, Reason: reason, Series: []SeriesPoint{}, ByProvider: []Breakdown{}, ByModel: []Breakdown{}, ByAccount: []Breakdown{}}
}

// Query aggregates statistics over the last span.
func (c *Collector) Query(ctx context.Context, span time.Duration) Report {
	if c == nil {
		return Unavailable("request statistics are not being collected by this server")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := c.now().UTC()
	fromStart, toStart := bucketStart(now.Add(-span)), bucketStart(now)
	c.ensureLoaded(ctx, fromStart, toStart)

	c.mu.Lock()
	defer c.mu.Unlock()
	report := Report{Available: true, BucketSeconds: bucketSeconds, Persistence: c.store.Persistence(), Series: []SeriesPoint{}}
	fromAt := time.Unix(fromStart, 0).UTC()
	report.From, report.To = &fromAt, &now
	var total Counts
	providers, models, accounts := map[string]*Counts{}, map[string]*Counts{}, map[string]*Counts{}
	earliest := int64(0)
	for start := fromStart; start <= toStart; start += bucketSeconds {
		point := SeriesPoint{At: time.Unix(start, 0).UTC()}
		if bucket := c.buckets[start]; bucket != nil {
			if earliest == 0 {
				earliest = start
			}
			point.Totals = totalsFrom(bucket.Totals)
			total.add(bucket.Totals)
			for _, pair := range []struct{ dst, src map[string]*Counts }{{providers, bucket.Providers}, {models, bucket.Models}, {accounts, bucket.Accounts}} {
				for key, value := range pair.src {
					if value != nil {
						dimension(pair.dst, key).add(*value)
					}
				}
			}
		}
		report.Series = append(report.Series, point)
	}
	observed := c.started.UTC()
	if earliest != 0 && time.Unix(earliest, 0).Before(observed) {
		observed = time.Unix(earliest, 0).UTC()
	}
	report.ObservedSince = &observed
	report.Totals = totalsFrom(total)
	report.ByProvider = breakdown(providers)
	report.ByModel = breakdown(models)
	report.ByAccount = breakdown(accounts)
	return report
}

func breakdown(values map[string]*Counts) []Breakdown {
	out := make([]Breakdown, 0, len(values))
	for name, counts := range values {
		out = append(out, Breakdown{Name: name, Totals: totalsFrom(*counts)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Requests != out[j].Requests {
			return out[i].Requests > out[j].Requests
		}
		if out[i].Failovers != out[j].Failovers {
			return out[i].Failovers > out[j].Failovers
		}
		return out[i].Name < out[j].Name
	})
	return out
}
