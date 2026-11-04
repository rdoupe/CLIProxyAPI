package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MoeForge/moego/ojson"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/kvstore"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	log "github.com/sirupsen/logrus"
)

// Account-pool states reported by the pool health API.
const (
	PoolStateReady        = "ready"
	PoolStatePaused       = "paused"
	PoolStateRateLimited  = "rate_limited"
	PoolStateOutOfCredits = "out_of_credits"
	PoolStateLoginExpired = "login_expired"
	PoolStateUnavailable  = "unavailable"
	PoolStateUnknown      = "unknown"
)

// Account kinds reported by the pool health API.
const (
	PoolKindSubscription = "subscription"
	PoolKindAPIKey       = "api_key"
)

// Pool routing strategies.
const (
	PoolStrategyExisting           = "existing"
	PoolStrategySubscriptionsFirst = "subscriptions_first"
)

// Pool persistence modes.
const (
	PoolPersistenceKV     = "kv"
	PoolPersistenceMemory = "memory"
)

// PoolErrorCodeOutOfCredits is returned when every account able to serve a model is out of credits.
const PoolErrorCodeOutOfCredits = "insufficient_credits"

const (
	poolHealthKVKey      = "cliproxy:pool:health:v1"
	poolHealthVersion    = 1
	poolRecheckBase      = time.Hour
	poolRecheckMax       = 24 * time.Hour
	poolSaveInterval     = time.Minute
	poolMaxRecords       = 2048
	poolTierStride       = 1_000_000_000
	poolPriorityClampAbs = poolTierStride/2 - 1
)

// PoolPolicy is the account-pool routing policy.
type PoolPolicy struct {
	Strategy          string `json:"strategy"`
	AllowPaidFallback bool   `json:"allowPaidFallback"`
}

// DefaultPoolPolicy preserves the pre-pool behavior: priority-only ordering with API keys allowed.
func DefaultPoolPolicy() PoolPolicy {
	return PoolPolicy{Strategy: PoolStrategyExisting, AllowPaidFallback: true}
}

// NormalizePoolStrategy maps accepted spellings to a canonical strategy.
func NormalizePoolStrategy(raw string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", PoolStrategyExisting, "default", "priority":
		return PoolStrategyExisting, true
	case PoolStrategySubscriptionsFirst, "subscriptions-first", "subscription-first", "subscription_first":
		return PoolStrategySubscriptionsFirst, true
	default:
		return "", false
	}
}

// PoolPolicyFromConfig derives the pool policy from routing.pool.
func PoolPolicyFromConfig(cfg *internalconfig.Config) PoolPolicy {
	policy := DefaultPoolPolicy()
	if cfg == nil {
		return policy
	}
	if strategy, ok := NormalizePoolStrategy(cfg.Routing.Pool.Strategy); ok {
		policy.Strategy = strategy
	}
	if cfg.Routing.Pool.AllowPaidFallback != nil {
		policy.AllowPaidFallback = *cfg.Routing.Pool.AllowPaidFallback
	}
	return policy
}

// PoolHealthRecord is the persisted health of one credential. It never contains
// credential material or raw upstream bodies.
type PoolHealthRecord struct {
	State         string    `json:"state,omitempty"`
	Reason        string    `json:"reason,omitempty"`
	Since         time.Time `json:"since,omitzero"`
	RecheckAt     time.Time `json:"recheckAt,omitzero"`
	Strikes       int       `json:"strikes,omitempty"`
	LastSuccessAt time.Time `json:"lastSuccessAt,omitzero"`
	LastFailureAt time.Time `json:"lastFailureAt,omitzero"`
	// seq is the execution watermark when the block was recorded; runtime only.
	seq uint64
}

func (r *PoolHealthRecord) blocked() bool {
	return r != nil && r.State == PoolStateOutOfCredits
}

func (r *PoolHealthRecord) lastActivity() time.Time {
	latest := r.LastSuccessAt
	for _, candidate := range []time.Time{r.LastFailureAt, r.Since} {
		if candidate.After(latest) {
			latest = candidate
		}
	}
	return latest
}

// PoolHealthSnapshot is the persisted pool health document. Accounts are keyed
// by a one-way hash of the credential ID.
type PoolHealthSnapshot struct {
	Version  int                         `json:"version"`
	SavedAt  time.Time                   `json:"savedAt"`
	Accounts map[string]PoolHealthRecord `json:"accounts"`
}

// PoolHealthStore persists pool health across restarts.
type PoolHealthStore interface {
	LoadPoolHealth(ctx context.Context) (*PoolHealthSnapshot, error)
	SavePoolHealth(ctx context.Context, snapshot *PoolHealthSnapshot) error
	PoolHealthPersistence() string
}

// KVPoolHealthStore stores pool health in the configured KV store (Home KV or
// the local bbolt store). Without an active KV store, health is memory-only.
type KVPoolHealthStore struct{}

// LoadPoolHealth implements PoolHealthStore.
func (KVPoolHealthStore) LoadPoolHealth(ctx context.Context) (*PoolHealthSnapshot, error) {
	var snapshot PoolHealthSnapshot
	_, found, err := kvstore.KVGetJSONRequired(ctx, poolHealthKVKey, &snapshot)
	if err != nil || !found {
		return nil, err
	}
	return &snapshot, nil
}

// SavePoolHealth implements PoolHealthStore.
func (KVPoolHealthStore) SavePoolHealth(ctx context.Context, snapshot *PoolHealthSnapshot) error {
	_, err := kvstore.KVSetJSONRequired(ctx, poolHealthKVKey, snapshot, 0)
	return err
}

// PoolHealthPersistence implements PoolHealthStore.
func (KVPoolHealthStore) PoolHealthPersistence() string {
	if _, kvMode, err := kvstore.CurrentKVClient(); kvMode && err == nil {
		return PoolPersistenceKV
	}
	return PoolPersistenceMemory
}

// PoolEventSink receives pool routing events (used for request statistics).
type PoolEventSink interface {
	RecordFailover(provider, model, accountID string, at time.Time)
}

// poolAuthState is the pool decision stamped on every committed Auth so the
// scheduler and selectors see it without consulting the tracker.
type poolAuthState struct {
	creditBlocked bool
	recheckAt     time.Time
	tierActive    bool
	tier          int
	paidExcluded  bool
}

func (s poolAuthState) creditBlockActive(now time.Time) bool {
	return s.creditBlocked && s.recheckAt.After(now)
}

type poolTracker struct {
	mu      sync.Mutex
	store   PoolHealthStore
	records map[string]*PoolHealthRecord
	policy  PoolPolicy
	dirty   bool
	saved   time.Time
	sink    PoolEventSink

	saveMu  sync.Mutex
	saving  atomic.Bool
	pending atomic.Bool
}

func newPoolTracker() *poolTracker {
	return &poolTracker{
		store:   KVPoolHealthStore{},
		records: make(map[string]*PoolHealthRecord),
		policy:  DefaultPoolPolicy(),
	}
}

func (m *Manager) poolTracker() *poolTracker {
	if m == nil {
		return nil
	}
	if t := m.pool.Load(); t != nil {
		return t
	}
	m.pool.CompareAndSwap(nil, newPoolTracker())
	return m.pool.Load()
}

func poolHealthKey(authID string) string {
	return kvstore.HashKeyPart("pool-health:" + strings.TrimSpace(authID))[:32]
}

// PoolKindForAuth reports whether a credential is an API key or a login-based subscription.
func PoolKindForAuth(auth *Auth) string {
	if auth != nil && auth.AuthKind() == AuthKindAPIKey {
		return PoolKindAPIKey
	}
	return PoolKindSubscription
}

// IsInsufficientCreditsError reports whether err means the credential has no
// prepaid credit or billing balance left. It covers the Anthropic
// "credit balance is too low" 400 and explicit billing error codes.
func IsInsufficientCreditsError(err error) bool {
	if err == nil {
		return false
	}
	if isCreditBalanceTooLowError(err) {
		return true
	}
	var authErr *Error
	if errors.As(err, &authErr) && authErr != nil {
		return isInsufficientCreditsResultError(authErr)
	}
	return hasInsufficientCreditsCode(err.Error())
}

func isInsufficientCreditsResultError(err *Error) bool {
	if err == nil {
		return false
	}
	if isCreditBalanceTooLowResultError(err) {
		return true
	}
	return hasInsufficientCreditsCode(err.Message)
}

func hasInsufficientCreditsCode(body string) bool {
	body = strings.TrimSpace(body)
	if body == "" || !ojson.Valid(body) {
		return false
	}
	for _, path := range []string{"error.code", "code", "error.type", "type", "response.error.code", "body.error.code"} {
		switch strings.ToLower(strings.TrimSpace(ojson.Get(body, path).String())) {
		case "insufficient_quota", "billing_hard_limit_reached", "insufficient_balance", "credit_balance_too_low", "insufficient_credits":
			return true
		}
	}
	return false
}

func poolRecheckDelay(strikes int) time.Duration {
	delay := poolRecheckBase
	for i := 1; i < strikes && delay < poolRecheckMax; i++ {
		delay *= 2
	}
	if delay > poolRecheckMax {
		delay = poolRecheckMax
	}
	return delay
}

func poolCreditReason(status int) string {
	if status > 0 {
		return fmt.Sprintf("Provider reported the account has no credit left (HTTP %d)", status)
	}
	return "Provider reported the account has no credit left"
}

// SetPoolHealthStore replaces the pool health persistence backend.
func (m *Manager) SetPoolHealthStore(store PoolHealthStore) {
	t := m.poolTracker()
	if t == nil || store == nil {
		return
	}
	t.mu.Lock()
	t.store = store
	t.mu.Unlock()
}

// SetPoolEventSink registers the receiver of pool routing events.
func (m *Manager) SetPoolEventSink(sink PoolEventSink) {
	t := m.poolTracker()
	if t == nil {
		return
	}
	t.mu.Lock()
	t.sink = sink
	t.mu.Unlock()
}

// PoolPolicy returns the active pool policy.
func (m *Manager) PoolPolicy() PoolPolicy {
	t := m.poolTracker()
	if t == nil {
		return DefaultPoolPolicy()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.policy
}

// SetPoolPolicy applies a pool policy to every registered and future credential.
func (m *Manager) SetPoolPolicy(policy PoolPolicy) {
	t := m.poolTracker()
	if t == nil {
		return
	}
	if strategy, ok := NormalizePoolStrategy(policy.Strategy); ok {
		policy.Strategy = strategy
	} else {
		policy.Strategy = PoolStrategyExisting
	}
	t.mu.Lock()
	changed := t.policy != policy
	t.policy = policy
	t.mu.Unlock()
	if changed {
		m.restampPoolAll()
	}
}

func (m *Manager) applyPoolPolicyFromConfig(cfg *internalconfig.Config) {
	m.SetPoolPolicy(PoolPolicyFromConfig(cfg))
}

// stampPoolLocked refreshes auth.pool from the tracker. Caller holds m.mu.
func (m *Manager) stampPoolLocked(auth *Auth) {
	if auth == nil {
		return
	}
	t := m.poolTracker()
	if t == nil {
		return
	}
	t.mu.Lock()
	auth.pool = t.stateForLocked(auth)
	t.mu.Unlock()
}

func (t *poolTracker) stateForLocked(auth *Auth) poolAuthState {
	state := poolAuthState{}
	if rec := t.records[poolHealthKey(auth.ID)]; rec.blocked() {
		state.creditBlocked = true
		state.recheckAt = rec.RecheckAt
	}
	kind := PoolKindForAuth(auth)
	if t.policy.Strategy == PoolStrategySubscriptionsFirst {
		state.tierActive = true
		if kind == PoolKindSubscription {
			state.tier = 1
		}
	}
	state.paidExcluded = !t.policy.AllowPaidFallback && kind == PoolKindAPIKey
	return state
}

// restampPoolAll re-evaluates pool state for every credential and refreshes the scheduler.
func (m *Manager) restampPoolAll() {
	if m == nil {
		return
	}
	var snapshots []*Auth
	m.mu.Lock()
	for _, auth := range m.auths {
		if auth == nil {
			continue
		}
		before := auth.pool
		m.stampPoolLocked(auth)
		if before == auth.pool {
			continue
		}
		auth.Generation++
		snapshots = append(snapshots, auth.Clone())
	}
	m.mu.Unlock()
	if m.scheduler != nil {
		for _, snapshot := range snapshots {
			m.scheduler.upsertAuth(snapshot)
		}
	}
}

// applyPoolResultLocked records a result in pool health. Caller holds m.mu.
// It reports whether the credential-wide block changed and whether a save is due.
func (m *Manager) applyPoolResultLocked(auth *Auth, result Result, now time.Time) (blockChanged bool, saveNow bool) {
	t := m.poolTracker()
	if t == nil || auth == nil {
		return false, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	key := poolHealthKey(auth.ID)
	rec := t.records[key]
	credits := !result.Success && isInsufficientCreditsResultError(result.Error)
	if rec == nil {
		if !result.Success && !credits {
			auth.pool = t.stateForLocked(auth)
			return false, false
		}
		rec = &PoolHealthRecord{}
		t.records[key] = rec
	}
	switch {
	case result.Success:
		rec.LastSuccessAt = now
		// A success dispatched before the block proves nothing about credits.
		if rec.blocked() && (result.ExecutionSeq == 0 || result.ExecutionSeq > rec.seq) {
			rec.State, rec.Reason, rec.Since, rec.RecheckAt, rec.Strikes, rec.seq = "", "", time.Time{}, time.Time{}, 0, 0
			blockChanged = true
			clearCreditCooldownsLocked(auth, now)
		}
	case credits:
		rec.LastFailureAt = now
		// Failures of requests that were in flight while already blocked do not
		// count as a new strike; only a failed recheck extends the backoff.
		if !(rec.blocked() && rec.RecheckAt.After(now)) {
			strikes := 1
			since := now
			if rec.blocked() {
				strikes = rec.Strikes + 1
				since = rec.Since
			}
			rec.State = PoolStateOutOfCredits
			rec.Reason = poolCreditReason(statusCodeFromResult(result.Error))
			rec.Since = since
			rec.Strikes = strikes
			rec.RecheckAt = now.Add(poolRecheckDelay(strikes))
			rec.seq = m.executionSeq.Load()
			blockChanged = true
		}
	default:
		rec.LastFailureAt = now
	}
	auth.pool = t.stateForLocked(auth)
	t.dirty = true
	saveNow = blockChanged || now.Sub(t.saved) >= poolSaveInterval
	return blockChanged, saveNow
}

// clearCreditCooldownsLocked lifts per-model cooldowns caused by a credit
// error once the credential proved it has credit again: credit is account-wide.
func clearCreditCooldownsLocked(auth *Auth, now time.Time) {
	for _, state := range auth.ModelStates {
		if state != nil && isInsufficientCreditsResultError(state.LastError) {
			resetModelState(state, now)
		}
	}
	if isInsufficientCreditsResultError(auth.LastError) {
		auth.Unavailable = false
		auth.NextRetryAfter = time.Time{}
		auth.LastError = nil
		auth.StatusMessage = ""
		if auth.Status == StatusError {
			auth.Status = StatusActive
		}
	}
	updateAggregatedAvailability(auth, now)
}

func (t *poolTracker) snapshotLocked(now time.Time) *PoolHealthSnapshot {
	if len(t.records) > poolMaxRecords {
		keys := make([]string, 0, len(t.records))
		for key := range t.records {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			return t.records[keys[i]].lastActivity().After(t.records[keys[j]].lastActivity())
		})
		for _, key := range keys[poolMaxRecords:] {
			delete(t.records, key)
		}
	}
	snapshot := &PoolHealthSnapshot{Version: poolHealthVersion, SavedAt: now, Accounts: make(map[string]PoolHealthRecord, len(t.records))}
	for key, rec := range t.records {
		if rec == nil {
			continue
		}
		snapshot.Accounts[key] = *rec
	}
	return snapshot
}

func (t *poolTracker) flush(ctx context.Context) error {
	t.saveMu.Lock()
	defer t.saveMu.Unlock()
	t.mu.Lock()
	if !t.dirty {
		t.mu.Unlock()
		return nil
	}
	now := time.Now()
	snapshot := t.snapshotLocked(now)
	store := t.store
	t.dirty = false
	t.saved = now
	t.mu.Unlock()
	if store == nil {
		return nil
	}
	if err := store.SavePoolHealth(ctx, snapshot); err != nil {
		t.mu.Lock()
		t.dirty = true
		t.mu.Unlock()
		return err
	}
	return nil
}

// requestSave persists pool health in the background, coalescing bursts.
func (t *poolTracker) requestSave() {
	if t == nil {
		return
	}
	t.pending.Store(true)
	if t.saving.CompareAndSwap(false, true) {
		go t.saveLoop()
	}
}

func (t *poolTracker) saveLoop() {
	for {
		for t.pending.Swap(false) {
			if err := t.flush(context.Background()); err != nil {
				log.Warnf("pool health: persist failed: %v", err)
			}
		}
		t.saving.Store(false)
		// A request that raced the exit above restarts the loop exactly once.
		if !t.pending.Load() || !t.saving.CompareAndSwap(false, true) {
			return
		}
	}
}

// FlushPoolHealth synchronously persists pending pool health.
func (m *Manager) FlushPoolHealth(ctx context.Context) error {
	t := m.poolTracker()
	if t == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return t.flush(ctx)
}

// RestorePoolHealth loads persisted pool health and applies it to registered credentials.
// Records already updated in this process win over persisted ones.
func (m *Manager) RestorePoolHealth(ctx context.Context) error {
	t := m.poolTracker()
	if t == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.Lock()
	store := t.store
	t.mu.Unlock()
	if store == nil {
		return nil
	}
	snapshot, err := store.LoadPoolHealth(ctx)
	if err != nil {
		return fmt.Errorf("load pool health: %w", err)
	}
	if snapshot != nil && len(snapshot.Accounts) > 0 {
		t.mu.Lock()
		for key, rec := range snapshot.Accounts {
			if _, exists := t.records[key]; exists {
				continue
			}
			restored := rec
			restored.seq = 0
			t.records[key] = &restored
		}
		t.mu.Unlock()
	}
	m.restampPoolAll()
	return nil
}

// ResetPoolAccount clears the pool block and cooldowns of the account with the
// given public ID (the credential auth index). It reports whether the account exists.
func (m *Manager) ResetPoolAccount(ctx context.Context, publicID string) (bool, error) {
	t := m.poolTracker()
	if t == nil {
		return false, nil
	}
	publicID = strings.TrimSpace(publicID)
	if publicID == "" {
		return false, nil
	}
	var authID string
	var snapshot *Auth
	m.mu.Lock()
	for id, auth := range m.auths {
		if auth != nil && auth.EnsureIndex() == publicID {
			authID = id
			t.mu.Lock()
			if rec := t.records[poolHealthKey(id)]; rec != nil {
				rec.State, rec.Reason, rec.Since, rec.RecheckAt, rec.Strikes, rec.seq = "", "", time.Time{}, time.Time{}, 0, 0
				t.dirty = true
			}
			t.mu.Unlock()
			m.stampPoolLocked(auth)
			auth.Generation++
			snapshot = auth.Clone()
			break
		}
	}
	m.mu.Unlock()
	if authID == "" {
		return false, nil
	}
	if m.scheduler != nil && snapshot != nil {
		m.scheduler.upsertAuth(snapshot)
	}
	if _, _, errReset := m.ResetQuota(ctx, authID); errReset != nil {
		return true, errReset
	}
	return true, t.flush(ctx)
}

func (m *Manager) setPoolRecheckForTest(authID string, at time.Time) {
	t := m.poolTracker()
	t.mu.Lock()
	if rec := t.records[poolHealthKey(authID)]; rec != nil {
		rec.RecheckAt = at
	}
	t.mu.Unlock()
	m.restampPoolAll()
}

func (m *Manager) authByIDForTest(authID string) *Auth {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.auths[authID].Clone()
}

func (m *Manager) poolRecordFor(authID string) (PoolHealthRecord, bool) {
	t := m.poolTracker()
	if t == nil {
		return PoolHealthRecord{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rec := t.records[poolHealthKey(authID)]
	if rec == nil {
		return PoolHealthRecord{}, false
	}
	return *rec, true
}

func (m *Manager) poolPersistence() string {
	t := m.poolTracker()
	if t == nil {
		return PoolPersistenceMemory
	}
	t.mu.Lock()
	store := t.store
	t.mu.Unlock()
	if store == nil {
		return PoolPersistenceMemory
	}
	return store.PoolHealthPersistence()
}

// notePoolFailover reports that a request moved to another account after a failure.
func (m *Manager) notePoolFailover(provider, model string, auth *Auth) {
	t := m.poolTracker()
	if t == nil || auth == nil {
		return
	}
	t.mu.Lock()
	sink := t.sink
	t.mu.Unlock()
	if sink == nil {
		return
	}
	sink.RecordFailover(provider, model, auth.EnsureIndex(), time.Now())
}

// poolPickError replaces a generic "no account available" pick failure with an
// explicit out-of-credits error when every account able to serve the model is
// blocked for credits. No other model is ever substituted.
func (m *Manager) poolPickError(providers []string, model string, errPick error) error {
	if m == nil || errPick == nil || strings.TrimSpace(model) == "" || isRequestStopError(errPick) {
		return errPick
	}
	if !isAuthUnavailableError(errPick) {
		var authErr *Error
		if !errors.As(errPick, &authErr) || authErr == nil || authErr.Code != "auth_not_found" {
			return errPick
		}
	}
	providerSet := make(map[string]struct{}, len(providers))
	for _, provider := range providers {
		if key := canonicalSchedulingProvider(provider); key != "" {
			providerSet[key] = struct{}{}
		}
	}
	now := time.Now()
	var earliest time.Time
	blocked := 0
	m.mu.RLock()
	registryRef := registry.GetGlobalRegistry()
	for _, candidate := range m.auths {
		if candidate == nil || candidate.Disabled || candidate.Status == StatusDisabled {
			continue
		}
		if len(providerSet) > 0 {
			if _, ok := providerSet[canonicalSchedulingProvider(executorKeyFromAuth(candidate))]; !ok {
				continue
			}
		}
		if !m.authSupportsRouteModel(registryRef, candidate, model) {
			continue
		}
		if !candidate.pool.creditBlockActive(now) {
			m.mu.RUnlock()
			return errPick
		}
		blocked++
		if earliest.IsZero() || candidate.pool.recheckAt.Before(earliest) {
			earliest = candidate.pool.recheckAt
		}
	}
	m.mu.RUnlock()
	if blocked == 0 {
		return errPick
	}
	return &Error{
		Code:       PoolErrorCodeOutOfCredits,
		Message:    fmt.Sprintf("every account that can serve model %q is out of credits; next automatic recheck at %s", model, earliest.UTC().Format(time.RFC3339)),
		HTTPStatus: http.StatusPaymentRequired,
	}
}
