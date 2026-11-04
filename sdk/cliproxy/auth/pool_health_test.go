package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

const poolTestCreditBody = `{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}}`

const (
	poolTestAPIKeySecret = "sk-ant-api03-pool-test-secret-value"
	poolTestOAuthToken   = "sk-ant-oat01-pool-test-access-token"
	poolTestRefresh      = "sk-ant-ort01-pool-test-refresh-token"
)

func poolCreditError() customStatusError {
	return customStatusError{code: http.StatusBadRequest, msg: poolTestCreditBody}
}

func TestPoolBillingErrorsAreNotInvalidRequests(t *testing.T) {
	for _, code := range []string{"billing_hard_limit_reached", "insufficient_balance", "insufficient_credits"} {
		err := customStatusError{code: http.StatusBadRequest, msg: `{"error":{"code":"` + code + `","message":"Billing limit reached"}}`}
		if !IsInsufficientCreditsError(err) {
			t.Fatalf("%s was not recognized as a credit error", code)
		}
		if isRequestInvalidError(err) {
			t.Errorf("%s incorrectly stops credential failover", code)
		}
	}
	err := customStatusError{code: http.StatusBadRequest, msg: `{"error":{"type":"invalid_request_error","message":"Malformed request"}}`}
	if !isRequestInvalidError(err) {
		t.Fatal("ordinary invalid requests must still stop credential failover")
	}
}

// memoryPoolHealthStore is an in-memory PoolHealthStore that survives manager restarts.
type memoryPoolHealthStore struct {
	mu    sync.Mutex
	raw   []byte
	saves int
}

func (s *memoryPoolHealthStore) LoadPoolHealth(context.Context) (*PoolHealthSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.raw) == 0 {
		return nil, nil
	}
	var snapshot PoolHealthSnapshot
	if err := json.Unmarshal(s.raw, &snapshot); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func (s *memoryPoolHealthStore) SavePoolHealth(_ context.Context, snapshot *PoolHealthSnapshot) error {
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raw = raw
	s.saves++
	return nil
}

func (s *memoryPoolHealthStore) PoolHealthPersistence() string { return PoolPersistenceKV }

func (s *memoryPoolHealthStore) rawString() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return string(s.raw)
}

type poolCall struct {
	authID string
	model  string
}

type poolTestRig struct {
	manager *Manager
	keyID   string
	subID   string
	models  []string
	mu      sync.Mutex
	calls   []poolCall
}

func (r *poolTestRig) record(auth *Auth, model string) {
	r.mu.Lock()
	r.calls = append(r.calls, poolCall{authID: auth.ID, model: model})
	r.mu.Unlock()
}

func (r *poolTestRig) takeCalls() []poolCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.calls
	r.calls = nil
	return out
}

func poolTestAuths(prefix string, keyPriority, subPriority string) (*Auth, *Auth) {
	key := &Auth{
		ID:       prefix + "-claude-key",
		Provider: "claude",
		Label:    "Console key",
		Status:   StatusActive,
		Attributes: map[string]string{
			"priority":        keyPriority,
			AttributeAPIKey:   poolTestAPIKeySecret,
			AttributeAuthKind: AuthKindAPIKey,
		},
	}
	sub := &Auth{
		ID:       prefix + "-claude-sub.json",
		Provider: "claude",
		Status:   StatusActive,
		Attributes: map[string]string{
			"priority":        subPriority,
			AttributeAuthKind: AuthKindOAuth,
		},
		Metadata: map[string]any{
			"type":          "claude",
			"email":         "pool-owner@example.com",
			"access_token":  poolTestOAuthToken,
			"refresh_token": poolTestRefresh,
		},
	}
	return key, sub
}

// newPoolTestRig registers an API key and an OAuth subscription for the same Claude models.
// keyFails decides whether the API key returns the Anthropic credit-balance 400.
func newPoolTestRig(t *testing.T, store PoolHealthStore, keyPriority, subPriority string, keyFails func() bool) *poolTestRig {
	t.Helper()
	previous := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(previous) })

	prefix := strings.ReplaceAll(t.Name(), "/", "-")
	rig := &poolTestRig{manager: NewManager(nil, nil, nil), models: []string{prefix + "-opus", prefix + "-sonnet"}}
	if store != nil {
		rig.manager.SetPoolHealthStore(store)
	}
	rig.manager.SetRetryConfig(0, 0, 0)
	key, sub := poolTestAuths(prefix, keyPriority, subPriority)
	rig.keyID, rig.subID = key.ID, sub.ID
	reg := registry.GetGlobalRegistry()
	infos := []*registry.ModelInfo{{ID: rig.models[0]}, {ID: rig.models[1]}}
	for _, auth := range []*Auth{key, sub} {
		reg.RegisterClient(auth.ID, "claude", infos)
		if _, err := rig.manager.Register(WithSkipPersist(context.Background()), auth); err != nil {
			t.Fatalf("register %s: %v", auth.ID, err)
		}
	}
	t.Cleanup(func() {
		reg.UnregisterClient(key.ID)
		reg.UnregisterClient(sub.ID)
	})

	rig.manager.RegisterExecutor(&customStreamMockExecutor{
		identifier: "claude",
		mockCustomErrorExecutor: mockCustomErrorExecutor{
			identifier: "claude",
			executeFn: func(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
				rig.record(auth, req.Model)
				if auth.ID == rig.keyID && keyFails() {
					return cliproxyexecutor.Response{}, poolCreditError()
				}
				return cliproxyexecutor.Response{Payload: []byte(`{"type":"message","stop_reason":"end_turn"}`)}, nil
			},
		},
		streamFn: func(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
			rig.record(auth, req.Model)
			if auth.ID == rig.keyID && keyFails() {
				return nil, poolCreditError()
			}
			return successStreamResult(), nil
		},
	})
	return rig
}

func always() bool { return true }

func poolAccountByLabelID(t *testing.T, m *Manager, authID string) PoolAccount {
	t.Helper()
	m.mu.RLock()
	auth := m.auths[authID]
	var index string
	if auth != nil {
		index = auth.EnsureIndex()
	}
	m.mu.RUnlock()
	for _, account := range m.PoolOverview(time.Now()).Accounts {
		if account.ID == index {
			return account
		}
	}
	t.Fatalf("account for %s not found in overview", authID)
	return PoolAccount{}
}

func TestPoolCreditBlock_EmptyKeyFailsOverToSubscriptionAcrossModels(t *testing.T) {
	// The key outranks the subscription so it is deliberately tried first.
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	ctx := context.Background()

	resp, err := rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[0]}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("expected failover to the subscription, got error: %v", err)
	}
	if len(resp.Payload) == 0 {
		t.Fatal("expected a response payload from the subscription")
	}
	calls := rig.takeCalls()
	if len(calls) != 2 || calls[0].authID != rig.keyID || calls[1].authID != rig.subID {
		t.Fatalf("calls = %+v, want key then subscription", calls)
	}
	for _, call := range calls {
		if call.model != rig.models[0] {
			t.Fatalf("model substituted: got %q, want %q", call.model, rig.models[0])
		}
	}

	// A different model must not try the out-of-credits key again.
	if _, err = rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[1]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("second model failed: %v", err)
	}
	calls = rig.takeCalls()
	if len(calls) != 1 || calls[0].authID != rig.subID || calls[0].model != rig.models[1] {
		t.Fatalf("second model calls = %+v, want only the subscription on %q", calls, rig.models[1])
	}

	key := poolAccountByLabelID(t, rig.manager, rig.keyID)
	if key.State != PoolStateOutOfCredits || key.Eligible {
		t.Fatalf("key account = %+v, want out_of_credits and not eligible", key)
	}
	if key.ResetAt == nil {
		t.Fatal("out-of-credits account must report when it will be rechecked")
	}
	sub := poolAccountByLabelID(t, rig.manager, rig.subID)
	if sub.State != PoolStateReady || !sub.Eligible || sub.LastSuccessAt == nil {
		t.Fatalf("subscription account = %+v, want ready, eligible, with lastSuccessAt", sub)
	}
}

func TestPoolCreditBlock_StreamFailsOverBeforeOutput(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	ctx := context.Background()

	result, err := rig.manager.ExecuteStream(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[0]}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("expected streaming failover, got %v", err)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("unexpected chunk error: %v", chunk.Err)
		}
	}
	calls := rig.takeCalls()
	if len(calls) != 2 || calls[1].authID != rig.subID {
		t.Fatalf("stream calls = %+v, want key then subscription", calls)
	}

	result, err = rig.manager.ExecuteStream(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[1]}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("second stream failed: %v", err)
	}
	for range result.Chunks {
	}
	calls = rig.takeCalls()
	if len(calls) != 1 || calls[0].authID != rig.subID {
		t.Fatalf("second stream calls = %+v, want only the subscription", calls)
	}
}

func TestPoolCreditBlock_StreamBootstrapChunkErrorFailsOver(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	rig.manager.RegisterExecutor(&customStreamMockExecutor{
		identifier: "claude",
		streamFn: func(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
			rig.record(auth, req.Model)
			if auth.ID == rig.keyID {
				ch := make(chan cliproxyexecutor.StreamChunk, 1)
				ch <- cliproxyexecutor.StreamChunk{Err: poolCreditError()}
				close(ch)
				return &cliproxyexecutor.StreamResult{Chunks: ch}, nil
			}
			return successStreamResult(), nil
		},
	})
	result, err := rig.manager.ExecuteStream(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[0]}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("expected failover when the credit error arrives before any output: %v", err)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("unexpected chunk error: %v", chunk.Err)
		}
	}
	if calls := rig.takeCalls(); len(calls) != 2 || calls[1].authID != rig.subID {
		t.Fatalf("calls = %+v, want key then subscription", calls)
	}
}

func TestPoolCreditBlock_StreamAfterOutputIsNotReplayed(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	rig.manager.RegisterExecutor(&customStreamMockExecutor{
		identifier: "claude",
		streamFn: func(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
			rig.record(auth, req.Model)
			ch := make(chan cliproxyexecutor.StreamChunk, 2)
			ch <- cliproxyexecutor.StreamChunk{Payload: []byte(`data: {"type":"content_block_delta","delta":{"text":"partial"}}`)}
			ch <- cliproxyexecutor.StreamChunk{Err: poolCreditError()}
			close(ch)
			return &cliproxyexecutor.StreamResult{Chunks: ch}, nil
		},
	})
	result, err := rig.manager.ExecuteStream(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[0]}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("stream start: %v", err)
	}
	sawPayload, sawErr := false, false
	for chunk := range result.Chunks {
		if len(chunk.Payload) > 0 {
			sawPayload = true
		}
		if chunk.Err != nil {
			sawErr = true
		}
	}
	if !sawPayload || !sawErr {
		t.Fatalf("client should see the committed output and then the error (payload=%v err=%v)", sawPayload, sawErr)
	}
	if calls := rig.takeCalls(); len(calls) != 1 {
		t.Fatalf("calls = %+v, a stream that already produced output must never be replayed", calls)
	}
}

func TestPoolCreditBlock_PersistsAcrossRestartAndRecovers(t *testing.T) {
	store := &memoryPoolHealthStore{}
	keyFails := true
	rig := newPoolTestRig(t, store, "1", "0", func() bool { return keyFails })
	ctx := context.Background()
	if _, err := rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[0]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("first request: %v", err)
	}
	if err := rig.manager.FlushPoolHealth(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	persisted := store.rawString()
	if persisted == "" {
		t.Fatal("expected the block to be persisted")
	}
	for _, secret := range []string{poolTestAPIKeySecret, poolTestOAuthToken, rig.keyID, rig.subID} {
		if strings.Contains(persisted, secret) {
			t.Fatalf("persisted health leaks %q: %s", secret, persisted)
		}
	}

	// Simulate a restart: a fresh manager restores health from the same store.
	restarted := newPoolTestRig(t, store, "1", "0", func() bool { return keyFails })
	if err := restarted.manager.RestorePoolHealth(ctx); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if _, err := restarted.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: restarted.models[1]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("request after restart: %v", err)
	}
	if calls := restarted.takeCalls(); len(calls) != 1 || calls[0].authID != restarted.subID {
		t.Fatalf("after restart calls = %+v, want only the subscription", calls)
	}

	// Controlled recheck: once the recheck time passes the key is probed again,
	// a renewed credit error extends the block with a longer backoff.
	restarted.manager.setPoolRecheckForTest(restarted.keyID, time.Now().Add(-time.Second))
	if _, err := restarted.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: restarted.models[1]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("recheck request: %v", err)
	}
	if calls := restarted.takeCalls(); len(calls) != 2 || calls[0].authID != restarted.keyID {
		t.Fatalf("recheck calls = %+v, want a key probe then the subscription", calls)
	}
	account := poolAccountByLabelID(t, restarted.manager, restarted.keyID)
	if account.State != PoolStateOutOfCredits || account.ResetAt == nil || time.Until(*account.ResetAt) < 90*time.Minute {
		t.Fatalf("after failed recheck account = %+v, want out_of_credits with a backed-off recheck", account)
	}

	// After a top-up the next recheck succeeds and clears the block.
	keyFails = false
	restarted.manager.setPoolRecheckForTest(restarted.keyID, time.Now().Add(-time.Second))
	if _, err := restarted.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: restarted.models[0]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("recovered request: %v", err)
	}
	if calls := restarted.takeCalls(); len(calls) != 1 || calls[0].authID != restarted.keyID {
		t.Fatalf("recovered calls = %+v, want the key to serve again", calls)
	}
	if account = poolAccountByLabelID(t, restarted.manager, restarted.keyID); account.State != PoolStateReady || !account.Eligible {
		t.Fatalf("recovered account = %+v, want ready", account)
	}
}

func TestPoolCreditBlock_ManualReset(t *testing.T) {
	keyFails := true
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", func() bool { return keyFails })
	ctx := context.Background()
	if _, err := rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[0]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("first request: %v", err)
	}
	rig.takeCalls()
	keyFails = false
	publicID := poolAccountByLabelID(t, rig.manager, rig.keyID).ID
	found, err := rig.manager.ResetPoolAccount(ctx, publicID)
	if err != nil || !found {
		t.Fatalf("reset found=%v err=%v", found, err)
	}
	if _, err = rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[1]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("after reset: %v", err)
	}
	if calls := rig.takeCalls(); len(calls) != 1 || calls[0].authID != rig.keyID {
		t.Fatalf("after reset calls = %+v, want the key", calls)
	}
	if found, _ = rig.manager.ResetPoolAccount(ctx, "does-not-exist"); found {
		t.Fatal("unknown account id must not be reported as found")
	}
}

func TestPoolCreditBlock_AllBlockedReturnsCreditErrorWithoutSubstitution(t *testing.T) {
	previous := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(previous) })

	prefix := strings.ReplaceAll(t.Name(), "/", "-")
	m := NewManager(nil, nil, nil)
	m.SetPoolHealthStore(&memoryPoolHealthStore{})
	m.SetRetryConfig(0, 0, 0)
	key, _ := poolTestAuths(prefix, "0", "0")
	models := []string{prefix + "-opus", prefix + "-sonnet"}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(key.ID, "claude", []*registry.ModelInfo{{ID: models[0]}, {ID: models[1]}})
	t.Cleanup(func() { reg.UnregisterClient(key.ID) })
	if _, err := m.Register(WithSkipPersist(context.Background()), key); err != nil {
		t.Fatalf("register: %v", err)
	}
	var mu sync.Mutex
	var seen []string
	m.RegisterExecutor(&mockCustomErrorExecutor{
		identifier: "claude",
		executeFn: func(_ context.Context, _ *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
			mu.Lock()
			seen = append(seen, req.Model)
			mu.Unlock()
			return cliproxyexecutor.Response{}, poolCreditError()
		},
	})
	ctx := context.Background()
	if _, err := m.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: models[0]}, cliproxyexecutor.Options{}); err == nil {
		t.Fatal("expected the upstream credit error")
	}
	_, err := m.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: models[1]}, cliproxyexecutor.Options{})
	if err == nil {
		t.Fatal("expected an error when every account is out of credits")
	}
	var authErr *Error
	if !errors.As(err, &authErr) || authErr.Code != PoolErrorCodeOutOfCredits {
		t.Fatalf("error = %v (%T), want code %q", err, err, PoolErrorCodeOutOfCredits)
	}
	if authErr.StatusCode() != http.StatusPaymentRequired {
		t.Fatalf("status = %d, want 402", authErr.StatusCode())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0] != models[0] {
		t.Fatalf("upstream calls = %v, want exactly one call for %q and no substitution", seen, models[0])
	}
}

func TestPoolCreditBlock_SessionAffinityCannotPinBlockedAccount(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &RoundRobinSelector{}, TTL: time.Hour})
	defer affinity.Stop()
	rig.manager.SetSelector(affinity)
	ctx := context.Background()
	opts := func() cliproxyexecutor.Options {
		return cliproxyexecutor.Options{Metadata: map[string]any{cliproxyexecutor.DerivedSessionIDMetadataKey: "pool-affinity-session"}}
	}
	// Bind the session to the key while it is still healthy.
	auth, _, _, err := rig.manager.pickNextMixed(ctx, []string{"claude"}, rig.models[1], opts(), nil)
	if err != nil || auth.ID != rig.keyID {
		t.Fatalf("cold binding = %v err=%v, want the key", auth, err)
	}
	rig.manager.MarkResult(ctx, Result{AuthID: rig.keyID, Provider: "claude", Model: rig.models[0], Success: false, Error: resultErrorFromError(poolCreditError())})
	auth, _, _, err = rig.manager.pickNextMixed(ctx, []string{"claude"}, rig.models[1], opts(), nil)
	if err != nil {
		t.Fatalf("pick after block: %v", err)
	}
	if auth.ID == rig.keyID {
		t.Fatal("session affinity kept an out-of-credits account on another model")
	}
}

func TestPoolPolicy_SubscriptionsFirstAndPaidFallback(t *testing.T) {
	keyFails := false
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "5", "0", func() bool { return keyFails })
	ctx := context.Background()

	// Existing behavior: the higher-priority key is chosen.
	if _, err := rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[0]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("existing policy: %v", err)
	}
	if calls := rig.takeCalls(); len(calls) != 1 || calls[0].authID != rig.keyID {
		t.Fatalf("existing policy calls = %+v, want the key", calls)
	}

	rig.manager.SetPoolPolicy(PoolPolicy{Strategy: PoolStrategySubscriptionsFirst, AllowPaidFallback: true})
	if _, err := rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[0]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("subscriptions-first: %v", err)
	}
	if calls := rig.takeCalls(); len(calls) != 1 || calls[0].authID != rig.subID {
		t.Fatalf("subscriptions-first calls = %+v, want the subscription", calls)
	}

	// With paid fallback allowed the key still serves when the subscription is paused.
	sub := rig.manager.authByIDForTest(rig.subID)
	sub.Disabled = true
	sub.Status = StatusDisabled
	if _, err := rig.manager.Update(WithSkipPersist(ctx), sub); err != nil {
		t.Fatalf("pause subscription: %v", err)
	}
	if _, err := rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[0]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("paid fallback allowed: %v", err)
	}
	if calls := rig.takeCalls(); len(calls) != 1 || calls[0].authID != rig.keyID {
		t.Fatalf("paid fallback calls = %+v, want the key", calls)
	}
	if paused := poolAccountByLabelID(t, rig.manager, rig.subID); paused.State != PoolStatePaused || paused.Eligible {
		t.Fatalf("paused subscription = %+v, want paused", paused)
	}

	rig.manager.SetPoolPolicy(PoolPolicy{Strategy: PoolStrategySubscriptionsFirst, AllowPaidFallback: false})
	if _, err := rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[0]}, cliproxyexecutor.Options{}); err == nil {
		t.Fatal("expected no eligible account when paid fallback is disabled and the subscription is paused")
	}
	if calls := rig.takeCalls(); len(calls) != 0 {
		t.Fatalf("paid fallback disabled calls = %+v, want none", calls)
	}
	key := poolAccountByLabelID(t, rig.manager, rig.keyID)
	if key.Eligible || !strings.Contains(strings.ToLower(key.Reason), "paid fallback") {
		t.Fatalf("key account = %+v, want ineligible because paid fallback is off", key)
	}

	explain := rig.manager.ExplainPoolRoute(rig.models[0], time.Now())
	if explain.SelectedAccountID != nil {
		t.Fatalf("explain selected %v, want none", *explain.SelectedAccountID)
	}
	if len(explain.Candidates) != 2 {
		t.Fatalf("explain candidates = %+v, want both accounts", explain.Candidates)
	}
}

func TestPoolPolicy_NewSubscriptionInheritsPolicy(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "5", "0", func() bool { return false })
	rig.manager.SetPoolPolicy(PoolPolicy{Strategy: PoolStrategySubscriptionsFirst, AllowPaidFallback: true})
	ctx := context.Background()
	prefix := strings.ReplaceAll(t.Name(), "/", "-")
	_, extra := poolTestAuths(prefix+"-late", "0", "0")
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(extra.ID, "claude", []*registry.ModelInfo{{ID: rig.models[0]}})
	t.Cleanup(func() { reg.UnregisterClient(extra.ID) })
	if _, err := rig.manager.Register(WithSkipPersist(ctx), extra); err != nil {
		t.Fatalf("register late subscription: %v", err)
	}
	explain := rig.manager.ExplainPoolRoute(rig.models[0], time.Now())
	for _, candidate := range explain.Candidates {
		if candidate.Kind == PoolKindAPIKey && explain.SelectedAccountID != nil && *explain.SelectedAccountID == candidate.ID {
			t.Fatalf("explain picked the API key over subscriptions: %+v", explain)
		}
	}
	if explain.SelectedAccountID == nil && len(explain.Tied) < 2 {
		t.Fatalf("explain = %+v, want both subscriptions tied ahead of the key", explain)
	}
}

func TestPoolOverview_Redaction(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	if _, err := rig.manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[0]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("request: %v", err)
	}
	raw, err := json.Marshal(rig.manager.PoolOverview(time.Now()))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	explain, _ := json.Marshal(rig.manager.ExplainPoolRoute(rig.models[0], time.Now()))
	for _, body := range []string{string(raw), string(explain)} {
		for _, secret := range []string{poolTestAPIKeySecret, poolTestOAuthToken, poolTestRefresh, rig.keyID, rig.subID, "Plans & Billing"} {
			if strings.Contains(body, secret) {
				t.Fatalf("pool output leaks %q: %s", secret, body)
			}
		}
	}
	if !strings.Contains(string(raw), "pool-owner@example.com") {
		t.Fatalf("expected the subscription email as its label: %s", raw)
	}
	if !strings.Contains(string(raw), `"balance":null`) {
		t.Fatalf("balance must be null when unknown: %s", raw)
	}
}
