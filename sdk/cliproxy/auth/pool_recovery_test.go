package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// blockPoolKeyForTest puts the rig's API key in the out-of-credits state via
// models[0]. That model also keeps its 30-minute per-model credit cooldown, so
// tests that force an early recheck probe through models[1].
func blockPoolKeyForTest(t *testing.T, rig *poolTestRig) {
	t.Helper()
	if _, err := rig.manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[0]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("blocking request: %v", err)
	}
	rig.takeCalls()
	if rec, ok := rig.manager.poolRecordFor(rig.keyID); !ok || !rec.blocked() || rec.Strikes != 1 {
		t.Fatalf("key record = %+v, want out_of_credits with one strike", rec)
	}
}

func countCalls(calls []poolCall, authID string) int {
	n := 0
	for _, call := range calls {
		if call.authID == authID {
			n++
		}
	}
	return n
}

func (m *Manager) expirePoolProbeForTest(authID string) {
	t := m.poolTracker()
	t.mu.Lock()
	if rec := t.records[poolHealthKey(authID)]; rec != nil && rec.probeToken != 0 {
		rec.probeUntil = time.Now().Add(-time.Second)
	}
	t.mu.Unlock()
	m.restampPoolAuth(authID)
}

// registerGatedKeyExecutor makes the key's generation wait for release and
// then return keyErr (nil = success). The subscription always succeeds.
func registerGatedKeyExecutor(rig *poolTestRig, entered chan<- struct{}, release <-chan struct{}, keyErr func() error) {
	rig.manager.RegisterExecutor(&customStreamMockExecutor{
		identifier: "claude",
		mockCustomErrorExecutor: mockCustomErrorExecutor{
			identifier: "claude",
			executeFn: func(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
				rig.record(auth, req.Model)
				if auth.ID == rig.keyID {
					entered <- struct{}{}
					select {
					case <-release:
					case <-ctx.Done():
						return cliproxyexecutor.Response{}, ctx.Err()
					}
					if err := keyErr(); err != nil {
						return cliproxyexecutor.Response{}, err
					}
				}
				return cliproxyexecutor.Response{Payload: []byte(`{"type":"message","stop_reason":"end_turn"}`)}, nil
			},
		},
		streamFn: func(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
			rig.record(auth, req.Model)
			if auth.ID == rig.keyID {
				entered <- struct{}{}
				return nil, poolCreditError()
			}
			return successStreamResult(), nil
		},
	})
}

func TestPoolRecheck_OnlyOneConcurrentRequestProbesDueAccount(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	blockPoolKeyForTest(t, rig)
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	registerGatedKeyExecutor(rig, entered, release, func() error { return poolCreditError() })
	rig.manager.setPoolRecheckForTest(rig.keyID, time.Now().Add(-time.Second))

	ctx := context.Background()
	probeDone := make(chan error, 1)
	go func() {
		_, err := rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[1]}, cliproxyexecutor.Options{})
		probeDone <- err
	}()
	<-entered // the probe holds the recheck reservation

	if account := poolAccountByLabelID(t, rig.manager, rig.keyID); account.Eligible || account.State != PoolStateOutOfCredits {
		t.Fatalf("account during recheck = %+v, want out_of_credits and not eligible", account)
	}
	// Concurrent generations must go to the subscription without touching the key.
	const others = 6
	var wg sync.WaitGroup
	errs := make(chan error, others)
	for i := 0; i < others; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				_, err := rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[i%2]}, cliproxyexecutor.Options{})
				errs <- err
				return
			}
			stream, err := rig.manager.ExecuteStream(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[1]}, cliproxyexecutor.Options{})
			if err == nil {
				for range stream.Chunks {
				}
			}
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent request failed: %v", err)
		}
	}
	select {
	case <-entered:
		t.Fatal("a second request reached the account while its recheck was reserved")
	default:
	}

	close(release)
	if err := <-probeDone; err != nil {
		t.Fatalf("probe request should fail over after the renewed credit error: %v", err)
	}
	calls := rig.takeCalls()
	if n := countCalls(calls, rig.keyID); n != 1 {
		t.Fatalf("key received %d generations, want exactly one probe: %+v", n, calls)
	}
	rec, _ := rig.manager.poolRecordFor(rig.keyID)
	if rec.Strikes != 2 || time.Until(rec.RecheckAt) < 90*time.Minute || rec.probeToken != 0 {
		t.Fatalf("after failed probe record = %+v, want strike 2, backed-off recheck, reservation consumed", rec)
	}
}

func TestPoolRecheck_ReservationReleasedOnNonCreditError(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	blockPoolKeyForTest(t, rig)
	entered := make(chan struct{}, 4)
	release := make(chan struct{}, 4)
	var keyErr error = customStatusError{code: http.StatusInternalServerError, msg: `{"error":{"type":"api_error","message":"overloaded"}}`}
	registerGatedKeyExecutor(rig, entered, release, func() error { return keyErr })
	rig.manager.setPoolRecheckForTest(rig.keyID, time.Now().Add(-time.Second))

	release <- struct{}{}
	if _, err := rig.manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[1]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("probe with server error should fail over: %v", err)
	}
	select {
	case <-entered:
	default:
		t.Fatal("the due key was not probed")
	}
	rec, _ := rig.manager.poolRecordFor(rig.keyID)
	if !rec.blocked() || rec.Strikes != 1 || rec.probeToken != 0 || rec.RecheckAt.After(time.Now()) {
		t.Fatalf("after inconclusive probe record = %+v, want still due, strike 1, reservation released", rec)
	}
	// The recheck is free for the next request (the 500 itself cools only that model).
	next, ok := rig.manager.admitPoolPick(rig.manager.authByIDForTest(rig.keyID))
	if !ok || next == nil {
		t.Fatal("the released recheck must be available to the next request")
	}
	next.release()
}

func TestPoolRecheck_CanceledProbeReleasesReservation(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	blockPoolKeyForTest(t, rig)
	entered := make(chan struct{}, 4)
	registerGatedKeyExecutor(rig, entered, make(chan struct{}), func() error { return nil })
	rig.manager.setPoolRecheckForTest(rig.keyID, time.Now().Add(-time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[1]}, cliproxyexecutor.Options{})
		done <- err
	}()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled probe returned %v, want context.Canceled", err)
	}
	if rec, _ := rig.manager.poolRecordFor(rig.keyID); rec.probeToken != 0 || rec.Strikes != 1 {
		t.Fatalf("after canceled probe record = %+v, want reservation released and no new strike", rec)
	}
	if account := poolAccountByLabelID(t, rig.manager, rig.keyID); !account.Eligible {
		t.Fatalf("after canceled probe account = %+v, want the recheck due again", account)
	}
}

func TestPoolRecheck_AbandonedReservationExpires(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	blockPoolKeyForTest(t, rig)
	rig.manager.setPoolRecheckForTest(rig.keyID, time.Now().Add(-time.Second))
	// A reservation whose request never reports back (e.g. a leaked stream).
	probe, ok := rig.manager.admitPoolPick(rig.manager.authByIDForTest(rig.keyID))
	if !ok || probe == nil {
		t.Fatal("expected the first pick to reserve the due recheck")
	}
	if _, again := rig.manager.admitPoolPick(rig.manager.authByIDForTest(rig.keyID)); again {
		t.Fatal("a second pick must not share the reservation")
	}
	rig.manager.expirePoolProbeForTest(rig.keyID)
	next, ok := rig.manager.admitPoolPick(rig.manager.authByIDForTest(rig.keyID))
	if !ok || next == nil {
		t.Fatal("an expired reservation must let the next request recheck")
	}
	probe.release() // the stale owner must not cancel the newer reservation
	if rec, _ := rig.manager.poolRecordFor(rig.keyID); rec.probeToken != next.token {
		t.Fatalf("stale release cleared the newer reservation: %+v", rec)
	}
	next.release()
}

func TestPoolRecheck_StreamHoldsReservationUntilStreamEnds(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	blockPoolKeyForTest(t, rig)
	chunks := make(chan cliproxyexecutor.StreamChunk, 4)
	rig.manager.RegisterExecutor(&customStreamMockExecutor{
		identifier: "claude",
		mockCustomErrorExecutor: mockCustomErrorExecutor{
			identifier: "claude",
			executeFn: func(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
				rig.record(auth, req.Model)
				return cliproxyexecutor.Response{Payload: []byte(`{"type":"message","stop_reason":"end_turn"}`)}, nil
			},
		},
		streamFn: func(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
			rig.record(auth, req.Model)
			if auth.ID == rig.keyID {
				return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
			}
			return successStreamResult(), nil
		},
	})
	rig.manager.setPoolRecheckForTest(rig.keyID, time.Now().Add(-time.Second))
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte(`data: {"type":"content_block_delta","delta":{"text":"hi"}}`)}
	stream, err := rig.manager.ExecuteStream(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[1]}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("probe stream: %v", err)
	}
	<-stream.Chunks // output is flowing from the key
	if calls := rig.takeCalls(); len(calls) != 1 || calls[0].authID != rig.keyID {
		t.Fatalf("probe stream calls = %+v, want the key", calls)
	}
	if _, err = rig.manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[1]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("concurrent request: %v", err)
	}
	if calls := rig.takeCalls(); len(calls) != 1 || calls[0].authID != rig.subID {
		t.Fatalf("while the probe stream is open calls = %+v, want only the subscription", calls)
	}
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte(`data: {"type":"message_stop"}`)}
	close(chunks)
	for range stream.Chunks {
	}
	// The wrapper records the stream's success before it closes the channel.
	if rec, _ := rig.manager.poolRecordFor(rig.keyID); rec.blocked() {
		t.Fatalf("completed probe stream did not clear the block: %+v", rec)
	}
}

func TestPoolRecheck_CountTokensNeitherClaimsNorClears(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	blockPoolKeyForTest(t, rig)
	rig.manager.RegisterExecutor(&customStreamMockExecutor{
		identifier: "claude",
		mockCustomErrorExecutor: mockCustomErrorExecutor{
			identifier: "claude",
			countFn: func(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
				rig.record(auth, req.Model)
				return cliproxyexecutor.Response{Payload: []byte(`{"input_tokens":3}`)}, nil
			},
		},
	})
	rig.manager.setPoolRecheckForTest(rig.keyID, time.Now().Add(-time.Second))
	before, _ := rig.manager.poolRecordFor(rig.keyID)
	if _, err := rig.manager.ExecuteCount(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[1]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("count tokens: %v", err)
	}
	if calls := rig.takeCalls(); len(calls) != 1 || calls[0].authID != rig.keyID {
		t.Fatalf("count calls = %+v, want the due key to count tokens", calls)
	}
	after, _ := rig.manager.poolRecordFor(rig.keyID)
	if !after.blocked() || after.Strikes != before.Strikes || after.probeToken != 0 {
		t.Fatalf("count_tokens changed the funding state: before %+v after %+v", before, after)
	}

	// Direct results: neither a token-count success nor a token-count credit
	// failure is recheck evidence.
	seq := rig.manager.nextExecutionSeq()
	rig.manager.MarkResult(context.Background(), Result{AuthID: rig.keyID, Provider: "claude", Model: rig.models[0], Success: true, SkipQuotaObservation: true, ExecutionSeq: seq})
	rig.manager.MarkResult(context.Background(), Result{AuthID: rig.keyID, Provider: "claude", Model: rig.models[0], Error: resultErrorFromError(poolCreditError()), SkipQuotaObservation: true, ExecutionSeq: rig.manager.nextExecutionSeq()})
	if rec, _ := rig.manager.poolRecordFor(rig.keyID); !rec.blocked() || rec.Strikes != 1 || rec.RecheckAt.After(time.Now()) {
		t.Fatalf("token-count results changed the recheck: %+v", rec)
	}

	// A generation success is the evidence that clears it.
	rig.manager.MarkResult(context.Background(), Result{AuthID: rig.keyID, Provider: "claude", Model: rig.models[0], Success: true, ExecutionSeq: rig.manager.nextExecutionSeq()})
	if rec, _ := rig.manager.poolRecordFor(rig.keyID); rec.blocked() || rec.Strikes != 0 {
		t.Fatalf("generation success did not clear the block: %+v", rec)
	}
}

func TestPoolRecheck_StaleFailureIsNotRecheckEvidence(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	ctx := context.Background()
	staleSeq := rig.manager.nextExecutionSeq() // dispatched before the block
	blockPoolKeyForTest(t, rig)
	rig.manager.setPoolRecheckForTest(rig.keyID, time.Now().Add(-time.Second))
	due, _ := rig.manager.poolRecordFor(rig.keyID)

	rig.manager.MarkResult(ctx, Result{AuthID: rig.keyID, Provider: "claude", Model: rig.models[0], Error: resultErrorFromError(poolCreditError()), ExecutionSeq: staleSeq})
	rec, _ := rig.manager.poolRecordFor(rig.keyID)
	if rec.Strikes != 1 || !rec.RecheckAt.Equal(due.RecheckAt) {
		t.Fatalf("a delayed old failure moved the recheck: before %+v after %+v", due, rec)
	}
	// A stale success proves nothing either.
	rig.manager.MarkResult(ctx, Result{AuthID: rig.keyID, Provider: "claude", Model: rig.models[0], Success: true, ExecutionSeq: staleSeq})
	if rec, _ = rig.manager.poolRecordFor(rig.keyID); !rec.blocked() {
		t.Fatalf("a delayed old success cleared the block: %+v", rec)
	}

	// The genuine probe, dispatched after the block, extends the backoff.
	rig.manager.MarkResult(ctx, Result{AuthID: rig.keyID, Provider: "claude", Model: rig.models[0], Error: resultErrorFromError(poolCreditError()), ExecutionSeq: rig.manager.nextExecutionSeq()})
	if rec, _ = rig.manager.poolRecordFor(rig.keyID); rec.Strikes != 2 || time.Until(rec.RecheckAt) < 90*time.Minute {
		t.Fatalf("genuine probe failure record = %+v, want strike 2 with a 2h recheck", rec)
	}
}

func TestPoolCreditBlock_OverridesDisableCooling(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	SetQuotaCooldownDisabled(true)
	t.Cleanup(func() { SetQuotaCooldownDisabled(false) })
	ctx := context.Background()
	if _, err := rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: rig.models[0]}, cliproxyexecutor.Options{}); err != nil {
		t.Fatalf("first request: %v", err)
	}
	rig.takeCalls()
	for _, model := range rig.models {
		if _, err := rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{}); err != nil {
			t.Fatalf("request for %s: %v", model, err)
		}
	}
	if calls := rig.takeCalls(); countCalls(calls, rig.keyID) != 0 {
		t.Fatalf("with cooling disabled the out-of-credits key was still used: %+v", calls)
	}
}

func TestPoolBillingCodes_FailOverEndToEnd(t *testing.T) {
	billing := map[string]func() error{
		"json error.code 400": func() error {
			return customStatusError{code: http.StatusBadRequest, msg: `{"error":{"code":"billing_hard_limit_reached","message":"Billing hard limit has been reached"}}`}
		},
		"json error.type 429": func() error {
			return customStatusError{code: http.StatusTooManyRequests, msg: `{"error":{"type":"insufficient_quota","code":"insufficient_quota","message":"You exceeded your current quota"}}`}
		},
		"typed Error.Code 400": func() error {
			return &Error{Code: "insufficient_balance", Message: "Account balance is insufficient", HTTPStatus: http.StatusBadRequest}
		},
	}
	for name, makeErr := range billing {
		t.Run(name, func(t *testing.T) {
			for _, stream := range []bool{false, true} {
				rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
				registerPoolKeyErrorExecutor(rig, makeErr)
				if err := runPoolRequest(rig, rig.models[0], stream); err != nil {
					t.Fatalf("stream=%v: expected same-model failover, got %v", stream, err)
				}
				calls := rig.takeCalls()
				if len(calls) != 2 || calls[0].authID != rig.keyID || calls[1].authID != rig.subID || calls[1].model != rig.models[0] {
					t.Fatalf("stream=%v calls = %+v, want key then subscription on the same model", stream, calls)
				}
				if rec, ok := rig.manager.poolRecordFor(rig.keyID); !ok || !rec.blocked() {
					t.Fatalf("stream=%v: billing code did not block the key: %+v", stream, rec)
				}
			}
		})
	}

	notBilling := map[string]func() error{
		"request fault 400": func() error {
			return customStatusError{code: http.StatusBadRequest, msg: `{"error":{"type":"invalid_request_error","message":"prompt is too long: insufficient_quota"}}`}
		},
		"server error with billing code": func() error {
			return customStatusError{code: http.StatusInternalServerError, msg: `{"error":{"code":"insufficient_quota","message":"upstream bug"}}`}
		},
	}
	for name, makeErr := range notBilling {
		t.Run(name, func(t *testing.T) {
			rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
			registerPoolKeyErrorExecutor(rig, makeErr)
			_ = runPoolRequest(rig, rig.models[0], false)
			if rec, ok := rig.manager.poolRecordFor(rig.keyID); ok && rec.blocked() {
				t.Fatalf("a non-billing error blocked the account: %+v", rec)
			}
		})
	}
	if IsInsufficientCreditsError(customStatusError{code: http.StatusBadRequest, msg: `{"messages":[{"content":"insufficient_quota"}]}`}) {
		t.Fatal("a billing word inside request content must not count")
	}
}

func registerPoolKeyErrorExecutor(rig *poolTestRig, makeErr func() error) {
	rig.manager.RegisterExecutor(&customStreamMockExecutor{
		identifier: "claude",
		mockCustomErrorExecutor: mockCustomErrorExecutor{
			identifier: "claude",
			executeFn: func(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
				rig.record(auth, req.Model)
				if auth.ID == rig.keyID {
					return cliproxyexecutor.Response{}, makeErr()
				}
				return cliproxyexecutor.Response{Payload: []byte(`{"type":"message","stop_reason":"end_turn"}`)}, nil
			},
		},
		streamFn: func(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
			rig.record(auth, req.Model)
			if auth.ID == rig.keyID {
				return nil, makeErr()
			}
			return successStreamResult(), nil
		},
	})
}

func runPoolRequest(rig *poolTestRig, model string, stream bool) error {
	ctx := context.Background()
	if !stream {
		_, err := rig.manager.Execute(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		return err
	}
	result, err := rig.manager.ExecuteStream(ctx, []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		return err
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			return chunk.Err
		}
	}
	return nil
}

func TestPoolView_PaidExclusionKeepsHealthReason(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "1", "0", always)
	blockPoolKeyForTest(t, rig)
	rig.manager.SetPoolPolicy(PoolPolicy{Strategy: PoolStrategyExisting, AllowPaidFallback: false})

	key := poolAccountByLabelID(t, rig.manager, rig.keyID)
	if key.State != PoolStateOutOfCredits || key.Eligible {
		t.Fatalf("key = %+v, want out_of_credits and not eligible", key)
	}
	if strings.Count(key.Reason, "paid fallback") != 1 || !strings.Contains(key.Reason, "no credit left") {
		t.Fatalf("reason = %q, want the policy note once plus the credit reason", key.Reason)
	}
	if strings.Contains(key.HealthReason, "paid fallback") || !strings.Contains(key.HealthReason, "no credit left") {
		t.Fatalf("healthReason = %q, want only the credit reason", key.HealthReason)
	}
	explanation := rig.manager.ExplainPoolRoute(rig.models[0], time.Now())
	for _, candidate := range explanation.Candidates {
		if candidate.ID == key.ID && candidate.Reason != key.Reason {
			t.Fatalf("explain reason = %q, want %q", candidate.Reason, key.Reason)
		}
	}
	sub := poolAccountByLabelID(t, rig.manager, rig.subID)
	if sub.State != PoolStateReady || !sub.Eligible || sub.Reason != "Ready" {
		t.Fatalf("used subscription = %+v, want plain Ready", sub)
	}
}

func TestPoolView_ExplainTieWordingFollowsStrategy(t *testing.T) {
	rig := newPoolTestRig(t, &memoryPoolHealthStore{}, "0", "0", always)
	unused := poolAccountByLabelID(t, rig.manager, rig.subID)
	if unused.State != PoolStateReady || !strings.Contains(unused.Reason, "no successful request observed") {
		t.Fatalf("never-used account = %+v, want ready with an unverified note", unused)
	}
	for strategy, want := range map[string]string{"round-robin": "rotates", "fill-first": "keeps using one of them"} {
		rig.manager.runtimeConfig.Store(&internalconfig.Config{Routing: internalconfig.RoutingConfig{Strategy: strategy}})
		explanation := rig.manager.ExplainPoolRoute(rig.models[0], time.Now())
		if len(explanation.Tied) != 2 || explanation.SelectedAccountID != nil {
			t.Fatalf("%s: explanation = %+v, want two tied accounts and no single selection", strategy, explanation)
		}
		if !strings.Contains(explanation.Reason, want) || (strategy == "fill-first" && strings.Contains(explanation.Reason, "rotates")) {
			t.Fatalf("%s: reason = %q, want %q", strategy, explanation.Reason, want)
		}
		if !strings.Contains(explanation.Note, "per-request filters") {
			t.Fatalf("%s: note = %q, want the model-only limitation", strategy, explanation.Note)
		}
	}
}

func TestPoolOverview_HomeModeIsMemoryOnly(t *testing.T) {
	rig := newPoolTestRig(t, nil, "1", "0", always)
	rig.manager.SetPoolHealthStore(MemoryPoolHealthStore{})
	cfg := &internalconfig.Config{}
	cfg.Home.Enabled = true
	rig.manager.runtimeConfig.Store(cfg)
	overview := rig.manager.PoolOverview(time.Now())
	if !overview.HomeMode || overview.Persistence != PoolPersistenceMemory {
		t.Fatalf("overview homeMode=%v persistence=%q, want Home mode reported as memory-only", overview.HomeMode, overview.Persistence)
	}
	if err := rig.manager.RestorePoolHealth(context.Background()); err != nil {
		t.Fatalf("restore from the memory store: %v", err)
	}
}
