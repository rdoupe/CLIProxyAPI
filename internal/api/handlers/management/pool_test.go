package management

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

const (
	poolHandlerKeySecret = "sk-ant-api03-handler-secret"
	poolHandlerToken     = "sk-ant-oat01-handler-token"
)

func newPoolHandlerFixture(t *testing.T) (*Handler, *coreauth.Manager, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(poolNoopExecutor{})
	ctx := coreauth.WithSkipPersist(context.Background())
	key := &coreauth.Auth{ID: "pool-handler-key", Provider: "claude", Status: coreauth.StatusActive, Label: "claude-apikey",
		Attributes: map[string]string{"api_key": poolHandlerKeySecret, "auth_kind": "apikey", "priority": "0"}}
	sub := &coreauth.Auth{ID: "pool-handler-sub.json", Provider: "claude", Status: coreauth.StatusActive,
		Attributes: map[string]string{"auth_kind": "oauth"},
		Metadata:   map[string]any{"email": "owner@example.com", "access_token": poolHandlerToken}}
	reg := registry.GetGlobalRegistry()
	for _, auth := range []*coreauth.Auth{key, sub} {
		reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "pool-handler-model"}})
		if _, err := manager.Register(ctx, auth); err != nil {
			t.Fatalf("register: %v", err)
		}
	}
	t.Cleanup(func() {
		reg.UnregisterClient(key.ID)
		reg.UnregisterClient(sub.ID)
	})
	manager.MarkResult(context.Background(), coreauth.Result{
		AuthID: key.ID, Provider: "claude", Model: "pool-handler-model", Success: false,
		Error: &coreauth.Error{HTTPStatus: http.StatusBadRequest, Message: `{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API."}}`},
	})
	h := &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t), authManager: manager}
	return h, manager, key.ID
}

func servePool(handler func(*gin.Context), method, target, body string, params ...gin.Param) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(method, target, strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	ctx.Params = params
	handler(ctx)
	return rec
}

func TestPoolOverviewContract(t *testing.T) {
	h, _, _ := newPoolHandlerFixture(t)
	rec := servePool(h.GetPoolOverview, http.MethodGet, "/v8/management/pool/overview", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, secret := range []string{poolHandlerKeySecret, poolHandlerToken, "pool-handler-key", "pool-handler-sub.json"} {
		if strings.Contains(body, secret) {
			t.Fatalf("overview leaks %q: %s", secret, body)
		}
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, key := range []string{"schemaVersion", "observedAt", "accounts", "policy", "stats", "persistence"} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("overview missing %q: %s", key, body)
		}
	}
	accounts := payload["accounts"].([]any)
	if len(accounts) != 2 {
		t.Fatalf("accounts = %d", len(accounts))
	}
	states := map[string]string{}
	for _, raw := range accounts {
		account := raw.(map[string]any)
		for _, key := range []string{"id", "provider", "label", "kind", "state", "reason", "eligible", "priority", "resetAt", "lastSuccessAt", "balance"} {
			if _, ok := account[key]; !ok {
				t.Fatalf("account missing %q: %v", key, account)
			}
		}
		if account["balance"] != nil {
			t.Fatalf("balance must be null when unknown: %v", account)
		}
		states[account["kind"].(string)] = account["state"].(string)
	}
	if states["api_key"] != "out_of_credits" || states["subscription"] != "ready" {
		t.Fatalf("states = %v", states)
	}
	policy := payload["policy"].(map[string]any)
	if policy["strategy"] != "existing" || policy["allowPaidFallback"] != true {
		t.Fatalf("default policy = %v", policy)
	}
	stats := payload["stats"].(map[string]any)
	if stats["available"] != false || stats["reason"] == "" || stats["inputTokens"] != nil {
		t.Fatalf("stats without a collector must be unavailable with unknown tokens: %v", stats)
	}
}

func TestPoolExplainStatsAndResetHandlers(t *testing.T) {
	h, manager, keyID := newPoolHandlerFixture(t)
	if rec := servePool(h.GetPoolExplain, http.MethodGet, "/v8/management/pool/explain", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("explain without model = %d", rec.Code)
	}
	rec := servePool(h.GetPoolExplain, http.MethodGet, "/v8/management/pool/explain?model=pool-handler-model", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("explain = %d %s", rec.Code, rec.Body.String())
	}
	var explain coreauth.PoolRouteExplanation
	if err := json.Unmarshal(rec.Body.Bytes(), &explain); err != nil {
		t.Fatalf("decode explain: %v", err)
	}
	if explain.SelectedAccountID == nil || len(explain.Candidates) != 2 {
		t.Fatalf("explain = %+v, want the subscription selected among 2 candidates", explain)
	}
	if strings.Contains(rec.Body.String(), poolHandlerKeySecret) {
		t.Fatal("explain leaks the API key")
	}
	if rec = servePool(h.GetPoolStats, http.MethodGet, "/v8/management/pool/stats?range=99d", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad range = %d", rec.Code)
	}
	if rec = servePool(h.GetPoolStats, http.MethodGet, "/v8/management/pool/stats?range=7d", ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"available":false`) {
		t.Fatalf("stats = %d %s", rec.Code, rec.Body.String())
	}
	if rec = servePool(h.ResetPoolAccount, http.MethodPost, "/v8/management/pool/accounts/missing/reset", "", gin.Param{Key: "id", Value: "missing"}); rec.Code != http.StatusNotFound {
		t.Fatalf("reset missing = %d", rec.Code)
	}
	var keyIndex string
	for _, auth := range manager.List() {
		if auth.ID == keyID {
			keyIndex = auth.EnsureIndex()
		}
	}
	rec = servePool(h.ResetPoolAccount, http.MethodPost, "/v8/management/pool/accounts/"+keyIndex+"/reset", "", gin.Param{Key: "id", Value: keyIndex})
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"state":"ready"`) {
		t.Fatalf("reset = %d %s", rec.Code, rec.Body.String())
	}
}

func TestPoolPolicyHandlerPersistsConfig(t *testing.T) {
	h, manager, _ := newPoolHandlerFixture(t)
	if rec := servePool(h.PutPoolPolicy, http.MethodPut, "/v8/management/pool/policy", `{"strategy":"bogus"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bogus strategy = %d", rec.Code)
	}
	rec := servePool(h.PutPoolPolicy, http.MethodPut, "/v8/management/pool/policy", `{"strategy":"subscriptions_first","allowPaidFallback":false}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("put policy = %d %s", rec.Code, rec.Body.String())
	}
	if got := manager.PoolPolicy(); got.Strategy != coreauth.PoolStrategySubscriptionsFirst || got.AllowPaidFallback {
		t.Fatalf("manager policy = %+v", got)
	}
	saved, err := os.ReadFile(h.configFilePath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	for _, want := range []string{"subscriptions-first", "allow-paid-fallback: false"} {
		if !strings.Contains(string(saved), want) {
			t.Fatalf("saved config missing %q:\n%s", want, saved)
		}
	}
	loaded, err := config.LoadConfig(h.configFilePath)
	if err != nil {
		t.Fatalf("reload config: %v", err)
	}
	if got := coreauth.PoolPolicyFromConfig(loaded); got.Strategy != coreauth.PoolStrategySubscriptionsFirst || got.AllowPaidFallback {
		t.Fatalf("policy from reloaded config = %+v", got)
	}
}

// poolNoopExecutor registers the claude provider without ever contacting it.
type poolNoopExecutor struct{}

func (poolNoopExecutor) Identifier() string { return "claude" }

func (poolNoopExecutor) Execute(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not used")
}

func (poolNoopExecutor) ExecuteStream(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, errors.New("not used")
}

func (poolNoopExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (poolNoopExecutor) CountTokens(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not used")
}

func (poolNoopExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not used")
}
