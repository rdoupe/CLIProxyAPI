package auth

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
)

// PoolSchemaVersion is the schema version of the pool health API.
const PoolSchemaVersion = 1

// PoolBalance is a provider-reported balance. The pool never estimates one, so
// accounts without a provider-reported balance carry a nil *PoolBalance.
type PoolBalance struct {
	Amount   *float64 `json:"amount"`
	Currency string   `json:"currency"`
}

// PoolAccount is the sanitized health view of one credential.
type PoolAccount struct {
	ID            string       `json:"id"`
	Provider      string       `json:"provider"`
	Label         string       `json:"label"`
	Kind          string       `json:"kind"`
	State         string       `json:"state"`
	Reason        string       `json:"reason"`
	Eligible      bool         `json:"eligible"`
	Priority      int          `json:"priority"`
	ResetAt       *time.Time   `json:"resetAt"`
	LastSuccessAt *time.Time   `json:"lastSuccessAt"`
	LastFailureAt *time.Time   `json:"lastFailureAt"`
	Balance       *PoolBalance `json:"balance"`
}

// PoolOverview is the account part of GET /v8/management/pool/overview.
type PoolOverview struct {
	SchemaVersion int           `json:"schemaVersion"`
	ObservedAt    time.Time     `json:"observedAt"`
	Accounts      []PoolAccount `json:"accounts"`
	Policy        PoolPolicy    `json:"policy"`
	Persistence   string        `json:"persistence"`
}

// PoolRouteCandidate is one credential considered by ExplainPoolRoute.
type PoolRouteCandidate struct {
	ID                string `json:"id"`
	Label             string `json:"label"`
	Provider          string `json:"provider"`
	Kind              string `json:"kind"`
	Eligible          bool   `json:"eligible"`
	Reason            string `json:"reason"`
	Priority          int    `json:"priority"`
	EffectivePriority int    `json:"effectivePriority"`
}

// PoolRouteExplanation is the read-only routing explanation for one model.
type PoolRouteExplanation struct {
	Model             string               `json:"model"`
	Policy            PoolPolicy           `json:"policy"`
	SelectedAccountID *string              `json:"selectedAccountId"`
	Tied              []string             `json:"tiedAccountIds"`
	Candidates        []PoolRouteCandidate `json:"candidates"`
	Reason            string               `json:"reason"`
}

// PoolLabelForAuth returns a display label that never contains credential material.
func PoolLabelForAuth(auth *Auth) string {
	if auth == nil {
		return ""
	}
	apiKey := authAttribute(auth, AttributeAPIKey)
	clean := func(value string) string {
		value = strings.TrimSpace(value)
		if value == "" || (apiKey != "" && strings.Contains(value, apiKey)) || looksLikeSecret(value) {
			return ""
		}
		return value
	}
	provider := poolProviderDisplay(auth.Provider)
	if PoolKindForAuth(auth) == PoolKindAPIKey {
		label := clean(auth.Label)
		if label == "" || strings.EqualFold(label, strings.ToLower(auth.Provider)+"-apikey") {
			label = provider + " API key"
		}
		if raw := authAttribute(auth, AttributeConfigIndex); raw != "" {
			if index, err := strconv.Atoi(raw); err == nil {
				label = fmt.Sprintf("%s #%d", label, index+1)
			}
		}
		return label
	}
	if _, email := auth.AccountInfo(); clean(email) != "" {
		return clean(email)
	}
	if label := clean(auth.Label); label != "" {
		return label
	}
	return provider + " account"
}

func looksLikeSecret(value string) bool {
	lower := strings.ToLower(value)
	for _, prefix := range []string{"sk-", "sk_", "eyj", "bearer ", "ya29.", "aiza", "xai-", "gsk_"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return len(value) >= 40 && !strings.ContainsAny(value, " @")
}

func poolProviderDisplay(provider string) string {
	provider = strings.TrimSpace(provider)
	switch strings.ToLower(provider) {
	case "claude":
		return "Claude"
	case "codex":
		return "Codex"
	case "gemini", "gemini-cli":
		return "Gemini"
	case "":
		return "Unknown"
	}
	return strings.ToUpper(provider[:1]) + provider[1:]
}

// poolAccountHealth derives state, reason, health eligibility and reset time.
func poolAccountHealth(auth *Auth, rec *PoolHealthRecord, now time.Time) (state, reason string, eligible bool, resetAt time.Time) {
	if auth.Disabled || auth.Status == StatusDisabled {
		return PoolStatePaused, "Paused: turned off by an operator", false, time.Time{}
	}
	if rec.blocked() {
		if rec.RecheckAt.After(now) {
			return PoolStateOutOfCredits, rec.Reason + "; it is skipped for every model until the automatic recheck", false, rec.RecheckAt
		}
		return PoolStateOutOfCredits, "Recheck due: the next request will test whether credit was added", true, time.Time{}
	}
	if hasUnauthorizedAuthFailure(auth) || auth.RequiresSignIn || hasDisabledInvalidGrantFailure(auth) {
		return PoolStateLoginExpired, "Sign-in expired or was revoked; sign in again", false, time.Time{}
	}
	if exp, ok := auth.AccessTokenExpirationTime(); ok && !exp.IsZero() && !exp.After(now) && !authHasRefreshCredential(auth) {
		return PoolStateLoginExpired, "Access token expired and cannot be renewed; sign in again", false, time.Time{}
	}
	if auth.Quota.Exceeded && auth.Quota.NextRecoverAt.After(now) && (auth.Quota.Reason == "credential_quota" || len(auth.ModelStates) == 0) {
		return PoolStateRateLimited, "Usage limit reached for this account", false, auth.Quota.NextRecoverAt
	}
	quotaModels, errorModels := 0, 0
	var quotaReset, errorReset time.Time
	for _, modelState := range auth.ModelStates {
		if modelState == nil || !isModelStateActiveCooldown(modelState, now) {
			continue
		}
		next := modelState.NextRetryAfter
		if modelState.Quota.Exceeded && modelState.Quota.NextRecoverAt.After(next) {
			next = modelState.Quota.NextRecoverAt
		}
		if modelState.Quota.Exceeded {
			quotaModels++
			if quotaReset.IsZero() || next.Before(quotaReset) {
				quotaReset = next
			}
		} else {
			errorModels++
			if errorReset.IsZero() || next.Before(errorReset) {
				errorReset = next
			}
		}
	}
	if auth.Unavailable && auth.NextRetryAfter.After(now) && len(auth.ModelStates) == 0 {
		return PoolStateUnavailable, poolErrorReason(auth.LastError), false, auth.NextRetryAfter
	}
	if quotaModels > 0 {
		return PoolStateRateLimited, fmt.Sprintf("Usage limit reached on %d model(s); other models still work", quotaModels), true, quotaReset
	}
	if errorModels > 0 {
		return PoolStateUnavailable, fmt.Sprintf("%d model(s) cooling down after upstream errors; other models still work", errorModels), true, errorReset
	}
	switch auth.Status {
	case StatusUnknown, StatusPending:
		return PoolStateUnknown, "Not yet verified", true, time.Time{}
	}
	return PoolStateReady, "Ready", true, time.Time{}
}

func poolErrorReason(err *Error) string {
	status := statusCodeFromResult(err)
	switch {
	case status == http.StatusTooManyRequests:
		return "Rate limited by the provider"
	case status == http.StatusForbidden || status == http.StatusPaymentRequired:
		return fmt.Sprintf("Provider refused the account (HTTP %d)", status)
	case status > 0:
		return fmt.Sprintf("Temporarily unavailable after an upstream error (HTTP %d)", status)
	}
	return "Temporarily unavailable after an upstream error"
}

func timePtr(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	utc := value.UTC()
	return &utc
}

// PoolOverview returns the sanitized health of every credential.
func (m *Manager) PoolOverview(now time.Time) PoolOverview {
	overview := PoolOverview{SchemaVersion: PoolSchemaVersion, ObservedAt: now.UTC(), Policy: m.PoolPolicy(), Persistence: m.poolPersistence(), Accounts: []PoolAccount{}}
	if m == nil {
		return overview
	}
	m.mu.RLock()
	auths := make([]*Auth, 0, len(m.auths))
	for _, auth := range m.auths {
		if auth != nil {
			auths = append(auths, auth.Clone())
		}
	}
	m.mu.RUnlock()
	for _, auth := range auths {
		// The index is a deterministic hash, so computing it on the copy is exact.
		auth.EnsureIndex()
		overview.Accounts = append(overview.Accounts, m.poolAccountFor(auth, now))
	}
	sort.SliceStable(overview.Accounts, func(i, j int) bool {
		a, b := overview.Accounts[i], overview.Accounts[j]
		if a.Provider != b.Provider {
			return a.Provider < b.Provider
		}
		if a.Kind != b.Kind {
			return a.Kind == PoolKindSubscription
		}
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		return a.Label < b.Label
	})
	return overview
}

func (m *Manager) poolAccountFor(auth *Auth, now time.Time) PoolAccount {
	rec, hasRecord := m.poolRecordFor(auth.ID)
	var recPtr *PoolHealthRecord
	if hasRecord {
		recPtr = &rec
	}
	state, reason, eligible, resetAt := poolAccountHealth(auth, recPtr, now)
	if auth.pool.paidExcluded {
		eligible = false
		reason = "Not used: paid fallback is disabled by policy"
		if state != PoolStateReady && state != PoolStateUnknown {
			reason = "Not used: paid fallback is disabled by policy; " + reason
		}
	}
	account := PoolAccount{
		ID:       auth.Index,
		Provider: strings.ToLower(strings.TrimSpace(auth.Provider)),
		Label:    PoolLabelForAuth(auth),
		Kind:     PoolKindForAuth(auth),
		State:    state,
		Reason:   reason,
		Eligible: eligible,
		Priority: authConfiguredPriority(auth),
		ResetAt:  timePtr(resetAt),
	}
	if hasRecord {
		account.LastSuccessAt = timePtr(rec.LastSuccessAt)
		account.LastFailureAt = timePtr(rec.LastFailureAt)
	}
	return account
}

// ExplainPoolRoute explains, without contacting any provider or changing
// selection state, which account a request for model would use next.
func (m *Manager) ExplainPoolRoute(model string, now time.Time) PoolRouteExplanation {
	model = strings.TrimSpace(model)
	explanation := PoolRouteExplanation{Model: model, Policy: m.PoolPolicy(), Tied: []string{}, Candidates: []PoolRouteCandidate{}}
	if m == nil || model == "" {
		explanation.Reason = "No model given"
		return explanation
	}
	var candidates []PoolRouteCandidate
	m.mu.RLock()
	registryRef := registry.GetGlobalRegistry()
	for _, auth := range m.auths {
		if auth == nil || !m.authSupportsRouteModel(registryRef, auth, model) {
			continue
		}
		snapshot := auth.Clone()
		snapshot.EnsureIndex()
		_, hasExecutor := m.executorLocked(executorKeyFromAuth(auth))
		checkModel := m.selectionModelForAuth(auth, model)
		blocked, _, next := isAuthBlockedForModel(snapshot, checkModel, now)
		candidates = append(candidates, m.poolRouteCandidate(snapshot, hasExecutor, blocked, next, now))
	}
	m.mu.RUnlock()
	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.Eligible != b.Eligible {
			return a.Eligible
		}
		if a.EffectivePriority != b.EffectivePriority {
			return a.EffectivePriority > b.EffectivePriority
		}
		return a.Label < b.Label
	})
	explanation.Candidates = append(explanation.Candidates, candidates...)
	if len(candidates) == 0 {
		explanation.Reason = "No registered account serves this model"
		return explanation
	}
	if !candidates[0].Eligible {
		explanation.Reason = "No eligible account: " + candidates[0].Reason
		return explanation
	}
	top := candidates[0].EffectivePriority
	for _, candidate := range candidates {
		if candidate.Eligible && candidate.EffectivePriority == top {
			explanation.Tied = append(explanation.Tied, candidate.ID)
		}
	}
	strategy := "round-robin"
	if cfg, ok := m.runtimeConfig.Load().(*internalconfig.Config); ok && cfg != nil && strings.TrimSpace(cfg.Routing.Strategy) != "" {
		strategy = strings.TrimSpace(cfg.Routing.Strategy)
	}
	affinityNote := ""
	if cfg, ok := m.runtimeConfig.Load().(*internalconfig.Config); ok && cfg != nil && cfg.Routing.SessionAffinity {
		affinityNote = " An existing session binding may keep a session on another eligible account."
	}
	if len(explanation.Tied) == 1 {
		id := explanation.Tied[0]
		explanation.SelectedAccountID = &id
		explanation.Reason = fmt.Sprintf("%s is the only eligible account at the top of the order.%s", candidates[0].Label, affinityNote)
		return explanation
	}
	explanation.Reason = fmt.Sprintf("%d eligible accounts share the top of the order; the %s strategy rotates between them.%s", len(explanation.Tied), strategy, affinityNote)
	return explanation
}

func (m *Manager) poolRouteCandidate(auth *Auth, hasExecutor, blocked bool, next time.Time, now time.Time) PoolRouteCandidate {
	account := m.poolAccountFor(auth, now)
	candidate := PoolRouteCandidate{
		ID:                account.ID,
		Label:             account.Label,
		Provider:          account.Provider,
		Kind:              account.Kind,
		Priority:          account.Priority,
		EffectivePriority: authPriority(auth),
	}
	switch {
	case !hasExecutor:
		candidate.Reason = "No executor is registered for this provider"
	case auth.pool.paidExcluded:
		candidate.Reason = "Not used: paid fallback is disabled by policy"
	case blocked && account.State != PoolStateReady:
		candidate.Reason = account.Reason
	case blocked && !next.IsZero():
		candidate.Reason = "Cooling down for this model until " + next.UTC().Format(time.RFC3339)
	case blocked:
		candidate.Reason = "Unavailable for this model"
	default:
		candidate.Eligible = true
		candidate.Reason = fmt.Sprintf("Eligible (%s, priority %d)", strings.ReplaceAll(account.Kind, "_", " "), account.Priority)
	}
	return candidate
}
