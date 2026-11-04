package management

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/poolstats"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// poolOverviewStatsRange is the window summarized in the overview.
const poolOverviewStatsRange = 24 * time.Hour

type poolStatsSummary struct {
	Available        bool       `json:"available"`
	Reason           string     `json:"reason,omitempty"`
	Range            string     `json:"range"`
	From             *time.Time `json:"from"`
	To               *time.Time `json:"to"`
	Persistence      string     `json:"persistence,omitempty"`
	Requests         int64      `json:"requests"`
	Successes        int64      `json:"successes"`
	Failures         int64      `json:"failures"`
	Failovers        int64      `json:"failovers"`
	InputTokens      *int64     `json:"inputTokens"`
	OutputTokens     *int64     `json:"outputTokens"`
	AverageLatencyMs *float64   `json:"averageLatencyMs"`
}

type poolOverviewResponse struct {
	coreauth.PoolOverview
	Stats poolStatsSummary `json:"stats"`
}

type poolAccountBreakdown struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	poolstats.Totals
}

type poolStatsResponse struct {
	Available     bool                    `json:"available"`
	Reason        string                  `json:"reason,omitempty"`
	Range         string                  `json:"range"`
	From          *time.Time              `json:"from"`
	To            *time.Time              `json:"to"`
	BucketSeconds int64                   `json:"bucketSeconds"`
	Persistence   string                  `json:"persistence,omitempty"`
	ObservedSince *time.Time              `json:"observedSince"`
	Totals        poolstats.Totals        `json:"totals"`
	Series        []poolstats.SeriesPoint `json:"series"`
	ByProvider    []poolstats.Breakdown   `json:"byProvider"`
	ByModel       []poolstats.Breakdown   `json:"byModel"`
	ByAccount     []poolAccountBreakdown  `json:"byAccount"`
}

func (h *Handler) poolManager(c *gin.Context) *coreauth.Manager {
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return nil
	}
	return h.authManager
}

func poolStatsReport(c *gin.Context, span time.Duration) poolstats.Report {
	collector := poolstats.Default()
	if collector == nil {
		return poolstats.Unavailable("request statistics are not being collected by this server")
	}
	return collector.Query(c.Request.Context(), span)
}

func poolRangeLabel(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return "24h"
	}
	return raw
}

// GetPoolOverview returns every account's sanitized health, the pool policy and a 24h stats summary.
func (h *Handler) GetPoolOverview(c *gin.Context) {
	manager := h.poolManager(c)
	if manager == nil {
		return
	}
	report := poolStatsReport(c, poolOverviewStatsRange)
	response := poolOverviewResponse{
		PoolOverview: manager.PoolOverview(time.Now()),
		Stats: poolStatsSummary{
			Available:        report.Available,
			Reason:           report.Reason,
			Range:            "24h",
			From:             report.From,
			To:               report.To,
			Persistence:      report.Persistence,
			Requests:         report.Totals.Requests,
			Successes:        report.Totals.Successes,
			Failures:         report.Totals.Failures,
			Failovers:        report.Totals.Failovers,
			InputTokens:      report.Totals.InputTokens,
			OutputTokens:     report.Totals.OutputTokens,
			AverageLatencyMs: report.Totals.AverageLatencyMs,
		},
	}
	c.JSON(http.StatusOK, response)
}

// GetPoolStats returns bounded request statistics for ?range= (1h..30d, default 24h).
func (h *Handler) GetPoolStats(c *gin.Context) {
	span, errRange := poolstats.ParseRange(c.Query("range"))
	if errRange != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_range", "message": errRange.Error()})
		return
	}
	report := poolStatsReport(c, span)
	labels := map[string]string{}
	if h != nil && h.authManager != nil {
		for _, account := range h.authManager.PoolOverview(time.Now()).Accounts {
			labels[account.ID] = account.Label
		}
	}
	response := poolStatsResponse{
		Available:     report.Available,
		Reason:        report.Reason,
		Range:         poolRangeLabel(c.Query("range")),
		From:          report.From,
		To:            report.To,
		BucketSeconds: report.BucketSeconds,
		Persistence:   report.Persistence,
		ObservedSince: report.ObservedSince,
		Totals:        report.Totals,
		Series:        report.Series,
		ByProvider:    report.ByProvider,
		ByModel:       report.ByModel,
		ByAccount:     make([]poolAccountBreakdown, 0, len(report.ByAccount)),
	}
	for _, row := range report.ByAccount {
		label, ok := labels[row.Name]
		switch {
		case ok:
		case row.Name == poolstats.UnknownKey:
			label = "Unattributed requests"
		case row.Name == poolstats.OverflowKey:
			label = "Other accounts"
		default:
			label = "Removed account"
		}
		response.ByAccount = append(response.ByAccount, poolAccountBreakdown{ID: row.Name, Label: label, Totals: row.Totals})
	}
	c.JSON(http.StatusOK, response)
}

// GetPoolExplain explains which account would serve ?model= next without contacting any provider.
func (h *Handler) GetPoolExplain(c *gin.Context) {
	manager := h.poolManager(c)
	if manager == nil {
		return
	}
	model := strings.TrimSpace(c.Query("model"))
	if model == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "model_required", "message": "query parameter model is required"})
		return
	}
	c.JSON(http.StatusOK, manager.ExplainPoolRoute(model, time.Now()))
}

// ResetPoolAccount clears the out-of-credits block and cooldowns of one account.
func (h *Handler) ResetPoolAccount(c *gin.Context) {
	manager := h.poolManager(c)
	if manager == nil {
		return
	}
	id := strings.TrimSpace(c.Param("id"))
	found, errReset := manager.ResetPoolAccount(c.Request.Context(), id)
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "account_not_found"})
		return
	}
	if errReset != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "reset_failed", "message": errReset.Error()})
		return
	}
	for _, account := range manager.PoolOverview(time.Now()).Accounts {
		if account.ID == id {
			c.JSON(http.StatusOK, gin.H{"status": "ok", "account": account})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// GetPoolPolicy returns the active pool policy.
func (h *Handler) GetPoolPolicy(c *gin.Context) {
	manager := h.poolManager(c)
	if manager == nil {
		return
	}
	c.JSON(http.StatusOK, manager.PoolPolicy())
}

// PutPoolPolicy updates routing.pool in the config file and applies it immediately.
func (h *Handler) PutPoolPolicy(c *gin.Context) {
	manager := h.poolManager(c)
	if manager == nil {
		return
	}
	var body struct {
		Strategy          *string `json:"strategy"`
		AllowPaidFallback *bool   `json:"allowPaidFallback"`
	}
	if errBind := c.ShouldBindJSON(&body); errBind != nil || (body.Strategy == nil && body.AllowPaidFallback == nil) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body", "message": "set strategy and/or allowPaidFallback"})
		return
	}
	policy := manager.PoolPolicy()
	if body.Strategy != nil {
		strategy, ok := coreauth.NormalizePoolStrategy(*body.Strategy)
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_strategy", "message": "strategy must be existing or subscriptions_first"})
			return
		}
		policy.Strategy = strategy
	}
	if body.AllowPaidFallback != nil {
		policy.AllowPaidFallback = *body.AllowPaidFallback
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if policy.Strategy == coreauth.PoolStrategySubscriptionsFirst {
		h.cfg.Routing.Pool.Strategy = "subscriptions-first"
	} else {
		h.cfg.Routing.Pool.Strategy = ""
	}
	if policy.AllowPaidFallback {
		h.cfg.Routing.Pool.AllowPaidFallback = nil
	} else {
		allow := false
		h.cfg.Routing.Pool.AllowPaidFallback = &allow
	}
	if h.persistLocked(c) {
		manager.SetPoolPolicy(policy)
	}
}
