# Account Pool API (schema version 1)

Read-mostly management endpoints that show every account in the pool (API keys
and login-based subscriptions), why each one is or is not being used, the
routing policy, and bounded request statistics.

All routes live under `/v8/management/pool/` and use the same management
authentication as every other `/v8/management` route. Times are RFC 3339 UTC
strings. A `null` value always means **unknown**: the server never estimates a
balance, a cost, a token count or a latency it did not observe.

## Account IDs and redaction

- `id` is the credential's public auth index (the same value other management
  routes call `auth_index`). It is a one-way hash, never a file name or key.
- `label` is a display name: the configured label, the subscription e-mail, or
  `"<Provider> API key #<n>"`. API keys, tokens, file names and raw upstream
  error bodies never appear in any response or in persisted health.

## Account states

| `state`          | Meaning                                                                 | `eligible` | `resetAt`                     |
|------------------|-------------------------------------------------------------------------|------------|-------------------------------|
| `ready`          | Healthy.                                                                | true       | null                          |
| `paused`         | Turned off by an operator (disabled).                                   | false      | null                          |
| `rate_limited`   | Usage limit reached. Account-wide: not eligible. Only some models: still eligible, `reason` says how many. | false / true | when the limit lifts (if known) |
| `out_of_credits` | Provider said the account has no credit left (e.g. Anthropic `400 Your credit balance is too low`). Skipped for **every** model. | false until the recheck is due, then true | time of the automatic recheck; null once due |
| `login_expired`  | Sign-in expired or was revoked; needs a new sign-in.                    | false      | null                          |
| `unavailable`    | Cooling down after upstream errors (5xx, 403, …).                      | false / true (model-only) | when the cooldown ends |
| `unknown`        | Not verified yet.                                                       | true       | null                          |

`eligible` also becomes `false` when policy forbids the account
(`allowPaidFallback: false` on an API key); `reason` then starts with
`"Not used: paid fallback is disabled by policy"`. `reason` is always a short
human-readable sentence safe to show as is.

`balance` is always `null` in this release (no provider balance source is
wired into the proxy). The UI must show "unknown", not zero.

## GET /v8/management/pool/overview

```jsonc
{
  "schemaVersion": 1,
  "observedAt": "2026-11-04T23:20:00Z",
  "persistence": "kv",                 // where account health survives restarts: "kv" | "memory"
  "accounts": [
    {
      "id": "a27dc58ccbb807a5",
      "provider": "claude",
      "label": "owner@example.com",
      "kind": "subscription",          // "subscription" | "api_key"
      "state": "ready",
      "reason": "Ready",
      "eligible": true,
      "priority": 0,                   // configured priority (higher is tried first)
      "resetAt": null,
      "lastSuccessAt": "2026-11-04T23:18:13Z",
      "lastFailureAt": null,
      "balance": null                  // {amount:number|null, currency:string} | null
    }
  ],
  "policy": { "strategy": "existing", "allowPaidFallback": true },
  "stats": {                           // last 24h; same fields as /stats totals
    "available": true,
    "reason": "…",                     // only present when available is false
    "range": "24h",
    "from": "2026-11-03T23:00:00Z",    // null when unavailable
    "to": "2026-11-04T23:20:00Z",      // null when unavailable
    "persistence": "kv",
    "requests": 42, "successes": 40, "failures": 2, "failovers": 1,
    "inputTokens": 120000,             // null when no request reported tokens
    "outputTokens": 30000,             // null when no request reported tokens
    "averageLatencyMs": 812.5          // null when no latency was observed
  }
}
```

Accounts are ordered by provider, subscriptions before API keys, then priority
(descending), then label.

## GET /v8/management/pool/stats?range=24h

`range`: `1h` … `30d` (`Nh` or `Nd`; default `24h`). Anything else → `400
{"error":"invalid_range"}`.

```jsonc
{
  "available": true,
  "reason": "…",                       // only when available is false
  "range": "24h",
  "from": "2026-11-03T23:00:00Z",      // start of the first hourly bucket
  "to": "2026-11-04T23:20:00Z",
  "bucketSeconds": 3600,
  "persistence": "kv",                 // "memory" = resets on restart
  "observedSince": "2026-11-01T08:00:00Z", // earliest data the server has
  "totals":  { "requests": 42, "successes": 40, "failures": 2, "failovers": 1,
               "inputTokens": 120000, "outputTokens": 30000, "averageLatencyMs": 812.5 },
  "series": [ { "at": "2026-11-04T22:00:00Z", "requests": 5, "successes": 5, "failures": 0,
                "failovers": 0, "inputTokens": null, "outputTokens": null, "averageLatencyMs": null } ],
  "byProvider": [ { "name": "claude", "requests": 42, "successes": 40, "failures": 2, "failovers": 1,
                    "inputTokens": 120000, "outputTokens": 30000, "averageLatencyMs": 812.5 } ],
  "byModel":    [ { "name": "claude-opus-5-5", "…": "same fields as byProvider" } ],
  "byAccount":  [ { "id": "a27dc58ccbb807a5", "label": "owner@example.com", "…": "same totals fields" } ]
}
```

- `series` has one point per hour in the range, including empty hours.
- Counting unit: **one upstream attempt**. A request that failed on an empty
  key and then succeeded on a subscription counts as 1 failure + 1 success and
  1 failover. `failovers` is attributed to the account the request moved **to**.
- Rows are sorted by `requests` descending. `byAccount` labels for accounts no
  longer configured read `"Removed account"`; `"(other)"` collects entries
  beyond 256 distinct names per hour; `"(unknown)"` has no attribution.
- When the collector is not running, `available` is `false`, `reason` explains,
  counters are 0 and every nullable field is `null`.

## GET /v8/management/pool/explain?model=…

Read-only: no provider is contacted and no rotation state moves. Missing
`model` → `400 {"error":"model_required"}`.

```jsonc
{
  "model": "claude-opus-5-5",
  "policy": { "strategy": "subscriptions_first", "allowPaidFallback": true },
  "selectedAccountId": "a27dc58ccbb807a5",   // null when none or when several tie
  "tiedAccountIds": ["a27dc58ccbb807a5"],    // eligible accounts sharing the top of the order
  "candidates": [
    { "id": "a27dc58ccbb807a5", "label": "owner@example.com", "provider": "claude",
      "kind": "subscription", "eligible": true, "reason": "Eligible (subscription, priority 0)",
      "priority": 0, "effectivePriority": 1000000000 },
    { "id": "cc9c30ba4f194403", "label": "Claude API key #1", "provider": "claude",
      "kind": "api_key", "eligible": false,
      "reason": "Provider reported the account has no credit left (HTTP 400); it is skipped for every model until the automatic recheck",
      "priority": 1, "effectivePriority": 1 }
  ],
  "reason": "owner@example.com is the only eligible account at the top of the order."
}
```

Candidates are the accounts registered for that exact model, eligible first,
then by `effectivePriority`. When several eligible accounts tie, the configured
`routing.strategy` (round-robin by default) rotates between them, so
`selectedAccountId` is `null` and `tiedAccountIds` lists them. With session
affinity on, an established session may stay on another eligible account; the
`reason` says so. The explanation never substitutes another model.

## GET /v8/management/pool/policy, PUT /v8/management/pool/policy

`GET` → `{"strategy":"existing"|"subscriptions_first","allowPaidFallback":bool}`.

`PUT` body (either field optional, at least one required):

```json
{ "strategy": "subscriptions_first", "allowPaidFallback": false }
```

→ `200 {"status":"ok"}` (re-read `/overview` for the new state), or `400` for a
bad body/strategy. The change is written to the config file as
`routing.pool.strategy` / `routing.pool.allow-paid-fallback` and applies
immediately.

- `existing` (default): order by configured priority only (unchanged behavior).
- `subscriptions_first`: every subscription is tried before any API key;
  priority orders accounts inside each group. Newly added subscriptions
  inherit this automatically.
- `allowPaidFallback: false`: API-key accounts are never selected, for any
  provider. Leave it `true` if some provider is only reachable with an API key.

## POST /v8/management/pool/accounts/{id}/reset

Clears the out-of-credits block and the account's cooldowns (manual override
after a top-up). → `200 {"status":"ok","account":{…account object…}}`, or
`404 {"error":"account_not_found"}`.

## Behavior notes

- **Detection.** Out-of-credits is recognised from Anthropic's
  `credit balance is too low` 400 and from explicit billing codes
  (`insufficient_quota`, `billing_hard_limit_reached`, `insufficient_balance`,
  `credit_balance_too_low`, `insufficient_credits`).
- **Failover.** The failing request moves to another account for the **same
  model** before any output reaches the client (including an error arriving as
  the first stream event). A stream that already sent output is never replayed.
- **Account-wide block.** Once out of credits, the account is skipped for every
  model; session affinity cannot keep a session on it.
- **Controlled recheck.** After 1 h the next request may probe the account; a
  renewed credit error fails over invisibly and doubles the wait (1 h, 2 h,
  4 h … max 24 h). A success clears the block and its per-model credit
  cooldowns. Manual reset is the endpoint above.
- **All blocked.** When every account for the model is out of credits, clients
  get HTTP 402 with code `insufficient_credits` and the next recheck time.
- **Persistence.** Health and statistics are stored in the configured KV store
  (`server.kv-store`, on by default; Home KV in Home mode). With the KV store
  disabled, `persistence` is `"memory"` and both reset on restart. Statistics
  keep 30 days of hourly buckets.
- **Home mode.** Health is recorded, but the Home dispatcher, not this policy,
  chooses accounts.
