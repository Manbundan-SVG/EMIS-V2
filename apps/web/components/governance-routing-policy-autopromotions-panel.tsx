"use client";

import { useState } from "react";
import type {
  GovernanceRoutingPolicyAutopromotionPolicyRow,
  GovernanceRoutingPolicyAutopromotionSummaryRow,
  GovernanceRoutingPolicyAutopromotionEligibilityRow,
  GovernanceRoutingPolicyAutopromotionRollbackCandidateRow,
} from "@/lib/queries/metrics";

type Props = {
  workspace: string;
  policies: GovernanceRoutingPolicyAutopromotionPolicyRow[];
  summary: GovernanceRoutingPolicyAutopromotionSummaryRow[];
  eligibility: GovernanceRoutingPolicyAutopromotionEligibilityRow[];
  rollbackCandidates: GovernanceRoutingPolicyAutopromotionRollbackCandidateRow[];
  loading: boolean;
  onChanged?: () => void | Promise<void>;
};

const OUTCOME_BADGE: Record<string, string> = {
  promoted: "badge-green",
  blocked: "badge-red",
  skipped: "badge-yellow",
};

const CONFIDENCE_BADGE: Record<string, string> = {
  high: "badge-green",
  medium: "badge-yellow",
  low: "badge-muted",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

export function GovernanceRoutingPolicyAutopromotionsPanel({
  workspace,
  policies,
  summary,
  eligibility,
  rollbackCandidates,
  loading,
  onChanged,
}: Props) {
  const [createBusy, setCreateBusy] = useState(false);
  const [formScope, setFormScope] = useState({ scopeType: "team", scopeValue: "", minConfidence: "high", cooldownHours: "168", promotionTarget: "rule" });

  async function createPolicy() {
    if (!formScope.scopeValue.trim()) return;
    setCreateBusy(true);
    try {
      const res = await fetch("/api/governance/routing-policy-autopromotions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workspace,
          action: "create_policy",
          scopeType: formScope.scopeType,
          scopeValue: formScope.scopeValue.trim(),
          minConfidence: formScope.minConfidence,
          cooldownHours: parseInt(formScope.cooldownHours, 10),
          promotionTarget: formScope.promotionTarget,
          createdBy: "ops-reviewer",
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => null) as { error?: string } | null;
        throw new Error(data?.error ?? "create policy request failed");
      }
      setFormScope((f) => ({ ...f, scopeValue: "" }));
      await onChanged?.();
    } finally {
      setCreateBusy(false);
    }
  }

  async function disablePolicy(scopeType: string, scopeValue: string) {
    try {
      const res = await fetch("/api/governance/routing-policy-autopromotions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ workspace, action: "disable_policy", scopeType, scopeValue }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => null) as { error?: string } | null;
        throw new Error(data?.error ?? "disable request failed");
      }
      await onChanged?.();
    } catch (err) {
      console.error(err);
    }
  }

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Routing Policy Autopromotion</h2>
          <p className="panel-subtitle">
            Policy-gated, cooldown-aware autopromotion of advisory routing recommendations into
            live routing rules and overrides. Requires a prior approved review and at least one
            successful manual application before autopromotion is permitted.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading autopromotion state…</p>}

      {!loading && (
        <>
          {/* ── 1. Eligibility / Candidate Queue ─────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Autopromotion Eligibility</h3>
                <p className="panel-subtitle">
                  Recommendations covered by an active autopromotion policy, with guardrail status.
                </p>
              </div>
            </div>
            {eligibility.length === 0 ? (
              <p className="muted">No eligible recommendations. Add an autopromotion policy below.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Scope</th>
                    <th>Confidence</th>
                    <th>Reviews</th>
                    <th>Applications</th>
                    <th>Last promoted</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {eligibility.map((row) => (
                    <tr key={row.recommendation_key}>
                      <td className="mono-cell text-sm">{row.recommendation_key.slice(0, 12)}</td>
                      <td>
                        <span className="badge-muted">{row.scope_type}</span>
                        {" "}<span className="text-sm">{row.scope_value}</span>
                      </td>
                      <td>
                        <span className={CONFIDENCE_BADGE[row.confidence] ?? "badge-muted"}>
                          {row.confidence}
                        </span>
                      </td>
                      <td>{row.approved_review_count}</td>
                      <td>{row.application_count}</td>
                      <td className="text-sm">{fmtTs(row.last_promoted_at)}</td>
                      <td>
                        {row.is_eligible ? (
                          <span className="badge-green">eligible</span>
                        ) : (
                          <span className="badge-red">{row.blocked_reason ?? "blocked"}</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── 2. Autopromotion Policies ─────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Autopromotion Policies</h3>
                <p className="panel-subtitle">
                  Per-scope policies defining guardrail thresholds and cooldown windows.
                </p>
              </div>
            </div>
            {/* create form */}
            <div className="btn-group" style={{ marginBottom: "0.75rem" }}>
              <select
                value={formScope.scopeType}
                onChange={(e) => setFormScope((f) => ({ ...f, scopeType: e.target.value }))}
              >
                {["team", "operator", "root_cause", "regime", "severity", "chronicity"].map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
              <input
                type="text"
                placeholder="scope value (e.g. platform)"
                value={formScope.scopeValue}
                onChange={(e) => setFormScope((f) => ({ ...f, scopeValue: e.target.value }))}
              />
              <select
                value={formScope.minConfidence}
                onChange={(e) => setFormScope((f) => ({ ...f, minConfidence: e.target.value }))}
              >
                {["high", "medium", "low"].map((c) => (
                  <option key={c} value={c}>min: {c}</option>
                ))}
              </select>
              <select
                value={formScope.promotionTarget}
                onChange={(e) => setFormScope((f) => ({ ...f, promotionTarget: e.target.value }))}
              >
                <option value="rule">rule</option>
                <option value="override">override</option>
              </select>
              <input
                type="number"
                placeholder="cooldown hrs"
                value={formScope.cooldownHours}
                onChange={(e) => setFormScope((f) => ({ ...f, cooldownHours: e.target.value }))}
                style={{ width: "6rem" }}
              />
              <button
                type="button"
                disabled={createBusy || !formScope.scopeValue.trim()}
                onClick={() => void createPolicy()}
              >
                Add Policy
              </button>
            </div>

            {policies.length === 0 ? (
              <p className="muted">No autopromotion policies configured.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Scope</th>
                    <th>Target</th>
                    <th>Min confidence</th>
                    <th>Min reviews</th>
                    <th>Min applications</th>
                    <th>Min samples</th>
                    <th>Cooldown (hrs)</th>
                    <th>Status</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {policies.map((p) => (
                    <tr key={p.id}>
                      <td>
                        <span className="badge-muted">{p.scope_type}</span>
                        {" "}<span className="text-sm">{p.scope_value}</span>
                      </td>
                      <td><span className="badge-muted">{p.promotion_target}</span></td>
                      <td>
                        <span className={CONFIDENCE_BADGE[p.min_confidence] ?? "badge-muted"}>
                          {p.min_confidence}
                        </span>
                      </td>
                      <td>{p.min_approved_review_count}</td>
                      <td>{p.min_application_count}</td>
                      <td>{p.min_sample_size}</td>
                      <td>{p.cooldown_hours}</td>
                      <td>
                        {p.enabled ? (
                          <span className="badge-green">enabled</span>
                        ) : (
                          <span className="badge-muted">disabled</span>
                        )}
                      </td>
                      <td>
                        {p.enabled && (
                          <button
                            type="button"
                            onClick={() => void disablePolicy(p.scope_type, p.scope_value)}
                          >
                            Disable
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── 3. Recent Autopromotion Executions ───────────────────────── */}
          {summary.length > 0 && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Recent Autopromotion Executions</h3>
                  <p className="panel-subtitle">Latest execution outcome per recommendation key.</p>
                </div>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Latest outcome</th>
                    <th>Reason</th>
                    <th>Executed at</th>
                    <th>Promoted</th>
                    <th>Blocked</th>
                    <th>Skipped</th>
                    <th>Open rollbacks</th>
                  </tr>
                </thead>
                <tbody>
                  {summary.map((s) => (
                    <tr key={s.recommendation_key}>
                      <td className="mono-cell text-sm">{s.recommendation_key.slice(0, 12)}</td>
                      <td>
                        {s.latest_outcome ? (
                          <span className={OUTCOME_BADGE[s.latest_outcome] ?? "badge-muted"}>
                            {s.latest_outcome}
                          </span>
                        ) : (
                          <span className="muted">—</span>
                        )}
                      </td>
                      <td className="text-sm muted">
                        {s.latest_blocked_reason ?? s.latest_skipped_reason ?? "—"}
                      </td>
                      <td className="text-sm">{fmtTs(s.latest_executed_at)}</td>
                      <td>{s.promoted_count}</td>
                      <td>{s.blocked_count}</td>
                      <td>{s.skipped_count}</td>
                      <td>
                        {s.open_rollback_count > 0 ? (
                          <span className="badge-yellow">{s.open_rollback_count}</span>
                        ) : (
                          <span className="muted">0</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* ── 4. Open Rollback Candidates ──────────────────────────────── */}
          {rollbackCandidates.length > 0 && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Open Rollback Candidates</h3>
                  <p className="panel-subtitle">
                    Every autopromotion generates a rollback candidate. These have not yet been resolved.
                  </p>
                </div>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Scope</th>
                    <th>Routing table</th>
                    <th>Applied policy</th>
                    <th>Prior policy</th>
                    <th>Created at</th>
                  </tr>
                </thead>
                <tbody>
                  {rollbackCandidates.map((rc) => (
                    <tr key={rc.id}>
                      <td className="mono-cell text-sm">{rc.recommendation_key.slice(0, 12)}</td>
                      <td>
                        <span className="badge-muted">{rc.scope_type}</span>
                        {" "}<span className="text-sm">{rc.scope_value}</span>
                      </td>
                      <td className="text-sm muted">{rc.routing_table ?? "—"}</td>
                      <td className="mono-cell text-xs">
                        {JSON.stringify(rc.applied_policy).slice(0, 40)}
                        {JSON.stringify(rc.applied_policy).length > 40 ? "…" : ""}
                      </td>
                      <td className="mono-cell text-xs">
                        {JSON.stringify(rc.prior_policy).slice(0, 40)}
                        {JSON.stringify(rc.prior_policy).length > 40 ? "…" : ""}
                      </td>
                      <td className="text-sm">{fmtTs(rc.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </section>
  );
}
