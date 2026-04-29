"use client";

import type {
  GovernanceRoutingContextFitRow,
  GovernanceRoutingFeatureEffectivenessRow,
  GovernanceRoutingOptimizationSnapshotRow,
  GovernanceRoutingPolicyOpportunityRow,
} from "@/lib/queries/metrics";

type Props = {
  snapshot: GovernanceRoutingOptimizationSnapshotRow | null;
  featureEffectiveness: GovernanceRoutingFeatureEffectivenessRow[];
  contextFit: GovernanceRoutingContextFitRow[];
  policyOpportunities: GovernanceRoutingPolicyOpportunityRow[];
  loading: boolean;
};

const REASON_LABELS: Record<string, string> = {
  prefer_operator: "Prefer operator",
  prefer_team: "Prefer team",
  avoid_operator_under_load: "Avoid overloaded operator",
  split_routing_by_root_cause: "Split by root cause",
  split_routing_by_regime: "Split by regime",
  tighten_chronicity_routing: "Tighten chronicity routing",
  prefer_team_for_reopen_cases: "Prefer team for reopens",
};

const CONFIDENCE_BADGE: Record<string, string> = {
  high: "badge-green",
  medium: "badge-yellow",
  low: "badge-muted",
};

function fmt2(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return v.toFixed(2);
}

function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

function fmtSec(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const h = v / 3600;
  return h >= 1 ? `${h.toFixed(1)}h` : `${Math.round(v / 60)}m`;
}

function fmtTs(ts: string): string {
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function NetFitBadge({ score }: { score: number }) {
  const cls =
    score >= 0.15 ? "badge-green"
    : score >= 0.0 ? "badge-yellow"
    : "badge-red";
  return <span className={cls}>{fmt2(score)}</span>;
}

function BenefitBar({ score }: { score: number }) {
  const pct = Math.round(Math.min(1, Math.max(0, score)) * 100);
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
      <div
        style={{
          width: pct,
          height: 8,
          background: pct > 60 ? "#22c55e" : pct > 30 ? "#eab308" : "#6b7280",
          borderRadius: 4,
          minWidth: 2,
        }}
      />
      <span className="muted" style={{ fontSize: "0.75rem" }}>{pct}%</span>
    </div>
  );
}

export function GovernanceRoutingOptimizationPanel({
  snapshot,
  featureEffectiveness,
  contextFit,
  policyOpportunities,
  loading,
}: Props) {
  const weakZones = featureEffectiveness
    .filter((r) => r.net_fit_score < 0 || r.reassignment_count + r.reopen_count > r.case_count * 0.3)
    .slice(0, 10);

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Routing Optimization</h2>
          <p className="panel-subtitle">
            Advisory routing policy opportunities derived from live outcome, effectiveness, and workload signals.
            All recommendations are read-only — no rules are modified in Phase 3.5A.
          </p>
        </div>
        {snapshot && (
          <div style={{ textAlign: "right", fontSize: "0.8rem" }} className="muted">
            <div>Last refresh: {fmtTs(snapshot.snapshot_at)}</div>
            <div>Window: {snapshot.window_label} · {snapshot.recommendation_count} recommendations</div>
          </div>
        )}
      </div>

      {loading && <p className="muted">Loading routing optimization data…</p>}

      {!loading && (
        <>
          {/* ── 1. Top Policy Opportunities ───────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Top Routing Opportunities</h3>
                <p className="panel-subtitle">
                  Ranked by expected benefit. Advisory only — review and act in 3.5B.
                </p>
              </div>
            </div>
            {policyOpportunities.length === 0 ? (
              <p className="muted">No routing opportunities detected yet. Run more jobs to accumulate outcome signal.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Reason</th>
                    <th>Scope</th>
                    <th>Recommended</th>
                    <th>Confidence</th>
                    <th>Benefit</th>
                    <th>Risk</th>
                    <th>n</th>
                  </tr>
                </thead>
                <tbody>
                  {policyOpportunities.map((row) => {
                    const rec = row.recommended_policy as Record<string, unknown>;
                    const recLabel =
                      (rec.preferred_operator as string | undefined) ??
                      (rec.preferred_team as string | undefined) ??
                      (rec.preferred_team_for_reopens as string | undefined) ??
                      (rec.routing_mode as string | undefined) ??
                      "—";
                    return (
                      <tr key={row.recommendation_key}>
                        <td>{REASON_LABELS[row.reason_code] ?? row.reason_code}</td>
                        <td>
                          <span className="badge-muted">{row.scope_type}</span>
                          {" "}
                          <span style={{ fontSize: "0.8rem" }}>{row.scope_value}</span>
                        </td>
                        <td style={{ fontFamily: "monospace", fontSize: "0.8rem" }}>{recLabel}</td>
                        <td>
                          <span className={CONFIDENCE_BADGE[row.confidence] ?? "badge-muted"}>
                            {row.confidence}
                          </span>
                        </td>
                        <td>
                          <BenefitBar score={row.expected_benefit_score} />
                        </td>
                        <td>{fmtPct(row.risk_score)}</td>
                        <td>{row.sample_size}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>

          {/* ── 2 & 3. Context Fit + Weak Zones ───────────────────────── */}
          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Best Context Fits</h3>
                  <p className="panel-subtitle">
                    Operators/teams with strongest outcome patterns per incident context.
                  </p>
                </div>
              </div>
              {contextFit.length === 0 ? (
                <p className="muted">No context fit data yet. Needs routing outcome history.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Context</th>
                      <th>Recommended</th>
                      <th>Fit score</th>
                      <th>Confidence</th>
                      <th>n</th>
                    </tr>
                  </thead>
                  <tbody>
                    {contextFit.slice(0, 12).map((row) => (
                      <tr key={`${row.workspace_id}-${row.context_key}`}>
                        <td style={{ fontFamily: "monospace", fontSize: "0.8rem" }}>{row.context_key}</td>
                        <td>{row.recommended_user ?? row.recommended_team ?? "—"}</td>
                        <td>
                          <NetFitBadge score={row.operator_fit_score ?? row.team_fit_score ?? 0} />
                        </td>
                        <td>
                          <span className={CONFIDENCE_BADGE[row.confidence] ?? "badge-muted"}>
                            {row.confidence}
                          </span>
                        </td>
                        <td>{row.sample_size}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Weak Routing Zones</h3>
                  <p className="panel-subtitle">
                    Feature dimensions with high override/reassignment/reopen burden.
                  </p>
                </div>
              </div>
              {weakZones.length === 0 ? (
                <p className="muted">No weak routing zones detected.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Dimension</th>
                      <th>Value</th>
                      <th>Net fit</th>
                      <th>Reassign</th>
                      <th>Reopen</th>
                      <th>Escalate</th>
                      <th>Cases</th>
                    </tr>
                  </thead>
                  <tbody>
                    {weakZones.map((row) => (
                      <tr key={`${row.feature_type}-${row.feature_key}`}>
                        <td>
                          <span className="badge-muted">{row.feature_type}</span>
                        </td>
                        <td style={{ fontFamily: "monospace", fontSize: "0.8rem" }}>{row.feature_key}</td>
                        <td>
                          <NetFitBadge score={row.net_fit_score} />
                        </td>
                        <td>{row.reassignment_count}</td>
                        <td>{row.reopen_count}</td>
                        <td>{row.escalation_count}</td>
                        <td>{row.case_count}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>

          {/* ── 4. Feature Effectiveness Full Table ───────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Routing Effectiveness by Feature</h3>
                <p className="panel-subtitle">
                  Effectiveness scores across all tracked routing dimensions.
                  Net fit = effectiveness − override burden. Lower = higher optimization opportunity.
                </p>
              </div>
            </div>
            {featureEffectiveness.length === 0 ? (
              <p className="muted">No feature effectiveness rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Dimension</th>
                    <th>Value</th>
                    <th>Cases</th>
                    <th>Accepted rec.</th>
                    <th>Overrides</th>
                    <th>Avg ack</th>
                    <th>Avg resolve</th>
                    <th>Net fit</th>
                  </tr>
                </thead>
                <tbody>
                  {featureEffectiveness.map((row) => (
                    <tr key={`${row.feature_type}-${row.feature_key}`}>
                      <td>
                        <span className="badge-muted">{row.feature_type}</span>
                      </td>
                      <td style={{ fontFamily: "monospace", fontSize: "0.8rem" }}>{row.feature_key}</td>
                      <td>{row.case_count}</td>
                      <td>{row.accepted_recommendation_count}</td>
                      <td>{row.override_count}</td>
                      <td>{fmtSec(row.avg_ack_latency_seconds)}</td>
                      <td>{fmtSec(row.avg_resolve_latency_seconds)}</td>
                      <td>
                        <NetFitBadge score={row.net_fit_score} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </>
      )}
    </section>
  );
}
