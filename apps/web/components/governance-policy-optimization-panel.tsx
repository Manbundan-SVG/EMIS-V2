"use client";

import type {
  GovernancePolicyOptimizationSnapshotRow,
  GovernancePolicyFeatureEffectivenessRow,
  GovernancePolicyContextFitRow,
  GovernancePolicyOpportunityRow,
} from "@/lib/queries/metrics";

type Props = {
  snapshot: GovernancePolicyOptimizationSnapshotRow | null;
  featureEffectiveness: GovernancePolicyFeatureEffectivenessRow[];
  contextFit: GovernancePolicyContextFitRow[];
  policyOpportunities: GovernancePolicyOpportunityRow[];
  loading: boolean;
};

const CONFIDENCE_BADGE: Record<string, string> = {
  high: "badge-green",
  medium: "badge-yellow",
  low: "badge-muted",
};

const FAMILY_BADGE: Record<string, string> = {
  threshold: "badge-blue",
  routing: "badge-purple",
  threshold_autopromotion: "badge-green",
  routing_autopromotion: "badge-green",
  rollback: "badge-red",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtScore(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return v.toFixed(3);
}

function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

function scoreClass(v: number | null | undefined, higherIsBetter = true): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "";
  if (higherIsBetter) {
    if (v >= 0.6) return "badge-green";
    if (v >= 0.3) return "badge-yellow";
    return "badge-red";
  }
  if (v <= 0.2) return "badge-green";
  if (v <= 0.4) return "badge-yellow";
  return "badge-red";
}

function familyBadge(family: string): string {
  return FAMILY_BADGE[family] ?? "badge-muted";
}

export function GovernancePolicyOptimizationPanel({
  snapshot,
  featureEffectiveness,
  contextFit,
  policyOpportunities,
  loading,
}: Props) {
  const weakZones = featureEffectiveness.filter(
    (r) => r.net_policy_fit_score < 0 || (r.recurrence_rate ?? 0) > 0.3 || (r.escalation_rate ?? 0) > 0.25
  );

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Governance Policy Optimization</h2>
          <p className="panel-subtitle">
            Advisory-only. Identifies which policy patterns help or hurt outcomes across threshold,
            routing, autopromotion, and rollback families. No live policies are changed here.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading governance policy optimization…</p>}

      {!loading && (
        <>
          {/* ── Snapshot metadata ──────────────────────────────────────────── */}
          {snapshot && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Optimization Snapshot</h3>
                  <p className="panel-subtitle">Last computed advisory pass.</p>
                </div>
              </div>
              <div className="flex gap-4 text-sm">
                <span className="muted">Last refresh:</span>
                <span>{fmtTs(snapshot.snapshot_at)}</span>
                <span className="muted">Window:</span>
                <span className="badge-muted">{snapshot.window_label}</span>
                <span className="muted">Recommendations:</span>
                <span className="badge-yellow">{snapshot.recommendation_count}</span>
              </div>
            </div>
          )}

          {/* ── 1. Top policy opportunities ───────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Top Governance Policy Opportunities</h3>
                <p className="panel-subtitle">
                  Ranked advisory recommendations. Higher expected benefit and lower risk = act sooner.
                </p>
              </div>
            </div>
            {policyOpportunities.length === 0 ? (
              <p className="muted">No policy opportunities computed yet. Run the analytics worker to generate.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Family</th>
                    <th>Scope</th>
                    <th>Reason</th>
                    <th>Confidence</th>
                    <th>Expected benefit</th>
                    <th>Risk</th>
                    <th>Sample</th>
                    <th>Recommended action</th>
                  </tr>
                </thead>
                <tbody>
                  {policyOpportunities.map((opp) => (
                    <tr key={opp.recommendation_key}>
                      <td className="mono-cell text-sm">{opp.recommendation_key.slice(0, 28)}</td>
                      <td>
                        <span className={familyBadge(opp.policy_family)}>{opp.policy_family}</span>
                      </td>
                      <td>
                        <span className="badge-muted">{opp.scope_type}</span>
                        {" "}<span className="text-sm">{opp.scope_value}</span>
                      </td>
                      <td className="text-sm">{opp.reason_code.replace(/_/g, " ")}</td>
                      <td>
                        <span className={CONFIDENCE_BADGE[opp.confidence] ?? "badge-muted"}>
                          {opp.confidence}
                        </span>
                      </td>
                      <td>
                        <span className={scoreClass(opp.expected_benefit_score)}>
                          {fmtScore(opp.expected_benefit_score)}
                        </span>
                      </td>
                      <td>
                        <span className={scoreClass(opp.risk_score, false)}>
                          {fmtScore(opp.risk_score)}
                        </span>
                      </td>
                      <td className="text-sm">{opp.sample_size}</td>
                      <td className="text-sm muted">
                        {typeof opp.recommended_policy?.action === "string"
                          ? opp.recommended_policy.action
                          : JSON.stringify(opp.recommended_policy).slice(0, 60)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── 2. Best context fits ───────────────────────────────────────── */}
          {contextFit.length > 0 && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Best Context Fits</h3>
                  <p className="panel-subtitle">
                    Which governance policy family fits each incident context best.
                  </p>
                </div>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Context</th>
                    <th>Best family</th>
                    <th>Variant</th>
                    <th>Fit score</th>
                    <th>Sample</th>
                    <th>Confidence</th>
                  </tr>
                </thead>
                <tbody>
                  {contextFit.map((row) => (
                    <tr key={`${row.workspace_id}:${row.context_key}`}>
                      <td className="mono-cell text-sm">{row.context_key}</td>
                      <td>
                        <span className={familyBadge(row.best_policy_family)}>{row.best_policy_family}</span>
                      </td>
                      <td className="text-sm muted">{row.best_policy_variant}</td>
                      <td>
                        <span className={scoreClass(row.fit_score)}>{fmtScore(row.fit_score)}</span>
                      </td>
                      <td className="text-sm">{row.sample_size}</td>
                      <td>
                        <span className={CONFIDENCE_BADGE[row.confidence] ?? "badge-muted"}>
                          {row.confidence}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* ── 3. Weak governance zones ──────────────────────────────────── */}
          {weakZones.length > 0 && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Weak Governance Zones</h3>
                  <p className="panel-subtitle">
                    Policy families and contexts with negative net fit, high recurrence, or high escalation burden.
                  </p>
                </div>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Family</th>
                    <th>Feature</th>
                    <th>Key</th>
                    <th>Net fit</th>
                    <th>Recurrence</th>
                    <th>Escalation</th>
                    <th>Reassignment</th>
                    <th>Rollback rate</th>
                    <th>Sample</th>
                  </tr>
                </thead>
                <tbody>
                  {weakZones.map((row) => (
                    <tr key={`${row.policy_family}:${row.feature_type}:${row.feature_key}`}>
                      <td>
                        <span className={familyBadge(row.policy_family)}>{row.policy_family}</span>
                      </td>
                      <td className="text-sm muted">{row.feature_type}</td>
                      <td className="text-sm">{row.feature_key}</td>
                      <td>
                        <span className={scoreClass(row.net_policy_fit_score)}>
                          {fmtScore(row.net_policy_fit_score)}
                        </span>
                      </td>
                      <td>{fmtPct(row.recurrence_rate)}</td>
                      <td>{fmtPct(row.escalation_rate)}</td>
                      <td>{fmtPct(row.reassignment_rate)}</td>
                      <td>{fmtPct(row.rollback_rate)}</td>
                      <td className="text-sm">{row.sample_size}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* ── 4. Feature effectiveness table ────────────────────────────── */}
          {featureEffectiveness.length > 0 && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Policy Feature Effectiveness</h3>
                  <p className="panel-subtitle">
                    Scoring per policy family and feature dimension. Net fit = effectiveness − risk.
                  </p>
                </div>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Family</th>
                    <th>Feature</th>
                    <th>Key</th>
                    <th>Effectiveness</th>
                    <th>Risk</th>
                    <th>Net fit</th>
                    <th>Sample</th>
                    <th>Recurrence</th>
                    <th>Escalation</th>
                  </tr>
                </thead>
                <tbody>
                  {featureEffectiveness.map((row) => (
                    <tr key={`${row.policy_family}:${row.feature_type}:${row.feature_key}`}>
                      <td>
                        <span className={familyBadge(row.policy_family)}>{row.policy_family}</span>
                      </td>
                      <td className="text-sm muted">{row.feature_type}</td>
                      <td className="text-sm">{row.feature_key}</td>
                      <td>
                        <span className={scoreClass(row.effectiveness_score)}>
                          {fmtScore(row.effectiveness_score)}
                        </span>
                      </td>
                      <td>
                        <span className={scoreClass(row.risk_score, false)}>
                          {fmtScore(row.risk_score)}
                        </span>
                      </td>
                      <td>
                        <span className={scoreClass(row.net_policy_fit_score)}>
                          {fmtScore(row.net_policy_fit_score)}
                        </span>
                      </td>
                      <td className="text-sm">{row.sample_size}</td>
                      <td>{fmtPct(row.recurrence_rate)}</td>
                      <td>{fmtPct(row.escalation_rate)}</td>
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
