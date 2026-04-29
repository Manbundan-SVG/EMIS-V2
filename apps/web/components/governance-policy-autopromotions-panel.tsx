"use client";

import { useState } from "react";
import type {
  GovernancePolicyAutopromotionSummaryRow,
  GovernancePolicyAutopromotionEligibilityRow,
  GovernancePolicyAutopromotionRollbackCandidateRow,
} from "@/lib/queries/metrics";

type Props = {
  workspace: string;
  autopromotionSummary: GovernancePolicyAutopromotionSummaryRow[];
  eligibility: GovernancePolicyAutopromotionEligibilityRow[];
  rollbackCandidates: GovernancePolicyAutopromotionRollbackCandidateRow[];
  loading: boolean;
  onChanged: () => void;
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

export function GovernancePolicyAutopromotionsPanel({
  workspace,
  autopromotionSummary,
  eligibility,
  rollbackCandidates,
  loading,
  onChanged,
}: Props) {
  const [pfamily, setPfamily] = useState("threshold");
  const [pscope, setPscope] = useState("team");
  const [pscopeVal, setPscopeVal] = useState("");
  const [ptarget, setPtarget] = useState<"routing_rule" | "routing_override" | "threshold_profile" | "autopromotion_policy">("routing_rule");
  const [pconfidence, setPconfidence] = useState<"low" | "medium" | "high">("high");
  const [pcooldown, setPcooldown] = useState("72");
  const [pby, setPby] = useState("");
  const [psubmitting, setPsubmitting] = useState(false);
  const [perror, setPerror] = useState<string | null>(null);

  async function addPolicy(e: React.FormEvent) {
    e.preventDefault();
    if (!pscopeVal || !pby) return;
    setPsubmitting(true);
    setPerror(null);
    try {
      const res = await fetch("/api/governance/policy-autopromotions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          workspace,
          action: "upsert_policy",
          policyFamily: pfamily,
          scopeType: pscope,
          scopeValue: pscopeVal,
          promotionTarget: ptarget,
          minConfidence: pconfidence,
          cooldownHours: Number(pcooldown),
          createdBy: pby,
        }),
      });
      const data = await res.json() as { ok: boolean; error?: string };
      if (!data.ok) throw new Error(data.error ?? "unknown error");
      setPscopeVal("");
      setPby("");
      onChanged();
    } catch (err) {
      setPerror(err instanceof Error ? err.message : "error");
    } finally {
      setPsubmitting(false);
    }
  }

  async function disablePolicy(policyId: string) {
    try {
      await fetch("/api/governance/policy-autopromotions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ workspace, action: "disable_policy", policyId }),
      });
      onChanged();
    } catch {
      // silently reload; error visible on next fetch
    }
  }

  const eligible = eligibility.filter((r) => r.eligible);
  const blocked = eligibility.filter((r) => !r.eligible);

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Governance Policy Autopromotions</h2>
          <p className="panel-subtitle">
            Policy-gated autopromotion. Requires approved review + successful manual application.
            Every autopromotion creates a rollback candidate.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading governance policy autopromotion state…</p>}

      {!loading && (
        <>
          {/* ── Eligibility ───────────────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Autopromotion Eligibility</h3>
                <p className="panel-subtitle">
                  Recommendations that meet all policy guardrails are eligible for autopromotion.
                </p>
              </div>
            </div>
            {eligibility.length === 0 ? (
              <p className="muted">No recommendations matched by any autopromotion policy yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Family</th>
                    <th>Scope</th>
                    <th>Target</th>
                    <th>Eligible</th>
                    <th>Blocked reason</th>
                    <th>Confidence</th>
                    <th>Reviews</th>
                    <th>Applications</th>
                    <th>Sample</th>
                    <th>Cooldown ends</th>
                  </tr>
                </thead>
                <tbody>
                  {eligibility.map((row) => (
                    <tr key={`${row.workspace_id}:${row.recommendation_key}:${row.policy_id}`}>
                      <td className="mono-cell text-sm">{row.recommendation_key.slice(0, 24)}</td>
                      <td><span className="badge-muted">{row.policy_family}</span></td>
                      <td>
                        <span className="badge-muted">{row.scope_type}</span>
                        {" "}<span className="text-sm">{row.scope_value}</span>
                      </td>
                      <td className="text-sm">{row.promotion_target}</td>
                      <td>
                        {row.eligible
                          ? <span className="badge-green">eligible</span>
                          : <span className="badge-red">blocked</span>}
                      </td>
                      <td className="text-sm muted">{row.blocked_reason_code?.replace(/_/g, " ") ?? "—"}</td>
                      <td>
                        <span className={CONFIDENCE_BADGE[row.confidence ?? ""] ?? "badge-muted"}>
                          {row.confidence ?? "—"}
                        </span>
                      </td>
                      <td className="text-sm">{row.approved_review_count}</td>
                      <td className="text-sm">{row.application_count}</td>
                      <td className="text-sm">{row.sample_size}</td>
                      <td className="text-sm muted">{fmtTs(row.cooldown_ends_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Policy summary ────────────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Autopromotion Policies</h3>
                <p className="panel-subtitle">Configure policy-gated autopromotion guardrails.</p>
              </div>
            </div>
            <form onSubmit={(e) => void addPolicy(e)} className="form-row flex gap-2 flex-wrap mb-3">
              <select className="input" value={pfamily} onChange={(e) => setPfamily(e.target.value)}>
                <option value="threshold">threshold</option>
                <option value="routing">routing</option>
                <option value="routing_autopromotion">routing_autopromotion</option>
              </select>
              <select className="input" value={pscope} onChange={(e) => setPscope(e.target.value)}>
                <option value="team">team</option>
                <option value="operator">operator</option>
                <option value="regime">regime</option>
                <option value="root_cause">root_cause</option>
                <option value="severity">severity</option>
              </select>
              <input
                className="input"
                placeholder="Scope value"
                value={pscopeVal}
                onChange={(e) => setPscopeVal(e.target.value)}
                required
              />
              <select className="input" value={ptarget} onChange={(e) => setPtarget(e.target.value as typeof ptarget)}>
                <option value="routing_rule">routing_rule</option>
                <option value="routing_override">routing_override</option>
                <option value="threshold_profile">threshold_profile</option>
                <option value="autopromotion_policy">autopromotion_policy</option>
              </select>
              <select className="input" value={pconfidence} onChange={(e) => setPconfidence(e.target.value as "low" | "medium" | "high")}>
                <option value="high">high</option>
                <option value="medium">medium</option>
                <option value="low">low</option>
              </select>
              <input
                className="input"
                placeholder="Cooldown hours"
                value={pcooldown}
                onChange={(e) => setPcooldown(e.target.value)}
                style={{ width: 120 }}
              />
              <input
                className="input"
                placeholder="Created by"
                value={pby}
                onChange={(e) => setPby(e.target.value)}
                required
              />
              <button type="submit" className="btn btn-sm" disabled={psubmitting}>
                {psubmitting ? "Saving…" : "Add Policy"}
              </button>
            </form>
            {perror && <p className="badge-red text-sm mb-2">{perror}</p>}

            {autopromotionSummary.length === 0 ? (
              <p className="muted">No autopromotion policies configured yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Family</th>
                    <th>Scope</th>
                    <th>Target</th>
                    <th>Min confidence</th>
                    <th>Min reviews</th>
                    <th>Min applications</th>
                    <th>Cooldown</th>
                    <th>Enabled</th>
                    <th>Executions</th>
                    <th>Rollback candidates</th>
                    <th>Latest execution</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {autopromotionSummary.map((row) => (
                    <tr key={row.policy_id}>
                      <td><span className="badge-muted">{row.policy_family}</span></td>
                      <td>
                        <span className="badge-muted">{row.scope_type}</span>
                        {" "}<span className="text-sm">{row.scope_value}</span>
                      </td>
                      <td className="text-sm">{row.promotion_target}</td>
                      <td>
                        <span className={CONFIDENCE_BADGE[row.min_confidence] ?? "badge-muted"}>
                          {row.min_confidence}
                        </span>
                      </td>
                      <td className="text-sm">{row.min_approved_review_count}</td>
                      <td className="text-sm">{row.min_application_count}</td>
                      <td className="text-sm">{row.cooldown_hours}h</td>
                      <td>
                        {row.enabled
                          ? <span className="badge-green">yes</span>
                          : <span className="badge-muted">no</span>}
                      </td>
                      <td className="text-sm">{row.execution_count}</td>
                      <td className="text-sm">{row.rollback_candidate_count}</td>
                      <td className="text-sm">{fmtTs(row.latest_execution_at)}</td>
                      <td>
                        {row.enabled && (
                          <button
                            type="button"
                            className="btn btn-sm"
                            onClick={() => void disablePolicy(row.policy_id)}
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

          {/* ── Open rollback candidates ───────────────────────────────────── */}
          {rollbackCandidates.length > 0 && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Open Rollback Candidates</h3>
                  <p className="panel-subtitle">
                    Autopromotions with rollback candidates not yet resolved.
                  </p>
                </div>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Family</th>
                    <th>Scope</th>
                    <th>Target</th>
                    <th>Risk score</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {rollbackCandidates.map((row) => (
                    <tr key={row.id}>
                      <td className="mono-cell text-sm">{row.recommendation_key.slice(0, 24)}</td>
                      <td><span className="badge-muted">{row.policy_family}</span></td>
                      <td>
                        <span className="badge-muted">{row.scope_type}</span>
                        {" "}<span className="text-sm">{row.scope_value}</span>
                      </td>
                      <td className="text-sm">{row.target_type}</td>
                      <td className="text-sm">{Number(row.rollback_risk_score).toFixed(3)}</td>
                      <td className="text-sm">{fmtTs(row.created_at)}</td>
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
