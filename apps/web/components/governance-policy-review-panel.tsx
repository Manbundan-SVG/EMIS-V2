"use client";

import { useState } from "react";
import type {
  GovernancePolicyReviewSummaryRow,
  GovernancePolicyPromotionSummaryRow,
  GovernancePolicyPendingPromotionRow,
  GovernancePolicyOpportunityRow,
} from "@/lib/queries/metrics";

type Props = {
  workspace: string;
  reviewSummary: GovernancePolicyReviewSummaryRow[];
  promotionSummary: GovernancePolicyPromotionSummaryRow[];
  pendingPromotions: GovernancePolicyPendingPromotionRow[];
  opportunities: GovernancePolicyOpportunityRow[];
  loading: boolean;
  onChanged: () => void;
};

const STATUS_BADGE: Record<string, string> = {
  approved: "badge-green",
  rejected: "badge-red",
  deferred: "badge-yellow",
  pending: "badge-yellow",
  applied: "badge-green",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

export function GovernancePolicyReviewPanel({
  workspace,
  reviewSummary,
  promotionSummary,
  pendingPromotions,
  opportunities,
  loading,
  onChanged,
}: Props) {
  const [reviewKey, setReviewKey] = useState("");
  const [reviewFamily, setReviewFamily] = useState("threshold");
  const [reviewStatus, setReviewStatus] = useState<"approved" | "rejected" | "deferred">("approved");
  const [reviewedBy, setReviewedBy] = useState("");
  const [reviewReason, setReviewReason] = useState("");
  const [reviewNotes, setReviewNotes] = useState("");
  const [reviewSubmitting, setReviewSubmitting] = useState(false);
  const [reviewError, setReviewError] = useState<string | null>(null);

  const [propKey, setPropKey] = useState("");
  const [propFamily, setPropFamily] = useState("threshold");
  const [propTarget, setPropTarget] = useState<"threshold_profile" | "routing_rule" | "routing_override" | "autopromotion_policy">("routing_rule");
  const [propScopeType, setPropScopeType] = useState("team");
  const [propScopeValue, setPropScopeValue] = useState("");
  const [propBy, setPropBy] = useState("");
  const [propReason, setPropReason] = useState("");
  const [propSubmitting, setPropSubmitting] = useState(false);
  const [propError, setPropError] = useState<string | null>(null);

  const [actionError, setActionError] = useState<string | null>(null);

  async function submitReview(e: React.FormEvent) {
    e.preventDefault();
    if (!reviewKey || !reviewedBy) return;
    setReviewSubmitting(true);
    setReviewError(null);
    try {
      const res = await fetch("/api/governance/policy-reviews", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          workspace,
          recommendationKey: reviewKey,
          policyFamily: reviewFamily,
          reviewStatus,
          reviewedBy,
          reviewReason: reviewReason || null,
          notes: reviewNotes || null,
        }),
      });
      const data = await res.json() as { ok: boolean; error?: string };
      if (!data.ok) throw new Error(data.error ?? "unknown error");
      setReviewKey("");
      setReviewedBy("");
      setReviewReason("");
      setReviewNotes("");
      onChanged();
    } catch (err) {
      setReviewError(err instanceof Error ? err.message : "error");
    } finally {
      setReviewSubmitting(false);
    }
  }

  async function submitProposal(e: React.FormEvent) {
    e.preventDefault();
    if (!propKey || !propBy || !propScopeValue) return;
    setPropSubmitting(true);
    setPropError(null);
    try {
      const res = await fetch("/api/governance/policy-promotions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          workspace,
          action: "propose",
          recommendationKey: propKey,
          policyFamily: propFamily,
          promotionTarget: propTarget,
          scopeType: propScopeType,
          scopeValue: propScopeValue,
          currentPolicy: {},
          recommendedPolicy: {},
          proposedBy: propBy,
          proposalReason: propReason || null,
        }),
      });
      const data = await res.json() as { ok: boolean; error?: string };
      if (!data.ok) throw new Error(data.error ?? "unknown error");
      setPropKey("");
      setPropScopeValue("");
      setPropBy("");
      setPropReason("");
      onChanged();
    } catch (err) {
      setPropError(err instanceof Error ? err.message : "error");
    } finally {
      setPropSubmitting(false);
    }
  }

  async function handleApprove(proposalId: string, approvedBy: string) {
    setActionError(null);
    try {
      const res = await fetch("/api/governance/policy-promotions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ workspace, action: "approve", proposalId, approvedBy }),
      });
      const data = await res.json() as { ok: boolean; error?: string };
      if (!data.ok) throw new Error(data.error ?? "unknown error");
      onChanged();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "error");
    }
  }

  async function handleApply(proposalId: string, appliedBy: string) {
    setActionError(null);
    try {
      const res = await fetch("/api/governance/policy-promotions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ workspace, action: "apply", proposalId, appliedBy }),
      });
      const data = await res.json() as { ok: boolean; error?: string };
      if (!data.ok) throw new Error(data.error ?? "unknown error");
      onChanged();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "error");
    }
  }

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Governance Policy Review &amp; Promotion</h2>
          <p className="panel-subtitle">
            Review optimization recommendations, propose and approve policy promotions.
            All applications are operator-controlled, approval-gated, and audited.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading governance policy review state…</p>}

      {!loading && (
        <>
          {/* ── Submit review ──────────────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Submit Review</h3>
                <p className="panel-subtitle">Approve, reject, or defer a governance policy recommendation.</p>
              </div>
            </div>
            <form onSubmit={(e) => void submitReview(e)} className="form-row flex gap-2 flex-wrap">
              <input
                className="input"
                placeholder="Recommendation key"
                value={reviewKey}
                onChange={(e) => setReviewKey(e.target.value)}
                required
              />
              <select
                className="input"
                value={reviewFamily}
                onChange={(e) => setReviewFamily(e.target.value)}
              >
                <option value="threshold">threshold</option>
                <option value="routing">routing</option>
                <option value="threshold_autopromotion">threshold_autopromotion</option>
                <option value="routing_autopromotion">routing_autopromotion</option>
                <option value="rollback">rollback</option>
              </select>
              <select
                className="input"
                value={reviewStatus}
                onChange={(e) => setReviewStatus(e.target.value as "approved" | "rejected" | "deferred")}
              >
                <option value="approved">approved</option>
                <option value="rejected">rejected</option>
                <option value="deferred">deferred</option>
              </select>
              <input
                className="input"
                placeholder="Reviewed by"
                value={reviewedBy}
                onChange={(e) => setReviewedBy(e.target.value)}
                required
              />
              <input
                className="input"
                placeholder="Reason (optional)"
                value={reviewReason}
                onChange={(e) => setReviewReason(e.target.value)}
              />
              <input
                className="input"
                placeholder="Notes (optional)"
                value={reviewNotes}
                onChange={(e) => setReviewNotes(e.target.value)}
              />
              <button type="submit" className="btn btn-sm" disabled={reviewSubmitting}>
                {reviewSubmitting ? "Submitting…" : "Submit Review"}
              </button>
            </form>
            {reviewError && <p className="badge-red text-sm mt-2">{reviewError}</p>}
          </div>

          {/* ── Latest reviews ─────────────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Review History</h3>
                <p className="panel-subtitle">Latest review per recommendation key.</p>
              </div>
            </div>
            {reviewSummary.length === 0 ? (
              <p className="muted">No reviews submitted yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Family</th>
                    <th>Status</th>
                    <th>Reviewed by</th>
                    <th>Reason</th>
                    <th>Reviews total</th>
                    <th>Reviewed at</th>
                  </tr>
                </thead>
                <tbody>
                  {reviewSummary.map((row) => (
                    <tr key={`${row.workspace_id}:${row.recommendation_key}`}>
                      <td className="mono-cell text-sm">{row.recommendation_key.slice(0, 30)}</td>
                      <td><span className="badge-muted">{row.policy_family}</span></td>
                      <td>
                        <span className={STATUS_BADGE[row.latest_review_status ?? ""] ?? "badge-muted"}>
                          {row.latest_review_status ?? "—"}
                        </span>
                      </td>
                      <td className="text-sm">{row.latest_reviewed_by ?? "—"}</td>
                      <td className="text-sm muted">{row.latest_review_reason ?? "—"}</td>
                      <td className="text-sm">{row.review_count}</td>
                      <td className="text-sm">{fmtTs(row.latest_reviewed_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Pending promotions ─────────────────────────────────────────── */}
          {pendingPromotions.length > 0 && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Pending Promotions</h3>
                  <p className="panel-subtitle">
                    Proposals awaiting approval or application. Execute is approval-gated.
                  </p>
                </div>
              </div>
              {actionError && <p className="badge-red text-sm mb-2">{actionError}</p>}
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Family</th>
                    <th>Target</th>
                    <th>Scope</th>
                    <th>Status</th>
                    <th>Proposed by</th>
                    <th>Applications</th>
                    <th>Proposed at</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {pendingPromotions.map((row) => (
                    <tr key={`${row.workspace_id}:${row.recommendation_key}`}>
                      <td className="mono-cell text-sm">{row.recommendation_key.slice(0, 24)}</td>
                      <td><span className="badge-muted">{row.policy_family}</span></td>
                      <td className="text-sm">{row.latest_promotion_target ?? "—"}</td>
                      <td>
                        <span className="badge-muted">{row.latest_scope_type}</span>
                        {" "}<span className="text-sm">{row.latest_scope_value}</span>
                      </td>
                      <td>
                        <span className={STATUS_BADGE[row.latest_proposal_status ?? ""] ?? "badge-muted"}>
                          {row.latest_proposal_status ?? "—"}
                        </span>
                      </td>
                      <td className="text-sm">{row.latest_proposed_by ?? "—"}</td>
                      <td className="text-sm">{row.application_count}</td>
                      <td className="text-sm">{fmtTs(row.latest_proposed_at)}</td>
                      <td>
                        <div className="flex gap-1">
                          {row.latest_proposal_status === "pending" && (
                            <button
                              type="button"
                              className="btn btn-sm"
                              onClick={() => void handleApprove(row.recommendation_key, "ops")}
                            >
                              Approve
                            </button>
                          )}
                          {row.latest_proposal_status === "approved" && (
                            <button
                              type="button"
                              className="btn btn-sm"
                              onClick={() => void handleApply(row.recommendation_key, "ops")}
                            >
                              Apply
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* ── Create proposal ───────────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Create Promotion Proposal</h3>
                <p className="panel-subtitle">
                  Propose a governance policy change from an approved recommendation.
                </p>
              </div>
            </div>
            <form onSubmit={(e) => void submitProposal(e)} className="form-row flex gap-2 flex-wrap">
              <input
                className="input"
                placeholder="Recommendation key"
                value={propKey}
                onChange={(e) => setPropKey(e.target.value)}
                required
              />
              <select className="input" value={propFamily} onChange={(e) => setPropFamily(e.target.value)}>
                <option value="threshold">threshold</option>
                <option value="routing">routing</option>
                <option value="threshold_autopromotion">threshold_autopromotion</option>
                <option value="routing_autopromotion">routing_autopromotion</option>
                <option value="rollback">rollback</option>
              </select>
              <select
                className="input"
                value={propTarget}
                onChange={(e) => setPropTarget(e.target.value as typeof propTarget)}
              >
                <option value="routing_rule">routing_rule</option>
                <option value="routing_override">routing_override</option>
                <option value="threshold_profile">threshold_profile</option>
                <option value="autopromotion_policy">autopromotion_policy</option>
              </select>
              <select className="input" value={propScopeType} onChange={(e) => setPropScopeType(e.target.value)}>
                <option value="team">team</option>
                <option value="operator">operator</option>
                <option value="regime">regime</option>
                <option value="root_cause">root_cause</option>
                <option value="severity">severity</option>
              </select>
              <input
                className="input"
                placeholder="Scope value"
                value={propScopeValue}
                onChange={(e) => setPropScopeValue(e.target.value)}
                required
              />
              <input
                className="input"
                placeholder="Proposed by"
                value={propBy}
                onChange={(e) => setPropBy(e.target.value)}
                required
              />
              <input
                className="input"
                placeholder="Reason (optional)"
                value={propReason}
                onChange={(e) => setPropReason(e.target.value)}
              />
              <button type="submit" className="btn btn-sm" disabled={propSubmitting}>
                {propSubmitting ? "Proposing…" : "Propose"}
              </button>
            </form>
            {propError && <p className="badge-red text-sm mt-2">{propError}</p>}
          </div>

          {/* ── Promotion history ─────────────────────────────────────────── */}
          {promotionSummary.length > 0 && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Promotion History</h3>
                  <p className="panel-subtitle">Latest proposal state per recommendation key.</p>
                </div>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Family</th>
                    <th>Status</th>
                    <th>Target</th>
                    <th>Scope</th>
                    <th>Proposals</th>
                    <th>Applications</th>
                    <th>Applied at</th>
                  </tr>
                </thead>
                <tbody>
                  {promotionSummary.map((row) => (
                    <tr key={`${row.workspace_id}:${row.recommendation_key}`}>
                      <td className="mono-cell text-sm">{row.recommendation_key.slice(0, 24)}</td>
                      <td><span className="badge-muted">{row.policy_family}</span></td>
                      <td>
                        <span className={STATUS_BADGE[row.latest_proposal_status ?? ""] ?? "badge-muted"}>
                          {row.latest_proposal_status ?? "—"}
                        </span>
                      </td>
                      <td className="text-sm">{row.latest_promotion_target ?? "—"}</td>
                      <td>
                        <span className="badge-muted">{row.latest_scope_type}</span>
                        {" "}<span className="text-sm">{row.latest_scope_value}</span>
                      </td>
                      <td className="text-sm">{row.proposal_count}</td>
                      <td className="text-sm">{row.application_count}</td>
                      <td className="text-sm">{fmtTs(row.latest_applied_at)}</td>
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
