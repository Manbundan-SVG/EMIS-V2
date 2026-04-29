"use client";

import { useState } from "react";
import type {
  GovernanceRoutingPolicyApplicationRow,
  GovernanceRoutingPolicyPromotionSummaryRow,
  GovernanceRoutingPolicyReviewSummaryRow,
  GovernanceRoutingPolicyOpportunityRow,
} from "@/lib/queries/metrics";

type Props = {
  workspace: string;
  policyOpportunities: GovernanceRoutingPolicyOpportunityRow[];
  reviewSummary: GovernanceRoutingPolicyReviewSummaryRow[];
  promotionSummary: GovernanceRoutingPolicyPromotionSummaryRow[];
  applications: GovernanceRoutingPolicyApplicationRow[];
  loading: boolean;
  onChanged?: () => void | Promise<void>;
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

const STATUS_BADGE: Record<string, string> = {
  approved: "badge-green",
  rejected: "badge-red",
  deferred: "badge-yellow",
  pending: "badge-muted",
  applied: "badge-green",
};

function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

export function GovernanceRoutingPolicyReviewPanel({
  workspace,
  policyOpportunities,
  reviewSummary,
  promotionSummary,
  applications,
  loading,
  onChanged,
}: Props) {
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [proposeBusyId, setProposeBusyId] = useState<string | null>(null);
  const [approveBusyId, setApproveBusyId] = useState<string | null>(null);
  const [applyBusyId, setApplyBusyId] = useState<string | null>(null);
  const [actorName] = useState<string>("ops-reviewer");

  const reviewMap = new Map(reviewSummary.map((r) => [r.recommendation_key, r]));
  const promotionMap = new Map(promotionSummary.map((p) => [p.recommendation_key, p]));

  async function submitReview(
    recommendationKey: string,
    reviewStatus: "approved" | "rejected" | "deferred",
  ) {
    setBusyKey(recommendationKey);
    try {
      const res = await fetch("/api/governance/routing-policy-reviews", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workspace,
          recommendationKey,
          reviewStatus,
          reviewedBy: actorName,
          reviewReason: `reviewed_${reviewStatus}`,
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => null) as { error?: string } | null;
        throw new Error(data?.error ?? "review request failed");
      }
      await onChanged?.();
    } finally {
      setBusyKey(null);
    }
  }

  async function proposePromotion(opp: GovernanceRoutingPolicyOpportunityRow) {
    const defaultTarget =
      opp.scope_type === "operator" ? "override" : "rule";
    setProposeBusyId(opp.recommendation_key);
    try {
      const res = await fetch("/api/governance/routing-policy-promotions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workspace,
          action: "propose",
          recommendationKey: opp.recommendation_key,
          promotionTarget: defaultTarget,
          scopeType: opp.scope_type,
          scopeValue: opp.scope_value ?? "",
          proposedBy: actorName,
          proposalReason: opp.reason_code,
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => null) as { error?: string } | null;
        throw new Error(data?.error ?? "propose request failed");
      }
      await onChanged?.();
    } finally {
      setProposeBusyId(null);
    }
  }

  async function approveProposal(proposalId: string, recommendationKey: string) {
    setApproveBusyId(recommendationKey);
    try {
      const res = await fetch("/api/governance/routing-policy-promotions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workspace,
          action: "approve",
          proposalId,
          approvedBy: actorName,
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => null) as { error?: string } | null;
        throw new Error(data?.error ?? "approve request failed");
      }
      await onChanged?.();
    } finally {
      setApproveBusyId(null);
    }
  }

  async function applyPromotion(proposalId: string, recommendationKey: string) {
    setApplyBusyId(recommendationKey);
    try {
      const res = await fetch("/api/governance/routing-policy-promotions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workspace,
          action: "apply",
          proposalId,
          appliedBy: actorName,
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => null) as { error?: string } | null;
        throw new Error(data?.error ?? "apply request failed");
      }
      await onChanged?.();
    } finally {
      setApplyBusyId(null);
    }
  }

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Routing Policy Review &amp; Promotion</h2>
          <p className="panel-subtitle">
            Review advisory routing policy opportunities, create promotion proposals, and
            apply approved proposals into live routing rules and overrides.
            Application is approval-gated — no live changes without a prior approved review and proposal.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading routing policy review state…</p>}

      {!loading && (
        <>
          {/* ── 1. Pending Routing Opportunities + Review Controls ─────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Routing Opportunities</h3>
                <p className="panel-subtitle">Review, approve/reject, then propose a live promotion.</p>
              </div>
            </div>
            {policyOpportunities.length === 0 ? (
              <p className="muted">No routing opportunities available. Run jobs to accumulate signal.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Reason</th>
                    <th>Scope</th>
                    <th>Confidence</th>
                    <th>Benefit</th>
                    <th>Risk</th>
                    <th>Review</th>
                    <th>Proposal</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {policyOpportunities.map((opp) => {
                    const review = reviewMap.get(opp.recommendation_key);
                    const promo = promotionMap.get(opp.recommendation_key);
                    const isBusy = busyKey === opp.recommendation_key;
                    const isProposeBusy = proposeBusyId === opp.recommendation_key;
                    const canPropose =
                      review?.latest_review_status === "approved" &&
                      !["pending", "approved", "applied"].includes(promo?.latest_proposal_status ?? "");

                    return (
                      <tr key={opp.recommendation_key}>
                        <td>
                          <div>{REASON_LABELS[opp.reason_code] ?? opp.reason_code}</div>
                          <div className="muted mono-cell text-xs">
                            {opp.recommendation_key.slice(0, 12)}
                          </div>
                        </td>
                        <td>
                          <span className="badge-muted">{opp.scope_type}</span>
                          {" "}
                          <span className="text-sm">{opp.scope_value}</span>
                        </td>
                        <td>
                          <span className={CONFIDENCE_BADGE[opp.confidence] ?? "badge-muted"}>
                            {opp.confidence}
                          </span>
                        </td>
                        <td>{fmtPct(opp.expected_benefit_score)}</td>
                        <td>{fmtPct(opp.risk_score)}</td>
                        <td>
                          {review ? (
                            <div>
                              <span className={STATUS_BADGE[review.latest_review_status] ?? "badge-muted"}>
                                {review.latest_review_status}
                              </span>
                              <div className="muted text-xs">
                                {review.latest_reviewed_by} · {fmtTs(review.latest_reviewed_at)}
                              </div>
                            </div>
                          ) : (
                            <span className="badge-muted">pending</span>
                          )}
                        </td>
                        <td>
                          {promo ? (
                            <div>
                              <span className={STATUS_BADGE[promo.latest_proposal_status ?? ""] ?? "badge-muted"}>
                                {promo.latest_proposal_status}
                              </span>
                              <div className="muted text-xs">
                                {promo.latest_promotion_target} · {promo.latest_proposed_by}
                              </div>
                            </div>
                          ) : (
                            <span className="muted">—</span>
                          )}
                        </td>
                        <td>
                          <div className="btn-group">
                            <button
                              type="button"
                              disabled={isBusy}
                              onClick={() => void submitReview(opp.recommendation_key, "approved")}
                            >
                              Approve
                            </button>
                            <button
                              type="button"
                              disabled={isBusy}
                              onClick={() => void submitReview(opp.recommendation_key, "rejected")}
                            >
                              Reject
                            </button>
                            <button
                              type="button"
                              disabled={isBusy}
                              onClick={() => void submitReview(opp.recommendation_key, "deferred")}
                            >
                              Defer
                            </button>
                            <button
                              type="button"
                              disabled={isProposeBusy || !canPropose}
                              title={!canPropose ? "Requires approved review and no existing proposal" : undefined}
                              onClick={() => void proposePromotion(opp)}
                            >
                              Propose
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>

          {/* ── 2. Promotion Proposals ─────────────────────────────────── */}
          {promotionSummary.length > 0 && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Promotion Proposals</h3>
                  <p className="panel-subtitle">
                    Approve proposals, then apply to push into live routing rules/overrides.
                  </p>
                </div>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Target</th>
                    <th>Scope</th>
                    <th>Status</th>
                    <th>Proposed</th>
                    <th>Approved</th>
                    <th>Applied</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {promotionSummary.map((promo) => {
                    const isApproveBusy = approveBusyId === promo.recommendation_key;
                    const isApplyBusy = applyBusyId === promo.recommendation_key;
                    const canApprove = ["pending", "deferred"].includes(promo.latest_proposal_status ?? "");
                    const canApply = promo.latest_proposal_status === "approved" && promo.application_count === 0;

                    return (
                      <tr key={promo.recommendation_key}>
                        <td className="mono-cell text-sm">
                          {promo.recommendation_key.slice(0, 12)}
                        </td>
                        <td>
                          <span className="badge-muted">{promo.latest_promotion_target ?? "—"}</span>
                        </td>
                        <td>
                          <span className="badge-muted">{promo.latest_scope_type}</span>
                          {" "}{promo.latest_scope_value}
                        </td>
                        <td>
                          <span className={STATUS_BADGE[promo.latest_proposal_status ?? ""] ?? "badge-muted"}>
                            {promo.latest_proposal_status ?? "—"}
                          </span>
                        </td>
                        <td>
                          <div>{promo.latest_proposed_by ?? "—"}</div>
                          <div className="muted">{fmtTs(promo.latest_proposed_at)}</div>
                        </td>
                        <td>
                          <div>{promo.latest_approved_by ?? "—"}</div>
                          <div className="muted">{fmtTs(promo.latest_approved_at)}</div>
                        </td>
                        <td>
                          {promo.latest_applied_at ? fmtTs(promo.latest_applied_at) : "—"}
                        </td>
                        <td>
                          <div className="btn-group">
                            <button
                              type="button"
                              disabled={isApproveBusy || !canApprove || !promo.latest_proposal_id}
                              onClick={() => void approveProposal(promo.latest_proposal_id!, promo.recommendation_key)}
                            >
                              Approve
                            </button>
                            <button
                              type="button"
                              disabled={isApplyBusy || !canApply || !promo.latest_proposal_id}
                              title={!canApply ? "Requires approved proposal with no prior application" : undefined}
                              onClick={() => void applyPromotion(promo.latest_proposal_id!, promo.recommendation_key)}
                            >
                              Apply
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          {/* ── 3. Applied Promotions ──────────────────────────────────── */}
          {applications.length > 0 && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Applied Promotions</h3>
                  <p className="panel-subtitle">Routing policy changes applied to live rules/overrides.</p>
                </div>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Target</th>
                    <th>Scope</th>
                    <th>Applied by</th>
                    <th>Applied at</th>
                    <th>Rollback candidate</th>
                  </tr>
                </thead>
                <tbody>
                  {applications.map((app) => (
                    <tr key={app.id}>
                      <td className="mono-cell text-sm">
                        {app.recommendation_key.slice(0, 12)}
                      </td>
                      <td>
                        <span className="badge-muted">{app.applied_target}</span>
                      </td>
                      <td>
                        <span className="badge-muted">{app.applied_scope_type}</span>
                        {" "}{app.applied_scope_value}
                      </td>
                      <td>{app.applied_by}</td>
                      <td>{fmtTs(app.applied_at)}</td>
                      <td>
                        {app.rollback_candidate ? (
                          <span className="badge-yellow">yes</span>
                        ) : (
                          <span className="badge-muted">no</span>
                        )}
                      </td>
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
