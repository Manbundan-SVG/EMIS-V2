"use client";

import { useState } from "react";
import type {
  GovernanceRoutingPolicyPendingRollbackRow,
  GovernanceRoutingPolicyRollbackReviewSummaryRow,
  GovernanceRoutingPolicyRollbackExecutionSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  workspace: string;
  pendingRollbacks: GovernanceRoutingPolicyPendingRollbackRow[];
  reviewSummary: GovernanceRoutingPolicyRollbackReviewSummaryRow[];
  executionSummary: GovernanceRoutingPolicyRollbackExecutionSummaryRow[];
  loading: boolean;
  onChanged?: () => void | Promise<void>;
};

const REVIEW_BADGE: Record<string, string> = {
  approved: "badge-green",
  rejected: "badge-red",
  deferred: "badge-yellow",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

export function GovernanceRoutingPolicyRollbacksPanel({
  workspace,
  pendingRollbacks,
  reviewSummary,
  executionSummary,
  loading,
  onChanged,
}: Props) {
  const [busyCandidateId, setBusyCandidateId] = useState<string | null>(null);
  const [executeBusyId, setExecuteBusyId] = useState<string | null>(null);
  const [actorName] = useState<string>("ops-reviewer");
  const [notesByCandidate, setNotesByCandidate] = useState<Record<string, string>>({});

  const reviewMap = new Map(reviewSummary.map((r) => [r.rollback_candidate_id, r]));

  async function submitReview(
    rollbackCandidateId: string,
    reviewStatus: "approved" | "rejected" | "deferred",
  ) {
    setBusyCandidateId(rollbackCandidateId);
    try {
      const res = await fetch("/api/governance/routing-policy-rollbacks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workspace,
          action: "submit_review",
          rollbackCandidateId,
          reviewStatus,
          reviewedBy: actorName,
          reviewReason: `reviewed_${reviewStatus}`,
          notes: notesByCandidate[rollbackCandidateId] ?? null,
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => null) as { error?: string } | null;
        throw new Error(data?.error ?? "review request failed");
      }
      await onChanged?.();
    } finally {
      setBusyCandidateId(null);
    }
  }

  async function executeRollback(rollbackCandidateId: string) {
    setExecuteBusyId(rollbackCandidateId);
    try {
      const res = await fetch("/api/governance/routing-policy-rollbacks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          workspace,
          action: "execute_rollback",
          rollbackCandidateId,
          executedBy: actorName,
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => null) as { error?: string } | null;
        throw new Error(data?.error ?? "rollback execution failed");
      }
      await onChanged?.();
    } finally {
      setExecuteBusyId(null);
    }
  }

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Routing Policy Rollback Review &amp; Execution</h2>
          <p className="panel-subtitle">
            Governed, approval-gated rollback of autopromotion candidates.
            Every rollback restores the captured prior policy state — no guessing.
            Execution requires an approved review.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading rollback state…</p>}

      {!loading && (
        <>
          {/* ── 1. Pending Rollback Candidates + Review Controls ──────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Pending Rollback Candidates</h3>
                <p className="panel-subtitle">
                  Autopromotion candidates awaiting review or approved for rollback execution.
                </p>
              </div>
            </div>
            {pendingRollbacks.length === 0 ? (
              <p className="muted">No pending rollback candidates. All candidates resolved.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Scope</th>
                    <th>Risk</th>
                    <th>Created</th>
                    <th>Review status</th>
                    <th>Notes</th>
                    <th>Review actions</th>
                    <th>Execute</th>
                  </tr>
                </thead>
                <tbody>
                  {pendingRollbacks.map((rc) => {
                    const review = reviewMap.get(rc.rollback_candidate_id);
                    const isBusy = busyCandidateId === rc.rollback_candidate_id;
                    const isExecBusy = executeBusyId === rc.rollback_candidate_id;
                    const canExecute = review?.latest_review_status === "approved";

                    return (
                      <tr key={rc.rollback_candidate_id}>
                        <td className="mono-cell text-sm">
                          {rc.recommendation_key.slice(0, 12)}
                        </td>
                        <td>
                          <span className="badge-muted">{rc.scope_type}</span>
                          {" "}<span className="text-sm">{rc.scope_value}</span>
                        </td>
                        <td>
                          <span className={rc.rollback_risk_score > 0.3 ? "badge-red" : "badge-muted"}>
                            {fmtPct(rc.rollback_risk_score)}
                          </span>
                        </td>
                        <td className="text-sm">{fmtTs(rc.created_at)}</td>
                        <td>
                          {review ? (
                            <div>
                              <span className={REVIEW_BADGE[review.latest_review_status ?? ""] ?? "badge-muted"}>
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
                          <input
                            type="text"
                            placeholder="optional note"
                            value={notesByCandidate[rc.rollback_candidate_id] ?? ""}
                            onChange={(e) =>
                              setNotesByCandidate((n) => ({
                                ...n,
                                [rc.rollback_candidate_id]: e.target.value,
                              }))
                            }
                            style={{ width: "8rem" }}
                          />
                        </td>
                        <td>
                          <div className="btn-group">
                            <button
                              type="button"
                              disabled={isBusy}
                              onClick={() => void submitReview(rc.rollback_candidate_id, "approved")}
                            >
                              Approve
                            </button>
                            <button
                              type="button"
                              disabled={isBusy}
                              onClick={() => void submitReview(rc.rollback_candidate_id, "rejected")}
                            >
                              Reject
                            </button>
                            <button
                              type="button"
                              disabled={isBusy}
                              onClick={() => void submitReview(rc.rollback_candidate_id, "deferred")}
                            >
                              Defer
                            </button>
                          </div>
                        </td>
                        <td>
                          <button
                            type="button"
                            disabled={isExecBusy || !canExecute}
                            title={!canExecute ? "Requires approved review" : "Execute rollback — restores prior policy"}
                            onClick={() => void executeRollback(rc.rollback_candidate_id)}
                          >
                            {isExecBusy ? "Rolling back…" : "Execute Rollback"}
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>

          {/* ── 2. Recent Rollback Executions ─────────────────────────────── */}
          {executionSummary.length > 0 && (
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Rollback Execution History</h3>
                  <p className="panel-subtitle">
                    Completed rollback operations with restored policy targets.
                  </p>
                </div>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Key</th>
                    <th>Scope</th>
                    <th>Target</th>
                    <th>Risk</th>
                    <th>Rolled back</th>
                    <th>Executed by</th>
                    <th>Executions</th>
                  </tr>
                </thead>
                <tbody>
                  {executionSummary.map((es) => (
                    <tr key={es.rollback_candidate_id}>
                      <td className="mono-cell text-sm">{es.recommendation_key.slice(0, 12)}</td>
                      <td>
                        <span className="badge-muted">{es.scope_type}</span>
                        {" "}<span className="text-sm">{es.scope_value}</span>
                      </td>
                      <td><span className="badge-muted">{es.target_type ?? "—"}</span></td>
                      <td className="text-sm">{fmtPct(es.rollback_risk_score)}</td>
                      <td>
                        {es.rolled_back ? (
                          <div>
                            <span className="badge-green">yes</span>
                            <div className="muted text-xs">{fmtTs(es.rolled_back_at)}</div>
                          </div>
                        ) : (
                          <span className="badge-muted">no</span>
                        )}
                      </td>
                      <td className="text-sm">{es.latest_executed_by ?? "—"}</td>
                      <td>{es.execution_count}</td>
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
