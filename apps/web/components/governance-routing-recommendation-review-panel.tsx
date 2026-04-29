"use client";

import { useState } from "react";
import type { GovernanceRoutingRecommendationRow } from "@/lib/queries/metrics";
import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";

type Props = {
  recommendations: GovernanceRoutingRecommendationRow[];
  loading: boolean;
  onChanged?: () => void | Promise<void>;
};

export function GovernanceRoutingRecommendationReviewPanel({
  recommendations,
  loading,
  onChanged,
}: Props) {
  const [busyId, setBusyId] = useState<string | null>(null);

  async function review(
    recommendationId: string,
    caseId: string,
    reviewStatus: "approved" | "rejected" | "deferred",
  ) {
    setBusyId(recommendationId);
    try {
      const res = await fetch("/api/governance/routing-recommendations/reviews", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          recommendationId,
          caseId,
          reviewStatus,
          reviewReason: `reviewed_${reviewStatus}`,
          appliedImmediately: false,
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => null);
        throw new Error(data?.error ?? "review request failed");
      }
      await onChanged?.();
    } finally {
      setBusyId(null);
    }
  }

  async function applyRecommendation(recommendationId: string) {
    setBusyId(recommendationId);
    try {
      const res = await fetch("/api/governance/routing-recommendations/apply", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          recommendationId,
          applicationReason: "approved_routing_recommendation",
        }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => null);
        throw new Error(data?.error ?? "apply request failed");
      }
      await onChanged?.();
    } finally {
      setBusyId(null);
    }
  }

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Routing Recommendation Review</h2>
          <p className="panel-subtitle">Review advisory routing recommendations, then apply approved recommendations to live cases with audit trail.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading routing recommendation review state...</p>}

      {!loading && (
        <div className="panel">
          {recommendations.length === 0 ? (
            <p className="muted">No routing recommendations available.</p>
          ) : (
            <table className="table">
              <thead>
                <tr>
                  <th>Case</th>
                  <th>Recommendation</th>
                  <th>Review</th>
                  <th>Applied</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {recommendations.slice(0, 12).map((row) => (
                  <tr key={row.id}>
                    <td>
                      <div>{row.case_title}</div>
                      <div className="muted">{row.case_status} · {row.severity}</div>
                    </td>
                    <td>
                      <div>{row.recommended_user ?? "unassigned"}</div>
                      <div className="muted">{formatNullable(row.recommended_team)} · {row.confidence} · {row.reason_code}</div>
                    </td>
                    <td>
                      <div>{row.latest_review_status ?? "pending"}</div>
                      <div className="muted">{formatTimestamp(row.latest_reviewed_at ?? null)}</div>
                    </td>
                    <td>
                      <div>{row.application_count && row.application_count > 0 ? "applied" : "not applied"}</div>
                      <div className="muted">
                        {formatNullable(row.latest_applied_user ?? row.latest_applied_team ?? null)} · {formatTimestamp(row.latest_applied_at ?? null)}
                      </div>
                    </td>
                    <td>
                      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                        <button disabled={busyId === row.id} onClick={() => review(row.id, row.case_id, "approved")}>Approve</button>
                        <button disabled={busyId === row.id} onClick={() => review(row.id, row.case_id, "rejected")}>Reject</button>
                        <button disabled={busyId === row.id} onClick={() => review(row.id, row.case_id, "deferred")}>Defer</button>
                        <button
                          disabled={
                            busyId === row.id ||
                            row.latest_review_status !== "approved" ||
                            Boolean(row.application_count && row.application_count > 0)
                          }
                          onClick={() => applyRecommendation(row.id)}
                        >
                          Apply
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </section>
  );
}
