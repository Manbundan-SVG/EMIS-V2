"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type {
  GovernanceThresholdAutopromotionSummaryRow,
  GovernanceThresholdReviewSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  reviewSummary: GovernanceThresholdReviewSummaryRow[];
  autopromotionSummary: GovernanceThresholdAutopromotionSummaryRow[];
  loading: boolean;
};

function formatNumeric(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return value.toFixed(4);
}

export function GovernanceThresholdLearningReviewPanel({
  reviewSummary,
  autopromotionSummary,
  loading,
}: Props) {
  const pending = reviewSummary.filter((row) => row.status === "pending").length;
  const approved = reviewSummary.filter((row) => row.status === "approved").length;
  const executed = autopromotionSummary.length;

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Threshold Review</h2>
          <p className="panel-subtitle">Advisory review, promotion approval, and guardrailed execution history for learned threshold changes.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading threshold review...</p>}
      {!loading && reviewSummary.length === 0 && autopromotionSummary.length === 0 && (
        <p className="muted">No threshold review or promotion activity is available for this workspace yet.</p>
      )}

      {!loading && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card">
              <div className="kpi-label">Pending proposals</div>
              <div className="kpi-value">{pending}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Approved proposals</div>
              <div className="kpi-value">{approved}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Executions</div>
              <div className="kpi-value">{executed}</div>
            </div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Promotion Proposals</h3>
                  <p className="panel-subtitle">Operator-reviewed threshold change proposals derived from 3.2A learning output.</p>
                </div>
              </div>
              {reviewSummary.length === 0 ? (
                <p className="muted">No proposals are currently available.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Event</th>
                      <th>Scope</th>
                      <th>Change</th>
                      <th>Status</th>
                      <th>Decision</th>
                      <th>Updated</th>
                    </tr>
                  </thead>
                  <tbody>
                    {reviewSummary.slice(0, 10).map((row) => (
                      <tr key={row.proposal_id}>
                        <td>{row.event_type}</td>
                        <td>{`${row.dimension_type}=${row.dimension_value ?? "any"}`}</td>
                        <td>{`${formatNumeric(row.current_value)} → ${formatNumeric(row.proposed_value)}`}</td>
                        <td>{row.status}</td>
                        <td>{formatNullable(row.latest_decision)}</td>
                        <td>{formatTimestamp(row.updated_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Execution History</h3>
                  <p className="panel-subtitle">Manual and automatic promotions with rollback candidates for safe post-change monitoring.</p>
                </div>
              </div>
              {autopromotionSummary.length === 0 ? (
                <p className="muted">No threshold promotions have executed yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Event</th>
                      <th>Mode</th>
                      <th>Change</th>
                      <th>Rollback</th>
                      <th>Executed</th>
                    </tr>
                  </thead>
                  <tbody>
                    {autopromotionSummary.slice(0, 10).map((row) => (
                      <tr key={row.execution_id}>
                        <td>{row.event_type}</td>
                        <td>{row.execution_mode}</td>
                        <td>{`${formatNumeric(row.previous_value)} → ${formatNumeric(row.new_value)}`}</td>
                        <td>{formatNullable(row.rollback_status)}</td>
                        <td>{formatTimestamp(row.created_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </>
      )}
    </section>
  );
}
