"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { GovernanceRoutingRecommendationRow } from "@/lib/queries/metrics";

type Props = {
  recommendations: GovernanceRoutingRecommendationRow[];
  loading: boolean;
};

function formatScore(value: number): string {
  return value.toFixed(3);
}

export function GovernanceRoutingRecommendationsPanel({ recommendations, loading }: Props) {
  const latest = recommendations[0] ?? null;

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Routing Recommendations</h2>
          <p className="panel-subtitle">Advisory assignment suggestions derived from routing outcomes, workload pressure, and case-specific fit.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading routing recommendations...</p>}

      {!loading && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card">
              <div className="kpi-label">Recommendation rows</div>
              <div className="kpi-value">{recommendations.length}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Latest recommendation</div>
              <div className="kpi-value">{latest?.recommended_user ?? latest?.recommended_team ?? "none"}</div>
              <div className="muted">{latest ? latest.reason_code : "No recommendations yet"}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Accepted rows</div>
              <div className="kpi-value">{recommendations.filter((row) => row.accepted === true).length}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Overrides</div>
              <div className="kpi-value">{recommendations.filter((row) => row.accepted === false).length}</div>
            </div>
          </div>

          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Recent Recommendations</h3>
                <p className="panel-subtitle">Latest advisory targets, fallback targets, and whether later assignment behavior matched the recommendation.</p>
              </div>
            </div>
            {recommendations.length === 0 ? (
              <p className="muted">No routing recommendations available yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Case</th>
                    <th>Recommended</th>
                    <th>Fallback</th>
                    <th>Confidence</th>
                    <th>Score</th>
                    <th>Accepted</th>
                    <th>Created</th>
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
                        <div className="muted">{formatNullable(row.recommended_team)}</div>
                      </td>
                      <td>
                        <div>{formatNullable(row.fallback_user)}</div>
                        <div className="muted">{formatNullable(row.fallback_team)}</div>
                      </td>
                      <td>{row.confidence}</td>
                      <td>{formatScore(row.score)}</td>
                      <td>
                        {row.accepted === null ? "pending" : row.accepted ? "accepted" : "overridden"}
                      </td>
                      <td>{formatTimestamp(row.created_at)}</td>
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
