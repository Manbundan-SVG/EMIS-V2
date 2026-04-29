"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type {
  GovernanceThresholdLearningRecommendationRow,
  GovernanceThresholdPerformanceSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  performance: GovernanceThresholdPerformanceSummaryRow[];
  recommendations: GovernanceThresholdLearningRecommendationRow[];
  loading: boolean;
};

function formatScore(value: number | null, digits = 2): string {
  if (value === null || Number.isNaN(value)) return "-";
  return value.toFixed(digits);
}

export function GovernanceThresholdLearningPanel({
  performance,
  recommendations,
  loading,
}: Props) {
  const noisyRow = [...performance].sort((a, b) => b.avg_noise_score - a.avg_noise_score)[0] ?? null;
  const strongestSignal = [...performance].sort((a, b) => b.avg_precision_proxy - a.avg_precision_proxy)[0] ?? null;

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Threshold Learning</h2>
          <p className="panel-subtitle">Advisory threshold recommendations based on case outcomes, escalations, and recoveries.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading threshold learning...</p>}
      {!loading && performance.length === 0 && recommendations.length === 0 && (
        <p className="muted">No threshold learning data is available for this workspace yet.</p>
      )}

      {!loading && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card">
              <div className="kpi-label">Feedback rows</div>
              <div className="kpi-value">{performance.reduce((sum, row) => sum + row.feedback_rows, 0)}</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Noisiest event</div>
              <div className="kpi-value">{noisyRow ? noisyRow.event_type : "none"}</div>
              <div className="muted">
                {noisyRow ? `${noisyRow.regime} / ${formatScore(noisyRow.avg_noise_score)}` : "Need more downstream feedback"}
              </div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Strongest precision</div>
              <div className="kpi-value">{strongestSignal ? strongestSignal.event_type : "none"}</div>
              <div className="muted">
                {strongestSignal ? `${strongestSignal.regime} / ${formatScore(strongestSignal.avg_precision_proxy)}` : "No learned precision yet"}
              </div>
            </div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Active Recommendations</h3>
                  <p className="panel-subtitle">These are advisory only. They do not change live thresholds automatically.</p>
                </div>
              </div>
              {recommendations.length === 0 ? (
                <p className="muted">No active threshold recommendations.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Event</th>
                      <th>Dimension</th>
                      <th>Direction</th>
                      <th>Reason</th>
                      <th>Confidence</th>
                      <th>Created</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recommendations.slice(0, 10).map((row) => (
                      <tr key={row.recommendation_id}>
                        <td>{row.event_type}</td>
                        <td>{`${row.dimension_type}=${row.dimension_value}`}</td>
                        <td>{row.direction}</td>
                        <td>{row.reason_code}</td>
                        <td>{formatScore(row.confidence)}</td>
                        <td>{formatTimestamp(row.created_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Performance Summary</h3>
                  <p className="panel-subtitle">Aggregated downstream outcomes by event family and regime.</p>
                </div>
              </div>
              {performance.length === 0 ? (
                <p className="muted">No threshold feedback has been recorded yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Event</th>
                      <th>Regime</th>
                      <th>Feedback</th>
                      <th>Precision</th>
                      <th>Noise</th>
                      <th>Escalations</th>
                      <th>Latest</th>
                    </tr>
                  </thead>
                  <tbody>
                    {performance.slice(0, 12).map((row) => (
                      <tr key={`${row.threshold_profile_key}:${row.event_type}:${row.regime}`}>
                        <td>{row.event_type}</td>
                        <td>{row.regime}</td>
                        <td>{row.feedback_rows}</td>
                        <td>{formatScore(row.avg_precision_proxy)}</td>
                        <td>{formatScore(row.avg_noise_score)}</td>
                        <td>{row.escalation_count}</td>
                        <td>{formatTimestamp(row.latest_feedback_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
              {!loading && performance.length > 0 && (
                <p className="muted">
                  Profiles tracked: {new Set(performance.map((row) => row.threshold_profile_key)).size}. Latest recommendation status: {formatNullable(recommendations[0]?.status ?? null)}.
                </p>
              )}
            </div>
          </div>
        </>
      )}
    </section>
  );
}
