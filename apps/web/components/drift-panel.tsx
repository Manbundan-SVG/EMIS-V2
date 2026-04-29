"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { RunDriftMetricRow, RunDriftRow } from "@/lib/queries/runs";

interface Props {
  drift: RunDriftRow | null;
  loading: boolean;
}

function formatNumber(value: number | null | undefined, digits = 4): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return value.toFixed(digits);
}

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return `${value.toFixed(1)}%`;
}

function severityRank(severity: string): number {
  if (severity === "high") return 0;
  if (severity === "medium") return 1;
  return 2;
}

function topFlaggedMetrics(rows: RunDriftMetricRow[]): RunDriftMetricRow[] {
  return [...rows]
    .filter((row) => row.drift_flag)
    .sort((left, right) => {
      const severityDiff = severityRank(left.severity) - severityRank(right.severity);
      if (severityDiff !== 0) return severityDiff;
      return Math.abs(right.delta_pct ?? 0) - Math.abs(left.delta_pct ?? 0);
    })
    .slice(0, 8);
}

export function DriftPanel({ drift, loading }: Props) {
  const summary = drift?.summary;
  const flagged = topFlaggedMetrics(drift?.metrics ?? []);
  const summaryPayload = (summary?.drift_summary ?? {}) as Record<string, unknown>;
  const currentVersions = (summaryPayload.current_versions ?? {}) as Record<string, unknown>;
  const baselineVersions = (summaryPayload.baseline_versions ?? {}) as Record<string, unknown>;

  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Run Drift</h2>
          <p className="panel-subtitle">Comparator metadata, flagged movers, and run-to-run drift reconciliation.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading drift metrics...</p>}
      {!loading && !summary && <p className="muted">No drift data is available for this run yet.</p>}

      {summary && (
        <div className="panel-stack">
          <div className="stats-grid">
            <div className="stat-card">
              <div className="kpi-label">Severity</div>
              <div className="kpi-value kpi-value-sm">{formatNullable(summary.drift_severity)}</div>
              <div className="kpi-sub">flagged: {summary.flagged_metric_count}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Comparator</div>
              <div className="kpi-value kpi-value-sm">{summary.comparison_run_id ? summary.comparison_run_id.slice(0, 8) : "-"}</div>
              <div className="kpi-sub">metrics: {summary.metric_count}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Composite Delta</div>
              <div className="kpi-value kpi-value-sm">{formatPercent(summaryPayload.composite_delta_pct as number | null | undefined)}</div>
              <div className="kpi-sub">{formatNumber(summaryPayload.composite_score as number | null | undefined)} vs {formatNumber(summaryPayload.baseline_composite_score as number | null | undefined)}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Computed</div>
              <div className="kpi-value kpi-value-sm">{formatTimestamp(summary.computed_at)}</div>
              <div className="kpi-sub">regime changed: {Boolean(summaryPayload.regime_changed) ? "yes" : "no"}</div>
            </div>
          </div>

          <div className="detail-grid">
            <div className="detail-list">
              <div><span className="muted">Current regime</span> {formatNullable(summaryPayload.regime)}</div>
              <div><span className="muted">Baseline regime</span> {formatNullable(summaryPayload.baseline_regime)}</div>
              <div><span className="muted">Current compute</span> {formatNullable(currentVersions.compute_version)}</div>
              <div><span className="muted">Baseline compute</span> {formatNullable(baselineVersions.compute_version)}</div>
              <div><span className="muted">Current signal registry</span> {formatNullable(currentVersions.signal_registry_version)}</div>
              <div><span className="muted">Baseline signal registry</span> {formatNullable(baselineVersions.signal_registry_version)}</div>
              <div><span className="muted">Current model</span> {formatNullable(currentVersions.model_version)}</div>
              <div><span className="muted">Baseline model</span> {formatNullable(baselineVersions.model_version)}</div>
            </div>
            <div className="json-panel">
              <div className="panel-mini-title">Summary</div>
              <pre>{JSON.stringify(summaryPayload, null, 2)}</pre>
            </div>
          </div>

          <div className="scroll-table">
            <table className="table">
              <thead>
                <tr>
                  <th>Metric</th>
                  <th>Entity</th>
                  <th>Severity</th>
                  <th>Current</th>
                  <th>Baseline</th>
                  <th>Delta</th>
                  <th>Delta %</th>
                </tr>
              </thead>
              <tbody>
                {flagged.map((metric) => (
                  <tr key={`${metric.metric_type}:${metric.entity_name}`}>
                    <td>{metric.metric_type}</td>
                    <td>{metric.entity_name}</td>
                    <td>{metric.severity}</td>
                    <td>{formatNumber(metric.current_value)}</td>
                    <td>{formatNumber(metric.baseline_value)}</td>
                    <td>{formatNumber(metric.delta_abs)}</td>
                    <td>{formatPercent(metric.delta_pct)}</td>
                  </tr>
                ))}
                {flagged.length === 0 && (
                  <tr>
                    <td colSpan={7} className="muted">No flagged drift metrics were computed for this run.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </section>
  );
}
