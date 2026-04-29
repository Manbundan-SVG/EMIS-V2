"use client";

import { formatNullable } from "@/lib/formatters/ops";
import type { RegimeTransitionRow } from "@/lib/queries/runs";

interface Props {
  regimeTransition: RegimeTransitionRow | null;
  loading: boolean;
}

function formatNumber(value: number | null | undefined, digits = 4): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return value.toFixed(digits);
}

function formatScore(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  if (value >= 0.80) return `high (${value.toFixed(2)})`;
  if (value >= 0.50) return `medium (${value.toFixed(2)})`;
  return `low (${value.toFixed(2)})`;
}

export function RegimeTransitionPanel({ regimeTransition, loading }: Props) {
  const summary = regimeTransition?.summary;
  const familyShifts = regimeTransition?.familyShifts ?? [];

  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Regime Transition</h2>
          <p className="panel-subtitle">Prior-vs-current regime boundaries, family redistribution, and transition stability diagnostics.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading regime transition diagnostics...</p>}
      {!loading && !summary && <p className="muted">No regime transition data is available for this run yet.</p>}

      {summary && (
        <div className="panel-stack">
          <div className="stats-grid">
            <div className="stat-card">
              <div className="kpi-label">Classification</div>
              <div className="kpi-value kpi-value-sm">{formatNullable(summary.transition_classification)}</div>
              <div className="kpi-sub">transition: {summary.transition_detected ? "yes" : "no"}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Regime Pair</div>
              <div className="kpi-value kpi-value-sm">{`${formatNullable(summary.from_regime)} -> ${formatNullable(summary.to_regime)}`}</div>
              <div className="kpi-sub">prior: {summary.prior_run_id ? summary.prior_run_id.slice(0, 8) : "-"}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Stability</div>
              <div className="kpi-value kpi-value-sm">{formatScore(summary.stability_score)}</div>
              <div className="kpi-sub">anomaly: {formatScore(summary.anomaly_likelihood)}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Composite Shift</div>
              <div className="kpi-value kpi-value-sm">{formatNumber(summary.composite_shift, 6)}</div>
              <div className="kpi-sub">abs: {formatNumber(summary.composite_shift_abs, 6)}</div>
            </div>
          </div>

          <div className="detail-grid">
            <div className="detail-list">
              <div><span className="muted">Dominant family gained</span> {formatNullable(summary.dominant_family_gained)}</div>
              <div><span className="muted">Dominant family lost</span> {formatNullable(summary.dominant_family_lost)}</div>
              <div><span className="muted">Replay</span> {summary.is_replay ? "yes" : "no"}</div>
              <div><span className="muted">Replay source</span> {formatNullable(summary.replayed_from_run_id)}</div>
            </div>
            <div className="json-panel">
              <div className="panel-mini-title">Transition Metadata</div>
              <pre>{JSON.stringify(summary.metadata ?? {}, null, 2)}</pre>
            </div>
          </div>

          <div className="scroll-table">
            <table className="table">
              <thead>
                <tr>
                  <th>Family</th>
                  <th>Prior</th>
                  <th>Current</th>
                  <th>Delta</th>
                  <th>Direction</th>
                  <th>Ranks</th>
                </tr>
              </thead>
              <tbody>
                {familyShifts.map((row) => (
                  <tr key={row.id}>
                    <td>{row.signal_family}</td>
                    <td>{formatNumber(row.prior_family_score, 6)}</td>
                    <td>{formatNumber(row.current_family_score, 6)}</td>
                    <td>{formatNumber(row.family_delta, 6)}</td>
                    <td>{row.shift_direction}</td>
                    <td>{`${formatNullable(row.prior_family_rank)} -> ${formatNullable(row.current_family_rank)}`}</td>
                  </tr>
                ))}
                {familyShifts.length === 0 && (
                  <tr>
                    <td colSpan={6} className="muted">No family redistribution rows were recorded for this run.</td>
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
