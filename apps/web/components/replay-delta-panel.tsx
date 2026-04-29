"use client";

import { formatNullable } from "@/lib/formatters/ops";
import type { ReplayDeltaRow, RunInspectionRow } from "@/lib/queries/runs";

interface Props {
  run: RunInspectionRow | null;
  replayDelta: ReplayDeltaRow | null;
  loading: boolean;
}

function formatNumber(value: number | null | undefined, digits = 4): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return value.toFixed(digits);
}

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

export function ReplayDeltaPanel({ run, replayDelta, loading }: Props) {
  const diagnosis = (replayDelta?.summary?.diagnosis as string | undefined) ?? null;

  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Replay Delta</h2>
          <p className="panel-subtitle">Replay-vs-source diagnostic deltas for inputs, versions, regime, and attribution behavior.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading replay diagnostics...</p>}
      {!loading && !run && <p className="muted">Select a run to inspect replay diagnostics.</p>}
      {!loading && run && !run.is_replay && <p className="muted">The selected run is not a replay.</p>}
      {!loading && run?.is_replay && !replayDelta && <p className="muted">No replay delta has been persisted for this replay yet.</p>}

      {run?.is_replay && replayDelta && (
        <div className="panel-stack">
          <div className="stats-grid">
            <div className="stat-card">
              <div className="kpi-label">Severity</div>
              <div className="kpi-value kpi-value-sm">{replayDelta.severity}</div>
              <div className="kpi-sub">{formatNullable(diagnosis)}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Input Match</div>
              <div className="kpi-value kpi-value-sm">{formatPercent(replayDelta.input_match_score)}</div>
              <div className="kpi-sub">version match: {replayDelta.version_match ? "yes" : "no"}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Composite Delta</div>
              <div className="kpi-value kpi-value-sm">{formatNumber(replayDelta.composite_delta, 6)}</div>
              <div className="kpi-sub">abs: {formatNumber(replayDelta.composite_delta_abs, 6)}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Source Run</div>
              <div className="kpi-value kpi-value-sm">{replayDelta.source_run_id.slice(0, 8)}</div>
              <div className="kpi-sub">replay: {replayDelta.replay_run_id.slice(0, 8)}</div>
            </div>
          </div>

          <div className="detail-grid">
            <div className="detail-list">
              <div><span className="muted">Source regime</span> {formatNullable(replayDelta.source_regime)}</div>
              <div><span className="muted">Replay regime</span> {formatNullable(replayDelta.replay_regime)}</div>
              <div><span className="muted">Regime changed</span> {replayDelta.regime_changed ? "yes" : "no"}</div>
              <div><span className="muted">Compute changed</span> {replayDelta.compute_version_changed ? "yes" : "no"}</div>
              <div><span className="muted">Signal registry changed</span> {replayDelta.signal_registry_version_changed ? "yes" : "no"}</div>
              <div><span className="muted">Model changed</span> {replayDelta.model_version_changed ? "yes" : "no"}</div>
            </div>
            <div className="json-panel">
              <div className="panel-mini-title">Input Match Details</div>
              <pre>{JSON.stringify(replayDelta.input_match_details ?? {}, null, 2)}</pre>
            </div>
          </div>

          <div className="scroll-table">
            <table className="table">
              <thead>
                <tr>
                  <th>Signal</th>
                  <th>Source</th>
                  <th>Replay</th>
                  <th>Delta</th>
                </tr>
              </thead>
              <tbody>
                {replayDelta.largest_signal_deltas.map((row) => (
                  <tr key={row.signal_key ?? `${row.source_value}:${row.replay_value}`}>
                    <td>{formatNullable(row.signal_key)}</td>
                    <td>{formatNumber(row.source_value, 6)}</td>
                    <td>{formatNumber(row.replay_value, 6)}</td>
                    <td>{formatNumber(row.delta, 6)}</td>
                  </tr>
                ))}
                {replayDelta.largest_signal_deltas.length === 0 && (
                  <tr>
                    <td colSpan={4} className="muted">No signal deltas are available for this replay.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          <div className="scroll-table">
            <table className="table">
              <thead>
                <tr>
                  <th>Family</th>
                  <th>Source</th>
                  <th>Replay</th>
                  <th>Delta</th>
                </tr>
              </thead>
              <tbody>
                {replayDelta.largest_family_deltas.map((row) => (
                  <tr key={row.signal_family ?? `${row.source_value}:${row.replay_value}`}>
                    <td>{formatNullable(row.signal_family)}</td>
                    <td>{formatNumber(row.source_value, 6)}</td>
                    <td>{formatNumber(row.replay_value, 6)}</td>
                    <td>{formatNumber(row.delta, 6)}</td>
                  </tr>
                ))}
                {replayDelta.largest_family_deltas.length === 0 && (
                  <tr>
                    <td colSpan={4} className="muted">No family deltas are available for this replay.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          <div className="json-panel">
            <div className="panel-mini-title">Summary</div>
            <pre>{JSON.stringify(replayDelta.summary ?? {}, null, 2)}</pre>
          </div>
        </div>
      )}
    </section>
  );
}
