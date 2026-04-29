"use client";

import { formatDurationMs, formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { VersionGovernanceRow } from "@/lib/queries/metrics";

interface Props {
  rows: VersionGovernanceRow[];
  loading: boolean;
}

function formatRatio(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return `${(value * 100).toFixed(digits)}%`;
}

function formatNumber(value: number | null | undefined, digits = 3): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return value.toFixed(digits);
}

export function VersionGovernancePanel({ rows, loading }: Props) {
  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Version Governance</h2>
          <p className="panel-subtitle">Health ranking across compute, signal-registry, and model versions using stability, replay consistency, and transition behavior.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading version governance...</p>}
      {!loading && rows.length === 0 && <p className="muted">No version-governance rows are available for this workspace yet.</p>}

      {rows.length > 0 && (
        <div className="scroll-table">
          <table className="table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Compute</th>
                <th>Signal Registry</th>
                <th>Model</th>
                <th>Health</th>
                <th>Runs</th>
                <th>Failure</th>
                <th>Family</th>
                <th>Replay Match</th>
                <th>Replay Delta</th>
                <th>Conflicting Transitions</th>
                <th>Last Completed</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={`${row.compute_version}|${row.signal_registry_version}|${row.model_version}`}>
                  <td>{row.health_rank}</td>
                  <td className="mono-cell">{formatNullable(row.compute_version)}</td>
                  <td className="mono-cell">{formatNullable(row.signal_registry_version)}</td>
                  <td className="mono-cell">{formatNullable(row.model_version)}</td>
                  <td>{formatNumber(row.governance_health_score)}</td>
                  <td>
                    {row.run_count}
                    <div className="muted">replays: {row.replay_count}</div>
                  </td>
                  <td>{formatRatio(row.failure_rate)}</td>
                  <td>{formatNumber(row.avg_family_instability)}</td>
                  <td>{formatNumber(row.avg_input_match_score)}</td>
                  <td>
                    {formatNumber(row.avg_replay_composite_delta_abs)}
                    <div className="muted">risk: {formatNumber(row.avg_replay_consistency_risk)}</div>
                  </td>
                  <td>{formatRatio(row.conflicting_transition_rate)}</td>
                  <td>
                    {formatTimestamp(row.last_completed_at)}
                    <div className="muted">replay: {formatTimestamp(row.last_replay_completed_at)}</div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {rows.length > 0 && (
        <div className="detail-grid">
          <div className="detail-list">
            <div><span className="muted">Top tuple</span> {formatNullable(rows[0]?.compute_version)} / {formatNullable(rows[0]?.signal_registry_version)} / {formatNullable(rows[0]?.model_version)}</div>
            <div><span className="muted">Top health</span> {formatNumber(rows[0]?.governance_health_score)}</div>
            <div><span className="muted">Top runtime</span> {formatDurationMs(rows[0]?.avg_runtime_ms)}</div>
          </div>
          <div className="detail-list">
            <div><span className="muted">Weakest tuple</span> {formatNullable(rows.at(-1)?.compute_version)} / {formatNullable(rows.at(-1)?.signal_registry_version)} / {formatNullable(rows.at(-1)?.model_version)}</div>
            <div><span className="muted">Weakest health</span> {formatNumber(rows.at(-1)?.governance_health_score)}</div>
            <div><span className="muted">Weakest failure</span> {formatRatio(rows.at(-1)?.failure_rate)}</div>
          </div>
        </div>
      )}
    </section>
  );
}
