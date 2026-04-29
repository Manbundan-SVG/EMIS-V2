"use client";

import { formatNullable } from "@/lib/formatters/ops";
import type { StabilitySummaryRow } from "@/lib/queries/metrics";

interface Props {
  rows: StabilitySummaryRow[];
  loading: boolean;
}

function formatNumber(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return value.toFixed(digits);
}

function topFamilies(row: StabilitySummaryRow): string {
  return row.family_rows
    .slice(0, 3)
    .map((family) => `${family.signal_family} (${formatNumber(family.instability_score)})`)
    .join(", ");
}

function severityRank(value: string): number {
  if (value === "critical") return 0;
  if (value === "unstable") return 1;
  if (value === "watch") return 2;
  return 3;
}

export function StabilityPanel({ rows, loading }: Props) {
  const ordered = [...rows].sort((left, right) => {
    const severityDelta = severityRank(left.stability_classification) - severityRank(right.stability_classification);
    if (severityDelta !== 0) return severityDelta;
    return (right.created_at ?? "").localeCompare(left.created_at ?? "");
  });

  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Stability Baselines</h2>
          <p className="panel-subtitle">Long-window monitoring for composite drift, family churn, replay consistency risk, and regime instability.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading stability baselines...</p>}
      {!loading && ordered.length === 0 && <p className="muted">No stability baselines have been recorded yet.</p>}

      {ordered.length > 0 && (
        <div className="scroll-table">
          <table className="table">
            <thead>
              <tr>
                <th>Watchlist</th>
                <th>Class</th>
                <th>Composite</th>
                <th>Family</th>
                <th>Replay Risk</th>
                <th>Regime</th>
                <th>Top Unstable Families</th>
              </tr>
            </thead>
            <tbody>
              {ordered.map((row) => (
                <tr key={row.run_id}>
                  <td>{formatNullable(row.watchlist_name ?? row.watchlist_slug ?? row.queue_name)}</td>
                  <td>{row.stability_classification}</td>
                  <td>{formatNumber(row.composite_instability_score)}</td>
                  <td>{formatNumber(row.family_instability_score)}</td>
                  <td>{formatNumber(row.replay_consistency_risk_score)}</td>
                  <td>{formatNumber(row.regime_instability_score)}</td>
                  <td>{topFamilies(row) || "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
