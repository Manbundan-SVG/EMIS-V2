"use client";

import type { RunAttributionRow } from "@/lib/queries/runs";

interface Props {
  attribution: RunAttributionRow | null;
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

function formatInvalidators(values: string[]): string {
  return values.length > 0 ? values.join(", ") : "-";
}

export function AttributionPanel({ attribution, loading }: Props) {
  const topSignals = attribution?.signal_attributions.slice(0, 10) ?? [];

  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Attribution</h2>
          <p className="panel-subtitle">Signal-family rollups, top drivers, and composite reconciliation for the selected run.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading attribution...</p>}
      {!loading && !attribution && <p className="muted">No attribution has been persisted for this run yet.</p>}

      {attribution && (
        <div className="panel-stack">
          <div className="stats-grid">
            <div className="stat-card">
              <div className="kpi-label">Version</div>
              <div className="kpi-value kpi-value-sm">{attribution.attribution_version ?? "-"}</div>
              <div className="kpi-sub">status: {attribution.status}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Reconciled</div>
              <div className="kpi-value kpi-value-sm">{attribution.attribution_reconciled ? "yes" : "no"}</div>
              <div className="kpi-sub">delta: {formatNumber(attribution.attribution_reconciliation_delta, 6)}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Attribution Total</div>
              <div className="kpi-value kpi-value-sm">{formatNumber(attribution.attribution_total)}</div>
              <div className="kpi-sub">target: {formatNumber(attribution.attribution_target_total)}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Rows</div>
              <div className="kpi-value kpi-value-sm">{attribution.signal_attributions.length}</div>
              <div className="kpi-sub">families: {attribution.family_attributions.length}</div>
            </div>
          </div>

          <div className="scroll-table">
            <table className="table">
              <thead>
                <tr>
                  <th>Rank</th>
                  <th>Family</th>
                  <th>Score</th>
                  <th>% of Total</th>
                  <th>Positive</th>
                  <th>Negative</th>
                  <th>Invalidators</th>
                </tr>
              </thead>
              <tbody>
                {attribution.family_attributions.map((family) => (
                  <tr key={family.signal_family}>
                    <td>{family.family_rank}</td>
                    <td>{family.signal_family}</td>
                    <td>{formatNumber(family.family_score)}</td>
                    <td>{formatPercent(family.family_pct_of_total)}</td>
                    <td>{formatNumber(family.positive_contribution)}</td>
                    <td>{formatNumber(family.negative_contribution)}</td>
                    <td>{formatInvalidators(family.active_invalidators)}</td>
                  </tr>
                ))}
                {attribution.family_attributions.length === 0 && (
                  <tr>
                    <td colSpan={7} className="muted">No signal-family attribution rows are available for this run.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          <div className="scroll-table">
            <table className="table">
              <thead>
                <tr>
                  <th>Signal</th>
                  <th>Asset</th>
                  <th>Family</th>
                  <th>Regime</th>
                  <th>Weight</th>
                  <th>Contribution</th>
                  <th>Invalidators</th>
                </tr>
              </thead>
              <tbody>
                {topSignals.map((signal) => (
                  <tr key={`${signal.asset_id ?? "na"}:${signal.signal_name}`}>
                    <td>{signal.signal_name}</td>
                    <td>{signal.asset_symbol ?? "-"}</td>
                    <td>{signal.signal_family}</td>
                    <td>{signal.regime ?? "-"}</td>
                    <td>{formatNumber(signal.weight_applied)}</td>
                    <td>{formatNumber(signal.contribution_value)}</td>
                    <td>{formatInvalidators(signal.active_invalidators)}</td>
                  </tr>
                ))}
                {topSignals.length === 0 && (
                  <tr>
                    <td colSpan={7} className="muted">No signal attribution rows are available for this run.</td>
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
