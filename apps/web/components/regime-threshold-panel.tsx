"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type {
  ActiveRegimeThresholdRow,
  GovernanceThresholdApplicationRow,
  MacroSyncHealthRow,
  RegimeThresholdOverrideRow,
  RegimeThresholdProfileRow,
} from "@/lib/queries/metrics";

interface Props {
  profiles: RegimeThresholdProfileRow[];
  overrides: RegimeThresholdOverrideRow[];
  active: ActiveRegimeThresholdRow[];
  applications: GovernanceThresholdApplicationRow[];
  macroSyncHealth: MacroSyncHealthRow[];
  loading: boolean;
}

function fmt(value: number | null | undefined, digits = 3): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  return value.toFixed(digits);
}

export function RegimeThresholdPanel({
  profiles,
  overrides,
  active,
  applications,
  macroSyncHealth,
  loading,
}: Props) {
  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Regime Thresholds</h2>
          <p className="panel-subtitle">Active regime-aware governance thresholds, recent applications, and macro sync health.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading regime thresholds...</p>}
      {!loading && profiles.length === 0 && active.length === 0 && applications.length === 0 && macroSyncHealth.length === 0 && (
        <p className="muted">No regime-aware threshold data is available for this workspace.</p>
      )}

      {active.length > 0 && (
        <div className="scroll-table">
          <table className="table">
            <thead>
              <tr>
                <th>Regime</th>
                <th>Profile</th>
                <th>Health Floor</th>
                <th>Family Ceiling</th>
                <th>Replay Floor</th>
                <th>Regime Ceiling</th>
                <th>Conflict Ceiling</th>
              </tr>
            </thead>
            <tbody>
              {active.map((row) => (
                <tr key={`${row.workspace_id ?? "global"}:${row.regime}:${row.profile_id}:${row.override_id ?? "default"}`}>
                  <td>{row.regime}</td>
                  <td>
                    {row.profile_name}
                    <div className="muted">{row.override_id ? "override" : "default"}</div>
                  </td>
                  <td>{fmt(row.version_health_floor)}</td>
                  <td>{fmt(row.family_instability_ceiling)}</td>
                  <td>{fmt(row.replay_consistency_floor)}</td>
                  <td>{fmt(row.regime_instability_ceiling)}</td>
                  <td>{fmt(row.conflicting_transition_ceiling)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {applications.length > 0 && (
        <div className="scroll-table">
          <table className="table">
            <thead>
              <tr>
                <th>Applied</th>
                <th>Stage</th>
                <th>Regime</th>
                <th>Profile</th>
                <th>Watchlist</th>
                <th>Run</th>
              </tr>
            </thead>
            <tbody>
              {applications.map((row) => (
                <tr key={row.id}>
                  <td>{formatTimestamp(row.created_at)}</td>
                  <td>{row.evaluation_stage}</td>
                  <td>{row.regime}</td>
                  <td>{formatNullable(row.profile_name)}</td>
                  <td>{formatNullable(row.watchlist_name ?? row.watchlist_slug)}</td>
                  <td className="mono-cell">{row.run_id ? row.run_id.slice(0, 8) : "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {macroSyncHealth.length > 0 && (
        <div className="scroll-table">
          <table className="table">
            <thead>
              <tr>
                <th>Macro Provider</th>
                <th>Completed</th>
                <th>Failed</th>
                <th>Last Completed</th>
                <th>Last Error</th>
              </tr>
            </thead>
            <tbody>
              {macroSyncHealth.map((row) => (
                <tr key={`${row.workspace_id}:${row.provider_mode}`}>
                  <td>{row.provider_mode}</td>
                  <td>{row.completed_runs}</td>
                  <td>{row.failed_runs}</td>
                  <td>{formatTimestamp(row.last_completed_at)}</td>
                  <td>{formatNullable(row.last_error)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!loading && profiles.length > 0 && (
        <p className="muted">
          Profiles: {profiles.length}. Overrides: {overrides.length}.
        </p>
      )}
    </section>
  );
}
