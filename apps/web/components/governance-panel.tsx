"use client";

import type { QueueGovernanceRuleRow } from "@/lib/queries/governance";
import type { QueueGovernanceStateRow } from "@/lib/queries/metrics";

interface Props {
  state: QueueGovernanceStateRow[];
  rules: QueueGovernanceRuleRow[];
  loading: boolean;
}

function formatDateTime(value: string | null): string {
  return value ? new Date(value).toLocaleString() : "-";
}

export function GovernancePanel({ state, rules, loading }: Props) {
  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Queue Governance</h2>
          <p className="panel-subtitle">Live queue pressure plus the active suppression and priority rules.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading queue governance...</p>}

      <div className="panel-stack">
        <div>
          <div className="panel-mini-title">Current State</div>
          {state.length === 0 ? (
            <p className="muted">No queued or claimed jobs are currently visible.</p>
          ) : (
            <div className="scroll-table">
              <table className="table">
                <thead>
                  <tr>
                    <th>Watchlist</th>
                    <th>Job type</th>
                    <th>Queued</th>
                    <th>Claimed</th>
                    <th>Oldest queued</th>
                    <th>Highest priority</th>
                  </tr>
                </thead>
                <tbody>
                  {state.map((row) => (
                    <tr key={`${row.workspace_id}:${row.watchlist_id ?? "all"}:${row.job_type}`}>
                      <td>{row.watchlist_name ?? row.watchlist_slug ?? "workspace"}</td>
                      <td>{row.job_type}</td>
                      <td>{row.queued_count}</td>
                      <td>{row.claimed_count}</td>
                      <td>{formatDateTime(row.oldest_queued_at)}</td>
                      <td>{row.highest_priority_queued ?? "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <div>
          <div className="panel-mini-title">Rules</div>
          {rules.length === 0 ? (
            <p className="muted">No queue governance rules configured; the default scheduler behavior applies.</p>
          ) : (
            <div className="scroll-table">
              <table className="table">
                <thead>
                  <tr>
                    <th>Watchlist</th>
                    <th>Job type</th>
                    <th>Concurrent</th>
                    <th>Dedupe</th>
                    <th>Manual prio</th>
                    <th>Scheduled prio</th>
                  </tr>
                </thead>
                <tbody>
                  {rules.map((rule) => (
                    <tr key={rule.id}>
                      <td>{rule.watchlist_id ?? "workspace"}</td>
                      <td>{rule.job_type}</td>
                      <td>{rule.max_concurrent}</td>
                      <td>{rule.dedupe_window_seconds}s</td>
                      <td>{rule.manual_priority}</td>
                      <td>{rule.scheduled_priority}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
