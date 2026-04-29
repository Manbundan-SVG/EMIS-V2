"use client";

import { ReplayActionButton } from "@/components/replay-action-button";
import { formatDurationMs, formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { JobRun } from "@/lib/queries/jobs";
import type { RunInspectionRow } from "@/lib/queries/runs";

interface Props {
  jobs: JobRun[];
  loading: boolean;
  selectedRunId: string | null;
  onSelectRun: (runId: string) => void;
  run: RunInspectionRow | null;
  onReplayQueued: () => void | Promise<void>;
}

export function RunInspectionPanel({
  jobs,
  loading,
  selectedRunId,
  onSelectRun,
  run,
  onReplayQueued,
}: Props) {
  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Run Inspection</h2>
          <p className="panel-subtitle">Inspect queue context, retry state, and persisted lineage.</p>
        </div>
        {run ? <ReplayActionButton runId={run.run_id} onQueued={onReplayQueued} /> : null}
      </div>

      {loading && <p className="muted">Loading run history...</p>}
      {!loading && jobs.length === 0 && <p className="muted">No runs are available for inspection.</p>}

      {jobs.length > 0 && (
        <div className="panel-stack">
          <div className="scroll-table">
            <table className="table">
              <thead>
                <tr>
                  <th>Run</th>
                  <th>Status</th>
                  <th>Trigger</th>
                  <th>Attempts</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr
                    key={job.id}
                    className={selectedRunId === job.id ? "table-row-active" : undefined}
                    onClick={() => onSelectRun(job.id)}
                  >
                    <td className="mono-cell">{job.id.slice(0, 8)}</td>
                    <td>{job.status}</td>
                    <td>{job.trigger_type}</td>
                    <td>{job.attempt_count}/{job.max_attempts}</td>
                    <td>{formatTimestamp(job.created_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {run ? (
            <div className="panel-stack">
              <div className="stats-grid">
                <div className="stat-card">
                  <div className="kpi-label">Queue</div>
                  <div className="kpi-value kpi-value-sm">{formatNullable(run.queue_id)}</div>
                  <div className="kpi-sub">{run.queue_name}</div>
                </div>
                <div className="stat-card">
                  <div className="kpi-label">Status</div>
                  <div className="kpi-value kpi-value-sm">{run.status}</div>
                  <div className="kpi-sub">{run.trigger_type}{run.is_replay ? " / replay" : ""}</div>
                </div>
                <div className="stat-card">
                  <div className="kpi-label">Runtime</div>
                  <div className="kpi-value kpi-value-sm">{formatDurationMs(run.runtime_ms)}</div>
                  <div className="kpi-sub">alerts: {run.alert_count}</div>
                </div>
                <div className="stat-card">
                  <div className="kpi-label">Retry</div>
                  <div className="kpi-value kpi-value-sm">{run.retry_count ?? 0}</div>
                  <div className="kpi-sub">priority: {formatNullable(run.priority)}</div>
                </div>
              </div>

              <div className="detail-grid">
                <div className="detail-list">
                  <div><span className="muted">Workspace</span> {run.workspace_slug}</div>
                  <div><span className="muted">Watchlist</span> {formatNullable(run.watchlist_slug ?? run.watchlist_name)}</div>
                  <div><span className="muted">Requested by</span> {formatNullable(run.requested_by)}</div>
                  <div><span className="muted">Claimed by</span> {formatNullable(run.claimed_by)}</div>
                  <div><span className="muted">Queued at</span> {formatTimestamp(run.queued_at)}</div>
                  <div><span className="muted">Started at</span> {formatTimestamp(run.started_at)}</div>
                  <div><span className="muted">Completed at</span> {formatTimestamp(run.completed_at)}</div>
                </div>
                <div className="detail-list">
                  <div><span className="muted">Compute version</span> {formatNullable(run.compute_version)}</div>
                  <div><span className="muted">Signal registry</span> {formatNullable(run.signal_registry_version)}</div>
                  <div><span className="muted">Model version</span> {formatNullable(run.model_version)}</div>
                  <div><span className="muted">Failure stage</span> {formatNullable(run.failure_stage)}</div>
                  <div><span className="muted">Failure code</span> {formatNullable(run.failure_code)}</div>
                  <div><span className="muted">Replay source</span> {formatNullable(run.replayed_from_run_id)}</div>
                  <div><span className="muted">Terminal queue state</span> {formatNullable(run.terminal_queue_status)}</div>
                  <div><span className="muted">Terminal promoted at</span> {formatTimestamp(run.terminal_promoted_at)}</div>
                  <div><span className="muted">Last alert</span> {formatTimestamp(run.last_alert_at)}</div>
                  <div><span className="muted">Last error</span> {formatNullable(run.last_error)}</div>
                </div>
              </div>

              <div className="detail-grid">
                <div className="json-panel">
                  <div className="panel-mini-title">Lineage</div>
                  <pre>{JSON.stringify(run.lineage ?? {}, null, 2)}</pre>
                </div>
                <div className="json-panel">
                  <div className="panel-mini-title">Metadata</div>
                  <pre>{JSON.stringify(run.metadata ?? {}, null, 2)}</pre>
                </div>
              </div>
            </div>
          ) : (
            <p className="muted">Select a run to inspect the persisted lifecycle.</p>
          )}
        </div>
      )}
    </section>
  );
}
