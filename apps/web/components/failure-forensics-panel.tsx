"use client";

import { formatDurationMs, formatNullable, formatTimestamp, orderStages } from "@/lib/formatters/ops";
import type { RunInputSnapshotRow, RunInspectionRow, RunStageTimingRow } from "@/lib/queries/runs";

interface Props {
  run: RunInspectionRow | null;
  stageTimings: RunStageTimingRow[];
  inputSnapshot: RunInputSnapshotRow | null;
  loading: boolean;
}

export function FailureForensicsPanel({ run, stageTimings, inputSnapshot, loading }: Props) {
  const orderedStageTimings = orderStages(stageTimings);

  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Failure Forensics</h2>
          <p className="panel-subtitle">Stage timings, failure tags, and replay/debug input evidence.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading forensics...</p>}
      {!loading && !run && <p className="muted">Select a run to inspect forensics.</p>}

      {run && (
        <div className="panel-stack">
          <div className="stats-grid">
            <div className="stat-card">
              <div className="kpi-label">Failure Stage</div>
              <div className="kpi-value kpi-value-sm">{formatNullable(run.failure_stage)}</div>
              <div className="kpi-sub">status: {run.status}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Failure Code</div>
              <div className="kpi-value kpi-value-sm">{formatNullable(run.failure_code)}</div>
              <div className="kpi-sub">error: {formatNullable(run.last_error)}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Replay</div>
              <div className="kpi-value kpi-value-sm">{run.is_replay ? "yes" : "no"}</div>
              <div className="kpi-sub">{formatNullable(run.replayed_from_run_id)}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Input Snapshot</div>
              <div className="kpi-value kpi-value-sm">{formatNullable(inputSnapshot?.id ?? run.input_snapshot_id)}</div>
              <div className="kpi-sub">{formatTimestamp(inputSnapshot?.source_window_end ?? null)}</div>
            </div>
          </div>

          <div className="scroll-table">
            <table className="table">
              <thead>
                <tr>
                  <th>Stage</th>
                  <th>Status</th>
                  <th>Started</th>
                  <th>Completed</th>
                  <th>Runtime</th>
                  <th>Failure code</th>
                </tr>
              </thead>
              <tbody>
                {orderedStageTimings.map((stage) => (
                  <tr key={stage.id}>
                    <td>{stage.stage_name}</td>
                    <td>{stage.stage_status}</td>
                    <td>{formatTimestamp(stage.started_at)}</td>
                    <td>{formatTimestamp(stage.completed_at)}</td>
                    <td>{formatDurationMs(stage.runtime_ms)}</td>
                    <td>{formatNullable(stage.failure_code)}</td>
                  </tr>
                ))}
                {orderedStageTimings.length === 0 && (
                  <tr>
                    <td colSpan={6} className="muted">No stage timings are available for this run.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          <div className="detail-grid">
            <div className="json-panel">
              <div className="panel-mini-title">Input Coverage</div>
              <pre>{JSON.stringify(inputSnapshot?.source_coverage ?? {}, null, 2)}</pre>
            </div>
            <div className="json-panel">
              <div className="panel-mini-title">Version Pins</div>
              <pre>{JSON.stringify(inputSnapshot?.version_pins ?? {}, null, 2)}</pre>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
