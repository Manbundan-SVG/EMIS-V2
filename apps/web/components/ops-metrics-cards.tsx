"use client";

import type { QueueDepthRow, QueueRuntimeRow } from "@/lib/queries/metrics";

interface Props {
  depth: QueueDepthRow[];
  runtime: QueueRuntimeRow[];
  loading: boolean;
}

function sumDepth(rows: QueueDepthRow[], key: keyof QueueDepthRow): number {
  return rows.reduce((acc, r) => acc + (r[key] as number), 0);
}

export function OpsMetricsCards({ depth, runtime, loading }: Props) {
  const totalQueued = sumDepth(depth, "queued_count");
  const totalClaimed = sumDepth(depth, "claimed_count");
  const totalFailed = sumDepth(depth, "failed_count");
  const totalDead = sumDepth(depth, "dead_letter_count");

  const completedRuns = runtime.reduce((a, r) => a + r.completed_runs, 0);
  const avgRuntime =
    runtime.length === 0
      ? null
      : runtime.reduce((a, r) => a + (r.avg_runtime_seconds ?? 0), 0) / runtime.length;

  return (
    <div className="ops-grid">
      <div className="card">
        <div className="kpi-label">Queued</div>
        <div className="kpi-value">{loading ? "—" : totalQueued}</div>
        <div className="kpi-sub">waiting to be claimed</div>
      </div>

      <div className="card">
        <div className="kpi-label">Claimed</div>
        <div className="kpi-value">{loading ? "—" : totalClaimed}</div>
        <div className="kpi-sub">in-flight jobs</div>
      </div>

      <div className="card">
        <div className="kpi-label">Failed / Dead</div>
        <div className={`kpi-value${totalFailed + totalDead > 0 ? " signal-negative" : ""}`}>
          {loading ? "—" : `${totalFailed} / ${totalDead}`}
        </div>
        <div className="kpi-sub">failed jobs / dead letters</div>
      </div>

      <div className="card">
        <div className="kpi-label">Avg Runtime</div>
        <div className="kpi-value">
          {loading || avgRuntime === null ? "—" : `${avgRuntime.toFixed(1)}s`}
        </div>
        <div className="kpi-sub text-success">
          {loading ? "" : `${completedRuns} completed`}
        </div>
      </div>
    </div>
  );
}
