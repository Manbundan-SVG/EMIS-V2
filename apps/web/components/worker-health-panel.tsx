"use client";

import type { WorkerHeartbeatRow } from "@/lib/queries/metrics";

interface Props {
  workers: WorkerHeartbeatRow[];
  loading: boolean;
}

function isAlive(lastSeenAt: string): boolean {
  return Date.now() - new Date(lastSeenAt).getTime() < 90_000;
}

export function WorkerHealthPanel({ workers, loading }: Props) {
  return (
    <div className="card">
      <h2 className="section-title">Workers</h2>

      {loading && <p className="muted">Loading…</p>}
      {!loading && workers.length === 0 && (
        <p className="muted">No workers have checked in yet.</p>
      )}

      {workers.map((w) => {
        const alive = isAlive(w.last_seen_at);
        return (
          <div key={w.worker_id} className="worker-row">
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div>
                <span style={{ fontWeight: 600, fontSize: 14 }}>{w.worker_id}</span>
                {w.hostname && (
                  <span className="muted" style={{ marginLeft: 8, fontSize: 12 }}>
                    {w.hostname}{w.pid != null ? `:${w.pid}` : ""}
                  </span>
                )}
              </div>
              <span className={alive ? "badge-alive" : "badge-stale"}>
                {alive ? "alive" : "stale"}
              </span>
            </div>
            <div className="muted" style={{ fontSize: 12, marginTop: 4 }}>
              Last seen: {new Date(w.last_seen_at).toLocaleString()}
            </div>
          </div>
        );
      })}
    </div>
  );
}
