"use client";

import type { DeadLetterRow } from "@/lib/queries/dead_letters";

interface Props {
  rows: DeadLetterRow[];
  loading: boolean;
  onRequeue: (id: number, reset: boolean) => void;
}

export function DeadLetterPanel({ rows, loading, onRequeue }: Props) {
  return (
    <div className="card">
      <h2 className="section-title">Dead Letters</h2>

      {loading && <p className="muted">Loading…</p>}
      {!loading && rows.length === 0 && (
        <p className="muted">No dead letters — all jobs resolving cleanly.</p>
      )}

      {rows.map((row) => (
        <div key={row.id} className="worker-row">
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 12 }}>
            <div style={{ flex: 1, minWidth: 0 }}>
              <span className="badge">{row.job_type}</span>
              {" "}
              <span className="severity-high">
                {row.retry_count}/{row.max_retries} retries
              </span>
              {row.failure_stage && (
                <span className="muted" style={{ marginLeft: 8, fontSize: 12 }}>
                  [{row.failure_stage}]
                </span>
              )}
              <div className="muted" style={{ fontSize: 12, marginTop: 4 }}>
                Failed: {new Date(row.failed_at).toLocaleString()}
                {row.requeued_at && (
                  <span className="text-success" style={{ marginLeft: 8 }}>
                    Requeued: {new Date(row.requeued_at).toLocaleString()}
                  </span>
                )}
              </div>
            </div>
            {!row.requeued_at && (
              <button
                type="button"
                className="btn btn-sm"
                onClick={() => onRequeue(row.id, false)}
              >
                Requeue
              </button>
            )}
          </div>

          {row.last_error && (
            <div className="code-block">{row.last_error}</div>
          )}
        </div>
      ))}
    </div>
  );
}
