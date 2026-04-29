"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { useDashboardRealtime } from "@/hooks/use-dashboard-realtime";
import { AlertsPanel } from "@/components/alerts-panel";
import type { AlertEventRow } from "@/lib/queries/alerts";

type JobRun = {
  id: string; status: string; trigger_type: string;
  attempt_count: number; error_message: string | null;
  created_at: string; updated_at: string; workspace_id: string;
};
type CompositeRow = {
  id: string; workspace_id: string; timestamp: string; regime: string;
  long_score: number; short_score: number; confidence: number;
  assets: { symbol: string; name: string; asset_class: string };
};

export default function DashboardShell({ workspaceSlug = "demo" }: { workspaceSlug?: string }) {
  const [jobs, setJobs] = useState<JobRun[]>([]);
  const [composites, setComposites] = useState<CompositeRow[]>([]);
  const [alerts, setAlerts] = useState<AlertEventRow[]>([]);
  const workspaceId = useMemo(
    () => jobs[0]?.workspace_id ?? composites[0]?.workspace_id ?? null,
    [jobs, composites],
  );

  const loadJobs = useCallback(async () => {
    const res = await fetch(`/api/jobs?workspaceSlug=${workspaceSlug}`, { cache: "no-store" });
    const payload = await res.json();
    if (payload.ok) setJobs(payload.jobs);
  }, [workspaceSlug]);

  const loadComposites = useCallback(async () => {
    const res = await fetch(`/api/composites?workspaceSlug=${workspaceSlug}`, { cache: "no-store" });
    const payload = await res.json();
    if (payload.ok) setComposites(payload.composites);
  }, [workspaceSlug]);

  const loadAlerts = useCallback(async () => {
    const res = await fetch(`/api/alerts?workspaceSlug=${workspaceSlug}`, { cache: "no-store" });
    const payload = await res.json();
    if (payload.ok) setAlerts(payload.alerts);
  }, [workspaceSlug]);

  useEffect(() => {
    void loadJobs();
    void loadComposites();
    void loadAlerts();
  }, [loadJobs, loadComposites, loadAlerts]);

  useDashboardRealtime({
    workspaceId,
    onJobChange: () => { void loadJobs(); },
    onCompositeChange: () => { void loadComposites(); },
    onAlert: () => { void loadAlerts(); },
  });

  async function triggerRecompute() {
    await fetch("/api/recompute", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ workspaceSlug, requestedBy: "dashboard-button" }),
    });
    setTimeout(() => { void loadJobs(); }, 500);
  }

  return (
    <main className="container">
      <div className="header">
        <h1>EMIS Dashboard</h1>
        <button type="button" className="btn" onClick={triggerRecompute}>
          Trigger recompute
        </button>
      </div>

      <div className="section card">
        <h2 className="section-title">Jobs</h2>
        <table className="table">
          <thead>
            <tr>
              <th>Status</th><th>Trigger</th><th>Attempts</th>
              <th>Created</th><th>Error</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => (
              <tr key={job.id}>
                <td>{job.status}</td>
                <td>{job.trigger_type}</td>
                <td>{job.attempt_count}</td>
                <td>{new Date(job.created_at).toLocaleString()}</td>
                <td className="muted">{job.error_message ?? "—"}</td>
              </tr>
            ))}
            {jobs.length === 0 && (
              <tr><td colSpan={5} className="muted">No jobs yet.</td></tr>
            )}
          </tbody>
        </table>
      </div>

      <div className="section card">
        <h2 className="section-title">Composite scores</h2>
        <table className="table">
          <thead>
            <tr>
              <th>Asset</th><th>Regime</th><th>Long</th>
              <th>Short</th><th>Confidence</th><th>Timestamp</th>
            </tr>
          </thead>
          <tbody>
            {composites.map((row) => (
              <tr key={row.id}>
                <td>{row.assets.symbol}</td>
                <td>{row.regime}</td>
                <td className="signal-positive">{row.long_score.toFixed(3)}</td>
                <td className="signal-negative">{row.short_score.toFixed(3)}</td>
                <td>{row.confidence.toFixed(3)}</td>
                <td className="muted">{new Date(row.timestamp).toLocaleString()}</td>
              </tr>
            ))}
            {composites.length === 0 && (
              <tr><td colSpan={6} className="muted">No composites yet.</td></tr>
            )}
          </tbody>
        </table>
      </div>

      <AlertsPanel alerts={alerts} />
    </main>
  );
}
