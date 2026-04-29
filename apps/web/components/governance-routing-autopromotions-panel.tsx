"use client";

import { useEffect, useState } from "react";
import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type {
  GovernanceRoutingAutopromotionPolicyRow,
  GovernanceRoutingAutopromotionRollbackCandidateRow,
  GovernanceRoutingAutopromotionSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  workspace: string;
};

export function GovernanceRoutingAutopromotionsPanel({ workspace }: Props) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activePolicies, setActivePolicies] = useState<GovernanceRoutingAutopromotionPolicyRow[]>([]);
  const [recentExecutions, setRecentExecutions] = useState<GovernanceRoutingAutopromotionSummaryRow[]>([]);
  const [rollbackCandidates, setRollbackCandidates] = useState<GovernanceRoutingAutopromotionRollbackCandidateRow[]>([]);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      setError(null);
      try {
        const res = await fetch(`/api/governance/routing-autopromotions?workspace=${workspace}`, {
          cache: "no-store",
        });
        const data = await res.json() as {
          ok: boolean;
          error?: string;
          activePolicies?: GovernanceRoutingAutopromotionPolicyRow[];
          recentExecutions?: GovernanceRoutingAutopromotionSummaryRow[];
          rollbackCandidates?: GovernanceRoutingAutopromotionRollbackCandidateRow[];
        };
        if (!res.ok || !data.ok) {
          throw new Error(data.error ?? "routing autopromotions unavailable");
        }
        if (!cancelled) {
          setActivePolicies(data.activePolicies ?? []);
          setRecentExecutions(data.recentExecutions ?? []);
          setRollbackCandidates(data.rollbackCandidates ?? []);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "routing autopromotions unavailable");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void load();
    return () => {
      cancelled = true;
    };
  }, [workspace]);

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Routing Autopromotions</h2>
          <p className="panel-subtitle">
            Policy-gated routing promotions, recent executions, and rollback candidates for guarded routing automation.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading routing autopromotions...</p>}
      {error && !loading && <p className="muted">Routing autopromotions unavailable: {error}</p>}

      {!loading && !error && (
        <div className="panel" style={{ display: "grid", gap: 16 }}>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 12 }}>
            <div>
              <div className="muted">Active policies</div>
              <div>{activePolicies.length}</div>
            </div>
            <div>
              <div className="muted">Recent executions</div>
              <div>{recentExecutions.length}</div>
            </div>
            <div>
              <div className="muted">Rollback candidates</div>
              <div>{rollbackCandidates.length}</div>
            </div>
          </div>

          <table className="table">
            <thead>
              <tr>
                <th>Scope</th>
                <th>Target</th>
                <th>Guardrails</th>
                <th>Latest execution</th>
              </tr>
            </thead>
            <tbody>
              {activePolicies.length === 0 ? (
                <tr>
                  <td colSpan={4} className="muted">No active routing autopromotion policies.</td>
                </tr>
              ) : (
                activePolicies.slice(0, 12).map((row) => {
                  const latest = recentExecutions.find((execution) => execution.policy_id === row.id);
                  return (
                    <tr key={row.id}>
                      <td>
                        <div>{row.scope_type}</div>
                        <div className="muted">{formatNullable(row.scope_value)}</div>
                      </td>
                      <td>
                        <div>{row.promotion_target}</div>
                        <div className="muted">{row.min_confidence} confidence</div>
                      </td>
                      <td>
                        <div>Acceptance {row.min_acceptance_rate.toFixed(2)}</div>
                        <div className="muted">
                          sample {row.min_sample_size} · override {row.max_recent_override_rate.toFixed(2)} · cooldown {row.cooldown_hours}h
                        </div>
                      </td>
                      <td>
                        <div>{latest?.execution_status ?? "none"}</div>
                        <div className="muted">{formatTimestamp(latest?.created_at ?? null)}</div>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>

          <table className="table">
            <thead>
              <tr>
                <th>Execution</th>
                <th>Recommendation</th>
                <th>Guardrail metrics</th>
                <th>Rollback</th>
              </tr>
            </thead>
            <tbody>
              {recentExecutions.length === 0 ? (
                <tr>
                  <td colSpan={4} className="muted">No routing autopromotion executions yet.</td>
                </tr>
              ) : (
                recentExecutions.slice(0, 12).map((row) => (
                  <tr key={row.execution_id}>
                    <td>
                      <div>{row.target_type} · {row.target_key}</div>
                      <div className="muted">{row.execution_status} · {formatTimestamp(row.created_at)}</div>
                    </td>
                    <td>
                      <div>{formatNullable(row.recommended_user ?? row.recommended_team ?? null)}</div>
                      <div className="muted">{row.confidence} · {formatNullable(row.execution_reason)}</div>
                    </td>
                    <td>
                      <div>accept {row.acceptance_rate ?? 0}</div>
                      <div className="muted">sample {row.sample_size ?? 0} · override {row.override_rate ?? 0}</div>
                    </td>
                    <td>
                      <div>{row.rollback_candidate_id ? "candidate" : "none"}</div>
                      <div className="muted">{row.rollback_reason ?? "n/a"}</div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
