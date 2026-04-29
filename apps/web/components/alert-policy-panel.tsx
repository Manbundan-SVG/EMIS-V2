"use client";

import type { AlertPolicyRuleRow } from "@/lib/queries/governance";

interface Props {
  rules: AlertPolicyRuleRow[];
  loading: boolean;
}

export function AlertPolicyPanel({ rules, loading }: Props) {
  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Alert Policy</h2>
          <p className="panel-subtitle">Cooldown and routing rules that gate terminal alert fanout.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading alert policies...</p>}
      {!loading && rules.length === 0 && (
        <p className="muted">No alert policy rules configured; worker fallback alerts remain active.</p>
      )}

      {rules.length > 0 && (
        <div className="scroll-table">
          <table className="table">
            <thead>
              <tr>
                <th>Watchlist</th>
                <th>Event</th>
                <th>Severity</th>
                <th>Channel</th>
                <th>Cooldown</th>
                <th>Terminal only</th>
              </tr>
            </thead>
            <tbody>
              {rules.map((rule) => (
                <tr key={rule.id}>
                  <td>{rule.watchlist_id ?? "workspace"}</td>
                  <td>{rule.event_type}</td>
                  <td>{rule.severity}</td>
                  <td>{rule.channel}</td>
                  <td>{rule.cooldown_seconds}s</td>
                  <td>{rule.notify_on_terminal_only ? "yes" : "no"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
