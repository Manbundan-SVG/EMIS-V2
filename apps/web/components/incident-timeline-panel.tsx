import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { GovernanceIncidentDetail } from "@emis-types/ops";
import { GovernanceCaseEvidencePanel } from "./governance-case-evidence-panel";
import { GovernanceCaseNotesPanel } from "./governance-case-notes-panel";
import { GovernanceCaseSummaryPanel } from "./governance-case-summary-panel";

type Props = {
  incident: GovernanceIncidentDetail | null;
  loading: boolean;
};

export function IncidentTimelinePanel({ incident, loading }: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Incident Timeline</h2>
          <p className="panel-subtitle">Ordered incident history, linked evidence, and current investigation context.</p>
        </div>
      </div>
      {loading && <p className="muted">Loading incident timeline...</p>}
      {!loading && !incident && <p className="muted">Select a governance case to inspect its incident timeline.</p>}
      {!loading && incident && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card"><div className="kpi-label">Status</div><div className="kpi-value">{incident.status}</div></div>
            <div className="kpi-card"><div className="kpi-label">Severity</div><div className="kpi-value">{incident.severity}</div></div>
            <div className="kpi-card"><div className="kpi-label">Timeline events</div><div className="kpi-value">{incident.timeline_event_count}</div></div>
            <div className="kpi-card"><div className="kpi-label">Evidence links</div><div className="kpi-value">{incident.evidence_count}</div></div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>{incident.title}</h3>
                  <p className="panel-subtitle">{formatNullable(incident.summary)}</p>
                </div>
              </div>
              <table className="table">
                <tbody>
                  <tr><th>Watchlist</th><td>{formatNullable(incident.watchlist_name ?? incident.watchlist_slug)}</td></tr>
                  <tr><th>Owner</th><td>{formatNullable(incident.current_assignee ?? incident.current_team)}</td></tr>
                  <tr><th>Opened</th><td>{formatTimestamp(incident.opened_at)}</td></tr>
                  <tr><th>Latest event</th><td>{formatNullable(incident.latest_event_type)} / {formatTimestamp(incident.latest_event_at)}</td></tr>
                </tbody>
              </table>
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Investigation Context</h3>
                  <p className="panel-subtitle">Current ownership and operator-facing case context.</p>
                </div>
              </div>
              <table className="table">
                <tbody>
                  <tr><th>Last operator summary</th><td>{formatNullable(incident.investigation_summary.last_operator_summary?.note ?? null)}</td></tr>
                  <tr><th>Latest handoff</th><td>{formatNullable(incident.investigation_summary.latest_handoff_note?.note ?? null)}</td></tr>
                  <tr><th>Latest root cause</th><td>{formatNullable(incident.investigation_summary.latest_root_cause_note?.note ?? null)}</td></tr>
                  <tr><th>Latest closure</th><td>{formatNullable(incident.investigation_summary.latest_closure_note?.note ?? null)}</td></tr>
                </tbody>
              </table>
            </div>
          </div>

          <div className="ops-phase24-grid">
            <GovernanceCaseSummaryPanel summary={incident.generated_summary} />

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Recurrence Summary</h3>
                  <p className="panel-subtitle">Reopen linkage and related prior incidents.</p>
                </div>
              </div>
              <table className="table">
                <tbody>
                  <tr><th>Repeat count</th><td>{incident.recurrence.repeatCount}</td></tr>
                  <tr><th>Reopened</th><td>{incident.recurrence.isReopened ? "Yes" : "No"}</td></tr>
                  <tr><th>Recurring</th><td>{incident.recurrence.isRecurring ? "Yes" : "No"}</td></tr>
                  <tr><th>Reopened from</th><td>{formatNullable(incident.recurrence.reopenedFromCaseId)}</td></tr>
                  <tr><th>Latest similar case</th><td>{formatNullable(incident.recurrence.latestPriorCaseId)}</td></tr>
                  <tr><th>Latest prior closed</th><td>{formatTimestamp(incident.recurrence.latestPriorClosedAt)}</td></tr>
                  <tr><th>Reopen reason</th><td>{formatNullable(incident.recurrence.reopenReason)}</td></tr>
                </tbody>
              </table>
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Related Prior Cases</h3>
                  <p className="panel-subtitle">Recent cases in the same recurrence group.</p>
                </div>
              </div>
              {incident.recurrence.relatedCases.length === 0 ? (
                <p className="muted">No prior related cases linked.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Case</th>
                      <th>Status</th>
                      <th>Opened</th>
                      <th>Closed</th>
                    </tr>
                  </thead>
                  <tbody>
                    {incident.recurrence.relatedCases.map((row) => (
                      <tr key={row.id}>
                        <td>
                          <div>{row.title}</div>
                          <div className="muted">{row.id}</div>
                        </td>
                        <td>{row.status} / repeat {row.repeat_count}</td>
                        <td>{formatTimestamp(row.opened_at)}</td>
                        <td>{formatTimestamp(row.closed_at ?? row.resolved_at)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>

          <div className="ops-phase24-grid">
            <GovernanceCaseNotesPanel notes={incident.notes} summary={incident.investigation_summary} />

            <GovernanceCaseEvidencePanel evidence={incident.evidence} summary={incident.evidence_summary} />
          </div>

          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Timeline Events</h3>
                <p className="panel-subtitle">Chronological case activity and automated lifecycle transitions.</p>
              </div>
            </div>
            {incident.timeline.length === 0 ? (
              <p className="muted">No timeline events recorded.</p>
            ) : (
              <div className="space-y-3">
                {incident.timeline.map((event) => (
                  <div key={`${event.id}-${event.event_at}`} className="rounded-lg border p-3">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <div>{event.title}</div>
                        <div className="muted">{event.event_type} / {event.event_source}</div>
                      </div>
                      <div className="muted">{formatTimestamp(event.event_at)}</div>
                    </div>
                    {(event.actor || event.detail) && (
                      <div className="mt-2">
                        {event.actor ? <div className="muted">Actor: {event.actor}</div> : null}
                        {event.detail ? <div>{event.detail}</div> : null}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        </>
      )}
    </section>
  );
}
