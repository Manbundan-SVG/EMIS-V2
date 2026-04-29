import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { GovernanceCaseSummaryRow } from "@/lib/queries/metrics";

type Props = {
  activeCases: GovernanceCaseSummaryRow[];
  recentCases: GovernanceCaseSummaryRow[];
  selectedCaseId: string | null;
  onSelectCase: (caseId: string) => void;
  loading: boolean;
};

export function GovernanceCasePanel({ activeCases, recentCases, selectedCaseId, onSelectCase, loading }: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Governance Cases</h2>
          <p className="panel-subtitle">Operator-facing case tracking built on top of degradation states.</p>
        </div>
      </div>
      {loading && <p className="muted">Loading governance cases...</p>}
      {!loading && (
        <>
          <div className="kpi-grid">
            <div className="kpi-card"><div className="kpi-label">Active cases</div><div className="kpi-value">{activeCases.length}</div></div>
            <div className="kpi-card"><div className="kpi-label">Recent cases</div><div className="kpi-value">{recentCases.length}</div></div>
          </div>

          <div className="ops-phase24-grid">
            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Active Cases</h3>
                  <p className="panel-subtitle">Open, acknowledged, and in-progress governance cases.</p>
                </div>
              </div>
              {activeCases.length === 0 ? (
                <p className="muted">No active governance cases.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Title</th>
                      <th>Status</th>
                      <th>Owner</th>
                      <th>Opened</th>
                      <th>Events</th>
                    </tr>
                  </thead>
                  <tbody>
                    {activeCases.slice(0, 10).map((row) => (
                      <tr
                        key={row.id}
                        className={selectedCaseId === row.id ? "bg-slate-50" : undefined}
                        onClick={() => onSelectCase(row.id)}
                        style={{ cursor: "pointer" }}
                      >
                        <td>
                          <div>{row.title}</div>
                          <div className="muted">
                            {formatNullable(row.summary)}
                            {row.is_reopened ? " / reopened" : row.is_recurring ? " / recurring" : ""}
                            {row.repeat_count > 1 ? ` / repeat ${row.repeat_count}` : ""}
                          </div>
                        </td>
                        <td>
                          <div>{row.status}</div>
                          <div className={`muted severity-${row.severity}`}>{row.severity}</div>
                        </td>
                        <td>{formatNullable(row.current_assignee ?? row.current_team)}</td>
                        <td>{formatTimestamp(row.opened_at)}</td>
                        <td>{row.event_count}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>

            <div className="panel">
              <div className="panel-header">
                <div>
                  <h3>Recent Case Activity</h3>
                  <p className="panel-subtitle">Latest lifecycle and assignment activity across governance cases.</p>
                </div>
              </div>
              {recentCases.length === 0 ? (
                <p className="muted">No governance cases recorded yet.</p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Case</th>
                      <th>Last event</th>
                      <th>Notes</th>
                      <th>Evidence</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recentCases.slice(0, 10).map((row) => (
                      <tr
                        key={row.id}
                        className={selectedCaseId === row.id ? "bg-slate-50" : undefined}
                        onClick={() => onSelectCase(row.id)}
                        style={{ cursor: "pointer" }}
                      >
                        <td>
                          <div>{row.title}</div>
                          <div className="muted">
                            {row.status} / {row.severity}
                            {row.is_reopened ? " / reopened" : row.is_recurring ? " / recurring" : ""}
                            {row.repeat_count > 1 ? ` / repeat ${row.repeat_count}` : ""}
                          </div>
                        </td>
                        <td>
                          <div>{formatNullable(row.last_event_type)}</div>
                          <div className="muted">{formatTimestamp(row.last_event_at)}</div>
                        </td>
                        <td>{row.note_count}</td>
                        <td>{row.evidence_count}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </>
      )}
    </section>
  );
}
