import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { GovernanceCaseInvestigationSummary, GovernanceCaseNote } from "@emis-types/ops";

type Props = {
  notes: GovernanceCaseNote[];
  summary: GovernanceCaseInvestigationSummary;
};

function renderSummaryValue(note: GovernanceCaseNote | null) {
  if (!note) return <span className="muted">None</span>;
  return (
    <div>
      <div>{note.note}</div>
      <div className="muted">
        {formatNullable(note.author)} / {formatTimestamp(note.created_at)}
      </div>
    </div>
  );
}

export function GovernanceCaseNotesPanel({ notes, summary }: Props) {
  return (
    <div className="panel">
      <div className="panel-header">
        <div>
          <h3>Operator Notes</h3>
          <p className="panel-subtitle">Structured investigation, handoff, root-cause, and closure context.</p>
        </div>
      </div>

      <table className="table">
        <tbody>
          <tr><th>Last operator summary</th><td>{renderSummaryValue(summary.last_operator_summary)}</td></tr>
          <tr><th>Latest handoff</th><td>{renderSummaryValue(summary.latest_handoff_note)}</td></tr>
          <tr><th>Latest root cause</th><td>{renderSummaryValue(summary.latest_root_cause_note)}</td></tr>
          <tr><th>Latest closure</th><td>{renderSummaryValue(summary.latest_closure_note)}</td></tr>
        </tbody>
      </table>

      {notes.length === 0 ? (
        <p className="muted">No structured notes recorded yet.</p>
      ) : (
        <div className="space-y-3">
          {notes.map((note) => (
            <div key={note.id} className="rounded-lg border p-3">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div>{note.note_type.replace(/_/g, " ")}</div>
                  <div className="muted">
                    {formatNullable(note.author)} / {note.visibility}
                  </div>
                </div>
                <div className="muted">{formatTimestamp(note.created_at)}</div>
              </div>
              <div className="mt-2">{note.note}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
