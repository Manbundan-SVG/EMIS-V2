import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { GovernanceCaseGeneratedSummary } from "@emis-types/ops";

type Props = {
  summary: GovernanceCaseGeneratedSummary | null;
};

export function GovernanceCaseSummaryPanel({ summary }: Props) {
  return (
    <div className="panel">
      <div className="panel-header">
        <div>
          <h3>Case Summary</h3>
          <p className="panel-subtitle">Persisted incident summary, likely root cause, and recommended next action.</p>
        </div>
      </div>
      {!summary ? (
        <p className="muted">No persisted case summary is available yet.</p>
      ) : (
        <>
          <table className="table">
            <tbody>
              <tr><th>Status summary</th><td>{formatNullable(summary.status_summary)}</td></tr>
              <tr><th>Root cause code</th><td>{formatNullable(summary.root_cause_code)}</td></tr>
              <tr><th>Root cause confidence</th><td>{summary.root_cause_confidence ?? "None"}</td></tr>
              <tr><th>Recommended next action</th><td>{formatNullable(summary.recommended_next_action)}</td></tr>
              <tr><th>Generated</th><td>{formatTimestamp(summary.generated_at)}</td></tr>
            </tbody>
          </table>

          <div className="space-y-3">
            <div className="rounded-lg border p-3">
              <div>Root cause summary</div>
              <div className="muted">{formatNullable(summary.root_cause_summary)}</div>
            </div>
            <div className="rounded-lg border p-3">
              <div>Evidence summary</div>
              <div className="muted">{formatNullable(summary.evidence_summary)}</div>
            </div>
            <div className="rounded-lg border p-3">
              <div>Recurrence summary</div>
              <div className="muted">{formatNullable(summary.recurrence_summary)}</div>
            </div>
            <div className="rounded-lg border p-3">
              <div>Operator summary</div>
              <div className="muted">{formatNullable(summary.operator_summary)}</div>
            </div>
            <div className="rounded-lg border p-3">
              <div>Closure summary</div>
              <div className="muted">{formatNullable(summary.closure_summary)}</div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
