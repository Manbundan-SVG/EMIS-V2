import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { GovernanceCaseEvidence, GovernanceCaseEvidenceSummary } from "@emis-types/ops";

type Props = {
  evidence: GovernanceCaseEvidence[];
  summary: GovernanceCaseEvidenceSummary | null;
};

function formatEvidenceTypeCounts(summary: GovernanceCaseEvidenceSummary | null): string {
  if (!summary) return "None";
  const entries = Object.entries(summary.evidence_type_counts ?? {});
  if (entries.length === 0) return "None";
  return entries.map(([key, value]) => `${key}: ${value}`).join(" / ");
}

export function GovernanceCaseEvidencePanel({ evidence, summary }: Props) {
  return (
    <div className="panel">
      <div className="panel-header">
        <div>
          <h3>Linked Evidence</h3>
          <p className="panel-subtitle">Typed artifacts attached to the case with quick investigation context.</p>
        </div>
      </div>

      <table className="table">
        <tbody>
          <tr><th>Total evidence</th><td>{summary?.evidence_count ?? evidence.length}</td></tr>
          <tr><th>Type mix</th><td>{formatEvidenceTypeCounts(summary)}</td></tr>
          <tr><th>Latest run</th><td>{formatNullable(summary?.latest_run_id ?? null)}</td></tr>
          <tr><th>Latest replay delta</th><td>{formatNullable(summary?.latest_replay_delta_id ?? null)}</td></tr>
          <tr><th>Latest regime transition</th><td>{formatNullable(summary?.latest_regime_transition_id ?? null)}</td></tr>
          <tr><th>Latest threshold application</th><td>{formatNullable(summary?.latest_threshold_application_id ?? null)}</td></tr>
        </tbody>
      </table>

      {evidence.length === 0 ? (
        <p className="muted">No linked evidence recorded.</p>
      ) : (
        <div className="space-y-3">
          {evidence.slice(0, 12).map((item) => (
            <div key={item.id} className="rounded-lg border p-3">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div>{item.title ?? item.evidence_type}</div>
                  <div className="muted">
                    {item.evidence_type} / {item.reference_id}
                  </div>
                </div>
                <div className="muted">{formatTimestamp(item.created_at)}</div>
              </div>
              {item.summary ? <div className="mt-2">{item.summary}</div> : null}
              {!item.summary && item.payload && Object.keys(item.payload).length > 0 ? (
                <div className="mt-2 muted">{JSON.stringify(item.payload)}</div>
              ) : null}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
