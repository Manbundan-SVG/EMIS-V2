"use client";

import type { RunExplanationRow } from "@/lib/queries/runs";

interface Props {
  explanation: RunExplanationRow | null;
  loading: boolean;
}

function entries(record: Record<string, unknown> | undefined): Array<[string, unknown]> {
  return Object.entries(record ?? {});
}

export function ExplanationPanel({ explanation, loading }: Props) {
  const regimeCounts = entries((explanation?.regime_summary?.regime_counts ?? {}) as Record<string, unknown>);
  const topPositive = Array.isArray(explanation?.top_positive_contributors) ? explanation.top_positive_contributors : [];
  const topNegative = Array.isArray(explanation?.top_negative_contributors) ? explanation.top_negative_contributors : [];

  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Run Explanation</h2>
          <p className="panel-subtitle">Structured contributors, regimes, and composite context for the selected run.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading explanation...</p>}
      {!loading && !explanation && <p className="muted">No explanation has been persisted for this run yet.</p>}

      {explanation && (
        <div className="panel-stack">
          <div className="detail-list">
            <div><span className="muted">Version</span> {explanation.explanation_version}</div>
            <div><span className="muted">Summary</span> {explanation.summary ?? "-"}</div>
          </div>

          <div className="pill-row">
            {regimeCounts.length > 0 ? regimeCounts.map(([regime, count]) => (
              <span key={regime} className="badge">{regime}: {String(count)}</span>
            )) : <span className="muted">No regime counts available.</span>}
          </div>

          <div className="detail-grid">
            <div className="json-panel">
              <div className="panel-mini-title">Top Positive Contributors</div>
              <pre>{JSON.stringify(topPositive, null, 2)}</pre>
            </div>
            <div className="json-panel">
              <div className="panel-mini-title">Top Negative Contributors</div>
              <pre>{JSON.stringify(topNegative, null, 2)}</pre>
            </div>
          </div>

          <div className="detail-grid">
            <div className="json-panel">
              <div className="panel-mini-title">Signal Summary</div>
              <pre>{JSON.stringify(explanation.signal_summary ?? {}, null, 2)}</pre>
            </div>
            <div className="json-panel">
              <div className="panel-mini-title">Composite Summary</div>
              <pre>{JSON.stringify(explanation.composite_summary ?? {}, null, 2)}</pre>
            </div>
          </div>

          <div className="json-panel">
            <div className="panel-mini-title">Invalidators</div>
            <pre>{JSON.stringify(explanation.invalidator_summary ?? {}, null, 2)}</pre>
          </div>
        </div>
      )}
    </section>
  );
}
