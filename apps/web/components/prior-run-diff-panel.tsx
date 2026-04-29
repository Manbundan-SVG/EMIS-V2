"use client";

import type { RunPriorComparisonRow } from "@/lib/queries/runs";

interface Props {
  comparison: RunPriorComparisonRow | null;
  loading: boolean;
}

function diffCount(value: Record<string, unknown> | null | undefined): number {
  return Object.keys(value ?? {}).length;
}

export function PriorRunDiffPanel({ comparison, loading }: Props) {
  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Prior-Run Diff</h2>
          <p className="panel-subtitle">What changed relative to the prior successful run for this queue context.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading prior-run comparison...</p>}
      {!loading && !comparison && <p className="muted">No prior-run comparison is available for this run yet.</p>}

      {comparison && (
        <div className="panel-stack">
          <div className="detail-list">
            <div><span className="muted">Prior successful run</span> {comparison.prior_run_id ?? "-"}</div>
            <div><span className="muted">Current summary</span> {comparison.current_summary ?? "-"}</div>
            <div><span className="muted">Prior summary</span> {comparison.prior_summary ?? "-"}</div>
          </div>

          <div className="pill-row">
            <span className="badge">regime changes: {diffCount(comparison.regime_changes)}</span>
            <span className="badge">signal changes: {diffCount(comparison.signal_changes)}</span>
            <span className="badge">composite changes: {diffCount(comparison.composite_changes)}</span>
            <span className="badge">invalidator changes: {diffCount(comparison.invalidator_changes)}</span>
            <span className="badge">input coverage changes: {diffCount(comparison.input_coverage_changes)}</span>
          </div>

          <div className="detail-grid">
            <div className="json-panel">
              <div className="panel-mini-title">Regime Changes</div>
              <pre>{JSON.stringify(comparison.regime_changes ?? {}, null, 2)}</pre>
            </div>
            <div className="json-panel">
              <div className="panel-mini-title">Signal Changes</div>
              <pre>{JSON.stringify(comparison.signal_changes ?? {}, null, 2)}</pre>
            </div>
          </div>

          <div className="detail-grid">
            <div className="json-panel">
              <div className="panel-mini-title">Composite Changes</div>
              <pre>{JSON.stringify(comparison.composite_changes ?? {}, null, 2)}</pre>
            </div>
            <div className="json-panel">
              <div className="panel-mini-title">Invalidator Changes</div>
              <pre>{JSON.stringify(comparison.invalidator_changes ?? {}, null, 2)}</pre>
            </div>
          </div>

          <div className="json-panel">
            <div className="panel-mini-title">Input Coverage Changes</div>
            <pre>{JSON.stringify(comparison.input_coverage_changes ?? {}, null, 2)}</pre>
          </div>
        </div>
      )}
    </section>
  );
}
