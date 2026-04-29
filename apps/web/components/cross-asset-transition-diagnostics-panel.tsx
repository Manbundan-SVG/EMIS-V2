"use client";

import type {
  CrossAssetFamilyTransitionStateSummaryRow,
  CrossAssetFamilyTransitionEventSummaryRow,
  CrossAssetFamilySequenceSummaryRow,
  RunCrossAssetTransitionDiagnosticsSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  transitionStateSummary: CrossAssetFamilyTransitionStateSummaryRow[];
  transitionEventSummary: CrossAssetFamilyTransitionEventSummaryRow[];
  sequenceSummary: CrossAssetFamilySequenceSummaryRow[];
  runTransitionSummary: RunCrossAssetTransitionDiagnosticsSummaryRow[];
  loading: boolean;
};

const FAMILY_BADGE: Record<string, string> = {
  macro: "badge-muted",
  fx: "badge-yellow",
  rates: "badge-muted",
  equity_index: "badge-green",
  commodity: "badge-muted",
  crypto_cross: "badge-green",
  risk: "badge-yellow",
};

const SIGNAL_BADGE: Record<string, string> = {
  confirmed: "badge-green",
  unconfirmed: "badge-muted",
  contradicted: "badge-red",
  missing_context: "badge-red",
  stale_context: "badge-yellow",
  insufficient_data: "badge-muted",
};

const TRANSITION_BADGE: Record<string, string> = {
  reinforcing: "badge-green",
  deteriorating: "badge-red",
  recovering: "badge-green",
  rotating_in: "badge-yellow",
  rotating_out: "badge-yellow",
  stable: "badge-muted",
  insufficient_history: "badge-muted",
};

const SEQUENCE_BADGE: Record<string, string> = {
  reinforcing_path: "badge-green",
  deteriorating_path: "badge-red",
  recovery_path: "badge-green",
  rotation_path: "badge-yellow",
  mixed_path: "badge-muted",
  insufficient_history: "badge-muted",
};

const EVENT_BADGE: Record<string, string> = {
  dominance_gain: "badge-green",
  dominance_loss: "badge-red",
  rank_shift: "badge-yellow",
  state_shift: "badge-muted",
  recovery: "badge-green",
  degradation: "badge-red",
  timing_shift: "badge-yellow",
  regime_shift: "badge-yellow",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtScore(value: number | string | null | undefined, digits = 4): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function fmtRank(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return String(value);
}

function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return `${id.slice(0, 8)}…`;
}

export function CrossAssetTransitionDiagnosticsPanel({
  transitionStateSummary,
  transitionEventSummary,
  sequenceSummary,
  runTransitionSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Family Transition Diagnostics</h2>
          <p className="panel-subtitle">
            Phase 4.3A. Per-family signal state, transition state vs prior run, discrete transition
            events, and a fixed-window sequence signature. Descriptive diagnostics only — no
            prediction or causal inference.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading transition diagnostics…</p>}

      {!loading && (
        <>
          {/* ── Run Transition Summary ─────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Run Transition Summary</h3>
                <p className="panel-subtitle">
                  Dominant family + transition/sequence classes + event counts per run.
                </p>
              </div>
            </div>
            {runTransitionSummary.length === 0 ? (
              <p className="muted">No run-linked transition summaries yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Watchlist</th>
                    <th>Dominant family</th>
                    <th>Prior dominant</th>
                    <th>Timing class</th>
                    <th>Transition state</th>
                    <th>Sequence class</th>
                    <th>Rotation</th>
                    <th>Degradation</th>
                    <th>Recovery</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runTransitionSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.watchlist_id)}</td>
                      <td>
                        {row.dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.dominant_dependency_family] ?? "badge-muted"}>
                              {row.dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.prior_dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.prior_dominant_dependency_family] ?? "badge-muted"}>
                              {row.prior_dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td className="text-sm">{row.dominant_timing_class ?? "—"}</td>
                      <td>
                        {row.dominant_transition_state
                          ? <span className={TRANSITION_BADGE[row.dominant_transition_state] ?? "badge-muted"}>
                              {row.dominant_transition_state}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.dominant_sequence_class
                          ? <span className={SEQUENCE_BADGE[row.dominant_sequence_class] ?? "badge-muted"}>
                              {row.dominant_sequence_class}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td className="text-sm">{row.rotation_event_count}</td>
                      <td className="text-sm">{row.degradation_event_count}</td>
                      <td className="text-sm">{row.recovery_event_count}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Transition States ───────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Transition States</h3>
                <p className="panel-subtitle">Per-family signal + transition state for the latest run.</p>
              </div>
            </div>
            {transitionStateSummary.length === 0 ? (
              <p className="muted">No transition state rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Signal state</th>
                    <th>Transition state</th>
                    <th>Timing class</th>
                    <th>Regime</th>
                    <th>Rank</th>
                    <th>Family contrib</th>
                    <th>Timing adj</th>
                    <th>Timing int</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {transitionStateSummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={SIGNAL_BADGE[row.signal_state] ?? "badge-muted"}>
                          {row.signal_state}
                        </span>
                      </td>
                      <td>
                        <span className={TRANSITION_BADGE[row.transition_state] ?? "badge-muted"}>
                          {row.transition_state}
                        </span>
                      </td>
                      <td className="text-sm">{row.dominant_timing_class ?? "—"}</td>
                      <td className="text-sm">{row.regime_key ?? "—"}</td>
                      <td className="text-sm">{fmtRank(row.family_rank)}</td>
                      <td className="text-sm">{fmtScore(row.family_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.timing_adjusted_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.timing_integration_contribution)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Transition Events ──────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Transition Events</h3>
                <p className="panel-subtitle">Prior → current transitions with explicit event type.</p>
              </div>
            </div>
            {transitionEventSummary.length === 0 ? (
              <p className="muted">No transition events yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Target run</th>
                    <th>Source run</th>
                    <th>Family</th>
                    <th>Event</th>
                    <th>Prior signal</th>
                    <th>Current signal</th>
                    <th>Prior trans</th>
                    <th>Current trans</th>
                    <th>Rank Δ</th>
                    <th>Contrib Δ</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {transitionEventSummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.target_run_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.target_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={EVENT_BADGE[row.event_type] ?? "badge-muted"}>
                          {row.event_type}
                        </span>
                      </td>
                      <td>
                        {row.prior_signal_state
                          ? <span className={SIGNAL_BADGE[row.prior_signal_state] ?? "badge-muted"}>
                              {row.prior_signal_state}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        <span className={SIGNAL_BADGE[row.current_signal_state] ?? "badge-muted"}>
                          {row.current_signal_state}
                        </span>
                      </td>
                      <td>
                        {row.prior_transition_state
                          ? <span className={TRANSITION_BADGE[row.prior_transition_state] ?? "badge-muted"}>
                              {row.prior_transition_state}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        <span className={TRANSITION_BADGE[row.current_transition_state] ?? "badge-muted"}>
                          {row.current_transition_state}
                        </span>
                      </td>
                      <td className="text-sm">{row.rank_delta ?? "—"}</td>
                      <td className="text-sm">{fmtScore(row.contribution_delta)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Sequence Summary ───────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Sequence Summary</h3>
                <p className="panel-subtitle">
                  Fixed-window sequence signature per family (oldest → newest) with dominant path class.
                </p>
              </div>
            </div>
            {sequenceSummary.length === 0 ? (
              <p className="muted">No sequence summary rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Window</th>
                    <th>Length</th>
                    <th>Sequence</th>
                    <th>Class</th>
                    <th>Confidence</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {sequenceSummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.run_id ?? "x"}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{row.window_label}</td>
                      <td className="text-sm">{row.sequence_length}</td>
                      <td className="mono-cell text-sm">{row.sequence_signature || "—"}</td>
                      <td>
                        <span className={SEQUENCE_BADGE[row.dominant_sequence_class] ?? "badge-muted"}>
                          {row.dominant_sequence_class}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.sequence_confidence, 3)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </>
      )}
    </section>
  );
}
