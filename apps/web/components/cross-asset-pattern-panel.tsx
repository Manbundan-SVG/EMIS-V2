"use client";

import type {
  CrossAssetFamilyArchetypeSummaryRow,
  CrossAssetRunArchetypeSummaryRow,
  CrossAssetRegimeArchetypeSummaryRow,
  RunCrossAssetPatternSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  familyArchetypeSummary: CrossAssetFamilyArchetypeSummaryRow[];
  runArchetypeSummary: CrossAssetRunArchetypeSummaryRow[];
  regimeArchetypeSummary: CrossAssetRegimeArchetypeSummaryRow[];
  runPatternSummary: RunCrossAssetPatternSummaryRow[];
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

const ARCHETYPE_BADGE: Record<string, string> = {
  rotation_handoff:         "badge-yellow",
  reinforcing_continuation: "badge-green",
  recovering_reentry:       "badge-green",
  deteriorating_breakdown:  "badge-red",
  mixed_transition_noise:   "badge-muted",
  insufficient_history:     "badge-muted",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtScore(value: number | string | null | undefined, digits = 3): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function confidenceBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.7) return "badge-green";
  if (n >= 0.4) return "badge-yellow";
  return "badge-red";
}

function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return `${id.slice(0, 8)}…`;
}

function reasonList(codes: string[] | null | undefined, maxShown = 3): string {
  if (!codes || codes.length === 0) return "—";
  if (codes.length <= maxShown) return codes.join(", ");
  return `${codes.slice(0, maxShown).join(", ")} +${codes.length - maxShown}`;
}

export function CrossAssetPatternPanel({
  familyArchetypeSummary,
  runArchetypeSummary,
  regimeArchetypeSummary,
  runPatternSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Sequencing Patterns & Archetypes</h2>
          <p className="panel-subtitle">
            Phase 4.4A. Deterministic classification of family + run transition/sequence state into a
            small controlled archetype vocabulary (rotation_handoff, reinforcing_continuation,
            recovering_reentry, deteriorating_breakdown, mixed_transition_noise, insufficient_history).
            Each classification carries explicit reason codes and a bounded [0, 1] confidence.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading sequencing patterns…</p>}

      {!loading && (
        <>
          {/* ── Run Archetype ──────────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Run Archetype</h3>
                <p className="panel-subtitle">Dominant archetype, family, transition state, and bucket counts per run.</p>
              </div>
            </div>
            {runArchetypeSummary.length === 0 ? (
              <p className="muted">No run archetype rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Regime</th>
                    <th>Archetype</th>
                    <th>Dominant family</th>
                    <th>Transition</th>
                    <th>Sequence</th>
                    <th>Confidence</th>
                    <th>Rotation</th>
                    <th>Recovery</th>
                    <th>Degradation</th>
                    <th>Mixed</th>
                    <th>Reasons</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runArchetypeSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{row.regime_key ?? "—"}</td>
                      <td>
                        <span className={ARCHETYPE_BADGE[row.dominant_archetype_key] ?? "badge-muted"}>
                          {row.dominant_archetype_key}
                        </span>
                      </td>
                      <td>
                        {row.dominant_dependency_family ? (
                          <span className={FAMILY_BADGE[row.dominant_dependency_family] ?? "badge-muted"}>
                            {row.dominant_dependency_family}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.dominant_transition_state ? (
                          <span className={TRANSITION_BADGE[row.dominant_transition_state] ?? "badge-muted"}>
                            {row.dominant_transition_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.dominant_sequence_class ? (
                          <span className={SEQUENCE_BADGE[row.dominant_sequence_class] ?? "badge-muted"}>
                            {row.dominant_sequence_class}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={confidenceBadge(row.archetype_confidence)}>
                          {fmtScore(row.archetype_confidence)}
                        </span>
                      </td>
                      <td className="text-sm">{row.rotation_event_count}</td>
                      <td className="text-sm">{row.recovery_event_count}</td>
                      <td className="text-sm">{row.degradation_event_count}</td>
                      <td className="text-sm">{row.mixed_event_count}</td>
                      <td className="mono-cell text-sm">{reasonList(row.classification_reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Archetypes ──────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Archetypes</h3>
                <p className="panel-subtitle">Per-family archetype + confidence with supporting reason codes.</p>
              </div>
            </div>
            {familyArchetypeSummary.length === 0 ? (
              <p className="muted">No family archetype rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Archetype</th>
                    <th>Transition</th>
                    <th>Sequence</th>
                    <th>Timing</th>
                    <th>Contribution</th>
                    <th>Confidence</th>
                    <th>Reasons</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {familyArchetypeSummary.slice(0, 120).map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="text-sm">{row.family_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={ARCHETYPE_BADGE[row.archetype_key] ?? "badge-muted"}>
                          {row.archetype_key}
                        </span>
                      </td>
                      <td>
                        <span className={TRANSITION_BADGE[row.transition_state] ?? "badge-muted"}>
                          {row.transition_state}
                        </span>
                      </td>
                      <td>
                        <span className={SEQUENCE_BADGE[row.dominant_sequence_class] ?? "badge-muted"}>
                          {row.dominant_sequence_class}
                        </span>
                      </td>
                      <td className="text-sm">{row.dominant_timing_class ?? "—"}</td>
                      <td className="text-sm">{fmtScore(row.family_contribution, 4)}</td>
                      <td>
                        <span className={confidenceBadge(row.archetype_confidence)}>
                          {fmtScore(row.archetype_confidence)}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{reasonList(row.classification_reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Regime Archetype Summary ───────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Regime Archetype Summary</h3>
                <p className="panel-subtitle">Which archetypes recur under which regimes, with avg confidence.</p>
              </div>
            </div>
            {regimeArchetypeSummary.length === 0 ? (
              <p className="muted">No regime archetype rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Regime</th>
                    <th>Archetype</th>
                    <th>Run count</th>
                    <th>Avg confidence</th>
                    <th>Latest</th>
                  </tr>
                </thead>
                <tbody>
                  {regimeArchetypeSummary.map((row, idx) => (
                    <tr key={`${row.regime_key ?? "none"}:${row.archetype_key}:${idx}`}>
                      <td className="text-sm">{row.regime_key ?? "—"}</td>
                      <td>
                        <span className={ARCHETYPE_BADGE[row.archetype_key] ?? "badge-muted"}>
                          {row.archetype_key}
                        </span>
                      </td>
                      <td className="text-sm">{row.run_count}</td>
                      <td>
                        <span className={confidenceBadge(row.avg_confidence)}>
                          {fmtScore(row.avg_confidence)}
                        </span>
                      </td>
                      <td className="text-sm muted">{fmtTs(row.latest_seen_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Run Pattern Summary ────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Run Pattern Summary</h3>
                <p className="panel-subtitle">Compact run-linked pattern row for run inspection.</p>
              </div>
            </div>
            {runPatternSummary.length === 0 ? (
              <p className="muted">No run pattern rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Regime</th>
                    <th>Archetype</th>
                    <th>Dominant family</th>
                    <th>Transition</th>
                    <th>Sequence</th>
                    <th>Confidence</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runPatternSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{row.regime_key ?? "—"}</td>
                      <td>
                        <span className={ARCHETYPE_BADGE[row.dominant_archetype_key] ?? "badge-muted"}>
                          {row.dominant_archetype_key}
                        </span>
                      </td>
                      <td>
                        {row.dominant_dependency_family ? (
                          <span className={FAMILY_BADGE[row.dominant_dependency_family] ?? "badge-muted"}>
                            {row.dominant_dependency_family}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.dominant_transition_state ? (
                          <span className={TRANSITION_BADGE[row.dominant_transition_state] ?? "badge-muted"}>
                            {row.dominant_transition_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.dominant_sequence_class ? (
                          <span className={SEQUENCE_BADGE[row.dominant_sequence_class] ?? "badge-muted"}>
                            {row.dominant_sequence_class}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={confidenceBadge(row.archetype_confidence)}>
                          {fmtScore(row.archetype_confidence)}
                        </span>
                      </td>
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
