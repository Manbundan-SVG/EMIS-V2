"use client";

import type {
  CrossAssetFamilyTransitionAttributionSummaryRow,
  CrossAssetSymbolTransitionAttributionSummaryRow,
  RunCrossAssetTransitionAttributionSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  familyTransitionAttributionSummary: CrossAssetFamilyTransitionAttributionSummaryRow[];
  symbolTransitionAttributionSummary: CrossAssetSymbolTransitionAttributionSummaryRow[];
  runTransitionAttributionSummary: RunCrossAssetTransitionAttributionSummaryRow[];
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

function fmtWeight(value: number | string | null | undefined): string {
  return fmtScore(value, 3);
}

function netBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n > 0.05) return "badge-green";
  if (n < -0.05) return "badge-red";
  return "badge-muted";
}

function shiftBadge(
  a: string | null | undefined, b: string | null | undefined,
): string {
  if (!a && !b) return "badge-muted";
  if (!a || !b) return "badge-yellow";
  return a === b ? "badge-green" : "badge-yellow";
}

function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return `${id.slice(0, 8)}…`;
}

function symbolList(syms: string[] | null | undefined, maxShown = 4): string {
  if (!syms || syms.length === 0) return "—";
  if (syms.length <= maxShown) return syms.join(", ");
  return `${syms.slice(0, maxShown).join(", ")} +${syms.length - maxShown}`;
}

export function CrossAssetTransitionAttributionPanel({
  familyTransitionAttributionSummary,
  symbolTransitionAttributionSummary,
  runTransitionAttributionSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Transition-Aware Cross-Asset Attribution</h2>
          <p className="panel-subtitle">
            Phase 4.3B. Conditions timing-aware family/symbol contribution on 4.3A transition
            state × sequence class. Multiplier clipped to [0.75, 1.20]. Recovering / rotating_in
            receive small positive bonuses; deteriorating / rotating_out receive sign-aware
            penalties. All upstream layers persist side-by-side.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading transition-aware attribution…</p>}

      {!loading && (
        <>
          {/* ── Family Transition Attribution ──────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Transition-Aware Family Attribution</h3>
                <p className="panel-subtitle">Per-family transition state, sequence class, and adjusted contribution.</p>
              </div>
            </div>
            {familyTransitionAttributionSummary.length === 0 ? (
              <p className="muted">No transition-aware family attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Transition</th>
                    <th>Sequence</th>
                    <th>Timing adj</th>
                    <th>State wt</th>
                    <th>Seq wt</th>
                    <th>Bonus</th>
                    <th>Penalty</th>
                    <th>Transition adj</th>
                    <th>Top symbols</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {familyTransitionAttributionSummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="text-sm">{row.transition_family_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
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
                      <td className="text-sm">{fmtScore(row.timing_adjusted_family_contribution)}</td>
                      <td className="text-sm">{fmtWeight(row.transition_state_weight)}</td>
                      <td className="text-sm">{fmtWeight(row.sequence_class_weight)}</td>
                      <td className="text-sm">{fmtScore(row.transition_bonus)}</td>
                      <td className="text-sm">{fmtScore(row.transition_penalty)}</td>
                      <td>
                        <span className={netBadge(row.transition_adjusted_family_contribution)}>
                          {fmtScore(row.transition_adjusted_family_contribution)}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{symbolList(row.top_symbols)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Symbol Transition Attribution ──────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Transition-Aware Symbol Attribution</h3>
                <p className="panel-subtitle">Per-symbol transition-adjusted scores, ranked across families.</p>
              </div>
            </div>
            {symbolTransitionAttributionSummary.length === 0 ? (
              <p className="muted">No transition-aware symbol attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Symbol</th>
                    <th>Family</th>
                    <th>Transition</th>
                    <th>Sequence</th>
                    <th>Timing adj</th>
                    <th>State wt</th>
                    <th>Seq wt</th>
                    <th>Transition adj</th>
                  </tr>
                </thead>
                <tbody>
                  {symbolTransitionAttributionSummary.slice(0, 80).map((row, idx) => (
                    <tr key={`${row.run_id}:${row.symbol}:${idx}`}>
                      <td className="text-sm">{row.symbol_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="mono-cell text-sm">{row.symbol}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
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
                      <td className="text-sm">{fmtScore(row.timing_adjusted_symbol_score)}</td>
                      <td className="text-sm">{fmtWeight(row.transition_state_weight)}</td>
                      <td className="text-sm">{fmtWeight(row.sequence_class_weight)}</td>
                      <td>
                        <span className={netBadge(row.transition_adjusted_symbol_score)}>
                          {fmtScore(row.transition_adjusted_symbol_score)}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Run Integration Summary ────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Transition Integration Summary</h3>
                <p className="panel-subtitle">
                  All five attribution layers per run; dominant family shift across layers.
                </p>
              </div>
            </div>
            {runTransitionAttributionSummary.length === 0 ? (
              <p className="muted">No transition integration rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Watchlist</th>
                    <th>Raw net</th>
                    <th>Wt net</th>
                    <th>Regime net</th>
                    <th>Timing net</th>
                    <th>Transition net</th>
                    <th>Raw dom</th>
                    <th>Wt dom</th>
                    <th>Regime dom</th>
                    <th>Timing dom</th>
                    <th>Trans dom</th>
                    <th>Trans state</th>
                    <th>Seq class</th>
                    <th>Shift</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runTransitionAttributionSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.watchlist_id)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.weighted_cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.regime_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.timing_adjusted_cross_asset_contribution)}</td>
                      <td>
                        <span className={netBadge(row.transition_adjusted_cross_asset_contribution)}>
                          {fmtScore(row.transition_adjusted_cross_asset_contribution)}
                        </span>
                      </td>
                      <td>
                        {row.dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.dominant_dependency_family] ?? "badge-muted"}>
                              {row.dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.weighted_dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.weighted_dominant_dependency_family] ?? "badge-muted"}>
                              {row.weighted_dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.regime_dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.regime_dominant_dependency_family] ?? "badge-muted"}>
                              {row.regime_dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.timing_dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.timing_dominant_dependency_family] ?? "badge-muted"}>
                              {row.timing_dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.transition_dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.transition_dominant_dependency_family] ?? "badge-muted"}>
                              {row.transition_dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
                      </td>
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
                      <td>
                        <span className={shiftBadge(row.timing_dominant_dependency_family, row.transition_dominant_dependency_family)}>
                          {row.timing_dominant_dependency_family === row.transition_dominant_dependency_family
                            ? "stable"
                            : "shifted"}
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
