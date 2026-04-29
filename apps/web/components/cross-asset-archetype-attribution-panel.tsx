"use client";

import type {
  CrossAssetFamilyArchetypeAttributionSummaryRow,
  CrossAssetSymbolArchetypeAttributionSummaryRow,
  RunCrossAssetArchetypeAttributionSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  familyArchetypeAttributionSummary: CrossAssetFamilyArchetypeAttributionSummaryRow[];
  symbolArchetypeAttributionSummary: CrossAssetSymbolArchetypeAttributionSummaryRow[];
  runArchetypeAttributionSummary: RunCrossAssetArchetypeAttributionSummaryRow[];
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

export function CrossAssetArchetypeAttributionPanel({
  familyArchetypeAttributionSummary,
  symbolArchetypeAttributionSummary,
  runArchetypeAttributionSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Archetype-Aware Cross-Asset Attribution</h2>
          <p className="panel-subtitle">
            Phase 4.4B. Conditions transition-aware family/symbol contribution on the 4.4A archetype
            classification. Weight clipped to [0.75, 1.20]. Recovering reentry / rotation handoff earn
            small positive bonuses; deteriorating breakdowns get sign-aware penalties. All upstream
            attribution layers persist side-by-side.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading archetype-aware attribution…</p>}

      {!loading && (
        <>
          {/* ── Archetype Integration Summary ──────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Archetype Integration Summary</h3>
                <p className="panel-subtitle">Raw vs weighted vs regime vs timing vs transition vs archetype-adjusted contribution per run.</p>
              </div>
            </div>
            {runArchetypeAttributionSummary.length === 0 ? (
              <p className="muted">No archetype integration rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Raw</th>
                    <th>Weighted</th>
                    <th>Regime</th>
                    <th>Timing</th>
                    <th>Transition</th>
                    <th>Archetype</th>
                    <th>Raw dom.</th>
                    <th>Archetype dom.</th>
                    <th>Dom. archetype</th>
                    <th>Shift</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runArchetypeAttributionSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.weighted_cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.regime_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.timing_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.transition_adjusted_cross_asset_contribution)}</td>
                      <td>
                        <span className={netBadge(row.archetype_adjusted_cross_asset_contribution)}>
                          {fmtScore(row.archetype_adjusted_cross_asset_contribution)}
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
                        {row.archetype_dominant_dependency_family ? (
                          <span className={FAMILY_BADGE[row.archetype_dominant_dependency_family] ?? "badge-muted"}>
                            {row.archetype_dominant_dependency_family}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.dominant_archetype_key ? (
                          <span className={ARCHETYPE_BADGE[row.dominant_archetype_key] ?? "badge-muted"}>
                            {row.dominant_archetype_key}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={shiftBadge(row.dominant_dependency_family, row.archetype_dominant_dependency_family)}>
                          {row.dominant_dependency_family === row.archetype_dominant_dependency_family ? "stable" : "shifted"}
                        </span>
                      </td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Archetype-Aware Family Attribution ─────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Archetype-Aware Family Attribution</h3>
                <p className="panel-subtitle">Per-family archetype, weight, and archetype-adjusted contribution.</p>
              </div>
            </div>
            {familyArchetypeAttributionSummary.length === 0 ? (
              <p className="muted">No archetype-aware family attribution rows yet.</p>
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
                    <th>Transition adj</th>
                    <th>Weight</th>
                    <th>Bonus</th>
                    <th>Penalty</th>
                    <th>Archetype adj</th>
                    <th>Top symbols</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {familyArchetypeAttributionSummary.slice(0, 120).map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="text-sm">{row.archetype_family_rank ?? "—"}</td>
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
                      <td className="text-sm">{fmtScore(row.transition_adjusted_family_contribution)}</td>
                      <td className="text-sm">{fmtWeight(row.archetype_weight)}</td>
                      <td className="text-sm">{fmtScore(row.archetype_bonus)}</td>
                      <td className="text-sm">{fmtScore(row.archetype_penalty)}</td>
                      <td>
                        <span className={netBadge(row.archetype_adjusted_family_contribution)}>
                          {fmtScore(row.archetype_adjusted_family_contribution)}
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

          {/* ── Archetype-Aware Symbol Attribution ─────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Archetype-Aware Symbol Attribution</h3>
                <p className="panel-subtitle">Top symbols by archetype-adjusted score. Symbol inherits family archetype context.</p>
              </div>
            </div>
            {symbolArchetypeAttributionSummary.length === 0 ? (
              <p className="muted">No archetype-aware symbol rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Symbol</th>
                    <th>Family</th>
                    <th>Archetype</th>
                    <th>Transition</th>
                    <th>Transition adj</th>
                    <th>Weight</th>
                    <th>Archetype adj</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {symbolArchetypeAttributionSummary.slice(0, 150).map((row, idx) => (
                    <tr key={`${row.run_id}:${row.symbol}:${idx}`}>
                      <td className="text-sm">{row.symbol_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="mono-cell">{row.symbol}</td>
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
                      <td className="text-sm">{fmtScore(row.transition_adjusted_symbol_score)}</td>
                      <td className="text-sm">{fmtWeight(row.archetype_weight)}</td>
                      <td>
                        <span className={netBadge(row.archetype_adjusted_symbol_score)}>
                          {fmtScore(row.archetype_adjusted_symbol_score)}
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
