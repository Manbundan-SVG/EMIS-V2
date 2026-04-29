"use client";

import type {
  CrossAssetFamilyPersistenceAttributionSummaryRow,
  CrossAssetSymbolPersistenceAttributionSummaryRow,
  RunCrossAssetPersistenceAttributionSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  familyPersistenceAttributionSummary: CrossAssetFamilyPersistenceAttributionSummaryRow[];
  symbolPersistenceAttributionSummary: CrossAssetSymbolPersistenceAttributionSummaryRow[];
  runPersistenceAttributionSummary: RunCrossAssetPersistenceAttributionSummaryRow[];
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

const PERSISTENCE_BADGE: Record<string, string> = {
  persistent:           "badge-green",
  fragile:              "badge-red",
  rotating:             "badge-yellow",
  breaking_down:        "badge-red",
  recovering:           "badge-green",
  mixed:                "badge-muted",
  insufficient_history: "badge-muted",
};

const EVENT_BADGE: Record<string, string> = {
  persistence_gain:        "badge-green",
  persistence_loss:        "badge-red",
  regime_memory_break:     "badge-red",
  cluster_memory_break:    "badge-yellow",
  archetype_memory_break:  "badge-yellow",
  state_rotation:          "badge-yellow",
  stabilization:           "badge-green",
  insufficient_history:    "badge-muted",
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

function memoryBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.65) return "badge-green";
  if (n >= 0.30) return "badge-yellow";
  return "badge-red";
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

function reasonList(codes: string[] | null | undefined, maxShown = 3): string {
  if (!codes || codes.length === 0) return "—";
  if (codes.length <= maxShown) return codes.join(", ");
  return `${codes.slice(0, maxShown).join(", ")} +${codes.length - maxShown}`;
}

export function CrossAssetPersistenceAttributionPanel({
  familyPersistenceAttributionSummary,
  symbolPersistenceAttributionSummary,
  runPersistenceAttributionSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Persistence-Aware Cross-Asset Attribution</h2>
          <p className="panel-subtitle">
            Phase 4.6B. Conditions cluster-aware family/symbol contribution on the 4.6A
            persistence diagnostics (persistence state, memory score, state age, latest
            persistence event). Weight clipped to [0.75, 1.20]. Persistent / recovering states
            with high memory score earn small positive bonuses (memory + state-age + stabilization);
            fragile / breaking_down states get sign-aware penalties; memory-break events add
            additional penalty. All upstream attribution layers persist side-by-side.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading persistence-aware attribution…</p>}

      {!loading && (
        <>
          {/* ── Persistence Attribution Summary ────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Persistence Attribution Summary</h3>
                <p className="panel-subtitle">Raw vs weighted vs regime vs timing vs transition vs archetype vs cluster vs persistence-adjusted contribution per run.</p>
              </div>
            </div>
            {runPersistenceAttributionSummary.length === 0 ? (
              <p className="muted">No persistence integration rows yet.</p>
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
                    <th>Cluster</th>
                    <th>Persistence</th>
                    <th>Persistence state</th>
                    <th>Memory</th>
                    <th>Age</th>
                    <th>Latest event</th>
                    <th>Raw dom.</th>
                    <th>Persist dom.</th>
                    <th>Shift</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runPersistenceAttributionSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{fmtScore(row.cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.weighted_cross_asset_net_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.regime_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.timing_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.transition_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.archetype_adjusted_cross_asset_contribution)}</td>
                      <td className="text-sm">{fmtScore(row.cluster_adjusted_cross_asset_contribution)}</td>
                      <td>
                        <span className={netBadge(row.persistence_adjusted_cross_asset_contribution)}>
                          {fmtScore(row.persistence_adjusted_cross_asset_contribution)}
                        </span>
                      </td>
                      <td>
                        {row.persistence_state ? (
                          <span className={PERSISTENCE_BADGE[row.persistence_state] ?? "badge-muted"}>
                            {row.persistence_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={memoryBadge(row.memory_score)}>
                          {fmtScore(row.memory_score, 3)}
                        </span>
                      </td>
                      <td className="text-sm">{row.state_age_runs ?? "—"}</td>
                      <td>
                        {row.latest_persistence_event_type ? (
                          <span className={EVENT_BADGE[row.latest_persistence_event_type] ?? "badge-muted"}>
                            {row.latest_persistence_event_type}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.dominant_dependency_family ? (
                          <span className={FAMILY_BADGE[row.dominant_dependency_family] ?? "badge-muted"}>
                            {row.dominant_dependency_family}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.persistence_dominant_dependency_family ? (
                          <span className={FAMILY_BADGE[row.persistence_dominant_dependency_family] ?? "badge-muted"}>
                            {row.persistence_dominant_dependency_family}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={shiftBadge(row.dominant_dependency_family, row.persistence_dominant_dependency_family)}>
                          {row.dominant_dependency_family === row.persistence_dominant_dependency_family ? "stable" : "shifted"}
                        </span>
                      </td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Persistence-Aware Family Attribution ───────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Persistence-Aware Family Attribution</h3>
                <p className="panel-subtitle">Per-family persistence state, weight, bonus/penalty, and adjusted contribution.</p>
              </div>
            </div>
            {familyPersistenceAttributionSummary.length === 0 ? (
              <p className="muted">No persistence-aware family attribution rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Persistence state</th>
                    <th>Memory</th>
                    <th>State age</th>
                    <th>Latest event</th>
                    <th>Cluster adj</th>
                    <th>Weight</th>
                    <th>Bonus</th>
                    <th>Penalty</th>
                    <th>Persist adj</th>
                    <th>Top symbols</th>
                    <th>Reasons</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {familyPersistenceAttributionSummary.slice(0, 120).map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="text-sm">{row.persistence_family_rank ?? "—"}</td>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={PERSISTENCE_BADGE[row.persistence_state] ?? "badge-muted"}>
                          {row.persistence_state}
                        </span>
                      </td>
                      <td>
                        <span className={memoryBadge(row.memory_score)}>
                          {fmtScore(row.memory_score, 3)}
                        </span>
                      </td>
                      <td className="text-sm">{row.state_age_runs ?? "—"}</td>
                      <td>
                        {row.latest_persistence_event_type ? (
                          <span className={EVENT_BADGE[row.latest_persistence_event_type] ?? "badge-muted"}>
                            {row.latest_persistence_event_type}
                          </span>
                        ) : "—"}
                      </td>
                      <td className="text-sm">{fmtScore(row.cluster_adjusted_family_contribution)}</td>
                      <td className="text-sm">{fmtWeight(row.persistence_weight)}</td>
                      <td className="text-sm">{fmtScore(row.persistence_bonus)}</td>
                      <td className="text-sm">{fmtScore(row.persistence_penalty)}</td>
                      <td>
                        <span className={netBadge(row.persistence_adjusted_family_contribution)}>
                          {fmtScore(row.persistence_adjusted_family_contribution)}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{symbolList(row.top_symbols)}</td>
                      <td className="mono-cell text-sm">{reasonList(row.reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Persistence-Aware Symbol Attribution ───────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Persistence-Aware Symbol Attribution</h3>
                <p className="panel-subtitle">Top symbols by persistence-adjusted score.</p>
              </div>
            </div>
            {symbolPersistenceAttributionSummary.length === 0 ? (
              <p className="muted">No persistence-aware symbol rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Rank</th>
                    <th>Run</th>
                    <th>Symbol</th>
                    <th>Family</th>
                    <th>Persistence state</th>
                    <th>Memory</th>
                    <th>Cluster adj</th>
                    <th>Weight</th>
                    <th>Persist adj</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {symbolPersistenceAttributionSummary.slice(0, 150).map((row, idx) => (
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
                        <span className={PERSISTENCE_BADGE[row.persistence_state] ?? "badge-muted"}>
                          {row.persistence_state}
                        </span>
                      </td>
                      <td>
                        <span className={memoryBadge(row.memory_score)}>
                          {fmtScore(row.memory_score, 3)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.cluster_adjusted_symbol_score)}</td>
                      <td className="text-sm">{fmtWeight(row.persistence_weight)}</td>
                      <td>
                        <span className={netBadge(row.persistence_adjusted_symbol_score)}>
                          {fmtScore(row.persistence_adjusted_symbol_score)}
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
