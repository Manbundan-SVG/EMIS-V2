"use client";

import type {
  CrossAssetSignalSummaryRow,
  CrossAssetDependencyHealthRow,
  RunCrossAssetContextSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  signalSummary: CrossAssetSignalSummaryRow[];
  dependencyHealth: CrossAssetDependencyHealthRow[];
  runContextSummary: RunCrossAssetContextSummaryRow[];
  loading: boolean;
};

const FAMILY_BADGE: Record<string, string> = {
  macro_confirmation: "badge-muted",
  risk_context: "badge-green",
  fx_pressure: "badge-yellow",
  rates_pressure: "badge-muted",
  commodity_context: "badge-muted",
  cross_asset_divergence: "badge-yellow",
  macro: "badge-muted",
  fx: "badge-yellow",
  rates: "badge-muted",
  equity_index: "badge-green",
  commodity: "badge-muted",
  crypto_cross: "badge-green",
  risk: "badge-yellow",
};

const STATE_BADGE: Record<string, string> = {
  confirmed: "badge-green",
  contradicted: "badge-red",
  unconfirmed: "badge-muted",
  missing_context: "badge-red",
  stale_context: "badge-yellow",
  computed: "badge-muted",
};

const DIRECTION_BADGE: Record<string, string> = {
  bullish: "badge-green",
  bearish: "badge-red",
  neutral: "badge-muted",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtNum(value: number | string | null | undefined, digits = 3): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function shortHash(hash: string | null | undefined): string {
  if (!hash) return "—";
  return `${hash.slice(0, 8)}…`;
}

export function CrossAssetSignalsPanel({
  signalSummary,
  dependencyHealth,
  runContextSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Cross-Asset Signals</h2>
          <p className="panel-subtitle">
            Phase 4.0C. Cross-asset features and derived signals computed against the 4.0B
            dependency context. States mark confirmation, contradiction, or missing/stale context
            explicitly — no silent zeroing.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading cross-asset signal state…</p>}

      {!loading && (
        <>
          {/* ── Signal Summary ─────────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Cross-Asset Signal Summary</h3>
                <p className="panel-subtitle">
                  Latest signal per (family, key, base). Confirmed = dependency context agrees
                  with base direction; contradicted = it opposes.
                </p>
              </div>
            </div>
            {signalSummary.length === 0 ? (
              <p className="muted">No cross-asset signals persisted yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Family</th>
                    <th>Key</th>
                    <th>Base</th>
                    <th>Value</th>
                    <th>Direction</th>
                    <th>State</th>
                    <th>Deps</th>
                    <th>Families</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {signalSummary.slice(0, 60).map((row, idx) => (
                    <tr key={`${row.watchlist_id}:${row.signal_family}:${row.signal_key}:${row.base_symbol ?? "x"}:${idx}`}>
                      <td>
                        <span className={FAMILY_BADGE[row.signal_family] ?? "badge-muted"}>
                          {row.signal_family}
                        </span>
                      </td>
                      <td className="text-sm">{row.signal_key}</td>
                      <td className="mono-cell text-sm">{row.base_symbol ?? "—"}</td>
                      <td className="text-sm">{fmtNum(row.signal_value)}</td>
                      <td>
                        {row.signal_direction
                          ? <span className={DIRECTION_BADGE[row.signal_direction]}>{row.signal_direction}</span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        <span className={STATE_BADGE[row.signal_state] ?? "badge-muted"}>
                          {row.signal_state}
                        </span>
                      </td>
                      <td className="text-sm">{row.dependency_symbol_count}</td>
                      <td className="text-sm">{row.dependency_family_count}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Dependency Health ──────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Dependency Health</h3>
                <p className="panel-subtitle">
                  Feature/signal counts rolled up by dependency family + context snapshot.
                </p>
              </div>
            </div>
            {dependencyHealth.length === 0 ? (
              <p className="muted">No dependency health rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Watchlist</th>
                    <th>Context</th>
                    <th>Family</th>
                    <th>Features</th>
                    <th>Signals</th>
                    <th>Confirmed</th>
                    <th>Contradicted</th>
                    <th>Missing</th>
                    <th>Stale</th>
                    <th>Latest</th>
                  </tr>
                </thead>
                <tbody>
                  {dependencyHealth.map((row, idx) => (
                    <tr key={`${row.watchlist_id}:${row.context_snapshot_id ?? "x"}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{row.watchlist_id.slice(0, 8)}…</td>
                      <td className="mono-cell text-sm">{shortHash(row.context_snapshot_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{row.feature_count}</td>
                      <td className="text-sm">{row.signal_count}</td>
                      <td className="text-sm">{row.confirmed_count}</td>
                      <td className="text-sm">{row.contradicted_count}</td>
                      <td className="text-sm">{row.missing_dependency_count}</td>
                      <td className="text-sm">{row.stale_dependency_count}</td>
                      <td className="text-sm muted">{fmtTs(row.latest_created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Run Context Summary ────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Run Cross-Asset Context Summary</h3>
                <p className="panel-subtitle">
                  One row per run that persisted cross-asset features/signals. dominant_dependency_family
                  flags which family dominated that run.
                </p>
              </div>
            </div>
            {runContextSummary.length === 0 ? (
              <p className="muted">No run-linked cross-asset summaries yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Watchlist</th>
                    <th>Context</th>
                    <th>Features</th>
                    <th>Signals</th>
                    <th>Confirmed</th>
                    <th>Contradicted</th>
                    <th>Missing</th>
                    <th>Stale</th>
                    <th>Dominant family</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runContextSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{row.run_id.slice(0, 8)}…</td>
                      <td className="mono-cell text-sm">{row.watchlist_id.slice(0, 8)}…</td>
                      <td className="mono-cell text-sm">{shortHash(row.context_snapshot_id)}</td>
                      <td className="text-sm">{row.cross_asset_feature_count}</td>
                      <td className="text-sm">{row.cross_asset_signal_count}</td>
                      <td className="text-sm">{row.confirmed_signal_count}</td>
                      <td className="text-sm">{row.contradicted_signal_count}</td>
                      <td className="text-sm">{row.missing_context_count}</td>
                      <td className="text-sm">{row.stale_context_count}</td>
                      <td>
                        {row.dominant_dependency_family
                          ? <span className={FAMILY_BADGE[row.dominant_dependency_family] ?? "badge-muted"}>
                              {row.dominant_dependency_family}
                            </span>
                          : <span className="badge-muted">—</span>}
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
