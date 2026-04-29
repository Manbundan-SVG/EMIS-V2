"use client";

import type {
  WatchlistContextSnapshotRow,
  WatchlistDependencyCoverageSummaryRow,
  WatchlistDependencyContextDetailRow,
  WatchlistDependencyFamilyStateRow,
} from "@/lib/queries/metrics";

type Props = {
  latestContexts: WatchlistContextSnapshotRow[];
  coverageSummary: WatchlistDependencyCoverageSummaryRow[];
  contextDetail: WatchlistDependencyContextDetailRow[];
  familyState: WatchlistDependencyFamilyStateRow[];
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

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtRatio(value: number | string | null): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

function ratioBadge(value: number | string | null): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.9) return "badge-green";
  if (n >= 0.6) return "badge-yellow";
  return "badge-red";
}

function shortHash(hash: string | null | undefined): string {
  if (!hash) return "—";
  return `${hash.slice(0, 8)}…`;
}

export function DependencyContextPanel({
  latestContexts,
  coverageSummary,
  contextDetail,
  familyState,
  loading,
}: Props) {
  const detailSample = contextDetail.slice(0, 30);

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Dependency Context Model</h2>
          <p className="panel-subtitle">
            Phase 4.0B. Deterministic dependency graph + per-watchlist context snapshots.
            Foundation only — no cross-asset signal logic yet.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading dependency context state…</p>}

      {!loading && (
        <>
          {/* ── Latest Context Snapshots ───────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Latest Context Snapshot</h3>
                <p className="panel-subtitle">Most recent assembled context per watchlist.</p>
              </div>
            </div>
            {latestContexts.length === 0 ? (
              <p className="muted">No context snapshots persisted yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Watchlist</th>
                    <th>Primary</th>
                    <th>Deps</th>
                    <th>Families</th>
                    <th>Context hash</th>
                    <th>Snapshot at</th>
                  </tr>
                </thead>
                <tbody>
                  {latestContexts.map((row) => (
                    <tr key={row.id}>
                      <td className="mono-cell text-sm">{row.watchlist_id.slice(0, 8)}…</td>
                      <td className="text-sm">{row.primary_symbols?.length ?? 0}</td>
                      <td className="text-sm">{row.dependency_symbols?.length ?? 0}</td>
                      <td className="text-sm">{row.dependency_families?.length ?? 0}</td>
                      <td className="mono-cell text-sm">{shortHash(row.context_hash)}</td>
                      <td className="text-sm muted">{fmtTs(row.snapshot_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Coverage Summary ───────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Coverage Summary</h3>
                <p className="panel-subtitle">
                  Dependency freshness against the 4.0A normalized market state.
                  Stale thresholds: 72h for fx/rates/macro, 48h elsewhere.
                </p>
              </div>
            </div>
            {coverageSummary.length === 0 ? (
              <p className="muted">No coverage rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Watchlist</th>
                    <th>Context</th>
                    <th>Primary</th>
                    <th>Deps</th>
                    <th>Families</th>
                    <th>Covered</th>
                    <th>Missing</th>
                    <th>Stale</th>
                    <th>Coverage</th>
                    <th>Snapshot</th>
                  </tr>
                </thead>
                <tbody>
                  {coverageSummary.map((row) => (
                    <tr key={`${row.watchlist_id}:${row.context_hash}`}>
                      <td className="mono-cell text-sm">{row.watchlist_id.slice(0, 8)}…</td>
                      <td className="mono-cell text-sm">{shortHash(row.context_hash)}</td>
                      <td className="text-sm">{row.primary_symbol_count}</td>
                      <td className="text-sm">{row.dependency_symbol_count}</td>
                      <td className="text-sm">{row.dependency_family_count}</td>
                      <td className="text-sm">{row.covered_dependency_count}</td>
                      <td className="text-sm">{row.missing_dependency_count}</td>
                      <td className="text-sm">{row.stale_dependency_count}</td>
                      <td>
                        <span className={ratioBadge(row.coverage_ratio)}>
                          {fmtRatio(row.coverage_ratio)}
                        </span>
                      </td>
                      <td className="text-sm muted">{fmtTs(row.latest_context_snapshot_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Dependency Detail ──────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Dependency Detail Sample</h3>
                <p className="panel-subtitle">
                  Per-symbol view of the current context. is_primary distinguishes watchlist
                  primaries from graph-derived dependencies.
                </p>
              </div>
            </div>
            {detailSample.length === 0 ? (
              <p className="muted">No context detail rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Asset class</th>
                    <th>Family</th>
                    <th>Dep type</th>
                    <th>Priority</th>
                    <th>Primary?</th>
                    <th>Latest</th>
                    <th>Missing</th>
                    <th>Stale</th>
                  </tr>
                </thead>
                <tbody>
                  {detailSample.map((row, idx) => (
                    <tr key={`${row.watchlist_id}:${row.symbol}:${idx}`}>
                      <td className="mono-cell text-sm">{row.symbol}</td>
                      <td className="text-sm">{row.asset_class ?? "—"}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{row.dependency_type ?? "—"}</td>
                      <td className="text-sm">{row.priority ?? "—"}</td>
                      <td>
                        {row.is_primary
                          ? <span className="badge-green">primary</span>
                          : <span className="badge-muted">dep</span>}
                      </td>
                      <td className="text-sm muted">{fmtTs(row.latest_timestamp)}</td>
                      <td>
                        {row.is_missing
                          ? <span className="badge-red">missing</span>
                          : <span className="badge-muted">—</span>}
                      </td>
                      <td>
                        {row.is_stale
                          ? <span className="badge-yellow">stale</span>
                          : <span className="badge-muted">—</span>}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family State ───────────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family State</h3>
                <p className="panel-subtitle">Dependency coverage rolled up by family.</p>
              </div>
            </div>
            {familyState.length === 0 ? (
              <p className="muted">No family state rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Watchlist</th>
                    <th>Family</th>
                    <th>Symbols</th>
                    <th>Covered</th>
                    <th>Missing</th>
                    <th>Stale</th>
                    <th>Latest</th>
                  </tr>
                </thead>
                <tbody>
                  {familyState.map((row, idx) => (
                    <tr key={`${row.watchlist_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{row.watchlist_id.slice(0, 8)}…</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td className="text-sm">{row.symbol_count}</td>
                      <td className="text-sm">{row.covered_count}</td>
                      <td className="text-sm">{row.missing_count}</td>
                      <td className="text-sm">{row.stale_count}</td>
                      <td className="text-sm muted">{fmtTs(row.latest_timestamp)}</td>
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
