"use client";

import type {
  MultiAssetSyncHealthRow,
  NormalizedMultiAssetMarketStateRow,
  MultiAssetFamilyStateSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  syncHealth: MultiAssetSyncHealthRow[];
  marketStateSample: NormalizedMultiAssetMarketStateRow[];
  familySummary: MultiAssetFamilyStateSummaryRow[];
  loading: boolean;
};

const ASSET_CLASS_BADGE: Record<string, string> = {
  crypto: "badge-muted",
  index: "badge-green",
  equity: "badge-green",
  fx: "badge-yellow",
  rates: "badge-muted",
  macro_proxy: "badge-muted",
  commodity: "badge-muted",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtNum(value: number | string | null | undefined, digits = 4): string {
  if (value === null || value === undefined || value === "") return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return n.toFixed(digits);
}

function statusBadge(status: string | null | undefined): string {
  if (status === "completed") return "badge-green";
  if (status === "running") return "badge-yellow";
  if (status === "failed") return "badge-red";
  return "badge-muted";
}

function pickSampleRows(
  rows: NormalizedMultiAssetMarketStateRow[],
): NormalizedMultiAssetMarketStateRow[] {
  const byClass = new Map<string, NormalizedMultiAssetMarketStateRow[]>();
  for (const r of rows) {
    const list = byClass.get(r.asset_class) ?? [];
    list.push(r);
    byClass.set(r.asset_class, list);
  }
  const order = ["crypto", "index", "commodity", "fx", "rates", "macro_proxy"];
  const sample: NormalizedMultiAssetMarketStateRow[] = [];
  for (const cls of order) {
    const list = byClass.get(cls);
    if (!list) continue;
    sample.push(...list.slice(0, 2));
  }
  return sample;
}

export function MultiAssetFoundationPanel({
  syncHealth,
  marketStateSample,
  familySummary,
  loading,
}: Props) {
  const sample = pickSampleRows(marketStateSample);

  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Multi-Asset Data Foundation</h2>
          <p className="panel-subtitle">
            Phase 4.0A surface. Normalized per-asset-class sync health, live market-state sample,
            and per-family rollups. Foundation only — no cross-asset signal logic yet.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading multi-asset foundation state…</p>}

      {!loading && (
        <>
          {/* ── Sync Health ────────────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Multi-Asset Sync Health</h3>
                <p className="panel-subtitle">
                  Latest market_data_sync_runs grouped by provider family and asset class.
                </p>
              </div>
            </div>
            {syncHealth.length === 0 ? (
              <p className="muted">No multi-asset sync runs recorded yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Provider family</th>
                    <th>Asset class</th>
                    <th>Requested</th>
                    <th>Synced</th>
                    <th>Failed</th>
                    <th>Status</th>
                    <th>Provider mode</th>
                    <th>Started</th>
                    <th>Completed</th>
                  </tr>
                </thead>
                <tbody>
                  {syncHealth.map((r, idx) => (
                    <tr key={`${r.workspace_id}:${r.provider_family ?? "x"}:${r.asset_class}:${idx}`}>
                      <td><span className="badge-muted">{r.provider_family ?? "—"}</span></td>
                      <td>
                        <span className={ASSET_CLASS_BADGE[r.asset_class] ?? "badge-muted"}>
                          {r.asset_class}
                        </span>
                      </td>
                      <td className="text-sm">{r.requested_symbol_count}</td>
                      <td className="text-sm">{r.synced_symbol_count}</td>
                      <td className="text-sm">{r.failed_symbol_count}</td>
                      <td>
                        <span className={statusBadge(r.latest_status)}>
                          {r.latest_status ?? "—"}
                        </span>
                      </td>
                      <td className="text-sm">{r.latest_provider_mode ?? "—"}</td>
                      <td className="text-sm muted">{fmtTs(r.latest_run_started_at)}</td>
                      <td className="text-sm muted">{fmtTs(r.latest_run_completed_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Market State Sample ────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Market State Sample</h3>
                <p className="panel-subtitle">
                  Up to two rows per asset class from the normalized multi-asset view.
                </p>
              </div>
            </div>
            {sample.length === 0 ? (
              <p className="muted">No normalized market state rows yet — run a multi-asset sync first.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Canonical</th>
                    <th>Asset class</th>
                    <th>Provider</th>
                    <th>Price</th>
                    <th>Volume 24h</th>
                    <th>Yield</th>
                    <th>FX 1d</th>
                    <th>Macro proxy</th>
                    <th>Timestamp</th>
                  </tr>
                </thead>
                <tbody>
                  {sample.map((r) => (
                    <tr key={`${r.workspace_id}:${r.symbol}:${r.asset_class}`}>
                      <td className="mono-cell text-sm">{r.symbol}</td>
                      <td className="mono-cell text-sm">{r.canonical_symbol}</td>
                      <td>
                        <span className={ASSET_CLASS_BADGE[r.asset_class] ?? "badge-muted"}>
                          {r.asset_class}
                        </span>
                      </td>
                      <td className="text-sm">{r.provider_family ?? "—"}</td>
                      <td className="text-sm">{fmtNum(r.price)}</td>
                      <td className="text-sm">{fmtNum(r.volume_24h, 0)}</td>
                      <td className="text-sm">{fmtNum(r.yield_value)}</td>
                      <td className="text-sm">{fmtNum(r.fx_return_1d)}</td>
                      <td className="text-sm">{fmtNum(r.macro_proxy_value)}</td>
                      <td className="text-sm muted">{fmtTs(r.price_timestamp)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Summary ─────────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Summary</h3>
                <p className="panel-subtitle">
                  Per-class rollup. avg_return_1d and avg_volatility_proxy remain null
                  until cross-asset analytics land in 4.0B+.
                </p>
              </div>
            </div>
            {familySummary.length === 0 ? (
              <p className="muted">No family summary rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Asset class</th>
                    <th>Family</th>
                    <th>Symbols</th>
                    <th>Latest timestamp</th>
                    <th>Avg return 1d</th>
                    <th>Avg volatility</th>
                    <th>Providers</th>
                  </tr>
                </thead>
                <tbody>
                  {familySummary.map((r) => {
                    const providers = Array.isArray((r.metadata as { provider_families?: unknown }).provider_families)
                      ? ((r.metadata as { provider_families: string[] }).provider_families).join(", ")
                      : "—";
                    return (
                      <tr key={`${r.workspace_id}:${r.asset_class}:${r.family_key}`}>
                        <td>
                          <span className={ASSET_CLASS_BADGE[r.asset_class] ?? "badge-muted"}>
                            {r.asset_class}
                          </span>
                        </td>
                        <td className="text-sm">{r.family_key}</td>
                        <td className="text-sm">{r.symbol_count}</td>
                        <td className="text-sm muted">{fmtTs(r.latest_timestamp)}</td>
                        <td className="text-sm">{fmtNum(r.avg_return_1d)}</td>
                        <td className="text-sm">{fmtNum(r.avg_volatility_proxy)}</td>
                        <td className="text-sm muted">{providers}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>
        </>
      )}
    </section>
  );
}
