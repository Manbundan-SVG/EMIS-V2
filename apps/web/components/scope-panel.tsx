"use client";

import { formatNullable, formatTimestamp } from "@/lib/formatters/ops";
import type { RunScopeInspectionRow } from "@/lib/queries/runs";

interface Props {
  scope: RunScopeInspectionRow | null;
  loading: boolean;
}

function AssetChips({ items }: { items: string[] }) {
  if (items.length === 0) {
    return <p className="muted">None</p>;
  }

  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem" }}>
      {items.map((item) => (
        <span
          key={item}
          style={{
            border: "1px solid var(--color-border, #d9d9d9)",
            borderRadius: "999px",
            padding: "0.2rem 0.55rem",
            fontSize: "0.8rem",
          }}
        >
          {item}
        </span>
      ))}
    </div>
  );
}

export function ScopePanel({ scope, loading }: Props) {
  return (
    <section className="card">
      <div className="panel-header">
        <div>
          <h2 className="section-title">Compute Scope</h2>
          <p className="panel-subtitle">Persisted primary universe, dependency policy, and replay-safe scope evidence.</p>
        </div>
      </div>

      {loading && <p className="muted">Loading scope...</p>}
      {!loading && !scope && <p className="muted">No compute scope snapshot has been persisted for this run.</p>}

      {scope && (
        <div className="panel-stack">
          <div className="stats-grid">
            <div className="stat-card">
              <div className="kpi-label">Scope Version</div>
              <div className="kpi-value kpi-value-sm">{formatNullable(scope.scope_version)}</div>
              <div className="kpi-sub">scope row: {formatNullable(scope.compute_scope_id)}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Primary</div>
              <div className="kpi-value kpi-value-sm">{scope.primary_asset_count}</div>
              <div className="kpi-sub">dependencies: {scope.dependency_asset_count}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Universe</div>
              <div className="kpi-value kpi-value-sm">{scope.asset_universe_count}</div>
              <div className="kpi-sub">{scope.is_replay ? "replay-scoped" : "primary run"}</div>
            </div>
            <div className="stat-card">
              <div className="kpi-label">Created</div>
              <div className="kpi-value kpi-value-sm">{formatTimestamp(scope.scope_created_at)}</div>
              <div className="kpi-sub">queue: {scope.queue_name}</div>
            </div>
          </div>

          <div className="detail-list">
            <div><span className="muted">Workspace</span> {scope.workspace_slug}</div>
            <div><span className="muted">Watchlist</span> {formatNullable(scope.watchlist_slug ?? scope.watchlist_name)}</div>
            <div><span className="muted">Replay source</span> {formatNullable(scope.replayed_from_run_id)}</div>
            <div><span className="muted">Scope hash</span> <span className="mono-cell">{formatNullable(scope.scope_hash)}</span></div>
          </div>

          <div className="detail-grid">
            <div className="json-panel">
              <div className="panel-mini-title">Primary Assets</div>
              <AssetChips items={scope.primary_assets ?? []} />
            </div>
            <div className="json-panel">
              <div className="panel-mini-title">Dependency Assets</div>
              <AssetChips items={scope.dependency_assets ?? []} />
            </div>
          </div>

          <div className="detail-grid">
            <div className="json-panel">
              <div className="panel-mini-title">Asset Universe</div>
              <AssetChips items={scope.asset_universe ?? []} />
            </div>
            <div className="json-panel">
              <div className="panel-mini-title">Dependency Policy</div>
              <pre>{JSON.stringify(scope.dependency_policy ?? {}, null, 2)}</pre>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
