"use client";

import type {
  CrossAssetSignalDecaySummaryRow,
  CrossAssetFamilySignalDecaySummaryRow,
  CrossAssetStaleMemoryEventSummaryRow,
  RunCrossAssetSignalDecaySummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  signalDecaySummary: CrossAssetSignalDecaySummaryRow[];
  familySignalDecaySummary: CrossAssetFamilySignalDecaySummaryRow[];
  staleMemoryEventSummary: CrossAssetStaleMemoryEventSummaryRow[];
  runSignalDecaySummary: RunCrossAssetSignalDecaySummaryRow[];
  loading: boolean;
};

const FRESHNESS_BADGE: Record<string, string> = {
  fresh:                "badge-green",
  decaying:             "badge-yellow",
  stale:                "badge-red",
  contradicted:         "badge-red",
  mixed:                "badge-yellow",
  insufficient_history: "badge-muted",
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
  memory_freshened:     "badge-green",
  memory_decayed:       "badge-yellow",
  memory_became_stale:  "badge-red",
  memory_contradicted:  "badge-red",
  memory_reconfirmed:   "badge-green",
  insufficient_history: "badge-muted",
};

const FAMILY_BADGE: Record<string, string> = {
  macro:        "badge-muted",
  fx:           "badge-yellow",
  rates:        "badge-muted",
  equity_index: "badge-green",
  commodity:    "badge-muted",
  crypto_cross: "badge-green",
  risk:         "badge-yellow",
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

function decayBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.75) return "badge-green";
  if (n >= 0.50) return "badge-yellow";
  if (n <= 0.30) return "badge-red";
  return "badge-yellow";
}

function memoryBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.65) return "badge-green";
  if (n >= 0.30) return "badge-yellow";
  return "badge-red";
}

function flagBadge(flag: boolean): string {
  return flag ? "badge-red" : "badge-muted";
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

export function CrossAssetSignalDecayPanel({
  signalDecaySummary,
  familySignalDecaySummary,
  staleMemoryEventSummary,
  runSignalDecaySummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>Signal Decay & Stale-Memory Diagnostics</h2>
          <p className="panel-subtitle">
            Phase 4.7A. Decay-aware diagnostics over the live persistence stack.
            Aggregate decay = 20% regime + 15% timing + 15% transition + 15% archetype +
            20% cluster + 15% persistence half-life decay (blended 70/30 with current
            persistence ratio support). Fresh ≥ 0.75, decaying ≥ 0.50, stale ≤ 0.30.
            All thresholds and half-lives are deterministic, bounded, and metadata-stamped.
            Diagnostic only — no predictive forecasting.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading signal decay diagnostics…</p>}

      {!loading && (
        <>
          {/* ── Signal Decay Summary ──────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Signal Decay Summary</h3>
                <p className="panel-subtitle">
                  Per-run freshness state with aggregate decay, memory score, and stale /
                  contradiction flags.
                </p>
              </div>
            </div>
            {signalDecaySummary.length === 0 ? (
              <p className="muted">No signal decay rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Freshness</th>
                    <th>Aggregate decay</th>
                    <th>Memory</th>
                    <th>State age</th>
                    <th>Persistence</th>
                    <th>Regime</th>
                    <th>Cluster</th>
                    <th>Stale</th>
                    <th>Contradiction</th>
                    <th>Reasons</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {signalDecaySummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FRESHNESS_BADGE[row.freshness_state] ?? "badge-muted"}>
                          {row.freshness_state}
                        </span>
                      </td>
                      <td>
                        <span className={decayBadge(row.aggregate_decay_score)}>
                          {fmtScore(row.aggregate_decay_score)}
                        </span>
                      </td>
                      <td>
                        <span className={memoryBadge(row.memory_score)}>
                          {fmtScore(row.memory_score)}
                        </span>
                      </td>
                      <td className="text-sm">{row.state_age_runs ?? "—"}</td>
                      <td>
                        {row.persistence_state ? (
                          <span className={PERSISTENCE_BADGE[row.persistence_state] ?? "badge-muted"}>
                            {row.persistence_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td className="text-sm">{row.regime_key ?? "—"}</td>
                      <td className="text-sm">{row.cluster_state ?? "—"}</td>
                      <td>
                        <span className={flagBadge(row.stale_memory_flag)}>
                          {row.stale_memory_flag ? "stale" : "ok"}
                        </span>
                      </td>
                      <td>
                        <span className={flagBadge(row.contradiction_flag)}>
                          {row.contradiction_flag ? "contradicted" : "ok"}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{reasonList(row.reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Family Signal Decay ───────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Family Signal Decay</h3>
                <p className="panel-subtitle">
                  Per-family freshness and decay diagnostics with stale / contradicted flags.
                </p>
              </div>
            </div>
            {familySignalDecaySummary.length === 0 ? (
              <p className="muted">No family decay rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Family</th>
                    <th>Freshness</th>
                    <th>Decay</th>
                    <th>Memory</th>
                    <th>Contribution</th>
                    <th>Rank</th>
                    <th>State age</th>
                    <th>Persistence</th>
                    <th>Stale</th>
                    <th>Contradicted</th>
                    <th>Reasons</th>
                  </tr>
                </thead>
                <tbody>
                  {familySignalDecaySummary.map((row, idx) => (
                    <tr key={`${row.run_id}:${row.dependency_family}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td>
                        <span className={FAMILY_BADGE[row.dependency_family] ?? "badge-muted"}>
                          {row.dependency_family}
                        </span>
                      </td>
                      <td>
                        <span className={FRESHNESS_BADGE[row.family_freshness_state] ?? "badge-muted"}>
                          {row.family_freshness_state}
                        </span>
                      </td>
                      <td>
                        <span className={decayBadge(row.family_decay_score)}>
                          {fmtScore(row.family_decay_score)}
                        </span>
                      </td>
                      <td>
                        <span className={memoryBadge(row.family_memory_score)}>
                          {fmtScore(row.family_memory_score)}
                        </span>
                      </td>
                      <td className="text-sm">{fmtScore(row.family_contribution, 4)}</td>
                      <td className="text-sm">{row.family_rank ?? "—"}</td>
                      <td className="text-sm">{row.family_state_age_runs ?? "—"}</td>
                      <td>
                        {row.persistence_state ? (
                          <span className={PERSISTENCE_BADGE[row.persistence_state] ?? "badge-muted"}>
                            {row.persistence_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={flagBadge(row.stale_family_memory_flag)}>
                          {row.stale_family_memory_flag ? "stale" : "ok"}
                        </span>
                      </td>
                      <td>
                        <span className={flagBadge(row.contradicted_family_flag)}>
                          {row.contradicted_family_flag ? "contradicted" : "ok"}
                        </span>
                      </td>
                      <td className="mono-cell text-sm">{reasonList(row.reason_codes)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Stale-Memory Events ───────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Stale-Memory Events</h3>
                <p className="panel-subtitle">
                  Discrete prior → current freshness transitions with reason codes and decay/memory deltas.
                </p>
              </div>
            </div>
            {staleMemoryEventSummary.length === 0 ? (
              <p className="muted">No stale-memory events recorded yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Source run</th>
                    <th>Target run</th>
                    <th>Event</th>
                    <th>Prior</th>
                    <th>Current</th>
                    <th>Prior decay</th>
                    <th>Current decay</th>
                    <th>Prior memory</th>
                    <th>Current memory</th>
                    <th>Regime</th>
                    <th>Reasons</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {staleMemoryEventSummary.map((row, idx) => (
                    <tr key={`${row.target_run_id}:${row.event_type}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.target_run_id)}</td>
                      <td>
                        <span className={EVENT_BADGE[row.event_type] ?? "badge-muted"}>
                          {row.event_type}
                        </span>
                      </td>
                      <td>
                        {row.prior_freshness_state ? (
                          <span className={FRESHNESS_BADGE[row.prior_freshness_state] ?? "badge-muted"}>
                            {row.prior_freshness_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={FRESHNESS_BADGE[row.current_freshness_state] ?? "badge-muted"}>
                          {row.current_freshness_state}
                        </span>
                      </td>
                      <td>
                        <span className={decayBadge(row.prior_aggregate_decay_score)}>
                          {fmtScore(row.prior_aggregate_decay_score)}
                        </span>
                      </td>
                      <td>
                        <span className={decayBadge(row.current_aggregate_decay_score)}>
                          {fmtScore(row.current_aggregate_decay_score)}
                        </span>
                      </td>
                      <td>
                        <span className={memoryBadge(row.prior_memory_score)}>
                          {fmtScore(row.prior_memory_score)}
                        </span>
                      </td>
                      <td>
                        <span className={memoryBadge(row.current_memory_score)}>
                          {fmtScore(row.current_memory_score)}
                        </span>
                      </td>
                      <td className="text-sm">{row.regime_key ?? "—"}</td>
                      <td className="mono-cell text-sm">{reasonList(row.reason_codes)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Run Signal Decay Summary ─────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Run Signal Decay Summary</h3>
                <p className="panel-subtitle">Compact run-linked decay row joined with the latest stale-memory event.</p>
              </div>
            </div>
            {runSignalDecaySummary.length === 0 ? (
              <p className="muted">No run signal decay rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Regime</th>
                    <th>Persistence</th>
                    <th>Freshness</th>
                    <th>Aggregate decay</th>
                    <th>Memory</th>
                    <th>Stale</th>
                    <th>Contradiction</th>
                    <th>Latest event</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runSignalDecaySummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{row.regime_key ?? "—"}</td>
                      <td>
                        {row.persistence_state ? (
                          <span className={PERSISTENCE_BADGE[row.persistence_state] ?? "badge-muted"}>
                            {row.persistence_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        {row.freshness_state ? (
                          <span className={FRESHNESS_BADGE[row.freshness_state] ?? "badge-muted"}>
                            {row.freshness_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={decayBadge(row.aggregate_decay_score)}>
                          {fmtScore(row.aggregate_decay_score)}
                        </span>
                      </td>
                      <td>
                        <span className={memoryBadge(row.memory_score)}>
                          {fmtScore(row.memory_score)}
                        </span>
                      </td>
                      <td>
                        <span className={flagBadge(row.stale_memory_flag)}>
                          {row.stale_memory_flag ? "stale" : "ok"}
                        </span>
                      </td>
                      <td>
                        <span className={flagBadge(row.contradiction_flag)}>
                          {row.contradiction_flag ? "contradicted" : "ok"}
                        </span>
                      </td>
                      <td>
                        {row.latest_stale_memory_event_type ? (
                          <span className={EVENT_BADGE[row.latest_stale_memory_event_type] ?? "badge-muted"}>
                            {row.latest_stale_memory_event_type}
                          </span>
                        ) : "—"}
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
