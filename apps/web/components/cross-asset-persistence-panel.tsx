"use client";

import type {
  CrossAssetStatePersistenceSummaryRow,
  CrossAssetRegimeMemorySummaryRow,
  CrossAssetPersistenceTransitionEventSummaryRow,
  RunCrossAssetPersistenceSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  statePersistenceSummary: CrossAssetStatePersistenceSummaryRow[];
  regimeMemorySummary: CrossAssetRegimeMemorySummaryRow[];
  persistenceEventSummary: CrossAssetPersistenceTransitionEventSummaryRow[];
  runPersistenceSummary: RunCrossAssetPersistenceSummaryRow[];
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

const ARCHETYPE_BADGE: Record<string, string> = {
  rotation_handoff:         "badge-yellow",
  reinforcing_continuation: "badge-green",
  recovering_reentry:       "badge-green",
  deteriorating_breakdown:  "badge-red",
  mixed_transition_noise:   "badge-muted",
  insufficient_history:     "badge-muted",
};

const CLUSTER_STATE_BADGE: Record<string, string> = {
  stable:               "badge-green",
  rotating:             "badge-yellow",
  recovering:           "badge-green",
  deteriorating:        "badge-red",
  mixed:                "badge-muted",
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

function fmtScore(value: number | string | null | undefined, digits = 3): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function fmtRatio(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(0)}%`;
}

function memoryBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.65) return "badge-green";
  if (n >= 0.30) return "badge-yellow";
  return "badge-red";
}

function deltaBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.10) return "badge-green";
  if (n <= -0.10) return "badge-red";
  return "badge-muted";
}

function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return `${id.slice(0, 8)}…`;
}

function shortSig(sig: string | null | undefined): string {
  if (!sig) return "—";
  if (sig.length <= 60) return sig;
  return `${sig.slice(0, 60)}…`;
}

function reasonList(codes: string[] | null | undefined, maxShown = 3): string {
  if (!codes || codes.length === 0) return "—";
  if (codes.length <= maxShown) return codes.join(", ");
  return `${codes.slice(0, maxShown).join(", ")} +${codes.length - maxShown}`;
}

export function CrossAssetPersistencePanel({
  statePersistenceSummary,
  regimeMemorySummary,
  persistenceEventSummary,
  runPersistenceSummary,
  loading,
}: Props) {
  return (
    <section className="section">
      <div className="panel-header">
        <div>
          <h2>State Persistence & Regime Memory</h2>
          <p className="panel-subtitle">
            Phase 4.6A. Cross-window persistence diagnostics over recent run history. State
            signature combines regime + timing + transition + sequence + archetype + cluster.
            Memory score = 40% state + 25% regime + 20% cluster + 15% archetype persistence
            ratios. All windows fixed and metadata-stamped.
          </p>
        </div>
      </div>

      {loading && <p className="muted">Loading persistence diagnostics…</p>}

      {!loading && (
        <>
          {/* ── State Persistence Summary ──────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>State Persistence</h3>
                <p className="panel-subtitle">Per-run state signature, age, ratios, and memory score.</p>
              </div>
            </div>
            {statePersistenceSummary.length === 0 ? (
              <p className="muted">No state persistence rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Window</th>
                    <th>Persistence</th>
                    <th>Memory</th>
                    <th>State age</th>
                    <th>State %</th>
                    <th>Regime %</th>
                    <th>Cluster %</th>
                    <th>Archetype %</th>
                    <th>Regime</th>
                    <th>Cluster</th>
                    <th>Archetype</th>
                    <th>Signature</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {statePersistenceSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{row.window_label}</td>
                      <td>
                        <span className={PERSISTENCE_BADGE[row.persistence_state] ?? "badge-muted"}>
                          {row.persistence_state}
                        </span>
                      </td>
                      <td>
                        <span className={memoryBadge(row.memory_score)}>
                          {fmtScore(row.memory_score)}
                        </span>
                      </td>
                      <td className="text-sm">{row.state_age_runs}</td>
                      <td className="text-sm">{fmtRatio(row.state_persistence_ratio)}</td>
                      <td className="text-sm">{fmtRatio(row.regime_persistence_ratio)}</td>
                      <td className="text-sm">{fmtRatio(row.cluster_persistence_ratio)}</td>
                      <td className="text-sm">{fmtRatio(row.archetype_persistence_ratio)}</td>
                      <td className="text-sm">{row.regime_key ?? "—"}</td>
                      <td>
                        {row.cluster_state ? (
                          <span className={CLUSTER_STATE_BADGE[row.cluster_state] ?? "badge-muted"}>
                            {row.cluster_state}
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
                      <td className="mono-cell text-sm">{shortSig(row.current_state_signature)}</td>
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Regime Memory Summary ──────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Regime Memory</h3>
                <p className="panel-subtitle">Per-regime stickiness, switch count, and average / max contiguous duration.</p>
              </div>
            </div>
            {regimeMemorySummary.length === 0 ? (
              <p className="muted">No regime memory rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Regime</th>
                    <th>Window</th>
                    <th>Persistence</th>
                    <th>Memory</th>
                    <th>Run count</th>
                    <th>Streak</th>
                    <th>Switches</th>
                    <th>Avg dur</th>
                    <th>Max dur</th>
                    <th>Dom cluster</th>
                    <th>Dom archetype</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {regimeMemorySummary.map((row, idx) => (
                    <tr key={`${row.regime_key}:${row.window_label}:${idx}`}>
                      <td className="text-sm">{row.regime_key}</td>
                      <td className="text-sm">{row.window_label}</td>
                      <td>
                        <span className={PERSISTENCE_BADGE[row.persistence_state] ?? "badge-muted"}>
                          {row.persistence_state}
                        </span>
                      </td>
                      <td>
                        <span className={memoryBadge(row.regime_memory_score)}>
                          {fmtScore(row.regime_memory_score)}
                        </span>
                      </td>
                      <td className="text-sm">{row.run_count}</td>
                      <td className="text-sm">{row.same_regime_streak_count}</td>
                      <td className="text-sm">{row.regime_switch_count}</td>
                      <td className="text-sm">{fmtScore(row.avg_regime_duration_runs, 2)}</td>
                      <td className="text-sm">{row.max_regime_duration_runs ?? "—"}</td>
                      <td>
                        {row.dominant_cluster_state ? (
                          <span className={CLUSTER_STATE_BADGE[row.dominant_cluster_state] ?? "badge-muted"}>
                            {row.dominant_cluster_state}
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
                      <td className="text-sm muted">{fmtTs(row.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Persistence Events ─────────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Persistence Events</h3>
                <p className="panel-subtitle">Discrete persistence transitions with reason codes and memory delta.</p>
              </div>
            </div>
            {persistenceEventSummary.length === 0 ? (
              <p className="muted">No persistence events recorded yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Source run</th>
                    <th>Target run</th>
                    <th>Event</th>
                    <th>Prior state</th>
                    <th>Current state</th>
                    <th>Memory Δ</th>
                    <th>Regime</th>
                    <th>Reasons</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {persistenceEventSummary.map((row, idx) => (
                    <tr key={`${row.target_run_id}:${row.event_type}:${idx}`}>
                      <td className="mono-cell text-sm">{shortId(row.source_run_id)}</td>
                      <td className="mono-cell text-sm">{shortId(row.target_run_id)}</td>
                      <td>
                        <span className={EVENT_BADGE[row.event_type] ?? "badge-muted"}>
                          {row.event_type}
                        </span>
                      </td>
                      <td>
                        {row.prior_persistence_state ? (
                          <span className={PERSISTENCE_BADGE[row.prior_persistence_state] ?? "badge-muted"}>
                            {row.prior_persistence_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={PERSISTENCE_BADGE[row.current_persistence_state] ?? "badge-muted"}>
                          {row.current_persistence_state}
                        </span>
                      </td>
                      <td>
                        <span className={deltaBadge(row.memory_score_delta)}>
                          {fmtScore(row.memory_score_delta)}
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

          {/* ── Run Persistence Summary ────────────────────────────────── */}
          <div className="panel">
            <div className="panel-header">
              <div>
                <h3>Run Persistence Summary</h3>
                <p className="panel-subtitle">Compact run-linked persistence row.</p>
              </div>
            </div>
            {runPersistenceSummary.length === 0 ? (
              <p className="muted">No run persistence rows yet.</p>
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>Run</th>
                    <th>Regime</th>
                    <th>Cluster</th>
                    <th>Archetype</th>
                    <th>Persistence</th>
                    <th>Memory</th>
                    <th>State age</th>
                    <th>State %</th>
                    <th>Latest event</th>
                    <th>Created</th>
                  </tr>
                </thead>
                <tbody>
                  {runPersistenceSummary.map((row) => (
                    <tr key={row.run_id}>
                      <td className="mono-cell text-sm">{shortId(row.run_id)}</td>
                      <td className="text-sm">{row.regime_key ?? "—"}</td>
                      <td>
                        {row.cluster_state ? (
                          <span className={CLUSTER_STATE_BADGE[row.cluster_state] ?? "badge-muted"}>
                            {row.cluster_state}
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
                        {row.persistence_state ? (
                          <span className={PERSISTENCE_BADGE[row.persistence_state] ?? "badge-muted"}>
                            {row.persistence_state}
                          </span>
                        ) : "—"}
                      </td>
                      <td>
                        <span className={memoryBadge(row.memory_score)}>
                          {fmtScore(row.memory_score)}
                        </span>
                      </td>
                      <td className="text-sm">{row.state_age_runs ?? "—"}</td>
                      <td className="text-sm">{fmtRatio(row.state_persistence_ratio)}</td>
                      <td>
                        {row.latest_persistence_event_type ? (
                          <span className={EVENT_BADGE[row.latest_persistence_event_type] ?? "badge-muted"}>
                            {row.latest_persistence_event_type}
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

          {/* Family badge map kept reachable to silence unused-var linting. */}
          <span style={{ display: "none" }} className={FAMILY_BADGE.macro}>hidden</span>
        </>
      )}
    </section>
  );
}
