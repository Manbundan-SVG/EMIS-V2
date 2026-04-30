"use client";

import type {
  CrossAssetConflictCompositeSummaryRow,
  CrossAssetFamilyConflictCompositeSummaryRow,
  RunCrossAssetConflictIntegrationSummaryRow,
} from "@/lib/queries/metrics";

type Props = {
  conflictCompositeSummary: CrossAssetConflictCompositeSummaryRow[];
  familyConflictCompositeSummary: CrossAssetFamilyConflictCompositeSummaryRow[];
  finalConflictIntegrationSummary: RunCrossAssetConflictIntegrationSummaryRow[];
  loading: boolean;
};

const CONSENSUS_BADGE: Record<string, string> = {
  aligned_supportive:   "badge-green",
  aligned_suppressive:  "badge-red",
  partial_agreement:    "badge-yellow",
  conflicted:           "badge-red",
  unreliable:           "badge-red",
  insufficient_context: "badge-muted",
};

const MODE_BADGE: Record<string, string> = {
  conflict_additive_guardrailed:        "badge-muted",
  aligned_supportive_confirmation_only: "badge-green",
  conflict_suppression_only:            "badge-red",
  unreliable_suppression_only:          "badge-red",
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

function deltaBadge(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "badge-muted";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "badge-muted";
  if (n >= 0.02) return "badge-green";
  if (n <= -0.02) return "badge-red";
  return "badge-muted";
}

function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return `${id.slice(0, 8)}…`;
}

export function CrossAssetConflictCompositePanel({
  conflictCompositeSummary,
  familyConflictCompositeSummary,
  finalConflictIntegrationSummary,
  loading,
}: Props) {
  if (loading) {
    return <section className="panel"><h2>Conflict-Aware Composite (4.8C)</h2><p>Loading…</p></section>;
  }

  return (
    <section className="panel">
      <h2>Conflict-Aware Composite (4.8C)</h2>

      {/* ── Section 1: Conflict-Aware Composite (run-level) ──────────── */}
      <h3>Conflict-Aware Composite</h3>
      {conflictCompositeSummary.length === 0 ? (
        <p className="muted">No conflict-aware composite snapshots yet.</p>
      ) : (
        <table className="dataTable">
          <thead>
            <tr>
              <th>Run</th>
              <th>Pre-Conflict</th>
              <th>Δ</th>
              <th>Post-Conflict</th>
              <th>Consensus</th>
              <th>Agreement</th>
              <th>Conflict</th>
              <th>Dominant Source</th>
              <th>Mode</th>
              <th>Source Layers</th>
              <th>When</th>
            </tr>
          </thead>
          <tbody>
            {conflictCompositeSummary.slice(0, 25).map((r) => (
              <tr key={`${r.run_id}-${r.created_at}`}>
                <td>{shortId(r.run_id)}</td>
                <td>{fmtScore(r.composite_pre_conflict)}</td>
                <td className={deltaBadge(r.conflict_net_contribution)}>
                  {fmtScore(r.conflict_net_contribution)}
                </td>
                <td>{fmtScore(r.composite_post_conflict)}</td>
                <td className={CONSENSUS_BADGE[r.layer_consensus_state] ?? "badge-muted"}>
                  {r.layer_consensus_state}
                </td>
                <td>{fmtScore(r.agreement_score, 3)}</td>
                <td>{fmtScore(r.conflict_score, 3)}</td>
                <td>{r.dominant_conflict_source ?? "—"}</td>
                <td className={MODE_BADGE[r.integration_mode] ?? "badge-muted"}>
                  {r.integration_mode}
                </td>
                <td className="muted small">
                  c:{r.source_contribution_layer ?? "—"} / b:{r.source_composite_layer ?? "—"}
                </td>
                <td>{fmtTs(r.created_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {/* ── Section 2: Family Conflict Composite Contribution ────────── */}
      <h3>Family Conflict Composite Contribution</h3>
      {familyConflictCompositeSummary.length === 0 ? (
        <p className="muted">No family conflict composite rows yet.</p>
      ) : (
        <table className="dataTable">
          <thead>
            <tr>
              <th>Run</th>
              <th>Family</th>
              <th>Conflict-Adj. Family</th>
              <th>Integration Δ</th>
              <th>Rank</th>
              <th>Consensus</th>
              <th>Dominant Source</th>
              <th>Reason Codes</th>
              <th>When</th>
            </tr>
          </thead>
          <tbody>
            {familyConflictCompositeSummary.slice(0, 50).map((r, i) => (
              <tr key={`${r.run_id}-${r.dependency_family}-${i}`}>
                <td>{shortId(r.run_id)}</td>
                <td>{r.dependency_family}</td>
                <td>{fmtScore(r.conflict_adjusted_family_contribution)}</td>
                <td className={deltaBadge(r.conflict_integration_contribution)}>
                  {fmtScore(r.conflict_integration_contribution)}
                </td>
                <td>{r.family_rank ?? "—"}</td>
                <td className={CONSENSUS_BADGE[r.family_consensus_state] ?? "badge-muted"}>
                  {r.family_consensus_state}
                </td>
                <td>{r.dominant_conflict_source ?? "—"}</td>
                <td className="muted small">
                  {(r.reason_codes ?? []).slice(0, 3).join(", ") || "—"}
                </td>
                <td>{fmtTs(r.created_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {/* ── Section 3: Final Conflict Integration Summary ────────────── */}
      <h3>Final Conflict Integration Summary</h3>
      {finalConflictIntegrationSummary.length === 0 ? (
        <p className="muted">No final conflict integration rows yet.</p>
      ) : (
        <table className="dataTable">
          <thead>
            <tr>
              <th>Run</th>
              <th>Raw</th>
              <th>Wgt</th>
              <th>Reg</th>
              <th>Tim</th>
              <th>Trn</th>
              <th>Arc</th>
              <th>Clu</th>
              <th>Per</th>
              <th>Dec</th>
              <th>Cnf</th>
              <th>Pre→Post</th>
              <th>Dominant (raw → conflict)</th>
              <th>Consensus</th>
              <th>Replay-Ready</th>
            </tr>
          </thead>
          <tbody>
            {finalConflictIntegrationSummary.slice(0, 25).map((r) => (
              <tr key={r.run_id}>
                <td>{shortId(r.run_id)}</td>
                <td>{fmtScore(r.cross_asset_net_contribution, 3)}</td>
                <td>{fmtScore(r.weighted_cross_asset_net_contribution, 3)}</td>
                <td>{fmtScore(r.regime_adjusted_cross_asset_contribution, 3)}</td>
                <td>{fmtScore(r.timing_adjusted_cross_asset_contribution, 3)}</td>
                <td>{fmtScore(r.transition_adjusted_cross_asset_contribution, 3)}</td>
                <td>{fmtScore(r.archetype_adjusted_cross_asset_contribution, 3)}</td>
                <td>{fmtScore(r.cluster_adjusted_cross_asset_contribution, 3)}</td>
                <td>{fmtScore(r.persistence_adjusted_cross_asset_contribution, 3)}</td>
                <td>{fmtScore(r.decay_adjusted_cross_asset_contribution, 3)}</td>
                <td>{fmtScore(r.conflict_adjusted_cross_asset_contribution, 3)}</td>
                <td>
                  {fmtScore(r.composite_pre_conflict, 3)} → {fmtScore(r.composite_post_conflict, 3)}
                </td>
                <td className="small">
                  {r.dominant_dependency_family ?? "—"} → {r.conflict_dominant_dependency_family ?? "—"}
                </td>
                <td className={CONSENSUS_BADGE[r.layer_consensus_state ?? ""] ?? "badge-muted"}>
                  {r.layer_consensus_state ?? "—"}
                </td>
                <td className="muted small">
                  v:{r.scoring_version ?? "—"} c:{r.source_contribution_layer ?? "—"} b:{r.source_composite_layer ?? "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
