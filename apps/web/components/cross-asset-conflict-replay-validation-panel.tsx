"use client";

import type {
  CrossAssetConflictReplayValidationSummaryRow,
  CrossAssetFamilyConflictReplayStabilitySummaryRow,
  CrossAssetConflictReplayStabilityAggregateRow,
} from "@/lib/queries/metrics";

type Props = {
  conflictReplayValidationSummary: CrossAssetConflictReplayValidationSummaryRow[];
  familyConflictReplayStabilitySummary: CrossAssetFamilyConflictReplayStabilitySummaryRow[];
  conflictReplayStabilityAggregate: CrossAssetConflictReplayStabilityAggregateRow | null;
  loading: boolean;
};

const VALIDATION_BADGE: Record<string, string> = {
  validated:           "badge-green",
  drift_detected:      "badge-red",
  insufficient_source: "badge-muted",
  insufficient_replay: "badge-muted",
  context_mismatch:    "badge-yellow",
  timing_mismatch:     "badge-yellow",
  transition_mismatch: "badge-yellow",
  archetype_mismatch:  "badge-yellow",
  cluster_mismatch:    "badge-yellow",
  persistence_mismatch:"badge-yellow",
  decay_mismatch:      "badge-yellow",
  conflict_mismatch:   "badge-red",
};

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtRate(value: number | string | null | undefined): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

function fmtScore(value: number | string | null | undefined, digits = 4): string {
  if (value === null || value === undefined) return "—";
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(digits);
}

function shortId(id: string | null | undefined): string {
  if (!id) return "—";
  return `${id.slice(0, 8)}…`;
}

function tick(b: boolean): string {
  return b ? "✓" : "✗";
}

function tickClass(b: boolean): string {
  return b ? "badge-green" : "badge-red";
}

export function CrossAssetConflictReplayValidationPanel({
  conflictReplayValidationSummary,
  familyConflictReplayStabilitySummary,
  conflictReplayStabilityAggregate,
  loading,
}: Props) {
  if (loading) {
    return <section className="panel"><h2>Conflict Replay Validation (4.8D)</h2><p>Loading…</p></section>;
  }

  return (
    <section className="panel">
      <h2>Conflict Replay Validation (4.8D)</h2>

      {/* ── Section 1: Validation summary ─────────────────────────────── */}
      <h3>Conflict Replay Validation Summary</h3>
      {conflictReplayValidationSummary.length === 0 ? (
        <p className="muted">No conflict replay validations yet.</p>
      ) : (
        <table className="dataTable">
          <thead>
            <tr>
              <th>Source → Replay</th>
              <th>State</th>
              <th>Ctx</th>
              <th>Reg</th>
              <th>Tim</th>
              <th>Trn</th>
              <th>Arc</th>
              <th>Clu</th>
              <th>Per</th>
              <th>Frs</th>
              <th>Cons</th>
              <th>Agr</th>
              <th>Cnf</th>
              <th>Dom</th>
              <th>Src-Layer</th>
              <th>Cmp-Layer</th>
              <th>Ver</th>
              <th>Attr</th>
              <th>Comp</th>
              <th>Fam</th>
              <th>When</th>
            </tr>
          </thead>
          <tbody>
            {conflictReplayValidationSummary.slice(0, 25).map((r) => (
              <tr key={`${r.source_run_id}-${r.replay_run_id}`}>
                <td className="small">{shortId(r.source_run_id)} → {shortId(r.replay_run_id)}</td>
                <td className={VALIDATION_BADGE[r.validation_state] ?? "badge-muted"}>
                  {r.validation_state}
                </td>
                <td className={tickClass(r.context_hash_match)}>{tick(r.context_hash_match)}</td>
                <td className={tickClass(r.regime_match)}>{tick(r.regime_match)}</td>
                <td className={tickClass(r.timing_class_match)}>{tick(r.timing_class_match)}</td>
                <td className={tickClass(r.transition_state_match && r.sequence_class_match)}>
                  {tick(r.transition_state_match && r.sequence_class_match)}
                </td>
                <td className={tickClass(r.archetype_match)}>{tick(r.archetype_match)}</td>
                <td className={tickClass(r.cluster_state_match)}>{tick(r.cluster_state_match)}</td>
                <td className={tickClass(r.persistence_state_match)}>{tick(r.persistence_state_match)}</td>
                <td className={tickClass(r.freshness_state_match)}>{tick(r.freshness_state_match)}</td>
                <td className={tickClass(r.layer_consensus_state_match)}>{tick(r.layer_consensus_state_match)}</td>
                <td className={tickClass(r.agreement_score_match)}>{tick(r.agreement_score_match)}</td>
                <td className={tickClass(r.conflict_score_match)}>{tick(r.conflict_score_match)}</td>
                <td className={tickClass(r.dominant_conflict_source_match)}>{tick(r.dominant_conflict_source_match)}</td>
                <td className={tickClass(r.source_contribution_layer_match)}>{tick(r.source_contribution_layer_match)}</td>
                <td className={tickClass(r.source_composite_layer_match)}>{tick(r.source_composite_layer_match)}</td>
                <td className={tickClass(r.scoring_version_match)}>{tick(r.scoring_version_match)}</td>
                <td className={tickClass(r.conflict_attribution_match)}>{tick(r.conflict_attribution_match)}</td>
                <td className={tickClass(r.conflict_composite_match)}>{tick(r.conflict_composite_match)}</td>
                <td className={tickClass(r.conflict_dominant_family_match)}>{tick(r.conflict_dominant_family_match)}</td>
                <td>{fmtTs(r.created_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {/* ── Section 2: Family stability ───────────────────────────────── */}
      <h3>Family Conflict Replay Stability</h3>
      {familyConflictReplayStabilitySummary.length === 0 ? (
        <p className="muted">No family conflict replay stability rows yet.</p>
      ) : (
        <table className="dataTable">
          <thead>
            <tr>
              <th>Source → Replay</th>
              <th>Family</th>
              <th>Src consensus</th>
              <th>Rep consensus</th>
              <th>Src Agr</th>
              <th>Rep Agr</th>
              <th>Src Cnf</th>
              <th>Rep Cnf</th>
              <th>Src Dom</th>
              <th>Rep Dom</th>
              <th>Δ Attr</th>
              <th>Δ Integ</th>
              <th>Cons</th>
              <th>Rank-Attr</th>
              <th>Rank-Comp</th>
              <th>When</th>
            </tr>
          </thead>
          <tbody>
            {familyConflictReplayStabilitySummary.slice(0, 50).map((r, i) => (
              <tr key={`${r.source_run_id}-${r.replay_run_id}-${r.dependency_family}-${i}`}>
                <td className="small">{shortId(r.source_run_id)} → {shortId(r.replay_run_id)}</td>
                <td>{r.dependency_family}</td>
                <td className="small">{r.source_family_consensus_state ?? "—"}</td>
                <td className="small">{r.replay_family_consensus_state ?? "—"}</td>
                <td>{fmtScore(r.source_agreement_score, 3)}</td>
                <td>{fmtScore(r.replay_agreement_score, 3)}</td>
                <td>{fmtScore(r.source_conflict_score, 3)}</td>
                <td>{fmtScore(r.replay_conflict_score, 3)}</td>
                <td className="small">{r.source_dominant_conflict_source ?? "—"}</td>
                <td className="small">{r.replay_dominant_conflict_source ?? "—"}</td>
                <td>{fmtScore(r.conflict_adjusted_delta)}</td>
                <td>{fmtScore(r.conflict_integration_delta)}</td>
                <td className={tickClass(r.family_consensus_state_match)}>{tick(r.family_consensus_state_match)}</td>
                <td className={tickClass(r.conflict_family_rank_match)}>{tick(r.conflict_family_rank_match)}</td>
                <td className={tickClass(r.conflict_composite_family_rank_match)}>{tick(r.conflict_composite_family_rank_match)}</td>
                <td>{fmtTs(r.created_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {/* ── Section 3: Aggregate stability ───────────────────────────── */}
      <h3>Conflict Replay Stability Aggregate</h3>
      {!conflictReplayStabilityAggregate ? (
        <p className="muted">No aggregate yet.</p>
      ) : (
        <table className="dataTable">
          <tbody>
            <tr><th>Validations</th><td>{conflictReplayStabilityAggregate.validation_count}</td></tr>
            <tr><th>Drift detected</th><td>{conflictReplayStabilityAggregate.drift_detected_count}</td></tr>
            <tr><th>Latest validated</th><td>{fmtTs(conflictReplayStabilityAggregate.latest_validated_at)}</td></tr>
            <tr><th>Context match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.context_match_rate)}</td></tr>
            <tr><th>Regime match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.regime_match_rate)}</td></tr>
            <tr><th>Timing class match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.timing_class_match_rate)}</td></tr>
            <tr><th>Transition state match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.transition_state_match_rate)}</td></tr>
            <tr><th>Sequence class match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.sequence_class_match_rate)}</td></tr>
            <tr><th>Archetype match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.archetype_match_rate)}</td></tr>
            <tr><th>Cluster state match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.cluster_state_match_rate)}</td></tr>
            <tr><th>Persistence state match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.persistence_state_match_rate)}</td></tr>
            <tr><th>Freshness state match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.freshness_state_match_rate)}</td></tr>
            <tr><th>Layer consensus match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.layer_consensus_state_match_rate)}</td></tr>
            <tr><th>Agreement score match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.agreement_score_match_rate)}</td></tr>
            <tr><th>Conflict score match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.conflict_score_match_rate)}</td></tr>
            <tr><th>Dominant conflict source match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.dominant_conflict_source_match_rate)}</td></tr>
            <tr><th>Source contribution layer match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.source_contribution_layer_match_rate)}</td></tr>
            <tr><th>Source composite layer match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.source_composite_layer_match_rate)}</td></tr>
            <tr><th>Scoring version match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.scoring_version_match_rate)}</td></tr>
            <tr><th>Conflict attribution match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.conflict_attribution_match_rate)}</td></tr>
            <tr><th>Conflict composite match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.conflict_composite_match_rate)}</td></tr>
            <tr><th>Conflict dominant family match rate</th><td>{fmtRate(conflictReplayStabilityAggregate.conflict_dominant_family_match_rate)}</td></tr>
          </tbody>
        </table>
      )}
    </section>
  );
}
