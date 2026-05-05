"use client";

// Phase Frontend V1.0 — Executive Summary
//
// Eight cards summarizing the current state of the EMIS cross-asset
// intelligence stack. Designed as the highest-level "what's the state
// right now" view; everything else in the console drills down from
// these cards.

import type { OpsIntelligence } from "@/lib/types/ops-intelligence";

const CONSENSUS_BADGE: Record<string, string> = {
  aligned_supportive:   "badge-green",
  aligned_suppressive:  "badge-red",
  partial_agreement:    "badge-yellow",
  conflicted:           "badge-red",
  unreliable:           "badge-red",
  insufficient_context: "badge-muted",
};

const FRESHNESS_BADGE: Record<string, string> = {
  fresh:                "badge-green",
  decaying:             "badge-yellow",
  stale:                "badge-red",
  contradicted:         "badge-red",
  mixed:                "badge-yellow",
  insufficient_history: "badge-muted",
};

function fmtScore(value: number | null | undefined, digits = 4): string {
  if (value === null || value === undefined) return "—";
  return Number(value).toFixed(digits);
}

function fmtRate(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return `${(Number(value) * 100).toFixed(1)}%`;
}

function fmtTs(ts: string | null | undefined): string {
  if (!ts) return "—";
  return new Date(ts).toLocaleString(undefined, { dateStyle: "short", timeStyle: "short" });
}

function fmtInt(v: number | null | undefined): string {
  if (v === null || v === undefined) return "—";
  return String(Math.round(v));
}

export function ExecutiveSummary({ intelligence }: { intelligence: OpsIntelligence }) {
  const c = intelligence.cards;
  const post = c.composite.postConflict;
  const post_signal_class =
    post === null ? "" : post >= 0 ? "signal-positive" : "signal-negative";

  return (
    <section className="section">
      <h2 className="section-title">Executive Summary</h2>
      <div className="muted text-sm" style={{ marginBottom: 12 }}>
        Latest run: {intelligence.latestRunId ? `${intelligence.latestRunId.slice(0, 8)}…` : "—"}
        {" · "}
        {fmtTs(intelligence.latestRunCreatedAt)}
        {intelligence.metadata.scoringVersion ? ` · ${intelligence.metadata.scoringVersion}` : ""}
      </div>

      <div className="grid grid-4">
        {/* 1. Composite score (post-conflict) */}
        <div className="card">
          <div className="kpi-label">Final composite score</div>
          <div className={`kpi-value ${post_signal_class}`}>{fmtScore(c.composite.postConflict)}</div>
          <div className="kpi-sub">
            pre {fmtScore(c.composite.preConflict)} · Δ {fmtScore(c.composite.netContribution)}
          </div>
        </div>

        {/* 2. Layer consensus state */}
        <div className="card">
          <div className="kpi-label">Layer consensus</div>
          <div>
            <span className={CONSENSUS_BADGE[c.layerConsensus.state ?? ""] ?? "badge-muted"}>
              {c.layerConsensus.state ?? "—"}
            </span>
          </div>
          <div className="kpi-sub" style={{ marginTop: 8 }}>
            agreement {fmtScore(c.layerConsensus.agreementScore, 3)}
          </div>
        </div>

        {/* 3. Conflict score */}
        <div className="card">
          <div className="kpi-label">Conflict score</div>
          <div className="kpi-value">{fmtScore(c.conflict.score, 3)}</div>
          <div className="kpi-sub">
            dominant: {c.conflict.dominantSource ?? "—"}
          </div>
        </div>

        {/* 4. Freshness state */}
        <div className="card">
          <div className="kpi-label">Freshness</div>
          <div>
            <span className={FRESHNESS_BADGE[c.freshness.state ?? ""] ?? "badge-muted"}>
              {c.freshness.state ?? "—"}
            </span>
          </div>
          <div className="kpi-sub" style={{ marginTop: 8 }}>
            decay {fmtScore(c.freshness.aggregateDecayScore, 3)}
            {c.freshness.staleMemoryFlag ? " · stale" : ""}
            {c.freshness.contradictionFlag ? " · contradicted" : ""}
          </div>
        </div>

        {/* 5. Persistence state */}
        <div className="card">
          <div className="kpi-label">Persistence</div>
          <div className="kpi-value-sm">{c.persistence.state ?? "—"}</div>
          <div className="kpi-sub">across cross-asset stack</div>
        </div>

        {/* 6. Replay state */}
        <div className="card">
          <div className="kpi-label">Replay stability</div>
          <div className="kpi-value-sm">
            {fmtRate(c.replay.contextMatchRate)} ctx · {fmtRate(c.replay.conflictCompositeMatchRate)} cmp
          </div>
          <div className="kpi-sub">
            {fmtInt(c.replay.validationCount)} validations
            {c.replay.driftDetectedCount ? ` · ${fmtInt(c.replay.driftDetectedCount)} drifts` : ""}
          </div>
        </div>

        {/* 7. Dominant family */}
        <div className="card">
          <div className="kpi-label">Dominant family</div>
          <div className="kpi-value-sm">{c.dominantFamily.conflict ?? c.dominantFamily.decay ?? c.dominantFamily.raw ?? "—"}</div>
          <div className="kpi-sub">
            raw → conflict: {c.dominantFamily.raw ?? "—"} → {c.dominantFamily.conflict ?? "—"}
          </div>
        </div>

        {/* 8. Latest event */}
        <div className="card">
          <div className="kpi-label">Latest layer-conflict event</div>
          <div className="kpi-value-sm">{c.latestEvent.type ?? "—"}</div>
          <div className="kpi-sub">
            {c.latestEvent.family ?? "—"} · {fmtTs(c.latestEvent.ts)}
          </div>
        </div>
      </div>

      {/* Replay-readiness footer */}
      <div className="muted text-xs" style={{ marginTop: 12 }}>
        Source contribution: {intelligence.metadata.sourceContributionLayer ?? "—"}
        {" · "}
        Source composite: {intelligence.metadata.sourceCompositeLayer ?? "—"}
        {" · "}
        Mode: {intelligence.metadata.integrationMode ?? "—"}
      </div>
    </section>
  );
}
