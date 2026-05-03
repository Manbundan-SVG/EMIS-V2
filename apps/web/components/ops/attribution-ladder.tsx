"use client";

// Phase Frontend V1.0 — Attribution Ladder
//
// Visualizes the full attribution chain from raw → final composite for
// the latest run. Each rung shows the cross-asset contribution at that
// layer, plus the delta from the previous rung. Drift between rungs is
// the narrative arc of "how the score changed as each refinement layer
// applied".

import type { OpsIntelligence, OpsIntelligenceAttributionLadder } from "@/lib/types/ops-intelligence";

type Rung = {
  key: keyof OpsIntelligenceAttributionLadder | "post";
  label: string;
  phase: string;
  description: string;
};

const RUNGS: Rung[] = [
  { key: "raw",         label: "Raw attribution",                phase: "4.1A", description: "cross-asset net contribution before weighting" },
  { key: "weighted",    label: "Weighted",                       phase: "4.1B", description: "dependency-priority weighting applied" },
  { key: "regime",      label: "Regime-aware",                   phase: "4.1C", description: "regime context applied" },
  { key: "timing",      label: "Timing-aware",                   phase: "4.2B", description: "timing class applied" },
  { key: "transition",  label: "Transition-aware",               phase: "4.3B", description: "transition / sequence applied" },
  { key: "archetype",   label: "Archetype-aware",                phase: "4.4B", description: "archetype identity applied" },
  { key: "cluster",     label: "Cluster-aware",                  phase: "4.5B", description: "cluster state applied" },
  { key: "persistence", label: "Persistence-aware",              phase: "4.6B", description: "persistence applied" },
  { key: "decay",       label: "Decay-aware",                    phase: "4.7B", description: "freshness / decay applied" },
  { key: "conflict",    label: "Conflict-aware (final attr.)",   phase: "4.8B", description: "layer consensus applied" },
  { key: "compositePre", label: "Composite (pre-conflict)",      phase: "4.7C", description: "decay composite — final pre-4.8C" },
  { key: "compositePost", label: "Composite (post-conflict)",    phase: "4.8C", description: "final integrated composite" },
];

function fmt(value: number | null | undefined, digits = 4): string {
  if (value === null || value === undefined) return "—";
  return Number(value).toFixed(digits);
}

function delta(curr: number | null, prev: number | null): { value: number | null; sign: "" | "signal-positive" | "signal-negative" } {
  if (curr === null || prev === null) return { value: null, sign: "" };
  const d = curr - prev;
  if (Math.abs(d) < 1e-9) return { value: 0, sign: "" };
  return { value: d, sign: d > 0 ? "signal-positive" : "signal-negative" };
}

export function AttributionLadder({ intelligence }: { intelligence: OpsIntelligence }) {
  const ladder = intelligence.attributionLadder;
  const dominants = intelligence.cards.dominantFamily;

  const dominantByKey: Record<string, string | null> = {
    raw: dominants.raw,
    weighted: dominants.weighted,
    regime: dominants.regime,
    timing: dominants.timing,
    transition: dominants.transition,
    archetype: dominants.archetype,
    cluster: dominants.cluster,
    persistence: dominants.persistence,
    decay: dominants.decay,
    conflict: dominants.conflict,
  };

  let prev: number | null = null;
  return (
    <section className="section">
      <h2 className="section-title">Attribution Ladder</h2>
      <p className="muted text-sm" style={{ marginBottom: 12 }}>
        How the cross-asset contribution evolves through each refinement layer for the latest run.
      </p>
      <div className="card" style={{ padding: 0 }}>
        <table className="table">
          <thead>
            <tr>
              <th>Layer</th>
              <th>Phase</th>
              <th style={{ textAlign: "right" }}>Contribution</th>
              <th style={{ textAlign: "right" }}>Δ from prev</th>
              <th>Dominant family</th>
              <th>Notes</th>
            </tr>
          </thead>
          <tbody>
            {RUNGS.map((rung) => {
              const value = (ladder[rung.key as keyof OpsIntelligenceAttributionLadder] as number | null) ?? null;
              const { value: dv, sign } = delta(value, prev);
              prev = value;
              const dom = dominantByKey[rung.key as string] ?? null;
              return (
                <tr key={rung.key as string}>
                  <td>{rung.label}</td>
                  <td className="muted text-xs">{rung.phase}</td>
                  <td className="mono-cell" style={{ textAlign: "right" }}>{fmt(value)}</td>
                  <td className={`mono-cell ${sign}`} style={{ textAlign: "right" }}>
                    {dv === null ? "—" : (dv === 0 ? "0" : (dv > 0 ? "+" : "") + fmt(dv))}
                  </td>
                  <td className="text-sm">{dom ?? "—"}</td>
                  <td className="muted text-xs">{rung.description}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
