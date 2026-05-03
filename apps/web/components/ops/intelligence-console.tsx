"use client";

// Phase Frontend V1.0 — Ops Intelligence Console
//
// Top-level shell for the new operational dashboard. Replaces the
// kitchen-sink panel dump (now at /ops/legacy) with a tabbed,
// summary-first view.
//
// V1.0 scope:
//   - Workspace selector (URL param)
//   - Latest-run display (read-only; selector lands in V1.1)
//   - Tab nav: Overview / Data Health / Signals / Attribution / Composite / Replay / Conflict & Decay
//   - Overview tab: ExecutiveSummary (centerpiece)
//   - Attribution tab: AttributionLadder (raw -> final)
//   - Other tabs: V1.1 placeholder linking to /ops/legacy
//   - Loading / empty / error states

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import { ExecutiveSummary } from "@/components/ops/executive-summary";
import { AttributionLadder } from "@/components/ops/attribution-ladder";
import type { OpsIntelligence } from "@/lib/types/ops-intelligence";

type Tab =
  | "overview"
  | "data_health"
  | "signals"
  | "attribution"
  | "composite"
  | "replay"
  | "conflict_decay";

const TABS: Array<{ key: Tab; label: string }> = [
  { key: "overview",      label: "Overview" },
  { key: "data_health",   label: "Data Health" },
  { key: "signals",       label: "Signals" },
  { key: "attribution",   label: "Attribution" },
  { key: "composite",     label: "Composite" },
  { key: "replay",        label: "Replay" },
  { key: "conflict_decay", label: "Conflict & Decay" },
];

type LoadState = "idle" | "loading" | "loaded" | "error";

export default function IntelligenceConsole({ workspaceSlug }: { workspaceSlug: string }) {
  const [activeTab, setActiveTab] = useState<Tab>("overview");
  const [intelligence, setIntelligence] = useState<OpsIntelligence | null>(null);
  const [state, setState] = useState<LoadState>("idle");
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setState("loading");
    setError(null);
    try {
      const res = await fetch(`/api/metrics/ops-intelligence?workspace=${encodeURIComponent(workspaceSlug)}`, {
        cache: "no-store",
      });
      const data = await res.json() as {
        ok: boolean;
        intelligence: OpsIntelligence | null;
        error?: string;
      };
      if (!data.ok) {
        setError(data.error ?? "unknown error");
        setState("error");
        return;
      }
      setIntelligence(data.intelligence);
      setState("loaded");
    } catch (e) {
      setError(e instanceof Error ? e.message : "fetch error");
      setState("error");
    }
  }, [workspaceSlug]);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <div className="container">
      <div className="header">
        <div>
          <h1 style={{ margin: 0, fontSize: 24, fontWeight: 700 }}>EMIS Intelligence Console</h1>
          <div className="muted text-sm" style={{ marginTop: 4 }}>
            Workspace: <span className="mono-cell">{workspaceSlug}</span>
            {" · "}
            <Link href="/ops/legacy" className="badge">Full Dashboard →</Link>
          </div>
        </div>
        <button className="btn btn-sm" onClick={() => void load()} disabled={state === "loading"}>
          {state === "loading" ? "Loading…" : "Refresh"}
        </button>
      </div>

      {/* Tab nav */}
      <nav style={{ display: "flex", gap: 8, flexWrap: "wrap", borderBottom: "1px solid var(--border)", marginBottom: 24 }}>
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setActiveTab(t.key)}
            style={{
              padding: "10px 14px",
              background: activeTab === t.key ? "rgba(122,162,255,0.15)" : "transparent",
              color: activeTab === t.key ? "var(--accent)" : "var(--muted)",
              border: "none",
              borderBottom: activeTab === t.key ? "2px solid var(--accent)" : "2px solid transparent",
              cursor: "pointer",
              fontSize: 14,
              fontWeight: activeTab === t.key ? 600 : 400,
              fontFamily: "inherit",
            }}
          >
            {t.label}
          </button>
        ))}
      </nav>

      {/* States */}
      {state === "loading" && (
        <div className="card">
          <p className="muted">Loading executive summary…</p>
        </div>
      )}

      {state === "error" && (
        <div className="card">
          <h3 className="section-title">Error</h3>
          <p className="muted">{error ?? "Unable to load ops intelligence."}</p>
          <button className="btn btn-sm" onClick={() => void load()}>Retry</button>
        </div>
      )}

      {state === "loaded" && !intelligence && (
        <div className="card">
          <p className="muted">No data for workspace {workspaceSlug}.</p>
        </div>
      )}

      {state === "loaded" && intelligence && intelligence.latestRunId === null && (
        <div className="card">
          <h3 className="section-title">Awaiting first run</h3>
          <p className="muted">
            No conflict-aware composite snapshot has been produced yet for this workspace.
            Once a recompute pass completes, the Executive Summary will populate here.
          </p>
        </div>
      )}

      {state === "loaded" && intelligence && intelligence.latestRunId !== null && (
        <>
          {activeTab === "overview" && <ExecutiveSummary intelligence={intelligence} />}
          {activeTab === "attribution" && <AttributionLadder intelligence={intelligence} />}
          {activeTab !== "overview" && activeTab !== "attribution" && (
            <DrilldownStub label={TABS.find((t) => t.key === activeTab)?.label ?? "section"} workspaceSlug={workspaceSlug} />
          )}
        </>
      )}
    </div>
  );
}

function DrilldownStub({ label, workspaceSlug }: { label: string; workspaceSlug: string }) {
  return (
    <section className="section">
      <h2 className="section-title">{label}</h2>
      <div className="card">
        <p className="muted">
          Detailed {label.toLowerCase()} drilldown is scheduled for V1.1.
        </p>
        <p className="muted" style={{ marginTop: 12 }}>
          For now, the legacy panel-dump view exposes everything:
        </p>
        <p style={{ marginTop: 12 }}>
          <Link href={`/ops/legacy?workspace=${encodeURIComponent(workspaceSlug)}`} className="btn btn-sm">
            Open full dashboard →
          </Link>
        </p>
      </div>
    </section>
  );
}
