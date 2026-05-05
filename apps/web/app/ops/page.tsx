// Phase Frontend V1.0 — Ops Intelligence Console
//
// Entry point for the new operational dashboard. The previous panel-dump
// view (every cross-asset surface stacked vertically) lives at
// /ops/legacy. The new V1 console is summary-first and tab-organized.

import IntelligenceConsole from "@/components/ops/intelligence-console";

export const dynamic = "force-dynamic";

export default async function OpsPage({
  searchParams,
}: {
  searchParams: Promise<{ workspace?: string }>;
}) {
  const params = await searchParams;
  const workspaceSlug = params.workspace
    ?? process.env.NEXT_PUBLIC_DEFAULT_WORKSPACE_SLUG
    ?? "demo";
  return <IntelligenceConsole workspaceSlug={workspaceSlug} />;
}
