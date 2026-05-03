// Phase Frontend V1.0 — Legacy ops dashboard
//
// The previous /ops view: a single shell that stacks every cross-asset
// surface vertically. Preserved here so power users can still see all
// panels at once while the V1 Intelligence Console replaces /ops as the
// default landing.

import OpsDashboardShell from "@/components/ops-dashboard-shell";

export const dynamic = "force-dynamic";

export default async function OpsLegacyPage({
  searchParams,
}: {
  searchParams: Promise<{ workspace?: string }>;
}) {
  const params = await searchParams;
  return (
    <OpsDashboardShell
      workspaceSlug={params.workspace ?? process.env.NEXT_PUBLIC_DEFAULT_WORKSPACE_SLUG ?? "demo"}
    />
  );
}
