import OpsDashboardShell from "@/components/ops-dashboard-shell";

export const dynamic = "force-dynamic";

export default async function OpsPage({
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
