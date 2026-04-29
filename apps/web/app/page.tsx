import DashboardShell from "@/components/dashboard-shell";

export default function Page() {
  return <DashboardShell workspaceSlug={process.env.NEXT_PUBLIC_DEFAULT_WORKSPACE_SLUG ?? "demo"} />;
}
