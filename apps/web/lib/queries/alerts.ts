import { createServiceSupabaseClient } from "@/lib/supabase";

export interface AlertEventRow {
  id: string;
  workspace_id: string;
  job_id: string | null;
  alert_type: string;
  severity: string;
  title: string;
  message: string;
  payload: Record<string, unknown>;
  metadata: Record<string, unknown>;
  delivered_channels: unknown[];
  created_at: string;
}

export async function listRecentAlerts(workspaceSlug: string, limit = 20): Promise<AlertEventRow[]> {
  const supabase = createServiceSupabaseClient();

  type WsResult = { data: { id: string } | null; error: { message: string } | null };
  const wsResult = await supabase
    .from("workspaces").select("id").eq("slug", workspaceSlug).single() as unknown as WsResult;
  if (wsResult.error || !wsResult.data) throw new Error(`Workspace not found: ${workspaceSlug}`);
  const workspaceId = wsResult.data.id;

  const { data, error } = await supabase
    .from("alert_events")
    .select("id, workspace_id, job_id, alert_type, severity, title, message, payload, metadata, delivered_channels, created_at")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(limit);

  if (error) throw new Error(`Failed to load alerts: ${error.message}`);
  return (data ?? []) as AlertEventRow[];
}
