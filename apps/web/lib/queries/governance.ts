import { createServiceSupabaseClient } from "@/lib/supabase";

export interface QueueGovernanceRuleRow {
  id: number;
  workspace_id: string;
  watchlist_id: string | null;
  enabled: boolean;
  job_type: string;
  max_concurrent: number;
  dedupe_window_seconds: number;
  suppress_if_queued: boolean;
  suppress_if_claimed: boolean;
  manual_priority: number;
  scheduled_priority: number;
  created_at: string;
  updated_at: string;
}

export interface AlertPolicyRuleRow {
  id: number;
  workspace_id: string;
  watchlist_id: string | null;
  enabled: boolean;
  event_type: string;
  severity: string;
  channel: string;
  notify_on_terminal_only: boolean;
  cooldown_seconds: number;
  dedupe_key_template: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

async function getWorkspaceId(workspaceSlug: string): Promise<string> {
  const supabase = createServiceSupabaseClient();
  type WorkspaceQueryResult = { data: { id: string } | null; error: { message: string } | null };

  const result = await supabase
    .from("workspaces")
    .select("id")
    .eq("slug", workspaceSlug)
    .single() as unknown as WorkspaceQueryResult;

  if (result.error || !result.data) {
    throw new Error(`Workspace not found: ${workspaceSlug}`);
  }

  return result.data.id;
}

export async function listQueueGovernanceRules(workspaceSlug: string): Promise<QueueGovernanceRuleRow[]> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const { data, error } = await supabase
    .from("queue_governance_rules")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("watchlist_id", { ascending: true, nullsFirst: false });

  if (error) throw new Error(`Queue governance rules error: ${error.message}`);
  return (data ?? []) as QueueGovernanceRuleRow[];
}

export async function listAlertPolicyRules(workspaceSlug: string): Promise<AlertPolicyRuleRow[]> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const { data, error } = await supabase
    .from("alert_policy_rules")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("watchlist_id", { ascending: true, nullsFirst: false })
    .order("event_type", { ascending: true });

  if (error) throw new Error(`Alert policy rules error: ${error.message}`);
  return (data ?? []) as AlertPolicyRuleRow[];
}
