import { createServiceSupabaseClient } from "@/lib/supabase";

export interface DeadLetterRow {
  id: number;
  job_run_id: string | null;
  queue_job_id: number | null;
  workspace_id: string;
  watchlist_id: string | null;
  job_type: string;
  payload: Record<string, unknown>;
  retry_count: number;
  max_retries: number;
  last_error: string | null;
  failure_stage: string | null;
  failed_at: string;
  requeued_at: string | null;
  metadata: Record<string, unknown>;
}

export async function listDeadLetters(workspaceSlug: string, limit = 50): Promise<DeadLetterRow[]> {
  const supabase = createServiceSupabaseClient();

  type WsResult = { data: { id: string } | null; error: { message: string } | null };
  const wsQuery = await supabase
    .from("workspaces").select("id").eq("slug", workspaceSlug).single() as unknown as WsResult;
  if (wsQuery.error || !wsQuery.data) throw new Error(`Workspace not found: ${workspaceSlug}`);
  const ws = wsQuery.data;

  const { data, error } = await supabase
    .from("job_dead_letters")
    .select("*")
    .eq("workspace_id", ws.id)
    .order("failed_at", { ascending: false })
    .limit(limit);

  if (error) throw new Error(`Failed to load dead letters: ${error.message}`);
  return (data ?? []) as DeadLetterRow[];
}

export async function requeueDeadLetter(deadLetterId: number, resetRetryCount = false): Promise<string> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase.rpc("requeue_dead_letter", {
    p_dead_letter_id: deadLetterId,
    p_reset_retry_count: resetRetryCount,
  });
  if (error) throw new Error(`Failed to requeue dead letter: ${error.message}`);
  return data as string;
}
