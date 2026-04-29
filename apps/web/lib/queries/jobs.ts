import { z } from "zod";
import { createServiceSupabaseClient } from "@/lib/supabase";

const enqueueJobResultSchema = z.object({
  allowed: z.boolean(),
  reason: z.string(),
  assigned_priority: z.number(),
  job_id: z.string().uuid().nullable(),
  workspace_id: z.string().uuid(),
  queue_id: z.number().nullable(),
  watchlist_id: z.string().uuid().nullable(),
});

const jobRunSchema = z.object({
  id: z.string().uuid(),
  workspace_id: z.string().uuid(),
  watchlist_id: z.string().uuid().nullable(),
  queue_name: z.string(),
  status: z.enum(["queued", "claimed", "running", "completed", "failed", "dead_lettered"]),
  trigger_type: z.enum(["api", "seed", "cron", "manual"]),
  requested_by: z.string().nullable(),
  payload: z.record(z.any()),
  metadata: z.record(z.any()),
  attempt_count: z.number(),
  max_attempts: z.number(),
  claimed_by: z.string().nullable(),
  claimed_at: z.string().nullable(),
  started_at: z.string().nullable(),
  finished_at: z.string().nullable(),
  queue_id: z.number().nullable(),
  lineage: z.record(z.any()),
  compute_version: z.string().nullable(),
  signal_registry_version: z.string().nullable(),
  model_version: z.string().nullable(),
  runtime_ms: z.number().nullable(),
  completed_at: z.string().nullable(),
  error_message: z.string().nullable(),
  created_at: z.string(),
  updated_at: z.string()
});

export type JobRun = z.infer<typeof jobRunSchema>;
export type EnqueueRecomputeResult = z.infer<typeof enqueueJobResultSchema>;

export async function enqueueRecomputeJob(args: {
  workspaceSlug: string;
  watchlistSlug?: string | null;
  triggerType?: "api" | "seed" | "cron" | "manual";
  requestedBy?: string | null;
  payload?: Record<string, unknown>;
}): Promise<EnqueueRecomputeResult> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase.rpc("enqueue_governed_recompute", {
    p_workspace_slug: args.workspaceSlug,
    p_watchlist_slug: args.watchlistSlug ?? null,
    p_trigger_type: args.triggerType ?? "api",
    p_requested_by: args.requestedBy ?? null,
    p_payload: args.payload ?? {}
  });
  if (error) throw new Error(`Failed to enqueue governed recompute job: ${error.message}`);
  return z.array(enqueueJobResultSchema).parse(data)[0];
}

export async function listRecentJobs(workspaceSlug: string, limit = 20) {
  const supabase = createServiceSupabaseClient();
  type WsResult = { data: { id: string } | null; error: { message: string } | null };
  const wsQuery = await supabase
    .from("workspaces").select("id").eq("slug", workspaceSlug).single() as unknown as WsResult;
  if (wsQuery.error || !wsQuery.data) throw new Error(`Workspace not found for slug=${workspaceSlug}`);
  const { data, error } = await supabase
    .from("job_runs").select("*").eq("workspace_id", wsQuery.data.id)
    .order("created_at", { ascending: false }).limit(limit);
  if (error) throw new Error(`Failed to list jobs: ${error.message}`);
  return z.array(jobRunSchema).parse(data);
}
