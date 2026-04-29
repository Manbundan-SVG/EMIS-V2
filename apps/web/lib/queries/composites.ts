import { z } from "zod";
import { createServiceSupabaseClient } from "@/lib/supabase";

const compositeRowSchema = z.object({
  id: z.string().uuid(),
  workspace_id: z.string().uuid(),
  asset_id: z.string().uuid(),
  timestamp: z.string(),
  regime: z.string(),
  long_score: z.number(),
  short_score: z.number(),
  confidence: z.number(),
  invalidators: z.record(z.any()).nullable(),
  created_at: z.string(),
  assets: z.object({ symbol: z.string(), name: z.string(), asset_class: z.string() })
});

export type CompositeRow = z.infer<typeof compositeRowSchema>;

export async function listLatestComposites(workspaceSlug: string, limit = 25) {
  const supabase = createServiceSupabaseClient();
  type WsResult = { data: { id: string } | null; error: { message: string } | null };
  const wsQuery = await supabase
    .from("workspaces").select("id").eq("slug", workspaceSlug).single() as unknown as WsResult;
  if (wsQuery.error || !wsQuery.data) throw new Error(`Workspace not found for slug=${workspaceSlug}`);
  const { data, error } = await supabase
    .from("composite_scores")
    .select(`
      id, workspace_id, asset_id, timestamp, regime, long_score, short_score, confidence, invalidators, created_at,
      assets:asset_id (symbol, name, asset_class)
    `)
    .eq("workspace_id", wsQuery.data.id)
    .order("timestamp", { ascending: false })
    .limit(limit);
  if (error) throw new Error(`Failed to load composites: ${error.message}`);
  return z.array(compositeRowSchema).parse(data);
}
