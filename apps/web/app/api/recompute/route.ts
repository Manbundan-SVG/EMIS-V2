import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";
import { enqueueRecomputeJob } from "@/lib/queries/jobs";

const requestSchema = z.object({
  workspaceSlug: z.string().min(1),
  watchlistSlug: z.string().min(1).nullable().optional(),
  requestedBy: z.string().nullable().optional(),
  assetSymbols: z.array(z.string()).optional(),
  horizon: z.enum(["intraday", "swing", "daily"]).optional()
});

export async function POST(request: NextRequest) {
  try {
    const body = requestSchema.parse(await request.json());
    const result = await enqueueRecomputeJob({
      workspaceSlug: body.workspaceSlug,
      watchlistSlug: body.watchlistSlug ?? null,
      triggerType: "api",
      requestedBy: body.requestedBy ?? "web-api",
      payload: {
        assetSymbols: body.assetSymbols ?? [],
        horizon: body.horizon ?? "intraday",
        watchlistSlug: body.watchlistSlug ?? null,
      }
    });
    if (!result.allowed) {
      return NextResponse.json({
        ok: true,
        allowed: false,
        reason: result.reason,
        workspaceId: result.workspace_id,
        watchlistId: result.watchlist_id,
        assignedPriority: result.assigned_priority,
      });
    }

    return NextResponse.json({
      ok: true,
      allowed: true,
      reason: result.reason,
      jobId: result.job_id,
      workspaceId: result.workspace_id,
      queueId: result.queue_id,
      watchlistId: result.watchlist_id,
      assignedPriority: result.assigned_priority,
    }, { status: 202 });
  } catch (error) {
    return NextResponse.json({ ok: false, error: error instanceof Error ? error.message : "Unknown recompute error" }, { status: 400 });
  }
}
