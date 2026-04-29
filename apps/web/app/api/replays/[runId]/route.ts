import { NextRequest, NextResponse } from "next/server";
import { enqueueReplayRun } from "@/lib/queries/runs";

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ runId: string }> },
) {
  try {
    const { runId } = await context.params;
    const body = request.headers.get("content-type")?.includes("application/json")
      ? await request.json() as { requestedBy?: string | null }
      : {};
    const result = await enqueueReplayRun(runId, body.requestedBy ?? "ops-dashboard");
    return NextResponse.json(
      {
        ok: true,
        allowed: result.allowed,
        reason: result.reason,
        assignedPriority: result.assigned_priority,
        jobId: result.job_id,
        queueId: result.queue_id,
        workspaceId: result.workspace_id,
        watchlistId: result.watchlist_id,
        replayedFromRunId: result.replayed_from_run_id,
      },
      { status: result.allowed ? 202 : 200 },
    );
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "Unknown replay enqueue error" },
      { status: 400 },
    );
  }
}
