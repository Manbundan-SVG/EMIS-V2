import { NextRequest, NextResponse } from "next/server";
import { getReplayDelta } from "@/lib/queries/runs";

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ runId: string }> },
) {
  try {
    const { runId } = await context.params;
    const replayDelta = await getReplayDelta(runId);
    return NextResponse.json({ ok: true, replayDelta });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "Unknown replay delta error" },
      { status: 500 },
    );
  }
}
