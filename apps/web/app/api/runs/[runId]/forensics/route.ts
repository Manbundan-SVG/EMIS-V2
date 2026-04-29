import { NextRequest, NextResponse } from "next/server";
import { getRunInputSnapshot, getRunStageTimings } from "@/lib/queries/runs";

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ runId: string }> },
) {
  try {
    const { runId } = await context.params;
    const [stageTimings, inputSnapshot] = await Promise.all([
      getRunStageTimings(runId),
      getRunInputSnapshot(runId),
    ]);
    return NextResponse.json({ ok: true, stageTimings, inputSnapshot });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "Unknown run forensics error" },
      { status: 500 },
    );
  }
}
