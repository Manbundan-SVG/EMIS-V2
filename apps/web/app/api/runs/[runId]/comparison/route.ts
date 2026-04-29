import { NextRequest, NextResponse } from "next/server";
import { getRunPriorComparison } from "@/lib/queries/runs";

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ runId: string }> },
) {
  try {
    const { runId } = await context.params;
    const comparison = await getRunPriorComparison(runId);
    return NextResponse.json({ ok: true, comparison });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "Unknown run comparison error" },
      { status: 500 },
    );
  }
}
