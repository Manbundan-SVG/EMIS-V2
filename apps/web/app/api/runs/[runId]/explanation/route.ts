import { NextRequest, NextResponse } from "next/server";
import { getRunExplanation } from "@/lib/queries/runs";

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ runId: string }> },
) {
  try {
    const { runId } = await context.params;
    const explanation = await getRunExplanation(runId);
    return NextResponse.json({ ok: true, explanation });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "Unknown run explanation error" },
      { status: 500 },
    );
  }
}
