import { NextRequest, NextResponse } from "next/server";
import { getRegimeTransition } from "@/lib/queries/runs";

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ runId: string }> },
) {
  try {
    const { runId } = await context.params;
    const regimeTransition = await getRegimeTransition(runId);
    return NextResponse.json({ ok: true, regimeTransition });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "Unknown regime transition error" },
      { status: 500 },
    );
  }
}

