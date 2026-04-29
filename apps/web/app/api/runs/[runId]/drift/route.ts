import { NextRequest, NextResponse } from "next/server";
import { getRunDrift } from "@/lib/queries/runs";

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ runId: string }> },
) {
  try {
    const { runId } = await context.params;
    const drift = await getRunDrift(runId);
    return NextResponse.json({ ok: true, drift });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "Unknown run drift error" },
      { status: 500 },
    );
  }
}
