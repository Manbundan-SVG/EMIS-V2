import { NextRequest, NextResponse } from "next/server";
import { getRunScopeInspection } from "@/lib/queries/runs";

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ runId: string }> },
) {
  try {
    const { runId } = await context.params;
    const scope = await getRunScopeInspection(runId);
    return NextResponse.json({ ok: true, scope });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "Unknown run scope inspection error" },
      { status: 500 },
    );
  }
}
