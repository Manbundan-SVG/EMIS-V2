import { NextRequest, NextResponse } from "next/server";
import { getRunAttribution } from "@/lib/queries/runs";

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ runId: string }> },
) {
  try {
    const { runId } = await context.params;
    const attribution = await getRunAttribution(runId);
    return NextResponse.json({ ok: true, attribution });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "Unknown run attribution error" },
      { status: 500 },
    );
  }
}
