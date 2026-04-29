import { NextRequest, NextResponse } from "next/server";
import { getCrossAssetTimingReplayValidationMetrics } from "@/lib/queries/metrics";

export async function GET(request: NextRequest) {
  try {
    const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
    const watchlist = request.nextUrl.searchParams.get("watchlist") ?? undefined;
    const data = await getCrossAssetTimingReplayValidationMetrics(workspace, watchlist);
    return NextResponse.json({ ok: true, ...data });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
