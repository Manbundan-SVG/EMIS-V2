import { NextRequest, NextResponse } from "next/server";
import { getOpsIntelligence } from "@/lib/queries/ops-intelligence";

export async function GET(request: NextRequest) {
  try {
    const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
    const watchlist = request.nextUrl.searchParams.get("watchlist") ?? undefined;
    const intelligence = await getOpsIntelligence(workspace, watchlist);
    return NextResponse.json({ ok: true, intelligence });
  } catch (error) {
    return NextResponse.json(
      { ok: false, intelligence: null, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
