import { NextRequest, NextResponse } from "next/server";
import { getWorkerHeartbeats } from "@/lib/queries/metrics";

export async function GET(request: NextRequest) {
  const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
  try {
    const rows = await getWorkerHeartbeats(workspace);
    return NextResponse.json({ ok: true, rows });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
