import { NextRequest, NextResponse } from "next/server";
import { getGovernanceRoutingQualityMetrics } from "@/lib/queries/metrics";

export async function GET(request: NextRequest) {
  const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
  const data = await getGovernanceRoutingQualityMetrics(workspace);
  return NextResponse.json({ ok: true, ...data });
}
