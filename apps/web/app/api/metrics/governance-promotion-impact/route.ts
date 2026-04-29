import { NextRequest, NextResponse } from "next/server";
import { getGovernanceIncidentAnalyticsMetrics } from "@/lib/queries/metrics";

export async function GET(request: NextRequest) {
  try {
    const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
    const data = await getGovernanceIncidentAnalyticsMetrics(workspace);
    return NextResponse.json({
      ok: true,
      thresholdPromotionImpact: data.thresholdPromotionImpact,
      routingPromotionImpact: data.routingPromotionImpact,
      rollbackRisk: data.rollbackRisk,
    });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
