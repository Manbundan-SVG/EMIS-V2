import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";
import { getGovernanceRoutingRecommendations } from "@/lib/queries/metrics";

export async function GET(request: NextRequest) {
  try {
    const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
    const caseId = request.nextUrl.searchParams.get("caseId") ?? undefined;
    const data = await getGovernanceRoutingRecommendations(workspace, caseId);
    return NextResponse.json({ ok: true, ...data });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json() as {
      recommendationId?: string;
      accepted?: boolean;
      acceptedBy?: string | null;
      overrideReason?: string | null;
      applied?: boolean;
    };

    if (!body.recommendationId) throw new Error("recommendationId is required");
    if (typeof body.accepted !== "boolean") throw new Error("accepted must be a boolean");

    const supabase = createServiceSupabaseClient();
    const update = await supabase
      .from("governance_routing_recommendations")
      .update({
        accepted: body.accepted,
        accepted_at: new Date().toISOString(),
        accepted_by: body.acceptedBy ?? null,
        override_reason: body.overrideReason ?? null,
        applied: body.applied ?? false,
        applied_at: body.applied ? new Date().toISOString() : null,
        updated_at: new Date().toISOString(),
      })
      .eq("id", body.recommendationId)
      .select("*")
      .single();

    if (update.error) throw new Error(update.error.message);

    return NextResponse.json({ ok: true, recommendation: update.data });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
