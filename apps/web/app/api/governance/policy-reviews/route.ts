import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";
import { getGovernancePolicyReviewMetrics } from "@/lib/queries/metrics";

export async function GET(request: NextRequest) {
  try {
    const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
    const data = await getGovernancePolicyReviewMetrics(workspace);
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
      workspace?: string;
      recommendationKey?: string;
      policyFamily?: string;
      reviewStatus?: "approved" | "rejected" | "deferred";
      reviewReason?: string | null;
      reviewedBy?: string;
      notes?: string | null;
    };

    if (!body.recommendationKey) throw new Error("recommendationKey is required");
    if (!body.policyFamily) throw new Error("policyFamily is required");
    if (!body.reviewStatus) throw new Error("reviewStatus is required");
    if (!["approved", "rejected", "deferred"].includes(body.reviewStatus)) {
      throw new Error("reviewStatus must be one of: approved | rejected | deferred");
    }
    if (!body.reviewedBy) throw new Error("reviewedBy is required");

    const supabase = createServiceSupabaseClient();
    const workspace = body.workspace ?? "demo";

    const workspaceLookup = await supabase
      .from("workspaces")
      .select("id")
      .eq("slug", workspace)
      .single();
    if (workspaceLookup.error || !workspaceLookup.data) {
      throw new Error(workspaceLookup.error?.message ?? "workspace not found");
    }
    const workspaceId = workspaceLookup.data.id as string;

    // verify recommendation exists in this workspace
    const recLookup = await supabase
      .from("governance_policy_recommendations")
      .select("recommendation_key, policy_family")
      .eq("workspace_id", workspaceId)
      .eq("recommendation_key", body.recommendationKey)
      .maybeSingle();
    if (recLookup.error) throw new Error(recLookup.error.message);
    if (!recLookup.data) throw new Error("recommendation not found in this workspace");

    const reviewInsert = await supabase
      .from("governance_policy_recommendation_reviews")
      .insert({
        workspace_id: workspaceId,
        recommendation_key: body.recommendationKey,
        policy_family: body.policyFamily,
        review_status: body.reviewStatus,
        review_reason: body.reviewReason ?? null,
        reviewed_by: body.reviewedBy,
        notes: body.notes ?? null,
        metadata: { source: "ops_api" },
      })
      .select("*")
      .single();
    if (reviewInsert.error) throw new Error(reviewInsert.error.message);

    return NextResponse.json({ ok: true, review: reviewInsert.data });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
