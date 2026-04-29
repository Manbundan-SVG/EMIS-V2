import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json() as {
      recommendationId?: string;
      caseId?: string | null;
      reviewStatus?: "approved" | "rejected" | "deferred";
      reviewReason?: string | null;
      notes?: string | null;
      reviewedBy?: string | null;
      appliedImmediately?: boolean;
    };

    if (!body.recommendationId) throw new Error("recommendationId is required");
    if (!body.reviewStatus) throw new Error("reviewStatus is required");
    if (!["approved", "rejected", "deferred"].includes(body.reviewStatus)) {
      throw new Error("invalid reviewStatus");
    }
    if (body.appliedImmediately && body.reviewStatus !== "approved") {
      throw new Error("appliedImmediately requires approved reviewStatus");
    }

    const supabase = createServiceSupabaseClient();
    const recommendationLookup = await supabase
      .from("governance_routing_recommendation_summary")
      .select("id, workspace_id, case_id")
      .eq("id", body.recommendationId)
      .single();
    if (recommendationLookup.error || !recommendationLookup.data) {
      throw new Error(recommendationLookup.error?.message ?? "recommendation not found");
    }

    const reviewInsert = await supabase
      .from("governance_routing_recommendation_reviews")
      .insert({
        workspace_id: recommendationLookup.data.workspace_id,
        recommendation_id: body.recommendationId,
        case_id: body.caseId ?? recommendationLookup.data.case_id ?? null,
        review_status: body.reviewStatus,
        review_reason: body.reviewReason ?? null,
        notes: body.notes ?? null,
        reviewed_by: body.reviewedBy ?? null,
        applied_immediately: body.appliedImmediately ?? false,
        metadata: { source: "ops_api" },
      })
      .select("*")
      .single();
    if (reviewInsert.error) throw new Error(reviewInsert.error.message);

    const accepted =
      body.reviewStatus === "approved"
        ? true
        : body.reviewStatus === "rejected"
          ? false
          : null;
    const recommendationUpdate = await supabase
      .from("governance_routing_recommendations")
      .update({
        accepted,
        accepted_at: accepted === null ? null : new Date().toISOString(),
        accepted_by: accepted === null ? null : (body.reviewedBy ?? null),
        override_reason: body.reviewStatus === "rejected" ? (body.reviewReason ?? "recommendation_rejected") : null,
        updated_at: new Date().toISOString(),
      })
      .eq("id", body.recommendationId)
      .select("*")
      .single();
    if (recommendationUpdate.error) throw new Error(recommendationUpdate.error.message);

    return NextResponse.json({
      ok: true,
      review: reviewInsert.data,
      recommendation: recommendationUpdate.data,
    });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
