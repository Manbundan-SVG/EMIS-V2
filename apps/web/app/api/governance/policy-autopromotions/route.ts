import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";
import { getGovernancePolicyAutopromotionMetrics } from "@/lib/queries/metrics";

export async function GET(request: NextRequest) {
  try {
    const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
    const data = await getGovernancePolicyAutopromotionMetrics(workspace);
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
      action?: "upsert_policy" | "disable_policy";
      policyFamily?: string;
      scopeType?: string;
      scopeValue?: string;
      promotionTarget?: string;
      minConfidence?: "low" | "medium" | "high";
      minApprovedReviewCount?: number;
      minApplicationCount?: number;
      minSampleSize?: number;
      maxRecentOverrideRate?: number;
      maxRecentReassignmentRate?: number;
      cooldownHours?: number;
      createdBy?: string;
      policyId?: string;
    };

    if (!body.action) throw new Error("action is required: upsert_policy | disable_policy");

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

    if (body.action === "upsert_policy") {
      if (!body.policyFamily) throw new Error("policyFamily is required");
      if (!body.scopeType || !body.scopeValue) throw new Error("scopeType and scopeValue are required");
      if (!body.promotionTarget) throw new Error("promotionTarget is required");

      const VALID_TARGETS = ["threshold_profile", "routing_rule", "routing_override", "autopromotion_policy"];
      if (!VALID_TARGETS.includes(body.promotionTarget)) {
        throw new Error(`promotionTarget must be one of: ${VALID_TARGETS.join(" | ")}`);
      }
      const VALID_CONFIDENCE = ["low", "medium", "high"];
      const confidence = body.minConfidence ?? "high";
      if (!VALID_CONFIDENCE.includes(confidence)) {
        throw new Error("minConfidence must be low | medium | high");
      }

      const insert = await supabase
        .from("governance_policy_autopromotion_policies")
        .upsert({
          workspace_id: workspaceId,
          enabled: true,
          policy_family: body.policyFamily,
          scope_type: body.scopeType,
          scope_value: body.scopeValue,
          promotion_target: body.promotionTarget,
          min_confidence: confidence,
          min_approved_review_count: body.minApprovedReviewCount ?? 1,
          min_application_count: body.minApplicationCount ?? 1,
          min_sample_size: body.minSampleSize ?? 5,
          max_recent_override_rate: body.maxRecentOverrideRate ?? 0.25,
          max_recent_reassignment_rate: body.maxRecentReassignmentRate ?? 0.25,
          cooldown_hours: body.cooldownHours ?? 72,
          created_by: body.createdBy ?? "ops",
          metadata: { source: "ops_api" },
        }, {
          onConflict: "workspace_id,policy_family,scope_type,scope_value,promotion_target",
        })
        .select("*")
        .single();
      if (insert.error) throw new Error(insert.error.message);
      return NextResponse.json({ ok: true, policy: insert.data });
    }

    if (body.action === "disable_policy") {
      if (!body.policyId) throw new Error("policyId is required");
      const update = await supabase
        .from("governance_policy_autopromotion_policies")
        .update({ enabled: false })
        .eq("workspace_id", workspaceId)
        .eq("id", body.policyId)
        .select("*")
        .single();
      if (update.error) throw new Error(update.error.message);
      return NextResponse.json({ ok: true, policy: update.data });
    }

    throw new Error(`unknown action: ${body.action as string}`);
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
