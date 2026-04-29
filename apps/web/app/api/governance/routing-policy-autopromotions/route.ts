import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";
import { getGovernanceRoutingPolicyAutopromotionMetrics } from "@/lib/queries/metrics";

export async function GET(request: NextRequest) {
  try {
    const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
    const data = await getGovernanceRoutingPolicyAutopromotionMetrics(workspace);
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
      action?: "create_policy" | "update_policy" | "disable_policy";
      // policy fields
      scopeType?: string;
      scopeValue?: string;
      promotionTarget?: "rule" | "override";
      enabled?: boolean;
      minConfidence?: "low" | "medium" | "high";
      minApprovedReviewCount?: number;
      minApplicationCount?: number;
      minSampleSize?: number;
      maxRecentOverrideRate?: number;
      maxRecentReassignmentRate?: number;
      cooldownHours?: number;
      createdBy?: string;
    };

    if (!body.action) {
      throw new Error("action is required: create_policy | update_policy | disable_policy");
    }

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
    const workspaceId = workspaceLookup.data.id;

    // ── create_policy / update_policy ─────────────────────────────────────
    if (body.action === "create_policy" || body.action === "update_policy") {
      if (!body.scopeType) throw new Error("scopeType is required");
      if (!body.scopeValue) throw new Error("scopeValue is required");
      if (!body.createdBy) throw new Error("createdBy is required");

      const upsertResult = await supabase
        .from("governance_routing_policy_autopromotion_policies")
        .upsert(
          {
            workspace_id: workspaceId,
            scope_type: body.scopeType,
            scope_value: body.scopeValue,
            promotion_target: body.promotionTarget ?? "rule",
            enabled: body.enabled ?? true,
            min_confidence: body.minConfidence ?? "high",
            min_approved_review_count: body.minApprovedReviewCount ?? 1,
            min_application_count: body.minApplicationCount ?? 1,
            min_sample_size: body.minSampleSize ?? 50,
            max_recent_override_rate: body.maxRecentOverrideRate ?? 0.20,
            max_recent_reassignment_rate: body.maxRecentReassignmentRate ?? 0.15,
            cooldown_hours: body.cooldownHours ?? 168,
            created_by: body.createdBy,
            metadata: { source: "ops_api" },
          },
          { onConflict: "workspace_id,scope_type,scope_value" },
        )
        .select("*")
        .single();
      if (upsertResult.error) throw new Error(upsertResult.error.message);

      return NextResponse.json({ ok: true, policy: upsertResult.data });
    }

    // ── disable_policy ────────────────────────────────────────────────────
    if (body.action === "disable_policy") {
      if (!body.scopeType) throw new Error("scopeType is required");
      if (!body.scopeValue) throw new Error("scopeValue is required");

      const disableResult = await supabase
        .from("governance_routing_policy_autopromotion_policies")
        .update({ enabled: false, updated_at: new Date().toISOString() })
        .eq("workspace_id", workspaceId)
        .eq("scope_type", body.scopeType)
        .eq("scope_value", body.scopeValue)
        .select("*")
        .single();
      if (disableResult.error) throw new Error(disableResult.error.message);

      return NextResponse.json({ ok: true, policy: disableResult.data });
    }

    throw new Error(`unknown action: ${body.action as string}`);
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
