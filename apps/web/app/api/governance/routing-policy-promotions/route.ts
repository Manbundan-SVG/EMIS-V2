import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";
import { getGovernanceRoutingPolicyPromotionMetrics } from "@/lib/queries/metrics";

export async function GET(request: NextRequest) {
  try {
    const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
    const data = await getGovernanceRoutingPolicyPromotionMetrics(workspace);
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
      action?: "propose" | "approve" | "apply";
      // propose
      recommendationKey?: string;
      promotionTarget?: "override" | "rule";
      scopeType?: string;
      scopeValue?: string;
      proposedBy?: string;
      proposalReason?: string | null;
      // approve
      proposalId?: string;
      approvedBy?: string;
      // apply
      appliedBy?: string;
    };

    if (!body.action) throw new Error("action is required: propose | approve | apply");

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

    // ── propose ────────────────────────────────────────────────────────────
    if (body.action === "propose") {
      if (!body.recommendationKey) throw new Error("recommendationKey is required");
      if (!body.promotionTarget) throw new Error("promotionTarget is required");
      if (!body.scopeType) throw new Error("scopeType is required");
      if (!body.scopeValue) throw new Error("scopeValue is required");
      if (!body.proposedBy) throw new Error("proposedBy is required");

      // require an approved review before a proposal can be created
      const reviewCheck = await supabase
        .from("governance_routing_policy_review_summary")
        .select("latest_review_status")
        .eq("workspace_id", workspaceId)
        .eq("recommendation_key", body.recommendationKey)
        .maybeSingle();
      if (reviewCheck.error) throw new Error(reviewCheck.error.message);
      if (reviewCheck.data?.latest_review_status !== "approved") {
        throw new Error("recommendation must have an approved review before a proposal can be created");
      }

      // look up the recommendation to get recommended_policy
      const recLookup = await supabase
        .from("governance_routing_policy_recommendations")
        .select("recommended_policy, scope_type, scope_value")
        .eq("workspace_id", workspaceId)
        .eq("recommendation_key", body.recommendationKey)
        .maybeSingle();
      if (recLookup.error) throw new Error(recLookup.error.message);
      const recommendedPolicy = (recLookup.data?.recommended_policy as Record<string, unknown>) ?? {};

      // capture current live policy (best-effort; empty if no matching rule exists yet)
      const currentPolicy: Record<string, unknown> = {
        scope_type: body.scopeType,
        scope_value: body.scopeValue,
        captured_at: new Date().toISOString(),
      };

      const proposalInsert = await supabase
        .from("governance_routing_policy_promotion_proposals")
        .insert({
          workspace_id: workspaceId,
          recommendation_key: body.recommendationKey,
          proposal_status: "pending",
          promotion_target: body.promotionTarget,
          scope_type: body.scopeType,
          scope_value: body.scopeValue,
          current_policy: currentPolicy,
          recommended_policy: recommendedPolicy,
          proposed_by: body.proposedBy,
          proposal_reason: body.proposalReason ?? null,
          metadata: { source: "ops_api" },
        })
        .select("*")
        .single();
      if (proposalInsert.error) throw new Error(proposalInsert.error.message);

      return NextResponse.json({ ok: true, proposal: proposalInsert.data });
    }

    // ── approve ────────────────────────────────────────────────────────────
    if (body.action === "approve") {
      if (!body.proposalId) throw new Error("proposalId is required");
      if (!body.approvedBy) throw new Error("approvedBy is required");

      const proposalLookup = await supabase
        .from("governance_routing_policy_promotion_proposals")
        .select("*")
        .eq("id", body.proposalId)
        .eq("workspace_id", workspaceId)
        .single();
      if (proposalLookup.error || !proposalLookup.data) {
        throw new Error(proposalLookup.error?.message ?? "proposal not found");
      }
      if (!["pending", "deferred"].includes(proposalLookup.data.proposal_status)) {
        throw new Error(`proposal is already ${proposalLookup.data.proposal_status}`);
      }

      const approvalUpdate = await supabase
        .from("governance_routing_policy_promotion_proposals")
        .update({
          proposal_status: "approved",
          approved_by: body.approvedBy,
          approved_at: new Date().toISOString(),
        })
        .eq("id", body.proposalId)
        .select("*")
        .single();
      if (approvalUpdate.error) throw new Error(approvalUpdate.error.message);

      return NextResponse.json({ ok: true, proposal: approvalUpdate.data });
    }

    // ── apply ──────────────────────────────────────────────────────────────
    if (body.action === "apply") {
      if (!body.proposalId) throw new Error("proposalId is required");
      if (!body.appliedBy) throw new Error("appliedBy is required");

      const proposalLookup = await supabase
        .from("governance_routing_policy_promotion_proposals")
        .select("*")
        .eq("id", body.proposalId)
        .eq("workspace_id", workspaceId)
        .single();
      if (proposalLookup.error || !proposalLookup.data) {
        throw new Error(proposalLookup.error?.message ?? "proposal not found");
      }

      const proposal = proposalLookup.data;

      // approval gate — hard stop
      if (proposal.proposal_status !== "approved") {
        throw new Error(
          `proposal must be approved before application; current status: ${proposal.proposal_status}`
        );
      }

      const recommendedPolicy = proposal.recommended_policy as Record<string, unknown>;
      const promotionTarget = proposal.promotion_target as "override" | "rule";

      // write to live routing table
      let appliedRoutingRow: Record<string, unknown> | null = null;
      if (promotionTarget === "override") {
        const assignedUser =
          (recommendedPolicy.preferred_operator as string | undefined) ??
          (proposal.scope_type === "operator" ? proposal.scope_value : null);
        const assignedTeam =
          (recommendedPolicy.preferred_team as string | undefined) ??
          (proposal.scope_type === "team" ? proposal.scope_value : null);

        const overrideInsert = await supabase
          .from("governance_routing_overrides")
          .insert({
            workspace_id: workspaceId,
            assigned_user: assignedUser ?? null,
            assigned_team: assignedTeam ?? null,
            reason: `applied_from_policy_promotion:${body.appliedBy}`,
            is_enabled: true,
            metadata: {
              source: "routing_policy_promotion",
              applied_by: body.appliedBy,
              proposal_id: body.proposalId,
              recommendation_key: proposal.recommendation_key,
            },
          })
          .select("*")
          .single();
        if (overrideInsert.error) throw new Error(overrideInsert.error.message);
        appliedRoutingRow = overrideInsert.data as Record<string, unknown>;
      } else {
        const assignTeam =
          (recommendedPolicy.preferred_team as string | undefined) ??
          (recommendedPolicy.preferred_team_for_reopens as string | undefined) ??
          (proposal.scope_type === "team" ? proposal.scope_value : null);
        const assignUser = recommendedPolicy.preferred_operator as string | undefined ?? null;
        const rootCauseCode = proposal.scope_type === "root_cause" ? proposal.scope_value : null;
        const regime = proposal.scope_type === "regime" ? proposal.scope_value : null;
        const severity = proposal.scope_type === "severity" ? proposal.scope_value : null;
        const chronicOnly = proposal.scope_type === "chronicity";

        const ruleInsert = await supabase
          .from("governance_routing_rules")
          .insert({
            workspace_id: workspaceId,
            is_enabled: true,
            priority: 5,
            root_cause_code: rootCauseCode ?? null,
            severity: severity ?? null,
            regime: regime ?? null,
            chronic_only: chronicOnly,
            assign_team: assignTeam ?? null,
            assign_user: assignUser,
            routing_reason_template: `policy_promotion:${proposal.scope_type}:${proposal.scope_value}`,
            metadata: {
              source: "routing_policy_promotion",
              applied_by: body.appliedBy,
              proposal_id: body.proposalId,
              recommendation_key: proposal.recommendation_key,
            },
          })
          .select("*")
          .single();
        if (ruleInsert.error) throw new Error(ruleInsert.error.message);
        appliedRoutingRow = ruleInsert.data as Record<string, unknown>;
      }

      // persist application record
      const applicationInsert = await supabase
        .from("governance_routing_policy_applications")
        .insert({
          workspace_id: workspaceId,
          proposal_id: body.proposalId,
          recommendation_key: proposal.recommendation_key,
          applied_target: promotionTarget,
          applied_scope_type: proposal.scope_type,
          applied_scope_value: proposal.scope_value,
          prior_policy: proposal.current_policy as Record<string, unknown>,
          applied_policy: recommendedPolicy,
          applied_by: body.appliedBy,
          rollback_candidate: true,
          metadata: {
            source: "ops_api",
            routing_row_id: appliedRoutingRow?.id ?? null,
          },
        })
        .select("*")
        .single();
      if (applicationInsert.error) throw new Error(applicationInsert.error.message);

      // mark proposal applied
      const proposalUpdate = await supabase
        .from("governance_routing_policy_promotion_proposals")
        .update({
          proposal_status: "applied",
          applied_at: new Date().toISOString(),
        })
        .eq("id", body.proposalId)
        .select("*")
        .single();
      if (proposalUpdate.error) throw new Error(proposalUpdate.error.message);

      return NextResponse.json({
        ok: true,
        application: applicationInsert.data,
        proposal: proposalUpdate.data,
        routingRow: appliedRoutingRow,
      });
    }

    throw new Error(`unknown action: ${body.action as string}`);
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
