import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";
import { getGovernancePolicyPromotionMetrics } from "@/lib/queries/metrics";

export async function GET(request: NextRequest) {
  try {
    const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
    const data = await getGovernancePolicyPromotionMetrics(workspace);
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
      // propose fields
      recommendationKey?: string;
      policyFamily?: string;
      promotionTarget?: "threshold_profile" | "routing_rule" | "routing_override" | "autopromotion_policy";
      scopeType?: string;
      scopeValue?: string;
      currentPolicy?: Record<string, unknown>;
      recommendedPolicy?: Record<string, unknown>;
      proposedBy?: string;
      proposalReason?: string | null;
      // approve fields
      proposalId?: string;
      approvedBy?: string;
      // apply fields
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
    const workspaceId = workspaceLookup.data.id as string;

    // ── propose ───────────────────────────────────────────────────────────────
    if (body.action === "propose") {
      if (!body.recommendationKey) throw new Error("recommendationKey is required");
      if (!body.policyFamily) throw new Error("policyFamily is required");
      if (!body.promotionTarget) throw new Error("promotionTarget is required");
      if (!body.scopeType || !body.scopeValue) throw new Error("scopeType and scopeValue are required");
      if (!body.proposedBy) throw new Error("proposedBy is required");
      if (!body.currentPolicy || !body.recommendedPolicy) {
        throw new Error("currentPolicy and recommendedPolicy are required");
      }

      const VALID_TARGETS = ["threshold_profile", "routing_rule", "routing_override", "autopromotion_policy"];
      if (!VALID_TARGETS.includes(body.promotionTarget)) {
        throw new Error(`promotionTarget must be one of: ${VALID_TARGETS.join(" | ")}`);
      }

      // verify recommendation belongs to workspace
      const recLookup = await supabase
        .from("governance_policy_recommendations")
        .select("recommendation_key")
        .eq("workspace_id", workspaceId)
        .eq("recommendation_key", body.recommendationKey)
        .maybeSingle();
      if (recLookup.error) throw new Error(recLookup.error.message);
      if (!recLookup.data) throw new Error("recommendation not found in this workspace");

      const insert = await supabase
        .from("governance_policy_promotion_proposals")
        .insert({
          workspace_id: workspaceId,
          recommendation_key: body.recommendationKey,
          policy_family: body.policyFamily,
          proposal_status: "pending",
          promotion_target: body.promotionTarget,
          scope_type: body.scopeType,
          scope_value: body.scopeValue,
          current_policy: body.currentPolicy,
          recommended_policy: body.recommendedPolicy,
          proposed_by: body.proposedBy,
          proposal_reason: body.proposalReason ?? null,
          metadata: { source: "ops_api" },
        })
        .select("*")
        .single();
      if (insert.error) throw new Error(insert.error.message);

      return NextResponse.json({ ok: true, proposal: insert.data });
    }

    // ── approve ───────────────────────────────────────────────────────────────
    if (body.action === "approve") {
      if (!body.proposalId) throw new Error("proposalId is required");
      if (!body.approvedBy) throw new Error("approvedBy is required");

      const proposalLookup = await supabase
        .from("governance_policy_promotion_proposals")
        .select("id, proposal_status")
        .eq("workspace_id", workspaceId)
        .eq("id", body.proposalId)
        .single();
      if (proposalLookup.error || !proposalLookup.data) {
        throw new Error(proposalLookup.error?.message ?? "proposal not found");
      }
      if (proposalLookup.data.proposal_status !== "pending") {
        throw new Error(
          `proposal must be in 'pending' state to approve; current: ${proposalLookup.data.proposal_status}`
        );
      }

      const update = await supabase
        .from("governance_policy_promotion_proposals")
        .update({
          proposal_status: "approved",
          approved_by: body.approvedBy,
          approved_at: new Date().toISOString(),
        })
        .eq("workspace_id", workspaceId)
        .eq("id", body.proposalId)
        .select("*")
        .single();
      if (update.error) throw new Error(update.error.message);

      return NextResponse.json({ ok: true, proposal: update.data });
    }

    // ── apply ─────────────────────────────────────────────────────────────────
    if (body.action === "apply") {
      if (!body.proposalId) throw new Error("proposalId is required");
      if (!body.appliedBy) throw new Error("appliedBy is required");

      // Load full proposal
      const proposalLookup = await supabase
        .from("governance_policy_promotion_proposals")
        .select("*")
        .eq("workspace_id", workspaceId)
        .eq("id", body.proposalId)
        .single();
      if (proposalLookup.error || !proposalLookup.data) {
        throw new Error(proposalLookup.error?.message ?? "proposal not found");
      }
      const proposal = proposalLookup.data as Record<string, unknown>;

      // Hard approval gate
      if (proposal.proposal_status !== "approved") {
        throw new Error(
          `proposal must be in 'approved' state before apply; current: ${proposal.proposal_status}`
        );
      }

      const currentPolicy = (proposal.current_policy ?? {}) as Record<string, unknown>;
      const recommendedPolicy = (proposal.recommended_policy ?? {}) as Record<string, unknown>;
      const promotionTarget = proposal.promotion_target as string;
      const scopeType = proposal.scope_type as string;
      const scopeValue = proposal.scope_value as string;

      // Apply to live policy tables — additive INSERT for routing, UPDATE for threshold/autopromotion
      let appliedRowId: string | null = null;

      if (promotionTarget === "routing_rule") {
        const assignTeam = (recommendedPolicy.assign_team as string | undefined) ??
          (scopeType === "team" ? scopeValue : null);
        const rootCauseCode = (recommendedPolicy.root_cause_code as string | undefined) ??
          (scopeType === "root_cause" ? scopeValue : null);
        const regime = (recommendedPolicy.regime as string | undefined) ??
          (scopeType === "regime" ? scopeValue : null);
        const severity = (recommendedPolicy.severity as string | undefined) ??
          (scopeType === "severity" ? scopeValue : null);

        const ruleInsert = await supabase
          .from("governance_routing_rules")
          .insert({
            workspace_id: workspaceId,
            is_enabled: true,
            priority: 4,
            root_cause_code: rootCauseCode ?? null,
            severity: severity ?? null,
            regime: regime ?? null,
            chronic_only: (recommendedPolicy.chronic_only as boolean | undefined) ?? false,
            assign_team: assignTeam ?? null,
            routing_reason_template: `governance_policy_promotion:${body.appliedBy}`,
            metadata: {
              source: "governance_policy_promotion",
              applied_by: body.appliedBy,
              proposal_id: body.proposalId,
            },
          })
          .select("id")
          .single();
        if (ruleInsert.error) throw new Error(ruleInsert.error.message);
        appliedRowId = ruleInsert.data?.id as string ?? null;

      } else if (promotionTarget === "routing_override") {
        const assignedUser = (recommendedPolicy.assigned_user as string | undefined) ??
          (scopeType === "operator" ? scopeValue : null);
        const assignTeam = (recommendedPolicy.assign_team as string | undefined) ??
          (scopeType === "team" ? scopeValue : null);

        const overrideInsert = await supabase
          .from("governance_routing_overrides")
          .insert({
            workspace_id: workspaceId,
            assigned_user: assignedUser ?? null,
            assign_team: assignTeam ?? null,
            reason: `governance_policy_promotion:${body.appliedBy}`,
            is_enabled: true,
            metadata: {
              source: "governance_policy_promotion",
              applied_by: body.appliedBy,
              proposal_id: body.proposalId,
            },
          })
          .select("id")
          .single();
        if (overrideInsert.error) throw new Error(overrideInsert.error.message);
        appliedRowId = overrideInsert.data?.id as string ?? null;

      } else if (promotionTarget === "autopromotion_policy") {
        const ALLOWED_AP = ["min_confidence", "min_sample_size", "max_override_rate",
          "max_reassignment_rate", "cooldown_hours", "promotion_target", "enabled"];
        const apUpdates: Record<string, unknown> = {};
        for (const k of ALLOWED_AP) {
          if (recommendedPolicy[k] !== undefined) apUpdates[k] = recommendedPolicy[k];
        }
        if (Object.keys(apUpdates).length === 0) {
          throw new Error("no recognized autopromotion_policy fields in recommended_policy");
        }
        const apUpdate = await supabase
          .from("governance_routing_policy_autopromotion_policies")
          .update(apUpdates)
          .eq("workspace_id", workspaceId)
          .eq("scope_type", scopeType)
          .eq("scope_value", scopeValue)
          .select("id")
          .maybeSingle();
        if (apUpdate.error) throw new Error(apUpdate.error.message);
        appliedRowId = apUpdate.data?.id as string ?? null;

      } else if (promotionTarget === "threshold_profile") {
        const ALLOWED_TP = ["alert_threshold", "warning_threshold", "critical_threshold",
          "min_sample_size", "cooldown_minutes", "is_active"];
        const tpUpdates: Record<string, unknown> = {};
        for (const k of ALLOWED_TP) {
          if (recommendedPolicy[k] !== undefined) tpUpdates[k] = recommendedPolicy[k];
        }
        if (Object.keys(tpUpdates).length === 0) {
          throw new Error("no recognized threshold_profile fields in recommended_policy");
        }
        const tpUpdate = await supabase
          .from("regime_threshold_profiles")
          .update(tpUpdates)
          .eq("workspace_id", workspaceId)
          .eq("regime", scopeValue)
          .select("id")
          .maybeSingle();
        if (tpUpdate.error) throw new Error(tpUpdate.error.message);
        appliedRowId = tpUpdate.data?.id as string ?? null;

      } else {
        throw new Error(`unsupported promotion_target: ${promotionTarget}`);
      }

      // Record application audit row
      const appInsert = await supabase
        .from("governance_policy_applications")
        .insert({
          workspace_id: workspaceId,
          proposal_id: body.proposalId,
          recommendation_key: proposal.recommendation_key as string,
          policy_family: proposal.policy_family as string,
          applied_target: promotionTarget,
          applied_scope_type: scopeType,
          applied_scope_value: scopeValue,
          prior_policy: currentPolicy,
          applied_policy: recommendedPolicy,
          applied_by: body.appliedBy,
          rollback_candidate: true,
          metadata: {
            source: "ops_api",
            applied_row_id: appliedRowId,
          },
        })
        .select("*")
        .single();
      if (appInsert.error) throw new Error(appInsert.error.message);

      // Mark proposal applied
      const markApplied = await supabase
        .from("governance_policy_promotion_proposals")
        .update({
          proposal_status: "applied",
          applied_at: new Date().toISOString(),
        })
        .eq("workspace_id", workspaceId)
        .eq("id", body.proposalId)
        .select("*")
        .single();
      if (markApplied.error) throw new Error(markApplied.error.message);

      return NextResponse.json({
        ok: true,
        application: appInsert.data,
        proposal: markApplied.data,
        appliedRowId,
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
