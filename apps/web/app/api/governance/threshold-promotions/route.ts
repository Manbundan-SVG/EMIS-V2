import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient, type Database } from "@/lib/supabase";

type PromotionAction = "approve" | "reject" | "defer" | "execute" | "cancel";
type ProfileRow = Database["public"]["Tables"]["governance_threshold_profiles"]["Row"];
type OverrideRow = Database["public"]["Tables"]["regime_threshold_overrides"]["Row"];

function thresholdColumnForEvent(eventType: string): string {
  switch (eventType) {
    case "version_regression":
      return "version_health_floor";
    case "replay_degradation":
      return "replay_consistency_floor";
    case "family_instability_spike":
    case "stability_classification_downgrade":
      return "family_instability_ceiling";
    case "regime_conflict_persistence":
      return "conflicting_transition_ceiling";
    case "regime_instability_spike":
      return "regime_instability_ceiling";
    default:
      throw new Error(`unsupported event type: ${eventType}`);
  }
}

function thresholdUpdateForEvent(eventType: string, value: number):
  Database["public"]["Tables"]["governance_threshold_profiles"]["Update"] &
  Database["public"]["Tables"]["regime_threshold_overrides"]["Update"] {
  switch (eventType) {
    case "version_regression":
      return { version_health_floor: value };
    case "replay_degradation":
      return { replay_consistency_floor: value };
    case "family_instability_spike":
    case "stability_classification_downgrade":
      return { family_instability_ceiling: value };
    case "regime_conflict_persistence":
      return { conflicting_transition_ceiling: value };
    case "regime_instability_spike":
      return { regime_instability_ceiling: value };
    default:
      throw new Error(`unsupported event type: ${eventType}`);
  }
}

function currentThresholdValue(eventType: string, row: ProfileRow | OverrideRow): number | null {
  switch (eventType) {
    case "version_regression":
      return row.version_health_floor ?? null;
    case "replay_degradation":
      return row.replay_consistency_floor ?? null;
    case "family_instability_spike":
    case "stability_classification_downgrade":
      return row.family_instability_ceiling ?? null;
    case "regime_conflict_persistence":
      return row.conflicting_transition_ceiling ?? null;
    case "regime_instability_spike":
      return row.regime_instability_ceiling ?? null;
    default:
      return null;
  }
}

async function applyPromotionValue(
  supabase: ReturnType<typeof createServiceSupabaseClient>,
  proposal: Record<string, unknown>,
) {
  thresholdColumnForEvent(String(proposal.event_type));
  const profileId = String(proposal.profile_id);
  const dimensionType = proposal.dimension_type ? String(proposal.dimension_type) : null;
  const dimensionValue = proposal.dimension_value ? String(proposal.dimension_value) : null;
  const workspaceId = String(proposal.workspace_id);
  const proposedValue = Number(proposal.proposed_value);

  if (dimensionType === "regime" && dimensionValue && !["any", "default"].includes(dimensionValue)) {
    const overrideLookup = await supabase
      .from("regime_threshold_overrides")
      .select("*")
      .eq("profile_id", profileId)
      .eq("regime", dimensionValue)
      .maybeSingle();
    if (overrideLookup.error) throw new Error(overrideLookup.error.message);

    if (overrideLookup.data) {
      const updatePayload: Database["public"]["Tables"]["regime_threshold_overrides"]["Update"] = {
        ...thresholdUpdateForEvent(String(proposal.event_type), proposedValue),
        updated_at: new Date().toISOString(),
      };
      const update = await supabase
        .from("regime_threshold_overrides")
        .update(updatePayload)
        .eq("id", overrideLookup.data.id)
        .select("*")
        .single();
      if (update.error) throw new Error(update.error.message);
      return {
        targetTable: "regime_threshold_overrides",
        targetId: update.data.id,
        previousValue: Number(currentThresholdValue(String(proposal.event_type), overrideLookup.data) ?? proposal.current_value),
      };
    }

    const profileLookup = await supabase
      .from("governance_threshold_profiles")
      .select("*")
      .eq("id", profileId)
      .single();
    if (profileLookup.error || !profileLookup.data) throw new Error(profileLookup.error?.message ?? "profile not found");

    const insertPayload: Database["public"]["Tables"]["regime_threshold_overrides"]["Insert"] = {
      workspace_id: workspaceId,
      regime: dimensionValue,
      profile_id: profileId,
      enabled: true,
      version_health_floor: profileLookup.data.version_health_floor,
      family_instability_ceiling: profileLookup.data.family_instability_ceiling,
      replay_consistency_floor: profileLookup.data.replay_consistency_floor,
      regime_instability_ceiling: profileLookup.data.regime_instability_ceiling,
      conflicting_transition_ceiling: profileLookup.data.conflicting_transition_ceiling,
      ...thresholdUpdateForEvent(String(proposal.event_type), proposedValue),
      metadata: { source: "threshold_promotion_api" },
    };
    const insert = await supabase
      .from("regime_threshold_overrides")
      .insert(insertPayload)
      .select("*")
      .single();
    if (insert.error) throw new Error(insert.error.message);
    return {
      targetTable: "regime_threshold_overrides",
      targetId: insert.data.id,
      previousValue: Number(currentThresholdValue(String(proposal.event_type), profileLookup.data) ?? proposal.current_value),
    };
  }

  const profileLookup = await supabase
    .from("governance_threshold_profiles")
    .select("*")
    .eq("id", profileId)
    .single();
  if (profileLookup.error || !profileLookup.data) throw new Error(profileLookup.error?.message ?? "profile not found");
  const updatePayload: Database["public"]["Tables"]["governance_threshold_profiles"]["Update"] = {
    ...thresholdUpdateForEvent(String(proposal.event_type), proposedValue),
    updated_at: new Date().toISOString(),
  };
  const update = await supabase
    .from("governance_threshold_profiles")
    .update(updatePayload)
    .eq("id", profileId)
    .select("*")
    .single();
  if (update.error) throw new Error(update.error.message);
  return {
    targetTable: "governance_threshold_profiles",
    targetId: update.data.id,
    previousValue: Number(currentThresholdValue(String(proposal.event_type), profileLookup.data) ?? proposal.current_value),
  };
}

export async function GET(request: NextRequest) {
  try {
    const workspace = request.nextUrl.searchParams.get("workspace");
    const supabase = createServiceSupabaseClient();
    let query = supabase
      .from("governance_threshold_review_summary")
      .select("*")
      .order("updated_at", { ascending: false })
      .limit(50);

    if (workspace) {
      const ws = await supabase.from("workspaces").select("id").eq("slug", workspace).single();
      if (ws.error || !ws.data) throw new Error(ws.error?.message ?? "workspace not found");
      query = query.eq("workspace_id", ws.data.id);
    }

    const { data, error } = await query;
    if (error) throw new Error(error.message);
    return NextResponse.json({ ok: true, proposals: data ?? [] });
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
      proposalId?: string;
      action?: PromotionAction;
      actor?: string;
      rationale?: string | null;
      metadata?: Record<string, unknown>;
    };
    if (!body.proposalId) throw new Error("proposalId is required");
    if (!body.action) throw new Error("action is required");

    const supabase = createServiceSupabaseClient();
    const proposalLookup = await supabase
      .from("governance_threshold_review_summary")
      .select("*")
      .eq("proposal_id", body.proposalId)
      .single();
    if (proposalLookup.error || !proposalLookup.data) {
      throw new Error(proposalLookup.error?.message ?? "proposal not found");
    }
    const proposal = proposalLookup.data;

    const actor = body.actor ?? "ops";
    const rationale = body.rationale?.trim() || null;
    const metadata = body.metadata ?? {};

    if (body.action === "approve" || body.action === "reject" || body.action === "defer") {
      const reviewInsert = await supabase
        .from("governance_threshold_recommendation_reviews")
        .insert({
          workspace_id: proposal.workspace_id,
          recommendation_id: proposal.recommendation_id,
          reviewer: actor,
          decision: body.action === "approve" ? "approved" : body.action === "reject" ? "rejected" : "deferred",
          rationale,
          metadata,
        })
        .select("*")
        .single();
      if (reviewInsert.error) throw new Error(reviewInsert.error.message);

      const proposalStatus =
        body.action === "approve" ? "approved" : body.action === "reject" ? "blocked" : "pending";
      const recommendationStatus =
        body.action === "approve" ? "accepted" : body.action === "reject" ? "dismissed" : "open";

      const [proposalUpdate, recommendationUpdate] = await Promise.all([
        supabase
          .from("governance_threshold_promotion_proposals")
          .update({
            status: proposalStatus,
            approved_by: body.action === "approve" ? actor : null,
            approved_at: body.action === "approve" ? new Date().toISOString() : null,
            blocked_reason: body.action === "reject" ? rationale : null,
            updated_at: new Date().toISOString(),
            metadata: {
              ...(proposal.metadata ?? {}),
              review_action: body.action,
              review_actor: actor,
            },
          })
          .eq("id", body.proposalId)
          .select("*")
          .single(),
        supabase
          .from("governance_threshold_recommendations")
          .update({
            status: recommendationStatus,
            updated_at: new Date().toISOString(),
          })
          .eq("id", proposal.recommendation_id)
          .select("*")
          .single(),
      ]);
      if (proposalUpdate.error) throw new Error(proposalUpdate.error.message);
      if (recommendationUpdate.error) throw new Error(recommendationUpdate.error.message);

      return NextResponse.json({
        ok: true,
        action: body.action,
        review: reviewInsert.data,
        proposal: proposalUpdate.data,
        recommendation: recommendationUpdate.data,
      });
    }

    if (body.action === "cancel") {
      const cancelUpdate = await supabase
        .from("governance_threshold_promotion_proposals")
        .update({
          status: "cancelled",
          blocked_reason: rationale,
          updated_at: new Date().toISOString(),
          metadata: {
            ...(proposal.metadata ?? {}),
            cancelled_by: actor,
          },
        })
        .eq("id", body.proposalId)
        .select("*")
        .single();
      if (cancelUpdate.error) throw new Error(cancelUpdate.error.message);
      return NextResponse.json({ ok: true, action: body.action, proposal: cancelUpdate.data });
    }

    if (body.action !== "execute") {
      throw new Error(`unsupported action: ${body.action}`);
    }
    if (proposal.status !== "approved") {
      throw new Error("proposal must be approved before execution");
    }

    const applied = await applyPromotionValue(supabase, proposal);
    const executionInsert = await supabase
      .from("governance_threshold_promotion_executions")
      .insert({
        workspace_id: proposal.workspace_id,
        proposal_id: proposal.proposal_id,
        profile_id: proposal.profile_id,
        event_type: proposal.event_type,
        dimension_type: proposal.dimension_type,
        dimension_value: proposal.dimension_value,
        previous_value: applied.previousValue,
        new_value: proposal.proposed_value,
        executed_by: actor,
        execution_mode: "manual",
        rationale,
        metadata: {
          ...metadata,
          target_table: applied.targetTable,
          target_id: applied.targetId,
        },
      })
      .select("*")
      .single();
    if (executionInsert.error) throw new Error(executionInsert.error.message);

    const rollbackInsert = await supabase
      .from("governance_threshold_rollback_candidates")
      .insert({
        workspace_id: proposal.workspace_id,
        execution_id: executionInsert.data.id,
        profile_id: proposal.profile_id,
        rollback_to_value: applied.previousValue,
        reason: "manual_execution_rollback_candidate",
        metadata: { proposal_id: proposal.proposal_id },
      })
      .select("*")
      .single();
    if (rollbackInsert.error) throw new Error(rollbackInsert.error.message);

    const [proposalUpdate, recommendationUpdate] = await Promise.all([
      supabase
        .from("governance_threshold_promotion_proposals")
        .update({
          status: "executed",
          approved_by: proposal.approved_by ?? actor,
          approved_at: proposal.approved_at ?? new Date().toISOString(),
          updated_at: new Date().toISOString(),
          metadata: {
            ...(proposal.metadata ?? {}),
            execution_id: executionInsert.data.id,
          },
        })
        .eq("id", body.proposalId)
        .select("*")
        .single(),
      supabase
        .from("governance_threshold_recommendations")
        .update({
          status: "accepted",
          updated_at: new Date().toISOString(),
        })
        .eq("id", proposal.recommendation_id)
        .select("*")
        .single(),
    ]);
    if (proposalUpdate.error) throw new Error(proposalUpdate.error.message);
    if (recommendationUpdate.error) throw new Error(recommendationUpdate.error.message);

    return NextResponse.json({
      ok: true,
      action: body.action,
      execution: executionInsert.data,
      rollbackCandidate: rollbackInsert.data,
      proposal: proposalUpdate.data,
      recommendation: recommendationUpdate.data,
    });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
