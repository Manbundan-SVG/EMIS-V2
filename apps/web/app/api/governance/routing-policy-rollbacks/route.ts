import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";
import { getGovernanceRoutingPolicyRollbackMetrics } from "@/lib/queries/metrics";

export async function GET(request: NextRequest) {
  try {
    const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
    const data = await getGovernanceRoutingPolicyRollbackMetrics(workspace);
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
      action?: "submit_review" | "execute_rollback";
      // review fields
      rollbackCandidateId?: string;
      reviewStatus?: "approved" | "rejected" | "deferred";
      reviewReason?: string | null;
      reviewedBy?: string;
      notes?: string | null;
      // execute fields
      executedBy?: string;
    };

    if (!body.action) {
      throw new Error("action is required: submit_review | execute_rollback");
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

    // ── submit_review ─────────────────────────────────────────────────────
    if (body.action === "submit_review") {
      if (!body.rollbackCandidateId) throw new Error("rollbackCandidateId is required");
      if (!body.reviewStatus) throw new Error("reviewStatus is required");
      if (!body.reviewedBy) throw new Error("reviewedBy is required");
      if (!["approved", "rejected", "deferred"].includes(body.reviewStatus)) {
        throw new Error("reviewStatus must be approved | rejected | deferred");
      }

      // verify candidate belongs to this workspace
      const candidateLookup = await supabase
        .from("governance_routing_policy_autopromotion_rollback_candidates")
        .select("id, resolved, recommendation_key")
        .eq("id", body.rollbackCandidateId)
        .eq("workspace_id", workspaceId)
        .single();
      if (candidateLookup.error || !candidateLookup.data) {
        throw new Error(candidateLookup.error?.message ?? "rollback candidate not found");
      }
      if (candidateLookup.data.resolved) {
        throw new Error("rollback candidate is already resolved; review not applicable");
      }

      const reviewInsert = await supabase
        .from("governance_routing_policy_rollback_reviews")
        .insert({
          workspace_id: workspaceId,
          rollback_candidate_id: body.rollbackCandidateId,
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
    }

    // ── execute_rollback ──────────────────────────────────────────────────
    if (body.action === "execute_rollback") {
      if (!body.rollbackCandidateId) throw new Error("rollbackCandidateId is required");
      if (!body.executedBy) throw new Error("executedBy is required");

      // load full candidate
      const candidateLookup = await supabase
        .from("governance_routing_policy_autopromotion_rollback_candidates")
        .select("*")
        .eq("id", body.rollbackCandidateId)
        .eq("workspace_id", workspaceId)
        .single();
      if (candidateLookup.error || !candidateLookup.data) {
        throw new Error(candidateLookup.error?.message ?? "rollback candidate not found");
      }
      const candidate = candidateLookup.data;

      // approval gate — must have approved review
      if (candidate.resolved) {
        throw new Error("rollback candidate is already rolled back");
      }

      const reviewSummary = await supabase
        .from("governance_routing_policy_rollback_review_summary")
        .select("latest_review_status")
        .eq("workspace_id", workspaceId)
        .eq("rollback_candidate_id", body.rollbackCandidateId)
        .maybeSingle();
      if (reviewSummary.error) throw new Error(reviewSummary.error.message);

      if (reviewSummary.data?.latest_review_status !== "approved") {
        throw new Error(
          `rollback requires an approved review; current status: ${reviewSummary.data?.latest_review_status ?? "none"}`
        );
      }

      // load autopromotion execution to get promotion_execution_id
      const execLookup = await supabase
        .from("governance_routing_policy_autopromotion_executions")
        .select("id")
        .eq("id", candidate.execution_id)
        .single();
      if (execLookup.error || !execLookup.data) {
        throw new Error(execLookup.error?.message ?? "autopromotion execution not found");
      }
      const promotionExecutionId = execLookup.data.id;

      const priorPolicy = (candidate.prior_policy as Record<string, unknown>) ?? {};
      const appliedPolicy = (candidate.applied_policy as Record<string, unknown>) ?? {};
      const scopeType: string = candidate.scope_type;
      const scopeValue: string = candidate.scope_value;
      const routingTable: string | null = candidate.routing_table;
      const executionTarget: "override" | "rule" =
        routingTable === "governance_routing_overrides" ? "override" : "rule";

      // restore routing row — additive insert preserving prior state
      let restoredRoutingRow: Record<string, unknown> | null = null;

      if (executionTarget === "override") {
        const assignedUser =
          (priorPolicy.preferred_operator as string | undefined) ??
          (priorPolicy.assigned_user as string | undefined) ??
          (scopeType === "operator" ? scopeValue : null);
        const assignedTeam =
          (priorPolicy.preferred_team as string | undefined) ??
          (priorPolicy.assigned_team as string | undefined) ??
          (scopeType === "team" ? scopeValue : null);

        const overrideInsert = await supabase
          .from("governance_routing_overrides")
          .insert({
            workspace_id: workspaceId,
            assigned_user: assignedUser ?? null,
            assigned_team: assignedTeam ?? null,
            reason: `rollback_restore:${body.executedBy}`,
            is_enabled: true,
            metadata: {
              source: "routing_policy_rollback",
              restored_by: body.executedBy,
              rollback_candidate_id: body.rollbackCandidateId,
              prior_policy: priorPolicy,
            },
          })
          .select("*")
          .single();
        if (overrideInsert.error) throw new Error(overrideInsert.error.message);
        restoredRoutingRow = overrideInsert.data as Record<string, unknown>;
      } else {
        const assignTeam =
          (priorPolicy.preferred_team as string | undefined) ??
          (priorPolicy.assign_team as string | undefined) ??
          (scopeType === "team" ? scopeValue : null);
        const assignUser =
          (priorPolicy.preferred_operator as string | undefined) ??
          (priorPolicy.assign_user as string | undefined) ?? null;
        const rootCauseCode =
          (priorPolicy.root_cause_code as string | undefined) ??
          (scopeType === "root_cause" ? scopeValue : null);
        const regime =
          (priorPolicy.regime as string | undefined) ??
          (scopeType === "regime" ? scopeValue : null);
        const severity =
          (priorPolicy.severity as string | undefined) ??
          (scopeType === "severity" ? scopeValue : null);
        const chronicOnly =
          (priorPolicy.chronic_only as boolean | undefined) ?? scopeType === "chronicity";

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
            routing_reason_template: `rollback_restore:${scopeType}:${scopeValue}`,
            metadata: {
              source: "routing_policy_rollback",
              restored_by: body.executedBy,
              rollback_candidate_id: body.rollbackCandidateId,
              prior_policy: priorPolicy,
            },
          })
          .select("*")
          .single();
        if (ruleInsert.error) throw new Error(ruleInsert.error.message);
        restoredRoutingRow = ruleInsert.data as Record<string, unknown>;
      }

      // persist rollback execution record
      const execInsert = await supabase
        .from("governance_routing_policy_rollback_executions")
        .insert({
          workspace_id: workspaceId,
          rollback_candidate_id: body.rollbackCandidateId,
          execution_target: executionTarget,
          scope_type: scopeType,
          scope_value: scopeValue,
          promotion_execution_id: promotionExecutionId,
          restored_policy: priorPolicy,
          replaced_policy: appliedPolicy,
          executed_by: body.executedBy,
          metadata: {
            source: "ops_api",
            restored_routing_row_id: restoredRoutingRow?.id ?? null,
          },
        })
        .select("*")
        .single();
      if (execInsert.error) throw new Error(execInsert.error.message);

      // mark candidate resolved
      const resolveUpdate = await supabase
        .from("governance_routing_policy_autopromotion_rollback_candidates")
        .update({
          resolved: true,
          resolved_at: new Date().toISOString(),
          resolved_by: body.executedBy,
        })
        .eq("id", body.rollbackCandidateId)
        .eq("workspace_id", workspaceId)
        .select("*")
        .single();
      if (resolveUpdate.error) throw new Error(resolveUpdate.error.message);

      return NextResponse.json({
        ok: true,
        execution: execInsert.data,
        candidate: resolveUpdate.data,
        restoredRoutingRow,
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
