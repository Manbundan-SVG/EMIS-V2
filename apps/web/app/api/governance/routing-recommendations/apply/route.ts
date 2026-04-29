import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";
import { refreshGovernanceCaseGeneratedSummary } from "@/lib/queries/governance_cases";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json() as {
      recommendationId?: string;
      appliedBy?: string | null;
      applicationReason?: string | null;
    };

    if (!body.recommendationId) throw new Error("recommendationId is required");

    const supabase = createServiceSupabaseClient();
    const recommendationLookup = await supabase
      .from("governance_routing_recommendation_summary")
      .select("*")
      .eq("id", body.recommendationId)
      .single();
    if (recommendationLookup.error || !recommendationLookup.data) {
      throw new Error(recommendationLookup.error?.message ?? "recommendation not found");
    }

    const latestReview = await supabase
      .from("governance_routing_recommendation_reviews")
      .select("*")
      .eq("recommendation_id", body.recommendationId)
      .order("reviewed_at", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (latestReview.error) throw new Error(latestReview.error.message);
    if (!latestReview.data || latestReview.data.review_status !== "approved") {
      throw new Error("recommendation must be approved before application");
    }

    if (!recommendationLookup.data.recommended_user && !recommendationLookup.data.recommended_team) {
      throw new Error("recommendation must target a user or team");
    }

    const caseLookup = await supabase
      .from("governance_case_summary")
      .select("id, workspace_id, current_assignee, current_team, severity, watchlist_id, version_tuple, recurrence_group_id, repeat_count")
      .eq("id", recommendationLookup.data.case_id)
      .single();
    if (caseLookup.error || !caseLookup.data) {
      throw new Error(caseLookup.error?.message ?? "case not found");
    }

    const summaryLookup = await supabase
      .from("governance_case_summary_latest")
      .select("root_cause_code")
      .eq("case_id", recommendationLookup.data.case_id)
      .maybeSingle();
    if (summaryLookup.error) throw new Error(summaryLookup.error.message);

    await supabase
      .from("governance_assignments")
      .update({ active: false })
      .eq("case_id", recommendationLookup.data.case_id)
      .eq("active", true);

    const assignmentInsert = await supabase
      .from("governance_assignments")
      .insert({
        case_id: recommendationLookup.data.case_id,
        workspace_id: recommendationLookup.data.workspace_id,
        assigned_to: recommendationLookup.data.recommended_user,
        assigned_team: recommendationLookup.data.recommended_team,
        assigned_by: body.appliedBy ?? null,
        reason: body.applicationReason ?? "approved_routing_recommendation",
        active: true,
        metadata: {
          source: "routing_recommendation_apply",
          recommendation_id: body.recommendationId,
          review_id: latestReview.data.id,
        },
      })
      .select("*")
      .single();
    if (assignmentInsert.error) throw new Error(assignmentInsert.error.message);

    const historyInsert = await supabase
      .from("governance_assignment_history")
      .insert({
        case_id: recommendationLookup.data.case_id,
        workspace_id: recommendationLookup.data.workspace_id,
        previous_assignee: caseLookup.data.current_assignee,
        previous_team: caseLookup.data.current_team,
        new_assignee: recommendationLookup.data.recommended_user,
        new_team: recommendationLookup.data.recommended_team,
        changed_by: body.appliedBy ?? null,
        reason: body.applicationReason ?? "approved_routing_recommendation",
        metadata: {
          source: "routing_recommendation_apply",
          recommendation_id: body.recommendationId,
          review_id: latestReview.data.id,
        },
      });
    if (historyInsert.error) throw new Error(historyInsert.error.message);

    const caseUpdate = await supabase
      .from("governance_cases")
      .update({
        current_assignee: recommendationLookup.data.recommended_user,
        current_team: recommendationLookup.data.recommended_team,
        status: "in_progress",
        updated_at: new Date().toISOString(),
      })
      .eq("id", recommendationLookup.data.case_id)
      .select("*")
      .single();
    if (caseUpdate.error) throw new Error(caseUpdate.error.message);

    const applicationInsert = await supabase
      .from("governance_routing_applications")
      .insert({
        workspace_id: recommendationLookup.data.workspace_id,
        recommendation_id: body.recommendationId,
        review_id: latestReview.data.id,
        case_id: recommendationLookup.data.case_id,
        previous_assigned_user: caseLookup.data.current_assignee,
        previous_assigned_team: caseLookup.data.current_team,
        applied_user: recommendationLookup.data.recommended_user,
        applied_team: recommendationLookup.data.recommended_team,
        application_mode: "api_reviewed",
        application_reason: body.applicationReason ?? "approved_routing_recommendation",
        applied_by: body.appliedBy ?? null,
        metadata: { source: "ops_api" },
      })
      .select("*")
      .single();
    if (applicationInsert.error) throw new Error(applicationInsert.error.message);

    const [computeVersion, signalRegistryVersion, modelVersion] = (caseLookup.data.version_tuple ?? "").split("|");
    const routingOutcomeInsert = await supabase
      .from("governance_routing_outcomes")
      .insert({
        workspace_id: recommendationLookup.data.workspace_id,
        case_id: recommendationLookup.data.case_id,
        routing_decision_id: null,
        assignment_id: assignmentInsert.data.id,
        assigned_to: recommendationLookup.data.recommended_user,
        assigned_team: recommendationLookup.data.recommended_team,
        root_cause_code: summaryLookup.data?.root_cause_code ?? null,
        severity: caseLookup.data.severity,
        watchlist_id: caseLookup.data.watchlist_id,
        compute_version: computeVersion || null,
        signal_registry_version: signalRegistryVersion || null,
        model_version: modelVersion || null,
        recurrence_group_id: caseLookup.data.recurrence_group_id,
        repeat_count: caseLookup.data.repeat_count ?? 1,
        outcome_type: "reassigned",
        outcome_value: null,
        outcome_context: {
          source: "routing_recommendation_apply",
          recommendation_id: body.recommendationId,
          review_id: latestReview.data.id,
          application_id: applicationInsert.data.id,
        },
        occurred_at: applicationInsert.data.applied_at,
      })
      .select("*")
      .single();
    if (routingOutcomeInsert.error) throw new Error(routingOutcomeInsert.error.message);

    const feedbackInsert = await supabase
      .from("governance_routing_feedback")
      .insert({
        workspace_id: recommendationLookup.data.workspace_id,
        case_id: recommendationLookup.data.case_id,
        routing_decision_id: null,
        feedback_type: "accepted",
        feedback_status: "active",
        assigned_to: recommendationLookup.data.recommended_user,
        assigned_team: recommendationLookup.data.recommended_team,
        prior_assigned_to: caseLookup.data.current_assignee,
        prior_assigned_team: caseLookup.data.current_team,
        root_cause_code: summaryLookup.data?.root_cause_code ?? null,
        severity: caseLookup.data.severity,
        recurrence_group_id: caseLookup.data.recurrence_group_id,
        repeat_count: caseLookup.data.repeat_count ?? 1,
        reason: body.applicationReason ?? "approved_routing_recommendation",
        metadata: {
          source: "routing_recommendation_apply",
          recommendation_id: body.recommendationId,
          review_id: latestReview.data.id,
          application_id: applicationInsert.data.id,
        },
      })
      .select("*")
      .single();
    if (feedbackInsert.error) throw new Error(feedbackInsert.error.message);

    const caseEventInsert = await supabase
      .from("governance_case_events")
      .insert({
        case_id: recommendationLookup.data.case_id,
        workspace_id: recommendationLookup.data.workspace_id,
        event_type: "routing_recommendation_applied",
        actor: body.appliedBy ?? null,
        payload: {
          recommendation_id: body.recommendationId,
          review_id: latestReview.data.id,
          application_id: applicationInsert.data.id,
          previous_assignee: caseLookup.data.current_assignee,
          previous_team: caseLookup.data.current_team,
          applied_user: recommendationLookup.data.recommended_user,
          applied_team: recommendationLookup.data.recommended_team,
        },
      });
    if (caseEventInsert.error) throw new Error(caseEventInsert.error.message);

    const timelineInsert = await supabase
      .from("governance_incident_timeline_events")
      .insert({
        case_id: recommendationLookup.data.case_id,
        workspace_id: recommendationLookup.data.workspace_id,
        event_type: "assignment_changed",
        event_source: "routing_recommendation",
        actor: body.appliedBy ?? null,
        title: `Recommendation applied to ${recommendationLookup.data.recommended_user ?? recommendationLookup.data.recommended_team ?? "unassigned"}`,
        detail: body.applicationReason ?? "approved routing recommendation",
        metadata: {
          recommendation_id: body.recommendationId,
          review_id: latestReview.data.id,
          application_id: applicationInsert.data.id,
          previous_assignee: caseLookup.data.current_assignee,
          previous_team: caseLookup.data.current_team,
          applied_user: recommendationLookup.data.recommended_user,
          applied_team: recommendationLookup.data.recommended_team,
        },
        source_table: "governance_routing_applications",
        source_id: applicationInsert.data.id,
      });
    if (timelineInsert.error) throw new Error(timelineInsert.error.message);

    const recommendationUpdate = await supabase
      .from("governance_routing_recommendations")
      .update({
        accepted: true,
        accepted_at: latestReview.data.reviewed_at,
        accepted_by: latestReview.data.reviewed_by ?? body.appliedBy ?? null,
        applied: true,
        applied_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      })
      .eq("id", body.recommendationId)
      .select("*")
      .single();
    if (recommendationUpdate.error) throw new Error(recommendationUpdate.error.message);

    return NextResponse.json({
      ok: true,
      assignment: assignmentInsert.data,
      application: applicationInsert.data,
      routingOutcome: routingOutcomeInsert.data,
      routingFeedback: feedbackInsert.data,
      recommendation: recommendationUpdate.data,
      case: caseUpdate.data,
      summary: await refreshGovernanceCaseGeneratedSummary(recommendationLookup.data.case_id),
    });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
