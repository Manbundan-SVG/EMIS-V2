import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";
import { refreshGovernanceCaseGeneratedSummary } from "@/lib/queries/governance_cases";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json() as {
      caseId?: string;
      assignedTo?: string | null;
      assignedTeam?: string | null;
      assignedBy?: string | null;
      reason?: string | null;
      handoffNote?: string | null;
    };

    if (!body.caseId) throw new Error("caseId is required");

    const supabase = createServiceSupabaseClient();
    const caseLookup = await supabase
      .from("governance_case_summary")
      .select("id, workspace_id, watchlist_id, version_tuple, current_assignee, current_team, opened_at, severity, recurrence_group_id, repeat_count")
      .eq("id", body.caseId)
      .single();

    if (caseLookup.error || !caseLookup.data) throw new Error(caseLookup.error?.message ?? "case not found");

    const [summaryLookup, latestAssignmentLookup] = await Promise.all([
      supabase
        .from("governance_case_summary_latest")
        .select("root_cause_code")
        .eq("case_id", body.caseId)
        .maybeSingle(),
      supabase
        .from("governance_assignments")
        .select("id, assigned_at")
        .eq("case_id", body.caseId)
        .eq("active", true)
        .order("assigned_at", { ascending: false })
        .limit(1)
        .maybeSingle(),
    ]);
    if (summaryLookup.error) throw new Error(summaryLookup.error.message);
    if (latestAssignmentLookup.error) throw new Error(latestAssignmentLookup.error.message);

    await supabase
      .from("governance_assignments")
      .update({ active: false })
      .eq("case_id", body.caseId)
      .eq("active", true);

    const assignmentInsert = await supabase
      .from("governance_assignments")
      .insert({
        case_id: body.caseId,
        workspace_id: caseLookup.data.workspace_id,
        assigned_to: body.assignedTo ?? null,
        assigned_team: body.assignedTeam ?? null,
        assigned_by: body.assignedBy ?? null,
        reason: body.reason ?? null,
        active: true,
        metadata: { source: "ops_api" },
      })
      .select("*")
      .single();
    if (assignmentInsert.error) throw new Error(assignmentInsert.error.message);

    const historyInsert = await supabase
      .from("governance_assignment_history")
      .insert({
        case_id: body.caseId,
        workspace_id: caseLookup.data.workspace_id,
        previous_assignee: caseLookup.data.current_assignee,
        previous_team: caseLookup.data.current_team,
        new_assignee: body.assignedTo ?? null,
        new_team: body.assignedTeam ?? null,
        changed_by: body.assignedBy ?? null,
        reason: body.reason ?? null,
        metadata: { source: "ops_api" },
      });
    if (historyInsert.error) throw new Error(historyInsert.error.message);

    const caseUpdate = await supabase
      .from("governance_cases")
      .update({
        current_assignee: body.assignedTo ?? null,
        current_team: body.assignedTeam ?? null,
        status: "in_progress",
        updated_at: new Date().toISOString(),
      })
      .eq("id", body.caseId)
      .select("*")
      .single();
    if (caseUpdate.error) throw new Error(caseUpdate.error.message);

    const caseEventInsert = await supabase
      .from("governance_case_events")
      .insert({
        case_id: body.caseId,
        workspace_id: caseLookup.data.workspace_id,
        event_type: "assignment_changed",
        actor: body.assignedBy ?? null,
        payload: {
          previous_assignee: caseLookup.data.current_assignee,
          previous_team: caseLookup.data.current_team,
          new_assignee: body.assignedTo ?? null,
          new_team: body.assignedTeam ?? null,
          reason: body.reason ?? null,
        },
      });
    if (caseEventInsert.error) throw new Error(caseEventInsert.error.message);

    const assignmentChanged =
      caseLookup.data.current_assignee !== (body.assignedTo ?? null) ||
      caseLookup.data.current_team !== (body.assignedTeam ?? null);
    const minutesSinceOpen = caseLookup.data.opened_at
      ? Math.max(0, Math.floor((Date.now() - Date.parse(caseLookup.data.opened_at)) / 60000))
      : null;
    const minutesSinceLastAssignment = latestAssignmentLookup.data?.assigned_at
      ? Math.max(0, Math.floor((Date.now() - Date.parse(latestAssignmentLookup.data.assigned_at)) / 60000))
      : null;
    const [computeVersion, signalRegistryVersion, modelVersion] = (caseLookup.data.version_tuple ?? "").split("|");
    const trimmedHandoff = body.handoffNote?.trim();

    const feedbackInsert = await supabase
      .from("governance_routing_feedback")
      .insert({
        workspace_id: caseLookup.data.workspace_id,
        case_id: body.caseId,
        routing_decision_id: null,
        feedback_type: assignmentChanged ? "manual_reassign" : "accepted",
        feedback_status: "active",
        assigned_to: body.assignedTo ?? null,
        assigned_team: body.assignedTeam ?? null,
        prior_assigned_to: caseLookup.data.current_assignee,
        prior_assigned_team: caseLookup.data.current_team,
        root_cause_code: summaryLookup.data?.root_cause_code ?? null,
        severity: caseLookup.data.severity,
        recurrence_group_id: caseLookup.data.recurrence_group_id,
        repeat_count: caseLookup.data.repeat_count ?? 1,
        reason: body.reason ?? null,
        metadata: {
          source: "ops_api",
          assignment_id: assignmentInsert.data.id,
          assignment_changed: assignmentChanged,
        },
      })
      .select("*")
      .single();
    if (feedbackInsert.error) throw new Error(feedbackInsert.error.message);

    let reassignmentRow: Record<string, unknown> | null = null;
    if (assignmentChanged) {
      const reassignmentInsert = await supabase
        .from("governance_reassignment_events")
        .insert({
          workspace_id: caseLookup.data.workspace_id,
          case_id: body.caseId,
          routing_decision_id: null,
          previous_assigned_to: caseLookup.data.current_assignee,
          previous_assigned_team: caseLookup.data.current_team,
          new_assigned_to: body.assignedTo ?? null,
          new_assigned_team: body.assignedTeam ?? null,
          reassignment_type: "manual_override",
          reassignment_reason: body.reason ?? null,
          minutes_since_open: minutesSinceOpen,
          minutes_since_last_assignment: minutesSinceLastAssignment,
          metadata: {
            source: "ops_api",
            assignment_id: assignmentInsert.data.id,
            feedback_id: feedbackInsert.data.id,
          },
        })
        .select("*")
        .single();
      if (reassignmentInsert.error) throw new Error(reassignmentInsert.error.message);
      reassignmentRow = reassignmentInsert.data;
    }

    const routingOutcomeInsert = await supabase
      .from("governance_routing_outcomes")
      .insert({
        workspace_id: caseLookup.data.workspace_id,
        case_id: body.caseId,
        routing_decision_id: null,
        assignment_id: assignmentInsert.data.id,
        assigned_to: body.assignedTo ?? null,
        assigned_team: body.assignedTeam ?? null,
        root_cause_code: summaryLookup.data?.root_cause_code ?? null,
        severity: caseLookup.data.severity,
        watchlist_id: caseLookup.data.watchlist_id,
        compute_version: computeVersion || null,
        signal_registry_version: signalRegistryVersion || null,
        model_version: modelVersion || null,
        recurrence_group_id: caseLookup.data.recurrence_group_id,
        repeat_count: caseLookup.data.repeat_count ?? 1,
        outcome_type: assignmentChanged ? "reassigned" : "assigned",
        outcome_value: null,
        outcome_context: {
          source: "ops_api",
          reason: body.reason ?? null,
          assignment_changed: assignmentChanged,
          handoff_note_present: Boolean(trimmedHandoff),
        },
        occurred_at: assignmentInsert.data.assigned_at ?? new Date().toISOString(),
      })
      .select("*")
      .single();
    if (routingOutcomeInsert.error) throw new Error(routingOutcomeInsert.error.message);

    const recommendationLookup = await supabase
      .from("governance_routing_recommendation_summary")
      .select("id, recommended_user, recommended_team")
      .eq("case_id", body.caseId)
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (recommendationLookup.error) throw new Error(recommendationLookup.error.message);

    let recommendationFeedbackRow: Record<string, unknown> | null = null;
    if (recommendationLookup.data) {
      const recommendationAccepted =
        recommendationLookup.data.recommended_user === (body.assignedTo ?? null) &&
        recommendationLookup.data.recommended_team === (body.assignedTeam ?? null);
      const recommendationFeedback = await supabase
        .from("governance_routing_recommendations")
        .update({
          accepted: recommendationAccepted,
          accepted_at: new Date().toISOString(),
          accepted_by: body.assignedBy ?? null,
          override_reason: recommendationAccepted ? null : (body.reason ?? "manual_assignment_override"),
          applied: recommendationAccepted,
          applied_at: recommendationAccepted ? new Date().toISOString() : null,
          updated_at: new Date().toISOString(),
        })
        .eq("id", recommendationLookup.data.id)
        .select("*")
        .single();
      if (recommendationFeedback.error) throw new Error(recommendationFeedback.error.message);
      recommendationFeedbackRow = recommendationFeedback.data;
    }

    const target = body.assignedTo ?? body.assignedTeam ?? "unassigned";
    const timelineInsert = await supabase
      .from("governance_incident_timeline_events")
      .insert({
        case_id: body.caseId,
        workspace_id: caseLookup.data.workspace_id,
        event_type: "assignment_changed",
        event_source: "governance_assignment",
        actor: body.assignedBy ?? null,
        title: `Assigned to ${target}`,
        detail: body.reason ?? null,
        metadata: {
          previous_assignee: caseLookup.data.current_assignee,
          previous_team: caseLookup.data.current_team,
          new_assignee: body.assignedTo ?? null,
          new_team: body.assignedTeam ?? null,
          source: "ops_api",
        },
        source_table: "governance_assignments",
        source_id: assignmentInsert.data.id,
      });
    if (timelineInsert.error) throw new Error(timelineInsert.error.message);

    let handoffNoteRow: Record<string, unknown> | null = null;
    if (trimmedHandoff) {
      const noteInsert = await supabase
        .from("governance_case_notes")
        .insert({
          case_id: body.caseId,
          workspace_id: caseLookup.data.workspace_id,
          author: body.assignedBy ?? null,
          note: trimmedHandoff,
          note_type: "handoff",
          visibility: "internal",
          metadata: {
            source: "ops_api",
            assignment_id: assignmentInsert.data.id,
            previous_assignee: caseLookup.data.current_assignee,
            previous_team: caseLookup.data.current_team,
            new_assignee: body.assignedTo ?? null,
            new_team: body.assignedTeam ?? null,
          },
          edited_at: null,
        })
        .select("*")
        .single();
      if (noteInsert.error) throw new Error(noteInsert.error.message);
      handoffNoteRow = noteInsert.data;

      const [noteEventInsert, noteTimelineInsert] = await Promise.all([
        supabase
          .from("governance_case_events")
          .insert({
            case_id: body.caseId,
            workspace_id: caseLookup.data.workspace_id,
            event_type: "case_note_added",
            actor: body.assignedBy ?? null,
            payload: {
              note_type: "handoff",
              note_id: noteInsert.data.id,
              assignment_id: assignmentInsert.data.id,
            },
          }),
        supabase
          .from("governance_incident_timeline_events")
          .insert({
            case_id: body.caseId,
            workspace_id: caseLookup.data.workspace_id,
            event_type: "case_note_added",
            event_source: "governance_case_notes",
            actor: body.assignedBy ?? null,
            title: "Handoff note added",
            detail: trimmedHandoff,
            metadata: {
              note_type: "handoff",
              note_id: noteInsert.data.id,
              assignment_id: assignmentInsert.data.id,
              source: "ops_api",
            },
            source_table: "governance_case_notes",
            source_id: noteInsert.data.id,
          }),
      ]);
      if (noteEventInsert.error) throw new Error(noteEventInsert.error.message);
      if (noteTimelineInsert.error) throw new Error(noteTimelineInsert.error.message);
    }

    return NextResponse.json({
      ok: true,
      assignment: assignmentInsert.data,
      case: caseUpdate.data,
      routingFeedback: feedbackInsert.data,
      routingOutcome: routingOutcomeInsert.data,
      recommendationFeedback: recommendationFeedbackRow,
      reassignment: reassignmentRow,
      handoffNote: handoffNoteRow,
      summary: await refreshGovernanceCaseGeneratedSummary(body.caseId),
    });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
