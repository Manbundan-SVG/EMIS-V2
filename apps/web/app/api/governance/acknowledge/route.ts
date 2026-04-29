import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";
import { refreshGovernanceCaseGeneratedSummary } from "@/lib/queries/governance_cases";

async function getWorkspaceIdFromSlug(workspaceSlug: string): Promise<string> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("workspaces")
    .select("id")
    .eq("slug", workspaceSlug)
    .single();
  if (error || !data) throw new Error(`Workspace not found: ${workspaceSlug}`);
  return data.id;
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json() as {
      workspace?: string;
      degradationStateId?: string;
      acknowledgedBy?: string;
      note?: string | null;
    };

    const workspaceSlug = body.workspace ?? "demo";
    if (!body.degradationStateId) throw new Error("degradationStateId is required");
    if (!body.acknowledgedBy) throw new Error("acknowledgedBy is required");

    const workspaceId = await getWorkspaceIdFromSlug(workspaceSlug);
    const supabase = createServiceSupabaseClient();
    const now = new Date().toISOString();
    const caseLookup = await supabase
      .from("governance_case_summary")
      .select("id, workspace_id, watchlist_id, version_tuple, current_assignee, current_team, opened_at, severity, recurrence_group_id, repeat_count")
      .eq("degradation_state_id", body.degradationStateId)
      .in("status", ["open", "in_progress", "acknowledged"])
      .order("opened_at", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (caseLookup.error) throw new Error(caseLookup.error.message);

    const [ackInsert, actionInsert] = await Promise.all([
      supabase
        .from("governance_acknowledgments")
        .insert({
          workspace_id: workspaceId,
          degradation_state_id: body.degradationStateId,
          acknowledged_by: body.acknowledgedBy,
          note: body.note ?? null,
          metadata: { source: "ops_api" },
          acknowledged_at: now,
        })
        .select("*")
        .single(),
      supabase
        .from("governance_resolution_actions")
        .insert({
          workspace_id: workspaceId,
          degradation_state_id: body.degradationStateId,
          action_type: "acknowledged",
          performed_by: body.acknowledgedBy,
          note: body.note ?? null,
          metadata: { source: "ops_api" },
          created_at: now,
        })
        .select("*")
        .single(),
    ]);

    if (ackInsert.error) throw new Error(ackInsert.error.message);
    if (actionInsert.error) throw new Error(actionInsert.error.message);

    const caseUpdate = await supabase
      .from("governance_cases")
      .update({
        status: "acknowledged",
        acknowledged_at: now,
        updated_at: now,
      })
      .eq("degradation_state_id", body.degradationStateId)
      .in("status", ["open", "in_progress"])
      .select("id")
      .maybeSingle();
    if (caseUpdate.error) throw new Error(caseUpdate.error.message);
    if (caseUpdate.data?.id) {
      const activeCase = caseLookup.data && caseLookup.data.id === caseUpdate.data.id
        ? caseLookup.data
        : null;
      const [computeVersion, signalRegistryVersion, modelVersion] = (activeCase?.version_tuple ?? "").split("|");
      const summaryLookup = activeCase
        ? await supabase
            .from("governance_case_summary_latest")
            .select("root_cause_code")
            .eq("case_id", activeCase.id)
            .maybeSingle()
        : null;
      if (summaryLookup?.error) throw new Error(summaryLookup.error.message);
      const caseEventInsert = await supabase
        .from("governance_case_events")
        .insert({
          case_id: caseUpdate.data.id,
          workspace_id: workspaceId,
          event_type: "case_acknowledged",
          actor: body.acknowledgedBy,
          payload: {
            note: body.note ?? null,
            degradation_state_id: body.degradationStateId,
          },
        });
      if (caseEventInsert.error) throw new Error(caseEventInsert.error.message);

      const timelineInsert = await supabase
        .from("governance_incident_timeline_events")
        .insert({
          case_id: caseUpdate.data.id,
          workspace_id: workspaceId,
          event_type: "case_acknowledged",
          event_source: "governance_acknowledgment",
          event_at: now,
          actor: body.acknowledgedBy,
          title: "Case acknowledged",
          detail: body.note ?? null,
          metadata: {
            degradation_state_id: body.degradationStateId,
            source: "ops_api",
          },
          source_table: "governance_acknowledgments",
          source_id: ackInsert.data.id,
      });
      if (timelineInsert.error) throw new Error(timelineInsert.error.message);

      if (activeCase) {
        const ackHours = activeCase.opened_at
          ? Math.max((Date.parse(now) - Date.parse(activeCase.opened_at)) / 3600000, 0)
          : null;
        const outcomeRows = [
          {
            workspace_id: workspaceId,
            case_id: caseUpdate.data.id,
            routing_decision_id: null,
            assignment_id: null,
            assigned_to: activeCase.current_assignee,
            assigned_team: activeCase.current_team,
            root_cause_code: summaryLookup?.data?.root_cause_code ?? null,
            severity: activeCase.severity,
            watchlist_id: activeCase.watchlist_id,
            compute_version: computeVersion || null,
            signal_registry_version: signalRegistryVersion || null,
            model_version: modelVersion || null,
            recurrence_group_id: activeCase.recurrence_group_id,
            repeat_count: activeCase.repeat_count ?? 1,
            outcome_type: "acknowledged",
            outcome_value: ackHours,
            outcome_context: {
              source: "ops_api",
              metric: "time_to_ack_hours",
            },
            occurred_at: now,
          },
        ];
        if (ackHours !== null) {
          outcomeRows.push({
            ...outcomeRows[0],
            outcome_type: "time_to_ack_hours",
          });
        }
        const outcomeInsert = await supabase
          .from("governance_routing_outcomes")
          .insert(outcomeRows);
        if (outcomeInsert.error) throw new Error(outcomeInsert.error.message);
      }
    }

    return NextResponse.json({
      ok: true,
      acknowledgment: ackInsert.data,
      action: actionInsert.data,
      summary: caseUpdate.data?.id ? await refreshGovernanceCaseGeneratedSummary(caseUpdate.data.id) : null,
    });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
