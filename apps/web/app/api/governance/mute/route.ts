import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";

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
      targetType?: string;
      targetKey?: string;
      createdBy?: string;
      mutedUntil?: string | null;
      reason?: string | null;
      degradationStateId?: string | null;
    };

    const workspaceSlug = body.workspace ?? "demo";
    if (!body.targetType) throw new Error("targetType is required");
    if (!body.targetKey) throw new Error("targetKey is required");
    if (!body.createdBy) throw new Error("createdBy is required");

    const workspaceId = await getWorkspaceIdFromSlug(workspaceSlug);
    const supabase = createServiceSupabaseClient();

    const muteInsert = await supabase
      .from("governance_muting_rules")
      .insert({
        workspace_id: workspaceId,
        target_type: body.targetType,
        target_key: body.targetKey,
        reason: body.reason ?? null,
        muted_until: body.mutedUntil ?? null,
        created_by: body.createdBy,
        is_active: true,
        metadata: { source: "ops_api" },
      })
      .select("*")
      .single();

    if (muteInsert.error) throw new Error(muteInsert.error.message);

    let action = null;
    let caseId: string | null = null;
    if (body.degradationStateId) {
      const caseLookup = await supabase
        .from("governance_cases")
        .select("id")
        .eq("degradation_state_id", body.degradationStateId)
        .in("status", ["open", "acknowledged", "in_progress"])
        .maybeSingle();
      if (caseLookup.error) throw new Error(caseLookup.error.message);
      caseId = caseLookup.data?.id ?? null;

      const actionInsert = await supabase
        .from("governance_resolution_actions")
        .insert({
          workspace_id: workspaceId,
          degradation_state_id: body.degradationStateId,
          action_type: "muted",
          performed_by: body.createdBy,
          note: body.reason ?? null,
          metadata: {
            source: "ops_api",
            target_type: body.targetType,
            target_key: body.targetKey,
            muted_until: body.mutedUntil ?? null,
          },
        })
        .select("*")
        .single();
      if (actionInsert.error) throw new Error(actionInsert.error.message);
      action = actionInsert.data;

      if (caseId) {
        const caseEventInsert = await supabase
          .from("governance_case_events")
          .insert({
            case_id: caseId,
            workspace_id: workspaceId,
            event_type: "case_muted",
            actor: body.createdBy,
            payload: {
              target_type: body.targetType,
              target_key: body.targetKey,
              muted_until: body.mutedUntil ?? null,
              reason: body.reason ?? null,
            },
          });
        if (caseEventInsert.error) throw new Error(caseEventInsert.error.message);

        const timelineInsert = await supabase
          .from("governance_incident_timeline_events")
          .insert({
            case_id: caseId,
            workspace_id: workspaceId,
            event_type: "case_muted",
            event_source: "governance_muting_rule",
            actor: body.createdBy,
            title: "Case muted",
            detail: body.reason ?? null,
            metadata: {
              target_type: body.targetType,
              target_key: body.targetKey,
              muted_until: body.mutedUntil ?? null,
              source: "ops_api",
            },
            source_table: "governance_muting_rules",
            source_id: muteInsert.data.id,
          });
        if (timelineInsert.error) throw new Error(timelineInsert.error.message);
      }
    }

    return NextResponse.json({
      ok: true,
      muteRule: muteInsert.data,
      action,
    });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
