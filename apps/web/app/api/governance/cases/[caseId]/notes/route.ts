import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";
import {
  getGovernanceCaseNotes,
  refreshGovernanceCaseGeneratedSummary,
} from "@/lib/queries/governance_cases";

async function getCaseWorkspace(caseId: string): Promise<{ id: string; workspace_id: string } | null> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("governance_cases")
    .select("id, workspace_id")
    .eq("id", caseId)
    .maybeSingle();

  if (error) throw error;
  return data;
}

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ caseId: string }> },
) {
  try {
    const { caseId } = await context.params;
    const notes = await getGovernanceCaseNotes(caseId);
    return NextResponse.json({ ok: true, notes });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ caseId: string }> },
) {
  try {
    const { caseId } = await context.params;
    const body = await request.json() as {
      note?: string;
      noteType?: string | null;
      author?: string | null;
      visibility?: string | null;
      metadata?: Record<string, unknown> | null;
    };

    const note = body.note?.trim();
    if (!note) throw new Error("note is required");

    const caseRow = await getCaseWorkspace(caseId);
    if (!caseRow) {
      return NextResponse.json({ ok: false, error: "not_found" }, { status: 404 });
    }

    const supabase = createServiceSupabaseClient();
    const noteType = body.noteType?.trim() || "investigation";
    const visibility = body.visibility?.trim() || "internal";
    const metadata = {
      source: "ops_api",
      ...(body.metadata ?? {}),
    };

    const noteInsert = await supabase
      .from("governance_case_notes")
      .insert({
        case_id: caseId,
        workspace_id: caseRow.workspace_id,
        author: body.author ?? null,
        note,
        note_type: noteType,
        visibility,
        metadata,
        edited_at: null,
      })
      .select("*")
      .single();
    if (noteInsert.error) throw new Error(noteInsert.error.message);

    const [caseEventInsert, timelineInsert] = await Promise.all([
      supabase
        .from("governance_case_events")
        .insert({
          case_id: caseId,
          workspace_id: caseRow.workspace_id,
          event_type: "case_note_added",
          actor: body.author ?? null,
          payload: {
            note_type: noteType,
            visibility,
            note_id: noteInsert.data.id,
          },
        }),
      supabase
        .from("governance_incident_timeline_events")
        .insert({
          case_id: caseId,
          workspace_id: caseRow.workspace_id,
          event_type: "case_note_added",
          event_source: "governance_case_notes",
          actor: body.author ?? null,
          title: `${noteType.replace(/_/g, " ")} note added`,
          detail: note,
          metadata: {
            note_type: noteType,
            visibility,
            note_id: noteInsert.data.id,
            source: "ops_api",
          },
          source_table: "governance_case_notes",
          source_id: noteInsert.data.id,
        }),
    ]);

    if (caseEventInsert.error) throw new Error(caseEventInsert.error.message);
    if (timelineInsert.error) throw new Error(timelineInsert.error.message);

    await supabase
      .from("governance_cases")
      .update({ updated_at: new Date().toISOString() })
      .eq("id", caseId);

    const summary = await refreshGovernanceCaseGeneratedSummary(caseId);
    return NextResponse.json({ ok: true, note: noteInsert.data, summary });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
