import { NextRequest, NextResponse } from "next/server";
import { createServiceSupabaseClient } from "@/lib/supabase";
import {
  getGovernanceCaseEvidence,
  getGovernanceCaseEvidenceSummary,
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
    const [evidence, summary] = await Promise.all([
      getGovernanceCaseEvidence(caseId),
      getGovernanceCaseEvidenceSummary(caseId),
    ]);
    return NextResponse.json({ ok: true, evidence, summary });
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
      evidenceType?: string;
      referenceId?: string;
      title?: string | null;
      summary?: string | null;
      payload?: Record<string, unknown> | null;
    };

    if (!body.evidenceType?.trim()) throw new Error("evidenceType is required");
    if (!body.referenceId?.trim()) throw new Error("referenceId is required");

    const caseRow = await getCaseWorkspace(caseId);
    if (!caseRow) {
      return NextResponse.json({ ok: false, error: "not_found" }, { status: 404 });
    }

    const supabase = createServiceSupabaseClient();
    const evidenceType = body.evidenceType.trim();
    const referenceId = body.referenceId.trim();

    const existing = await supabase
      .from("governance_case_evidence")
      .select("*")
      .eq("case_id", caseId)
      .eq("evidence_type", evidenceType)
      .eq("reference_id", referenceId)
      .maybeSingle();
    if (existing.error) throw new Error(existing.error.message);
    if (existing.data) {
      const summary = await getGovernanceCaseEvidenceSummary(caseId);
      const generatedSummary = await refreshGovernanceCaseGeneratedSummary(caseId);
      return NextResponse.json({ ok: true, evidence: existing.data, summary, generatedSummary, deduped: true });
    }

    const evidenceInsert = await supabase
      .from("governance_case_evidence")
      .insert({
        case_id: caseId,
        workspace_id: caseRow.workspace_id,
        evidence_type: evidenceType,
        reference_id: referenceId,
        title: body.title ?? null,
        summary: body.summary ?? null,
        payload: {
          source: "ops_api",
          ...(body.payload ?? {}),
        },
      })
      .select("*")
      .single();
    if (evidenceInsert.error) throw new Error(evidenceInsert.error.message);

    const [caseEventInsert, timelineInsert] = await Promise.all([
      supabase
        .from("governance_case_events")
        .insert({
          case_id: caseId,
          workspace_id: caseRow.workspace_id,
          event_type: "case_evidence_linked",
          actor: null,
          payload: {
            evidence_type: evidenceType,
            reference_id: referenceId,
            evidence_id: evidenceInsert.data.id,
          },
        }),
      supabase
        .from("governance_incident_timeline_events")
        .insert({
          case_id: caseId,
          workspace_id: caseRow.workspace_id,
          event_type: "case_evidence_linked",
          event_source: "governance_case_evidence",
          actor: null,
          title: `${evidenceType.replace(/_/g, " ")} evidence linked`,
          detail: body.summary ?? null,
          metadata: {
            evidence_type: evidenceType,
            reference_id: referenceId,
            evidence_id: evidenceInsert.data.id,
            source: "ops_api",
          },
          source_table: "governance_case_evidence",
          source_id: evidenceInsert.data.id,
        }),
    ]);

    if (caseEventInsert.error) throw new Error(caseEventInsert.error.message);
    if (timelineInsert.error) throw new Error(timelineInsert.error.message);

    await supabase
      .from("governance_cases")
      .update({ updated_at: new Date().toISOString() })
      .eq("id", caseId);

    const summary = await getGovernanceCaseEvidenceSummary(caseId);
    const generatedSummary = await refreshGovernanceCaseGeneratedSummary(caseId);
    return NextResponse.json({ ok: true, evidence: evidenceInsert.data, summary, generatedSummary, deduped: false });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
