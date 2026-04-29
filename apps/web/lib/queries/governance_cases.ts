import { createServiceSupabaseClient } from "@/lib/supabase";
import type {
  GovernanceCaseEvidence,
  GovernanceCaseEvidenceSummary,
  GovernanceCaseGeneratedSummary,
  GovernanceCaseInvestigationSummary,
  GovernanceCaseNote,
  GovernanceCaseRecurrence,
  GovernanceIncidentDetail,
  GovernanceRelatedCase,
} from "@emis-types/ops";

type CaseSummaryInputs = {
  caseRow: Record<string, unknown>;
  notes: GovernanceCaseNote[];
  evidenceSummary: GovernanceCaseEvidenceSummary | null;
  recurrence: GovernanceCaseRecurrence;
  lifecycle: Record<string, unknown> | null;
};

function buildInvestigationSummary(notes: GovernanceCaseNote[]): GovernanceCaseInvestigationSummary {
  const latestByType = (noteType: string): GovernanceCaseNote | null =>
    notes.find((note) => note.note_type === noteType) ?? null;

  const latestNote = notes[0] ?? null;
  const latestHandoffNote = latestByType("handoff");
  const latestRootCauseNote = latestByType("root_cause");
  const latestClosureNote = latestByType("closure");

  return {
    latest_note: latestNote,
    latest_investigation_note: latestByType("investigation"),
    latest_handoff_note: latestHandoffNote,
    latest_root_cause_note: latestRootCauseNote,
    latest_closure_note: latestClosureNote,
    last_operator_summary: latestHandoffNote ?? latestRootCauseNote ?? latestClosureNote ?? latestNote,
  };
}

function buildRootCauseCode(
  caseRow: Record<string, unknown>,
  notes: GovernanceCaseNote[],
  evidence: GovernanceCaseEvidence[],
): string | null {
  const evidenceTypes = new Set(evidence.map((item) => item.evidence_type));
  const noteText = notes.map((note) => note.note).join(" ").toLowerCase();
  const summaryText = evidence.map((item) => item.summary ?? "").join(" ").toLowerCase();
  const combined = `${noteText} ${summaryText}`;

  if (evidenceTypes.has("replay_delta") || combined.includes("replay")) return "replay_inconsistency";
  if ((evidenceTypes.has("threshold_application") || evidenceTypes.has("regime_transition")) && combined.includes("regime")) {
    return "regime_conflict";
  }
  if (evidenceTypes.has("version_tuple") || combined.includes("version") || combined.includes("canary")) {
    return "version_regression";
  }
  if (combined.includes("scope")) return "watchlist_scope_drift";
  if (String(caseRow.severity ?? "") === "high" || String(caseRow.severity ?? "") === "critical") {
    return "family_instability";
  }
  if (notes.length > 0 || evidence.length > 0) return "mixed_or_unresolved";
  return null;
}

function buildGeneratedSummary(inputs: CaseSummaryInputs): Omit<GovernanceCaseGeneratedSummary, "id" | "workspace_id" | "case_id" | "generated_at" | "updated_at"> {
  const evidence = inputs.evidenceSummary?.evidence_items ?? [];
  const rootCauseCode = buildRootCauseCode(inputs.caseRow, inputs.notes, evidence);
  const investigation = buildInvestigationSummary(inputs.notes);
  const status = String(inputs.caseRow.status ?? "unknown");
  const severity = String(inputs.caseRow.severity ?? "unknown");
  const owner = String(inputs.caseRow.current_assignee ?? inputs.caseRow.current_team ?? "unassigned");

  let rootCauseSummary: string | null = null;
  if (rootCauseCode === "version_regression") rootCauseSummary = "Evidence points to a version-linked behavior change rather than a transient market-only issue.";
  else if (rootCauseCode === "regime_conflict") rootCauseSummary = "Signals and thresholds suggest the incident is tied to a regime transition or regime conflict.";
  else if (rootCauseCode === "replay_inconsistency") rootCauseSummary = "Replay-linked evidence suggests the incident is driven by inconsistent replay behavior.";
  else if (rootCauseCode === "watchlist_scope_drift") rootCauseSummary = "Evidence suggests the effective watchlist or dependency scope drifted from prior healthy behavior.";
  else if (rootCauseCode === "family_instability") rootCauseSummary = `Case severity and evidence profile indicate sustained family instability across ${evidence.length} linked artifacts.`;
  else if (rootCauseCode === "mixed_or_unresolved") rootCauseSummary = "Available evidence is insufficient to isolate a single root cause, so the case remains mixed or unresolved.";

  const evidenceTitles = evidence.slice(0, 3).map((item) => item.title ?? item.evidence_type);
  const evidenceSummary = evidenceTitles.length > 0 ? `Key evidence: ${evidenceTitles.join(", ")}` : null;

  let recurrenceSummary: string | null = null;
  if (inputs.recurrence.isReopened) {
    recurrenceSummary = `Case reopened from a prior related incident and is now on repeat count ${inputs.recurrence.repeatCount}.`;
  } else if (inputs.recurrence.isRecurring) {
    recurrenceSummary = `Case is part of a recurrence group with repeat count ${inputs.recurrence.repeatCount}.`;
  }

  let closureSummary = investigation.latest_closure_note?.note ?? null;
  if (!closureSummary && (status === "resolved" || status === "closed")) {
    closureSummary = String(inputs.lifecycle?.last_resolution_note ?? inputs.lifecycle?.last_resolution_action ?? "Case is resolved, but no explicit closure note was recorded.");
  }

  let recommendedNextAction: string | null;
  if (status === "resolved" || status === "closed") {
    recommendedNextAction = "Monitor for recurrence and review related prior cases before fully closing the loop.";
  } else if (rootCauseCode === "version_regression") {
    recommendedNextAction = "Compare the current version tuple against the last stable baseline and inspect replay drift.";
  } else if (rootCauseCode === "regime_conflict") {
    recommendedNextAction = "Review regime transitions and threshold applications before escalating the case.";
  } else if (rootCauseCode === "replay_inconsistency") {
    recommendedNextAction = "Inspect replay delta evidence and confirm whether the source and replay inputs diverged.";
  } else if (rootCauseCode === "watchlist_scope_drift") {
    recommendedNextAction = "Verify the compute scope hash and dependency asset set against the last healthy run.";
  } else {
    recommendedNextAction = "Continue investigation, attach more supporting evidence, and leave a clear handoff note if ownership changes.";
  }

  return {
    summary_version: "v1",
    status_summary: inputs.lifecycle?.acknowledged_by
      ? `Case is ${status} with severity ${severity}, owned by ${owner}, and has already been acknowledged.`
      : `Case is ${status} with severity ${severity} and is currently owned by ${owner}.`,
    root_cause_code: rootCauseCode,
    root_cause_confidence: rootCauseCode && rootCauseCode !== "mixed_or_unresolved" ? 0.85 : rootCauseCode ? 0.55 : null,
    root_cause_summary: rootCauseSummary,
    evidence_summary: evidenceSummary,
    recurrence_summary: recurrenceSummary,
    operator_summary: investigation.last_operator_summary?.note ?? null,
    closure_summary: closureSummary,
    recommended_next_action: recommendedNextAction,
    source_note_ids: inputs.notes.map((note) => note.id),
    source_evidence_ids: evidence.map((item) => item.id),
    metadata: {
      note_count: inputs.notes.length,
      evidence_count: evidence.length,
      status,
      severity,
    },
  };
}

async function getCaseLifecycle(caseRow: Record<string, unknown>): Promise<Record<string, unknown> | null> {
  if (!caseRow.degradation_state_id) return null;
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("governance_lifecycle_summary")
    .select("*")
    .eq("degradation_state_id", String(caseRow.degradation_state_id))
    .maybeSingle();
  if (error) throw error;
  return (data as Record<string, unknown> | null) ?? null;
}

export async function getGovernanceCaseNotes(caseId: string): Promise<GovernanceCaseNote[]> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("governance_case_notes")
    .select("*")
    .eq("case_id", caseId)
    .order("created_at", { ascending: false })
    .order("id", { ascending: false });
  if (error) throw error;
  return (data ?? []) as GovernanceCaseNote[];
}

export async function getGovernanceCaseEvidenceSummary(caseId: string): Promise<GovernanceCaseEvidenceSummary | null> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("governance_case_evidence_summary")
    .select("*")
    .eq("case_id", caseId)
    .maybeSingle();
  if (error) throw error;
  if (!data) return null;

  return {
    ...(data as unknown as Omit<GovernanceCaseEvidenceSummary, "evidence_items">),
    evidence_items: Array.isArray(data.evidence_items)
      ? (data.evidence_items as unknown as GovernanceCaseEvidence[])
      : [],
  };
}

export async function getGovernanceCaseEvidence(caseId: string): Promise<GovernanceCaseEvidence[]> {
  const summary = await getGovernanceCaseEvidenceSummary(caseId);
  return summary?.evidence_items ?? [];
}

export async function getGovernanceCaseGeneratedSummary(caseId: string): Promise<GovernanceCaseGeneratedSummary | null> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("governance_case_summary_latest")
    .select("*")
    .eq("case_id", caseId)
    .maybeSingle();
  if (error) throw error;
  return (data as GovernanceCaseGeneratedSummary | null) ?? null;
}

export async function refreshGovernanceCaseGeneratedSummary(caseId: string): Promise<GovernanceCaseGeneratedSummary | null> {
  const supabase = createServiceSupabaseClient();
  const { data: caseRow, error: caseError } = await supabase
    .from("governance_case_summary")
    .select("*")
    .eq("id", caseId)
    .maybeSingle();
  if (caseError) throw caseError;
  if (!caseRow) return null;

  const [notes, evidenceSummary, recurrenceSummary, lifecycle] = await Promise.all([
    getGovernanceCaseNotes(caseId),
    getGovernanceCaseEvidenceSummary(caseId),
    supabase.from("governance_case_recurrence_summary").select("*").eq("case_id", caseId).maybeSingle(),
    getCaseLifecycle(caseRow as Record<string, unknown>),
  ]);

  if (recurrenceSummary.error) throw recurrenceSummary.error;

  let relatedCases: GovernanceRelatedCase[] = [];
  const recurrenceGroupId =
    (caseRow.recurrence_group_id as string | null | undefined) ??
    (recurrenceSummary.data?.recurrence_group_id as string | null | undefined) ??
    null;

  if (recurrenceGroupId) {
    const { data: related, error: relatedError } = await supabase
      .from("governance_case_summary")
      .select("id,status,severity,title,closed_at,resolved_at,opened_at,repeat_count,is_reopened")
      .eq("recurrence_group_id", recurrenceGroupId)
      .neq("id", caseId)
      .order("opened_at", { ascending: false })
      .limit(12);
    if (relatedError) throw relatedError;
    relatedCases = (related ?? []) as GovernanceRelatedCase[];
  }

  const recurrence: GovernanceCaseRecurrence = {
    recurrenceGroupId,
    reopenedFromCaseId: (caseRow.reopened_from_case_id as string | null | undefined) ?? null,
    repeatCount: Number(caseRow.repeat_count ?? recurrenceSummary.data?.repeat_count ?? 1),
    isReopened: Boolean(caseRow.is_reopened ?? recurrenceSummary.data?.is_reopened ?? false),
    isRecurring: Boolean(caseRow.is_recurring ?? recurrenceSummary.data?.is_recurring ?? false),
    reopenReason: (caseRow.reopen_reason as string | null | undefined) ?? null,
    matchBasis: (caseRow.recurrence_match_basis as Record<string, unknown> | null | undefined) ?? {},
    priorRelatedCaseCount: Number(caseRow.prior_related_case_count ?? recurrenceSummary.data?.prior_related_case_count ?? 0),
    latestPriorCaseId: (caseRow.latest_prior_case_id as string | null | undefined) ?? null,
    latestPriorClosedAt: (caseRow.latest_prior_closed_at as string | null | undefined) ?? null,
    latestPriorStatus: (caseRow.latest_prior_status as string | null | undefined) ?? null,
    relatedCases,
  };

  const generated = buildGeneratedSummary({
    caseRow: caseRow as Record<string, unknown>,
    notes,
    evidenceSummary,
    recurrence,
    lifecycle,
  });

  const { data: upserted, error: upsertError } = await supabase
    .from("governance_case_summaries")
    .upsert({
      workspace_id: String(caseRow.workspace_id),
      case_id: caseId,
      ...generated,
      generated_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    }, { onConflict: "case_id,summary_version" })
    .select("*")
    .single();
  if (upsertError) throw upsertError;
  return upserted as GovernanceCaseGeneratedSummary;
}

export async function getGovernanceCaseDetail(caseId: string): Promise<GovernanceIncidentDetail | null> {
  const supabase = createServiceSupabaseClient();

  const detailPromise = supabase.from("governance_incident_detail").select("*").eq("case_id", caseId).maybeSingle();
  const caseSummaryPromise = supabase.from("governance_case_summary").select("*").eq("id", caseId).maybeSingle();
  const timelinePromise = supabase
    .from("governance_incident_timeline_events")
    .select("*")
    .eq("case_id", caseId)
    .order("event_at", { ascending: true })
    .order("id", { ascending: true });
  const recurrenceSummaryPromise = supabase.from("governance_case_recurrence_summary").select("*").eq("case_id", caseId).maybeSingle();
  const notesPromise = getGovernanceCaseNotes(caseId);
  const evidenceSummaryPromise = getGovernanceCaseEvidenceSummary(caseId);
  const generatedSummaryPromise = getGovernanceCaseGeneratedSummary(caseId);

  const [
    { data: detail, error: detailError },
    { data: caseSummary, error: caseSummaryError },
    { data: timeline, error: timelineError },
    { data: recurrenceSummary, error: recurrenceError },
    notes,
    evidenceSummary,
    generatedSummary,
  ] = await Promise.all([
    detailPromise,
    caseSummaryPromise,
    timelinePromise,
    recurrenceSummaryPromise,
    notesPromise,
    evidenceSummaryPromise,
    generatedSummaryPromise,
  ]);

  if (detailError) throw detailError;
  if (!detail) return null;
  if (caseSummaryError) throw caseSummaryError;
  if (timelineError) throw timelineError;
  if (recurrenceError) throw recurrenceError;

  let relatedCases: GovernanceRelatedCase[] = [];
  const recurrenceGroupId =
    (caseSummary?.recurrence_group_id as string | null | undefined) ??
    (recurrenceSummary?.recurrence_group_id as string | null | undefined) ??
    null;

  if (recurrenceGroupId) {
    const { data: related, error: relatedError } = await supabase
      .from("governance_case_summary")
      .select("id,status,severity,title,closed_at,resolved_at,opened_at,repeat_count,is_reopened")
      .eq("recurrence_group_id", recurrenceGroupId)
      .neq("id", caseId)
      .order("opened_at", { ascending: false })
      .limit(12);
    if (relatedError) throw relatedError;
    relatedCases = (related ?? []) as GovernanceRelatedCase[];
  }

  const recurrence: GovernanceCaseRecurrence = {
    recurrenceGroupId,
    reopenedFromCaseId: (caseSummary?.reopened_from_case_id as string | null | undefined) ?? null,
    repeatCount: Number(caseSummary?.repeat_count ?? recurrenceSummary?.repeat_count ?? 1),
    isReopened: Boolean(caseSummary?.is_reopened ?? recurrenceSummary?.is_reopened ?? false),
    isRecurring: Boolean(caseSummary?.is_recurring ?? recurrenceSummary?.is_recurring ?? false),
    reopenReason: (caseSummary?.reopen_reason as string | null | undefined) ?? null,
    matchBasis: (caseSummary?.recurrence_match_basis as Record<string, unknown> | null | undefined) ?? {},
    priorRelatedCaseCount: Number(caseSummary?.prior_related_case_count ?? recurrenceSummary?.prior_related_case_count ?? 0),
    latestPriorCaseId: (caseSummary?.latest_prior_case_id as string | null | undefined) ?? null,
    latestPriorClosedAt: (caseSummary?.latest_prior_closed_at as string | null | undefined) ?? null,
    latestPriorStatus: (caseSummary?.latest_prior_status as string | null | undefined) ?? null,
    relatedCases,
  };

  return {
    ...(detail as unknown as Omit<GovernanceIncidentDetail, "recurrence" | "timeline" | "notes" | "evidence" | "evidence_summary" | "investigation_summary" | "generated_summary">),
    recurrence,
    timeline: (timeline ?? []) as GovernanceIncidentDetail["timeline"],
    notes,
    evidence: evidenceSummary?.evidence_items ?? [],
    evidence_summary: evidenceSummary,
    investigation_summary: buildInvestigationSummary(notes),
    generated_summary: generatedSummary,
  };
}
