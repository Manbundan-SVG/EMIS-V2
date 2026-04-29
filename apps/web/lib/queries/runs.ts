import { createServiceSupabaseClient } from "@/lib/supabase";

export interface RunInspectionRow {
  run_id: string;
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  queue_id: number | null;
  queue_name: string;
  status: string;
  trigger_type: string;
  requested_by: string | null;
  attempt_count: number;
  max_attempts: number;
  claimed_by: string | null;
  claimed_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  runtime_ms: number | null;
  compute_version: string | null;
  signal_registry_version: string | null;
  model_version: string | null;
  lineage: Record<string, unknown>;
  metadata: Record<string, unknown>;
  priority: number | null;
  retry_count: number | null;
  last_error: string | null;
  queued_at: string | null;
  terminal_queue_status: string | null;
  terminal_promoted_at: string | null;
  failure_stage: string | null;
  failure_code: string | null;
  is_replay: boolean;
  replayed_from_run_id: string | null;
  input_snapshot_id: number | null;
  explanation_version: string | null;
  alert_count: number;
  last_alert_at: string | null;
}

export interface RunStageTimingRow {
  id: number;
  job_run_id: string;
  workspace_id: string;
  watchlist_id: string | null;
  stage_name: string;
  stage_status: "completed" | "failed" | "skipped";
  started_at: string;
  completed_at: string | null;
  runtime_ms: number | null;
  error_summary: string | null;
  failure_code: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface RunExplanationRow {
  id: number;
  job_run_id: string;
  workspace_id: string;
  watchlist_id: string | null;
  explanation_version: string;
  summary: string | null;
  regime_summary: Record<string, unknown>;
  signal_summary: Record<string, unknown>;
  composite_summary: Record<string, unknown>;
  invalidator_summary: Record<string, unknown>;
  top_positive_contributors: unknown[];
  top_negative_contributors: unknown[];
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface RunInputSnapshotRow {
  id: number;
  job_run_id: string;
  workspace_id: string;
  watchlist_id: string | null;
  source_window_start: string | null;
  source_window_end: string | null;
  asset_count: number;
  source_coverage: Record<string, unknown>;
  input_values: Record<string, unknown>;
  version_pins: Record<string, unknown>;
  metadata: Record<string, unknown>;
  compute_scope_id?: string | null;
  scope_hash?: string | null;
  scope_version?: string | null;
  primary_asset_count?: number | null;
  dependency_asset_count?: number | null;
  asset_universe_count?: number | null;
  created_at: string;
  updated_at: string;
}

export interface RunScopeInspectionRow {
  run_id: string;
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  queue_id: number | null;
  queue_name: string;
  status: string;
  is_replay: boolean;
  replayed_from_run_id: string | null;
  compute_scope_id: string | null;
  scope_version: string | null;
  scope_hash: string | null;
  primary_assets: string[];
  dependency_assets: string[];
  asset_universe: string[];
  primary_asset_count: number;
  dependency_asset_count: number;
  asset_universe_count: number;
  dependency_policy: Record<string, unknown>;
  metadata: Record<string, unknown>;
  scope_created_at: string | null;
}

export interface RunPriorComparisonRow {
  run_id: string;
  prior_run_id: string | null;
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  queue_name: string;
  current_summary: string | null;
  prior_summary: string | null;
  regime_changes: Record<string, unknown>;
  signal_changes: Record<string, unknown>;
  composite_changes: Record<string, unknown>;
  invalidator_changes: Record<string, unknown>;
  input_coverage_changes: Record<string, unknown>;
}

export interface ReplayRunResult {
  allowed: boolean;
  reason: string;
  assigned_priority: number;
  job_id: string | null;
  workspace_id: string;
  queue_id: number | null;
  watchlist_id: string | null;
  replayed_from_run_id: string;
}

export interface SignalAttributionRow {
  asset_id: string | null;
  asset_symbol: string | null;
  regime: string | null;
  signal_name: string;
  signal_family: string;
  raw_value: number | null;
  normalized_value: number | null;
  weight_applied: number;
  contribution_value: number;
  contribution_direction: "positive" | "negative" | "neutral";
  is_invalidator: boolean;
  active_invalidators: string[];
  metadata: Record<string, unknown>;
}

export interface SignalFamilyAttributionRow {
  signal_family: string;
  family_rank: number;
  family_weight: number;
  family_score: number;
  family_pct_of_total: number;
  positive_contribution: number;
  negative_contribution: number;
  invalidator_contribution: number;
  active_invalidators: string[];
  metadata: Record<string, unknown>;
}

export interface RunAttributionRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string | null;
  status: string;
  compute_version: string | null;
  signal_registry_version: string | null;
  model_version: string | null;
  attribution_version: string | null;
  attribution_reconciled: boolean;
  attribution_total: number | null;
  attribution_target_total: number | null;
  attribution_reconciliation_delta: number | null;
  family_attributions: SignalFamilyAttributionRow[];
  signal_attributions: SignalAttributionRow[];
}

export interface RunDriftMetricRow {
  id: number;
  run_id: string;
  comparison_run_id: string | null;
  workspace_id: string;
  watchlist_id: string | null;
  metric_type: "composite" | "regime" | "family" | "signal" | string;
  entity_name: string;
  current_value: number | null;
  baseline_value: number | null;
  delta_abs: number | null;
  delta_pct: number | null;
  z_score: number | null;
  drift_flag: boolean;
  severity: "low" | "medium" | "high" | string;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface RunDriftSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string | null;
  comparison_run_id: string | null;
  drift_severity: string | null;
  drift_summary: Record<string, unknown>;
  current_compute_version: string | null;
  comparison_compute_version: string | null;
  current_signal_registry_version: string | null;
  comparison_signal_registry_version: string | null;
  current_model_version: string | null;
  comparison_model_version: string | null;
  metric_count: number;
  flagged_metric_count: number;
  computed_at: string | null;
}

export interface RunDriftRow {
  summary: RunDriftSummaryRow | null;
  metrics: RunDriftMetricRow[];
}

export interface ReplayDeltaDiffRow {
  signal_key?: string;
  signal_family?: string;
  source_value: number;
  replay_value: number;
  delta: number;
  delta_abs: number;
}

export interface ReplayDeltaRow {
  replay_run_id: string;
  source_run_id: string;
  workspace_id: string;
  watchlist_id: string | null;
  input_match_score: number;
  input_match_details: Record<string, unknown>;
  version_match: boolean;
  compute_version_changed: boolean;
  signal_registry_version_changed: boolean;
  model_version_changed: boolean;
  regime_changed: boolean;
  source_regime: string | null;
  replay_regime: string | null;
  source_composite: number | null;
  replay_composite: number | null;
  composite_delta: number | null;
  composite_delta_abs: number | null;
  largest_signal_deltas: ReplayDeltaDiffRow[];
  largest_family_deltas: ReplayDeltaDiffRow[];
  summary: Record<string, unknown>;
  severity: "low" | "medium" | "high" | string;
  created_at: string;
}

export interface VersionBehaviorComparisonRow {
  workspace_id: string;
  watchlist_id: string | null;
  queue_name: string;
  compute_version: string | null;
  signal_registry_version: string | null;
  model_version: string | null;
  run_count: number;
  replay_run_count: number;
  avg_composite_score: number | null;
  avg_flagged_drift_metrics: number | null;
  avg_replay_input_match_score: number | null;
  avg_replay_composite_delta_abs: number | null;
  high_severity_replay_count: number;
  latest_completed_at: string | null;
}

export interface RegimeTransitionFamilyShiftRow {
  id: string;
  transition_event_id: string;
  run_id: string;
  prior_run_id: string | null;
  workspace_id: string;
  watchlist_id: string | null;
  signal_family: string;
  prior_family_score: number | null;
  current_family_score: number | null;
  family_delta: number | null;
  family_delta_abs: number | null;
  prior_family_rank: number | null;
  current_family_rank: number | null;
  shift_direction: string;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface RegimeTransitionSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string | null;
  queue_name: string;
  status: string;
  is_replay: boolean;
  replayed_from_run_id: string | null;
  started_at: string | null;
  completed_at: string | null;
  prior_run_id: string | null;
  from_regime: string | null;
  to_regime: string | null;
  transition_detected: boolean;
  transition_classification: string;
  stability_score: number | null;
  anomaly_likelihood: number | null;
  composite_shift: number | null;
  composite_shift_abs: number | null;
  dominant_family_gained: string | null;
  dominant_family_lost: string | null;
  metadata: Record<string, unknown>;
  created_at: string | null;
  updated_at: string | null;
}

export interface RegimeTransitionRow {
  summary: RegimeTransitionSummaryRow | null;
  familyShifts: RegimeTransitionFamilyShiftRow[];
}

export async function getRunInspection(runId: string): Promise<RunInspectionRow | null> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("run_inspection")
    .select("*")
    .eq("run_id", runId)
    .maybeSingle();

  if (error) throw new Error(`Run inspection error: ${error.message}`);
  return (data ?? null) as RunInspectionRow | null;
}

export async function getRunStageTimings(runId: string): Promise<RunStageTimingRow[]> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("job_run_stage_timings")
    .select("*")
    .eq("job_run_id", runId)
    .order("started_at", { ascending: true });

  if (error) throw new Error(`Run stage timing error: ${error.message}`);
  return (data ?? []) as RunStageTimingRow[];
}

export async function getRunExplanation(runId: string): Promise<RunExplanationRow | null> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("job_run_explanations")
    .select("*")
    .eq("job_run_id", runId)
    .maybeSingle();

  if (error) throw new Error(`Run explanation error: ${error.message}`);
  return (data ?? null) as RunExplanationRow | null;
}

export async function getRunInputSnapshot(runId: string): Promise<RunInputSnapshotRow | null> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("job_run_input_snapshots")
    .select("*")
    .eq("job_run_id", runId)
    .maybeSingle();

  if (error) throw new Error(`Run input snapshot error: ${error.message}`);
  return (data ?? null) as RunInputSnapshotRow | null;
}

export async function getRunScopeInspection(runId: string): Promise<RunScopeInspectionRow | null> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("run_scope_inspection")
    .select("*")
    .eq("run_id", runId)
    .maybeSingle();

  if (error) throw new Error(`Run scope inspection error: ${error.message}`);
  return (data ?? null) as RunScopeInspectionRow | null;
}

export async function getRunPriorComparison(runId: string): Promise<RunPriorComparisonRow | null> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("job_run_prior_comparison")
    .select("*")
    .eq("run_id", runId)
    .maybeSingle();

  if (error) throw new Error(`Run prior comparison error: ${error.message}`);
  return (data ?? null) as RunPriorComparisonRow | null;
}

export async function getRunAttribution(runId: string): Promise<RunAttributionRow | null> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("run_attribution_summary")
    .select("*")
    .eq("run_id", runId)
    .maybeSingle();

  if (error) throw new Error(`Run attribution error: ${error.message}`);
  return (data ?? null) as RunAttributionRow | null;
}

export async function getRunDrift(runId: string): Promise<RunDriftRow> {
  const supabase = createServiceSupabaseClient();
  const [{ data: summary, error: summaryError }, { data: metrics, error: metricsError }] = await Promise.all([
    supabase
      .from("job_run_drift_summary")
      .select("*")
      .eq("run_id", runId)
      .maybeSingle(),
    supabase
      .from("job_run_drift_metrics")
      .select("*")
      .eq("run_id", runId)
      .order("drift_flag", { ascending: false })
      .order("severity", { ascending: true })
      .order("metric_type", { ascending: true }),
  ]);

  if (summaryError) throw new Error(`Run drift summary error: ${summaryError.message}`);
  if (metricsError) throw new Error(`Run drift metric error: ${metricsError.message}`);
  return {
    summary: (summary ?? null) as RunDriftSummaryRow | null,
    metrics: (metrics ?? []) as RunDriftMetricRow[],
  };
}

export async function getReplayDelta(runId: string): Promise<ReplayDeltaRow | null> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("job_run_replay_deltas")
    .select("*")
    .eq("replay_run_id", runId)
    .maybeSingle();

  if (error) throw new Error(`Replay delta error: ${error.message}`);
  return (data ?? null) as ReplayDeltaRow | null;
}

export async function getRegimeTransition(runId: string): Promise<RegimeTransitionRow> {
  const supabase = createServiceSupabaseClient();
  const [{ data: summary, error: summaryError }, { data: familyShifts, error: familyError }] = await Promise.all([
    supabase
      .from("run_regime_stability_summary")
      .select("*")
      .eq("run_id", runId)
      .maybeSingle(),
    supabase
      .from("regime_transition_family_shifts")
      .select("*")
      .eq("run_id", runId)
      .order("family_delta_abs", { ascending: false }),
  ]);

  if (summaryError) throw new Error(`Regime transition summary error: ${summaryError.message}`);
  if (familyError) throw new Error(`Regime transition family shift error: ${familyError.message}`);
  return {
    summary: (summary ?? null) as RegimeTransitionSummaryRow | null,
    familyShifts: (familyShifts ?? []) as RegimeTransitionFamilyShiftRow[],
  };
}

export async function getVersionBehaviorComparisons(workspaceId: string): Promise<VersionBehaviorComparisonRow[]> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("job_run_version_behavior_comparison")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("latest_completed_at", { ascending: false });

  if (error) throw new Error(`Version behavior comparison error: ${error.message}`);
  return (data ?? []) as VersionBehaviorComparisonRow[];
}

export async function enqueueReplayRun(runId: string, requestedBy?: string | null): Promise<ReplayRunResult> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase.rpc("enqueue_replay_run", {
    p_source_run_id: runId,
    p_requested_by: requestedBy ?? null,
  });

  if (error) throw new Error(`Replay enqueue error: ${error.message}`);
  const row = (data ?? [])[0];
  if (!row) throw new Error("Replay enqueue returned no rows");
  return row as ReplayRunResult;
}
