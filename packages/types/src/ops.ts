export interface QueueGovernanceDecision {
  allowed: boolean;
  reason: string;
  assigned_priority: number;
  dedupe_window_seconds: number;
  max_concurrent: number;
}

export interface QueueGovernanceRule {
  id: number;
  workspace_id: string;
  watchlist_id: string | null;
  watchlist_slug?: string | null;
  watchlist_name?: string | null;
  job_type: string;
  enabled: boolean;
  max_concurrent: number;
  dedupe_window_seconds: number;
  suppress_if_queued: boolean;
  suppress_if_claimed: boolean;
  manual_priority: number;
  scheduled_priority: number;
  created_at: string;
  updated_at: string;
}

export interface AlertPolicyRule {
  id: number;
  workspace_id: string;
  watchlist_id: string | null;
  watchlist_slug?: string | null;
  watchlist_name?: string | null;
  enabled: boolean;
  event_type: string;
  severity: string;
  channel: string;
  notify_on_terminal_only: boolean;
  cooldown_seconds: number;
  dedupe_key_template: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface QueueGovernanceState {
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  job_type: string;
  queued_count: number;
  claimed_count: number;
  oldest_queued_at: string | null;
  highest_priority_queued: number | null;
}

export interface WatchlistSlaSummary {
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  completed_24h: number;
  failed_24h: number;
  last_success_at: string | null;
  seconds_since_last_success: number | null;
  avg_runtime_ms_24h: number | null;
}

export interface RunInspection {
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

export interface JobRunStageTiming {
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

export interface JobRunExplanation {
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

export interface JobRunInputSnapshot {
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
  created_at: string;
  updated_at: string;
}

export interface JobRunPriorComparison {
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

export interface SignalAttribution {
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

export interface SignalFamilyAttribution {
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

export interface RunAttribution {
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
  family_attributions: SignalFamilyAttribution[];
  signal_attributions: SignalAttribution[];
}

export interface RunDriftMetric {
  id: number;
  run_id: string;
  comparison_run_id: string | null;
  workspace_id: string;
  watchlist_id: string | null;
  metric_type: string;
  entity_name: string;
  current_value: number | null;
  baseline_value: number | null;
  delta_abs: number | null;
  delta_pct: number | null;
  z_score: number | null;
  drift_flag: boolean;
  severity: string;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface RunDriftSummary {
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

export interface RunDriftResponse {
  summary: RunDriftSummary | null;
  metrics: RunDriftMetric[];
}

export interface ReplayDeltaDiff {
  signal_key?: string;
  signal_family?: string;
  source_value: number;
  replay_value: number;
  delta: number;
  delta_abs: number;
}

export interface ReplayDeltaRecord {
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
  largest_signal_deltas: ReplayDeltaDiff[];
  largest_family_deltas: ReplayDeltaDiff[];
  summary: Record<string, unknown>;
  severity: "low" | "medium" | "high" | string;
  created_at: string;
}

export interface VersionBehaviorComparison {
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

export interface RegimeTransitionFamilyShift {
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

export interface RegimeTransitionSummary {
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

export interface RegimeTransitionResponse {
  summary: RegimeTransitionSummary | null;
  familyShifts: RegimeTransitionFamilyShift[];
}

export interface StabilityFamilyMetric {
  signal_family: string;
  family_score_current: number | null;
  family_score_baseline: number | null;
  family_delta_abs: number | null;
  family_delta_pct: number | null;
  instability_score: number;
  family_rank: number;
  metadata: Record<string, unknown>;
}

export interface StabilitySummary {
  run_id: string;
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  queue_name: string;
  window_size: number;
  baseline_run_count: number;
  composite_current: number | null;
  composite_baseline: number | null;
  composite_delta_abs: number | null;
  composite_delta_pct: number | null;
  composite_instability_score: number;
  family_instability_score: number;
  replay_consistency_risk_score: number;
  regime_instability_score: number;
  dominant_family: string | null;
  dominant_family_changed: boolean;
  dominant_regime: string | null;
  regime_changed: boolean;
  stability_classification: "stable" | "watch" | "unstable" | "critical" | string;
  replay_runs_considered: number | null;
  mismatch_rate: number | null;
  avg_input_match_score: number | null;
  avg_composite_delta_abs: number | null;
  transitions_considered: number | null;
  conflicting_transition_count: number | null;
  abrupt_transition_count: number | null;
  family_rows: StabilityFamilyMetric[];
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface VersionGovernance {
  workspace_id: string;
  workspace_slug: string;
  compute_version: string | null;
  signal_registry_version: string | null;
  model_version: string | null;
  run_count: number;
  avg_runtime_ms: number | null;
  completion_rate: number;
  failure_rate: number;
  avg_family_instability: number;
  avg_regime_instability: number;
  avg_replay_consistency_risk: number;
  replay_count: number;
  avg_input_match_score: number;
  avg_replay_composite_delta_abs: number;
  elevated_replay_rate: number;
  transition_count: number;
  conflicting_transition_rate: number;
  avg_transition_stability_score: number;
  avg_transition_anomaly_likelihood: number;
  governance_health_score: number;
  health_rank: number;
  last_completed_at: string | null;
  last_replay_completed_at: string | null;
}

export interface RegimeThresholdProfile {
  id: string;
  workspace_id: string | null;
  profile_name: string;
  is_default: boolean;
  enabled: boolean;
  version_health_floor: number;
  family_instability_ceiling: number;
  replay_consistency_floor: number;
  regime_instability_ceiling: number;
  conflicting_transition_ceiling: number;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface RegimeThresholdOverride {
  id: string;
  workspace_id: string | null;
  regime: string;
  profile_id: string;
  enabled: boolean;
  version_health_floor: number | null;
  family_instability_ceiling: number | null;
  replay_consistency_floor: number | null;
  regime_instability_ceiling: number | null;
  conflicting_transition_ceiling: number | null;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface ActiveRegimeThreshold {
  workspace_id: string | null;
  workspace_slug: string | null;
  profile_id: string;
  override_id: string | null;
  profile_name: string;
  regime: string;
  version_health_floor: number;
  family_instability_ceiling: number;
  replay_consistency_floor: number;
  regime_instability_ceiling: number;
  conflicting_transition_ceiling: number;
  profile_metadata: Record<string, unknown>;
  override_metadata: Record<string, unknown>;
}

export interface GovernanceThresholdApplication {
  id: string;
  run_id: string | null;
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  regime: string;
  profile_id: string | null;
  profile_name: string | null;
  override_id: string | null;
  evaluation_stage: string;
  applied_thresholds: Record<string, unknown>;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface MacroSyncHealth {
  workspace_id: string;
  workspace_slug: string;
  provider_mode: string;
  last_completed_at: string | null;
  completed_runs: number;
  failed_runs: number;
  last_error: string | null;
}

export interface RegimeThresholdMetricsResponse {
  profiles: RegimeThresholdProfile[];
  overrides: RegimeThresholdOverride[];
  active: ActiveRegimeThreshold[];
  applications: GovernanceThresholdApplication[];
  macroSyncHealth: MacroSyncHealth[];
}

export interface GovernanceAlertEvent {
  id: string;
  workspace_id: string;
  watchlist_id: string | null;
  run_id: string | null;
  rule_name: string;
  event_type:
    | "version_regression"
    | "replay_degradation"
    | "family_instability_spike"
    | "regime_instability_spike"
    | "regime_conflict_persistence"
    | "stability_classification_downgrade"
    | string;
  severity: "info" | "medium" | "high" | "critical" | string;
  dedupe_key: string;
  metric_source: string;
  metric_name: string;
  metric_value_numeric: number | null;
  metric_value_text: string | null;
  threshold_numeric: number | null;
  threshold_text: string | null;
  compute_version: string | null;
  signal_registry_version: string | null;
  model_version: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface GovernanceAlertStateRow {
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  rule_name: string;
  event_type: GovernanceAlertEvent["event_type"];
  severity: GovernanceAlertEvent["severity"];
  compute_version: string | null;
  signal_registry_version: string | null;
  model_version: string | null;
  latest_triggered_at: string;
  trigger_count: number;
}

export interface GovernanceAnomalyCluster {
  id: string;
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  version_tuple: string;
  cluster_key: string;
  alert_type: string;
  regime: string | null;
  severity: "low" | "medium" | "high" | string;
  status: "open" | "resolved" | "suppressed" | string;
  first_seen_at: string;
  last_seen_at: string;
  event_count: number;
  latest_event_id: string | null;
  latest_run_id: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface WatchlistAnomalySummary {
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  open_cluster_count: number;
  total_cluster_count: number;
  high_open_cluster_count: number;
  open_event_count: number;
  last_seen_at: string | null;
}

export interface GovernanceDegradationState {
  id: string;
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  degradation_type: string;
  version_tuple: string;
  regime: string | null;
  state_status: "active" | "escalated" | "resolved" | string;
  severity: "low" | "medium" | "high" | "critical" | string;
  first_seen_at: string;
  last_seen_at: string;
  escalated_at: string | null;
  resolved_at: string | null;
  event_count: number;
  cluster_count: number;
  member_count: number;
  source_summary: Record<string, unknown>;
  resolution_summary: Record<string, unknown> | null;
  metadata: Record<string, unknown>;
  state_duration_hours: number;
}

export interface GovernanceRecoveryEvent {
  id: string;
  workspace_id: string;
  workspace_slug: string;
  state_id: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  degradation_type: string;
  version_tuple: string;
  regime: string | null;
  recovered_at: string;
  recovery_reason: string;
  prior_severity: string;
  trailing_metrics: Record<string, unknown>;
  metadata: Record<string, unknown>;
  state_first_seen_at: string | null;
  state_last_seen_at: string | null;
  state_event_count: number | null;
  state_cluster_count: number | null;
}

export interface GovernanceDegradationMetricsResponse {
  activeStates: GovernanceDegradationState[];
  resolvedStates: GovernanceDegradationState[];
  recoveries: GovernanceRecoveryEvent[];
  openStateCount: number;
  escalatedStateCount: number;
  recentRecoveryCount: number;
}

export interface GovernanceLifecycleState {
  degradation_state_id: string;
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  degradation_type: string;
  version_tuple: string;
  regime: string | null;
  state_status: "active" | "escalated" | "resolved" | string;
  severity: "low" | "medium" | "high" | "critical" | string;
  first_seen_at: string;
  last_seen_at: string;
  escalated_at: string | null;
  resolved_at: string | null;
  event_count: number;
  cluster_count: number;
  source_summary: Record<string, unknown>;
  resolution_summary: Record<string, unknown> | null;
  metadata: Record<string, unknown>;
  acknowledgment_id: string | null;
  acknowledged_at: string | null;
  acknowledged_by: string | null;
  acknowledgment_note: string | null;
  acknowledgment_metadata: Record<string, unknown> | null;
  muting_rule_id: string | null;
  mute_target_type: string | null;
  mute_target_key: string | null;
  mute_reason: string | null;
  muted_until: string | null;
  muted_by: string | null;
  mute_is_active: boolean | null;
  mute_metadata: Record<string, unknown> | null;
  resolution_action_id: string | null;
  last_resolution_action: string | null;
  last_resolution_actor: string | null;
  last_resolution_note: string | null;
  last_resolution_metadata: Record<string, unknown> | null;
  last_resolution_at: string | null;
}

export interface GovernanceLifecycleMetricsResponse {
  activeStates: GovernanceLifecycleState[];
  acknowledgedStates: GovernanceLifecycleState[];
  resolvedStates: GovernanceLifecycleState[];
  recoveries: GovernanceRecoveryEvent[];
}

export interface GovernanceCaseSummary {
  id: string;
  workspace_id: string;
  workspace_slug: string;
  degradation_state_id: string | null;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  version_tuple: string | null;
  status: "open" | "acknowledged" | "in_progress" | "resolved" | "closed" | string;
  severity: "low" | "medium" | "high" | "critical" | string;
  title: string;
  summary: string | null;
  opened_at: string;
  acknowledged_at: string | null;
  resolved_at: string | null;
  closed_at: string | null;
  reopened_count: number;
  current_assignee: string | null;
  current_team: string | null;
  metadata: Record<string, unknown>;
  note_count: number;
  evidence_count: number;
  event_count: number;
  last_event_type: string | null;
  last_event_at: string | null;
  recurrence_group_id: string | null;
  reopened_from_case_id: string | null;
  repeat_count: number;
  reopened_at: string | null;
  reopen_reason: string | null;
  recurrence_match_basis: Record<string, unknown>;
  prior_related_case_count: number;
  latest_prior_case_id: string | null;
  latest_prior_closed_at: string | null;
  latest_prior_status: string | null;
  is_reopened: boolean;
  is_recurring: boolean;
}

export interface GovernanceCaseMetricsResponse {
  activeCases: GovernanceCaseSummary[];
  recentCases: GovernanceCaseSummary[];
}

export interface GovernanceRoutingDecision {
  id: string;
  workspace_id: string;
  workspace_slug: string;
  case_id: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  case_title: string;
  case_status: string;
  severity: string;
  version_tuple: string | null;
  root_cause_code: string | null;
  routing_rule_id: string | null;
  override_id: string | null;
  assigned_team: string | null;
  assigned_user: string | null;
  routing_reason: string;
  workload_snapshot: Record<string, unknown>;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface GovernanceOperatorCaseMetric {
  workspace_id: string;
  operator_id: string;
  assigned_team: string | null;
  open_case_count: number;
  severe_open_case_count: number;
  avg_open_age_hours: number | null;
  reopened_open_case_count: number;
  stale_open_case_count: number;
}

export interface GovernanceTeamCaseMetric {
  workspace_id: string;
  assigned_team: string;
  open_case_count: number;
  severe_open_case_count: number;
  avg_open_age_hours: number | null;
  reopened_open_case_count: number;
  stale_open_case_count: number;
}

export interface GovernanceRoutingSummary {
  routingDecisions: GovernanceRoutingDecision[];
  operatorMetrics: GovernanceOperatorCaseMetric[];
  teamMetrics: GovernanceTeamCaseMetric[];
}

export interface GovernanceRoutingQuality {
  workspace_id: string;
  workspace_slug: string;
  root_cause_code: string;
  assigned_team: string;
  feedback_count: number;
  accepted_count: number;
  rerouted_count: number;
  acceptance_rate: number;
  latest_feedback_at: string | null;
}

export interface GovernanceReassignmentPressure {
  workspace_id: string;
  workspace_slug: string;
  assigned_team: string;
  reassignment_count: number;
  manual_override_count: number;
  escalation_reassign_count: number;
  workload_rebalance_count: number;
  avg_minutes_since_open: number | null;
  avg_minutes_since_last_assignment: number | null;
  latest_reassignment_at: string | null;
}

export interface GovernanceRoutingQualityMetrics {
  routingQuality: GovernanceRoutingQuality[];
  reassignmentPressure: GovernanceReassignmentPressure[];
}

export interface GovernanceOperatorEffectiveness {
  workspace_id: string;
  workspace_slug: string;
  assigned_to: string;
  assignments: number;
  acknowledgments: number;
  resolutions: number;
  reassignments: number;
  escalations: number;
  reopens: number;
  avg_ack_hours: number | null;
  avg_resolve_hours: number | null;
  latest_outcome_at: string | null;
  resolution_rate: number | null;
  reassignment_rate: number | null;
  escalation_rate: number | null;
}

export interface GovernanceTeamEffectiveness {
  workspace_id: string;
  workspace_slug: string;
  assigned_team: string;
  assignments: number;
  acknowledgments: number;
  resolutions: number;
  reassignments: number;
  escalations: number;
  reopens: number;
  avg_ack_hours: number | null;
  avg_resolve_hours: number | null;
  latest_outcome_at: string | null;
  resolution_rate: number | null;
  reassignment_rate: number | null;
  escalation_rate: number | null;
}

export interface GovernanceRoutingRecommendationInput {
  workspace_id: string;
  workspace_slug: string;
  routing_target: string;
  root_cause_code: string | null;
  severity: string | null;
  compute_version: string | null;
  signal_registry_version: string | null;
  model_version: string | null;
  avg_ack_hours: number | null;
  avg_resolve_hours: number | null;
  resolved_count: number;
  reassigned_count: number;
  escalated_count: number;
  reopened_count: number;
  latest_outcome_at: string | null;
}

export interface GovernanceRoutingEffectivenessMetrics {
  operators: GovernanceOperatorEffectiveness[];
  teams: GovernanceTeamEffectiveness[];
  recommendationInputs: GovernanceRoutingRecommendationInput[];
}

export interface GovernanceRoutingRecommendationSummary {
  id: string;
  workspace_id: string;
  workspace_slug: string;
  case_id: string;
  case_title: string;
  case_status: string;
  severity: string;
  recommendation_key: string;
  recommended_user: string | null;
  recommended_team: string | null;
  fallback_user: string | null;
  fallback_team: string | null;
  reason_code: string;
  confidence: "low" | "medium" | "high";
  score: number;
  accepted: boolean | null;
  accepted_at: string | null;
  accepted_by: string | null;
  override_reason: string | null;
  applied: boolean;
  applied_at: string | null;
  supporting_metrics: Record<string, unknown>;
  model_inputs: Record<string, unknown>;
  alternatives: Record<string, unknown>[];
  created_at: string;
  updated_at: string;
}

export interface GovernanceRoutingRecommendationMetrics {
  recommendations: GovernanceRoutingRecommendationSummary[];
}

export interface GovernanceRoutingRecommendationReviewSummary {
  workspace_id: string;
  recommendation_id: string;
  latest_reviewed_at: string | null;
  latest_review_status: "approved" | "rejected" | "deferred" | null;
  review_count: number;
  any_applied_immediately: boolean;
}

export interface GovernanceRoutingApplicationSummary {
  workspace_id: string;
  case_id: string;
  recommendation_id: string;
  application_count: number;
  latest_applied_at: string | null;
  latest_applied_user: string | null;
  latest_applied_team: string | null;
}

export interface GovernanceRoutingAutopromotionPolicy {
  id: string;
  workspace_id: string;
  enabled: boolean;
  scope_type: "global" | "team" | "watchlist" | "root_cause" | "version_tuple" | "regime";
  scope_value: string | null;
  promotion_target: "override" | "rule";
  min_confidence: "low" | "medium" | "high";
  min_acceptance_rate: number;
  min_sample_size: number;
  max_recent_override_rate: number;
  cooldown_hours: number;
  created_by: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface GovernanceRoutingAutopromotionSummary {
  workspace_id: string;
  workspace_slug: string;
  execution_id: string;
  policy_id: string;
  scope_type: "global" | "team" | "watchlist" | "root_cause" | "version_tuple" | "regime";
  scope_value: string | null;
  promotion_target: "override" | "rule";
  recommendation_id: string;
  target_type: "override" | "rule";
  target_key: string;
  recommended_user: string | null;
  recommended_team: string | null;
  confidence: "low" | "medium" | "high" | string;
  acceptance_rate: number | null;
  sample_size: number | null;
  override_rate: number | null;
  execution_status: "executed" | "skipped" | "rolled_back" | string;
  execution_reason: string | null;
  cooldown_bucket: string | null;
  prior_state: Record<string, unknown>;
  new_state: Record<string, unknown>;
  metadata: Record<string, unknown>;
  created_at: string;
  rollback_candidate_id: string | null;
  rollback_reason: string | null;
  rolled_back: boolean | null;
  rolled_back_at: string | null;
}

export interface GovernanceRoutingAutopromotionRollbackCandidate {
  id: string;
  workspace_id: string;
  execution_id: string;
  target_type: "override" | "rule";
  target_key: string;
  prior_state: Record<string, unknown>;
  rollback_reason: string | null;
  rolled_back: boolean;
  rolled_back_at: string | null;
  created_at: string;
}

export interface GovernanceIncidentAnalyticsSummary {
  workspace_id: string;
  open_case_count: number;
  acknowledged_case_count: number;
  resolved_case_count: number;
  reopened_case_count: number;
  recurring_case_count: number;
  high_severity_open_count: number;
  mean_ack_hours: number | null;
  mean_resolve_hours: number | null;
}

export interface GovernanceRootCauseTrend {
  workspace_id: string;
  root_cause_code: string;
  case_count: number;
  reopened_count: number;
  recurring_count: number;
  severe_count: number;
  avg_case_age_hours: number | null;
}

export interface GovernanceRecurrenceBurden {
  workspace_id: string;
  watchlist_id: string | null;
  recurring_case_count: number;
  max_repeat_count: number | null;
  recurrence_group_count: number;
  reopened_case_count: number;
}

export interface GovernanceEscalationEffectivenessSummary {
  workspace_id: string;
  escalated_case_count: number;
  escalated_resolved_count: number;
  escalated_reopened_count: number;
  escalation_resolution_rate: number;
  escalation_reopen_rate: number;
}

export interface GovernanceIncidentAnalyticsResponse {
  summary: GovernanceIncidentAnalyticsSummary | null;
  rootCauseTrends: GovernanceRootCauseTrend[];
  recurrenceBurden: GovernanceRecurrenceBurden[];
  escalationEffectiveness: GovernanceEscalationEffectivenessSummary | null;
  thresholdPromotionImpact: GovernanceThresholdPromotionImpactSummary[];
  routingPromotionImpact: GovernanceRoutingPromotionImpactSummary[];
  rollbackRisk: GovernancePromotionRollbackRiskSummary[];
}

export interface GovernanceManagerOverviewSummary {
  workspace_id: string;
  workspace_slug: string;
  snapshot_id: string;
  snapshot_at: string;
  window_days: number;
  open_case_count: number;
  recurring_case_count: number;
  escalated_case_count: number;
  chronic_watchlist_count: number;
  degraded_promotion_count: number;
  rollback_risk_count: number;
  total_operating_burden: number;
  metadata: Record<string, unknown>;
}

export interface GovernanceChronicWatchlistSummary {
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  recurring_case_count: number;
  reopened_case_count: number;
  max_repeat_count: number | null;
  recurrence_group_count: number;
  latest_case_at: string | null;
}

export interface GovernanceOperatorTeamComparisonSummary {
  workspace_id: string;
  workspace_slug: string;
  entity_type: "operator" | "team" | string;
  actor_name: string;
  team_name: string | null;
  assigned_case_count: number;
  active_open_case_count: number;
  resolution_quality_proxy: number;
  reopen_rate: number;
  escalation_rate: number;
  reassignment_rate: number;
  chronic_case_count: number;
  severe_case_count: number;
  avg_ack_seconds: number | null;
  avg_resolve_seconds: number | null;
  severity_weighted_load: number | null;
}

export interface GovernancePromotionHealthOverview {
  workspace_id: string;
  workspace_slug: string;
  promotion_type: "threshold" | "routing" | string;
  promotion_count: number;
  improved_count: number;
  neutral_count: number;
  degraded_count: number;
  rollback_candidate_count: number;
  avg_rollback_risk_score: number | null;
  max_rollback_risk_score: number | null;
  latest_created_at: string | null;
}

export interface GovernanceOperatingRiskSummary {
  workspace_id: string;
  workspace_slug: string;
  snapshot_at: string;
  operating_risk: "low" | "medium" | "high" | string;
  supporting_metrics: Record<string, unknown>;
}

export interface GovernanceReviewPriorityRow {
  workspace_id: string;
  priority_rank: number;
  entity_type: "watchlist" | "team" | "operator" | "promotion" | "root_cause" | string;
  entity_key: string;
  entity_label: string;
  priority_score: number;
  priority_reason_code: string;
  open_case_count: number;
  recurring_case_count: number;
  escalated_case_count: number;
  rollback_risk_count: number;
  stale_case_count: number;
  latest_regime: string | null;
  latest_root_cause: string | null;
  snapshot_at: string;
}

export interface GovernanceTrendWindowRow {
  workspace_id: string;
  window_label: "7d" | "30d" | "90d" | string;
  metric_name:
    | "open_case_count"
    | "recurring_case_count"
    | "escalated_case_count"
    | "stale_case_count"
    | "rollback_risk_count"
    | "operator_pressure_count"
    | "team_pressure_count"
    | string;
  current_value: number;
  prior_value: number;
  delta_abs: number;
  delta_pct: number | null;
  trend_direction: "up" | "down" | "flat" | string;
  computed_at: string;
}

export interface GovernanceManagerOverviewMetrics {
  managerOverview: GovernanceManagerOverviewSummary[];
  chronicWatchlists: GovernanceChronicWatchlistSummary[];
  operatorTeamComparison: GovernanceOperatorTeamComparisonSummary[];
  promotionHealth: GovernancePromotionHealthOverview[];
  operatingRisk: GovernanceOperatingRiskSummary[];
  reviewPriorities: GovernanceReviewPriorityRow[];
  trendWindows: GovernanceTrendWindowRow[];
}

export interface GovernanceThresholdPromotionImpactSummary {
  workspace_id: string;
  workspace_slug: string;
  execution_id: string;
  scope_type: string;
  scope_value: string | null;
  impact_classification: "improved" | "neutral" | "degraded" | "rollback_candidate" | "insufficient_data" | string;
  pre_window_start: string;
  pre_window_end: string;
  post_window_start: string;
  post_window_end: string;
  recurrence_rate_before: number | null;
  recurrence_rate_after: number | null;
  escalation_rate_before: number | null;
  escalation_rate_after: number | null;
  resolution_latency_before_ms: number | null;
  resolution_latency_after_ms: number | null;
  rollback_risk_score: number | null;
  supporting_metrics: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface GovernanceRoutingPromotionImpactSummary {
  workspace_id: string;
  workspace_slug: string;
  execution_id: string;
  scope_type: string;
  scope_value: string | null;
  impact_classification: "improved" | "neutral" | "degraded" | "rollback_candidate" | "insufficient_data" | string;
  pre_window_start: string;
  pre_window_end: string;
  post_window_start: string;
  post_window_end: string;
  recurrence_rate_before: number | null;
  recurrence_rate_after: number | null;
  escalation_rate_before: number | null;
  escalation_rate_after: number | null;
  resolution_latency_before_ms: number | null;
  resolution_latency_after_ms: number | null;
  reassignment_rate_before: number | null;
  reassignment_rate_after: number | null;
  rollback_risk_score: number | null;
  supporting_metrics: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface GovernancePromotionRollbackRiskSummary {
  workspace_id: string;
  workspace_slug: string;
  promotion_type: "threshold" | "routing" | string;
  execution_id: string;
  scope_type: string;
  scope_value: string | null;
  impact_classification: "degraded" | "rollback_candidate" | string;
  rollback_risk_score: number | null;
  supporting_metrics: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface GovernanceOperatorPerformanceSummary {
  workspace_id: string;
  workspace_slug: string;
  operator_name: string;
  assigned_case_count: number;
  active_open_case_count: number;
  avg_ack_seconds: number | null;
  median_ack_seconds: number | null;
  avg_resolve_seconds: number | null;
  median_resolve_seconds: number | null;
  reopened_case_count: number;
  escalated_case_count: number;
  chronic_case_count: number;
  resolved_case_count: number;
  reassigned_case_count: number;
  severe_case_count: number;
  reopen_rate: number;
  escalation_rate: number;
  reassignment_rate: number;
  resolution_rate: number;
  resolution_quality_proxy: number;
}

export interface GovernanceTeamPerformanceSummary {
  workspace_id: string;
  workspace_slug: string;
  assigned_team: string;
  assigned_case_count: number;
  active_open_case_count: number;
  avg_ack_seconds: number | null;
  median_ack_seconds: number | null;
  avg_resolve_seconds: number | null;
  median_resolve_seconds: number | null;
  reopened_case_count: number;
  escalated_case_count: number;
  chronic_case_count: number;
  resolved_case_count: number;
  reassigned_case_count: number;
  severe_case_count: number;
  reopen_rate: number;
  escalation_rate: number;
  reassignment_rate: number;
  resolution_rate: number;
  resolution_quality_proxy: number;
}

export interface GovernanceCaseMixSummary {
  workspace_id: string;
  workspace_slug: string;
  actor_name: string;
  root_cause_code: string;
  severity: string;
  regime: string;
  case_count: number;
  recurring_case_count: number;
  chronic_case_count: number;
  severe_case_count: number;
}

export interface GovernanceThresholdPerformanceSummary {
  workspace_id: string;
  workspace_slug: string;
  threshold_profile_key: string;
  event_type: string;
  regime: string;
  compute_version: string;
  signal_registry_version: string;
  model_version: string;
  feedback_rows: number;
  trigger_count: number;
  ack_count: number;
  mute_count: number;
  escalation_count: number;
  resolution_count: number;
  reopen_count: number;
  avg_precision_proxy: number;
  avg_noise_score: number;
  latest_feedback_at: string | null;
}

export interface GovernanceThresholdLearningRecommendation {
  workspace_id: string;
  workspace_slug: string;
  recommendation_id: string;
  recommendation_key?: string;
  threshold_profile_id: string | null;
  dimension_type: string;
  dimension_value: string;
  event_type: string;
  current_value: number | null;
  recommended_value: number | null;
  direction: string;
  reason_code: string;
  confidence: number;
  supporting_metrics: Record<string, unknown>;
  status: string;
  created_at: string;
  updated_at: string;
}

export interface GovernanceThresholdLearningMetrics {
  performance: GovernanceThresholdPerformanceSummary[];
  recommendations: GovernanceThresholdLearningRecommendation[];
}

export interface GovernanceThresholdRecommendationReview {
  id: string;
  workspace_id: string;
  recommendation_id: string;
  reviewer: string;
  decision: "approved" | "rejected" | "deferred";
  rationale: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface GovernanceThresholdPromotionProposal {
  proposal_id: string;
  workspace_id: string;
  workspace_slug: string;
  recommendation_id: string;
  recommendation_key: string;
  profile_id: string;
  event_type: string;
  dimension_type: string;
  dimension_value: string | null;
  current_value: number;
  proposed_value: number;
  status: "pending" | "approved" | "blocked" | "executed" | "cancelled";
  approved_by: string | null;
  approved_at: string | null;
  blocked_reason: string | null;
  source_metrics: Record<string, unknown>;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
  direction: string;
  reason_code: string;
  confidence: number;
  latest_review_id: string | null;
  latest_reviewer: string | null;
  latest_decision: string | null;
  latest_rationale: string | null;
  latest_reviewed_at: string | null;
}

export interface GovernanceThresholdAutopromotionPolicy {
  id: string;
  workspace_id: string;
  profile_id: string | null;
  event_type: string | null;
  dimension_type: string | null;
  dimension_value: string | null;
  enabled: boolean;
  min_confidence: number;
  min_support: number;
  max_step_pct: number;
  cooldown_hours: number;
  allow_regime_specific: boolean;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface GovernanceThresholdPromotionExecution {
  execution_id: string;
  workspace_id: string;
  workspace_slug: string;
  proposal_id: string;
  recommendation_id: string;
  profile_id: string;
  event_type: string;
  dimension_type: string;
  dimension_value: string | null;
  previous_value: number;
  new_value: number;
  execution_mode: "manual" | "automatic";
  executed_by: string;
  rationale: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
  rollback_candidate_id: string | null;
  rollback_status: string | null;
  rollback_reason: string | null;
  rollback_to_value: number | null;
  rollback_updated_at: string | null;
}

export interface GovernanceThresholdLearningReviewMetrics {
  reviewSummary: GovernanceThresholdPromotionProposal[];
  autopromotionSummary: GovernanceThresholdPromotionExecution[];
}

export interface GovernanceCaseAgingRow {
  case_id: string;
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  title: string;
  status: string;
  severity: string;
  current_assignee: string | null;
  current_team: string | null;
  recurrence_group_id: string | null;
  repeat_count: number;
  opened_at: string;
  acknowledged_at: string | null;
  resolved_at: string | null;
  closed_at: string | null;
  age_minutes: number | null;
}

export interface GovernanceCaseSlaRow {
  case_id: string;
  workspace_id: string;
  workspace_slug: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  title: string;
  status: string;
  severity: string;
  current_assignee: string | null;
  current_team: string | null;
  repeat_count: number;
  opened_at: string;
  acknowledged_at: string | null;
  resolved_at: string | null;
  closed_at: string | null;
  policy_id: string | null;
  chronicity_class: string | null;
  ack_due_at: string | null;
  resolve_due_at: string | null;
  ack_breached: boolean;
  resolve_breached: boolean;
  breach_severity: string | null;
  metadata: Record<string, unknown>;
  evaluated_at: string | null;
}

export interface GovernanceStaleCaseRow extends GovernanceCaseSlaRow {
  age_minutes: number | null;
}

export interface GovernanceOperatorPressureRow {
  workspace_id: string;
  workspace_slug: string;
  assigned_to: string;
  assigned_team: string | null;
  open_case_count: number;
  recurring_case_count: number;
  severe_open_case_count: number;
  ack_breached_case_count: number;
  resolve_breached_case_count: number;
  avg_open_age_minutes: number | null;
  severity_weighted_load: number | null;
}

export interface GovernanceTeamPressureRow {
  workspace_id: string;
  workspace_slug: string;
  assigned_team: string;
  open_case_count: number;
  recurring_case_count: number;
  severe_open_case_count: number;
  ack_breached_case_count: number;
  resolve_breached_case_count: number;
  avg_open_age_minutes: number | null;
  severity_weighted_load: number | null;
}

export interface GovernanceWorkloadMetrics {
  aging: GovernanceCaseAgingRow[];
  stale: GovernanceStaleCaseRow[];
  operatorPressure: GovernanceOperatorPressureRow[];
  teamPressure: GovernanceTeamPressureRow[];
  sla: GovernanceCaseSlaRow[];
}

export interface GovernanceEscalationSummary {
  id: string;
  workspace_id: string;
  workspace_slug: string;
  case_id: string;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  case_title: string;
  case_status: string;
  severity: string;
  current_assignee: string | null;
  current_team: string | null;
  repeat_count: number;
  root_cause_code: string | null;
  escalation_level: string;
  status: string;
  escalated_to_team: string | null;
  escalated_to_user: string | null;
  reason: string | null;
  source_policy_id: string | null;
  escalated_at: string;
  last_evaluated_at: string;
  repeated_count: number;
  cleared_at: string | null;
  metadata: Record<string, unknown>;
}

export interface GovernanceEscalationEvent {
  id: string;
  workspace_id: string;
  case_id: string;
  escalation_state_id: string | null;
  event_type: string;
  escalation_level: string | null;
  escalated_to_team: string | null;
  escalated_to_user: string | null;
  reason: string | null;
  source_policy_id: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface GovernanceEscalationMetrics {
  activeEscalations: GovernanceEscalationSummary[];
  recentEvents: GovernanceEscalationEvent[];
  candidateCases: GovernanceStaleCaseRow[];
  activeEscalationCount: number;
  repeatedEscalationCount: number;
  candidateCount: number;
}

export interface GovernanceRelatedCase {
  id: string;
  status: string;
  severity: string;
  title: string;
  closed_at: string | null;
  resolved_at: string | null;
  opened_at: string;
  repeat_count: number;
  is_reopened: boolean;
}

export interface GovernanceCaseRecurrence {
  recurrenceGroupId: string | null;
  reopenedFromCaseId: string | null;
  repeatCount: number;
  isReopened: boolean;
  isRecurring: boolean;
  reopenReason: string | null;
  matchBasis: Record<string, unknown>;
  priorRelatedCaseCount: number;
  latestPriorCaseId: string | null;
  latestPriorClosedAt: string | null;
  latestPriorStatus: string | null;
  relatedCases: GovernanceRelatedCase[];
}

export interface GovernanceCaseGeneratedSummary {
  id: string;
  workspace_id: string;
  case_id: string;
  summary_version: string;
  status_summary: string | null;
  root_cause_code: string | null;
  root_cause_confidence: number | null;
  root_cause_summary: string | null;
  evidence_summary: string | null;
  recurrence_summary: string | null;
  operator_summary: string | null;
  closure_summary: string | null;
  recommended_next_action: string | null;
  source_note_ids: string[];
  source_evidence_ids: string[];
  metadata: Record<string, unknown>;
  generated_at: string;
  updated_at: string;
}

export interface GovernanceIncidentTimelineEvent {
  id: number;
  case_id: string;
  workspace_id: string;
  event_type: string;
  event_source: string;
  event_at: string;
  actor: string | null;
  title: string;
  detail: string | null;
  metadata: Record<string, unknown>;
  source_table: string | null;
  source_id: string | null;
  created_at: string;
}

export interface GovernanceCaseNote {
  id: string;
  case_id: string;
  workspace_id: string;
  author: string | null;
  note: string;
  note_type: string;
  visibility: string;
  metadata: Record<string, unknown>;
  created_at: string;
  edited_at: string | null;
}

export interface GovernanceCaseEvidence {
  id: string;
  case_id: string;
  workspace_id: string;
  evidence_type: string;
  reference_id: string;
  title: string | null;
  summary: string | null;
  payload: Record<string, unknown>;
  created_at: string;
}

export type GovernanceIncidentEvidence = GovernanceCaseEvidence;

export interface GovernanceCaseEvidenceSummary {
  case_id: string;
  workspace_id: string;
  evidence_count: number;
  latest_evidence_at: string | null;
  latest_run_id: string | null;
  latest_replay_delta_id: string | null;
  latest_regime_transition_id: string | null;
  latest_threshold_application_id: string | null;
  evidence_type_counts: Record<string, number>;
  evidence_items: GovernanceCaseEvidence[];
}

export interface GovernanceCaseInvestigationSummary {
  latest_note: GovernanceCaseNote | null;
  latest_investigation_note: GovernanceCaseNote | null;
  latest_handoff_note: GovernanceCaseNote | null;
  latest_root_cause_note: GovernanceCaseNote | null;
  latest_closure_note: GovernanceCaseNote | null;
  last_operator_summary: GovernanceCaseNote | null;
}

export interface GovernanceIncidentDetail {
  case_id: string;
  workspace_id: string;
  workspace_slug: string;
  degradation_state_id: string | null;
  watchlist_id: string | null;
  watchlist_slug: string | null;
  watchlist_name: string | null;
  version_tuple: string | null;
  status: string;
  severity: string;
  title: string;
  summary: string | null;
  opened_at: string;
  acknowledged_at: string | null;
  resolved_at: string | null;
  closed_at: string | null;
  reopened_count: number;
  current_assignee: string | null;
  current_team: string | null;
  metadata: Record<string, unknown>;
  evidence_count: number;
  run_evidence_count: number;
  cluster_evidence_count: number;
  version_evidence_count: number;
  assigned_to: string | null;
  assigned_team: string | null;
  last_assigned_at: string | null;
  timeline_event_count: number;
  latest_event_type: string | null;
  latest_event_at: string | null;
  recurrence: GovernanceCaseRecurrence;
  timeline: GovernanceIncidentTimelineEvent[];
  notes: GovernanceCaseNote[];
  evidence: GovernanceIncidentEvidence[];
  evidence_summary: GovernanceCaseEvidenceSummary | null;
  investigation_summary: GovernanceCaseInvestigationSummary;
  generated_summary: GovernanceCaseGeneratedSummary | null;
}

export interface RunScopeInspection {
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

export interface GovernanceRoutingOptimizationSnapshot {
  id: string;
  workspace_id: string;
  snapshot_at: string;
  window_label: string;
  recommendation_count: number;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface GovernanceRoutingFeatureEffectivenessRow {
  workspace_id: string;
  feature_type: string;
  feature_key: string;
  case_count: number;
  accepted_recommendation_count: number;
  override_count: number;
  reassignment_count: number;
  reopen_count: number;
  escalation_count: number;
  avg_ack_latency_seconds: number | null;
  avg_resolve_latency_seconds: number | null;
  effectiveness_score: number;
  workload_penalty_score: number;
  net_fit_score: number;
}

export interface GovernanceRoutingContextFitRow {
  workspace_id: string;
  context_key: string;
  recommended_user: string | null;
  recommended_team: string | null;
  operator_fit_score: number | null;
  team_fit_score: number | null;
  confidence: string;
  sample_size: number;
}

export interface GovernanceRoutingPolicyOpportunityRow {
  id: string;
  workspace_id: string;
  recommendation_key: string;
  reason_code: string;
  scope_type: string;
  scope_value: string | null;
  recommended_policy: Record<string, unknown>;
  confidence: string;
  expected_benefit_score: number;
  risk_score: number;
  sample_size: number;
  signal_payload: Record<string, unknown>;
  snapshot_id: string | null;
  created_at: string;
  updated_at: string;
}

export interface GovernanceRoutingOptimizationMetrics {
  snapshot: GovernanceRoutingOptimizationSnapshot | null;
  featureEffectiveness: GovernanceRoutingFeatureEffectivenessRow[];
  contextFit: GovernanceRoutingContextFitRow[];
  policyOpportunities: GovernanceRoutingPolicyOpportunityRow[];
}

export interface GovernanceRoutingPolicyRecommendationReview {
  id: string;
  workspace_id: string;
  recommendation_key: string;
  review_status: "approved" | "rejected" | "deferred";
  review_reason: string | null;
  reviewed_by: string;
  reviewed_at: string;
  notes: string | null;
  metadata: Record<string, unknown>;
}

export interface GovernanceRoutingPolicyReviewSummaryRow {
  workspace_id: string;
  recommendation_key: string;
  latest_review_status: "approved" | "rejected" | "deferred";
  latest_review_reason: string | null;
  latest_reviewed_by: string | null;
  latest_reviewed_at: string | null;
  review_count: number;
  has_approved_review: boolean;
  has_rejected_review: boolean;
  has_deferred_review: boolean;
}

export interface GovernanceRoutingPolicyPromotionProposal {
  id: string;
  workspace_id: string;
  recommendation_key: string;
  proposal_status: "pending" | "approved" | "rejected" | "applied" | "deferred";
  promotion_target: "override" | "rule";
  scope_type: string;
  scope_value: string;
  current_policy: Record<string, unknown>;
  recommended_policy: Record<string, unknown>;
  proposed_by: string;
  proposed_at: string;
  approved_by: string | null;
  approved_at: string | null;
  applied_at: string | null;
  proposal_reason: string | null;
  metadata: Record<string, unknown>;
}

export interface GovernanceRoutingPolicyPromotionSummaryRow {
  workspace_id: string;
  recommendation_key: string;
  latest_proposal_id: string | null;
  proposal_count: number;
  latest_proposal_status: "pending" | "approved" | "rejected" | "applied" | "deferred" | null;
  latest_promotion_target: "override" | "rule" | null;
  latest_scope_type: string | null;
  latest_scope_value: string | null;
  latest_proposed_by: string | null;
  latest_proposed_at: string | null;
  latest_approved_by: string | null;
  latest_approved_at: string | null;
  latest_applied_at: string | null;
  application_count: number;
}

export interface GovernanceRoutingPolicyApplication {
  id: string;
  workspace_id: string;
  proposal_id: string;
  recommendation_key: string;
  applied_target: "override" | "rule";
  applied_scope_type: string;
  applied_scope_value: string;
  prior_policy: Record<string, unknown>;
  applied_policy: Record<string, unknown>;
  applied_by: string;
  applied_at: string;
  rollback_candidate: boolean;
  metadata: Record<string, unknown>;
}

export interface GovernanceRoutingPolicyReviewMetrics {
  reviewSummary: GovernanceRoutingPolicyReviewSummaryRow[];
  promotionSummary: GovernanceRoutingPolicyPromotionSummaryRow[];
  applications: GovernanceRoutingPolicyApplication[];
}

// ── Phase 3.5C: Routing Policy Autopromotion ────────────────────────────────

export interface GovernanceRoutingPolicyAutopromotionPolicy {
  id: string;
  workspace_id: string;
  enabled: boolean;
  scope_type: string;
  scope_value: string;
  promotion_target: "rule" | "override";
  min_confidence: "low" | "medium" | "high";
  min_approved_review_count: number;
  min_application_count: number;
  min_sample_size: number;
  max_recent_override_rate: number;
  max_recent_reassignment_rate: number;
  cooldown_hours: number;
  created_by: string;
  created_at: string;
  updated_at: string;
  metadata: Record<string, unknown>;
}

export interface GovernanceRoutingPolicyAutopromotionExecution {
  id: string;
  workspace_id: string;
  policy_id: string;
  recommendation_key: string;
  proposal_id: string | null;
  application_id: string | null;
  outcome: "promoted" | "skipped" | "blocked";
  blocked_reason: string | null;
  skipped_reason: string | null;
  executed_by: string;
  executed_at: string;
  prior_policy: Record<string, unknown>;
  applied_policy: Record<string, unknown>;
  metadata: Record<string, unknown>;
}

export interface GovernanceRoutingPolicyAutopromotionRollbackCandidate {
  id: string;
  workspace_id: string;
  execution_id: string;
  recommendation_key: string;
  scope_type: string;
  scope_value: string;
  prior_policy: Record<string, unknown>;
  applied_policy: Record<string, unknown>;
  routing_row_id: string | null;
  routing_table: string | null;
  resolved: boolean;
  resolved_at: string | null;
  resolved_by: string | null;
  created_at: string;
  metadata: Record<string, unknown>;
}

export interface GovernanceRoutingPolicyAutopromotionSummaryRow {
  workspace_id: string;
  recommendation_key: string;
  latest_execution_id: string | null;
  latest_policy_id: string | null;
  latest_proposal_id: string | null;
  latest_application_id: string | null;
  latest_outcome: "promoted" | "skipped" | "blocked" | null;
  latest_blocked_reason: string | null;
  latest_skipped_reason: string | null;
  latest_executed_by: string | null;
  latest_executed_at: string | null;
  promoted_count: number;
  blocked_count: number;
  skipped_count: number;
  total_executions: number;
  open_rollback_count: number;
}

export interface GovernanceRoutingPolicyAutopromotionEligibilityRow {
  workspace_id: string;
  recommendation_key: string;
  scope_type: string;
  scope_value: string;
  confidence: string;
  sample_size: number;
  expected_benefit_score: number | null;
  risk_score: number | null;
  policy_id: string;
  promotion_target: string;
  approved_review_count: number;
  application_count: number;
  last_promoted_at: string | null;
  is_eligible: boolean;
  blocked_reason: string | null;
}

export interface GovernanceRoutingPolicyAutopromotionMetrics {
  policies: GovernanceRoutingPolicyAutopromotionPolicy[];
  summary: GovernanceRoutingPolicyAutopromotionSummaryRow[];
  eligibility: GovernanceRoutingPolicyAutopromotionEligibilityRow[];
  rollbackCandidates: GovernanceRoutingPolicyAutopromotionRollbackCandidate[];
}

// ── Phase 3.6A: Rollback Review + Execution ──────────────────────────────────

export interface GovernanceRoutingPolicyRollbackReview {
  id: string;
  workspace_id: string;
  rollback_candidate_id: string;
  review_status: "approved" | "rejected" | "deferred";
  review_reason: string | null;
  reviewed_by: string;
  reviewed_at: string;
  notes: string | null;
  metadata: Record<string, unknown>;
}

export interface GovernanceRoutingPolicyRollbackReviewSummaryRow {
  workspace_id: string;
  rollback_candidate_id: string;
  recommendation_key: string;
  latest_review_status: "approved" | "rejected" | "deferred" | null;
  latest_review_reason: string | null;
  latest_reviewed_by: string | null;
  latest_reviewed_at: string | null;
  review_count: number;
  has_approved_review: boolean;
  has_rejected_review: boolean;
  has_deferred_review: boolean;
}

export interface GovernanceRoutingPolicyRollbackExecution {
  id: string;
  workspace_id: string;
  rollback_candidate_id: string;
  execution_target: "override" | "rule";
  scope_type: string;
  scope_value: string;
  promotion_execution_id: string;
  restored_policy: Record<string, unknown>;
  replaced_policy: Record<string, unknown>;
  executed_by: string;
  executed_at: string;
  metadata: Record<string, unknown>;
}

export interface GovernanceRoutingPolicyRollbackExecutionSummaryRow {
  workspace_id: string;
  rollback_candidate_id: string;
  recommendation_key: string;
  scope_type: string;
  scope_value: string;
  target_type: string | null;
  rollback_risk_score: number;
  rolled_back: boolean;
  rolled_back_at: string | null;
  execution_count: number;
  latest_execution_id: string | null;
  latest_executed_by: string | null;
  latest_executed_at: string | null;
}

export interface GovernanceRoutingPolicyPendingRollbackRow {
  workspace_id: string;
  rollback_candidate_id: string;
  recommendation_key: string;
  scope_type: string;
  scope_value: string;
  rollback_risk_score: number;
  latest_review_status: "approved" | "rejected" | "deferred" | null;
  needs_action: boolean;
  created_at: string;
  latest_execution_at: string | null;
}

export interface GovernanceRoutingPolicyRollbackMetrics {
  pendingRollbacks: GovernanceRoutingPolicyPendingRollbackRow[];
  reviewSummary: GovernanceRoutingPolicyRollbackReviewSummaryRow[];
  executionSummary: GovernanceRoutingPolicyRollbackExecutionSummaryRow[];
}

// ── Phase 3.6B: Rollback Impact Analysis ─────────────────────────────────────

export interface GovernanceRoutingPolicyRollbackImpactRow {
  workspace_id: string;
  rollback_execution_id: string;
  rollback_candidate_id: string;
  recommendation_key: string;
  scope_type: string;
  scope_value: string;
  target_type: string;
  impact_classification: "improved" | "neutral" | "degraded" | "insufficient_data";
  evaluation_window_label: string;
  created_at: string;
  before_recurrence_rate: number | null;
  after_recurrence_rate: number | null;
  before_reassignment_rate: number | null;
  after_reassignment_rate: number | null;
  before_escalation_rate: number | null;
  after_escalation_rate: number | null;
  before_avg_resolve_latency_seconds: number | null;
  after_avg_resolve_latency_seconds: number | null;
  before_reopen_rate: number | null;
  after_reopen_rate: number | null;
  before_workload_pressure: number | null;
  after_workload_pressure: number | null;
  delta_recurrence_rate: number | null;
  delta_reassignment_rate: number | null;
  delta_escalation_rate: number | null;
  delta_avg_resolve_latency_seconds: number | null;
  delta_reopen_rate: number | null;
  delta_workload_pressure: number | null;
}

export interface GovernanceRoutingPolicyRollbackEffectivenessSummary {
  workspace_id: string;
  rollback_count: number;
  improved_count: number;
  neutral_count: number;
  degraded_count: number;
  insufficient_data_count: number;
  improved_rate: number | null;
  degraded_rate: number | null;
  latest_rollback_at: string | null;
  average_delta_recurrence_rate: number | null;
  average_delta_escalation_rate: number | null;
  average_delta_resolve_latency_seconds: number | null;
  average_delta_workload_pressure: number | null;
}

export interface GovernanceRoutingPolicyRollbackPendingEvaluationRow {
  workspace_id: string;
  rollback_execution_id: string;
  rollback_candidate_id: string;
  recommendation_key: string;
  scope_type: string;
  scope_value: string;
  target_type: string;
  executed_at: string;
  days_since_execution: number;
  has_impact_snapshot: boolean;
  sufficient_post_data: boolean;
  pending_reason_code: string;
}

export interface GovernanceRoutingPolicyRollbackImpactMetrics {
  impactRows: GovernanceRoutingPolicyRollbackImpactRow[];
  effectivenessSummary: GovernanceRoutingPolicyRollbackEffectivenessSummary | null;
  pendingEvaluations: GovernanceRoutingPolicyRollbackPendingEvaluationRow[];
}

// ── Phase 3.7A: Governance Policy Optimization ────────────────────────────────

export interface GovernancePolicyOptimizationSnapshot {
  id: string;
  workspace_id: string;
  snapshot_at: string;
  window_label: string;
  recommendation_count: number;
  metadata: Record<string, unknown>;
}

export interface GovernancePolicyFeatureEffectivenessRow {
  workspace_id: string;
  policy_family: string;
  feature_type: string;
  feature_key: string;
  sample_size: number;
  recurrence_rate: number | null;
  reopen_rate: number | null;
  escalation_rate: number | null;
  reassignment_rate: number | null;
  rollback_rate: number | null;
  mute_rate: number | null;
  approved_review_rate: number | null;
  application_rate: number | null;
  avg_ack_latency_seconds: number | null;
  avg_resolve_latency_seconds: number | null;
  effectiveness_score: number;
  risk_score: number;
  net_policy_fit_score: number;
}

export interface GovernancePolicyContextFitRow {
  workspace_id: string;
  context_key: string;
  best_policy_family: string;
  best_policy_variant: string;
  fit_score: number;
  sample_size: number;
  confidence: string;
  supporting_metrics: Record<string, unknown>;
}

export interface GovernancePolicyOpportunityRow {
  workspace_id: string;
  recommendation_key: string;
  policy_family: string;
  scope_type: string;
  scope_value: string;
  current_policy: Record<string, unknown>;
  recommended_policy: Record<string, unknown>;
  reason_code: string;
  confidence: string;
  sample_size: number;
  expected_benefit_score: number;
  risk_score: number;
  supporting_metrics: Record<string, unknown>;
  created_at: string;
}

export interface GovernancePolicyOptimizationMetrics {
  snapshot: GovernancePolicyOptimizationSnapshot | null;
  featureEffectiveness: GovernancePolicyFeatureEffectivenessRow[];
  contextFit: GovernancePolicyContextFitRow[];
  policyOpportunities: GovernancePolicyOpportunityRow[];
}

// ── Phase 3.7B: Governance Policy Review + Promotion ─────────────────────────

export interface GovernancePolicyReviewSummaryRow {
  workspace_id: string;
  recommendation_key: string;
  policy_family: string;
  latest_review_status: "approved" | "rejected" | "deferred" | null;
  latest_review_reason: string | null;
  latest_reviewed_by: string | null;
  latest_reviewed_at: string | null;
  review_count: number;
  has_approved_review: boolean;
  has_rejected_review: boolean;
  has_deferred_review: boolean;
}

export interface GovernancePolicyPromotionSummaryRow {
  workspace_id: string;
  recommendation_key: string;
  policy_family: string;
  proposal_count: number;
  latest_proposal_status: "pending" | "approved" | "rejected" | "applied" | "deferred" | null;
  latest_promotion_target: string | null;
  latest_scope_type: string | null;
  latest_scope_value: string | null;
  latest_proposed_by: string | null;
  latest_proposed_at: string | null;
  latest_approved_by: string | null;
  latest_approved_at: string | null;
  latest_applied_at: string | null;
  application_count: number;
}

export interface GovernancePolicyPendingPromotionRow {
  workspace_id: string;
  recommendation_key: string;
  policy_family: string;
  latest_proposal_status: string;
  latest_promotion_target: string | null;
  latest_scope_type: string | null;
  latest_scope_value: string | null;
  latest_proposed_by: string | null;
  latest_proposed_at: string | null;
  application_count: number;
  needs_action: boolean;
}

export interface GovernancePolicyApplicationRow {
  id: string;
  workspace_id: string;
  proposal_id: string;
  recommendation_key: string;
  policy_family: string;
  applied_target: string;
  applied_scope_type: string;
  applied_scope_value: string;
  prior_policy: Record<string, unknown>;
  applied_policy: Record<string, unknown>;
  applied_by: string;
  applied_at: string;
  rollback_candidate: boolean;
  metadata: Record<string, unknown>;
}

export interface GovernancePolicyReviewMetrics {
  reviewSummary: GovernancePolicyReviewSummaryRow[];
}

export interface GovernancePolicyPromotionMetrics {
  promotionSummary: GovernancePolicyPromotionSummaryRow[];
  pendingPromotions: GovernancePolicyPendingPromotionRow[];
}

// Phase 3.7C: Governance Policy Autopromotion
export interface GovernancePolicyAutopromotionSummaryRow {
  policy_id: string;
  workspace_id: string;
  policy_family: string;
  scope_type: string;
  scope_value: string;
  promotion_target: string;
  min_confidence: "low" | "medium" | "high";
  min_approved_review_count: number;
  min_application_count: number;
  cooldown_hours: number;
  enabled: boolean;
  execution_count: number;
  rollback_candidate_count: number;
  latest_execution_at: string | null;
}

export interface GovernancePolicyAutopromotionEligibilityRow {
  workspace_id: string;
  recommendation_key: string;
  policy_id: string;
  policy_family: string;
  scope_type: string;
  scope_value: string;
  promotion_target: string;
  eligible: boolean;
  blocked_reason_code: string | null;
  confidence: string | null;
  sample_size: number;
  approved_review_count: number;
  application_count: number;
  cooldown_ends_at: string | null;
}

export interface GovernancePolicyAutopromotionRollbackCandidateRow {
  id: string;
  workspace_id: string;
  execution_id: string;
  recommendation_key: string;
  policy_family: string;
  scope_type: string;
  scope_value: string;
  target_type: string;
  rollback_risk_score: number | string;
  rolled_back: boolean;
  created_at: string;
}

// Phase 4.0A: Multi-Asset Data Foundation
export interface MultiAssetSyncHealthRow {
  workspace_id: string;
  provider_family: string | null;
  asset_class: string;
  requested_symbol_count: number;
  synced_symbol_count: number;
  failed_symbol_count: number;
  latest_run_started_at: string | null;
  latest_run_completed_at: string | null;
  latest_status: string | null;
  latest_provider_mode: string | null;
  latest_metadata: Record<string, unknown>;
}

export interface NormalizedMultiAssetMarketStateRow {
  workspace_id: string;
  symbol: string;
  canonical_symbol: string;
  asset_class: string;
  provider_family: string | null;
  price: number | string | null;
  price_timestamp: string | null;
  volume_24h: number | string | null;
  oi_change_1h: number | string | null;
  funding_rate: number | string | null;
  yield_value: number | string | null;
  fx_return_1d: number | string | null;
  macro_proxy_value: number | string | null;
  liquidation_count: number | null;
  metadata: Record<string, unknown>;
}

export interface MultiAssetFamilyStateSummaryRow {
  workspace_id: string;
  asset_class: string;
  family_key: string;
  symbol_count: number;
  latest_timestamp: string | null;
  avg_return_1d: number | string | null;
  avg_volatility_proxy: number | string | null;
  metadata: Record<string, unknown>;
}

export interface MultiAssetFoundationMetrics {
  syncHealth: MultiAssetSyncHealthRow[];
  marketStateSample: NormalizedMultiAssetMarketStateRow[];
  familySummary: MultiAssetFamilyStateSummaryRow[];
}

// Phase 4.0B: Dependency Graph + Context Model
export interface WatchlistContextSnapshotRow {
  id: string;
  workspace_id: string;
  watchlist_id: string;
  profile_id: string | null;
  snapshot_at: string;
  primary_symbols: string[];
  dependency_symbols: string[];
  dependency_families: string[];
  context_hash: string;
  coverage_summary: Record<string, unknown>;
  metadata: Record<string, unknown>;
}

export interface WatchlistDependencyCoverageSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  context_hash: string;
  primary_symbol_count: number;
  dependency_symbol_count: number;
  dependency_family_count: number;
  covered_dependency_count: number;
  missing_dependency_count: number;
  stale_dependency_count: number;
  latest_context_snapshot_at: string | null;
  coverage_ratio: number | string | null;
  metadata: Record<string, unknown>;
}

export interface WatchlistDependencyContextDetailRow {
  workspace_id: string;
  watchlist_id: string;
  context_hash: string;
  symbol: string;
  asset_class: string | null;
  dependency_family: string;
  dependency_type: string | null;
  priority: number | null;
  weight: number | string | null;
  is_primary: boolean;
  latest_timestamp: string | null;
  is_missing: boolean;
  is_stale: boolean;
  metadata: Record<string, unknown>;
}

export interface WatchlistDependencyFamilyStateRow {
  workspace_id: string;
  watchlist_id: string;
  context_hash: string;
  dependency_family: string;
  symbol_count: number;
  covered_count: number;
  missing_count: number;
  stale_count: number;
  latest_timestamp: string | null;
  metadata: Record<string, unknown>;
}

export interface DependencyContextMetrics {
  latestContexts: WatchlistContextSnapshotRow[];
  coverageSummary: WatchlistDependencyCoverageSummaryRow[];
  contextDetail: WatchlistDependencyContextDetailRow[];
  familyState: WatchlistDependencyFamilyStateRow[];
}

// Phase 4.0C: Cross-Asset Signal Expansion
export interface CrossAssetSignalSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string | null;
  context_snapshot_id: string | null;
  signal_family: string;
  signal_key: string;
  signal_value: number | string | null;
  signal_direction: "bullish" | "bearish" | "neutral" | null;
  signal_state: "computed" | "confirmed" | "unconfirmed" | "contradicted" | "missing_context" | "stale_context";
  base_symbol: string | null;
  dependency_symbol_count: number;
  dependency_family_count: number;
  created_at: string;
}

export interface CrossAssetDependencyHealthRow {
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  feature_count: number;
  signal_count: number;
  missing_dependency_count: number;
  stale_dependency_count: number;
  confirmed_count: number;
  contradicted_count: number;
  latest_created_at: string | null;
}

export interface RunCrossAssetContextSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_feature_count: number;
  cross_asset_signal_count: number;
  confirmed_signal_count: number;
  contradicted_signal_count: number;
  missing_context_count: number;
  stale_context_count: number;
  dominant_dependency_family: string | null;
  created_at: string;
}

export interface CrossAssetSignalMetrics {
  signalSummary: CrossAssetSignalSummaryRow[];
  dependencyHealth: CrossAssetDependencyHealthRow[];
  runContextSummary: RunCrossAssetContextSummaryRow[];
}

// Phase 4.0D: Cross-Asset Explainability
export interface CrossAssetExplanationSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string | null;
  context_snapshot_id: string | null;
  dominant_dependency_family: string | null;
  cross_asset_confidence_score: number | string | null;
  confirmation_score: number | string | null;
  contradiction_score: number | string | null;
  missing_context_score: number | string | null;
  stale_context_score: number | string | null;
  top_confirming_symbols: string[];
  top_contradicting_symbols: string[];
  missing_dependency_symbols: string[];
  stale_dependency_symbols: string[];
  explanation_state: "computed" | "partial" | "missing_context" | "stale_context";
  created_at: string;
}

export interface CrossAssetFamilyExplanationSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string | null;
  context_snapshot_id: string | null;
  dependency_family: string;
  family_signal_count: number;
  confirmed_count: number;
  contradicted_count: number;
  missing_count: number;
  stale_count: number;
  family_confidence_score: number | string | null;
  family_support_score: number | string | null;
  family_contradiction_score: number | string | null;
  top_symbols: string[];
  created_at: string;
}

export interface RunCrossAssetExplanationBridgeRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  dominant_dependency_family: string | null;
  cross_asset_confidence_score: number | string | null;
  confirmation_score: number | string | null;
  contradiction_score: number | string | null;
  missing_context_score: number | string | null;
  stale_context_score: number | string | null;
  explanation_state: "computed" | "partial" | "missing_context" | "stale_context";
  created_at: string;
}

export interface CrossAssetExplainabilityMetrics {
  explanationSummary: CrossAssetExplanationSummaryRow[];
  familySummary: CrossAssetFamilyExplanationSummaryRow[];
  runBridgeSummary: RunCrossAssetExplanationBridgeRow[];
}

// Phase 4.1A: Cross-Asset Attribution + Composite Integration
export interface CrossAssetAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  base_signal_score: number | string | null;
  cross_asset_signal_score: number | string | null;
  cross_asset_confirmation_score: number | string | null;
  cross_asset_contradiction_penalty: number | string | null;
  cross_asset_missing_penalty: number | string | null;
  cross_asset_stale_penalty: number | string | null;
  cross_asset_net_contribution: number | string | null;
  composite_pre_cross_asset: number | string | null;
  composite_post_cross_asset: number | string | null;
  integration_mode: "additive_guardrailed" | "confirmation_only" | "suppression_only";
  created_at: string;
}

export interface CrossAssetFamilyAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  family_signal_score: number | string | null;
  family_confirmation_score: number | string | null;
  family_contradiction_penalty: number | string | null;
  family_missing_penalty: number | string | null;
  family_stale_penalty: number | string | null;
  family_net_contribution: number | string | null;
  family_rank: number | null;
  top_symbols: string[];
  created_at: string;
}

export interface RunCompositeIntegrationSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  base_signal_score: number | string | null;
  cross_asset_signal_score: number | string | null;
  cross_asset_net_contribution: number | string | null;
  composite_pre_cross_asset: number | string | null;
  composite_post_cross_asset: number | string | null;
  dominant_dependency_family: string | null;
  cross_asset_confidence_score: number | string | null;
  created_at: string;
}

export interface CrossAssetAttributionMetrics {
  attributionSummary: CrossAssetAttributionSummaryRow[];
  familyAttributionSummary: CrossAssetFamilyAttributionSummaryRow[];
  runIntegrationSummary: RunCompositeIntegrationSummaryRow[];
}

// Phase 4.1B: Dependency-Priority-Aware Ranking + Contribution Weighting
export interface CrossAssetFamilyWeightedAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  raw_family_net_contribution: number | string | null;
  priority_weight: number | string | null;
  family_weight: number | string | null;
  type_weight: number | string | null;
  coverage_weight: number | string | null;
  weighted_family_net_contribution: number | string | null;
  weighted_family_rank: number | null;
  top_symbols: string[];
  created_at: string;
}

export interface CrossAssetSymbolWeightedAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  symbol: string;
  dependency_family: string;
  dependency_type: string | null;
  graph_priority: number | null;
  is_direct_dependency: boolean;
  raw_symbol_score: number | string | null;
  priority_weight: number | string | null;
  family_weight: number | string | null;
  type_weight: number | string | null;
  coverage_weight: number | string | null;
  weighted_symbol_score: number | string | null;
  symbol_rank: number | null;
  created_at: string;
}

export interface RunCrossAssetWeightedIntegrationSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  base_signal_score: number | string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  created_at: string;
}

export interface DependencyPriorityWeightingMetrics {
  familyWeightedSummary: CrossAssetFamilyWeightedAttributionSummaryRow[];
  symbolWeightedSummary: CrossAssetSymbolWeightedAttributionSummaryRow[];
  runWeightedSummary: RunCrossAssetWeightedIntegrationSummaryRow[];
}

// Phase 4.1C: Regime-Aware Cross-Asset Interpretation
export interface CrossAssetFamilyRegimeAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  regime_key: string;
  dependency_family: string;
  raw_family_net_contribution: number | string | null;
  weighted_family_net_contribution: number | string | null;
  regime_family_weight: number | string | null;
  regime_type_weight: number | string | null;
  regime_confirmation_scale: number | string | null;
  regime_contradiction_scale: number | string | null;
  regime_missing_penalty_scale: number | string | null;
  regime_stale_penalty_scale: number | string | null;
  regime_adjusted_family_contribution: number | string | null;
  regime_family_rank: number | null;
  interpretation_state: "computed" | "partial" | "missing_regime" | "regime_mismatch";
  top_symbols: string[];
  created_at: string;
}

export interface CrossAssetSymbolRegimeAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  regime_key: string;
  symbol: string;
  dependency_family: string;
  dependency_type: string | null;
  graph_priority: number | null;
  is_direct_dependency: boolean;
  raw_symbol_score: number | string | null;
  weighted_symbol_score: number | string | null;
  regime_family_weight: number | string | null;
  regime_type_weight: number | string | null;
  regime_adjusted_symbol_score: number | string | null;
  symbol_rank: number | null;
  created_at: string;
}

export interface RunCrossAssetRegimeIntegrationSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  regime_key: string;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  cross_asset_confidence_score: number | string | null;
  created_at: string;
}

export interface RegimeAwareCrossAssetMetrics {
  familyRegimeSummary: CrossAssetFamilyRegimeAttributionSummaryRow[];
  symbolRegimeSummary: CrossAssetSymbolRegimeAttributionSummaryRow[];
  runRegimeSummary: RunCrossAssetRegimeIntegrationSummaryRow[];
}

// Phase 4.1D: Cross-Asset Replay + Stability Validation
export interface CrossAssetReplayValidationSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  source_context_snapshot_id: string | null;
  replay_context_snapshot_id: string | null;
  source_regime_key: string | null;
  replay_regime_key: string | null;
  context_hash_match: boolean;
  regime_match: boolean;
  raw_attribution_match: boolean;
  weighted_attribution_match: boolean;
  regime_attribution_match: boolean;
  dominant_family_match: boolean;
  weighted_dominant_family_match: boolean;
  regime_dominant_family_match: boolean;
  drift_reason_codes: string[];
  validation_state: "validated" | "drift_detected" | "insufficient_source" | "insufficient_replay" | "context_mismatch";
  created_at: string;
}

export interface CrossAssetFamilyReplayStabilitySummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  dependency_family: string;
  source_raw_contribution: number | string | null;
  replay_raw_contribution: number | string | null;
  source_weighted_contribution: number | string | null;
  replay_weighted_contribution: number | string | null;
  source_regime_contribution: number | string | null;
  replay_regime_contribution: number | string | null;
  raw_delta: number | string | null;
  weighted_delta: number | string | null;
  regime_delta: number | string | null;
  family_rank_match: boolean;
  weighted_family_rank_match: boolean;
  regime_family_rank_match: boolean;
  drift_reason_codes: string[];
  created_at: string;
}

export interface CrossAssetReplayStabilityAggregateRow {
  workspace_id: string;
  validation_count: number;
  context_match_rate: number | string | null;
  regime_match_rate: number | string | null;
  raw_match_rate: number | string | null;
  weighted_match_rate: number | string | null;
  regime_match_rate_attribution: number | string | null;
  dominant_family_match_rate: number | string | null;
  weighted_dominant_family_match_rate: number | string | null;
  regime_dominant_family_match_rate: number | string | null;
  drift_detected_count: number;
  latest_validated_at: string | null;
}

export interface CrossAssetReplayValidationMetrics {
  replayValidationSummary: CrossAssetReplayValidationSummaryRow[];
  familyReplayStabilitySummary: CrossAssetFamilyReplayStabilitySummaryRow[];
  replayStabilityAggregate: CrossAssetReplayStabilityAggregateRow | null;
}

// Phase 4.2A: Cross-Asset Lead/Lag + Dependency Timing
export interface CrossAssetLeadLagPairSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string | null;
  context_snapshot_id: string | null;
  base_symbol: string;
  dependency_symbol: string;
  dependency_family: string;
  dependency_type: string | null;
  lag_bucket: "lead" | "coincident" | "lag" | "insufficient_data";
  best_lag_hours: number | null;
  timing_strength: number | string | null;
  correlation_at_best_lag: number | string | null;
  window_label: string;
  created_at: string;
}

export interface CrossAssetFamilyTimingSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string | null;
  context_snapshot_id: string | null;
  dependency_family: string;
  lead_pair_count: number;
  coincident_pair_count: number;
  lag_pair_count: number;
  avg_best_lag_hours: number | string | null;
  avg_timing_strength: number | string | null;
  dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data";
  top_leading_symbols: string[];
  created_at: string;
}

export interface RunCrossAssetTimingSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  lead_pair_count: number;
  coincident_pair_count: number;
  lag_pair_count: number;
  dominant_leading_family: string | null;
  strongest_leading_symbol: string | null;
  avg_timing_strength: number | string | null;
  created_at: string;
}

export interface CrossAssetTimingMetrics {
  pairSummary: CrossAssetLeadLagPairSummaryRow[];
  familyTimingSummary: CrossAssetFamilyTimingSummaryRow[];
  runTimingSummary: RunCrossAssetTimingSummaryRow[];
}

// Phase 4.2B: Family-Level Lead/Lag Attribution
export interface CrossAssetFamilyTimingAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  raw_family_net_contribution: number | string | null;
  weighted_family_net_contribution: number | string | null;
  regime_adjusted_family_contribution: number | string | null;
  dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data";
  lead_pair_count: number;
  coincident_pair_count: number;
  lag_pair_count: number;
  timing_class_weight: number | string | null;
  timing_bonus: number | string | null;
  timing_penalty: number | string | null;
  timing_adjusted_family_contribution: number | string | null;
  timing_family_rank: number | null;
  top_leading_symbols: string[];
  created_at: string;
}

export interface CrossAssetSymbolTimingAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  symbol: string;
  dependency_family: string;
  dependency_type: string | null;
  lag_bucket: "lead" | "coincident" | "lag" | "insufficient_data";
  best_lag_hours: number | null;
  raw_symbol_score: number | string | null;
  weighted_symbol_score: number | string | null;
  regime_adjusted_symbol_score: number | string | null;
  timing_class_weight: number | string | null;
  timing_adjusted_symbol_score: number | string | null;
  symbol_rank: number | null;
  created_at: string;
}

export interface RunCrossAssetTimingAttributionSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  timing_dominant_dependency_family: string | null;
  created_at: string;
}

export interface CrossAssetTimingAttributionMetrics {
  familyTimingAttributionSummary: CrossAssetFamilyTimingAttributionSummaryRow[];
  symbolTimingAttributionSummary: CrossAssetSymbolTimingAttributionSummaryRow[];
  runTimingAttributionSummary: RunCrossAssetTimingAttributionSummaryRow[];
}

// Phase 4.2C: Timing-Aware Composite Refinement
export interface CrossAssetTimingCompositeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  base_signal_score: number | string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  composite_pre_timing: number | string | null;
  timing_net_contribution: number | string | null;
  composite_post_timing: number | string | null;
  dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data";
  integration_mode: "timing_additive_guardrailed" | "lead_confirmation_only" | "lag_suppression_only";
  created_at: string;
}

export interface CrossAssetFamilyTimingCompositeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data";
  timing_adjusted_family_contribution: number | string | null;
  integration_weight_applied: number | string | null;
  timing_integration_contribution: number | string | null;
  family_rank: number | null;
  top_symbols: string[];
  created_at: string;
}

export interface RunCrossAssetFinalIntegrationSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  timing_net_contribution: number | string | null;
  composite_pre_timing: number | string | null;
  composite_post_timing: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  timing_dominant_dependency_family: string | null;
  dominant_timing_class: string | null;
  created_at: string;
}

export interface CrossAssetTimingCompositeMetrics {
  timingCompositeSummary: CrossAssetTimingCompositeSummaryRow[];
  familyTimingCompositeSummary: CrossAssetFamilyTimingCompositeSummaryRow[];
  finalIntegrationSummary: RunCrossAssetFinalIntegrationSummaryRow[];
}

// Phase 4.2D: Replay Validation for Timing-Aware Composite
export interface CrossAssetTimingReplayValidationSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  source_context_snapshot_id: string | null;
  replay_context_snapshot_id: string | null;
  source_regime_key: string | null;
  replay_regime_key: string | null;
  source_dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data" | null;
  replay_dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data" | null;
  context_hash_match: boolean;
  regime_match: boolean;
  timing_class_match: boolean;
  timing_attribution_match: boolean;
  timing_composite_match: boolean;
  timing_dominant_family_match: boolean;
  drift_reason_codes: string[];
  validation_state: "validated" | "drift_detected" | "insufficient_source" | "insufficient_replay" | "context_mismatch" | "timing_mismatch";
  created_at: string;
}

export interface CrossAssetFamilyTimingReplayStabilitySummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  dependency_family: string;
  source_dominant_timing_class: string | null;
  replay_dominant_timing_class: string | null;
  source_timing_adjusted_contribution: number | string | null;
  replay_timing_adjusted_contribution: number | string | null;
  source_timing_integration_contribution: number | string | null;
  replay_timing_integration_contribution: number | string | null;
  timing_adjusted_delta: number | string | null;
  timing_integration_delta: number | string | null;
  timing_class_match: boolean;
  timing_family_rank_match: boolean;
  timing_composite_family_rank_match: boolean;
  drift_reason_codes: string[];
  created_at: string;
}

export interface CrossAssetTimingReplayStabilityAggregateRow {
  workspace_id: string;
  validation_count: number;
  context_match_rate: number | string | null;
  regime_match_rate: number | string | null;
  timing_class_match_rate: number | string | null;
  timing_attribution_match_rate: number | string | null;
  timing_composite_match_rate: number | string | null;
  timing_dominant_family_match_rate: number | string | null;
  drift_detected_count: number;
  latest_validated_at: string | null;
}

export interface CrossAssetTimingReplayValidationMetrics {
  timingReplayValidationSummary: CrossAssetTimingReplayValidationSummaryRow[];
  familyTimingReplayStabilitySummary: CrossAssetFamilyTimingReplayStabilitySummaryRow[];
  timingReplayStabilityAggregate: CrossAssetTimingReplayStabilityAggregateRow | null;
}

// Phase 4.3A: Family-Level Sequencing + Transition-State Diagnostics
export interface CrossAssetFamilyTransitionStateSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  regime_key: string | null;
  dominant_timing_class: string | null;
  signal_state: "confirmed" | "unconfirmed" | "contradicted" | "missing_context" | "stale_context" | "insufficient_data";
  transition_state: "reinforcing" | "deteriorating" | "recovering" | "rotating_in" | "rotating_out" | "stable" | "insufficient_history";
  family_contribution: number | string | null;
  timing_adjusted_contribution: number | string | null;
  timing_integration_contribution: number | string | null;
  family_rank: number | null;
  created_at: string;
}

export interface CrossAssetFamilyTransitionEventSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string | null;
  target_run_id: string;
  dependency_family: string;
  prior_signal_state: string | null;
  current_signal_state: string;
  prior_transition_state: string | null;
  current_transition_state: string;
  prior_family_rank: number | null;
  current_family_rank: number | null;
  rank_delta: number | null;
  prior_family_contribution: number | string | null;
  current_family_contribution: number | string | null;
  contribution_delta: number | string | null;
  event_type: "state_shift" | "rank_shift" | "dominance_gain" | "dominance_loss" | "recovery" | "degradation" | "timing_shift" | "regime_shift";
  created_at: string;
}

export interface CrossAssetFamilySequenceSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string | null;
  dependency_family: string;
  window_label: string;
  sequence_signature: string;
  sequence_length: number;
  dominant_sequence_class: "reinforcing_path" | "deteriorating_path" | "recovery_path" | "rotation_path" | "mixed_path" | "insufficient_history";
  sequence_confidence: number | string | null;
  created_at: string;
}

export interface RunCrossAssetTransitionDiagnosticsSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  dominant_dependency_family: string | null;
  prior_dominant_dependency_family: string | null;
  dominant_timing_class: string | null;
  dominant_transition_state: string | null;
  dominant_sequence_class: string | null;
  rotation_event_count: number;
  degradation_event_count: number;
  recovery_event_count: number;
  created_at: string;
}

export interface CrossAssetTransitionDiagnosticsMetrics {
  transitionStateSummary: CrossAssetFamilyTransitionStateSummaryRow[];
  transitionEventSummary: CrossAssetFamilyTransitionEventSummaryRow[];
  sequenceSummary: CrossAssetFamilySequenceSummaryRow[];
  runTransitionSummary: RunCrossAssetTransitionDiagnosticsSummaryRow[];
}

// Phase 4.3B: Transition-Aware Attribution
export interface CrossAssetFamilyTransitionAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  raw_family_net_contribution: number | string | null;
  weighted_family_net_contribution: number | string | null;
  regime_adjusted_family_contribution: number | string | null;
  timing_adjusted_family_contribution: number | string | null;
  transition_state: "reinforcing" | "deteriorating" | "recovering" | "rotating_in" | "rotating_out" | "stable" | "insufficient_history";
  dominant_sequence_class: "reinforcing_path" | "deteriorating_path" | "recovery_path" | "rotation_path" | "mixed_path" | "insufficient_history";
  transition_state_weight: number | string | null;
  sequence_class_weight: number | string | null;
  transition_bonus: number | string | null;
  transition_penalty: number | string | null;
  transition_adjusted_family_contribution: number | string | null;
  transition_family_rank: number | null;
  top_symbols: string[];
  created_at: string;
}

export interface CrossAssetSymbolTransitionAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  symbol: string;
  dependency_family: string;
  dependency_type: string | null;
  transition_state: string;
  dominant_sequence_class: string;
  raw_symbol_score: number | string | null;
  weighted_symbol_score: number | string | null;
  regime_adjusted_symbol_score: number | string | null;
  timing_adjusted_symbol_score: number | string | null;
  transition_state_weight: number | string | null;
  sequence_class_weight: number | string | null;
  transition_adjusted_symbol_score: number | string | null;
  symbol_rank: number | null;
  created_at: string;
}

export interface RunCrossAssetTransitionAttributionSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  timing_dominant_dependency_family: string | null;
  transition_dominant_dependency_family: string | null;
  dominant_transition_state: string | null;
  dominant_sequence_class: string | null;
  created_at: string;
}

export interface CrossAssetTransitionAttributionMetrics {
  familyTransitionAttributionSummary: CrossAssetFamilyTransitionAttributionSummaryRow[];
  symbolTransitionAttributionSummary: CrossAssetSymbolTransitionAttributionSummaryRow[];
  runTransitionAttributionSummary: RunCrossAssetTransitionAttributionSummaryRow[];
}

// ── Phase 4.3C: Sequencing-Aware Composite Refinement ───────────────────
export interface CrossAssetTransitionCompositeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  base_signal_score: number | string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  composite_pre_transition: number | string | null;
  transition_net_contribution: number | string | null;
  composite_post_transition: number | string | null;
  dominant_transition_state: "reinforcing" | "deteriorating" | "recovering" | "rotating_in" | "rotating_out" | "stable" | "insufficient_history";
  integration_mode: "transition_additive_guardrailed" | "reinforcing_confirmation_only" | "deteriorating_suppression_only" | "rotation_handoff_sensitive";
  created_at: string;
}

export interface CrossAssetFamilyTransitionCompositeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  transition_state: "reinforcing" | "deteriorating" | "recovering" | "rotating_in" | "rotating_out" | "stable" | "insufficient_history";
  dominant_sequence_class: "reinforcing_path" | "deteriorating_path" | "recovery_path" | "rotation_path" | "mixed_path" | "insufficient_history";
  transition_adjusted_family_contribution: number | string | null;
  integration_weight_applied: number | string | null;
  transition_integration_contribution: number | string | null;
  family_rank: number | null;
  top_symbols: string[];
  created_at: string;
}

export interface RunCrossAssetSequencingIntegrationSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  transition_net_contribution: number | string | null;
  composite_pre_transition: number | string | null;
  composite_post_transition: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  timing_dominant_dependency_family: string | null;
  transition_dominant_dependency_family: string | null;
  dominant_transition_state: string | null;
  created_at: string;
}

export interface CrossAssetTransitionCompositeMetrics {
  transitionCompositeSummary: CrossAssetTransitionCompositeSummaryRow[];
  familyTransitionCompositeSummary: CrossAssetFamilyTransitionCompositeSummaryRow[];
  finalSequencingIntegrationSummary: RunCrossAssetSequencingIntegrationSummaryRow[];
}

// ── Phase 4.3D: Replay Validation for Sequencing-Aware Composite ────────
export interface CrossAssetTransitionReplayValidationSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  source_context_snapshot_id: string | null;
  replay_context_snapshot_id: string | null;
  source_regime_key: string | null;
  replay_regime_key: string | null;
  source_dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data" | null;
  replay_dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data" | null;
  source_dominant_transition_state: "reinforcing" | "deteriorating" | "recovering" | "rotating_in" | "rotating_out" | "stable" | "insufficient_history" | null;
  replay_dominant_transition_state: "reinforcing" | "deteriorating" | "recovering" | "rotating_in" | "rotating_out" | "stable" | "insufficient_history" | null;
  source_dominant_sequence_class: "reinforcing_path" | "deteriorating_path" | "recovery_path" | "rotation_path" | "mixed_path" | "insufficient_history" | null;
  replay_dominant_sequence_class: "reinforcing_path" | "deteriorating_path" | "recovery_path" | "rotation_path" | "mixed_path" | "insufficient_history" | null;
  context_hash_match: boolean;
  regime_match: boolean;
  timing_class_match: boolean;
  transition_state_match: boolean;
  sequence_class_match: boolean;
  transition_attribution_match: boolean;
  transition_composite_match: boolean;
  transition_dominant_family_match: boolean;
  drift_reason_codes: string[];
  validation_state: "validated" | "drift_detected" | "insufficient_source" | "insufficient_replay" | "context_mismatch" | "timing_mismatch" | "transition_mismatch";
  created_at: string;
}

export interface CrossAssetFamilyTransitionReplayStabilitySummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  dependency_family: string;
  source_transition_state: string | null;
  replay_transition_state: string | null;
  source_sequence_class: string | null;
  replay_sequence_class: string | null;
  source_transition_adjusted_contribution: number | string | null;
  replay_transition_adjusted_contribution: number | string | null;
  source_transition_integration_contribution: number | string | null;
  replay_transition_integration_contribution: number | string | null;
  transition_adjusted_delta: number | string | null;
  transition_integration_delta: number | string | null;
  transition_state_match: boolean;
  sequence_class_match: boolean;
  transition_family_rank_match: boolean;
  transition_composite_family_rank_match: boolean;
  drift_reason_codes: string[];
  created_at: string;
}

export interface CrossAssetTransitionReplayStabilityAggregateRow {
  workspace_id: string;
  validation_count: number;
  context_match_rate: number | string | null;
  regime_match_rate: number | string | null;
  timing_class_match_rate: number | string | null;
  transition_state_match_rate: number | string | null;
  sequence_class_match_rate: number | string | null;
  transition_attribution_match_rate: number | string | null;
  transition_composite_match_rate: number | string | null;
  transition_dominant_family_match_rate: number | string | null;
  drift_detected_count: number;
  latest_validated_at: string | null;
}

export interface CrossAssetTransitionReplayValidationMetrics {
  transitionReplayValidationSummary: CrossAssetTransitionReplayValidationSummaryRow[];
  familyTransitionReplayStabilitySummary: CrossAssetFamilyTransitionReplayStabilitySummaryRow[];
  transitionReplayStabilityAggregate: CrossAssetTransitionReplayStabilityAggregateRow | null;
}

// ── Phase 4.4A: Sequencing Pattern Registry + Transition Archetypes ─────
export interface CrossAssetFamilyArchetypeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  regime_key: string | null;
  archetype_key: string;
  transition_state: "reinforcing" | "deteriorating" | "recovering" | "rotating_in" | "rotating_out" | "stable" | "insufficient_history";
  dominant_sequence_class: "reinforcing_path" | "deteriorating_path" | "recovery_path" | "rotation_path" | "mixed_path" | "insufficient_history";
  dominant_timing_class: string | null;
  family_rank: number | null;
  family_contribution: number | string | null;
  archetype_confidence: number | string | null;
  classification_reason_codes: string[];
  created_at: string;
}

export interface CrossAssetRunArchetypeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  regime_key: string | null;
  dominant_archetype_key: string;
  dominant_dependency_family: string | null;
  dominant_transition_state: string | null;
  dominant_sequence_class: string | null;
  archetype_confidence: number | string | null;
  rotation_event_count: number;
  recovery_event_count: number;
  degradation_event_count: number;
  mixed_event_count: number;
  classification_reason_codes: string[];
  created_at: string;
}

export interface CrossAssetRegimeArchetypeSummaryRow {
  workspace_id: string;
  regime_key: string | null;
  archetype_key: string;
  run_count: number;
  avg_confidence: number | string | null;
  latest_seen_at: string | null;
}

export interface RunCrossAssetPatternSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  regime_key: string | null;
  dominant_archetype_key: string;
  dominant_dependency_family: string | null;
  dominant_transition_state: string | null;
  dominant_sequence_class: string | null;
  archetype_confidence: number | string | null;
  created_at: string;
}

export interface CrossAssetPatternMetrics {
  familyArchetypeSummary: CrossAssetFamilyArchetypeSummaryRow[];
  runArchetypeSummary: CrossAssetRunArchetypeSummaryRow[];
  regimeArchetypeSummary: CrossAssetRegimeArchetypeSummaryRow[];
  runPatternSummary: RunCrossAssetPatternSummaryRow[];
}

// ── Phase 4.4B: Archetype-Aware Attribution ─────────────────────────────
export interface CrossAssetFamilyArchetypeAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  raw_family_net_contribution: number | string | null;
  weighted_family_net_contribution: number | string | null;
  regime_adjusted_family_contribution: number | string | null;
  timing_adjusted_family_contribution: number | string | null;
  transition_adjusted_family_contribution: number | string | null;
  archetype_key: string;
  transition_state: "reinforcing" | "deteriorating" | "recovering" | "rotating_in" | "rotating_out" | "stable" | "insufficient_history";
  dominant_sequence_class: "reinforcing_path" | "deteriorating_path" | "recovery_path" | "rotation_path" | "mixed_path" | "insufficient_history";
  archetype_weight: number | string | null;
  archetype_bonus: number | string | null;
  archetype_penalty: number | string | null;
  archetype_adjusted_family_contribution: number | string | null;
  archetype_family_rank: number | null;
  top_symbols: string[];
  classification_reason_codes: string[];
  created_at: string;
}

export interface CrossAssetSymbolArchetypeAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  symbol: string;
  dependency_family: string;
  dependency_type: string | null;
  archetype_key: string;
  transition_state: string;
  dominant_sequence_class: string;
  raw_symbol_score: number | string | null;
  weighted_symbol_score: number | string | null;
  regime_adjusted_symbol_score: number | string | null;
  timing_adjusted_symbol_score: number | string | null;
  transition_adjusted_symbol_score: number | string | null;
  archetype_weight: number | string | null;
  archetype_adjusted_symbol_score: number | string | null;
  symbol_rank: number | null;
  classification_reason_codes: string[];
  created_at: string;
}

export interface RunCrossAssetArchetypeAttributionSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  archetype_adjusted_cross_asset_contribution: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  timing_dominant_dependency_family: string | null;
  transition_dominant_dependency_family: string | null;
  archetype_dominant_dependency_family: string | null;
  dominant_archetype_key: string | null;
  created_at: string;
}

export interface CrossAssetArchetypeAttributionMetrics {
  familyArchetypeAttributionSummary: CrossAssetFamilyArchetypeAttributionSummaryRow[];
  symbolArchetypeAttributionSummary: CrossAssetSymbolArchetypeAttributionSummaryRow[];
  runArchetypeAttributionSummary: RunCrossAssetArchetypeAttributionSummaryRow[];
}

// ── Phase 4.4C: Archetype-Aware Composite Refinement ────────────────────
export interface CrossAssetArchetypeCompositeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  base_signal_score: number | string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  archetype_adjusted_cross_asset_contribution: number | string | null;
  composite_pre_archetype: number | string | null;
  archetype_net_contribution: number | string | null;
  composite_post_archetype: number | string | null;
  dominant_archetype_key: string;
  integration_mode: "archetype_additive_guardrailed" | "reinforcing_confirmation_only" | "breakdown_suppression_only" | "rotation_sensitive";
  created_at: string;
}

export interface CrossAssetFamilyArchetypeCompositeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  archetype_key: string;
  transition_state: "reinforcing" | "deteriorating" | "recovering" | "rotating_in" | "rotating_out" | "stable" | "insufficient_history";
  dominant_sequence_class: "reinforcing_path" | "deteriorating_path" | "recovery_path" | "rotation_path" | "mixed_path" | "insufficient_history";
  archetype_adjusted_family_contribution: number | string | null;
  integration_weight_applied: number | string | null;
  archetype_integration_contribution: number | string | null;
  family_rank: number | null;
  top_symbols: string[];
  classification_reason_codes: string[];
  created_at: string;
}

export interface RunCrossAssetArchetypeIntegrationSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  archetype_adjusted_cross_asset_contribution: number | string | null;
  archetype_net_contribution: number | string | null;
  composite_pre_archetype: number | string | null;
  composite_post_archetype: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  timing_dominant_dependency_family: string | null;
  transition_dominant_dependency_family: string | null;
  archetype_dominant_dependency_family: string | null;
  dominant_archetype_key: string | null;
  created_at: string;
}

export interface CrossAssetArchetypeCompositeMetrics {
  archetypeCompositeSummary: CrossAssetArchetypeCompositeSummaryRow[];
  familyArchetypeCompositeSummary: CrossAssetFamilyArchetypeCompositeSummaryRow[];
  finalArchetypeIntegrationSummary: RunCrossAssetArchetypeIntegrationSummaryRow[];
}

// ── Phase 4.4D: Replay Validation for Archetype-Aware Composite ─────────
export interface CrossAssetArchetypeReplayValidationSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  source_context_snapshot_id: string | null;
  replay_context_snapshot_id: string | null;
  source_regime_key: string | null;
  replay_regime_key: string | null;
  source_dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data" | null;
  replay_dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data" | null;
  source_dominant_transition_state: "reinforcing" | "deteriorating" | "recovering" | "rotating_in" | "rotating_out" | "stable" | "insufficient_history" | null;
  replay_dominant_transition_state: "reinforcing" | "deteriorating" | "recovering" | "rotating_in" | "rotating_out" | "stable" | "insufficient_history" | null;
  source_dominant_sequence_class: "reinforcing_path" | "deteriorating_path" | "recovery_path" | "rotation_path" | "mixed_path" | "insufficient_history" | null;
  replay_dominant_sequence_class: "reinforcing_path" | "deteriorating_path" | "recovery_path" | "rotation_path" | "mixed_path" | "insufficient_history" | null;
  source_dominant_archetype_key: string | null;
  replay_dominant_archetype_key: string | null;
  context_hash_match: boolean;
  regime_match: boolean;
  timing_class_match: boolean;
  transition_state_match: boolean;
  sequence_class_match: boolean;
  archetype_match: boolean;
  archetype_attribution_match: boolean;
  archetype_composite_match: boolean;
  archetype_dominant_family_match: boolean;
  drift_reason_codes: string[];
  validation_state: "validated" | "drift_detected" | "insufficient_source" | "insufficient_replay" | "context_mismatch" | "timing_mismatch" | "transition_mismatch" | "archetype_mismatch";
  created_at: string;
}

export interface CrossAssetFamilyArchetypeReplayStabilitySummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  dependency_family: string;
  source_transition_state: string | null;
  replay_transition_state: string | null;
  source_sequence_class: string | null;
  replay_sequence_class: string | null;
  source_archetype_key: string | null;
  replay_archetype_key: string | null;
  source_archetype_adjusted_contribution: number | string | null;
  replay_archetype_adjusted_contribution: number | string | null;
  source_archetype_integration_contribution: number | string | null;
  replay_archetype_integration_contribution: number | string | null;
  archetype_adjusted_delta: number | string | null;
  archetype_integration_delta: number | string | null;
  transition_state_match: boolean;
  sequence_class_match: boolean;
  archetype_match: boolean;
  archetype_family_rank_match: boolean;
  archetype_composite_family_rank_match: boolean;
  drift_reason_codes: string[];
  created_at: string;
}

export interface CrossAssetArchetypeReplayStabilityAggregateRow {
  workspace_id: string;
  validation_count: number;
  context_match_rate: number | string | null;
  regime_match_rate: number | string | null;
  timing_class_match_rate: number | string | null;
  transition_state_match_rate: number | string | null;
  sequence_class_match_rate: number | string | null;
  archetype_match_rate: number | string | null;
  archetype_attribution_match_rate: number | string | null;
  archetype_composite_match_rate: number | string | null;
  archetype_dominant_family_match_rate: number | string | null;
  drift_detected_count: number;
  latest_validated_at: string | null;
}

export interface CrossAssetArchetypeReplayValidationMetrics {
  archetypeReplayValidationSummary: CrossAssetArchetypeReplayValidationSummaryRow[];
  familyArchetypeReplayStabilitySummary: CrossAssetFamilyArchetypeReplayStabilitySummaryRow[];
  archetypeReplayStabilityAggregate: CrossAssetArchetypeReplayStabilityAggregateRow | null;
}

// ── Phase 4.5A: Pattern-Cluster Drift + Archetype Regime Rotation ───────
export interface CrossAssetArchetypeClusterSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  regime_key: string | null;
  window_label: string;
  dominant_archetype_key: string;
  archetype_mix: Record<string, number>;
  reinforcement_share: number | string | null;
  recovery_share: number | string | null;
  rotation_share: number | string | null;
  degradation_share: number | string | null;
  mixed_share: number | string | null;
  pattern_entropy: number | string | null;
  cluster_state: "stable" | "rotating" | "deteriorating" | "recovering" | "mixed" | "insufficient_history";
  drift_score: number | string | null;
  created_at: string;
}

export interface CrossAssetArchetypeRegimeRotationSummaryRow {
  workspace_id: string;
  regime_key: string;
  window_label: string;
  prior_dominant_archetype_key: string | null;
  current_dominant_archetype_key: string | null;
  rotation_count: number;
  reinforcement_run_count: number;
  recovery_run_count: number;
  degradation_run_count: number;
  mixed_run_count: number;
  rotation_state: "stable" | "rotating" | "deteriorating" | "recovering" | "mixed" | "insufficient_history";
  regime_drift_score: number | string | null;
  created_at: string;
}

export interface CrossAssetPatternDriftEventSummaryRow {
  workspace_id: string;
  watchlist_id: string | null;
  source_run_id: string | null;
  target_run_id: string | null;
  regime_key: string | null;
  prior_cluster_state: string | null;
  current_cluster_state: string;
  prior_dominant_archetype_key: string | null;
  current_dominant_archetype_key: string;
  drift_event_type: "archetype_rotation" | "reinforcement_break" | "recovery_break" | "degradation_acceleration" | "mixed_noise_increase" | "stabilization" | "insufficient_history";
  drift_score: number | string | null;
  reason_codes: string[];
  created_at: string;
}

export interface RunCrossAssetPatternClusterSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  regime_key: string | null;
  dominant_archetype_key: string | null;
  cluster_state: string | null;
  drift_score: number | string | null;
  pattern_entropy: number | string | null;
  current_rotation_state: string | null;
  latest_drift_event_type: string | null;
  created_at: string;
}

export interface CrossAssetPatternClusterMetrics {
  clusterSummary: CrossAssetArchetypeClusterSummaryRow[];
  regimeRotationSummary: CrossAssetArchetypeRegimeRotationSummaryRow[];
  driftEventSummary: CrossAssetPatternDriftEventSummaryRow[];
  runPatternClusterSummary: RunCrossAssetPatternClusterSummaryRow[];
}

// ── Phase 4.5B: Cluster-Aware Attribution ───────────────────────────────
export interface CrossAssetFamilyClusterAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  raw_family_net_contribution: number | string | null;
  weighted_family_net_contribution: number | string | null;
  regime_adjusted_family_contribution: number | string | null;
  timing_adjusted_family_contribution: number | string | null;
  transition_adjusted_family_contribution: number | string | null;
  archetype_adjusted_family_contribution: number | string | null;
  cluster_state: "stable" | "rotating" | "deteriorating" | "recovering" | "mixed" | "insufficient_history";
  dominant_archetype_key: string;
  drift_score: number | string | null;
  pattern_entropy: number | string | null;
  cluster_weight: number | string | null;
  cluster_bonus: number | string | null;
  cluster_penalty: number | string | null;
  cluster_adjusted_family_contribution: number | string | null;
  cluster_family_rank: number | null;
  top_symbols: string[];
  reason_codes: string[];
  created_at: string;
}

export interface CrossAssetSymbolClusterAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  symbol: string;
  dependency_family: string;
  dependency_type: string | null;
  cluster_state: string;
  dominant_archetype_key: string;
  raw_symbol_score: number | string | null;
  weighted_symbol_score: number | string | null;
  regime_adjusted_symbol_score: number | string | null;
  timing_adjusted_symbol_score: number | string | null;
  transition_adjusted_symbol_score: number | string | null;
  archetype_adjusted_symbol_score: number | string | null;
  cluster_weight: number | string | null;
  cluster_adjusted_symbol_score: number | string | null;
  symbol_rank: number | null;
  reason_codes: string[];
  created_at: string;
}

export interface RunCrossAssetClusterAttributionSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  archetype_adjusted_cross_asset_contribution: number | string | null;
  cluster_adjusted_cross_asset_contribution: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  timing_dominant_dependency_family: string | null;
  transition_dominant_dependency_family: string | null;
  archetype_dominant_dependency_family: string | null;
  cluster_dominant_dependency_family: string | null;
  cluster_state: string | null;
  dominant_archetype_key: string | null;
  created_at: string;
}

export interface CrossAssetClusterAttributionMetrics {
  familyClusterAttributionSummary: CrossAssetFamilyClusterAttributionSummaryRow[];
  symbolClusterAttributionSummary: CrossAssetSymbolClusterAttributionSummaryRow[];
  runClusterAttributionSummary: RunCrossAssetClusterAttributionSummaryRow[];
}

// ── Phase 4.5C: Cluster-Aware Composite Refinement ──────────────────────
export interface CrossAssetClusterCompositeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  base_signal_score: number | string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  archetype_adjusted_cross_asset_contribution: number | string | null;
  cluster_adjusted_cross_asset_contribution: number | string | null;
  composite_pre_cluster: number | string | null;
  cluster_net_contribution: number | string | null;
  composite_post_cluster: number | string | null;
  cluster_state: "stable" | "rotating" | "deteriorating" | "recovering" | "mixed" | "insufficient_history";
  dominant_archetype_key: string;
  integration_mode: "cluster_additive_guardrailed" | "stable_confirmation_only" | "deterioration_suppression_only" | "rotation_sensitive";
  created_at: string;
}

export interface CrossAssetFamilyClusterCompositeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  cluster_state: "stable" | "rotating" | "deteriorating" | "recovering" | "mixed" | "insufficient_history";
  dominant_archetype_key: string;
  cluster_adjusted_family_contribution: number | string | null;
  integration_weight_applied: number | string | null;
  cluster_integration_contribution: number | string | null;
  family_rank: number | null;
  top_symbols: string[];
  reason_codes: string[];
  created_at: string;
}

export interface RunCrossAssetClusterIntegrationSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  archetype_adjusted_cross_asset_contribution: number | string | null;
  cluster_adjusted_cross_asset_contribution: number | string | null;
  cluster_net_contribution: number | string | null;
  composite_pre_cluster: number | string | null;
  composite_post_cluster: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  timing_dominant_dependency_family: string | null;
  transition_dominant_dependency_family: string | null;
  archetype_dominant_dependency_family: string | null;
  cluster_dominant_dependency_family: string | null;
  cluster_state: string | null;
  dominant_archetype_key: string | null;
  created_at: string;
}

export interface CrossAssetClusterCompositeMetrics {
  clusterCompositeSummary: CrossAssetClusterCompositeSummaryRow[];
  familyClusterCompositeSummary: CrossAssetFamilyClusterCompositeSummaryRow[];
  finalClusterIntegrationSummary: RunCrossAssetClusterIntegrationSummaryRow[];
}

// ── Phase 4.5D: Replay Validation for Cluster-Aware Composite ───────────
export interface CrossAssetClusterReplayValidationSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  source_context_snapshot_id: string | null;
  replay_context_snapshot_id: string | null;
  source_regime_key: string | null;
  replay_regime_key: string | null;
  source_dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data" | null;
  replay_dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data" | null;
  source_dominant_transition_state: "reinforcing" | "deteriorating" | "recovering" | "rotating_in" | "rotating_out" | "stable" | "insufficient_history" | null;
  replay_dominant_transition_state: "reinforcing" | "deteriorating" | "recovering" | "rotating_in" | "rotating_out" | "stable" | "insufficient_history" | null;
  source_dominant_sequence_class: "reinforcing_path" | "deteriorating_path" | "recovery_path" | "rotation_path" | "mixed_path" | "insufficient_history" | null;
  replay_dominant_sequence_class: "reinforcing_path" | "deteriorating_path" | "recovery_path" | "rotation_path" | "mixed_path" | "insufficient_history" | null;
  source_dominant_archetype_key: string | null;
  replay_dominant_archetype_key: string | null;
  source_cluster_state: "stable" | "rotating" | "deteriorating" | "recovering" | "mixed" | "insufficient_history" | null;
  replay_cluster_state: "stable" | "rotating" | "deteriorating" | "recovering" | "mixed" | "insufficient_history" | null;
  source_drift_score: number | string | null;
  replay_drift_score: number | string | null;
  context_hash_match: boolean;
  regime_match: boolean;
  timing_class_match: boolean;
  transition_state_match: boolean;
  sequence_class_match: boolean;
  archetype_match: boolean;
  cluster_state_match: boolean;
  drift_score_match: boolean;
  cluster_attribution_match: boolean;
  cluster_composite_match: boolean;
  cluster_dominant_family_match: boolean;
  drift_reason_codes: string[];
  validation_state: "validated" | "drift_detected" | "insufficient_source" | "insufficient_replay" | "context_mismatch" | "timing_mismatch" | "transition_mismatch" | "archetype_mismatch" | "cluster_mismatch";
  created_at: string;
}

export interface CrossAssetFamilyClusterReplayStabilitySummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  dependency_family: string;
  source_cluster_state: string | null;
  replay_cluster_state: string | null;
  source_dominant_archetype_key: string | null;
  replay_dominant_archetype_key: string | null;
  source_cluster_adjusted_contribution: number | string | null;
  replay_cluster_adjusted_contribution: number | string | null;
  source_cluster_integration_contribution: number | string | null;
  replay_cluster_integration_contribution: number | string | null;
  cluster_adjusted_delta: number | string | null;
  cluster_integration_delta: number | string | null;
  cluster_state_match: boolean;
  archetype_match: boolean;
  cluster_family_rank_match: boolean;
  cluster_composite_family_rank_match: boolean;
  drift_reason_codes: string[];
  created_at: string;
}

export interface CrossAssetClusterReplayStabilityAggregateRow {
  workspace_id: string;
  validation_count: number;
  context_match_rate: number | string | null;
  regime_match_rate: number | string | null;
  timing_class_match_rate: number | string | null;
  transition_state_match_rate: number | string | null;
  sequence_class_match_rate: number | string | null;
  archetype_match_rate: number | string | null;
  cluster_state_match_rate: number | string | null;
  drift_score_match_rate: number | string | null;
  cluster_attribution_match_rate: number | string | null;
  cluster_composite_match_rate: number | string | null;
  cluster_dominant_family_match_rate: number | string | null;
  drift_detected_count: number;
  latest_validated_at: string | null;
}

export interface CrossAssetClusterReplayValidationMetrics {
  clusterReplayValidationSummary: CrossAssetClusterReplayValidationSummaryRow[];
  familyClusterReplayStabilitySummary: CrossAssetFamilyClusterReplayStabilitySummaryRow[];
  clusterReplayStabilityAggregate: CrossAssetClusterReplayStabilityAggregateRow | null;
}

// ── Phase 4.6A: Cross-Window Regime Memory + Persistence Diagnostics ─────
export interface CrossAssetStatePersistenceSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  window_label: string;
  regime_key: string | null;
  dominant_timing_class: string | null;
  dominant_transition_state: string | null;
  dominant_sequence_class: string | null;
  dominant_archetype_key: string | null;
  cluster_state: string | null;
  current_state_signature: string;
  state_age_runs: number;
  same_state_count: number;
  state_persistence_ratio: number | string | null;
  regime_persistence_ratio: number | string | null;
  cluster_persistence_ratio: number | string | null;
  archetype_persistence_ratio: number | string | null;
  persistence_state: "persistent" | "fragile" | "rotating" | "breaking_down" | "recovering" | "mixed" | "insufficient_history";
  memory_score: number | string | null;
  created_at: string;
}

export interface CrossAssetRegimeMemorySummaryRow {
  workspace_id: string;
  regime_key: string;
  window_label: string;
  run_count: number;
  same_regime_streak_count: number;
  regime_switch_count: number;
  avg_regime_duration_runs: number | string | null;
  max_regime_duration_runs: number | null;
  regime_memory_score: number | string | null;
  dominant_cluster_state: string | null;
  dominant_archetype_key: string | null;
  persistence_state: "persistent" | "fragile" | "rotating" | "breaking_down" | "recovering" | "mixed" | "insufficient_history";
  created_at: string;
}

export interface CrossAssetPersistenceTransitionEventSummaryRow {
  workspace_id: string;
  watchlist_id: string | null;
  source_run_id: string | null;
  target_run_id: string;
  regime_key: string | null;
  prior_state_signature: string | null;
  current_state_signature: string;
  prior_persistence_state: string | null;
  current_persistence_state: string;
  prior_memory_score: number | string | null;
  current_memory_score: number | string | null;
  memory_score_delta: number | string | null;
  event_type: "persistence_gain" | "persistence_loss" | "regime_memory_break" | "cluster_memory_break" | "archetype_memory_break" | "state_rotation" | "stabilization" | "insufficient_history";
  reason_codes: string[];
  created_at: string;
}

export interface RunCrossAssetPersistenceSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  regime_key: string | null;
  cluster_state: string | null;
  dominant_archetype_key: string | null;
  persistence_state: string | null;
  memory_score: number | string | null;
  state_age_runs: number | null;
  state_persistence_ratio: number | string | null;
  latest_persistence_event_type: string | null;
  created_at: string;
}

export interface CrossAssetPersistenceMetrics {
  statePersistenceSummary: CrossAssetStatePersistenceSummaryRow[];
  regimeMemorySummary: CrossAssetRegimeMemorySummaryRow[];
  persistenceEventSummary: CrossAssetPersistenceTransitionEventSummaryRow[];
  runPersistenceSummary: RunCrossAssetPersistenceSummaryRow[];
}

// ── Phase 4.6B: Persistence-Aware Attribution ───────────────────────────
export interface CrossAssetFamilyPersistenceAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  raw_family_net_contribution: number | string | null;
  weighted_family_net_contribution: number | string | null;
  regime_adjusted_family_contribution: number | string | null;
  timing_adjusted_family_contribution: number | string | null;
  transition_adjusted_family_contribution: number | string | null;
  archetype_adjusted_family_contribution: number | string | null;
  cluster_adjusted_family_contribution: number | string | null;
  persistence_state: "persistent" | "fragile" | "rotating" | "breaking_down" | "recovering" | "mixed" | "insufficient_history";
  memory_score: number | string | null;
  state_age_runs: number | null;
  state_persistence_ratio: number | string | null;
  regime_persistence_ratio: number | string | null;
  cluster_persistence_ratio: number | string | null;
  archetype_persistence_ratio: number | string | null;
  latest_persistence_event_type: string | null;
  persistence_weight: number | string | null;
  persistence_bonus: number | string | null;
  persistence_penalty: number | string | null;
  persistence_adjusted_family_contribution: number | string | null;
  persistence_family_rank: number | null;
  top_symbols: string[];
  reason_codes: string[];
  created_at: string;
}

export interface CrossAssetSymbolPersistenceAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  symbol: string;
  dependency_family: string;
  dependency_type: string | null;
  persistence_state: string;
  memory_score: number | string | null;
  state_age_runs: number | null;
  latest_persistence_event_type: string | null;
  raw_symbol_score: number | string | null;
  weighted_symbol_score: number | string | null;
  regime_adjusted_symbol_score: number | string | null;
  timing_adjusted_symbol_score: number | string | null;
  transition_adjusted_symbol_score: number | string | null;
  archetype_adjusted_symbol_score: number | string | null;
  cluster_adjusted_symbol_score: number | string | null;
  persistence_weight: number | string | null;
  persistence_adjusted_symbol_score: number | string | null;
  symbol_rank: number | null;
  reason_codes: string[];
  created_at: string;
}

export interface RunCrossAssetPersistenceAttributionSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  archetype_adjusted_cross_asset_contribution: number | string | null;
  cluster_adjusted_cross_asset_contribution: number | string | null;
  persistence_adjusted_cross_asset_contribution: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  timing_dominant_dependency_family: string | null;
  transition_dominant_dependency_family: string | null;
  archetype_dominant_dependency_family: string | null;
  cluster_dominant_dependency_family: string | null;
  persistence_dominant_dependency_family: string | null;
  persistence_state: string | null;
  memory_score: number | string | null;
  state_age_runs: number | null;
  latest_persistence_event_type: string | null;
  created_at: string;
}

export interface CrossAssetPersistenceAttributionMetrics {
  familyPersistenceAttributionSummary: CrossAssetFamilyPersistenceAttributionSummaryRow[];
  symbolPersistenceAttributionSummary: CrossAssetSymbolPersistenceAttributionSummaryRow[];
  runPersistenceAttributionSummary: RunCrossAssetPersistenceAttributionSummaryRow[];
}

// ── Phase 4.6C: Persistence-Aware Composite Refinement ──────────────────
export interface CrossAssetPersistenceCompositeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  base_signal_score: number | string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  archetype_adjusted_cross_asset_contribution: number | string | null;
  cluster_adjusted_cross_asset_contribution: number | string | null;
  persistence_adjusted_cross_asset_contribution: number | string | null;
  composite_pre_persistence: number | string | null;
  persistence_net_contribution: number | string | null;
  composite_post_persistence: number | string | null;
  persistence_state: "persistent" | "fragile" | "rotating" | "breaking_down" | "recovering" | "mixed" | "insufficient_history";
  memory_score: number | string | null;
  state_age_runs: number | null;
  latest_persistence_event_type: string | null;
  integration_mode: "persistence_additive_guardrailed" | "persistent_confirmation_only" | "memory_break_suppression_only" | "recovery_sensitive";
  created_at: string;
}

export interface CrossAssetFamilyPersistenceCompositeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  persistence_state: "persistent" | "fragile" | "rotating" | "breaking_down" | "recovering" | "mixed" | "insufficient_history";
  memory_score: number | string | null;
  state_age_runs: number | null;
  latest_persistence_event_type: string | null;
  persistence_adjusted_family_contribution: number | string | null;
  integration_weight_applied: number | string | null;
  persistence_integration_contribution: number | string | null;
  family_rank: number | null;
  top_symbols: string[];
  reason_codes: string[];
  created_at: string;
}

export interface RunCrossAssetPersistenceIntegrationSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  archetype_adjusted_cross_asset_contribution: number | string | null;
  cluster_adjusted_cross_asset_contribution: number | string | null;
  persistence_adjusted_cross_asset_contribution: number | string | null;
  persistence_net_contribution: number | string | null;
  composite_pre_persistence: number | string | null;
  composite_post_persistence: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  timing_dominant_dependency_family: string | null;
  transition_dominant_dependency_family: string | null;
  archetype_dominant_dependency_family: string | null;
  cluster_dominant_dependency_family: string | null;
  persistence_dominant_dependency_family: string | null;
  persistence_state: string | null;
  memory_score: number | string | null;
  state_age_runs: number | null;
  latest_persistence_event_type: string | null;
  created_at: string;
}

export interface CrossAssetPersistenceCompositeMetrics {
  persistenceCompositeSummary: CrossAssetPersistenceCompositeSummaryRow[];
  familyPersistenceCompositeSummary: CrossAssetFamilyPersistenceCompositeSummaryRow[];
  finalPersistenceIntegrationSummary: RunCrossAssetPersistenceIntegrationSummaryRow[];
}

// ── Phase 4.6D: Replay Validation for Persistence-Aware Composite ───────
export interface CrossAssetPersistenceReplayValidationSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  source_context_snapshot_id: string | null;
  replay_context_snapshot_id: string | null;
  source_regime_key: string | null;
  replay_regime_key: string | null;
  source_dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data" | null;
  replay_dominant_timing_class: "lead" | "coincident" | "lag" | "insufficient_data" | null;
  source_dominant_transition_state: string | null;
  replay_dominant_transition_state: string | null;
  source_dominant_sequence_class: string | null;
  replay_dominant_sequence_class: string | null;
  source_dominant_archetype_key: string | null;
  replay_dominant_archetype_key: string | null;
  source_cluster_state: string | null;
  replay_cluster_state: string | null;
  source_persistence_state: "persistent" | "fragile" | "rotating" | "breaking_down" | "recovering" | "mixed" | "insufficient_history" | null;
  replay_persistence_state: "persistent" | "fragile" | "rotating" | "breaking_down" | "recovering" | "mixed" | "insufficient_history" | null;
  source_memory_score: number | string | null;
  replay_memory_score: number | string | null;
  source_state_age_runs: number | null;
  replay_state_age_runs: number | null;
  source_latest_persistence_event_type: string | null;
  replay_latest_persistence_event_type: string | null;
  context_hash_match: boolean;
  regime_match: boolean;
  timing_class_match: boolean;
  transition_state_match: boolean;
  sequence_class_match: boolean;
  archetype_match: boolean;
  cluster_state_match: boolean;
  persistence_state_match: boolean;
  memory_score_match: boolean;
  state_age_match: boolean;
  persistence_event_match: boolean;
  persistence_attribution_match: boolean;
  persistence_composite_match: boolean;
  persistence_dominant_family_match: boolean;
  drift_reason_codes: string[];
  validation_state:
    | "validated"
    | "drift_detected"
    | "insufficient_source"
    | "insufficient_replay"
    | "context_mismatch"
    | "timing_mismatch"
    | "transition_mismatch"
    | "archetype_mismatch"
    | "cluster_mismatch"
    | "persistence_mismatch";
  created_at: string;
}

export interface CrossAssetFamilyPersistenceReplayStabilitySummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  dependency_family: string;
  source_persistence_state: string | null;
  replay_persistence_state: string | null;
  source_memory_score: number | string | null;
  replay_memory_score: number | string | null;
  source_state_age_runs: number | null;
  replay_state_age_runs: number | null;
  source_latest_persistence_event_type: string | null;
  replay_latest_persistence_event_type: string | null;
  source_persistence_adjusted_contribution: number | string | null;
  replay_persistence_adjusted_contribution: number | string | null;
  source_persistence_integration_contribution: number | string | null;
  replay_persistence_integration_contribution: number | string | null;
  persistence_adjusted_delta: number | string | null;
  persistence_integration_delta: number | string | null;
  persistence_state_match: boolean;
  memory_score_match: boolean;
  state_age_match: boolean;
  persistence_event_match: boolean;
  persistence_family_rank_match: boolean;
  persistence_composite_family_rank_match: boolean;
  drift_reason_codes: string[];
  created_at: string;
}

export interface CrossAssetPersistenceReplayStabilityAggregateRow {
  workspace_id: string;
  validation_count: number;
  context_match_rate: number | string | null;
  regime_match_rate: number | string | null;
  timing_class_match_rate: number | string | null;
  transition_state_match_rate: number | string | null;
  sequence_class_match_rate: number | string | null;
  archetype_match_rate: number | string | null;
  cluster_state_match_rate: number | string | null;
  persistence_state_match_rate: number | string | null;
  memory_score_match_rate: number | string | null;
  state_age_match_rate: number | string | null;
  persistence_event_match_rate: number | string | null;
  persistence_attribution_match_rate: number | string | null;
  persistence_composite_match_rate: number | string | null;
  persistence_dominant_family_match_rate: number | string | null;
  drift_detected_count: number;
  latest_validated_at: string | null;
}

export interface CrossAssetPersistenceReplayValidationMetrics {
  persistenceReplayValidationSummary: CrossAssetPersistenceReplayValidationSummaryRow[];
  familyPersistenceReplayStabilitySummary: CrossAssetFamilyPersistenceReplayStabilitySummaryRow[];
  persistenceReplayStabilityAggregate: CrossAssetPersistenceReplayStabilityAggregateRow | null;
}

// ── Phase 4.7A: Signal Decay & Stale-Memory Diagnostics ─────────────────
export type CrossAssetSignalFreshnessState =
  | "fresh"
  | "decaying"
  | "stale"
  | "contradicted"
  | "mixed"
  | "insufficient_history";

export type CrossAssetStaleMemoryEventType =
  | "memory_freshened"
  | "memory_decayed"
  | "memory_became_stale"
  | "memory_contradicted"
  | "memory_reconfirmed"
  | "insufficient_history";

export interface CrossAssetSignalDecaySummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  regime_key: string | null;
  dominant_timing_class: string | null;
  dominant_transition_state: string | null;
  dominant_sequence_class: string | null;
  dominant_archetype_key: string | null;
  cluster_state: string | null;
  persistence_state: string | null;
  current_state_signature: string;
  state_age_runs: number | null;
  memory_score: number | string | null;
  regime_decay_score: number | string | null;
  timing_decay_score: number | string | null;
  transition_decay_score: number | string | null;
  archetype_decay_score: number | string | null;
  cluster_decay_score: number | string | null;
  persistence_decay_score: number | string | null;
  aggregate_decay_score: number | string | null;
  freshness_state: CrossAssetSignalFreshnessState;
  stale_memory_flag: boolean;
  contradiction_flag: boolean;
  reason_codes: string[];
  created_at: string;
}

export interface CrossAssetFamilySignalDecaySummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  transition_state: string | null;
  dominant_sequence_class: string | null;
  archetype_key: string | null;
  cluster_state: string | null;
  persistence_state: string | null;
  family_rank: number | null;
  family_contribution: number | string | null;
  family_state_age_runs: number | null;
  family_memory_score: number | string | null;
  family_decay_score: number | string | null;
  family_freshness_state: CrossAssetSignalFreshnessState;
  stale_family_memory_flag: boolean;
  contradicted_family_flag: boolean;
  reason_codes: string[];
  created_at: string;
}

export interface CrossAssetStaleMemoryEventSummaryRow {
  workspace_id: string;
  watchlist_id: string | null;
  source_run_id: string | null;
  target_run_id: string;
  regime_key: string | null;
  prior_freshness_state: CrossAssetSignalFreshnessState | null;
  current_freshness_state: CrossAssetSignalFreshnessState;
  prior_state_signature: string | null;
  current_state_signature: string;
  prior_memory_score: number | string | null;
  current_memory_score: number | string | null;
  prior_aggregate_decay_score: number | string | null;
  current_aggregate_decay_score: number | string | null;
  event_type: CrossAssetStaleMemoryEventType;
  reason_codes: string[];
  created_at: string;
}

export interface RunCrossAssetSignalDecaySummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  regime_key: string | null;
  persistence_state: string | null;
  memory_score: number | string | null;
  freshness_state: CrossAssetSignalFreshnessState | null;
  aggregate_decay_score: number | string | null;
  stale_memory_flag: boolean;
  contradiction_flag: boolean;
  latest_stale_memory_event_type: CrossAssetStaleMemoryEventType | null;
  created_at: string;
}

export interface CrossAssetSignalDecayMetrics {
  signalDecaySummary: CrossAssetSignalDecaySummaryRow[];
  familySignalDecaySummary: CrossAssetFamilySignalDecaySummaryRow[];
  staleMemoryEventSummary: CrossAssetStaleMemoryEventSummaryRow[];
  runSignalDecaySummary: RunCrossAssetSignalDecaySummaryRow[];
}

// ── Phase 4.7B: Decay-Aware Attribution ─────────────────────────────────
export interface CrossAssetFamilyDecayAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  raw_family_net_contribution: number | string | null;
  weighted_family_net_contribution: number | string | null;
  regime_adjusted_family_contribution: number | string | null;
  timing_adjusted_family_contribution: number | string | null;
  transition_adjusted_family_contribution: number | string | null;
  archetype_adjusted_family_contribution: number | string | null;
  cluster_adjusted_family_contribution: number | string | null;
  persistence_adjusted_family_contribution: number | string | null;
  freshness_state: CrossAssetSignalFreshnessState;
  aggregate_decay_score: number | string | null;
  family_decay_score: number | string | null;
  memory_score: number | string | null;
  state_age_runs: number | null;
  stale_memory_flag: boolean;
  contradiction_flag: boolean;
  decay_weight: number | string | null;
  decay_bonus: number | string | null;
  decay_penalty: number | string | null;
  decay_adjusted_family_contribution: number | string | null;
  decay_family_rank: number | null;
  top_symbols: string[];
  reason_codes: string[];
  created_at: string;
}

export interface CrossAssetSymbolDecayAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  symbol: string;
  dependency_family: string;
  dependency_type: string | null;
  freshness_state: string;
  aggregate_decay_score: number | string | null;
  family_decay_score: number | string | null;
  memory_score: number | string | null;
  state_age_runs: number | null;
  stale_memory_flag: boolean;
  contradiction_flag: boolean;
  raw_symbol_score: number | string | null;
  weighted_symbol_score: number | string | null;
  regime_adjusted_symbol_score: number | string | null;
  timing_adjusted_symbol_score: number | string | null;
  transition_adjusted_symbol_score: number | string | null;
  archetype_adjusted_symbol_score: number | string | null;
  cluster_adjusted_symbol_score: number | string | null;
  persistence_adjusted_symbol_score: number | string | null;
  decay_weight: number | string | null;
  decay_adjusted_symbol_score: number | string | null;
  symbol_rank: number | null;
  reason_codes: string[];
  created_at: string;
}

export interface RunCrossAssetDecayAttributionSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  archetype_adjusted_cross_asset_contribution: number | string | null;
  cluster_adjusted_cross_asset_contribution: number | string | null;
  persistence_adjusted_cross_asset_contribution: number | string | null;
  decay_adjusted_cross_asset_contribution: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  timing_dominant_dependency_family: string | null;
  transition_dominant_dependency_family: string | null;
  archetype_dominant_dependency_family: string | null;
  cluster_dominant_dependency_family: string | null;
  persistence_dominant_dependency_family: string | null;
  decay_dominant_dependency_family: string | null;
  freshness_state: string | null;
  aggregate_decay_score: number | string | null;
  stale_memory_flag: boolean;
  contradiction_flag: boolean;
  created_at: string;
}

export interface CrossAssetDecayAttributionMetrics {
  familyDecayAttributionSummary: CrossAssetFamilyDecayAttributionSummaryRow[];
  symbolDecayAttributionSummary: CrossAssetSymbolDecayAttributionSummaryRow[];
  runDecayAttributionSummary: RunCrossAssetDecayAttributionSummaryRow[];
}

// ── Phase 4.7C: Decay-Aware Composite Refinement ────────────────────────
export type CrossAssetDecayIntegrationMode =
  | "decay_additive_guardrailed"
  | "fresh_confirmation_only"
  | "stale_suppression_only"
  | "contradiction_suppression_only";

export interface CrossAssetDecayCompositeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  base_signal_score: number | string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  archetype_adjusted_cross_asset_contribution: number | string | null;
  cluster_adjusted_cross_asset_contribution: number | string | null;
  persistence_adjusted_cross_asset_contribution: number | string | null;
  decay_adjusted_cross_asset_contribution: number | string | null;
  composite_pre_decay: number | string | null;
  decay_net_contribution: number | string | null;
  composite_post_decay: number | string | null;
  freshness_state: CrossAssetSignalFreshnessState;
  aggregate_decay_score: number | string | null;
  stale_memory_flag: boolean;
  contradiction_flag: boolean;
  integration_mode: CrossAssetDecayIntegrationMode;
  created_at: string;
}

export interface CrossAssetFamilyDecayCompositeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  freshness_state: string;
  aggregate_decay_score: number | string | null;
  family_decay_score: number | string | null;
  stale_memory_flag: boolean;
  contradiction_flag: boolean;
  decay_adjusted_family_contribution: number | string | null;
  integration_weight_applied: number | string | null;
  decay_integration_contribution: number | string | null;
  family_rank: number | null;
  top_symbols: string[];
  reason_codes: string[];
  created_at: string;
}

export interface RunCrossAssetDecayIntegrationSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  archetype_adjusted_cross_asset_contribution: number | string | null;
  cluster_adjusted_cross_asset_contribution: number | string | null;
  persistence_adjusted_cross_asset_contribution: number | string | null;
  decay_adjusted_cross_asset_contribution: number | string | null;
  decay_net_contribution: number | string | null;
  composite_pre_decay: number | string | null;
  composite_post_decay: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  timing_dominant_dependency_family: string | null;
  transition_dominant_dependency_family: string | null;
  archetype_dominant_dependency_family: string | null;
  cluster_dominant_dependency_family: string | null;
  persistence_dominant_dependency_family: string | null;
  decay_dominant_dependency_family: string | null;
  freshness_state: string | null;
  aggregate_decay_score: number | string | null;
  stale_memory_flag: boolean;
  contradiction_flag: boolean;
  created_at: string;
}

export interface CrossAssetDecayCompositeMetrics {
  decayCompositeSummary: CrossAssetDecayCompositeSummaryRow[];
  familyDecayCompositeSummary: CrossAssetFamilyDecayCompositeSummaryRow[];
  finalDecayIntegrationSummary: RunCrossAssetDecayIntegrationSummaryRow[];
}

// ── Phase 4.7D: Replay Validation for Decay-Aware Composite ─────────────
export type CrossAssetDecayReplayValidationState =
  | "validated"
  | "drift_detected"
  | "insufficient_source"
  | "insufficient_replay"
  | "context_mismatch"
  | "timing_mismatch"
  | "transition_mismatch"
  | "archetype_mismatch"
  | "cluster_mismatch"
  | "persistence_mismatch"
  | "decay_mismatch";

export interface CrossAssetDecayReplayValidationSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  source_context_snapshot_id: string | null;
  replay_context_snapshot_id: string | null;
  source_regime_key: string | null;
  replay_regime_key: string | null;
  source_dominant_timing_class: string | null;
  replay_dominant_timing_class: string | null;
  source_dominant_transition_state: string | null;
  replay_dominant_transition_state: string | null;
  source_dominant_sequence_class: string | null;
  replay_dominant_sequence_class: string | null;
  source_dominant_archetype_key: string | null;
  replay_dominant_archetype_key: string | null;
  source_cluster_state: string | null;
  replay_cluster_state: string | null;
  source_persistence_state: string | null;
  replay_persistence_state: string | null;
  source_memory_score: number | string | null;
  replay_memory_score: number | string | null;
  source_freshness_state: string | null;
  replay_freshness_state: string | null;
  source_aggregate_decay_score: number | string | null;
  replay_aggregate_decay_score: number | string | null;
  source_stale_memory_flag: boolean | null;
  replay_stale_memory_flag: boolean | null;
  source_contradiction_flag: boolean | null;
  replay_contradiction_flag: boolean | null;
  context_hash_match: boolean;
  regime_match: boolean;
  timing_class_match: boolean;
  transition_state_match: boolean;
  sequence_class_match: boolean;
  archetype_match: boolean;
  cluster_state_match: boolean;
  persistence_state_match: boolean;
  memory_score_match: boolean;
  freshness_state_match: boolean;
  aggregate_decay_score_match: boolean;
  stale_memory_flag_match: boolean;
  contradiction_flag_match: boolean;
  decay_attribution_match: boolean;
  decay_composite_match: boolean;
  decay_dominant_family_match: boolean;
  drift_reason_codes: string[];
  validation_state: CrossAssetDecayReplayValidationState;
  created_at: string;
}

export interface CrossAssetFamilyDecayReplayStabilitySummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  dependency_family: string;
  source_freshness_state: string | null;
  replay_freshness_state: string | null;
  source_aggregate_decay_score: number | string | null;
  replay_aggregate_decay_score: number | string | null;
  source_family_decay_score: number | string | null;
  replay_family_decay_score: number | string | null;
  source_stale_memory_flag: boolean | null;
  replay_stale_memory_flag: boolean | null;
  source_contradiction_flag: boolean | null;
  replay_contradiction_flag: boolean | null;
  source_decay_adjusted_contribution: number | string | null;
  replay_decay_adjusted_contribution: number | string | null;
  source_decay_integration_contribution: number | string | null;
  replay_decay_integration_contribution: number | string | null;
  decay_adjusted_delta: number | string | null;
  decay_integration_delta: number | string | null;
  freshness_state_match: boolean;
  aggregate_decay_score_match: boolean;
  family_decay_score_match: boolean;
  stale_memory_flag_match: boolean;
  contradiction_flag_match: boolean;
  decay_family_rank_match: boolean;
  decay_composite_family_rank_match: boolean;
  drift_reason_codes: string[];
  created_at: string;
}

export interface CrossAssetDecayReplayStabilityAggregateRow {
  workspace_id: string;
  validation_count: number;
  context_match_rate: number | string | null;
  regime_match_rate: number | string | null;
  timing_class_match_rate: number | string | null;
  transition_state_match_rate: number | string | null;
  sequence_class_match_rate: number | string | null;
  archetype_match_rate: number | string | null;
  cluster_state_match_rate: number | string | null;
  persistence_state_match_rate: number | string | null;
  memory_score_match_rate: number | string | null;
  freshness_state_match_rate: number | string | null;
  aggregate_decay_score_match_rate: number | string | null;
  stale_memory_flag_match_rate: number | string | null;
  contradiction_flag_match_rate: number | string | null;
  decay_attribution_match_rate: number | string | null;
  decay_composite_match_rate: number | string | null;
  decay_dominant_family_match_rate: number | string | null;
  drift_detected_count: number;
  latest_validated_at: string | null;
}

export interface CrossAssetDecayReplayValidationMetrics {
  decayReplayValidationSummary: CrossAssetDecayReplayValidationSummaryRow[];
  familyDecayReplayStabilitySummary: CrossAssetFamilyDecayReplayStabilitySummaryRow[];
  decayReplayStabilityAggregate: CrossAssetDecayReplayStabilityAggregateRow | null;
}

// ── Phase 4.8A: Cross-Layer Conflict and Agreement Diagnostics ──────────
export type CrossAssetLayerDirection = "supportive" | "suppressive" | "neutral" | "missing";

export type CrossAssetLayerConsensusState =
  | "aligned_supportive"
  | "aligned_suppressive"
  | "partial_agreement"
  | "conflicted"
  | "unreliable"
  | "insufficient_context";

export type CrossAssetLayerConflictEventType =
  | "agreement_strengthened"
  | "agreement_weakened"
  | "conflict_emerged"
  | "conflict_resolved"
  | "unreliable_stack_detected"
  | "insufficient_context";

export interface CrossAssetLayerAgreementSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dominant_timing_class: string | null;
  dominant_transition_state: string | null;
  dominant_sequence_class: string | null;
  dominant_archetype_key: string | null;
  cluster_state: string | null;
  persistence_state: string | null;
  freshness_state: string | null;
  timing_direction: CrossAssetLayerDirection | null;
  transition_direction: CrossAssetLayerDirection | null;
  archetype_direction: CrossAssetLayerDirection | null;
  cluster_direction: CrossAssetLayerDirection | null;
  persistence_direction: CrossAssetLayerDirection | null;
  decay_direction: CrossAssetLayerDirection | null;
  supportive_weight: number | string | null;
  suppressive_weight: number | string | null;
  neutral_weight: number | string | null;
  missing_weight: number | string | null;
  agreement_score: number | string | null;
  conflict_score: number | string | null;
  layer_consensus_state: CrossAssetLayerConsensusState;
  dominant_conflict_source: string | null;
  conflict_reason_codes: string[];
  created_at: string;
}

export interface CrossAssetFamilyLayerAgreementSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  transition_state: string | null;
  dominant_sequence_class: string | null;
  archetype_key: string | null;
  cluster_state: string | null;
  persistence_state: string | null;
  freshness_state: string | null;
  family_contribution: number | string | null;
  transition_direction: CrossAssetLayerDirection | null;
  archetype_direction: CrossAssetLayerDirection | null;
  cluster_direction: CrossAssetLayerDirection | null;
  persistence_direction: CrossAssetLayerDirection | null;
  decay_direction: CrossAssetLayerDirection | null;
  agreement_score: number | string | null;
  conflict_score: number | string | null;
  family_consensus_state: CrossAssetLayerConsensusState;
  dominant_conflict_source: string | null;
  family_rank: number | null;
  conflict_reason_codes: string[];
  created_at: string;
}

export interface CrossAssetLayerConflictEventSummaryRow {
  workspace_id: string;
  watchlist_id: string | null;
  source_run_id: string | null;
  target_run_id: string;
  prior_consensus_state: string | null;
  current_consensus_state: string;
  prior_dominant_conflict_source: string | null;
  current_dominant_conflict_source: string | null;
  prior_agreement_score: number | string | null;
  current_agreement_score: number | string | null;
  prior_conflict_score: number | string | null;
  current_conflict_score: number | string | null;
  event_type: CrossAssetLayerConflictEventType;
  reason_codes: string[];
  created_at: string;
}

export interface RunCrossAssetLayerConflictSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  layer_consensus_state: CrossAssetLayerConsensusState | null;
  agreement_score: number | string | null;
  conflict_score: number | string | null;
  dominant_conflict_source: string | null;
  freshness_state: string | null;
  persistence_state: string | null;
  cluster_state: string | null;
  latest_conflict_event_type: string | null;
  created_at: string;
}

export interface CrossAssetLayerConflictMetrics {
  layerAgreementSummary: CrossAssetLayerAgreementSummaryRow[];
  familyLayerAgreementSummary: CrossAssetFamilyLayerAgreementSummaryRow[];
  layerConflictEventSummary: CrossAssetLayerConflictEventSummaryRow[];
  runLayerConflictSummary: RunCrossAssetLayerConflictSummaryRow[];
}

// ── Phase 4.8B: Conflict-Aware Attribution ──────────────────────────────

export type CrossAssetFamilyConsensusState =
  | "aligned_supportive"
  | "aligned_suppressive"
  | "partial_agreement"
  | "conflicted"
  | "unreliable"
  | "insufficient_context";

export interface CrossAssetFamilyConflictAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  raw_family_net_contribution: number | string | null;
  weighted_family_net_contribution: number | string | null;
  regime_adjusted_family_contribution: number | string | null;
  timing_adjusted_family_contribution: number | string | null;
  transition_adjusted_family_contribution: number | string | null;
  archetype_adjusted_family_contribution: number | string | null;
  cluster_adjusted_family_contribution: number | string | null;
  persistence_adjusted_family_contribution: number | string | null;
  decay_adjusted_family_contribution: number | string | null;
  family_consensus_state: CrossAssetFamilyConsensusState;
  agreement_score: number | string | null;
  conflict_score: number | string | null;
  dominant_conflict_source: string | null;
  transition_direction: string | null;
  archetype_direction: string | null;
  cluster_direction: string | null;
  persistence_direction: string | null;
  decay_direction: string | null;
  conflict_weight: number | string | null;
  conflict_bonus: number | string | null;
  conflict_penalty: number | string | null;
  conflict_adjusted_family_contribution: number | string | null;
  conflict_family_rank: number | null;
  top_symbols: string[] | null;
  reason_codes: string[] | null;
  created_at: string;
}

export interface CrossAssetSymbolConflictAttributionSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  symbol: string;
  dependency_family: string;
  dependency_type: string | null;
  family_consensus_state: CrossAssetFamilyConsensusState;
  agreement_score: number | string | null;
  conflict_score: number | string | null;
  dominant_conflict_source: string | null;
  raw_symbol_score: number | string | null;
  weighted_symbol_score: number | string | null;
  regime_adjusted_symbol_score: number | string | null;
  timing_adjusted_symbol_score: number | string | null;
  transition_adjusted_symbol_score: number | string | null;
  archetype_adjusted_symbol_score: number | string | null;
  cluster_adjusted_symbol_score: number | string | null;
  persistence_adjusted_symbol_score: number | string | null;
  decay_adjusted_symbol_score: number | string | null;
  conflict_weight: number | string | null;
  conflict_adjusted_symbol_score: number | string | null;
  symbol_rank: number | null;
  reason_codes: string[] | null;
  created_at: string;
}

export interface RunCrossAssetConflictAttributionSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | string | null;
  weighted_cross_asset_net_contribution: number | string | null;
  regime_adjusted_cross_asset_contribution: number | string | null;
  timing_adjusted_cross_asset_contribution: number | string | null;
  transition_adjusted_cross_asset_contribution: number | string | null;
  archetype_adjusted_cross_asset_contribution: number | string | null;
  cluster_adjusted_cross_asset_contribution: number | string | null;
  persistence_adjusted_cross_asset_contribution: number | string | null;
  decay_adjusted_cross_asset_contribution: number | string | null;
  conflict_adjusted_cross_asset_contribution: number | string | null;
  dominant_dependency_family: string | null;
  weighted_dominant_dependency_family: string | null;
  regime_dominant_dependency_family: string | null;
  timing_dominant_dependency_family: string | null;
  transition_dominant_dependency_family: string | null;
  archetype_dominant_dependency_family: string | null;
  cluster_dominant_dependency_family: string | null;
  persistence_dominant_dependency_family: string | null;
  decay_dominant_dependency_family: string | null;
  conflict_dominant_dependency_family: string | null;
  layer_consensus_state: string | null;
  agreement_score: number | string | null;
  conflict_score: number | string | null;
  dominant_conflict_source: string | null;
  created_at: string;
}

export interface CrossAssetConflictAttributionMetrics {
  familyConflictAttributionSummary: CrossAssetFamilyConflictAttributionSummaryRow[];
  symbolConflictAttributionSummary: CrossAssetSymbolConflictAttributionSummaryRow[];
  runConflictAttributionSummary: RunCrossAssetConflictAttributionSummaryRow[];
}

// ── Phase 4.7A: Signal Decay & Stale-Memory Diagnostics ─────────────────
export type CrossAssetSignalFreshnessState =
  | "fresh"
  | "decaying"
  | "stale"
  | "contradicted"
  | "mixed"
  | "insufficient_history";

export type CrossAssetStaleMemoryEventType =
  | "memory_freshened"
  | "memory_decayed"
  | "memory_became_stale"
  | "memory_contradicted"
  | "memory_reconfirmed"
  | "insufficient_history";

export interface CrossAssetSignalDecaySummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  regime_key: string | null;
  dominant_timing_class: string | null;
  dominant_transition_state: string | null;
  dominant_sequence_class: string | null;
  dominant_archetype_key: string | null;
  cluster_state: string | null;
  persistence_state: string | null;
  current_state_signature: string;
  state_age_runs: number | null;
  memory_score: number | string | null;
  regime_decay_score: number | string | null;
  timing_decay_score: number | string | null;
  transition_decay_score: number | string | null;
  archetype_decay_score: number | string | null;
  cluster_decay_score: number | string | null;
  persistence_decay_score: number | string | null;
  aggregate_decay_score: number | string | null;
  freshness_state: CrossAssetSignalFreshnessState;
  stale_memory_flag: boolean;
  contradiction_flag: boolean;
  reason_codes: string[];
  created_at: string;
}

export interface CrossAssetFamilySignalDecaySummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  transition_state: string | null;
  dominant_sequence_class: string | null;
  archetype_key: string | null;
  cluster_state: string | null;
  persistence_state: string | null;
  family_rank: number | null;
  family_contribution: number | string | null;
  family_state_age_runs: number | null;
  family_memory_score: number | string | null;
  family_decay_score: number | string | null;
  family_freshness_state: CrossAssetSignalFreshnessState;
  stale_family_memory_flag: boolean;
  contradicted_family_flag: boolean;
  reason_codes: string[];
  created_at: string;
}

export interface CrossAssetStaleMemoryEventSummaryRow {
  workspace_id: string;
  watchlist_id: string | null;
  source_run_id: string | null;
  target_run_id: string;
  regime_key: string | null;
  prior_freshness_state: CrossAssetSignalFreshnessState | null;
  current_freshness_state: CrossAssetSignalFreshnessState;
  prior_state_signature: string | null;
  current_state_signature: string;
  prior_memory_score: number | string | null;
  current_memory_score: number | string | null;
  prior_aggregate_decay_score: number | string | null;
  current_aggregate_decay_score: number | string | null;
  event_type: CrossAssetStaleMemoryEventType;
  reason_codes: string[];
  created_at: string;
}

export interface RunCrossAssetSignalDecaySummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  regime_key: string | null;
  persistence_state: string | null;
  memory_score: number | string | null;
  freshness_state: CrossAssetSignalFreshnessState | null;
  aggregate_decay_score: number | string | null;
  stale_memory_flag: boolean;
  contradiction_flag: boolean;
  latest_stale_memory_event_type: CrossAssetStaleMemoryEventType | null;
  created_at: string;
}

export interface CrossAssetSignalDecayMetrics {
  signalDecaySummary: CrossAssetSignalDecaySummaryRow[];
  familySignalDecaySummary: CrossAssetFamilySignalDecaySummaryRow[];
  staleMemoryEventSummary: CrossAssetStaleMemoryEventSummaryRow[];
  runSignalDecaySummary: RunCrossAssetSignalDecaySummaryRow[];
}
