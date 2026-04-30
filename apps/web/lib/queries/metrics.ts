import { createServiceSupabaseClient } from "@/lib/supabase";

export interface QueueDepthRow {
  workspace_id: string;
  watchlist_id: string | null;
  queued_count: number;
  claimed_count: number;
  failed_count: number;
  dead_letter_count: number;
  newest_job_at: string | null;
  oldest_queued_at: string | null;
}

export interface QueueRuntimeRow {
  workspace_id: string;
  watchlist_id: string | null;
  total_runs: number;
  avg_runtime_seconds: number | null;
  last_completed_at: string | null;
  failed_runs: number;
  completed_runs: number;
}

export interface WorkerHeartbeatRow {
  worker_id: string;
  workspace_id: string | null;
  hostname: string | null;
  pid: number | null;
  status: string;
  capabilities: Record<string, unknown>;
  metadata: Record<string, unknown>;
  started_at: string;
  last_seen_at: string;
}

export interface WatchlistSlaRow {
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

export interface QueueGovernanceStateRow {
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

export interface StabilityFamilyRow {
  signal_family: string;
  family_score_current: number | null;
  family_score_baseline: number | null;
  family_delta_abs: number | null;
  family_delta_pct: number | null;
  instability_score: number;
  family_rank: number;
  metadata: Record<string, unknown>;
}

export interface StabilitySummaryRow {
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
  stability_classification: string;
  replay_runs_considered: number | null;
  mismatch_rate: number | null;
  avg_input_match_score: number | null;
  avg_composite_delta_abs: number | null;
  transitions_considered: number | null;
  conflicting_transition_count: number | null;
  abrupt_transition_count: number | null;
  family_rows: StabilityFamilyRow[];
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface VersionGovernanceRow {
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

export interface RegimeThresholdProfileRow {
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

export interface RegimeThresholdOverrideRow {
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

export interface ActiveRegimeThresholdRow {
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

export interface GovernanceThresholdApplicationRow {
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

export interface MacroSyncHealthRow {
  workspace_id: string;
  workspace_slug: string;
  provider_mode: string;
  last_completed_at: string | null;
  completed_runs: number;
  failed_runs: number;
  last_error: string | null;
}

export interface GovernanceAlertEventRow {
  id: string;
  workspace_id: string;
  watchlist_id: string | null;
  run_id: string | null;
  rule_name: string;
  event_type: "version_regression" | "replay_degradation" | "family_instability_spike" | "regime_instability_spike" | "regime_conflict_persistence" | "stability_classification_downgrade" | string;
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
  event_type: GovernanceAlertEventRow["event_type"];
  severity: GovernanceAlertEventRow["severity"];
  compute_version: string | null;
  signal_registry_version: string | null;
  model_version: string | null;
  latest_triggered_at: string;
  trigger_count: number;
}

export interface GovernanceAnomalyClusterRow {
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

export interface WatchlistAnomalySummaryRow {
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

export interface GovernanceDegradationStateRow {
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
  source_summary: Record<string, unknown>;
  resolution_summary: Record<string, unknown> | null;
  metadata: Record<string, unknown>;
  member_count: number;
  state_duration_hours: number;
}

export interface GovernanceRecoveryEventRow {
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

export interface GovernanceLifecycleRow {
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

export interface GovernanceCaseSummaryRow {
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

export interface GovernanceRoutingDecisionRow {
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

export interface GovernanceRoutingQualityRow {
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

export interface GovernanceReassignmentPressureRow {
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

export interface GovernanceOperatorEffectivenessRow {
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

export interface GovernanceTeamEffectivenessRow {
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

export interface GovernanceRoutingRecommendationInputRow {
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

export interface GovernanceRoutingRecommendationRow {
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
  latest_reviewed_at?: string | null;
  latest_review_status?: "approved" | "rejected" | "deferred" | null;
  review_count?: number;
  any_applied_immediately?: boolean;
  application_count?: number;
  latest_applied_at?: string | null;
  latest_applied_user?: string | null;
  latest_applied_team?: string | null;
}

export interface GovernanceThresholdPerformanceSummaryRow {
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

export interface GovernanceThresholdLearningRecommendationRow {
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

export interface GovernanceThresholdReviewSummaryRow {
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
  status: string;
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

export interface GovernanceThresholdAutopromotionSummaryRow {
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
  execution_mode: string;
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

export interface GovernanceRoutingAutopromotionPolicyRow {
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

export interface GovernanceRoutingAutopromotionSummaryRow {
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

export interface GovernanceRoutingAutopromotionRollbackCandidateRow {
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

export interface GovernanceOperatorCaseMetricsRow {
  workspace_id: string;
  operator_id: string;
  assigned_team: string | null;
  open_case_count: number;
  severe_open_case_count: number;
  avg_open_age_hours: number | null;
  reopened_open_case_count: number;
  stale_open_case_count: number;
}

export interface GovernanceTeamCaseMetricsRow {
  workspace_id: string;
  assigned_team: string;
  open_case_count: number;
  severe_open_case_count: number;
  avg_open_age_hours: number | null;
  reopened_open_case_count: number;
  stale_open_case_count: number;
}

export interface GovernanceCaseAgingSummaryRow {
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

export interface GovernanceCaseSlaSummaryRow {
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

export interface GovernanceStaleCaseSummaryRow extends GovernanceCaseSlaSummaryRow {
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

export interface GovernanceEscalationSummaryRow {
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

export interface GovernanceEscalationEventRow {
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

export interface GovernanceIncidentAnalyticsSnapshotRow {
  id: string;
  workspace_id: string;
  snapshot_date: string;
  open_case_count: number;
  acknowledged_case_count: number;
  resolved_case_count: number;
  reopened_case_count: number;
  recurring_case_count: number;
  high_severity_open_count: number;
  escalated_case_count: number;
  stale_case_count: number;
  mean_ack_hours: number | null;
  mean_resolve_hours: number | null;
  created_at: string;
}

export interface GovernanceIncidentAnalyticsSummaryRow {
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

export interface GovernanceRootCauseTrendRow {
  workspace_id: string;
  root_cause_code: string;
  case_count: number;
  reopened_count: number;
  recurring_count: number;
  severe_count: number;
  avg_case_age_hours: number | null;
}

export interface GovernanceRecurrenceBurdenRow {
  workspace_id: string;
  watchlist_id: string | null;
  recurring_case_count: number;
  max_repeat_count: number | null;
  recurrence_group_count: number;
  reopened_case_count: number;
}

export interface GovernanceEscalationEffectivenessAnalyticsRow {
  workspace_id: string;
  escalated_case_count: number;
  escalated_resolved_count: number;
  escalated_reopened_count: number;
  escalation_resolution_rate: number;
  escalation_reopen_rate: number;
}

export interface GovernanceThresholdPromotionImpactSummaryRow {
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

export interface GovernanceRoutingPromotionImpactSummaryRow {
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

export interface GovernancePromotionRollbackRiskSummaryRow {
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

export interface GovernanceManagerOverviewSummaryRow {
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

export interface GovernanceChronicWatchlistSummaryRow {
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

export interface GovernanceOperatorTeamComparisonSummaryRow {
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

export interface GovernancePromotionHealthOverviewRow {
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

export interface GovernanceOperatingRiskSummaryRow {
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

export interface GovernanceOperatorPerformanceSummaryRow {
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

export interface GovernanceTeamPerformanceSummaryRow {
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

export interface GovernanceCaseMixSummaryRow {
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

export interface GovernancePerformanceSnapshotRow {
  id: string;
  workspace_id: string;
  snapshot_at: string;
  operator_count: number;
  team_count: number;
  operator_case_mix_count: number;
  team_case_mix_count: number;
  metadata: Record<string, unknown>;
  created_at: string;
}

async function getWorkspaceId(workspaceSlug: string): Promise<string> {
  const supabase = createServiceSupabaseClient();
  type WsResult = { data: { id: string } | null; error: { message: string } | null };
  const result = await supabase
    .from("workspaces").select("id").eq("slug", workspaceSlug).single() as unknown as WsResult;
  if (result.error || !result.data) throw new Error(`Workspace not found: ${workspaceSlug}`);
  return result.data.id;
}

export async function getQueueMetrics(workspaceSlug: string) {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [depth, runtime] = await Promise.all([
    supabase
      .from("queue_depth_by_watchlist")
      .select("*")
      .eq("workspace_id", workspaceId),
    supabase
      .from("queue_runtime_summary")
      .select("*")
      .eq("workspace_id", workspaceId),
  ]);

  if (depth.error) throw new Error(`Queue depth error: ${depth.error.message}`);
  if (runtime.error) throw new Error(`Runtime error: ${runtime.error.message}`);

  return {
    depth: (depth.data ?? []) as QueueDepthRow[],
    runtime: (runtime.data ?? []) as QueueRuntimeRow[],
  };
}

export async function getWorkerHeartbeats(workspaceSlug: string): Promise<WorkerHeartbeatRow[]> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const { data, error } = await supabase
    .from("worker_heartbeats")
    .select("*")
    .or(`workspace_id.is.null,workspace_id.eq.${workspaceId}`)
    .order("last_seen_at", { ascending: false });

  if (error) throw new Error(`Heartbeats error: ${error.message}`);
  return (data ?? []) as WorkerHeartbeatRow[];
}

export async function getWatchlistSla(workspaceSlug: string): Promise<WatchlistSlaRow[]> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const { data, error } = await supabase
    .from("watchlist_sla_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("watchlist_slug", { ascending: true, nullsFirst: false });

  if (error) throw new Error(`SLA metrics error: ${error.message}`);
  return (data ?? []) as WatchlistSlaRow[];
}

export async function getQueueGovernanceState(workspaceSlug: string): Promise<QueueGovernanceStateRow[]> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const { data, error } = await supabase
    .from("queue_governance_state")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("watchlist_slug", { ascending: true, nullsFirst: false });

  if (error) throw new Error(`Queue governance state error: ${error.message}`);
  return (data ?? []) as QueueGovernanceStateRow[];
}

export async function getLatestStabilitySummary(workspaceSlug: string): Promise<StabilitySummaryRow[]> {
  const supabase = createServiceSupabaseClient();
  const { data, error } = await supabase
    .from("latest_stability_summary")
    .select("*")
    .eq("workspace_slug", workspaceSlug)
    .order("created_at", { ascending: false });

  if (error) throw new Error(`Latest stability summary error: ${error.message}`);
  return (data ?? []) as StabilitySummaryRow[];
}

export async function getVersionGovernance(workspaceSlug: string): Promise<VersionGovernanceRow[]> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const { data, error } = await supabase
    .from("version_health_rankings")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("health_rank", { ascending: true })
    .order("governance_health_score", { ascending: false });

  if (error) throw new Error(`Version governance error: ${error.message}`);
  return (data ?? []) as VersionGovernanceRow[];
}

export async function getRegimeThresholdMetrics(workspaceSlug: string): Promise<{
  profiles: RegimeThresholdProfileRow[];
  overrides: RegimeThresholdOverrideRow[];
  active: ActiveRegimeThresholdRow[];
  applications: GovernanceThresholdApplicationRow[];
  macroSyncHealth: MacroSyncHealthRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [profiles, overrides, active, applications, macroSyncHealth] = await Promise.all([
    supabase
      .from("governance_threshold_profiles")
      .select("*")
      .or(`workspace_id.is.null,workspace_id.eq.${workspaceId}`)
      .order("is_default", { ascending: false })
      .order("profile_name", { ascending: true }),
    supabase
      .from("regime_threshold_overrides")
      .select("*")
      .or(`workspace_id.is.null,workspace_id.eq.${workspaceId}`)
      .order("regime", { ascending: true }),
    supabase
      .from("active_regime_thresholds")
      .select("*")
      .or(`workspace_id.is.null,workspace_id.eq.${workspaceId}`)
      .order("workspace_id", { ascending: false, nullsFirst: false })
      .order("regime", { ascending: true }),
    supabase
      .from("governance_threshold_application_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("created_at", { ascending: false })
      .limit(25),
    supabase
      .from("macro_sync_health")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("last_completed_at", { ascending: false, nullsFirst: false }),
  ]);

  if (profiles.error) throw new Error(`Threshold profiles error: ${profiles.error.message}`);
  if (overrides.error) throw new Error(`Threshold overrides error: ${overrides.error.message}`);
  if (active.error) throw new Error(`Active threshold error: ${active.error.message}`);
  if (applications.error) throw new Error(`Threshold applications error: ${applications.error.message}`);
  if (macroSyncHealth.error) throw new Error(`Macro sync health error: ${macroSyncHealth.error.message}`);

  return {
    profiles: (profiles.data ?? []) as RegimeThresholdProfileRow[],
    overrides: (overrides.data ?? []) as RegimeThresholdOverrideRow[],
    active: (active.data ?? []) as ActiveRegimeThresholdRow[],
    applications: (applications.data ?? []) as GovernanceThresholdApplicationRow[],
    macroSyncHealth: (macroSyncHealth.data ?? []) as MacroSyncHealthRow[],
  };
}

export async function getGovernanceAlerts(workspaceSlug: string): Promise<{
  events: GovernanceAlertEventRow[];
  state: GovernanceAlertStateRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [events, state] = await Promise.all([
    supabase
      .from("governance_alert_events")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("created_at", { ascending: false })
      .limit(25),
    supabase
      .from("governance_alert_state")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("latest_triggered_at", { ascending: false }),
  ]);

  if (events.error) throw new Error(`Governance alert events error: ${events.error.message}`);
  if (state.error) throw new Error(`Governance alert state error: ${state.error.message}`);

  return {
    events: (events.data ?? []) as GovernanceAlertEventRow[],
    state: (state.data ?? []) as GovernanceAlertStateRow[],
  };
}

export async function getGovernanceAnomalyClusters(workspaceSlug: string): Promise<{
  clusters: GovernanceAnomalyClusterRow[];
  summary: WatchlistAnomalySummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [clusters, summary] = await Promise.all([
    supabase
      .from("governance_anomaly_cluster_state")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("last_seen_at", { ascending: false })
      .limit(25),
    supabase
      .from("watchlist_anomaly_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("high_open_cluster_count", { ascending: false })
      .order("open_cluster_count", { ascending: false }),
  ]);

  if (clusters.error) throw new Error(`Governance anomaly clusters error: ${clusters.error.message}`);
  if (summary.error) throw new Error(`Watchlist anomaly summary error: ${summary.error.message}`);

  return {
    clusters: (clusters.data ?? []) as GovernanceAnomalyClusterRow[],
    summary: (summary.data ?? []) as WatchlistAnomalySummaryRow[],
  };
}

export async function getGovernanceDegradationMetrics(workspaceSlug: string): Promise<{
  activeStates: GovernanceDegradationStateRow[];
  resolvedStates: GovernanceDegradationStateRow[];
  recoveries: GovernanceRecoveryEventRow[];
  openStateCount: number;
  escalatedStateCount: number;
  recentRecoveryCount: number;
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [states, recoveries] = await Promise.all([
    supabase
      .from("governance_degradation_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("last_seen_at", { ascending: false })
      .limit(50),
    supabase
      .from("governance_recovery_event_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("recovered_at", { ascending: false })
      .limit(25),
  ]);

  if (states.error) throw new Error(`Governance degradation states error: ${states.error.message}`);
  if (recoveries.error) throw new Error(`Governance recovery events error: ${recoveries.error.message}`);

  const stateRows = (states.data ?? []) as GovernanceDegradationStateRow[];
  const activeStates = stateRows.filter((row) => row.state_status === "active" || row.state_status === "escalated");
  const resolvedStates = stateRows.filter((row) => row.state_status === "resolved");

  return {
    activeStates,
    resolvedStates,
    recoveries: (recoveries.data ?? []) as GovernanceRecoveryEventRow[],
    openStateCount: activeStates.filter((row) => row.state_status === "active").length,
    escalatedStateCount: activeStates.filter((row) => row.state_status === "escalated").length,
    recentRecoveryCount: (recoveries.data ?? []).length,
  };
}

export async function getGovernanceLifecycleMetrics(workspaceSlug: string): Promise<{
  activeStates: GovernanceLifecycleRow[];
  acknowledgedStates: GovernanceLifecycleRow[];
  resolvedStates: GovernanceLifecycleRow[];
  recoveries: GovernanceRecoveryEventRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [lifecycle, recoveries] = await Promise.all([
    supabase
      .from("governance_lifecycle_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("last_seen_at", { ascending: false })
      .limit(50),
    supabase
      .from("governance_recovery_event_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("recovered_at", { ascending: false })
      .limit(25),
  ]);

  if (lifecycle.error) throw new Error(`Governance lifecycle error: ${lifecycle.error.message}`);
  if (recoveries.error) throw new Error(`Governance lifecycle recoveries error: ${recoveries.error.message}`);

  const rows = (lifecycle.data ?? []) as GovernanceLifecycleRow[];
  return {
    activeStates: rows.filter((row) => row.state_status !== "resolved" && !row.acknowledged_at),
    acknowledgedStates: rows.filter((row) => row.state_status !== "resolved" && Boolean(row.acknowledged_at)),
    resolvedStates: rows.filter((row) => row.state_status === "resolved"),
    recoveries: (recoveries.data ?? []) as GovernanceRecoveryEventRow[],
  };
}

export async function getGovernanceCaseMetrics(workspaceSlug: string): Promise<{
  activeCases: GovernanceCaseSummaryRow[];
  recentCases: GovernanceCaseSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const { data, error } = await supabase
    .from("governance_case_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("opened_at", { ascending: false })
    .limit(50);

  if (error) throw new Error(`Governance case summary error: ${error.message}`);

  const rows = (data ?? []) as GovernanceCaseSummaryRow[];
  return {
    activeCases: rows.filter((row) => ["open", "acknowledged", "in_progress"].includes(row.status)),
    recentCases: rows.slice(0, 25),
  };
}

export async function getGovernanceRoutingMetrics(workspaceSlug: string): Promise<{
  routingDecisions: GovernanceRoutingDecisionRow[];
  operatorMetrics: GovernanceOperatorCaseMetricsRow[];
  teamMetrics: GovernanceTeamCaseMetricsRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [routingDecisions, operatorMetrics, teamMetrics] = await Promise.all([
    supabase
      .from("governance_routing_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("created_at", { ascending: false })
      .limit(25),
    supabase
      .from("governance_operator_case_metrics")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("severe_open_case_count", { ascending: false })
      .order("open_case_count", { ascending: false }),
    supabase
      .from("governance_team_case_metrics")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("severe_open_case_count", { ascending: false })
      .order("open_case_count", { ascending: false }),
  ]);

  if (routingDecisions.error) throw new Error(`Governance routing decisions error: ${routingDecisions.error.message}`);
  if (operatorMetrics.error) throw new Error(`Governance operator metrics error: ${operatorMetrics.error.message}`);
  if (teamMetrics.error) throw new Error(`Governance team metrics error: ${teamMetrics.error.message}`);

  return {
    routingDecisions: (routingDecisions.data ?? []) as GovernanceRoutingDecisionRow[],
    operatorMetrics: (operatorMetrics.data ?? []) as GovernanceOperatorCaseMetricsRow[],
    teamMetrics: (teamMetrics.data ?? []) as GovernanceTeamCaseMetricsRow[],
  };
}

export async function getGovernanceRoutingQualityMetrics(workspaceSlug: string): Promise<{
  routingQuality: GovernanceRoutingQualityRow[];
  reassignmentPressure: GovernanceReassignmentPressureRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [routingQuality, reassignmentPressure] = await Promise.all([
    supabase
      .from("governance_routing_quality_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("acceptance_rate", { ascending: true })
      .order("feedback_count", { ascending: false }),
    supabase
      .from("governance_reassignment_pressure_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("reassignment_count", { ascending: false }),
  ]);

  if (routingQuality.error) throw new Error(`Governance routing quality error: ${routingQuality.error.message}`);
  if (reassignmentPressure.error) throw new Error(`Governance reassignment pressure error: ${reassignmentPressure.error.message}`);

  return {
    routingQuality: (routingQuality.data ?? []) as GovernanceRoutingQualityRow[],
    reassignmentPressure: (reassignmentPressure.data ?? []) as GovernanceReassignmentPressureRow[],
  };
}

export async function getGovernanceRoutingEffectivenessMetrics(workspaceSlug: string): Promise<{
  operators: GovernanceOperatorEffectivenessRow[];
  teams: GovernanceTeamEffectivenessRow[];
  recommendationInputs: GovernanceRoutingRecommendationInputRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [operators, teams, recommendationInputs] = await Promise.all([
    supabase
      .from("governance_operator_effectiveness_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("resolution_rate", { ascending: false, nullsFirst: false })
      .order("assignments", { ascending: false }),
    supabase
      .from("governance_team_effectiveness_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("resolution_rate", { ascending: false, nullsFirst: false })
      .order("assignments", { ascending: false }),
    supabase
      .from("governance_routing_recommendation_inputs")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("resolved_count", { ascending: false })
      .order("avg_resolve_hours", { ascending: true, nullsFirst: false }),
  ]);

  if (operators.error) throw new Error(`Governance operator effectiveness error: ${operators.error.message}`);
  if (teams.error) throw new Error(`Governance team effectiveness error: ${teams.error.message}`);
  if (recommendationInputs.error) throw new Error(`Governance routing recommendation inputs error: ${recommendationInputs.error.message}`);

  return {
    operators: (operators.data ?? []) as GovernanceOperatorEffectivenessRow[],
    teams: (teams.data ?? []) as GovernanceTeamEffectivenessRow[],
    recommendationInputs: (recommendationInputs.data ?? []) as GovernanceRoutingRecommendationInputRow[],
  };
}

export async function getGovernanceRoutingRecommendations(
  workspaceSlug: string,
  caseId?: string,
): Promise<{ recommendations: GovernanceRoutingRecommendationRow[] }> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let query = supabase
    .from("governance_routing_recommendation_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(caseId ? 10 : 25);

  if (caseId) {
    query = query.eq("case_id", caseId);
  }

  const [recommendations, reviewSummaries, applicationSummaries] = await Promise.all([
    query,
    supabase
      .from("governance_routing_review_summary")
      .select("*")
      .eq("workspace_id", workspaceId),
    supabase
      .from("governance_routing_application_summary")
      .select("*")
      .eq("workspace_id", workspaceId),
  ]);

  if (recommendations.error) {
    throw new Error(`Governance routing recommendations error: ${recommendations.error.message}`);
  }
  if (reviewSummaries.error) {
    throw new Error(`Governance routing review summaries error: ${reviewSummaries.error.message}`);
  }
  if (applicationSummaries.error) {
    throw new Error(`Governance routing application summaries error: ${applicationSummaries.error.message}`);
  }

  const reviewByRecommendation = new Map(
    (reviewSummaries.data ?? []).map((row) => [row.recommendation_id, row] as const),
  );
  const applicationByRecommendation = new Map(
    (applicationSummaries.data ?? []).map((row) => [`${row.recommendation_id}:${row.case_id}`, row] as const),
  );

  return {
    recommendations: (recommendations.data ?? []).map((row) => {
      const review = reviewByRecommendation.get(row.id);
      const application = applicationByRecommendation.get(`${row.id}:${row.case_id}`);
      return {
        ...(row as GovernanceRoutingRecommendationRow),
        latest_reviewed_at: review?.latest_reviewed_at ?? null,
        latest_review_status: review?.latest_review_status ?? null,
        review_count: review?.review_count ?? 0,
        any_applied_immediately: review?.any_applied_immediately ?? false,
        application_count: application?.application_count ?? 0,
        latest_applied_at: application?.latest_applied_at ?? null,
        latest_applied_user: application?.latest_applied_user ?? null,
        latest_applied_team: application?.latest_applied_team ?? null,
      };
    }),
  };
}

export async function getGovernanceThresholdLearningMetrics(workspaceSlug: string): Promise<{
  performance: GovernanceThresholdPerformanceSummaryRow[];
  recommendations: GovernanceThresholdLearningRecommendationRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [performance, recommendations] = await Promise.all([
    supabase
      .from("governance_threshold_performance_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("avg_noise_score", { ascending: false })
      .order("feedback_rows", { ascending: false }),
    supabase
      .from("governance_threshold_learning_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("created_at", { ascending: false })
      .limit(25),
  ]);

  if (performance.error) throw new Error(`Governance threshold performance error: ${performance.error.message}`);
  if (recommendations.error) throw new Error(`Governance threshold learning error: ${recommendations.error.message}`);

  return {
    performance: (performance.data ?? []) as GovernanceThresholdPerformanceSummaryRow[],
    recommendations: (recommendations.data ?? []) as GovernanceThresholdLearningRecommendationRow[],
  };
}

export async function getGovernanceThresholdLearningReviewMetrics(workspaceSlug: string): Promise<{
  reviewSummary: GovernanceThresholdReviewSummaryRow[];
  autopromotionSummary: GovernanceThresholdAutopromotionSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [reviewSummary, autopromotionSummary] = await Promise.all([
    supabase
      .from("governance_threshold_review_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("updated_at", { ascending: false })
      .limit(25),
    supabase
      .from("governance_threshold_autopromotion_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("created_at", { ascending: false })
      .limit(25),
  ]);

  if (reviewSummary.error) throw new Error(`Governance threshold review error: ${reviewSummary.error.message}`);
  if (autopromotionSummary.error) throw new Error(`Governance threshold autopromotion error: ${autopromotionSummary.error.message}`);

  return {
    reviewSummary: (reviewSummary.data ?? []) as GovernanceThresholdReviewSummaryRow[],
    autopromotionSummary: (autopromotionSummary.data ?? []) as GovernanceThresholdAutopromotionSummaryRow[],
  };
}

export async function getGovernanceRoutingAutopromotionMetrics(workspaceSlug: string): Promise<{
  activePolicies: GovernanceRoutingAutopromotionPolicyRow[];
  recentExecutions: GovernanceRoutingAutopromotionSummaryRow[];
  rollbackCandidates: GovernanceRoutingAutopromotionRollbackCandidateRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [activePolicies, recentExecutions, rollbackCandidates] = await Promise.all([
    supabase
      .from("governance_routing_autopromotion_policies")
      .select("*")
      .eq("workspace_id", workspaceId)
      .eq("enabled", true)
      .order("updated_at", { ascending: false }),
    supabase
      .from("governance_routing_autopromotion_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("created_at", { ascending: false })
      .limit(25),
    supabase
      .from("governance_routing_autopromotion_rollback_candidates")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("created_at", { ascending: false })
      .limit(25),
  ]);

  if (activePolicies.error) {
    throw new Error(`Governance routing autopromotion policies error: ${activePolicies.error.message}`);
  }
  if (recentExecutions.error) {
    throw new Error(`Governance routing autopromotion summary error: ${recentExecutions.error.message}`);
  }
  if (rollbackCandidates.error) {
    throw new Error(`Governance routing rollback candidates error: ${rollbackCandidates.error.message}`);
  }

  return {
    activePolicies: (activePolicies.data ?? []) as GovernanceRoutingAutopromotionPolicyRow[],
    recentExecutions: (recentExecutions.data ?? []) as GovernanceRoutingAutopromotionSummaryRow[],
    rollbackCandidates: (rollbackCandidates.data ?? []) as GovernanceRoutingAutopromotionRollbackCandidateRow[],
  };
}

export async function getGovernanceWorkloadMetrics(workspaceSlug: string): Promise<{
  aging: GovernanceCaseAgingSummaryRow[];
  stale: GovernanceStaleCaseSummaryRow[];
  operatorPressure: GovernanceOperatorPressureRow[];
  teamPressure: GovernanceTeamPressureRow[];
  sla: GovernanceCaseSlaSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [aging, stale, operatorPressure, teamPressure, sla] = await Promise.all([
    supabase
      .from("governance_case_aging_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("age_minutes", { ascending: false })
      .limit(50),
    supabase
      .from("governance_stale_case_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("age_minutes", { ascending: false })
      .limit(50),
    supabase
      .from("governance_operator_workload_pressure")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("severity_weighted_load", { ascending: false })
      .order("open_case_count", { ascending: false }),
    supabase
      .from("governance_team_workload_pressure")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("severity_weighted_load", { ascending: false })
      .order("open_case_count", { ascending: false }),
    supabase
      .from("governance_case_sla_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("evaluated_at", { ascending: false, nullsFirst: false })
      .limit(50),
  ]);

  if (aging.error) throw new Error(`Governance case aging error: ${aging.error.message}`);
  if (stale.error) throw new Error(`Governance stale case error: ${stale.error.message}`);
  if (operatorPressure.error) throw new Error(`Governance operator pressure error: ${operatorPressure.error.message}`);
  if (teamPressure.error) throw new Error(`Governance team pressure error: ${teamPressure.error.message}`);
  if (sla.error) throw new Error(`Governance SLA summary error: ${sla.error.message}`);

  return {
    aging: (aging.data ?? []) as GovernanceCaseAgingSummaryRow[],
    stale: (stale.data ?? []) as GovernanceStaleCaseSummaryRow[],
    operatorPressure: (operatorPressure.data ?? []) as GovernanceOperatorPressureRow[],
    teamPressure: (teamPressure.data ?? []) as GovernanceTeamPressureRow[],
    sla: (sla.data ?? []) as GovernanceCaseSlaSummaryRow[],
  };
}

export async function getGovernanceEscalationMetrics(workspaceSlug: string): Promise<{
  activeEscalations: GovernanceEscalationSummaryRow[];
  recentEvents: GovernanceEscalationEventRow[];
  candidateCases: GovernanceStaleCaseSummaryRow[];
  activeEscalationCount: number;
  repeatedEscalationCount: number;
  candidateCount: number;
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [summary, events, stale] = await Promise.all([
    supabase
      .from("governance_escalation_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("escalated_at", { ascending: false })
      .limit(50),
    supabase
      .from("governance_escalation_events")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("created_at", { ascending: false })
      .limit(50),
    supabase
      .from("governance_stale_case_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("age_minutes", { ascending: false })
      .limit(50),
  ]);

  if (summary.error) throw new Error(`Governance escalation summary error: ${summary.error.message}`);
  if (events.error) throw new Error(`Governance escalation events error: ${events.error.message}`);
  if (stale.error) throw new Error(`Governance escalation candidate error: ${stale.error.message}`);

  const activeEscalations = ((summary.data ?? []) as GovernanceEscalationSummaryRow[])
    .filter((row) => row.status === "active");
  const activeCaseIds = new Set(activeEscalations.map((row) => row.case_id));
  const candidateCases = ((stale.data ?? []) as GovernanceStaleCaseSummaryRow[])
    .filter((row) => !activeCaseIds.has(row.case_id))
    .slice(0, 15);
  const recentEvents = (events.data ?? []) as GovernanceEscalationEventRow[];

  return {
    activeEscalations,
    recentEvents,
    candidateCases,
    activeEscalationCount: activeEscalations.length,
    repeatedEscalationCount: activeEscalations.filter((row) => row.repeated_count > 1).length,
    candidateCount: candidateCases.length,
  };
}

export async function getGovernanceIncidentAnalyticsMetrics(workspaceSlug: string): Promise<{
  summary: GovernanceIncidentAnalyticsSummaryRow | null;
  rootCauseTrends: GovernanceRootCauseTrendRow[];
  recurrenceBurden: GovernanceRecurrenceBurdenRow[];
  escalationEffectiveness: GovernanceEscalationEffectivenessAnalyticsRow | null;
  snapshots: GovernanceIncidentAnalyticsSnapshotRow[];
  thresholdPromotionImpact: GovernanceThresholdPromotionImpactSummaryRow[];
  routingPromotionImpact: GovernanceRoutingPromotionImpactSummaryRow[];
  rollbackRisk: GovernancePromotionRollbackRiskSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [
    summary,
    rootCauseTrends,
    recurrenceBurden,
    escalationEffectiveness,
    snapshots,
    thresholdPromotionImpact,
    routingPromotionImpact,
    rollbackRisk,
  ] = await Promise.all([
    supabase
      .from("governance_incident_analytics_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .maybeSingle(),
    supabase
      .from("governance_root_cause_trend_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("case_count", { ascending: false })
      .order("reopened_count", { ascending: false }),
    supabase
      .from("governance_recurrence_burden_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("recurring_case_count", { ascending: false })
      .order("reopened_case_count", { ascending: false }),
    supabase
      .from("governance_escalation_effectiveness_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .maybeSingle(),
    supabase
      .from("governance_incident_analytics_snapshots")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("snapshot_date", { ascending: false })
      .limit(30),
    supabase
      .from("governance_threshold_promotion_impact_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("created_at", { ascending: false })
      .limit(25),
    supabase
      .from("governance_routing_promotion_impact_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("created_at", { ascending: false })
      .limit(25),
    supabase
      .from("governance_promotion_rollback_risk_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("rollback_risk_score", { ascending: false, nullsFirst: false })
      .order("created_at", { ascending: false })
      .limit(25),
  ]);

  if (summary.error) {
    throw new Error(`Governance incident analytics summary error: ${summary.error.message}`);
  }
  if (rootCauseTrends.error) {
    throw new Error(`Governance root cause trend error: ${rootCauseTrends.error.message}`);
  }
  if (recurrenceBurden.error) {
    throw new Error(`Governance recurrence burden error: ${recurrenceBurden.error.message}`);
  }
  if (escalationEffectiveness.error) {
    throw new Error(`Governance escalation effectiveness error: ${escalationEffectiveness.error.message}`);
  }
  if (snapshots.error) {
    throw new Error(`Governance incident analytics snapshots error: ${snapshots.error.message}`);
  }
  if (thresholdPromotionImpact.error) {
    throw new Error(`Governance threshold promotion impact error: ${thresholdPromotionImpact.error.message}`);
  }
  if (routingPromotionImpact.error) {
    throw new Error(`Governance routing promotion impact error: ${routingPromotionImpact.error.message}`);
  }
  if (rollbackRisk.error) {
    throw new Error(`Governance promotion rollback risk error: ${rollbackRisk.error.message}`);
  }

  return {
    summary: (summary.data ?? null) as GovernanceIncidentAnalyticsSummaryRow | null,
    rootCauseTrends: (rootCauseTrends.data ?? []) as GovernanceRootCauseTrendRow[],
    recurrenceBurden: (recurrenceBurden.data ?? []) as GovernanceRecurrenceBurdenRow[],
    escalationEffectiveness: (escalationEffectiveness.data ?? null) as GovernanceEscalationEffectivenessAnalyticsRow | null,
    snapshots: (snapshots.data ?? []) as GovernanceIncidentAnalyticsSnapshotRow[],
    thresholdPromotionImpact: (thresholdPromotionImpact.data ?? []) as GovernanceThresholdPromotionImpactSummaryRow[],
    routingPromotionImpact: (routingPromotionImpact.data ?? []) as GovernanceRoutingPromotionImpactSummaryRow[],
    rollbackRisk: (rollbackRisk.data ?? []) as GovernancePromotionRollbackRiskSummaryRow[],
  };
}

export async function getGovernanceOperatorPerformanceMetrics(workspaceSlug: string): Promise<{
  operatorSummary: GovernanceOperatorPerformanceSummaryRow[];
  teamSummary: GovernanceTeamPerformanceSummaryRow[];
  operatorCaseMix: GovernanceCaseMixSummaryRow[];
  teamCaseMix: GovernanceCaseMixSummaryRow[];
  snapshots: GovernancePerformanceSnapshotRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [operatorSummary, teamSummary, operatorCaseMix, teamCaseMix, snapshots] = await Promise.all([
    supabase
      .from("governance_operator_performance_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("resolution_quality_proxy", { ascending: false })
      .order("assigned_case_count", { ascending: false }),
    supabase
      .from("governance_team_performance_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("resolution_quality_proxy", { ascending: false })
      .order("assigned_case_count", { ascending: false }),
    supabase
      .from("governance_operator_case_mix_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("case_count", { ascending: false })
      .order("chronic_case_count", { ascending: false }),
    supabase
      .from("governance_team_case_mix_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("case_count", { ascending: false })
      .order("chronic_case_count", { ascending: false }),
    supabase
      .from("governance_performance_snapshots")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("snapshot_at", { ascending: false })
      .limit(30),
  ]);

  if (operatorSummary.error) {
    throw new Error(`Governance operator performance error: ${operatorSummary.error.message}`);
  }
  if (teamSummary.error) {
    throw new Error(`Governance team performance error: ${teamSummary.error.message}`);
  }
  if (operatorCaseMix.error) {
    throw new Error(`Governance operator case mix error: ${operatorCaseMix.error.message}`);
  }
  if (teamCaseMix.error) {
    throw new Error(`Governance team case mix error: ${teamCaseMix.error.message}`);
  }
  if (snapshots.error) {
    throw new Error(`Governance performance snapshots error: ${snapshots.error.message}`);
  }

  return {
    operatorSummary: (operatorSummary.data ?? []) as GovernanceOperatorPerformanceSummaryRow[],
    teamSummary: (teamSummary.data ?? []) as GovernanceTeamPerformanceSummaryRow[],
    operatorCaseMix: (operatorCaseMix.data ?? []) as GovernanceCaseMixSummaryRow[],
    teamCaseMix: (teamCaseMix.data ?? []) as GovernanceCaseMixSummaryRow[],
    snapshots: (snapshots.data ?? []) as GovernancePerformanceSnapshotRow[],
  };
}

export async function getGovernanceManagerOverviewMetrics(workspaceSlug: string): Promise<{
  managerOverview: GovernanceManagerOverviewSummaryRow[];
  chronicWatchlists: GovernanceChronicWatchlistSummaryRow[];
  operatorTeamComparison: GovernanceOperatorTeamComparisonSummaryRow[];
  promotionHealth: GovernancePromotionHealthOverviewRow[];
  operatingRisk: GovernanceOperatingRiskSummaryRow[];
  reviewPriorities: GovernanceReviewPriorityRow[];
  trendWindows: GovernanceTrendWindowRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [
    managerOverview,
    chronicWatchlists,
    operatorTeamComparison,
    promotionHealth,
    operatingRisk,
    reviewPriorities,
    trendWindows,
  ] = await Promise.all([
    supabase
      .from("governance_manager_overview_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("snapshot_at", { ascending: false })
      .limit(20),
    supabase
      .from("governance_chronic_watchlist_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("recurring_case_count", { ascending: false })
      .order("reopened_case_count", { ascending: false })
      .limit(20),
    supabase
      .from("governance_operator_team_comparison_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("resolution_quality_proxy", { ascending: false })
      .order("severity_weighted_load", { ascending: false, nullsFirst: false })
      .limit(30),
    supabase
      .from("governance_promotion_health_overview")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("latest_created_at", { ascending: false, nullsFirst: false }),
    supabase
      .from("governance_operating_risk_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("snapshot_at", { ascending: false })
      .limit(10),
    supabase
      .from("governance_review_priority_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("priority_rank", { ascending: true })
      .limit(10),
    supabase
      .from("governance_trend_window_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("window_label", { ascending: true })
      .order("metric_name", { ascending: true }),
  ]);

  if (managerOverview.error) {
    throw new Error(`Governance manager overview error: ${managerOverview.error.message}`);
  }
  if (chronicWatchlists.error) {
    throw new Error(`Governance chronic watchlists error: ${chronicWatchlists.error.message}`);
  }
  if (operatorTeamComparison.error) {
    throw new Error(`Governance operator/team comparison error: ${operatorTeamComparison.error.message}`);
  }
  if (promotionHealth.error) {
    throw new Error(`Governance promotion health error: ${promotionHealth.error.message}`);
  }
  if (operatingRisk.error) {
    throw new Error(`Governance operating risk error: ${operatingRisk.error.message}`);
  }
  if (reviewPriorities.error) {
    throw new Error(`Governance review priorities error: ${reviewPriorities.error.message}`);
  }
  if (trendWindows.error) {
    throw new Error(`Governance trend windows error: ${trendWindows.error.message}`);
  }

  return {
    managerOverview: (managerOverview.data ?? []) as GovernanceManagerOverviewSummaryRow[],
    chronicWatchlists: (chronicWatchlists.data ?? []) as GovernanceChronicWatchlistSummaryRow[],
    operatorTeamComparison: (operatorTeamComparison.data ?? []) as GovernanceOperatorTeamComparisonSummaryRow[],
    promotionHealth: (promotionHealth.data ?? []) as GovernancePromotionHealthOverviewRow[],
    operatingRisk: (operatingRisk.data ?? []) as GovernanceOperatingRiskSummaryRow[],
    reviewPriorities: (reviewPriorities.data ?? []) as GovernanceReviewPriorityRow[],
    trendWindows: (trendWindows.data ?? []) as GovernanceTrendWindowRow[],
  };
}

// ── Phase 3.5A — Routing Optimization ───────────────────────────────────────

export interface GovernanceRoutingOptimizationSnapshotRow {
  id: string;
  workspace_id: string;
  snapshot_at: string;
  window_label: string;
  recommendation_count: number;
  metadata: Record<string, unknown>;
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
  sample_size: number;
  confidence: "low" | "medium" | "high" | string;
  supporting_metrics: Record<string, unknown>;
}

export interface GovernanceRoutingPolicyOpportunityRow {
  workspace_id: string;
  recommendation_key: string;
  scope_type: string;
  scope_value: string;
  current_policy: Record<string, unknown>;
  recommended_policy: Record<string, unknown>;
  reason_code: string;
  confidence: "low" | "medium" | "high" | string;
  sample_size: number;
  expected_benefit_score: number;
  risk_score: number;
  supporting_metrics: Record<string, unknown>;
  created_at: string;
}

export async function getGovernanceRoutingOptimizationMetrics(workspaceSlug: string): Promise<{
  snapshot: GovernanceRoutingOptimizationSnapshotRow | null;
  featureEffectiveness: GovernanceRoutingFeatureEffectivenessRow[];
  contextFit: GovernanceRoutingContextFitRow[];
  policyOpportunities: GovernanceRoutingPolicyOpportunityRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [snapshot, featureEffectiveness, contextFit, policyOpportunities] = await Promise.all([
    supabase
      .from("governance_routing_optimization_snapshots")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("snapshot_at", { ascending: false })
      .limit(1)
      .maybeSingle(),
    supabase
      .from("governance_routing_feature_effectiveness_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("net_fit_score", { ascending: true, nullsFirst: false })
      .limit(50),
    supabase
      .from("governance_routing_context_fit_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("operator_fit_score", { ascending: false, nullsFirst: false })
      .limit(30),
    supabase
      .from("governance_routing_policy_opportunity_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .limit(20),
  ]);

  if (snapshot.error) {
    throw new Error(`Routing optimization snapshot error: ${snapshot.error.message}`);
  }
  if (featureEffectiveness.error) {
    throw new Error(`Routing feature effectiveness error: ${featureEffectiveness.error.message}`);
  }
  if (contextFit.error) {
    throw new Error(`Routing context fit error: ${contextFit.error.message}`);
  }
  if (policyOpportunities.error) {
    throw new Error(`Routing policy opportunities error: ${policyOpportunities.error.message}`);
  }

  return {
    snapshot: (snapshot.data ?? null) as GovernanceRoutingOptimizationSnapshotRow | null,
    featureEffectiveness: (featureEffectiveness.data ?? []) as GovernanceRoutingFeatureEffectivenessRow[],
    contextFit: (contextFit.data ?? []) as GovernanceRoutingContextFitRow[],
    policyOpportunities: (policyOpportunities.data ?? []) as GovernanceRoutingPolicyOpportunityRow[],
  };
}

// ── Phase 3.5B — Routing Policy Review + Promotion ──────────────────────────

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

export interface GovernanceRoutingPolicyApplicationRow {
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

export async function getGovernanceRoutingPolicyReviewMetrics(workspaceSlug: string): Promise<{
  reviewSummary: GovernanceRoutingPolicyReviewSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const reviewSummary = await supabase
    .from("governance_routing_policy_review_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("latest_reviewed_at", { ascending: false })
    .limit(50);

  if (reviewSummary.error) {
    throw new Error(`Routing policy review summary error: ${reviewSummary.error.message}`);
  }

  return {
    reviewSummary: (reviewSummary.data ?? []) as GovernanceRoutingPolicyReviewSummaryRow[],
  };
}

export async function getGovernanceRoutingPolicyPromotionMetrics(workspaceSlug: string): Promise<{
  promotionSummary: GovernanceRoutingPolicyPromotionSummaryRow[];
  applications: GovernanceRoutingPolicyApplicationRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [promotionSummary, applications] = await Promise.all([
    supabase
      .from("governance_routing_policy_promotion_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("latest_proposed_at", { ascending: false })
      .limit(30),
    supabase
      .from("governance_routing_policy_applications")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("applied_at", { ascending: false })
      .limit(20),
  ]);

  if (promotionSummary.error) {
    throw new Error(`Routing promotion summary error: ${promotionSummary.error.message}`);
  }
  if (applications.error) {
    throw new Error(`Routing policy applications error: ${applications.error.message}`);
  }

  return {
    promotionSummary: (promotionSummary.data ?? []) as GovernanceRoutingPolicyPromotionSummaryRow[],
    applications: (applications.data ?? []) as GovernanceRoutingPolicyApplicationRow[],
  };
}

// ── Phase 3.5C: Routing Policy Autopromotion ─────────────────────────────────

export interface GovernanceRoutingPolicyAutopromotionPolicyRow {
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

export interface GovernanceRoutingPolicyAutopromotionRollbackCandidateRow {
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

export async function getGovernanceRoutingPolicyAutopromotionMetrics(workspaceSlug: string): Promise<{
  policies: GovernanceRoutingPolicyAutopromotionPolicyRow[];
  summary: GovernanceRoutingPolicyAutopromotionSummaryRow[];
  eligibility: GovernanceRoutingPolicyAutopromotionEligibilityRow[];
  rollbackCandidates: GovernanceRoutingPolicyAutopromotionRollbackCandidateRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [policies, summary, eligibility, rollbackCandidates] = await Promise.all([
    supabase
      .from("governance_routing_policy_autopromotion_policies")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("scope_type")
      .limit(50),
    supabase
      .from("governance_routing_policy_autopromotion_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("latest_executed_at", { ascending: false })
      .limit(30),
    supabase
      .from("governance_routing_policy_autopromotion_eligibility")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("is_eligible", { ascending: false })
      .limit(30),
    supabase
      .from("governance_routing_policy_autopromotion_rollback_candidates")
      .select("*")
      .eq("workspace_id", workspaceId)
      .eq("resolved", false)
      .order("created_at", { ascending: false })
      .limit(20),
  ]);

  if (policies.error) throw new Error(`Autopromotion policies error: ${policies.error.message}`);
  if (summary.error) throw new Error(`Autopromotion summary error: ${summary.error.message}`);
  if (eligibility.error) throw new Error(`Autopromotion eligibility error: ${eligibility.error.message}`);
  if (rollbackCandidates.error) throw new Error(`Autopromotion rollback error: ${rollbackCandidates.error.message}`);

  return {
    policies: (policies.data ?? []) as GovernanceRoutingPolicyAutopromotionPolicyRow[],
    summary: (summary.data ?? []) as GovernanceRoutingPolicyAutopromotionSummaryRow[],
    eligibility: (eligibility.data ?? []) as GovernanceRoutingPolicyAutopromotionEligibilityRow[],
    rollbackCandidates: (rollbackCandidates.data ?? []) as GovernanceRoutingPolicyAutopromotionRollbackCandidateRow[],
  };
}

// ── Phase 3.6A: Rollback Review + Execution ──────────────────────────────────

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

export async function getGovernanceRoutingPolicyRollbackMetrics(workspaceSlug: string): Promise<{
  pendingRollbacks: GovernanceRoutingPolicyPendingRollbackRow[];
  reviewSummary: GovernanceRoutingPolicyRollbackReviewSummaryRow[];
  executionSummary: GovernanceRoutingPolicyRollbackExecutionSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [pendingRollbacks, reviewSummary, executionSummary] = await Promise.all([
    supabase
      .from("governance_routing_policy_pending_rollback_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .limit(30),
    supabase
      .from("governance_routing_policy_rollback_review_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("latest_reviewed_at", { ascending: false })
      .limit(30),
    supabase
      .from("governance_routing_policy_rollback_execution_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("latest_executed_at", { ascending: false })
      .limit(20),
  ]);

  if (pendingRollbacks.error) throw new Error(`Pending rollback error: ${pendingRollbacks.error.message}`);
  if (reviewSummary.error) throw new Error(`Rollback review summary error: ${reviewSummary.error.message}`);
  if (executionSummary.error) throw new Error(`Rollback execution summary error: ${executionSummary.error.message}`);

  return {
    pendingRollbacks: (pendingRollbacks.data ?? []) as GovernanceRoutingPolicyPendingRollbackRow[],
    reviewSummary: (reviewSummary.data ?? []) as GovernanceRoutingPolicyRollbackReviewSummaryRow[],
    executionSummary: (executionSummary.data ?? []) as GovernanceRoutingPolicyRollbackExecutionSummaryRow[],
  };
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

export interface GovernanceRoutingPolicyRollbackEffectivenessSummaryRow {
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

export async function getGovernanceRoutingPolicyRollbackImpactMetrics(workspaceSlug: string): Promise<{
  impactRows: GovernanceRoutingPolicyRollbackImpactRow[];
  effectivenessSummary: GovernanceRoutingPolicyRollbackEffectivenessSummaryRow | null;
  pendingEvaluations: GovernanceRoutingPolicyRollbackPendingEvaluationRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [impactRows, effectiveness, pendingEvals] = await Promise.all([
    supabase
      .from("governance_routing_policy_rollback_impact_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("created_at", { ascending: false })
      .limit(30),
    supabase
      .from("governance_routing_policy_rollback_effectiveness_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .limit(1),
    supabase
      .from("governance_routing_policy_rollback_pending_evaluation_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .limit(20),
  ]);

  if (impactRows.error) throw new Error(`Rollback impact summary error: ${impactRows.error.message}`);
  if (effectiveness.error) throw new Error(`Rollback effectiveness error: ${effectiveness.error.message}`);
  if (pendingEvals.error) throw new Error(`Rollback pending eval error: ${pendingEvals.error.message}`);

  return {
    impactRows: (impactRows.data ?? []) as GovernanceRoutingPolicyRollbackImpactRow[],
    effectivenessSummary: ((effectiveness.data ?? [])[0] as GovernanceRoutingPolicyRollbackEffectivenessSummaryRow) ?? null,
    pendingEvaluations: (pendingEvals.data ?? []) as GovernanceRoutingPolicyRollbackPendingEvaluationRow[],
  };
}

// ── Phase 3.7A: Governance Policy Optimization ────────────────────────────────

export interface GovernancePolicyOptimizationSnapshotRow {
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

export async function getGovernancePolicyOptimizationMetrics(workspaceSlug: string): Promise<{
  snapshot: GovernancePolicyOptimizationSnapshotRow | null;
  featureEffectiveness: GovernancePolicyFeatureEffectivenessRow[];
  contextFit: GovernancePolicyContextFitRow[];
  policyOpportunities: GovernancePolicyOpportunityRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [snapshot, featureEffectiveness, contextFit, policyOpportunities] = await Promise.all([
    supabase
      .from("governance_policy_optimization_snapshots")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("snapshot_at", { ascending: false })
      .limit(1),
    supabase
      .from("governance_policy_feature_effectiveness_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("net_policy_fit_score", { ascending: false })
      .limit(50),
    supabase
      .from("governance_policy_context_fit_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("fit_score", { ascending: false })
      .limit(30),
    supabase
      .from("governance_policy_opportunity_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("expected_benefit_score", { ascending: false })
      .limit(20),
  ]);

  if (snapshot.error) throw new Error(`Policy optimization snapshot error: ${snapshot.error.message}`);
  if (featureEffectiveness.error) throw new Error(`Feature effectiveness error: ${featureEffectiveness.error.message}`);
  if (contextFit.error) throw new Error(`Context fit error: ${contextFit.error.message}`);
  if (policyOpportunities.error) throw new Error(`Policy opportunities error: ${policyOpportunities.error.message}`);

  return {
    snapshot: ((snapshot.data ?? [])[0] as GovernancePolicyOptimizationSnapshotRow) ?? null,
    featureEffectiveness: (featureEffectiveness.data ?? []) as GovernancePolicyFeatureEffectivenessRow[],
    contextFit: (contextFit.data ?? []) as GovernancePolicyContextFitRow[],
    policyOpportunities: (policyOpportunities.data ?? []) as GovernancePolicyOpportunityRow[],
  };
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

export async function getGovernancePolicyReviewMetrics(workspaceSlug: string): Promise<{
  reviewSummary: GovernancePolicyReviewSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const { data, error } = await supabase
    .from("governance_policy_review_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("latest_reviewed_at", { ascending: false })
    .limit(50);

  if (error) throw new Error(`Policy review summary error: ${error.message}`);
  return { reviewSummary: (data ?? []) as GovernancePolicyReviewSummaryRow[] };
}

export async function getGovernancePolicyPromotionMetrics(workspaceSlug: string): Promise<{
  promotionSummary: GovernancePolicyPromotionSummaryRow[];
  pendingPromotions: GovernancePolicyPendingPromotionRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [summary, pending] = await Promise.all([
    supabase
      .from("governance_policy_promotion_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("latest_proposed_at", { ascending: false })
      .limit(30),
    supabase
      .from("governance_policy_pending_promotion_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .limit(20),
  ]);

  if (summary.error) throw new Error(`Policy promotion summary error: ${summary.error.message}`);
  if (pending.error) throw new Error(`Policy pending promotion error: ${pending.error.message}`);

  return {
    promotionSummary: (summary.data ?? []) as GovernancePolicyPromotionSummaryRow[],
    pendingPromotions: (pending.data ?? []) as GovernancePolicyPendingPromotionRow[],
  };
}

// ── Phase 3.7C: Governance Policy Autopromotion ───────────────────────────────

export interface GovernancePolicyAutopromotionSummaryRow {
  workspace_id: string;
  policy_id: string;
  policy_family: string;
  scope_type: string;
  scope_value: string;
  promotion_target: string;
  enabled: boolean;
  min_confidence: string;
  min_approved_review_count: number;
  min_application_count: number;
  min_sample_size: number;
  cooldown_hours: number;
  latest_execution_at: string | null;
  execution_count: number;
  rollback_candidate_count: number;
  last_recommendation_key: string | null;
}

export interface GovernancePolicyAutopromotionEligibilityRow {
  workspace_id: string;
  recommendation_key: string;
  policy_id: string | null;
  policy_family: string;
  scope_type: string;
  scope_value: string;
  promotion_target: string;
  confidence: string | null;
  sample_size: number;
  approved_review_count: number;
  application_count: number;
  recent_override_rate: number;
  recent_reassignment_rate: number;
  last_execution_at: string | null;
  cooldown_ends_at: string | null;
  eligible: boolean;
  blocked_reason_code: string | null;
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
  prior_policy: Record<string, unknown>;
  applied_policy: Record<string, unknown>;
  rollback_reason_code: string | null;
  rollback_risk_score: number;
  created_at: string;
  rolled_back: boolean;
  rolled_back_at: string | null;
}

export async function getGovernancePolicyAutopromotionMetrics(workspaceSlug: string): Promise<{
  autopromotionSummary: GovernancePolicyAutopromotionSummaryRow[];
  eligibility: GovernancePolicyAutopromotionEligibilityRow[];
  rollbackCandidates: GovernancePolicyAutopromotionRollbackCandidateRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [summary, eligibility, rollbacks] = await Promise.all([
    supabase
      .from("governance_policy_autopromotion_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("latest_execution_at", { ascending: false })
      .limit(20),
    supabase
      .from("governance_policy_autopromotion_eligibility")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("eligible", { ascending: false })
      .limit(30),
    supabase
      .from("governance_policy_autopromotion_rollback_candidates")
      .select("*")
      .eq("workspace_id", workspaceId)
      .eq("rolled_back", false)
      .order("created_at", { ascending: false })
      .limit(20),
  ]);

  if (summary.error) throw new Error(`Autopromotion summary error: ${summary.error.message}`);
  if (eligibility.error) throw new Error(`Autopromotion eligibility error: ${eligibility.error.message}`);
  if (rollbacks.error) throw new Error(`Autopromotion rollbacks error: ${rollbacks.error.message}`);

  return {
    autopromotionSummary: (summary.data ?? []) as GovernancePolicyAutopromotionSummaryRow[],
    eligibility: (eligibility.data ?? []) as GovernancePolicyAutopromotionEligibilityRow[],
    rollbackCandidates: (rollbacks.data ?? []) as GovernancePolicyAutopromotionRollbackCandidateRow[],
  };
}

// ── Phase 4.0A: Multi-Asset Data Foundation ─────────────────────────────
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

export async function getMultiAssetFoundationMetrics(workspaceSlug: string): Promise<{
  syncHealth: MultiAssetSyncHealthRow[];
  marketStateSample: NormalizedMultiAssetMarketStateRow[];
  familySummary: MultiAssetFamilyStateSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  const [syncHealth, marketState, familySummary] = await Promise.all([
    supabase
      .from("multi_asset_sync_health_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("latest_run_started_at", { ascending: false })
      .limit(20),
    supabase
      .from("normalized_multi_asset_market_state")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("asset_class", { ascending: true })
      .limit(60),
    supabase
      .from("multi_asset_family_state_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("asset_class", { ascending: true })
      .limit(20),
  ]);

  if (syncHealth.error) throw new Error(`Multi-asset sync health error: ${syncHealth.error.message}`);
  if (marketState.error) throw new Error(`Multi-asset market state error: ${marketState.error.message}`);
  if (familySummary.error) throw new Error(`Multi-asset family summary error: ${familySummary.error.message}`);

  return {
    syncHealth: (syncHealth.data ?? []) as MultiAssetSyncHealthRow[],
    marketStateSample: (marketState.data ?? []) as NormalizedMultiAssetMarketStateRow[],
    familySummary: (familySummary.data ?? []) as MultiAssetFamilyStateSummaryRow[],
  };
}

// ── Phase 4.0B: Dependency Graph + Context Model ────────────────────────
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

export async function getDependencyContextMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  latestContexts: WatchlistContextSnapshotRow[];
  coverageSummary: WatchlistDependencyCoverageSummaryRow[];
  contextDetail: WatchlistDependencyContextDetailRow[];
  familyState: WatchlistDependencyFamilyStateRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseSnapshots = supabase
    .from("watchlist_context_snapshots")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("snapshot_at", { ascending: false })
    .limit(20);

  const baseCoverage = supabase
    .from("watchlist_dependency_coverage_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .limit(20);

  const baseDetail = supabase
    .from("watchlist_dependency_context_detail")
    .select("*")
    .eq("workspace_id", workspaceId)
    .limit(200);

  const baseFamily = supabase
    .from("watchlist_dependency_family_state")
    .select("*")
    .eq("workspace_id", workspaceId)
    .limit(40);

  const [latest, coverage, detail, family] = await Promise.all([
    watchlistId ? baseSnapshots.eq("watchlist_id", watchlistId) : baseSnapshots,
    watchlistId ? baseCoverage.eq("watchlist_id", watchlistId) : baseCoverage,
    watchlistId ? baseDetail.eq("watchlist_id", watchlistId) : baseDetail,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
  ]);

  if (latest.error) throw new Error(`Context snapshots error: ${latest.error.message}`);
  if (coverage.error) throw new Error(`Coverage summary error: ${coverage.error.message}`);
  if (detail.error) throw new Error(`Context detail error: ${detail.error.message}`);
  if (family.error) throw new Error(`Family state error: ${family.error.message}`);

  return {
    latestContexts: (latest.data ?? []) as WatchlistContextSnapshotRow[],
    coverageSummary: (coverage.data ?? []) as WatchlistDependencyCoverageSummaryRow[],
    contextDetail: (detail.data ?? []) as WatchlistDependencyContextDetailRow[],
    familyState: (family.data ?? []) as WatchlistDependencyFamilyStateRow[],
  };
}

// ── Phase 4.0C: Cross-Asset Signal Expansion ────────────────────────────
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

export async function getCrossAssetSignalMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  signalSummary: CrossAssetSignalSummaryRow[];
  dependencyHealth: CrossAssetDependencyHealthRow[];
  runContextSummary: RunCrossAssetContextSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseSummary = supabase
    .from("cross_asset_signal_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(100);

  const baseHealth = supabase
    .from("cross_asset_dependency_health_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .limit(40);

  const baseRun = supabase
    .from("run_cross_asset_context_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(20);

  const [summary, health, runCtx] = await Promise.all([
    watchlistId ? baseSummary.eq("watchlist_id", watchlistId) : baseSummary,
    watchlistId ? baseHealth.eq("watchlist_id", watchlistId) : baseHealth,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (summary.error) throw new Error(`Cross-asset signal summary error: ${summary.error.message}`);
  if (health.error) throw new Error(`Cross-asset dependency health error: ${health.error.message}`);
  if (runCtx.error) throw new Error(`Run cross-asset context error: ${runCtx.error.message}`);

  return {
    signalSummary: (summary.data ?? []) as CrossAssetSignalSummaryRow[],
    dependencyHealth: (health.data ?? []) as CrossAssetDependencyHealthRow[],
    runContextSummary: (runCtx.data ?? []) as RunCrossAssetContextSummaryRow[],
  };
}

// ── Phase 4.0D: Cross-Asset Explainability ──────────────────────────────
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

export async function getCrossAssetExplainabilityMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  explanationSummary: CrossAssetExplanationSummaryRow[];
  familySummary: CrossAssetFamilyExplanationSummaryRow[];
  runBridgeSummary: RunCrossAssetExplanationBridgeRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseExplanation = supabase
    .from("cross_asset_explanation_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const baseFamily = supabase
    .from("cross_asset_family_explanation_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(120);

  const baseRun = supabase
    .from("run_cross_asset_explanation_bridge")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const [explanation, family, runBridge] = await Promise.all([
    watchlistId ? baseExplanation.eq("watchlist_id", watchlistId) : baseExplanation,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (explanation.error) throw new Error(`Cross-asset explanation error: ${explanation.error.message}`);
  if (family.error) throw new Error(`Cross-asset family explanation error: ${family.error.message}`);
  if (runBridge.error) throw new Error(`Cross-asset run bridge error: ${runBridge.error.message}`);

  return {
    explanationSummary: (explanation.data ?? []) as CrossAssetExplanationSummaryRow[],
    familySummary: (family.data ?? []) as CrossAssetFamilyExplanationSummaryRow[],
    runBridgeSummary: (runBridge.data ?? []) as RunCrossAssetExplanationBridgeRow[],
  };
}

// ── Phase 4.1A: Cross-Asset Attribution + Composite Integration ─────────
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

export async function getCrossAssetAttributionMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  attributionSummary: CrossAssetAttributionSummaryRow[];
  familyAttributionSummary: CrossAssetFamilyAttributionSummaryRow[];
  runIntegrationSummary: RunCompositeIntegrationSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseAttribution = supabase
    .from("cross_asset_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const baseFamily = supabase
    .from("cross_asset_family_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(120);

  const baseRunIntegration = supabase
    .from("run_composite_integration_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const [attribution, family, runIntegration] = await Promise.all([
    watchlistId ? baseAttribution.eq("watchlist_id", watchlistId) : baseAttribution,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseRunIntegration.eq("watchlist_id", watchlistId) : baseRunIntegration,
  ]);

  if (attribution.error) throw new Error(`Cross-asset attribution error: ${attribution.error.message}`);
  if (family.error) throw new Error(`Cross-asset family attribution error: ${family.error.message}`);
  if (runIntegration.error) throw new Error(`Cross-asset run integration error: ${runIntegration.error.message}`);

  return {
    attributionSummary: (attribution.data ?? []) as CrossAssetAttributionSummaryRow[],
    familyAttributionSummary: (family.data ?? []) as CrossAssetFamilyAttributionSummaryRow[],
    runIntegrationSummary: (runIntegration.data ?? []) as RunCompositeIntegrationSummaryRow[],
  };
}

// ── Phase 4.1B: Dependency-Priority-Aware Ranking + Contribution Weighting
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

export async function getDependencyPriorityWeightingMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  familyWeightedSummary: CrossAssetFamilyWeightedAttributionSummaryRow[];
  symbolWeightedSummary: CrossAssetSymbolWeightedAttributionSummaryRow[];
  runWeightedSummary: RunCrossAssetWeightedIntegrationSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseFamily = supabase
    .from("cross_asset_family_weighted_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(120);

  const baseSymbol = supabase
    .from("cross_asset_symbol_weighted_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseRun = supabase
    .from("run_cross_asset_weighted_integration_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const [family, symbol, run] = await Promise.all([
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseSymbol.eq("watchlist_id", watchlistId) : baseSymbol,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (family.error) throw new Error(`Weighted family attribution error: ${family.error.message}`);
  if (symbol.error) throw new Error(`Weighted symbol attribution error: ${symbol.error.message}`);
  if (run.error) throw new Error(`Weighted run integration error: ${run.error.message}`);

  return {
    familyWeightedSummary: (family.data ?? []) as CrossAssetFamilyWeightedAttributionSummaryRow[],
    symbolWeightedSummary: (symbol.data ?? []) as CrossAssetSymbolWeightedAttributionSummaryRow[],
    runWeightedSummary: (run.data ?? []) as RunCrossAssetWeightedIntegrationSummaryRow[],
  };
}

// ── Phase 4.1C: Regime-Aware Cross-Asset Interpretation ─────────────────
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

export async function getRegimeAwareCrossAssetMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  familyRegimeSummary: CrossAssetFamilyRegimeAttributionSummaryRow[];
  symbolRegimeSummary: CrossAssetSymbolRegimeAttributionSummaryRow[];
  runRegimeSummary: RunCrossAssetRegimeIntegrationSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseFamily = supabase
    .from("cross_asset_family_regime_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(120);

  const baseSymbol = supabase
    .from("cross_asset_symbol_regime_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseRun = supabase
    .from("run_cross_asset_regime_integration_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const [family, symbol, run] = await Promise.all([
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseSymbol.eq("watchlist_id", watchlistId) : baseSymbol,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (family.error) throw new Error(`Regime family attribution error: ${family.error.message}`);
  if (symbol.error) throw new Error(`Regime symbol attribution error: ${symbol.error.message}`);
  if (run.error) throw new Error(`Regime run integration error: ${run.error.message}`);

  return {
    familyRegimeSummary: (family.data ?? []) as CrossAssetFamilyRegimeAttributionSummaryRow[],
    symbolRegimeSummary: (symbol.data ?? []) as CrossAssetSymbolRegimeAttributionSummaryRow[],
    runRegimeSummary: (run.data ?? []) as RunCrossAssetRegimeIntegrationSummaryRow[],
  };
}

// ── Phase 4.1D: Cross-Asset Replay + Stability Validation ───────────────
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

export async function getCrossAssetReplayValidationMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  replayValidationSummary: CrossAssetReplayValidationSummaryRow[];
  familyReplayStabilitySummary: CrossAssetFamilyReplayStabilitySummaryRow[];
  replayStabilityAggregate: CrossAssetReplayStabilityAggregateRow | null;
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseValidation = supabase
    .from("cross_asset_replay_validation_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const baseFamily = supabase
    .from("cross_asset_family_replay_stability_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const [validation, family, aggregate] = await Promise.all([
    watchlistId ? baseValidation.eq("watchlist_id", watchlistId) : baseValidation,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    supabase
      .from("cross_asset_replay_stability_aggregate")
      .select("*")
      .eq("workspace_id", workspaceId)
      .limit(1)
      .maybeSingle(),
  ]);

  if (validation.error) throw new Error(`Replay validation error: ${validation.error.message}`);
  if (family.error) throw new Error(`Family replay stability error: ${family.error.message}`);
  if (aggregate.error) throw new Error(`Replay stability aggregate error: ${aggregate.error.message}`);

  return {
    replayValidationSummary: (validation.data ?? []) as CrossAssetReplayValidationSummaryRow[],
    familyReplayStabilitySummary: (family.data ?? []) as CrossAssetFamilyReplayStabilitySummaryRow[],
    replayStabilityAggregate: (aggregate.data as CrossAssetReplayStabilityAggregateRow | null) ?? null,
  };
}

// ── Phase 4.2A: Cross-Asset Lead/Lag + Dependency Timing ────────────────
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

export async function getCrossAssetTimingMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  pairSummary: CrossAssetLeadLagPairSummaryRow[];
  familyTimingSummary: CrossAssetFamilyTimingSummaryRow[];
  runTimingSummary: RunCrossAssetTimingSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const basePair = supabase
    .from("cross_asset_lead_lag_pair_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseFamily = supabase
    .from("cross_asset_family_timing_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(100);

  const baseRun = supabase
    .from("run_cross_asset_timing_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const [pair, family, run] = await Promise.all([
    watchlistId ? basePair.eq("watchlist_id", watchlistId) : basePair,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (pair.error) throw new Error(`Cross-asset pair timing error: ${pair.error.message}`);
  if (family.error) throw new Error(`Cross-asset family timing error: ${family.error.message}`);
  if (run.error) throw new Error(`Cross-asset run timing error: ${run.error.message}`);

  return {
    pairSummary: (pair.data ?? []) as CrossAssetLeadLagPairSummaryRow[],
    familyTimingSummary: (family.data ?? []) as CrossAssetFamilyTimingSummaryRow[],
    runTimingSummary: (run.data ?? []) as RunCrossAssetTimingSummaryRow[],
  };
}

// ── Phase 4.2B: Family-Level Lead/Lag Attribution ───────────────────────
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

export async function getCrossAssetTimingAttributionMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  familyTimingAttributionSummary: CrossAssetFamilyTimingAttributionSummaryRow[];
  symbolTimingAttributionSummary: CrossAssetSymbolTimingAttributionSummaryRow[];
  runTimingAttributionSummary: RunCrossAssetTimingAttributionSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseFamily = supabase
    .from("cross_asset_family_timing_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(120);

  const baseSymbol = supabase
    .from("cross_asset_symbol_timing_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseRun = supabase
    .from("run_cross_asset_timing_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const [family, symbol, run] = await Promise.all([
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseSymbol.eq("watchlist_id", watchlistId) : baseSymbol,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (family.error) throw new Error(`Timing family attribution error: ${family.error.message}`);
  if (symbol.error) throw new Error(`Timing symbol attribution error: ${symbol.error.message}`);
  if (run.error) throw new Error(`Timing run integration error: ${run.error.message}`);

  return {
    familyTimingAttributionSummary: (family.data ?? []) as CrossAssetFamilyTimingAttributionSummaryRow[],
    symbolTimingAttributionSummary: (symbol.data ?? []) as CrossAssetSymbolTimingAttributionSummaryRow[],
    runTimingAttributionSummary: (run.data ?? []) as RunCrossAssetTimingAttributionSummaryRow[],
  };
}

// ── Phase 4.2C: Timing-Aware Composite Refinement ───────────────────────
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

export async function getCrossAssetTimingCompositeMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  timingCompositeSummary: CrossAssetTimingCompositeSummaryRow[];
  familyTimingCompositeSummary: CrossAssetFamilyTimingCompositeSummaryRow[];
  finalIntegrationSummary: RunCrossAssetFinalIntegrationSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseComposite = supabase
    .from("cross_asset_timing_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const baseFamily = supabase
    .from("cross_asset_family_timing_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(120);

  const baseFinal = supabase
    .from("run_cross_asset_final_integration_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const [composite, family, final] = await Promise.all([
    watchlistId ? baseComposite.eq("watchlist_id", watchlistId) : baseComposite,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseFinal.eq("watchlist_id", watchlistId) : baseFinal,
  ]);

  if (composite.error) throw new Error(`Timing composite error: ${composite.error.message}`);
  if (family.error) throw new Error(`Family timing composite error: ${family.error.message}`);
  if (final.error) throw new Error(`Final integration error: ${final.error.message}`);

  return {
    timingCompositeSummary: (composite.data ?? []) as CrossAssetTimingCompositeSummaryRow[],
    familyTimingCompositeSummary: (family.data ?? []) as CrossAssetFamilyTimingCompositeSummaryRow[],
    finalIntegrationSummary: (final.data ?? []) as RunCrossAssetFinalIntegrationSummaryRow[],
  };
}

// ── Phase 4.2D: Replay Validation for Timing-Aware Composite ────────────
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

export async function getCrossAssetTimingReplayValidationMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  timingReplayValidationSummary: CrossAssetTimingReplayValidationSummaryRow[];
  familyTimingReplayStabilitySummary: CrossAssetFamilyTimingReplayStabilitySummaryRow[];
  timingReplayStabilityAggregate: CrossAssetTimingReplayStabilityAggregateRow | null;
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseValidation = supabase
    .from("cross_asset_timing_replay_validation_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const baseFamily = supabase
    .from("cross_asset_family_timing_replay_stability_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const [validation, family, aggregate] = await Promise.all([
    watchlistId ? baseValidation.eq("watchlist_id", watchlistId) : baseValidation,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    supabase
      .from("cross_asset_timing_replay_stability_aggregate")
      .select("*")
      .eq("workspace_id", workspaceId)
      .limit(1)
      .maybeSingle(),
  ]);

  if (validation.error) throw new Error(`Timing replay validation error: ${validation.error.message}`);
  if (family.error) throw new Error(`Timing family replay stability error: ${family.error.message}`);
  if (aggregate.error) throw new Error(`Timing replay stability aggregate error: ${aggregate.error.message}`);

  return {
    timingReplayValidationSummary: (validation.data ?? []) as CrossAssetTimingReplayValidationSummaryRow[],
    familyTimingReplayStabilitySummary: (family.data ?? []) as CrossAssetFamilyTimingReplayStabilitySummaryRow[],
    timingReplayStabilityAggregate: (aggregate.data as CrossAssetTimingReplayStabilityAggregateRow | null) ?? null,
  };
}

// ── Phase 4.3A: Family-Level Sequencing + Transition-State Diagnostics ──
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

export async function getCrossAssetTransitionDiagnosticsMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  transitionStateSummary: CrossAssetFamilyTransitionStateSummaryRow[];
  transitionEventSummary: CrossAssetFamilyTransitionEventSummaryRow[];
  sequenceSummary: CrossAssetFamilySequenceSummaryRow[];
  runTransitionSummary: RunCrossAssetTransitionDiagnosticsSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseState = supabase
    .from("cross_asset_family_transition_state_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(120);

  const baseEvent = supabase
    .from("cross_asset_family_transition_event_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(120);

  const baseSequence = supabase
    .from("cross_asset_family_sequence_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(120);

  const baseRun = supabase
    .from("run_cross_asset_transition_diagnostics_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const [state, event, sequence, run] = await Promise.all([
    watchlistId ? baseState.eq("watchlist_id", watchlistId) : baseState,
    watchlistId ? baseEvent.eq("watchlist_id", watchlistId) : baseEvent,
    watchlistId ? baseSequence.eq("watchlist_id", watchlistId) : baseSequence,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (state.error) throw new Error(`Transition state error: ${state.error.message}`);
  if (event.error) throw new Error(`Transition event error: ${event.error.message}`);
  if (sequence.error) throw new Error(`Sequence summary error: ${sequence.error.message}`);
  if (run.error) throw new Error(`Run transition diagnostics error: ${run.error.message}`);

  return {
    transitionStateSummary: (state.data ?? []) as CrossAssetFamilyTransitionStateSummaryRow[],
    transitionEventSummary: (event.data ?? []) as CrossAssetFamilyTransitionEventSummaryRow[],
    sequenceSummary: (sequence.data ?? []) as CrossAssetFamilySequenceSummaryRow[],
    runTransitionSummary: (run.data ?? []) as RunCrossAssetTransitionDiagnosticsSummaryRow[],
  };
}

// ── Phase 4.3B: Transition-Aware Attribution ────────────────────────────
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

export async function getCrossAssetTransitionAttributionMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  familyTransitionAttributionSummary: CrossAssetFamilyTransitionAttributionSummaryRow[];
  symbolTransitionAttributionSummary: CrossAssetSymbolTransitionAttributionSummaryRow[];
  runTransitionAttributionSummary: RunCrossAssetTransitionAttributionSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseFamily = supabase
    .from("cross_asset_family_transition_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(120);

  const baseSymbol = supabase
    .from("cross_asset_symbol_transition_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseRun = supabase
    .from("run_cross_asset_transition_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const [family, symbol, run] = await Promise.all([
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseSymbol.eq("watchlist_id", watchlistId) : baseSymbol,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (family.error) throw new Error(`Transition family attribution error: ${family.error.message}`);
  if (symbol.error) throw new Error(`Transition symbol attribution error: ${symbol.error.message}`);
  if (run.error) throw new Error(`Transition run integration error: ${run.error.message}`);

  return {
    familyTransitionAttributionSummary: (family.data ?? []) as CrossAssetFamilyTransitionAttributionSummaryRow[],
    symbolTransitionAttributionSummary: (symbol.data ?? []) as CrossAssetSymbolTransitionAttributionSummaryRow[],
    runTransitionAttributionSummary: (run.data ?? []) as RunCrossAssetTransitionAttributionSummaryRow[],
  };
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

export async function getCrossAssetTransitionCompositeMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  transitionCompositeSummary: CrossAssetTransitionCompositeSummaryRow[];
  familyTransitionCompositeSummary: CrossAssetFamilyTransitionCompositeSummaryRow[];
  finalSequencingIntegrationSummary: RunCrossAssetSequencingIntegrationSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseComposite = supabase
    .from("cross_asset_transition_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const baseFamily = supabase
    .from("cross_asset_family_transition_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseFinal = supabase
    .from("run_cross_asset_sequencing_integration_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const [composite, family, finalRun] = await Promise.all([
    watchlistId ? baseComposite.eq("watchlist_id", watchlistId) : baseComposite,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseFinal.eq("watchlist_id", watchlistId) : baseFinal,
  ]);

  if (composite.error) throw new Error(`Transition composite summary error: ${composite.error.message}`);
  if (family.error) throw new Error(`Family transition composite error: ${family.error.message}`);
  if (finalRun.error) throw new Error(`Sequencing integration summary error: ${finalRun.error.message}`);

  return {
    transitionCompositeSummary: (composite.data ?? []) as CrossAssetTransitionCompositeSummaryRow[],
    familyTransitionCompositeSummary: (family.data ?? []) as CrossAssetFamilyTransitionCompositeSummaryRow[],
    finalSequencingIntegrationSummary: (finalRun.data ?? []) as RunCrossAssetSequencingIntegrationSummaryRow[],
  };
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
  source_dominant_timing_class: string | null;
  replay_dominant_timing_class: string | null;
  source_dominant_transition_state: string | null;
  replay_dominant_transition_state: string | null;
  source_dominant_sequence_class: string | null;
  replay_dominant_sequence_class: string | null;
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

export async function getCrossAssetTransitionReplayValidationMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  transitionReplayValidationSummary: CrossAssetTransitionReplayValidationSummaryRow[];
  familyTransitionReplayStabilitySummary: CrossAssetFamilyTransitionReplayStabilitySummaryRow[];
  transitionReplayStabilityAggregate: CrossAssetTransitionReplayStabilityAggregateRow | null;
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseValidation = supabase
    .from("cross_asset_transition_replay_validation_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const baseFamily = supabase
    .from("cross_asset_family_transition_replay_stability_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const [validation, family, aggregate] = await Promise.all([
    watchlistId ? baseValidation.eq("watchlist_id", watchlistId) : baseValidation,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    supabase
      .from("cross_asset_transition_replay_stability_aggregate")
      .select("*")
      .eq("workspace_id", workspaceId)
      .limit(1)
      .maybeSingle(),
  ]);

  if (validation.error) throw new Error(`Transition replay validation error: ${validation.error.message}`);
  if (family.error) throw new Error(`Family transition replay stability error: ${family.error.message}`);
  if (aggregate.error) throw new Error(`Transition replay stability aggregate error: ${aggregate.error.message}`);

  return {
    transitionReplayValidationSummary: (validation.data ?? []) as CrossAssetTransitionReplayValidationSummaryRow[],
    familyTransitionReplayStabilitySummary: (family.data ?? []) as CrossAssetFamilyTransitionReplayStabilitySummaryRow[],
    transitionReplayStabilityAggregate: (aggregate.data as CrossAssetTransitionReplayStabilityAggregateRow | null) ?? null,
  };
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

export async function getCrossAssetPatternMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  familyArchetypeSummary: CrossAssetFamilyArchetypeSummaryRow[];
  runArchetypeSummary: CrossAssetRunArchetypeSummaryRow[];
  regimeArchetypeSummary: CrossAssetRegimeArchetypeSummaryRow[];
  runPatternSummary: RunCrossAssetPatternSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseFamily = supabase
    .from("cross_asset_family_archetype_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseRun = supabase
    .from("cross_asset_run_archetype_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const basePattern = supabase
    .from("run_cross_asset_pattern_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const [family, run, regime, pattern] = await Promise.all([
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
    supabase
      .from("cross_asset_regime_archetype_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("latest_seen_at", { ascending: false, nullsFirst: false })
      .limit(100),
    watchlistId ? basePattern.eq("watchlist_id", watchlistId) : basePattern,
  ]);

  if (family.error) throw new Error(`Family archetype error: ${family.error.message}`);
  if (run.error) throw new Error(`Run archetype error: ${run.error.message}`);
  if (regime.error) throw new Error(`Regime archetype error: ${regime.error.message}`);
  if (pattern.error) throw new Error(`Run pattern summary error: ${pattern.error.message}`);

  return {
    familyArchetypeSummary: (family.data ?? []) as CrossAssetFamilyArchetypeSummaryRow[],
    runArchetypeSummary: (run.data ?? []) as CrossAssetRunArchetypeSummaryRow[],
    regimeArchetypeSummary: (regime.data ?? []) as CrossAssetRegimeArchetypeSummaryRow[],
    runPatternSummary: (pattern.data ?? []) as RunCrossAssetPatternSummaryRow[],
  };
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

export async function getCrossAssetArchetypeAttributionMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  familyArchetypeAttributionSummary: CrossAssetFamilyArchetypeAttributionSummaryRow[];
  symbolArchetypeAttributionSummary: CrossAssetSymbolArchetypeAttributionSummaryRow[];
  runArchetypeAttributionSummary: RunCrossAssetArchetypeAttributionSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseFamily = supabase
    .from("cross_asset_family_archetype_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseSymbol = supabase
    .from("cross_asset_symbol_archetype_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(300);

  const baseRun = supabase
    .from("run_cross_asset_archetype_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const [family, symbol, run] = await Promise.all([
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseSymbol.eq("watchlist_id", watchlistId) : baseSymbol,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (family.error) throw new Error(`Archetype family attribution error: ${family.error.message}`);
  if (symbol.error) throw new Error(`Archetype symbol attribution error: ${symbol.error.message}`);
  if (run.error) throw new Error(`Archetype run integration error: ${run.error.message}`);

  return {
    familyArchetypeAttributionSummary: (family.data ?? []) as CrossAssetFamilyArchetypeAttributionSummaryRow[],
    symbolArchetypeAttributionSummary: (symbol.data ?? []) as CrossAssetSymbolArchetypeAttributionSummaryRow[],
    runArchetypeAttributionSummary: (run.data ?? []) as RunCrossAssetArchetypeAttributionSummaryRow[],
  };
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

export async function getCrossAssetArchetypeCompositeMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  archetypeCompositeSummary: CrossAssetArchetypeCompositeSummaryRow[];
  familyArchetypeCompositeSummary: CrossAssetFamilyArchetypeCompositeSummaryRow[];
  finalArchetypeIntegrationSummary: RunCrossAssetArchetypeIntegrationSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseComposite = supabase
    .from("cross_asset_archetype_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const baseFamily = supabase
    .from("cross_asset_family_archetype_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseFinal = supabase
    .from("run_cross_asset_archetype_integration_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const [composite, family, finalRun] = await Promise.all([
    watchlistId ? baseComposite.eq("watchlist_id", watchlistId) : baseComposite,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseFinal.eq("watchlist_id", watchlistId) : baseFinal,
  ]);

  if (composite.error) throw new Error(`Archetype composite summary error: ${composite.error.message}`);
  if (family.error) throw new Error(`Family archetype composite error: ${family.error.message}`);
  if (finalRun.error) throw new Error(`Archetype integration summary error: ${finalRun.error.message}`);

  return {
    archetypeCompositeSummary: (composite.data ?? []) as CrossAssetArchetypeCompositeSummaryRow[],
    familyArchetypeCompositeSummary: (family.data ?? []) as CrossAssetFamilyArchetypeCompositeSummaryRow[],
    finalArchetypeIntegrationSummary: (finalRun.data ?? []) as RunCrossAssetArchetypeIntegrationSummaryRow[],
  };
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
  source_dominant_timing_class: string | null;
  replay_dominant_timing_class: string | null;
  source_dominant_transition_state: string | null;
  replay_dominant_transition_state: string | null;
  source_dominant_sequence_class: string | null;
  replay_dominant_sequence_class: string | null;
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

export async function getCrossAssetArchetypeReplayValidationMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  archetypeReplayValidationSummary: CrossAssetArchetypeReplayValidationSummaryRow[];
  familyArchetypeReplayStabilitySummary: CrossAssetFamilyArchetypeReplayStabilitySummaryRow[];
  archetypeReplayStabilityAggregate: CrossAssetArchetypeReplayStabilityAggregateRow | null;
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseValidation = supabase
    .from("cross_asset_archetype_replay_validation_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const baseFamily = supabase
    .from("cross_asset_family_archetype_replay_stability_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const [validation, family, aggregate] = await Promise.all([
    watchlistId ? baseValidation.eq("watchlist_id", watchlistId) : baseValidation,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    supabase
      .from("cross_asset_archetype_replay_stability_aggregate")
      .select("*")
      .eq("workspace_id", workspaceId)
      .limit(1)
      .maybeSingle(),
  ]);

  if (validation.error) throw new Error(`Archetype replay validation error: ${validation.error.message}`);
  if (family.error) throw new Error(`Family archetype replay stability error: ${family.error.message}`);
  if (aggregate.error) throw new Error(`Archetype replay stability aggregate error: ${aggregate.error.message}`);

  return {
    archetypeReplayValidationSummary: (validation.data ?? []) as CrossAssetArchetypeReplayValidationSummaryRow[],
    familyArchetypeReplayStabilitySummary: (family.data ?? []) as CrossAssetFamilyArchetypeReplayStabilitySummaryRow[],
    archetypeReplayStabilityAggregate: (aggregate.data as CrossAssetArchetypeReplayStabilityAggregateRow | null) ?? null,
  };
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

export async function getCrossAssetPatternClusterMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  clusterSummary: CrossAssetArchetypeClusterSummaryRow[];
  regimeRotationSummary: CrossAssetArchetypeRegimeRotationSummaryRow[];
  driftEventSummary: CrossAssetPatternDriftEventSummaryRow[];
  runPatternClusterSummary: RunCrossAssetPatternClusterSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseCluster = supabase
    .from("cross_asset_archetype_cluster_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const baseDrift = supabase
    .from("cross_asset_pattern_drift_event_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(100);

  const basePattern = supabase
    .from("run_cross_asset_pattern_cluster_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const [cluster, regime, drift, pattern] = await Promise.all([
    watchlistId ? baseCluster.eq("watchlist_id", watchlistId) : baseCluster,
    supabase
      .from("cross_asset_archetype_regime_rotation_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("created_at", { ascending: false })
      .limit(50),
    watchlistId ? baseDrift.eq("watchlist_id", watchlistId) : baseDrift,
    watchlistId ? basePattern.eq("watchlist_id", watchlistId) : basePattern,
  ]);

  if (cluster.error) throw new Error(`Cluster summary error: ${cluster.error.message}`);
  if (regime.error) throw new Error(`Regime rotation summary error: ${regime.error.message}`);
  if (drift.error) throw new Error(`Drift event summary error: ${drift.error.message}`);
  if (pattern.error) throw new Error(`Run pattern cluster summary error: ${pattern.error.message}`);

  return {
    clusterSummary: (cluster.data ?? []) as CrossAssetArchetypeClusterSummaryRow[],
    regimeRotationSummary: (regime.data ?? []) as CrossAssetArchetypeRegimeRotationSummaryRow[],
    driftEventSummary: (drift.data ?? []) as CrossAssetPatternDriftEventSummaryRow[],
    runPatternClusterSummary: (pattern.data ?? []) as RunCrossAssetPatternClusterSummaryRow[],
  };
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

export async function getCrossAssetClusterAttributionMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  familyClusterAttributionSummary: CrossAssetFamilyClusterAttributionSummaryRow[];
  symbolClusterAttributionSummary: CrossAssetSymbolClusterAttributionSummaryRow[];
  runClusterAttributionSummary: RunCrossAssetClusterAttributionSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseFamily = supabase
    .from("cross_asset_family_cluster_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseSymbol = supabase
    .from("cross_asset_symbol_cluster_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(300);

  const baseRun = supabase
    .from("run_cross_asset_cluster_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const [family, symbol, run] = await Promise.all([
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseSymbol.eq("watchlist_id", watchlistId) : baseSymbol,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (family.error) throw new Error(`Cluster family attribution error: ${family.error.message}`);
  if (symbol.error) throw new Error(`Cluster symbol attribution error: ${symbol.error.message}`);
  if (run.error) throw new Error(`Cluster run integration error: ${run.error.message}`);

  return {
    familyClusterAttributionSummary: (family.data ?? []) as CrossAssetFamilyClusterAttributionSummaryRow[],
    symbolClusterAttributionSummary: (symbol.data ?? []) as CrossAssetSymbolClusterAttributionSummaryRow[],
    runClusterAttributionSummary: (run.data ?? []) as RunCrossAssetClusterAttributionSummaryRow[],
  };
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

export async function getCrossAssetClusterCompositeMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  clusterCompositeSummary: CrossAssetClusterCompositeSummaryRow[];
  familyClusterCompositeSummary: CrossAssetFamilyClusterCompositeSummaryRow[];
  finalClusterIntegrationSummary: RunCrossAssetClusterIntegrationSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseComposite = supabase
    .from("cross_asset_cluster_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const baseFamily = supabase
    .from("cross_asset_family_cluster_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseFinal = supabase
    .from("run_cross_asset_cluster_integration_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const [composite, family, finalRun] = await Promise.all([
    watchlistId ? baseComposite.eq("watchlist_id", watchlistId) : baseComposite,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseFinal.eq("watchlist_id", watchlistId) : baseFinal,
  ]);

  if (composite.error) throw new Error(`Cluster composite summary error: ${composite.error.message}`);
  if (family.error) throw new Error(`Family cluster composite error: ${family.error.message}`);
  if (finalRun.error) throw new Error(`Cluster integration summary error: ${finalRun.error.message}`);

  return {
    clusterCompositeSummary: (composite.data ?? []) as CrossAssetClusterCompositeSummaryRow[],
    familyClusterCompositeSummary: (family.data ?? []) as CrossAssetFamilyClusterCompositeSummaryRow[],
    finalClusterIntegrationSummary: (finalRun.data ?? []) as RunCrossAssetClusterIntegrationSummaryRow[],
  };
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

export async function getCrossAssetClusterReplayValidationMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  clusterReplayValidationSummary: CrossAssetClusterReplayValidationSummaryRow[];
  familyClusterReplayStabilitySummary: CrossAssetFamilyClusterReplayStabilitySummaryRow[];
  clusterReplayStabilityAggregate: CrossAssetClusterReplayStabilityAggregateRow | null;
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseValidation = supabase
    .from("cross_asset_cluster_replay_validation_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const baseFamily = supabase
    .from("cross_asset_family_cluster_replay_stability_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const [validation, family, aggregate] = await Promise.all([
    watchlistId ? baseValidation.eq("watchlist_id", watchlistId) : baseValidation,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    supabase
      .from("cross_asset_cluster_replay_stability_aggregate")
      .select("*")
      .eq("workspace_id", workspaceId)
      .limit(1)
      .maybeSingle(),
  ]);

  if (validation.error) throw new Error(`Cluster replay validation error: ${validation.error.message}`);
  if (family.error) throw new Error(`Family cluster replay stability error: ${family.error.message}`);
  if (aggregate.error) throw new Error(`Cluster replay stability aggregate error: ${aggregate.error.message}`);

  return {
    clusterReplayValidationSummary: (validation.data ?? []) as CrossAssetClusterReplayValidationSummaryRow[],
    familyClusterReplayStabilitySummary: (family.data ?? []) as CrossAssetFamilyClusterReplayStabilitySummaryRow[],
    clusterReplayStabilityAggregate: (aggregate.data as CrossAssetClusterReplayStabilityAggregateRow | null) ?? null,
  };
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

export async function getCrossAssetPersistenceMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  statePersistenceSummary: CrossAssetStatePersistenceSummaryRow[];
  regimeMemorySummary: CrossAssetRegimeMemorySummaryRow[];
  persistenceEventSummary: CrossAssetPersistenceTransitionEventSummaryRow[];
  runPersistenceSummary: RunCrossAssetPersistenceSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseState = supabase
    .from("cross_asset_state_persistence_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const baseEvents = supabase
    .from("cross_asset_persistence_transition_event_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(100);

  const baseRun = supabase
    .from("run_cross_asset_persistence_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const [stateRows, regimeRows, eventRows, runRows] = await Promise.all([
    watchlistId ? baseState.eq("watchlist_id", watchlistId) : baseState,
    supabase
      .from("cross_asset_regime_memory_summary")
      .select("*")
      .eq("workspace_id", workspaceId)
      .order("created_at", { ascending: false })
      .limit(50),
    watchlistId ? baseEvents.eq("watchlist_id", watchlistId) : baseEvents,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (stateRows.error) throw new Error(`State persistence summary error: ${stateRows.error.message}`);
  if (regimeRows.error) throw new Error(`Regime memory summary error: ${regimeRows.error.message}`);
  if (eventRows.error) throw new Error(`Persistence event summary error: ${eventRows.error.message}`);
  if (runRows.error) throw new Error(`Run persistence summary error: ${runRows.error.message}`);

  return {
    statePersistenceSummary: (stateRows.data ?? []) as CrossAssetStatePersistenceSummaryRow[],
    regimeMemorySummary: (regimeRows.data ?? []) as CrossAssetRegimeMemorySummaryRow[],
    persistenceEventSummary: (eventRows.data ?? []) as CrossAssetPersistenceTransitionEventSummaryRow[],
    runPersistenceSummary: (runRows.data ?? []) as RunCrossAssetPersistenceSummaryRow[],
  };
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

export async function getCrossAssetPersistenceAttributionMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  familyPersistenceAttributionSummary: CrossAssetFamilyPersistenceAttributionSummaryRow[];
  symbolPersistenceAttributionSummary: CrossAssetSymbolPersistenceAttributionSummaryRow[];
  runPersistenceAttributionSummary: RunCrossAssetPersistenceAttributionSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseFamily = supabase
    .from("cross_asset_family_persistence_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseSymbol = supabase
    .from("cross_asset_symbol_persistence_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(300);

  const baseRun = supabase
    .from("run_cross_asset_persistence_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const [family, symbol, run] = await Promise.all([
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseSymbol.eq("watchlist_id", watchlistId) : baseSymbol,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (family.error) throw new Error(`Persistence family attribution error: ${family.error.message}`);
  if (symbol.error) throw new Error(`Persistence symbol attribution error: ${symbol.error.message}`);
  if (run.error) throw new Error(`Persistence run integration error: ${run.error.message}`);

  return {
    familyPersistenceAttributionSummary: (family.data ?? []) as CrossAssetFamilyPersistenceAttributionSummaryRow[],
    symbolPersistenceAttributionSummary: (symbol.data ?? []) as CrossAssetSymbolPersistenceAttributionSummaryRow[],
    runPersistenceAttributionSummary: (run.data ?? []) as RunCrossAssetPersistenceAttributionSummaryRow[],
  };
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

export async function getCrossAssetPersistenceCompositeMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  persistenceCompositeSummary: CrossAssetPersistenceCompositeSummaryRow[];
  familyPersistenceCompositeSummary: CrossAssetFamilyPersistenceCompositeSummaryRow[];
  finalPersistenceIntegrationSummary: RunCrossAssetPersistenceIntegrationSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseComposite = supabase
    .from("cross_asset_persistence_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const baseFamily = supabase
    .from("cross_asset_family_persistence_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseFinal = supabase
    .from("run_cross_asset_persistence_integration_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const [composite, family, finalRun] = await Promise.all([
    watchlistId ? baseComposite.eq("watchlist_id", watchlistId) : baseComposite,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseFinal.eq("watchlist_id", watchlistId) : baseFinal,
  ]);

  if (composite.error) throw new Error(`Persistence composite summary error: ${composite.error.message}`);
  if (family.error) throw new Error(`Family persistence composite error: ${family.error.message}`);
  if (finalRun.error) throw new Error(`Persistence integration summary error: ${finalRun.error.message}`);

  return {
    persistenceCompositeSummary: (composite.data ?? []) as CrossAssetPersistenceCompositeSummaryRow[],
    familyPersistenceCompositeSummary: (family.data ?? []) as CrossAssetFamilyPersistenceCompositeSummaryRow[],
    finalPersistenceIntegrationSummary: (finalRun.data ?? []) as RunCrossAssetPersistenceIntegrationSummaryRow[],
  };
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

export async function getCrossAssetPersistenceReplayValidationMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  persistenceReplayValidationSummary: CrossAssetPersistenceReplayValidationSummaryRow[];
  familyPersistenceReplayStabilitySummary: CrossAssetFamilyPersistenceReplayStabilitySummaryRow[];
  persistenceReplayStabilityAggregate: CrossAssetPersistenceReplayStabilityAggregateRow | null;
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseValidation = supabase
    .from("cross_asset_persistence_replay_validation_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(40);

  const baseFamily = supabase
    .from("cross_asset_family_persistence_replay_stability_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const [validation, family, aggregate] = await Promise.all([
    watchlistId ? baseValidation.eq("watchlist_id", watchlistId) : baseValidation,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    supabase
      .from("cross_asset_persistence_replay_stability_aggregate")
      .select("*")
      .eq("workspace_id", workspaceId)
      .limit(1)
      .maybeSingle(),
  ]);

  if (validation.error) throw new Error(`Persistence replay validation error: ${validation.error.message}`);
  if (family.error) throw new Error(`Family persistence replay stability error: ${family.error.message}`);
  if (aggregate.error) throw new Error(`Persistence replay stability aggregate error: ${aggregate.error.message}`);

  return {
    persistenceReplayValidationSummary: (validation.data ?? []) as CrossAssetPersistenceReplayValidationSummaryRow[],
    familyPersistenceReplayStabilitySummary: (family.data ?? []) as CrossAssetFamilyPersistenceReplayStabilitySummaryRow[],
    persistenceReplayStabilityAggregate: (aggregate.data as CrossAssetPersistenceReplayStabilityAggregateRow | null) ?? null,
  };
}

// ── Phase 4.7A: Signal Decay & Stale-Memory Diagnostics ─────────────────
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
  freshness_state: "fresh" | "decaying" | "stale" | "contradicted" | "mixed" | "insufficient_history";
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
  family_freshness_state: "fresh" | "decaying" | "stale" | "contradicted" | "mixed" | "insufficient_history";
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
  prior_freshness_state: string | null;
  current_freshness_state: string;
  prior_state_signature: string | null;
  current_state_signature: string;
  prior_memory_score: number | string | null;
  current_memory_score: number | string | null;
  prior_aggregate_decay_score: number | string | null;
  current_aggregate_decay_score: number | string | null;
  event_type: "memory_freshened" | "memory_decayed" | "memory_became_stale" | "memory_contradicted" | "memory_reconfirmed" | "insufficient_history";
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
  freshness_state: string | null;
  aggregate_decay_score: number | string | null;
  stale_memory_flag: boolean;
  contradiction_flag: boolean;
  latest_stale_memory_event_type: string | null;
  created_at: string;
}

export async function getCrossAssetSignalDecayMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  signalDecaySummary: CrossAssetSignalDecaySummaryRow[];
  familySignalDecaySummary: CrossAssetFamilySignalDecaySummaryRow[];
  staleMemoryEventSummary: CrossAssetStaleMemoryEventSummaryRow[];
  runSignalDecaySummary: RunCrossAssetSignalDecaySummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseDecay = supabase
    .from("cross_asset_signal_decay_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const baseFamily = supabase
    .from("cross_asset_family_signal_decay_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseEvents = supabase
    .from("cross_asset_stale_memory_event_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(100);

  const baseRun = supabase
    .from("run_cross_asset_signal_decay_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const [decayRows, familyRows, eventRows, runRows] = await Promise.all([
    watchlistId ? baseDecay.eq("watchlist_id", watchlistId) : baseDecay,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseEvents.eq("watchlist_id", watchlistId) : baseEvents,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (decayRows.error) throw new Error(`Signal decay summary error: ${decayRows.error.message}`);
  if (familyRows.error) throw new Error(`Family signal decay summary error: ${familyRows.error.message}`);
  if (eventRows.error) throw new Error(`Stale memory event summary error: ${eventRows.error.message}`);
  if (runRows.error) throw new Error(`Run signal decay summary error: ${runRows.error.message}`);

  return {
    signalDecaySummary: (decayRows.data ?? []) as CrossAssetSignalDecaySummaryRow[],
    familySignalDecaySummary: (familyRows.data ?? []) as CrossAssetFamilySignalDecaySummaryRow[],
    staleMemoryEventSummary: (eventRows.data ?? []) as CrossAssetStaleMemoryEventSummaryRow[],
    runSignalDecaySummary: (runRows.data ?? []) as RunCrossAssetSignalDecaySummaryRow[],
  };
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
  freshness_state: "fresh" | "decaying" | "stale" | "contradicted" | "mixed" | "insufficient_history";
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

export async function getCrossAssetDecayAttributionMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  familyDecayAttributionSummary: CrossAssetFamilyDecayAttributionSummaryRow[];
  symbolDecayAttributionSummary: CrossAssetSymbolDecayAttributionSummaryRow[];
  runDecayAttributionSummary: RunCrossAssetDecayAttributionSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseFamily = supabase
    .from("cross_asset_family_decay_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseSymbol = supabase
    .from("cross_asset_symbol_decay_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(300);

  const baseRun = supabase
    .from("run_cross_asset_decay_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const [familyRows, symbolRows, runRows] = await Promise.all([
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseSymbol.eq("watchlist_id", watchlistId) : baseSymbol,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (familyRows.error) throw new Error(`Family decay attribution summary error: ${familyRows.error.message}`);
  if (symbolRows.error) throw new Error(`Symbol decay attribution summary error: ${symbolRows.error.message}`);
  if (runRows.error) throw new Error(`Run decay attribution summary error: ${runRows.error.message}`);

  return {
    familyDecayAttributionSummary: (familyRows.data ?? []) as CrossAssetFamilyDecayAttributionSummaryRow[],
    symbolDecayAttributionSummary: (symbolRows.data ?? []) as CrossAssetSymbolDecayAttributionSummaryRow[],
    runDecayAttributionSummary: (runRows.data ?? []) as RunCrossAssetDecayAttributionSummaryRow[],
  };
}

// ── Phase 4.7C: Decay-Aware Composite Refinement ────────────────────────
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
  freshness_state: "fresh" | "decaying" | "stale" | "contradicted" | "mixed" | "insufficient_history";
  aggregate_decay_score: number | string | null;
  stale_memory_flag: boolean;
  contradiction_flag: boolean;
  integration_mode: "decay_additive_guardrailed" | "fresh_confirmation_only" | "stale_suppression_only" | "contradiction_suppression_only";
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

export async function getCrossAssetDecayCompositeMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  decayCompositeSummary: CrossAssetDecayCompositeSummaryRow[];
  familyDecayCompositeSummary: CrossAssetFamilyDecayCompositeSummaryRow[];
  finalDecayIntegrationSummary: RunCrossAssetDecayIntegrationSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseComposite = supabase
    .from("cross_asset_decay_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const baseFamily = supabase
    .from("cross_asset_family_decay_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseFinal = supabase
    .from("run_cross_asset_decay_integration_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const [compositeRows, familyRows, finalRows] = await Promise.all([
    watchlistId ? baseComposite.eq("watchlist_id", watchlistId) : baseComposite,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseFinal.eq("watchlist_id", watchlistId) : baseFinal,
  ]);

  if (compositeRows.error) throw new Error(`Decay composite summary error: ${compositeRows.error.message}`);
  if (familyRows.error) throw new Error(`Family decay composite summary error: ${familyRows.error.message}`);
  if (finalRows.error) throw new Error(`Final decay integration summary error: ${finalRows.error.message}`);

  return {
    decayCompositeSummary: (compositeRows.data ?? []) as CrossAssetDecayCompositeSummaryRow[],
    familyDecayCompositeSummary: (familyRows.data ?? []) as CrossAssetFamilyDecayCompositeSummaryRow[],
    finalDecayIntegrationSummary: (finalRows.data ?? []) as RunCrossAssetDecayIntegrationSummaryRow[],
  };
}

// ── Phase 4.7D: Replay Validation for Decay-Aware Composite ─────────────
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
    | "persistence_mismatch"
    | "decay_mismatch";
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

export async function getCrossAssetDecayReplayValidationMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  decayReplayValidationSummary: CrossAssetDecayReplayValidationSummaryRow[];
  familyDecayReplayStabilitySummary: CrossAssetFamilyDecayReplayStabilitySummaryRow[];
  decayReplayStabilityAggregate: CrossAssetDecayReplayStabilityAggregateRow | null;
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseValidation = supabase
    .from("cross_asset_decay_replay_validation_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const baseFamily = supabase
    .from("cross_asset_family_decay_replay_stability_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const [validation, family, aggregate] = await Promise.all([
    watchlistId ? baseValidation.eq("watchlist_id", watchlistId) : baseValidation,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    supabase
      .from("cross_asset_decay_replay_stability_aggregate")
      .select("*")
      .eq("workspace_id", workspaceId)
      .limit(1)
      .maybeSingle(),
  ]);

  if (validation.error) throw new Error(`Decay replay validation summary error: ${validation.error.message}`);
  if (family.error) throw new Error(`Family decay replay stability error: ${family.error.message}`);
  if (aggregate.error) throw new Error(`Decay replay stability aggregate error: ${aggregate.error.message}`);

  return {
    decayReplayValidationSummary: (validation.data ?? []) as CrossAssetDecayReplayValidationSummaryRow[],
    familyDecayReplayStabilitySummary: (family.data ?? []) as CrossAssetFamilyDecayReplayStabilitySummaryRow[],
    decayReplayStabilityAggregate: (aggregate.data as CrossAssetDecayReplayStabilityAggregateRow | null) ?? null,
  };
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

export async function getCrossAssetLayerConflictMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  layerAgreementSummary: CrossAssetLayerAgreementSummaryRow[];
  familyLayerAgreementSummary: CrossAssetFamilyLayerAgreementSummaryRow[];
  layerConflictEventSummary: CrossAssetLayerConflictEventSummaryRow[];
  runLayerConflictSummary: RunCrossAssetLayerConflictSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseAgreement = supabase
    .from("cross_asset_layer_agreement_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const baseFamily = supabase
    .from("cross_asset_family_layer_agreement_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseEvent = supabase
    .from("cross_asset_layer_conflict_event_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(100);

  const baseRun = supabase
    .from("run_cross_asset_layer_conflict_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const [agreement, family, events, run] = await Promise.all([
    watchlistId ? baseAgreement.eq("watchlist_id", watchlistId) : baseAgreement,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseEvent.eq("watchlist_id", watchlistId) : baseEvent,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (agreement.error) throw new Error(`Layer agreement summary error: ${agreement.error.message}`);
  if (family.error) throw new Error(`Family layer agreement summary error: ${family.error.message}`);
  if (events.error) throw new Error(`Layer conflict event summary error: ${events.error.message}`);
  if (run.error) throw new Error(`Run layer conflict summary error: ${run.error.message}`);

  return {
    layerAgreementSummary: (agreement.data ?? []) as CrossAssetLayerAgreementSummaryRow[],
    familyLayerAgreementSummary: (family.data ?? []) as CrossAssetFamilyLayerAgreementSummaryRow[],
    layerConflictEventSummary: (events.data ?? []) as CrossAssetLayerConflictEventSummaryRow[],
    runLayerConflictSummary: (run.data ?? []) as RunCrossAssetLayerConflictSummaryRow[],
  };
}

// ── Phase 4.8B: Conflict-Aware Attribution ─────────────────────────────────

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
  raw_family_net_contribution: number | null;
  weighted_family_net_contribution: number | null;
  regime_adjusted_family_contribution: number | null;
  timing_adjusted_family_contribution: number | null;
  transition_adjusted_family_contribution: number | null;
  archetype_adjusted_family_contribution: number | null;
  cluster_adjusted_family_contribution: number | null;
  persistence_adjusted_family_contribution: number | null;
  decay_adjusted_family_contribution: number | null;
  family_consensus_state: CrossAssetFamilyConsensusState;
  agreement_score: number | null;
  conflict_score: number | null;
  dominant_conflict_source: string | null;
  transition_direction: string | null;
  archetype_direction: string | null;
  cluster_direction: string | null;
  persistence_direction: string | null;
  decay_direction: string | null;
  conflict_weight: number | null;
  conflict_bonus: number | null;
  conflict_penalty: number | null;
  conflict_adjusted_family_contribution: number | null;
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
  agreement_score: number | null;
  conflict_score: number | null;
  dominant_conflict_source: string | null;
  raw_symbol_score: number | null;
  weighted_symbol_score: number | null;
  regime_adjusted_symbol_score: number | null;
  timing_adjusted_symbol_score: number | null;
  transition_adjusted_symbol_score: number | null;
  archetype_adjusted_symbol_score: number | null;
  cluster_adjusted_symbol_score: number | null;
  persistence_adjusted_symbol_score: number | null;
  decay_adjusted_symbol_score: number | null;
  conflict_weight: number | null;
  conflict_adjusted_symbol_score: number | null;
  symbol_rank: number | null;
  reason_codes: string[] | null;
  created_at: string;
}

export interface RunCrossAssetConflictAttributionSummaryRow {
  run_id: string;
  workspace_id: string;
  watchlist_id: string;
  context_snapshot_id: string | null;
  cross_asset_net_contribution: number | null;
  weighted_cross_asset_net_contribution: number | null;
  regime_adjusted_cross_asset_contribution: number | null;
  timing_adjusted_cross_asset_contribution: number | null;
  transition_adjusted_cross_asset_contribution: number | null;
  archetype_adjusted_cross_asset_contribution: number | null;
  cluster_adjusted_cross_asset_contribution: number | null;
  persistence_adjusted_cross_asset_contribution: number | null;
  decay_adjusted_cross_asset_contribution: number | null;
  conflict_adjusted_cross_asset_contribution: number | null;
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
  agreement_score: number | null;
  conflict_score: number | null;
  dominant_conflict_source: string | null;
  created_at: string;
}

export async function getCrossAssetConflictAttributionMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  familyConflictAttributionSummary: CrossAssetFamilyConflictAttributionSummaryRow[];
  symbolConflictAttributionSummary: CrossAssetSymbolConflictAttributionSummaryRow[];
  runConflictAttributionSummary: RunCrossAssetConflictAttributionSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseFamily = supabase
    .from("cross_asset_family_conflict_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .order("conflict_family_rank", { ascending: true, nullsFirst: false })
    .limit(200);

  const baseSymbol = supabase
    .from("cross_asset_symbol_conflict_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .order("symbol_rank", { ascending: true, nullsFirst: false })
    .limit(300);

  const baseRun = supabase
    .from("run_cross_asset_conflict_attribution_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const [family, symbol, run] = await Promise.all([
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseSymbol.eq("watchlist_id", watchlistId) : baseSymbol,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (family.error) throw new Error(`Family conflict attribution error: ${family.error.message}`);
  if (symbol.error) throw new Error(`Symbol conflict attribution error: ${symbol.error.message}`);
  if (run.error) throw new Error(`Run conflict attribution error: ${run.error.message}`);

  return {
    familyConflictAttributionSummary: (family.data ?? []) as CrossAssetFamilyConflictAttributionSummaryRow[],
    symbolConflictAttributionSummary: (symbol.data ?? []) as CrossAssetSymbolConflictAttributionSummaryRow[],
    runConflictAttributionSummary: (run.data ?? []) as RunCrossAssetConflictAttributionSummaryRow[],
  };
}

// ── Phase 4.8C: Conflict-Aware Composite Refinement ─────────────────────

export interface CrossAssetConflictCompositeSummaryRow {
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
  conflict_adjusted_cross_asset_contribution: number | string | null;
  composite_pre_conflict: number | string | null;
  conflict_net_contribution: number | string | null;
  composite_post_conflict: number | string | null;
  layer_consensus_state: string;
  agreement_score: number | string | null;
  conflict_score: number | string | null;
  dominant_conflict_source: string | null;
  integration_mode: string;
  source_contribution_layer: string | null;
  source_composite_layer: string | null;
  scoring_version: string;
  created_at: string;
}

export interface CrossAssetFamilyConflictCompositeSummaryRow {
  workspace_id: string;
  watchlist_id: string;
  run_id: string;
  context_snapshot_id: string | null;
  dependency_family: string;
  family_consensus_state: string;
  agreement_score: number | string | null;
  conflict_score: number | string | null;
  dominant_conflict_source: string | null;
  conflict_adjusted_family_contribution: number | string | null;
  integration_weight_applied: number | string | null;
  conflict_integration_contribution: number | string | null;
  family_rank: number | null;
  top_symbols: string[];
  reason_codes: string[];
  source_contribution_layer: string | null;
  scoring_version: string;
  created_at: string;
}

export interface RunCrossAssetConflictIntegrationSummaryRow {
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
  conflict_net_contribution: number | string | null;
  composite_pre_conflict: number | string | null;
  composite_post_conflict: number | string | null;
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
  integration_mode: string | null;
  source_contribution_layer: string | null;
  source_composite_layer: string | null;
  scoring_version: string | null;
  created_at: string;
}

export async function getCrossAssetConflictCompositeMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  conflictCompositeSummary: CrossAssetConflictCompositeSummaryRow[];
  familyConflictCompositeSummary: CrossAssetFamilyConflictCompositeSummaryRow[];
  finalConflictIntegrationSummary: RunCrossAssetConflictIntegrationSummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseComposite = supabase
    .from("cross_asset_conflict_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const baseFamily = supabase
    .from("cross_asset_family_conflict_composite_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .order("family_rank", { ascending: true, nullsFirst: false })
    .limit(200);

  const baseFinal = supabase
    .from("run_cross_asset_conflict_integration_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const [composite, family, final] = await Promise.all([
    watchlistId ? baseComposite.eq("watchlist_id", watchlistId) : baseComposite,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseFinal.eq("watchlist_id", watchlistId) : baseFinal,
  ]);

  if (composite.error) throw new Error(`Conflict composite summary error: ${composite.error.message}`);
  if (family.error) throw new Error(`Family conflict composite summary error: ${family.error.message}`);
  if (final.error) throw new Error(`Final conflict integration summary error: ${final.error.message}`);

  return {
    conflictCompositeSummary: (composite.data ?? []) as CrossAssetConflictCompositeSummaryRow[],
    familyConflictCompositeSummary: (family.data ?? []) as CrossAssetFamilyConflictCompositeSummaryRow[],
    finalConflictIntegrationSummary: (final.data ?? []) as RunCrossAssetConflictIntegrationSummaryRow[],
  };
}

// ── Phase 4.8D: Replay Validation for Conflict-Aware Behavior ───────────

export interface CrossAssetConflictReplayValidationSummaryRow {
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
  source_freshness_state: string | null;
  replay_freshness_state: string | null;
  source_layer_consensus_state: string | null;
  replay_layer_consensus_state: string | null;
  source_agreement_score: number | string | null;
  replay_agreement_score: number | string | null;
  source_conflict_score: number | string | null;
  replay_conflict_score: number | string | null;
  source_dominant_conflict_source: string | null;
  replay_dominant_conflict_source: string | null;
  source_contribution_layer: string | null;
  replay_contribution_layer: string | null;
  source_composite_layer: string | null;
  replay_composite_layer: string | null;
  source_scoring_version: string | null;
  replay_scoring_version: string | null;
  context_hash_match: boolean;
  regime_match: boolean;
  timing_class_match: boolean;
  transition_state_match: boolean;
  sequence_class_match: boolean;
  archetype_match: boolean;
  cluster_state_match: boolean;
  persistence_state_match: boolean;
  freshness_state_match: boolean;
  layer_consensus_state_match: boolean;
  agreement_score_match: boolean;
  conflict_score_match: boolean;
  dominant_conflict_source_match: boolean;
  source_contribution_layer_match: boolean;
  source_composite_layer_match: boolean;
  scoring_version_match: boolean;
  conflict_attribution_match: boolean;
  conflict_composite_match: boolean;
  conflict_dominant_family_match: boolean;
  drift_reason_codes: string[];
  validation_state: string;
  created_at: string;
}

export interface CrossAssetFamilyConflictReplayStabilitySummaryRow {
  workspace_id: string;
  watchlist_id: string;
  source_run_id: string;
  replay_run_id: string;
  dependency_family: string;
  source_family_consensus_state: string | null;
  replay_family_consensus_state: string | null;
  source_agreement_score: number | string | null;
  replay_agreement_score: number | string | null;
  source_conflict_score: number | string | null;
  replay_conflict_score: number | string | null;
  source_dominant_conflict_source: string | null;
  replay_dominant_conflict_source: string | null;
  source_contribution_layer: string | null;
  replay_contribution_layer: string | null;
  source_scoring_version: string | null;
  replay_scoring_version: string | null;
  source_conflict_adjusted_contribution: number | string | null;
  replay_conflict_adjusted_contribution: number | string | null;
  source_conflict_integration_contribution: number | string | null;
  replay_conflict_integration_contribution: number | string | null;
  conflict_adjusted_delta: number | string | null;
  conflict_integration_delta: number | string | null;
  family_consensus_state_match: boolean;
  agreement_score_match: boolean;
  conflict_score_match: boolean;
  dominant_conflict_source_match: boolean;
  source_contribution_layer_match: boolean;
  scoring_version_match: boolean;
  conflict_family_rank_match: boolean;
  conflict_composite_family_rank_match: boolean;
  drift_reason_codes: string[];
  created_at: string;
}

export interface CrossAssetConflictReplayStabilityAggregateRow {
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
  freshness_state_match_rate: number | string | null;
  layer_consensus_state_match_rate: number | string | null;
  agreement_score_match_rate: number | string | null;
  conflict_score_match_rate: number | string | null;
  dominant_conflict_source_match_rate: number | string | null;
  source_contribution_layer_match_rate: number | string | null;
  source_composite_layer_match_rate: number | string | null;
  scoring_version_match_rate: number | string | null;
  conflict_attribution_match_rate: number | string | null;
  conflict_composite_match_rate: number | string | null;
  conflict_dominant_family_match_rate: number | string | null;
  drift_detected_count: number;
  latest_validated_at: string | null;
}

export async function getCrossAssetConflictReplayValidationMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  conflictReplayValidationSummary: CrossAssetConflictReplayValidationSummaryRow[];
  familyConflictReplayStabilitySummary: CrossAssetFamilyConflictReplayStabilitySummaryRow[];
  conflictReplayStabilityAggregate: CrossAssetConflictReplayStabilityAggregateRow | null;
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseValidation = supabase
    .from("cross_asset_conflict_replay_validation_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const baseFamily = supabase
    .from("cross_asset_family_conflict_replay_stability_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const aggregateQuery = supabase
    .from("cross_asset_conflict_replay_stability_aggregate")
    .select("*")
    .eq("workspace_id", workspaceId)
    .limit(1);

  const [validation, family, aggregate] = await Promise.all([
    watchlistId ? baseValidation.eq("watchlist_id", watchlistId) : baseValidation,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    aggregateQuery,
  ]);

  if (validation.error) throw new Error(`Conflict replay validation error: ${validation.error.message}`);
  if (family.error) throw new Error(`Family conflict replay stability error: ${family.error.message}`);
  if (aggregate.error) throw new Error(`Conflict replay stability aggregate error: ${aggregate.error.message}`);

  return {
    conflictReplayValidationSummary: (validation.data ?? []) as CrossAssetConflictReplayValidationSummaryRow[],
    familyConflictReplayStabilitySummary: (family.data ?? []) as CrossAssetFamilyConflictReplayStabilitySummaryRow[],
    conflictReplayStabilityAggregate: ((aggregate.data ?? [])[0] ?? null) as CrossAssetConflictReplayStabilityAggregateRow | null,
  };
}
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
  freshness_state: "fresh" | "decaying" | "stale" | "contradicted" | "mixed" | "insufficient_history";
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
  family_freshness_state: "fresh" | "decaying" | "stale" | "contradicted" | "mixed" | "insufficient_history";
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
  prior_freshness_state: string | null;
  current_freshness_state: string;
  prior_state_signature: string | null;
  current_state_signature: string;
  prior_memory_score: number | string | null;
  current_memory_score: number | string | null;
  prior_aggregate_decay_score: number | string | null;
  current_aggregate_decay_score: number | string | null;
  event_type: "memory_freshened" | "memory_decayed" | "memory_became_stale" | "memory_contradicted" | "memory_reconfirmed" | "insufficient_history";
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
  freshness_state: string | null;
  aggregate_decay_score: number | string | null;
  stale_memory_flag: boolean;
  contradiction_flag: boolean;
  latest_stale_memory_event_type: string | null;
  created_at: string;
}

export async function getCrossAssetSignalDecayMetrics(
  workspaceSlug: string,
  watchlistSlug?: string,
): Promise<{
  signalDecaySummary: CrossAssetSignalDecaySummaryRow[];
  familySignalDecaySummary: CrossAssetFamilySignalDecaySummaryRow[];
  staleMemoryEventSummary: CrossAssetStaleMemoryEventSummaryRow[];
  runSignalDecaySummary: RunCrossAssetSignalDecaySummaryRow[];
}> {
  const supabase = createServiceSupabaseClient();
  const workspaceId = await getWorkspaceId(workspaceSlug);

  let watchlistId: string | null = null;
  if (watchlistSlug) {
    type WlResult = { data: { id: string } | null; error: { message: string } | null };
    const wl = await supabase
      .from("watchlists")
      .select("id")
      .eq("workspace_id", workspaceId)
      .eq("slug", watchlistSlug)
      .single() as unknown as WlResult;
    if (wl.error || !wl.data) throw new Error(`Watchlist not found: ${watchlistSlug}`);
    watchlistId = wl.data.id;
  }

  const baseDecay = supabase
    .from("cross_asset_signal_decay_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const baseFamily = supabase
    .from("cross_asset_family_signal_decay_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(200);

  const baseEvents = supabase
    .from("cross_asset_stale_memory_event_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(100);

  const baseRun = supabase
    .from("run_cross_asset_signal_decay_summary")
    .select("*")
    .eq("workspace_id", workspaceId)
    .order("created_at", { ascending: false })
    .limit(50);

  const [decayRows, familyRows, eventRows, runRows] = await Promise.all([
    watchlistId ? baseDecay.eq("watchlist_id", watchlistId) : baseDecay,
    watchlistId ? baseFamily.eq("watchlist_id", watchlistId) : baseFamily,
    watchlistId ? baseEvents.eq("watchlist_id", watchlistId) : baseEvents,
    watchlistId ? baseRun.eq("watchlist_id", watchlistId) : baseRun,
  ]);

  if (decayRows.error) throw new Error(`Signal decay summary error: ${decayRows.error.message}`);
  if (familyRows.error) throw new Error(`Family signal decay summary error: ${familyRows.error.message}`);
  if (eventRows.error) throw new Error(`Stale memory event summary error: ${eventRows.error.message}`);
  if (runRows.error) throw new Error(`Run signal decay summary error: ${runRows.error.message}`);

  return {
    signalDecaySummary: (decayRows.data ?? []) as CrossAssetSignalDecaySummaryRow[],
    familySignalDecaySummary: (familyRows.data ?? []) as CrossAssetFamilySignalDecaySummaryRow[],
    staleMemoryEventSummary: (eventRows.data ?? []) as CrossAssetStaleMemoryEventSummaryRow[],
    runSignalDecaySummary: (runRows.data ?? []) as RunCrossAssetSignalDecaySummaryRow[],
  };
}
