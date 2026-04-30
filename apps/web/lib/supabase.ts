import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { env } from "@/lib/env";

type TableDef<T extends Record<string, unknown>> = {
  Row: T;
  Insert: Partial<T>;
  Update: Partial<T>;
  Relationships: [];
};

export type Database = {
  public: {
    Tables: {
      workspaces: TableDef<{ id: string; slug: string; name: string; created_at: string }>;
      assets: TableDef<{ id: string; symbol: string; name: string; asset_class: string; created_at: string }>;
      watchlists: TableDef<{ id: string; workspace_id: string; slug: string; name: string; created_at: string }>;
      watchlist_assets: TableDef<{ watchlist_id: string; asset_id: string; sort_order: number; created_at: string }>;
      alert_events: TableDef<{
        id: string; workspace_id: string; job_id: string | null; alert_type: string;
        severity: string; title: string; message: string;
        payload: Record<string, unknown>; metadata: Record<string, unknown>;
        delivered_channels: unknown[]; created_at: string;
      }>;
      alert_rules: TableDef<{
        id: string; workspace_id: string; rule_key: string; channel: string;
        is_enabled: boolean; min_severity: string; config: Record<string, unknown>;
        created_at: string; updated_at: string;
      }>;
      job_runs: TableDef<{
        id: string; workspace_id: string; watchlist_id: string | null; queue_name: string;
        status: "queued" | "claimed" | "running" | "completed" | "failed" | "dead_lettered";
        trigger_type: "api" | "seed" | "cron" | "manual";
        requested_by: string | null; payload: Record<string, unknown>; metadata: Record<string, unknown>;
        attempt_count: number; max_attempts: number; claimed_by: string | null;
        claimed_at: string | null; started_at: string | null; finished_at: string | null;
        queue_id: number | null; lineage: Record<string, unknown>; compute_version: string | null;
        signal_registry_version: string | null; model_version: string | null; runtime_ms: number | null;
        completed_at: string | null; terminal_queue_priority: number | null;
        terminal_retry_count: number | null; terminal_last_error: string | null;
        terminal_queue_status: string | null; terminal_promoted_at: string | null;
        terminal_queued_at: string | null; failure_stage: string | null; failure_code: string | null;
        replayed_from_run_id: string | null; is_replay: boolean; input_snapshot_id: number | null;
        explanation_version: string | null; attribution_version: string | null;
        attribution_reconciled: boolean; attribution_total: number | null;
        attribution_target_total: number | null; attribution_reconciliation_delta: number | null;
        drift_summary: Record<string, unknown>; drift_severity: string | null;
        comparison_run_id: string | null;
        error_message: string | null; created_at: string; updated_at: string;
      }>;
      job_run_stage_timings: TableDef<{
        id: number; job_run_id: string; workspace_id: string; watchlist_id: string | null;
        stage_name: string; stage_status: "completed" | "failed" | "skipped";
        started_at: string; completed_at: string | null; runtime_ms: number | null;
        error_summary: string | null; failure_code: string | null; metadata: Record<string, unknown>;
        created_at: string;
      }>;
      job_run_explanations: TableDef<{
        id: number; job_run_id: string; workspace_id: string; watchlist_id: string | null;
        explanation_version: string; summary: string | null;
        regime_summary: Record<string, unknown>; signal_summary: Record<string, unknown>;
        composite_summary: Record<string, unknown>; invalidator_summary: Record<string, unknown>;
        top_positive_contributors: unknown[]; top_negative_contributors: unknown[];
        metadata: Record<string, unknown>; created_at: string; updated_at: string;
      }>;
      job_run_input_snapshots: TableDef<{
        id: number; job_run_id: string; workspace_id: string; watchlist_id: string | null;
        source_window_start: string | null; source_window_end: string | null; asset_count: number;
        source_coverage: Record<string, unknown>; input_values: Record<string, unknown>;
        version_pins: Record<string, unknown>; metadata: Record<string, unknown>;
        compute_scope_id: string | null; scope_hash: string | null; scope_version: string | null;
        primary_asset_count: number | null; dependency_asset_count: number | null;
        asset_universe_count: number | null;
        created_at: string; updated_at: string;
      }>;
      job_run_compute_scopes: TableDef<{
        id: string; run_id: string; workspace_id: string; watchlist_id: string | null;
        queue_name: string; scope_version: string; primary_assets: unknown[]; dependency_assets: unknown[];
        asset_universe: unknown[]; primary_asset_count: number; dependency_asset_count: number;
        asset_universe_count: number; dependency_policy: Record<string, unknown>;
        scope_hash: string; metadata: Record<string, unknown>; created_at: string;
      }>;
      job_run_attributions: TableDef<{
        id: number; run_id: string; workspace_id: string; watchlist_id: string | null;
        asset_id: string | null; asset_symbol: string | null; regime: string | null;
        signal_name: string; signal_family: string; raw_value: number | null;
        normalized_value: number | null; weight_applied: number; contribution_value: number;
        contribution_direction: "positive" | "negative" | "neutral"; is_invalidator: boolean;
        active_invalidators: unknown[]; metadata: Record<string, unknown>; created_at: string;
      }>;
      job_run_signal_family_attributions: TableDef<{
        id: number; run_id: string; workspace_id: string; watchlist_id: string | null;
        signal_family: string; family_rank: number; family_weight: number; family_score: number;
        family_pct_of_total: number; positive_contribution: number; negative_contribution: number;
        invalidator_contribution: number; active_invalidators: unknown[]; metadata: Record<string, unknown>;
        created_at: string;
      }>;
      job_run_drift_metrics: TableDef<{
        id: number; run_id: string; comparison_run_id: string | null; workspace_id: string;
        watchlist_id: string | null; metric_type: string; entity_name: string;
        current_value: number | null; baseline_value: number | null; delta_abs: number | null;
        delta_pct: number | null; z_score: number | null; drift_flag: boolean; severity: string;
        metadata: Record<string, unknown>; created_at: string;
      }>;
      job_run_replay_deltas: TableDef<{
        replay_run_id: string; source_run_id: string; workspace_id: string; watchlist_id: string | null;
        input_match_score: number; input_match_details: Record<string, unknown>; version_match: boolean;
        compute_version_changed: boolean; signal_registry_version_changed: boolean; model_version_changed: boolean;
        regime_changed: boolean; source_regime: string | null; replay_regime: string | null;
        source_composite: number | null; replay_composite: number | null; composite_delta: number | null;
        composite_delta_abs: number | null; largest_signal_deltas: unknown[]; largest_family_deltas: unknown[];
        summary: Record<string, unknown>; severity: string; created_at: string;
      }>;
      regime_transition_events: TableDef<{
        id: string; run_id: string; prior_run_id: string | null; workspace_id: string; watchlist_id: string | null;
        queue_name: string; from_regime: string | null; to_regime: string | null; transition_detected: boolean;
        transition_classification: string; stability_score: number | null; anomaly_likelihood: number | null;
        composite_shift: number | null; composite_shift_abs: number | null; dominant_family_gained: string | null;
        dominant_family_lost: string | null; source: string; metadata: Record<string, unknown>;
        created_at: string; updated_at: string;
      }>;
      regime_transition_family_shifts: TableDef<{
        id: string; transition_event_id: string; run_id: string; prior_run_id: string | null; workspace_id: string;
        watchlist_id: string | null; signal_family: string; prior_family_score: number | null;
        current_family_score: number | null; family_delta: number | null; family_delta_abs: number | null;
        prior_family_rank: number | null; current_family_rank: number | null; shift_direction: string;
        metadata: Record<string, unknown>; created_at: string;
      }>;
      run_stability_baselines: TableDef<{
        id: string; run_id: string; workspace_id: string; watchlist_id: string | null; queue_name: string;
        window_size: number; baseline_run_count: number; composite_baseline: number | null;
        composite_current: number | null; composite_delta_abs: number | null; composite_delta_pct: number | null;
        composite_instability_score: number; family_instability_score: number; replay_consistency_risk_score: number;
        regime_instability_score: number; dominant_family: string | null; dominant_family_changed: boolean;
        dominant_regime: string | null; regime_changed: boolean; stability_classification: string;
        metadata: Record<string, unknown>; created_at: string;
      }>;
      signal_family_stability_metrics: TableDef<{
        id: string; run_id: string; workspace_id: string; watchlist_id: string | null; signal_family: string;
        family_score_current: number | null; family_score_baseline: number | null; family_delta_abs: number | null;
        family_delta_pct: number | null; instability_score: number; family_rank: number;
        metadata: Record<string, unknown>; created_at: string;
      }>;
      replay_consistency_metrics: TableDef<{
        id: string; run_id: string; workspace_id: string; watchlist_id: string | null; queue_name: string;
        replay_runs_considered: number; mismatch_rate: number | null; avg_input_match_score: number | null;
        avg_composite_delta_abs: number | null; instability_score: number; metadata: Record<string, unknown>;
        created_at: string;
      }>;
      regime_stability_metrics: TableDef<{
        id: string; run_id: string; workspace_id: string; watchlist_id: string | null; queue_name: string;
        transitions_considered: number; conflicting_transition_count: number; abrupt_transition_count: number;
        instability_score: number; metadata: Record<string, unknown>; created_at: string;
      }>;
      composite_scores: TableDef<{
        id: string; workspace_id: string; asset_id: string; timestamp: string; regime: string;
        long_score: number; short_score: number; confidence: number;
        invalidators: Record<string, unknown> | null; created_at: string;
      }>;
      signal_values: TableDef<{
        id: string; workspace_id: string | null; asset_id: string; signal_name: string;
        ts: string; score: number; explanation: Record<string, unknown> | null;
        created_at: string; updated_at: string;
      }>;
      job_dead_letters: TableDef<{
        id: number; job_run_id: string | null; queue_job_id: number | null;
        workspace_id: string; watchlist_id: string | null; job_type: string;
        payload: Record<string, unknown>; retry_count: number; max_retries: number;
        last_error: string | null; failure_stage: string | null;
        failed_at: string; requeued_at: string | null; metadata: Record<string, unknown>;
      }>;
      worker_heartbeats: TableDef<{
        worker_id: string; workspace_id: string | null; hostname: string | null;
        pid: number | null; status: string; capabilities: Record<string, unknown>;
        metadata: Record<string, unknown>; started_at: string; last_seen_at: string;
      }>;
      queue_governance_rules: TableDef<{
        id: number; workspace_id: string; watchlist_id: string | null; job_type: string;
        enabled: boolean; max_concurrent: number; dedupe_window_seconds: number;
        suppress_if_queued: boolean; suppress_if_claimed: boolean;
        manual_priority: number; scheduled_priority: number;
        created_at: string; updated_at: string;
      }>;
      alert_policy_rules: TableDef<{
        id: number; workspace_id: string; watchlist_id: string | null; enabled: boolean;
        event_type: string; severity: string; channel: string;
        notify_on_terminal_only: boolean; cooldown_seconds: number;
        dedupe_key_template: string | null; metadata: Record<string, unknown>;
        created_at: string; updated_at: string;
      }>;
      governance_alert_rules: TableDef<{
        id: string; workspace_id: string | null; rule_name: string; enabled: boolean; event_type: string;
        metric_source: string; metric_name: string; comparator: string; threshold_numeric: number | null;
        threshold_text: string | null; severity: string; cooldown_seconds: number;
        metadata: Record<string, unknown>; created_at: string; updated_at: string;
      }>;
      governance_threshold_profiles: TableDef<{
        id: string; workspace_id: string | null; profile_name: string; is_default: boolean; enabled: boolean;
        version_health_floor: number; family_instability_ceiling: number; replay_consistency_floor: number;
        regime_instability_ceiling: number; conflicting_transition_ceiling: number;
        metadata: Record<string, unknown>; created_at: string; updated_at: string;
      }>;
      regime_threshold_overrides: TableDef<{
        id: string; workspace_id: string | null; regime: string; profile_id: string; enabled: boolean;
        version_health_floor: number | null; family_instability_ceiling: number | null; replay_consistency_floor: number | null;
        regime_instability_ceiling: number | null; conflicting_transition_ceiling: number | null;
        metadata: Record<string, unknown>; created_at: string; updated_at: string;
      }>;
      governance_threshold_applications: TableDef<{
        id: string; run_id: string | null; workspace_id: string; watchlist_id: string | null; regime: string;
        profile_id: string | null; override_id: string | null; evaluation_stage: string;
        applied_thresholds: Record<string, unknown>; metadata: Record<string, unknown>; created_at: string;
      }>;
      governance_threshold_feedback: TableDef<{
        id: string; workspace_id: string; watchlist_id: string | null; threshold_profile_id: string | null;
        event_type: string; regime: string | null; compute_version: string | null; signal_registry_version: string | null;
        model_version: string | null; case_id: string | null; degradation_state_id: string | null;
        threshold_applied_value: number | null; trigger_count: number; ack_count: number; mute_count: number;
        escalation_count: number; resolution_count: number; reopen_count: number; precision_proxy: number;
        noise_score: number; evidence: Record<string, unknown>; created_at: string;
      }>;
      governance_threshold_recommendations: TableDef<{
        id: string; workspace_id: string; threshold_profile_id: string | null; dimension_type: string;
        recommendation_key: string; dimension_value: string; event_type: string; current_value: number | null; recommended_value: number | null;
        direction: string; reason_code: string; confidence: number; supporting_metrics: Record<string, unknown>;
        status: string; created_at: string; updated_at: string;
      }>;
      governance_threshold_recommendation_reviews: TableDef<{
        id: string; workspace_id: string; recommendation_id: string; reviewer: string; decision: string;
        rationale: string | null; metadata: Record<string, unknown>; created_at: string;
      }>;
      governance_threshold_promotion_proposals: TableDef<{
        id: string; workspace_id: string; recommendation_id: string; profile_id: string; event_type: string;
        dimension_type: string; dimension_value: string | null; current_value: number; proposed_value: number;
        status: string; approved_by: string | null; approved_at: string | null; blocked_reason: string | null;
        source_metrics: Record<string, unknown>; metadata: Record<string, unknown>; created_at: string; updated_at: string;
      }>;
      governance_threshold_autopromotion_policies: TableDef<{
        id: string; workspace_id: string; profile_id: string | null; event_type: string | null;
        dimension_type: string | null; dimension_value: string | null; enabled: boolean; min_confidence: number;
        min_support: number; max_step_pct: number; cooldown_hours: number; allow_regime_specific: boolean;
        metadata: Record<string, unknown>; created_at: string; updated_at: string;
      }>;
      governance_threshold_promotion_executions: TableDef<{
        id: string; workspace_id: string; proposal_id: string; profile_id: string; event_type: string;
        dimension_type: string; dimension_value: string | null; previous_value: number; new_value: number;
        executed_by: string; execution_mode: string; rationale: string | null; metadata: Record<string, unknown>;
        created_at: string;
      }>;
      governance_threshold_rollback_candidates: TableDef<{
        id: string; workspace_id: string; execution_id: string; profile_id: string; rollback_to_value: number;
        reason: string; status: string; metadata: Record<string, unknown>; created_at: string; updated_at: string;
      }>;
      governance_alert_events: TableDef<{
        id: string; workspace_id: string; watchlist_id: string | null; run_id: string | null;
        rule_name: string; event_type: string; severity: string; dedupe_key: string;
        metric_source: string; metric_name: string; metric_value_numeric: number | null;
        metric_value_text: string | null; threshold_numeric: number | null; threshold_text: string | null;
        compute_version: string | null; signal_registry_version: string | null; model_version: string | null;
        metadata: Record<string, unknown>; created_at: string;
      }>;
      governance_degradation_states: TableDef<{
        id: string; workspace_id: string; watchlist_id: string | null; degradation_type: string;
        version_tuple: string; regime: string | null; state_status: string; severity: string;
        first_seen_at: string; last_seen_at: string; escalated_at: string | null; resolved_at: string | null;
        event_count: number; cluster_count: number; source_summary: Record<string, unknown>;
        resolution_summary: Record<string, unknown> | null; metadata: Record<string, unknown>;
        created_at: string; updated_at: string;
      }>;
      governance_degradation_state_members: TableDef<{
        id: string; state_id: string; workspace_id: string; governance_alert_event_id: string | null;
        anomaly_cluster_id: string | null; job_run_id: string | null; member_type: string; member_key: string;
        observed_at: string; metadata: Record<string, unknown>; created_at: string;
      }>;
      governance_recovery_events: TableDef<{
        id: string; workspace_id: string; state_id: string; watchlist_id: string | null;
        degradation_type: string; version_tuple: string; regime: string | null; recovered_at: string;
        recovery_reason: string; prior_severity: string; trailing_metrics: Record<string, unknown>;
        metadata: Record<string, unknown>; created_at: string;
      }>;
      governance_acknowledgments: TableDef<{
        id: string; workspace_id: string; degradation_state_id: string; acknowledged_by: string;
        note: string | null; metadata: Record<string, unknown>; acknowledged_at: string;
      }>;
      governance_muting_rules: TableDef<{
        id: string; workspace_id: string; target_type: string; target_key: string; reason: string | null;
        muted_until: string | null; created_by: string; is_active: boolean; metadata: Record<string, unknown>;
        created_at: string; updated_at: string;
      }>;
      governance_resolution_actions: TableDef<{
        id: string; workspace_id: string; degradation_state_id: string; action_type: string;
        performed_by: string; note: string | null; metadata: Record<string, unknown>; created_at: string;
      }>;
      governance_cases: TableDef<{
        id: string; workspace_id: string; degradation_state_id: string | null; watchlist_id: string | null;
        version_tuple: string | null; status: string; severity: string; title: string; summary: string | null;
        opened_at: string; acknowledged_at: string | null; resolved_at: string | null; closed_at: string | null;
        reopened_count: number; current_assignee: string | null; current_team: string | null;
        metadata: Record<string, unknown>; recurrence_group_id: string | null; reopened_from_case_id: string | null;
        repeat_count: number; reopened_at: string | null; reopen_reason: string | null;
        recurrence_match_basis: Record<string, unknown>; created_at: string; updated_at: string;
      }>;
      governance_case_events: TableDef<{
        id: string; case_id: string; workspace_id: string; event_type: string; actor: string | null;
        payload: Record<string, unknown>; created_at: string;
      }>;
      governance_case_notes: TableDef<{
        id: string; case_id: string; workspace_id: string; author: string | null; note: string;
        note_type: string; visibility: string; metadata: Record<string, unknown>;
        created_at: string; edited_at: string | null;
      }>;
      governance_case_evidence: TableDef<{
        id: string; case_id: string; workspace_id: string; evidence_type: string; reference_id: string;
        title: string | null; summary: string | null; payload: Record<string, unknown>; created_at: string;
      }>;
      governance_case_summaries: TableDef<{
        id: string; workspace_id: string; case_id: string; summary_version: string;
        status_summary: string | null; root_cause_code: string | null; root_cause_confidence: number | null;
        root_cause_summary: string | null; evidence_summary: string | null; recurrence_summary: string | null;
        operator_summary: string | null; closure_summary: string | null; recommended_next_action: string | null;
        source_note_ids: string[]; source_evidence_ids: string[]; metadata: Record<string, unknown>;
        generated_at: string; updated_at: string;
      }>;
      governance_routing_rules: TableDef<{
        id: string; workspace_id: string; is_enabled: boolean; priority: number;
        root_cause_code: string | null; severity: string | null; watchlist_id: string | null;
        version_tuple: string | null; regime: string | null; recurrence_min: number | null; chronic_only: boolean;
        assign_team: string | null; assign_user: string | null; fallback_team: string | null;
        routing_reason_template: string | null; metadata: Record<string, unknown>;
        created_at: string; updated_at: string;
      }>;
      governance_routing_overrides: TableDef<{
        id: string; workspace_id: string; case_id: string | null; watchlist_id: string | null;
        root_cause_code: string | null; severity: string | null; version_tuple: string | null; regime: string | null;
        assigned_team: string | null; assigned_user: string | null; reason: string | null;
        is_enabled: boolean; metadata: Record<string, unknown>; created_at: string;
      }>;
      governance_routing_decisions: TableDef<{
        id: string; workspace_id: string; case_id: string; routing_rule_id: string | null;
        override_id: string | null; assigned_team: string | null; assigned_user: string | null;
        routing_reason: string; workload_snapshot: Record<string, unknown>; metadata: Record<string, unknown>;
        created_at: string;
      }>;
      governance_routing_feedback: TableDef<{
        id: string; workspace_id: string; case_id: string; routing_decision_id: string | null;
        feedback_type: string; feedback_status: string; assigned_to: string | null; assigned_team: string | null;
        prior_assigned_to: string | null; prior_assigned_team: string | null; root_cause_code: string | null;
        severity: string | null; recurrence_group_id: string | null; repeat_count: number; reason: string | null;
        metadata: Record<string, unknown>; created_at: string;
      }>;
        governance_reassignment_events: TableDef<{
          id: string; workspace_id: string; case_id: string; routing_decision_id: string | null;
          previous_assigned_to: string | null; previous_assigned_team: string | null;
          new_assigned_to: string | null; new_assigned_team: string | null; reassignment_type: string;
          reassignment_reason: string | null; minutes_since_open: number | null;
          minutes_since_last_assignment: number | null; metadata: Record<string, unknown>; created_at: string;
        }>;
        governance_routing_outcomes: TableDef<{
          id: string; workspace_id: string; case_id: string; routing_decision_id: string | null;
          assignment_id: string | null; outcome_type: string; outcome_value: number | null;
          assigned_to: string | null; assigned_team: string | null; root_cause_code: string | null;
          severity: string | null; watchlist_id: string | null; compute_version: string | null;
          signal_registry_version: string | null; model_version: string | null;
          recurrence_group_id: string | null; repeat_count: number; outcome_context: Record<string, unknown>;
          occurred_at: string; created_at: string;
        }>;
        governance_routing_recommendations: TableDef<{
          id: string; workspace_id: string; case_id: string; recommendation_key: string;
          recommended_user: string | null; recommended_team: string | null;
          fallback_user: string | null; fallback_team: string | null;
          reason_code: string; confidence: "low" | "medium" | "high"; score: number;
          supporting_metrics: Record<string, unknown>; model_inputs: Record<string, unknown>;
          alternatives: Record<string, unknown>[]; accepted: boolean | null;
          accepted_at: string | null; accepted_by: string | null; override_reason: string | null;
          applied: boolean; applied_at: string | null; created_at: string; updated_at: string;
        }>;
        governance_routing_recommendation_reviews: TableDef<{
          id: string; workspace_id: string; recommendation_id: string; case_id: string | null;
          review_status: "approved" | "rejected" | "deferred"; review_reason: string | null;
          notes: string | null; reviewed_by: string | null; reviewed_at: string;
          applied_immediately: boolean; metadata: Record<string, unknown>;
        }>;
        governance_routing_applications: TableDef<{
          id: string; workspace_id: string; recommendation_id: string; review_id: string | null;
          case_id: string; previous_assigned_user: string | null; previous_assigned_team: string | null;
          applied_user: string | null; applied_team: string | null; application_mode: string;
          application_reason: string | null; applied_by: string | null; applied_at: string;
          metadata: Record<string, unknown>;
        }>;
        governance_routing_autopromotion_policies: TableDef<{
          id: string; workspace_id: string; enabled: boolean; scope_type: string; scope_value: string | null;
          promotion_target: "override" | "rule"; min_confidence: "low" | "medium" | "high";
          min_acceptance_rate: number; min_sample_size: number; max_recent_override_rate: number;
          cooldown_hours: number; created_by: string | null; metadata: Record<string, unknown>;
          created_at: string; updated_at: string;
        }>;
        governance_routing_autopromotion_executions: TableDef<{
          id: string; workspace_id: string; policy_id: string; recommendation_id: string;
          target_type: "override" | "rule"; target_key: string; recommended_user: string | null;
          recommended_team: string | null; confidence: string; acceptance_rate: number | null;
          sample_size: number | null; override_rate: number | null; execution_status: string;
          execution_reason: string | null; cooldown_bucket: string | null;
          prior_state: Record<string, unknown>; new_state: Record<string, unknown>;
          metadata: Record<string, unknown>; created_at: string;
        }>;
        governance_routing_autopromotion_rollback_candidates: TableDef<{
          id: string; workspace_id: string; execution_id: string; target_type: "override" | "rule";
          target_key: string; prior_state: Record<string, unknown>; rollback_reason: string | null;
          rolled_back: boolean; rolled_back_at: string | null; created_at: string;
        }>;
        governance_incident_analytics_snapshots: TableDef<{
          id: string; workspace_id: string; snapshot_date: string;
          open_case_count: number; acknowledged_case_count: number; resolved_case_count: number;
          reopened_case_count: number; recurring_case_count: number; escalated_case_count: number;
          high_severity_open_count: number; stale_case_count: number;
          mean_ack_hours: number | null; mean_resolve_hours: number | null; created_at: string;
        }>;
        governance_promotion_impact_snapshots: TableDef<{
          id: string; workspace_id: string; promotion_type: "threshold" | "routing";
          execution_id: string; scope_type: string; scope_value: string | null;
          impact_classification: "improved" | "neutral" | "degraded" | "rollback_candidate" | "insufficient_data";
          pre_window_start: string; pre_window_end: string; post_window_start: string; post_window_end: string;
          recurrence_rate_before: number | null; recurrence_rate_after: number | null;
          escalation_rate_before: number | null; escalation_rate_after: number | null;
          resolution_latency_before_ms: number | null; resolution_latency_after_ms: number | null;
          reassignment_rate_before: number | null; reassignment_rate_after: number | null;
          rollback_risk_score: number | null; supporting_metrics: Record<string, unknown>;
          created_at: string; updated_at: string;
        }>;
        governance_manager_analytics_snapshots: TableDef<{
          id: string; workspace_id: string; snapshot_at: string; window_days: number;
          open_case_count: number; recurring_case_count: number; escalated_case_count: number;
          chronic_watchlist_count: number; degraded_promotion_count: number; rollback_risk_count: number;
          metadata: Record<string, unknown>;
        }>;
        governance_performance_snapshots: TableDef<{
          id: string; workspace_id: string; snapshot_at: string;
          operator_count: number; team_count: number; operator_case_mix_count: number;
          team_case_mix_count: number; metadata: Record<string, unknown>; created_at: string;
        }>;
        governance_sla_policies: TableDef<{
          id: string; workspace_id: string; severity: string; chronicity_class: string | null;
          ack_within_minutes: number; resolve_within_minutes: number; enabled: boolean;
          metadata: Record<string, unknown>; created_at: string; updated_at: string;
        }>;
      governance_sla_evaluations: TableDef<{
        id: string; workspace_id: string; case_id: string; policy_id: string | null;
        chronicity_class: string | null; ack_due_at: string | null; resolve_due_at: string | null;
        ack_breached: boolean; resolve_breached: boolean; breach_severity: string | null;
        metadata: Record<string, unknown>; evaluated_at: string;
      }>;
      governance_escalation_policies: TableDef<{
        id: string; workspace_id: string; severity: string | null; chronicity_class: string | null;
        root_cause_code: string | null; min_case_age_minutes: number | null; min_ack_age_minutes: number | null;
        min_repeat_count: number | null; min_operator_pressure: number | null; escalation_level: string;
        escalate_to_team: string | null; escalate_to_user: string | null; cooldown_minutes: number;
        is_enabled: boolean; metadata: Record<string, unknown>; created_at: string;
      }>;
      governance_escalation_state: TableDef<{
        id: string; workspace_id: string; case_id: string; escalation_level: string; status: string;
        escalated_to_team: string | null; escalated_to_user: string | null; reason: string | null;
        source_policy_id: string | null; escalated_at: string; last_evaluated_at: string;
        repeated_count: number; cleared_at: string | null; metadata: Record<string, unknown>;
      }>;
      governance_escalation_events: TableDef<{
        id: string; workspace_id: string; case_id: string; escalation_state_id: string | null;
        event_type: string; escalation_level: string | null; escalated_to_team: string | null;
        escalated_to_user: string | null; reason: string | null; source_policy_id: string | null;
        metadata: Record<string, unknown>; created_at: string;
      }>;
      governance_assignments: TableDef<{
        id: string; case_id: string; workspace_id: string; assigned_to: string | null; assigned_team: string | null;
        assigned_by: string | null; reason: string | null; active: boolean; metadata: Record<string, unknown>;
        assigned_at: string;
      }>;
      governance_assignment_history: TableDef<{
        id: string; case_id: string; workspace_id: string; previous_assignee: string | null; previous_team: string | null;
        new_assignee: string | null; new_team: string | null; changed_by: string | null; reason: string | null;
        metadata: Record<string, unknown>; changed_at: string;
      }>;
      governance_case_recurrence: TableDef<{
        id: string; workspace_id: string; watchlist_id: string | null; recurrence_group_id: string;
        source_case_id: string; matched_case_id: string; match_type: string; match_score: number;
        matched_within_window: boolean; match_basis: Record<string, unknown>; created_at: string;
      }>;
      governance_incident_timeline_events: TableDef<{
        id: number; case_id: string; workspace_id: string; event_type: string; event_source: string;
        event_at: string; actor: string | null; title: string; detail: string | null;
        metadata: Record<string, unknown>; source_table: string | null; source_id: string | null;
        created_at: string;
      }>;
      governance_routing_optimization_snapshots: TableDef<{
        id: string; workspace_id: string; snapshot_at: string; window_label: string;
        recommendation_count: number; metadata: Record<string, unknown>; created_at: string;
      }>;
      governance_routing_policy_recommendation_reviews: TableDef<{
        id: string; workspace_id: string; recommendation_key: string;
        review_status: "approved" | "rejected" | "deferred";
        review_reason: string | null; reviewed_by: string; reviewed_at: string;
        notes: string | null; metadata: Record<string, unknown>;
      }>;
      governance_routing_policy_promotion_proposals: TableDef<{
        id: string; workspace_id: string; recommendation_key: string;
        proposal_status: "pending" | "approved" | "rejected" | "applied" | "deferred";
        promotion_target: "override" | "rule";
        scope_type: string; scope_value: string;
        current_policy: Record<string, unknown>; recommended_policy: Record<string, unknown>;
        proposed_by: string; proposed_at: string;
        approved_by: string | null; approved_at: string | null;
        applied_at: string | null; proposal_reason: string | null;
        metadata: Record<string, unknown>;
      }>;
      governance_routing_policy_applications: TableDef<{
        id: string; workspace_id: string; proposal_id: string; recommendation_key: string;
        applied_target: "override" | "rule";
        applied_scope_type: string; applied_scope_value: string;
        prior_policy: Record<string, unknown>; applied_policy: Record<string, unknown>;
        applied_by: string; applied_at: string; rollback_candidate: boolean;
        metadata: Record<string, unknown>;
      }>;
      governance_routing_policy_recommendations: TableDef<{
        id: string; workspace_id: string; recommendation_key: string; reason_code: string;
        scope_type: string; scope_value: string | null; recommended_policy: Record<string, unknown>;
        confidence: string; expected_benefit_score: number; risk_score: number; sample_size: number;
        signal_payload: Record<string, unknown>; snapshot_id: string | null;
        created_at: string; updated_at: string;
      }>;
    };
    Views: {
      queue_depth_by_watchlist: {
        Row: {
          workspace_id: string; watchlist_id: string | null;
          queued_count: number; claimed_count: number;
          failed_count: number; dead_letter_count: number;
          newest_job_at: string | null; oldest_queued_at: string | null;
        };
        Relationships: [];
      };
      queue_runtime_summary: {
        Row: {
          workspace_id: string; watchlist_id: string | null;
          total_runs: number; avg_runtime_seconds: number | null;
          last_completed_at: string | null; failed_runs: number; completed_runs: number;
        };
        Relationships: [];
      };
      stale_workers: {
        Row: {
          worker_id: string; workspace_id: string | null; hostname: string | null;
          pid: number | null; status: string; last_seen_at: string; seconds_since_seen: number;
        };
        Relationships: [];
      };
      run_inspection: {
        Row: {
          run_id: string; workspace_id: string; workspace_slug: string;
          watchlist_id: string | null; watchlist_slug: string | null; watchlist_name: string | null;
          queue_id: number | null; queue_name: string; status: string; trigger_type: string;
          requested_by: string | null; attempt_count: number; max_attempts: number;
          claimed_by: string | null; claimed_at: string | null; started_at: string | null;
          completed_at: string | null; runtime_ms: number | null; compute_version: string | null;
          signal_registry_version: string | null; model_version: string | null;
          lineage: Record<string, unknown>; metadata: Record<string, unknown>;
          priority: number | null; retry_count: number | null; last_error: string | null;
          queued_at: string | null; terminal_queue_status: string | null;
          terminal_promoted_at: string | null; alert_count: number; last_alert_at: string | null;
          failure_stage: string | null; failure_code: string | null; is_replay: boolean;
          replayed_from_run_id: string | null; input_snapshot_id: number | null;
          explanation_version: string | null;
        };
        Relationships: [];
      };
      run_scope_inspection: {
        Row: {
          run_id: string; workspace_id: string; workspace_slug: string;
          watchlist_id: string | null; watchlist_slug: string | null; watchlist_name: string | null;
          queue_id: number | null; queue_name: string; status: string; is_replay: boolean;
          replayed_from_run_id: string | null; compute_scope_id: string | null; scope_version: string | null;
          scope_hash: string | null; primary_assets: unknown[]; dependency_assets: unknown[];
          asset_universe: unknown[]; primary_asset_count: number; dependency_asset_count: number;
          asset_universe_count: number; dependency_policy: Record<string, unknown>;
          metadata: Record<string, unknown>; scope_created_at: string | null;
        };
        Relationships: [];
      };
      job_run_prior_comparison: {
        Row: {
          run_id: string; prior_run_id: string | null; workspace_id: string; workspace_slug: string;
          watchlist_id: string | null; watchlist_slug: string | null; watchlist_name: string | null;
          queue_name: string; current_summary: string | null; prior_summary: string | null;
          regime_changes: Record<string, unknown>; signal_changes: Record<string, unknown>;
          composite_changes: Record<string, unknown>; invalidator_changes: Record<string, unknown>;
          input_coverage_changes: Record<string, unknown>;
        };
        Relationships: [];
      };
      ops_run_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string | null; status: string;
          is_replay: boolean; replayed_from_run_id: string | null; failure_stage: string | null;
          failure_code: string | null; runtime_ms: number | null; started_at: string | null;
          completed_at: string | null; terminal_or_run_status: string | null;
        };
        Relationships: [];
      };
      run_attribution_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string | null; status: string;
          compute_version: string | null; signal_registry_version: string | null; model_version: string | null;
          attribution_version: string | null; attribution_reconciled: boolean;
          attribution_total: number | null; attribution_target_total: number | null;
          attribution_reconciliation_delta: number | null;
          family_attributions: unknown[]; signal_attributions: unknown[];
        };
        Relationships: [];
      };
      job_run_drift_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string | null; comparison_run_id: string | null;
          drift_severity: string | null; drift_summary: Record<string, unknown>;
          current_compute_version: string | null; comparison_compute_version: string | null;
          current_signal_registry_version: string | null; comparison_signal_registry_version: string | null;
          current_model_version: string | null; comparison_model_version: string | null;
          metric_count: number; flagged_metric_count: number; computed_at: string | null;
        };
        Relationships: [];
      };
      job_run_version_behavior_comparison: {
        Row: {
          workspace_id: string; watchlist_id: string | null; queue_name: string;
          compute_version: string | null; signal_registry_version: string | null; model_version: string | null;
          run_count: number; replay_run_count: number; avg_composite_score: number | null;
          avg_flagged_drift_metrics: number | null; avg_replay_input_match_score: number | null;
          avg_replay_composite_delta_abs: number | null; high_severity_replay_count: number;
          latest_completed_at: string | null;
        };
        Relationships: [];
      };
      run_regime_stability_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string | null; queue_name: string;
          status: string; is_replay: boolean; replayed_from_run_id: string | null;
          started_at: string | null; completed_at: string | null; prior_run_id: string | null;
          from_regime: string | null; to_regime: string | null; transition_detected: boolean;
          transition_classification: string; stability_score: number | null; anomaly_likelihood: number | null;
          composite_shift: number | null; composite_shift_abs: number | null;
          dominant_family_gained: string | null; dominant_family_lost: string | null;
          metadata: Record<string, unknown>; created_at: string | null; updated_at: string | null;
        };
        Relationships: [];
      };
      latest_stability_summary: {
        Row: {
          run_id: string; workspace_id: string; workspace_slug: string; watchlist_id: string | null;
          watchlist_slug: string | null; watchlist_name: string | null; queue_name: string; window_size: number;
          baseline_run_count: number; composite_current: number | null; composite_baseline: number | null;
          composite_delta_abs: number | null; composite_delta_pct: number | null; composite_instability_score: number;
          family_instability_score: number; replay_consistency_risk_score: number; regime_instability_score: number;
          dominant_family: string | null; dominant_family_changed: boolean; dominant_regime: string | null;
          regime_changed: boolean; stability_classification: string; replay_runs_considered: number | null;
          mismatch_rate: number | null; avg_input_match_score: number | null; avg_composite_delta_abs: number | null;
          transitions_considered: number | null; conflicting_transition_count: number | null;
          abrupt_transition_count: number | null; family_rows: unknown[]; metadata: Record<string, unknown>;
          created_at: string;
        };
        Relationships: [];
      };
      version_stability_summary: {
        Row: {
          workspace_id: string; compute_version: string; signal_registry_version: string; model_version: string;
          run_count: number; avg_runtime_ms: number | null; completion_rate: number; failure_rate: number;
          avg_family_instability: number; avg_regime_instability: number; avg_replay_consistency_risk: number;
          last_completed_at: string | null;
        };
        Relationships: [];
      };
      version_replay_consistency_summary: {
        Row: {
          workspace_id: string; compute_version: string; signal_registry_version: string; model_version: string;
          replay_count: number; avg_input_match_score: number; avg_replay_composite_delta_abs: number;
          elevated_replay_rate: number; last_replay_completed_at: string | null;
        };
        Relationships: [];
      };
      version_regime_behavior_summary: {
        Row: {
          workspace_id: string; compute_version: string; signal_registry_version: string; model_version: string;
          transition_count: number; conflicting_transition_rate: number | null;
          avg_transition_stability_score: number | null; avg_transition_anomaly_likelihood: number | null;
        };
        Relationships: [];
      };
      version_health_rankings: {
        Row: {
          workspace_id: string; workspace_slug: string; compute_version: string; signal_registry_version: string;
          model_version: string; run_count: number; avg_runtime_ms: number | null; completion_rate: number;
          failure_rate: number; avg_family_instability: number; avg_regime_instability: number;
          avg_replay_consistency_risk: number; replay_count: number; avg_input_match_score: number;
          avg_replay_composite_delta_abs: number; elevated_replay_rate: number; transition_count: number;
          conflicting_transition_rate: number; avg_transition_stability_score: number;
          avg_transition_anomaly_likelihood: number; governance_health_score: number; health_rank: number;
          last_completed_at: string | null; last_replay_completed_at: string | null;
        };
        Relationships: [];
      };
      governance_alert_state: {
        Row: {
          workspace_id: string; workspace_slug: string; watchlist_id: string | null;
          watchlist_slug: string | null; watchlist_name: string | null; rule_name: string;
          event_type: string; severity: string; compute_version: string | null;
          signal_registry_version: string | null; model_version: string | null;
          latest_triggered_at: string; trigger_count: number;
        };
        Relationships: [];
      };
      governance_anomaly_cluster_state: {
        Row: {
          id: string; workspace_id: string; workspace_slug: string; watchlist_id: string | null;
          watchlist_slug: string | null; watchlist_name: string | null; version_tuple: string;
          cluster_key: string; alert_type: string; regime: string | null; severity: string; status: string;
          first_seen_at: string; last_seen_at: string; event_count: number; latest_event_id: string | null;
          latest_run_id: string | null; metadata: Record<string, unknown>; created_at: string; updated_at: string;
        };
        Relationships: [];
      };
      governance_degradation_summary: {
        Row: {
          id: string; workspace_id: string; workspace_slug: string; watchlist_id: string | null;
          watchlist_slug: string | null; watchlist_name: string | null; degradation_type: string;
          version_tuple: string; regime: string | null; state_status: string; severity: string;
          first_seen_at: string; last_seen_at: string; escalated_at: string | null; resolved_at: string | null;
          event_count: number; cluster_count: number; source_summary: Record<string, unknown>;
          resolution_summary: Record<string, unknown> | null; metadata: Record<string, unknown>;
          member_count: number; state_duration_hours: number;
        };
        Relationships: [];
      };
      governance_recovery_event_summary: {
        Row: {
          id: string; workspace_id: string; workspace_slug: string; state_id: string;
          watchlist_id: string | null; watchlist_slug: string | null; watchlist_name: string | null;
          degradation_type: string; version_tuple: string; regime: string | null; recovered_at: string;
          recovery_reason: string; prior_severity: string; trailing_metrics: Record<string, unknown>;
          metadata: Record<string, unknown>; state_first_seen_at: string | null; state_last_seen_at: string | null;
          state_event_count: number | null; state_cluster_count: number | null;
        };
        Relationships: [];
      };
      governance_lifecycle_summary: {
        Row: {
          degradation_state_id: string; workspace_id: string; workspace_slug: string;
          watchlist_id: string | null; watchlist_slug: string | null; watchlist_name: string | null;
          degradation_type: string; version_tuple: string; regime: string | null; state_status: string;
          severity: string; first_seen_at: string; last_seen_at: string; escalated_at: string | null;
          resolved_at: string | null; event_count: number; cluster_count: number;
          source_summary: Record<string, unknown>; resolution_summary: Record<string, unknown> | null;
          metadata: Record<string, unknown>; acknowledgment_id: string | null; acknowledged_at: string | null;
          acknowledged_by: string | null; acknowledgment_note: string | null;
          acknowledgment_metadata: Record<string, unknown> | null; muting_rule_id: string | null;
          mute_target_type: string | null; mute_target_key: string | null; mute_reason: string | null;
          muted_until: string | null; muted_by: string | null; mute_is_active: boolean | null;
          mute_metadata: Record<string, unknown> | null; resolution_action_id: string | null;
          last_resolution_action: string | null; last_resolution_actor: string | null;
          last_resolution_note: string | null; last_resolution_metadata: Record<string, unknown> | null;
          last_resolution_at: string | null;
        };
        Relationships: [];
      };
      governance_case_summary: {
        Row: {
          id: string; workspace_id: string; workspace_slug: string; degradation_state_id: string | null;
          watchlist_id: string | null; watchlist_slug: string | null; watchlist_name: string | null;
          version_tuple: string | null; status: string; severity: string; title: string; summary: string | null;
          opened_at: string; acknowledged_at: string | null; resolved_at: string | null; closed_at: string | null;
          reopened_count: number; current_assignee: string | null; current_team: string | null;
          metadata: Record<string, unknown>; note_count: number; evidence_count: number; event_count: number;
          last_event_type: string | null; last_event_at: string | null; recurrence_group_id: string | null;
          reopened_from_case_id: string | null; repeat_count: number; reopened_at: string | null;
          reopen_reason: string | null; recurrence_match_basis: Record<string, unknown>;
          prior_related_case_count: number; latest_prior_case_id: string | null;
          latest_prior_closed_at: string | null; latest_prior_status: string | null;
          is_reopened: boolean; is_recurring: boolean;
        };
        Relationships: [];
      };
      governance_case_recurrence_summary: {
        Row: {
          case_id: string; workspace_id: string; recurrence_group_id: string | null;
          reopened_from_case_id: string | null; repeat_count: number; prior_related_case_count: number;
          latest_prior_case_id: string | null; latest_prior_closed_at: string | null;
          latest_prior_status: string | null; is_reopened: boolean; is_recurring: boolean;
        };
        Relationships: [];
      };
      governance_case_evidence_summary: {
        Row: {
          case_id: string; workspace_id: string; evidence_count: number; latest_evidence_at: string | null;
          latest_run_id: string | null; latest_replay_delta_id: string | null;
          latest_regime_transition_id: string | null; latest_threshold_application_id: string | null;
          evidence_type_counts: Record<string, number>; evidence_items: Record<string, unknown>[];
        };
        Relationships: [];
      };
      governance_case_summary_latest: {
        Row: {
          id: string; workspace_id: string; case_id: string; summary_version: string;
          status_summary: string | null; root_cause_code: string | null; root_cause_confidence: number | null;
          root_cause_summary: string | null; evidence_summary: string | null; recurrence_summary: string | null;
          operator_summary: string | null; closure_summary: string | null; recommended_next_action: string | null;
          source_note_ids: string[]; source_evidence_ids: string[]; metadata: Record<string, unknown>;
          generated_at: string; updated_at: string;
        };
        Relationships: [];
      };
      governance_assignment_workload_summary: {
        Row: {
          workspace_id: string; assigned_team: string; assigned_to: string;
          open_case_count: number; severe_open_case_count: number; avg_open_age_hours: number | null;
          reopened_open_case_count: number; stale_open_case_count: number;
        };
        Relationships: [];
      };
      governance_operator_case_metrics: {
        Row: {
          workspace_id: string; operator_id: string; assigned_team: string | null;
          open_case_count: number; severe_open_case_count: number; avg_open_age_hours: number | null;
          reopened_open_case_count: number; stale_open_case_count: number;
        };
        Relationships: [];
      };
      governance_team_case_metrics: {
        Row: {
          workspace_id: string; assigned_team: string;
          open_case_count: number; severe_open_case_count: number; avg_open_age_hours: number | null;
          reopened_open_case_count: number; stale_open_case_count: number;
        };
        Relationships: [];
      };
      governance_routing_summary: {
        Row: {
          id: string; workspace_id: string; workspace_slug: string; case_id: string;
          watchlist_id: string | null; watchlist_slug: string | null; case_title: string;
          case_status: string; severity: string; version_tuple: string | null; root_cause_code: string | null;
          routing_rule_id: string | null; override_id: string | null; assigned_team: string | null;
          assigned_user: string | null; routing_reason: string; workload_snapshot: Record<string, unknown>;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      governance_routing_quality_summary: {
        Row: {
          workspace_id: string; workspace_slug: string; root_cause_code: string; assigned_team: string;
          feedback_count: number; accepted_count: number; rerouted_count: number; acceptance_rate: number;
          latest_feedback_at: string | null;
        };
        Relationships: [];
      };
      governance_threshold_performance_summary: {
        Row: {
          workspace_id: string; workspace_slug: string; threshold_profile_key: string; event_type: string;
          regime: string; compute_version: string; signal_registry_version: string; model_version: string;
          feedback_rows: number; trigger_count: number; ack_count: number; mute_count: number;
          escalation_count: number; resolution_count: number; reopen_count: number;
          avg_precision_proxy: number; avg_noise_score: number; latest_feedback_at: string | null;
        };
        Relationships: [];
      };
      governance_threshold_learning_summary: {
        Row: {
          workspace_id: string; workspace_slug: string; recommendation_id: string; threshold_profile_id: string | null;
          recommendation_key: string; dimension_type: string; dimension_value: string; event_type: string; current_value: number | null;
          recommended_value: number | null; direction: string; reason_code: string; confidence: number;
          supporting_metrics: Record<string, unknown>; status: string; created_at: string; updated_at: string;
        };
        Relationships: [];
      };
      governance_threshold_review_summary: {
        Row: {
          proposal_id: string; workspace_id: string; workspace_slug: string; recommendation_id: string;
          recommendation_key: string; profile_id: string; event_type: string; dimension_type: string;
          dimension_value: string | null; current_value: number; proposed_value: number; status: string;
          approved_by: string | null; approved_at: string | null; blocked_reason: string | null;
          source_metrics: Record<string, unknown>; metadata: Record<string, unknown>;
          created_at: string; updated_at: string; direction: string; reason_code: string; confidence: number;
          latest_review_id: string | null; latest_reviewer: string | null; latest_decision: string | null;
          latest_rationale: string | null; latest_reviewed_at: string | null;
        };
        Relationships: [];
      };
      governance_threshold_autopromotion_summary: {
        Row: {
          execution_id: string; workspace_id: string; workspace_slug: string; proposal_id: string;
          recommendation_id: string; profile_id: string; event_type: string; dimension_type: string;
          dimension_value: string | null; previous_value: number; new_value: number; execution_mode: string;
          executed_by: string; rationale: string | null; metadata: Record<string, unknown>;
          created_at: string; rollback_candidate_id: string | null; rollback_status: string | null;
          rollback_reason: string | null; rollback_to_value: number | null; rollback_updated_at: string | null;
        };
        Relationships: [];
      };
        governance_reassignment_pressure_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; assigned_team: string; reassignment_count: number;
            manual_override_count: number; escalation_reassign_count: number; workload_rebalance_count: number;
            avg_minutes_since_open: number | null; avg_minutes_since_last_assignment: number | null;
            latest_reassignment_at: string | null;
          };
          Relationships: [];
        };
        governance_operator_effectiveness_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; assigned_to: string;
            assignments: number; acknowledgments: number; resolutions: number;
            reassignments: number; escalations: number; reopens: number;
            avg_ack_hours: number | null; avg_resolve_hours: number | null;
            latest_outcome_at: string | null; resolution_rate: number | null;
            reassignment_rate: number | null; escalation_rate: number | null;
          };
          Relationships: [];
        };
        governance_team_effectiveness_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; assigned_team: string;
            assignments: number; acknowledgments: number; resolutions: number;
            reassignments: number; escalations: number; reopens: number;
            avg_ack_hours: number | null; avg_resolve_hours: number | null;
            latest_outcome_at: string | null; resolution_rate: number | null;
            reassignment_rate: number | null; escalation_rate: number | null;
          };
          Relationships: [];
        };
        governance_routing_recommendation_inputs: {
          Row: {
            workspace_id: string; workspace_slug: string; routing_target: string;
            root_cause_code: string | null; severity: string | null;
            compute_version: string | null; signal_registry_version: string | null;
            model_version: string | null; avg_ack_hours: number | null;
            avg_resolve_hours: number | null; resolved_count: number;
            reassigned_count: number; escalated_count: number; reopened_count: number;
            latest_outcome_at: string | null;
          };
          Relationships: [];
        };
        governance_routing_recommendation_summary: {
          Row: {
            id: string; workspace_id: string; workspace_slug: string; case_id: string;
            case_title: string; case_status: string; severity: string; recommendation_key: string;
            recommended_user: string | null; recommended_team: string | null;
            fallback_user: string | null; fallback_team: string | null;
            reason_code: string; confidence: "low" | "medium" | "high"; score: number;
            accepted: boolean | null; accepted_at: string | null; accepted_by: string | null;
            override_reason: string | null; applied: boolean; applied_at: string | null;
            supporting_metrics: Record<string, unknown>; model_inputs: Record<string, unknown>;
            alternatives: Record<string, unknown>[]; created_at: string; updated_at: string;
          };
          Relationships: [];
        };
        governance_routing_review_summary: {
          Row: {
            workspace_id: string; recommendation_id: string; latest_reviewed_at: string | null;
            latest_review_status: "approved" | "rejected" | "deferred" | null;
            review_count: number; any_applied_immediately: boolean;
          };
          Relationships: [];
        };
        governance_routing_application_summary: {
          Row: {
            workspace_id: string; case_id: string; recommendation_id: string;
            application_count: number; latest_applied_at: string | null;
            latest_applied_user: string | null; latest_applied_team: string | null;
          };
          Relationships: [];
        };
        governance_routing_autopromotion_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; execution_id: string; policy_id: string;
            scope_type: string; scope_value: string | null; promotion_target: "override" | "rule";
            recommendation_id: string; target_type: "override" | "rule"; target_key: string;
            recommended_user: string | null; recommended_team: string | null; confidence: string;
            acceptance_rate: number | null; sample_size: number | null; override_rate: number | null;
            execution_status: string; execution_reason: string | null; cooldown_bucket: string | null;
            prior_state: Record<string, unknown>; new_state: Record<string, unknown>;
            metadata: Record<string, unknown>; created_at: string; rollback_candidate_id: string | null;
            rollback_reason: string | null; rolled_back: boolean | null; rolled_back_at: string | null;
          };
          Relationships: [];
        };
        governance_incident_analytics_summary: {
          Row: {
            workspace_id: string; open_case_count: number; acknowledged_case_count: number;
            resolved_case_count: number; reopened_case_count: number; recurring_case_count: number;
            high_severity_open_count: number; mean_ack_hours: number | null; mean_resolve_hours: number | null;
          };
          Relationships: [];
        };
        governance_root_cause_trend_summary: {
          Row: {
            workspace_id: string; root_cause_code: string; case_count: number;
            reopened_count: number; recurring_count: number; severe_count: number;
            avg_case_age_hours: number | null;
          };
          Relationships: [];
        };
        governance_recurrence_burden_summary: {
          Row: {
            workspace_id: string; watchlist_id: string | null; recurring_case_count: number;
            max_repeat_count: number | null; recurrence_group_count: number; reopened_case_count: number;
          };
          Relationships: [];
        };
        governance_escalation_effectiveness_summary: {
          Row: {
            workspace_id: string; escalated_case_count: number; escalated_resolved_count: number;
            escalated_reopened_count: number; escalation_resolution_rate: number; escalation_reopen_rate: number;
          };
          Relationships: [];
        };
        governance_threshold_promotion_impact_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; execution_id: string;
            scope_type: string; scope_value: string | null; impact_classification: string;
            pre_window_start: string; pre_window_end: string; post_window_start: string; post_window_end: string;
            recurrence_rate_before: number | null; recurrence_rate_after: number | null;
            escalation_rate_before: number | null; escalation_rate_after: number | null;
            resolution_latency_before_ms: number | null; resolution_latency_after_ms: number | null;
            rollback_risk_score: number | null; supporting_metrics: Record<string, unknown>;
            created_at: string; updated_at: string;
          };
          Relationships: [];
        };
        governance_routing_promotion_impact_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; execution_id: string;
            scope_type: string; scope_value: string | null; impact_classification: string;
            pre_window_start: string; pre_window_end: string; post_window_start: string; post_window_end: string;
            recurrence_rate_before: number | null; recurrence_rate_after: number | null;
            escalation_rate_before: number | null; escalation_rate_after: number | null;
            resolution_latency_before_ms: number | null; resolution_latency_after_ms: number | null;
            reassignment_rate_before: number | null; reassignment_rate_after: number | null;
            rollback_risk_score: number | null; supporting_metrics: Record<string, unknown>;
            created_at: string; updated_at: string;
          };
          Relationships: [];
        };
        governance_promotion_rollback_risk_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; promotion_type: string;
            execution_id: string; scope_type: string; scope_value: string | null;
            impact_classification: string; rollback_risk_score: number | null;
            supporting_metrics: Record<string, unknown>; created_at: string; updated_at: string;
          };
          Relationships: [];
        };
        governance_manager_overview_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; snapshot_id: string; snapshot_at: string;
            window_days: number; open_case_count: number; recurring_case_count: number; escalated_case_count: number;
            chronic_watchlist_count: number; degraded_promotion_count: number; rollback_risk_count: number;
            total_operating_burden: number; metadata: Record<string, unknown>;
          };
          Relationships: [];
        };
        governance_chronic_watchlist_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; watchlist_id: string | null;
            watchlist_slug: string | null; watchlist_name: string | null; recurring_case_count: number;
            reopened_case_count: number; max_repeat_count: number | null; recurrence_group_count: number;
            latest_case_at: string | null;
          };
          Relationships: [];
        };
        governance_operator_team_comparison_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; entity_type: string; actor_name: string;
            team_name: string | null; assigned_case_count: number; active_open_case_count: number;
            resolution_quality_proxy: number; reopen_rate: number; escalation_rate: number;
            reassignment_rate: number; chronic_case_count: number; severe_case_count: number;
            avg_ack_seconds: number | null; avg_resolve_seconds: number | null; severity_weighted_load: number | null;
          };
          Relationships: [];
        };
        governance_promotion_health_overview: {
          Row: {
            workspace_id: string; workspace_slug: string; promotion_type: string; promotion_count: number;
            improved_count: number; neutral_count: number; degraded_count: number; rollback_candidate_count: number;
            avg_rollback_risk_score: number | null; max_rollback_risk_score: number | null; latest_created_at: string | null;
          };
          Relationships: [];
        };
        governance_operating_risk_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; snapshot_at: string; operating_risk: string;
            supporting_metrics: Record<string, unknown>;
          };
          Relationships: [];
        };
        governance_review_priority_summary: {
          Row: {
            workspace_id: string; priority_rank: number; entity_type: string; entity_key: string;
            entity_label: string; priority_score: number; priority_reason_code: string;
            open_case_count: number; recurring_case_count: number; escalated_case_count: number;
            rollback_risk_count: number; stale_case_count: number; latest_regime: string | null;
            latest_root_cause: string | null; snapshot_at: string;
          };
          Relationships: [];
        };
        governance_trend_window_summary: {
          Row: {
            workspace_id: string; window_label: string; metric_name: string; current_value: number;
            prior_value: number; delta_abs: number; delta_pct: number | null; trend_direction: string;
            computed_at: string;
          };
          Relationships: [];
        };
        governance_operator_performance_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; operator_name: string;
            assigned_case_count: number; active_open_case_count: number; avg_ack_seconds: number | null;
            median_ack_seconds: number | null; avg_resolve_seconds: number | null; median_resolve_seconds: number | null;
            reopened_case_count: number; escalated_case_count: number; chronic_case_count: number;
            resolved_case_count: number; reassigned_case_count: number; severe_case_count: number;
            reopen_rate: number; escalation_rate: number; reassignment_rate: number;
            resolution_rate: number; resolution_quality_proxy: number;
          };
          Relationships: [];
        };
        governance_team_performance_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; assigned_team: string;
            assigned_case_count: number; active_open_case_count: number; avg_ack_seconds: number | null;
            median_ack_seconds: number | null; avg_resolve_seconds: number | null; median_resolve_seconds: number | null;
            reopened_case_count: number; escalated_case_count: number; chronic_case_count: number;
            resolved_case_count: number; reassigned_case_count: number; severe_case_count: number;
            reopen_rate: number; escalation_rate: number; reassignment_rate: number;
            resolution_rate: number; resolution_quality_proxy: number;
          };
          Relationships: [];
        };
        governance_operator_case_mix_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; actor_name: string;
            root_cause_code: string; severity: string; regime: string; case_count: number;
            recurring_case_count: number; chronic_case_count: number; severe_case_count: number;
          };
          Relationships: [];
        };
        governance_team_case_mix_summary: {
          Row: {
            workspace_id: string; workspace_slug: string; actor_name: string;
            root_cause_code: string; severity: string; regime: string; case_count: number;
            recurring_case_count: number; chronic_case_count: number; severe_case_count: number;
          };
          Relationships: [];
        };
        governance_case_aging_summary: {
          Row: {
            case_id: string; workspace_id: string; workspace_slug: string;
            watchlist_id: string | null; watchlist_slug: string | null; title: string;
            status: string; severity: string; current_assignee: string | null; current_team: string | null;
          recurrence_group_id: string | null; repeat_count: number; opened_at: string;
          acknowledged_at: string | null; resolved_at: string | null; closed_at: string | null;
          age_minutes: number | null;
        };
        Relationships: [];
      };
      governance_case_sla_summary: {
        Row: {
          case_id: string; workspace_id: string; workspace_slug: string;
          watchlist_id: string | null; watchlist_slug: string | null; title: string;
          status: string; severity: string; current_assignee: string | null; current_team: string | null;
          repeat_count: number; opened_at: string; acknowledged_at: string | null;
          resolved_at: string | null; closed_at: string | null; policy_id: string | null;
          chronicity_class: string | null; ack_due_at: string | null; resolve_due_at: string | null;
          ack_breached: boolean; resolve_breached: boolean; breach_severity: string | null;
          metadata: Record<string, unknown>; evaluated_at: string | null;
        };
        Relationships: [];
      };
      governance_operator_workload_pressure: {
        Row: {
          workspace_id: string; workspace_slug: string; assigned_to: string; assigned_team: string | null;
          open_case_count: number; recurring_case_count: number; severe_open_case_count: number;
          ack_breached_case_count: number; resolve_breached_case_count: number;
          avg_open_age_minutes: number | null; severity_weighted_load: number | null;
        };
        Relationships: [];
      };
      governance_team_workload_pressure: {
        Row: {
          workspace_id: string; workspace_slug: string; assigned_team: string;
          open_case_count: number; recurring_case_count: number; severe_open_case_count: number;
          ack_breached_case_count: number; resolve_breached_case_count: number;
          avg_open_age_minutes: number | null; severity_weighted_load: number | null;
        };
        Relationships: [];
      };
      governance_stale_case_summary: {
        Row: {
          case_id: string; workspace_id: string; workspace_slug: string;
          watchlist_id: string | null; watchlist_slug: string | null; title: string;
          status: string; severity: string; current_assignee: string | null; current_team: string | null;
          repeat_count: number; age_minutes: number | null; ack_due_at: string | null;
          resolve_due_at: string | null; ack_breached: boolean; resolve_breached: boolean;
          breach_severity: string | null; evaluated_at: string | null;
        };
        Relationships: [];
      };
      governance_escalation_summary: {
        Row: {
          id: string; workspace_id: string; workspace_slug: string; case_id: string;
          watchlist_id: string | null; watchlist_slug: string | null; case_title: string;
          case_status: string; severity: string; current_assignee: string | null; current_team: string | null;
          repeat_count: number; root_cause_code: string | null; escalation_level: string; status: string;
          escalated_to_team: string | null; escalated_to_user: string | null; reason: string | null;
          source_policy_id: string | null; escalated_at: string; last_evaluated_at: string;
          repeated_count: number; cleared_at: string | null; metadata: Record<string, unknown>;
        };
        Relationships: [];
      };
      governance_incident_timeline_summary: {
        Row: {
          case_id: string; workspace_id: string; workspace_slug: string; degradation_state_id: string | null;
          watchlist_id: string | null; watchlist_slug: string | null; watchlist_name: string | null;
          version_tuple: string | null; status: string; severity: string; title: string; summary: string | null;
          opened_at: string; acknowledged_at: string | null; resolved_at: string | null; closed_at: string | null;
          reopened_count: number; current_assignee: string | null; current_team: string | null;
          metadata: Record<string, unknown>; evidence_count: number; run_evidence_count: number;
          cluster_evidence_count: number; version_evidence_count: number; assigned_to: string | null;
          assigned_team: string | null; last_assigned_at: string | null; timeline_event_count: number;
          latest_event_type: string | null; latest_event_at: string | null;
        };
        Relationships: [];
      };
      governance_incident_detail: {
        Row: {
          case_id: string; workspace_id: string; workspace_slug: string; degradation_state_id: string | null;
          watchlist_id: string | null; watchlist_slug: string | null; watchlist_name: string | null;
          version_tuple: string | null; status: string; severity: string; title: string; summary: string | null;
          opened_at: string; acknowledged_at: string | null; resolved_at: string | null; closed_at: string | null;
          reopened_count: number; current_assignee: string | null; current_team: string | null;
          metadata: Record<string, unknown>; evidence_count: number; run_evidence_count: number;
          cluster_evidence_count: number; version_evidence_count: number; assigned_to: string | null;
          assigned_team: string | null; last_assigned_at: string | null; timeline_event_count: number;
          latest_event_type: string | null; latest_event_at: string | null;
        };
        Relationships: [];
      };
      watchlist_anomaly_summary: {
        Row: {
          workspace_id: string; workspace_slug: string; watchlist_id: string | null; watchlist_slug: string | null;
          watchlist_name: string | null; open_cluster_count: number; total_cluster_count: number;
          high_open_cluster_count: number; open_event_count: number; last_seen_at: string | null;
        };
        Relationships: [];
      };
      active_regime_thresholds: {
        Row: {
          workspace_id: string | null; workspace_slug: string | null; profile_id: string; override_id: string | null;
          profile_name: string; regime: string; version_health_floor: number; family_instability_ceiling: number;
          replay_consistency_floor: number; regime_instability_ceiling: number; conflicting_transition_ceiling: number;
          profile_metadata: Record<string, unknown>; override_metadata: Record<string, unknown>;
        };
        Relationships: [];
      };
      governance_threshold_application_summary: {
        Row: {
          id: string; run_id: string | null; workspace_id: string; workspace_slug: string;
          watchlist_id: string | null; watchlist_slug: string | null; watchlist_name: string | null;
          regime: string; profile_id: string | null; profile_name: string | null; override_id: string | null;
          evaluation_stage: string; applied_thresholds: Record<string, unknown>; metadata: Record<string, unknown>;
          created_at: string;
        };
        Relationships: [];
      };
      macro_sync_health: {
        Row: {
          workspace_id: string; workspace_slug: string; provider_mode: string; last_completed_at: string | null;
          completed_runs: number; failed_runs: number; last_error: string | null;
        };
        Relationships: [];
      };
      watchlist_sla_summary: {
        Row: {
          workspace_id: string; workspace_slug: string; watchlist_id: string | null;
          watchlist_slug: string | null; watchlist_name: string | null;
          completed_24h: number; failed_24h: number; last_success_at: string | null;
          seconds_since_last_success: number | null; avg_runtime_ms_24h: number | null;
        };
        Relationships: [];
      };
      queue_governance_state: {
        Row: {
          workspace_id: string; workspace_slug: string; watchlist_id: string | null;
          watchlist_slug: string | null; watchlist_name: string | null; job_type: string;
          queued_count: number; claimed_count: number; oldest_queued_at: string | null;
          highest_priority_queued: number | null;
        };
        Relationships: [];
      };
      governance_routing_policy_review_summary: {
        Row: {
          workspace_id: string; recommendation_key: string;
          latest_review_status: "approved" | "rejected" | "deferred";
          latest_review_reason: string | null; latest_reviewed_by: string | null;
          latest_reviewed_at: string | null; review_count: number;
          has_approved_review: boolean; has_rejected_review: boolean; has_deferred_review: boolean;
        };
        Relationships: [];
      };
      governance_routing_policy_promotion_summary: {
        Row: {
          workspace_id: string; recommendation_key: string;
          latest_proposal_id: string | null; proposal_count: number;
          latest_proposal_status: "pending" | "approved" | "rejected" | "applied" | "deferred" | null;
          latest_promotion_target: "override" | "rule" | null;
          latest_scope_type: string | null; latest_scope_value: string | null;
          latest_proposed_by: string | null; latest_proposed_at: string | null;
          latest_approved_by: string | null; latest_approved_at: string | null;
          latest_applied_at: string | null; application_count: number;
        };
        Relationships: [];
      };
      governance_routing_policy_rollback_candidate_summary: {
        Row: {
          workspace_id: string; recommendation_key: string; application_id: string;
          scope_type: string; scope_value: string; applied_at: string;
          rollback_risk_score: number; rollback_reason_code: string;
          supporting_metrics: Record<string, unknown>;
        };
        Relationships: [];
      };
      // Phase 3.5C: Autopromotion tables
      governance_routing_policy_autopromotion_policies: TableDef<{
        id: string; workspace_id: string; enabled: boolean;
        scope_type: string; scope_value: string;
        promotion_target: "rule" | "override";
        min_confidence: "low" | "medium" | "high";
        min_approved_review_count: number; min_application_count: number;
        min_sample_size: number; max_recent_override_rate: number;
        max_recent_reassignment_rate: number; cooldown_hours: number;
        created_by: string; created_at: string; updated_at: string;
        metadata: Record<string, unknown>;
      }>;
      governance_routing_policy_autopromotion_executions: TableDef<{
        id: string; workspace_id: string; policy_id: string; recommendation_key: string;
        proposal_id: string | null; application_id: string | null;
        outcome: "promoted" | "skipped" | "blocked";
        blocked_reason: string | null; skipped_reason: string | null;
        executed_by: string; executed_at: string;
        prior_policy: Record<string, unknown>; applied_policy: Record<string, unknown>;
        metadata: Record<string, unknown>;
      }>;
      governance_routing_policy_autopromotion_rollback_candidates: TableDef<{
        id: string; workspace_id: string; execution_id: string; recommendation_key: string;
        scope_type: string; scope_value: string;
        prior_policy: Record<string, unknown>; applied_policy: Record<string, unknown>;
        routing_row_id: string | null; routing_table: string | null;
        resolved: boolean; resolved_at: string | null; resolved_by: string | null;
        created_at: string; metadata: Record<string, unknown>;
      }>;
      // Phase 3.5C: Autopromotion views (listed under Tables for Supabase client compatibility)
      governance_routing_policy_autopromotion_summary: TableDef<{
        workspace_id: string; recommendation_key: string;
        latest_execution_id: string | null; latest_policy_id: string | null;
        latest_proposal_id: string | null; latest_application_id: string | null;
        latest_outcome: "promoted" | "skipped" | "blocked" | null;
        latest_blocked_reason: string | null; latest_skipped_reason: string | null;
        latest_executed_by: string | null; latest_executed_at: string | null;
        promoted_count: number; blocked_count: number; skipped_count: number;
        total_executions: number; open_rollback_count: number;
      }>;
      governance_routing_policy_autopromotion_eligibility: TableDef<{
        workspace_id: string; recommendation_key: string;
        scope_type: string; scope_value: string;
        confidence: string; sample_size: number;
        expected_benefit_score: number | null; risk_score: number | null;
        policy_id: string; promotion_target: string;
        approved_review_count: number; application_count: number;
        last_promoted_at: string | null; is_eligible: boolean; blocked_reason: string | null;
      }>;
      // Phase 3.6A: Rollback tables
      governance_routing_policy_rollback_reviews: TableDef<{
        id: string; workspace_id: string; rollback_candidate_id: string;
        review_status: "approved" | "rejected" | "deferred";
        review_reason: string | null; reviewed_by: string; reviewed_at: string;
        notes: string | null; metadata: Record<string, unknown>;
      }>;
      governance_routing_policy_rollback_executions: TableDef<{
        id: string; workspace_id: string; rollback_candidate_id: string;
        execution_target: "override" | "rule";
        scope_type: string; scope_value: string;
        promotion_execution_id: string;
        restored_policy: Record<string, unknown>; replaced_policy: Record<string, unknown>;
        executed_by: string; executed_at: string; metadata: Record<string, unknown>;
      }>;
      // Phase 3.6A: Rollback views (as TableDef for Supabase client)
      governance_routing_policy_rollback_review_summary: TableDef<{
        workspace_id: string; rollback_candidate_id: string; recommendation_key: string;
        latest_review_status: "approved" | "rejected" | "deferred" | null;
        latest_review_reason: string | null; latest_reviewed_by: string | null;
        latest_reviewed_at: string | null; review_count: number;
        has_approved_review: boolean; has_rejected_review: boolean; has_deferred_review: boolean;
      }>;
      governance_routing_policy_rollback_execution_summary: TableDef<{
        workspace_id: string; rollback_candidate_id: string; recommendation_key: string;
        scope_type: string; scope_value: string; target_type: string | null;
        rollback_risk_score: number; rolled_back: boolean; rolled_back_at: string | null;
        execution_count: number; latest_execution_id: string | null;
        latest_executed_by: string | null; latest_executed_at: string | null;
      }>;
      governance_routing_policy_pending_rollback_summary: TableDef<{
        workspace_id: string; rollback_candidate_id: string; recommendation_key: string;
        scope_type: string; scope_value: string; rollback_risk_score: number;
        latest_review_status: "approved" | "rejected" | "deferred" | null;
        needs_action: boolean; created_at: string; latest_execution_at: string | null;
      }>;
      // Phase 3.6B: Rollback impact table + views
      governance_routing_policy_rollback_impact_snapshots: TableDef<{
        id: string; workspace_id: string; rollback_execution_id: string;
        rollback_candidate_id: string; recommendation_key: string;
        scope_type: string; scope_value: string; target_type: string;
        evaluation_window_label: string; evaluation_started_at: string;
        impact_classification: "improved" | "neutral" | "degraded" | "insufficient_data";
        before_metrics: Record<string, unknown>; after_metrics: Record<string, unknown>;
        delta_metrics: Record<string, unknown>; metadata: Record<string, unknown>;
        created_at: string;
      }>;
      governance_routing_policy_rollback_impact_summary: TableDef<{
        workspace_id: string; rollback_execution_id: string; rollback_candidate_id: string;
        recommendation_key: string; scope_type: string; scope_value: string; target_type: string;
        impact_classification: "improved" | "neutral" | "degraded" | "insufficient_data";
        evaluation_window_label: string; created_at: string;
        before_recurrence_rate: number | null; after_recurrence_rate: number | null;
        before_reassignment_rate: number | null; after_reassignment_rate: number | null;
        before_escalation_rate: number | null; after_escalation_rate: number | null;
        before_avg_resolve_latency_seconds: number | null; after_avg_resolve_latency_seconds: number | null;
        before_reopen_rate: number | null; after_reopen_rate: number | null;
        before_workload_pressure: number | null; after_workload_pressure: number | null;
        delta_recurrence_rate: number | null; delta_reassignment_rate: number | null;
        delta_escalation_rate: number | null; delta_avg_resolve_latency_seconds: number | null;
        delta_reopen_rate: number | null; delta_workload_pressure: number | null;
      }>;
      governance_routing_policy_rollback_effectiveness_summary: TableDef<{
        workspace_id: string; rollback_count: number;
        improved_count: number; neutral_count: number;
        degraded_count: number; insufficient_data_count: number;
        improved_rate: number | null; degraded_rate: number | null;
        latest_rollback_at: string | null;
        average_delta_recurrence_rate: number | null; average_delta_escalation_rate: number | null;
        average_delta_resolve_latency_seconds: number | null; average_delta_workload_pressure: number | null;
      }>;
      governance_routing_policy_rollback_pending_evaluation_summary: TableDef<{
        workspace_id: string; rollback_execution_id: string; rollback_candidate_id: string;
        recommendation_key: string; scope_type: string; scope_value: string; target_type: string;
        executed_at: string; days_since_execution: number;
        has_impact_snapshot: boolean; sufficient_post_data: boolean; pending_reason_code: string;
      }>;
      governance_routing_feature_effectiveness_summary: {
        Row: {
          workspace_id: string; feature_type: string; feature_key: string; case_count: number;
          accepted_recommendation_count: number; override_count: number; reassignment_count: number;
          reopen_count: number; escalation_count: number; avg_ack_latency_seconds: number | null;
          avg_resolve_latency_seconds: number | null; effectiveness_score: number;
          workload_penalty_score: number; net_fit_score: number;
        };
        Relationships: [];
      };
      governance_routing_context_fit_summary: {
        Row: {
          workspace_id: string; context_key: string; recommended_user: string | null;
          recommended_team: string | null; operator_fit_score: number | null; team_fit_score: number | null;
          confidence: string; sample_size: number;
        };
        Relationships: [];
      };
      governance_routing_policy_opportunity_summary: {
        Row: {
          id: string; workspace_id: string; recommendation_key: string; reason_code: string;
          scope_type: string; scope_value: string | null; recommended_policy: Record<string, unknown>;
          confidence: string; expected_benefit_score: number; risk_score: number; sample_size: number;
          signal_payload: Record<string, unknown>; snapshot_id: string | null;
          created_at: string; updated_at: string;
        };
        Relationships: [];
      };
      governance_policy_optimization_snapshots: {
        Row: {
          id: string; workspace_id: string; snapshot_at: string; window_label: string;
          recommendation_count: number; metadata: Record<string, unknown>;
        };
        Relationships: [];
      };
      governance_policy_recommendations: {
        Row: {
          id: string; workspace_id: string; recommendation_key: string; policy_family: string;
          scope_type: string; scope_value: string; current_policy: Record<string, unknown>;
          recommended_policy: Record<string, unknown>; reason_code: string; confidence: string;
          sample_size: number; expected_benefit_score: number; risk_score: number;
          supporting_metrics: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      governance_policy_feature_effectiveness_summary: {
        Row: {
          workspace_id: string; policy_family: string; feature_type: string; feature_key: string;
          sample_size: number; recurrence_rate: number | null; reopen_rate: number | null;
          escalation_rate: number | null; reassignment_rate: number | null; rollback_rate: number | null;
          mute_rate: number | null; approved_review_rate: number | null; application_rate: number | null;
          avg_ack_latency_seconds: number | null; avg_resolve_latency_seconds: number | null;
          effectiveness_score: number; risk_score: number; net_policy_fit_score: number;
        };
        Relationships: [];
      };
      governance_policy_context_fit_summary: {
        Row: {
          workspace_id: string; context_key: string; best_policy_family: string;
          best_policy_variant: string; fit_score: number; sample_size: number;
          confidence: string; supporting_metrics: Record<string, unknown>;
        };
        Relationships: [];
      };
      governance_policy_opportunity_summary: {
        Row: {
          workspace_id: string; recommendation_key: string; policy_family: string;
          scope_type: string; scope_value: string; current_policy: Record<string, unknown>;
          recommended_policy: Record<string, unknown>; reason_code: string; confidence: string;
          sample_size: number; expected_benefit_score: number; risk_score: number;
          supporting_metrics: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      governance_policy_recommendation_reviews: {
        Row: {
          id: string; workspace_id: string; recommendation_key: string; policy_family: string;
          review_status: string; review_reason: string | null; reviewed_by: string;
          reviewed_at: string; notes: string | null; metadata: Record<string, unknown>;
        };
        Relationships: [];
      };
      governance_policy_promotion_proposals: {
        Row: {
          id: string; workspace_id: string; recommendation_key: string; policy_family: string;
          proposal_status: string; promotion_target: string; scope_type: string; scope_value: string;
          current_policy: Record<string, unknown>; recommended_policy: Record<string, unknown>;
          proposed_by: string; proposed_at: string; approved_by: string | null;
          approved_at: string | null; applied_at: string | null; proposal_reason: string | null;
          metadata: Record<string, unknown>;
        };
        Relationships: [];
      };
      governance_policy_applications: {
        Row: {
          id: string; workspace_id: string; proposal_id: string; recommendation_key: string;
          policy_family: string; applied_target: string; applied_scope_type: string;
          applied_scope_value: string; prior_policy: Record<string, unknown>;
          applied_policy: Record<string, unknown>; applied_by: string; applied_at: string;
          rollback_candidate: boolean; metadata: Record<string, unknown>;
        };
        Relationships: [];
      };
      governance_policy_review_summary: {
        Row: {
          workspace_id: string; recommendation_key: string; policy_family: string;
          latest_review_status: string | null; latest_review_reason: string | null;
          latest_reviewed_by: string | null; latest_reviewed_at: string | null;
          review_count: number; has_approved_review: boolean; has_rejected_review: boolean;
          has_deferred_review: boolean;
        };
        Relationships: [];
      };
      governance_policy_promotion_summary: {
        Row: {
          workspace_id: string; recommendation_key: string; policy_family: string;
          proposal_count: number; latest_proposal_status: string | null;
          latest_promotion_target: string | null; latest_scope_type: string | null;
          latest_scope_value: string | null; latest_proposed_by: string | null;
          latest_proposed_at: string | null; latest_approved_by: string | null;
          latest_approved_at: string | null; latest_applied_at: string | null;
          application_count: number;
        };
        Relationships: [];
      };
      governance_policy_pending_promotion_summary: {
        Row: {
          workspace_id: string; recommendation_key: string; policy_family: string;
          latest_proposal_status: string; latest_promotion_target: string | null;
          latest_scope_type: string | null; latest_scope_value: string | null;
          latest_proposed_by: string | null; latest_proposed_at: string | null;
          application_count: number; needs_action: boolean;
        };
        Relationships: [];
      };
      governance_policy_autopromotion_policies: {
        Row: {
          id: string; workspace_id: string; policy_family: string; scope_type: string;
          scope_value: string; promotion_target: string; min_confidence: string;
          min_approved_review_count: number; min_application_count: number;
          min_sample_size: number; max_recent_override_rate: number;
          max_recent_reassignment_rate: number; cooldown_hours: number;
          enabled: boolean; created_by: string; created_at: string;
          metadata: Record<string, unknown>;
        };
        Insert: Partial<{ id: string; workspace_id: string; policy_family: string; scope_type: string;
          scope_value: string; promotion_target: string; min_confidence: string;
          min_approved_review_count: number; min_application_count: number;
          min_sample_size: number; max_recent_override_rate: number;
          max_recent_reassignment_rate: number; cooldown_hours: number;
          enabled: boolean; created_by: string; metadata: Record<string, unknown>; }>;
        Update: Partial<{ enabled: boolean; min_confidence: string;
          min_approved_review_count: number; min_application_count: number;
          cooldown_hours: number; metadata: Record<string, unknown>; }>;
        Relationships: [];
      };
      governance_policy_autopromotion_executions: {
        Row: {
          id: string; workspace_id: string; policy_id: string; recommendation_key: string;
          policy_family: string; promotion_target: string; scope_type: string;
          scope_value: string; current_policy: Record<string, unknown>;
          applied_policy: Record<string, unknown>; executed_by: string;
          executed_at: string; cooldown_applied: boolean; metadata: Record<string, unknown>;
        };
        Insert: Partial<{ id: string; workspace_id: string; policy_id: string;
          recommendation_key: string; policy_family: string; promotion_target: string;
          scope_type: string; scope_value: string; current_policy: Record<string, unknown>;
          applied_policy: Record<string, unknown>; executed_by: string;
          cooldown_applied: boolean; metadata: Record<string, unknown>; }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      governance_policy_autopromotion_rollback_candidates: {
        Row: {
          id: string; workspace_id: string; execution_id: string;
          recommendation_key: string; policy_family: string; scope_type: string;
          scope_value: string; target_type: string; prior_policy: Record<string, unknown>;
          applied_policy: Record<string, unknown>; rollback_reason_code: string | null;
          rollback_risk_score: number; rolled_back: boolean; rolled_back_at: string | null;
          rolled_back_by: string | null; metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{ id: string; workspace_id: string; execution_id: string;
          recommendation_key: string; policy_family: string; scope_type: string;
          scope_value: string; target_type: string; prior_policy: Record<string, unknown>;
          applied_policy: Record<string, unknown>; rollback_reason_code: string | null;
          rollback_risk_score: number; rolled_back: boolean; metadata: Record<string, unknown>; }>;
        Update: Partial<{ rolled_back: boolean; rolled_back_at: string; rolled_back_by: string; }>;
        Relationships: [];
      };
      governance_policy_autopromotion_summary: {
        Row: {
          policy_id: string; workspace_id: string; policy_family: string;
          scope_type: string; scope_value: string; promotion_target: string;
          min_confidence: string; min_approved_review_count: number;
          min_application_count: number; cooldown_hours: number; enabled: boolean;
          execution_count: number; rollback_candidate_count: number;
          latest_execution_at: string | null;
        };
        Relationships: [];
      };
      governance_policy_autopromotion_eligibility: {
        Row: {
          workspace_id: string; recommendation_key: string; policy_id: string;
          policy_family: string; scope_type: string; scope_value: string;
          promotion_target: string; eligible: boolean; blocked_reason_code: string | null;
          confidence: string | null; sample_size: number; approved_review_count: number;
          application_count: number; cooldown_ends_at: string | null;
        };
        Relationships: [];
      };
      asset_universe_catalog: {
        Row: {
          id: string; symbol: string; canonical_symbol: string; asset_class: string;
          venue: string | null; quote_currency: string | null; base_currency: string | null;
          region: string | null; is_active: boolean;
          metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{
          id: string; symbol: string; canonical_symbol: string; asset_class: string;
          venue: string | null; quote_currency: string | null; base_currency: string | null;
          region: string | null; is_active: boolean; metadata: Record<string, unknown>;
        }>;
        Update: Partial<{
          is_active: boolean; metadata: Record<string, unknown>;
          venue: string | null; quote_currency: string | null; base_currency: string | null;
          region: string | null;
        }>;
        Relationships: [];
      };
      multi_asset_sync_health_summary: {
        Row: {
          workspace_id: string; provider_family: string | null; asset_class: string;
          requested_symbol_count: number; synced_symbol_count: number; failed_symbol_count: number;
          latest_run_started_at: string | null; latest_run_completed_at: string | null;
          latest_status: string | null; latest_provider_mode: string | null;
          latest_metadata: Record<string, unknown>;
        };
        Relationships: [];
      };
      normalized_multi_asset_market_state: {
        Row: {
          workspace_id: string; symbol: string; canonical_symbol: string; asset_class: string;
          provider_family: string | null; price: number | string | null;
          price_timestamp: string | null; volume_24h: number | string | null;
          oi_change_1h: number | string | null; funding_rate: number | string | null;
          yield_value: number | string | null; fx_return_1d: number | string | null;
          macro_proxy_value: number | string | null; liquidation_count: number | null;
          metadata: Record<string, unknown>;
        };
        Relationships: [];
      };
      multi_asset_family_state_summary: {
        Row: {
          workspace_id: string; asset_class: string; family_key: string;
          symbol_count: number; latest_timestamp: string | null;
          avg_return_1d: number | string | null; avg_volatility_proxy: number | string | null;
          metadata: Record<string, unknown>;
        };
        Relationships: [];
      };
      asset_dependency_graph: {
        Row: {
          id: string; from_symbol: string; to_symbol: string;
          dependency_type: string; dependency_family: string;
          priority: number; weight: number | string;
          is_active: boolean; metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{
          id: string; from_symbol: string; to_symbol: string;
          dependency_type: string; dependency_family: string;
          priority: number; weight: number | string;
          is_active: boolean; metadata: Record<string, unknown>;
        }>;
        Update: Partial<{
          dependency_type: string; dependency_family: string;
          priority: number; weight: number | string;
          is_active: boolean; metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      watchlist_dependency_profiles: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string; profile_name: string;
          include_macro: boolean; include_fx: boolean; include_rates: boolean;
          include_equity_index: boolean; include_commodity: boolean; include_crypto_cross: boolean;
          max_dependencies: number; is_active: boolean;
          metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string; profile_name: string;
          include_macro: boolean; include_fx: boolean; include_rates: boolean;
          include_equity_index: boolean; include_commodity: boolean; include_crypto_cross: boolean;
          max_dependencies: number; is_active: boolean; metadata: Record<string, unknown>;
        }>;
        Update: Partial<{
          profile_name: string; include_macro: boolean; include_fx: boolean; include_rates: boolean;
          include_equity_index: boolean; include_commodity: boolean; include_crypto_cross: boolean;
          max_dependencies: number; is_active: boolean; metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      asset_family_mappings: {
        Row: {
          id: string; symbol: string; asset_class: string; family_key: string;
          family_label: string; region: string | null;
          is_active: boolean; metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{
          id: string; symbol: string; asset_class: string; family_key: string;
          family_label: string; region: string | null;
          is_active: boolean; metadata: Record<string, unknown>;
        }>;
        Update: Partial<{
          family_label: string; region: string | null;
          is_active: boolean; metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      watchlist_context_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          profile_id: string | null; snapshot_at: string;
          primary_symbols: string[]; dependency_symbols: string[]; dependency_families: string[];
          context_hash: string;
          coverage_summary: Record<string, unknown>; metadata: Record<string, unknown>;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          profile_id: string | null;
          primary_symbols: string[]; dependency_symbols: string[]; dependency_families: string[];
          context_hash: string;
          coverage_summary: Record<string, unknown>; metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      watchlist_dependency_coverage_summary: {
        Row: {
          workspace_id: string; watchlist_id: string; context_hash: string;
          primary_symbol_count: number; dependency_symbol_count: number;
          dependency_family_count: number;
          covered_dependency_count: number; missing_dependency_count: number;
          stale_dependency_count: number;
          latest_context_snapshot_at: string | null;
          coverage_ratio: number | string | null;
          metadata: Record<string, unknown>;
        };
        Relationships: [];
      };
      watchlist_dependency_context_detail: {
        Row: {
          workspace_id: string; watchlist_id: string; context_hash: string;
          symbol: string; asset_class: string | null;
          dependency_family: string; dependency_type: string | null;
          priority: number | null; weight: number | string | null;
          is_primary: boolean; latest_timestamp: string | null;
          is_missing: boolean; is_stale: boolean;
          metadata: Record<string, unknown>;
        };
        Relationships: [];
      };
      watchlist_dependency_family_state: {
        Row: {
          workspace_id: string; watchlist_id: string; context_hash: string;
          dependency_family: string;
          symbol_count: number; covered_count: number; missing_count: number; stale_count: number;
          latest_timestamp: string | null;
          metadata: Record<string, unknown>;
        };
        Relationships: [];
      };
      cross_asset_feature_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          feature_family: string; feature_key: string;
          feature_value: number | string | null; feature_state: string;
          dependency_symbols: string[]; dependency_families: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          feature_family: string; feature_key: string;
          feature_value: number | string | null; feature_state: string;
          dependency_symbols: string[]; dependency_families: string[];
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_signal_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          signal_family: string; signal_key: string;
          signal_value: number | string | null;
          signal_direction: string | null; signal_state: string;
          base_symbol: string | null;
          dependency_symbols: string[]; dependency_families: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          signal_family: string; signal_key: string;
          signal_value: number | string | null;
          signal_direction: string | null; signal_state: string;
          base_symbol: string | null;
          dependency_symbols: string[]; dependency_families: string[];
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_signal_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          signal_family: string; signal_key: string;
          signal_value: number | string | null;
          signal_direction: string | null; signal_state: string;
          base_symbol: string | null;
          dependency_symbol_count: number; dependency_family_count: number;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_dependency_health_summary: {
        Row: {
          workspace_id: string; watchlist_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          feature_count: number; signal_count: number;
          missing_dependency_count: number; stale_dependency_count: number;
          confirmed_count: number; contradicted_count: number;
          latest_created_at: string | null;
        };
        Relationships: [];
      };
      run_cross_asset_context_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
          context_snapshot_id: string | null;
          cross_asset_feature_count: number; cross_asset_signal_count: number;
          confirmed_signal_count: number; contradicted_signal_count: number;
          missing_context_count: number; stale_context_count: number;
          dominant_dependency_family: string | null;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_explanation_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
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
          explanation_state: string;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
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
          explanation_state: string;
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_family_contribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          dependency_family: string;
          family_signal_count: number;
          confirmed_count: number; contradicted_count: number;
          missing_count: number; stale_count: number;
          family_confidence_score: number | string | null;
          family_support_score: number | string | null;
          family_contradiction_score: number | string | null;
          top_symbols: string[];
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          dependency_family: string;
          family_signal_count: number;
          confirmed_count: number; contradicted_count: number;
          missing_count: number; stale_count: number;
          family_confidence_score: number | string | null;
          family_support_score: number | string | null;
          family_contradiction_score: number | string | null;
          top_symbols: string[];
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_explanation_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
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
          explanation_state: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_explanation_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          dependency_family: string;
          family_signal_count: number;
          confirmed_count: number; contradicted_count: number;
          missing_count: number; stale_count: number;
          family_confidence_score: number | string | null;
          family_support_score: number | string | null;
          family_contradiction_score: number | string | null;
          top_symbols: string[];
          created_at: string;
        };
        Relationships: [];
      };
      run_cross_asset_explanation_bridge: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
          context_snapshot_id: string | null;
          dominant_dependency_family: string | null;
          cross_asset_confidence_score: number | string | null;
          confirmation_score: number | string | null;
          contradiction_score: number | string | null;
          missing_context_score: number | string | null;
          stale_context_score: number | string | null;
          explanation_state: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          base_signal_score: number | string | null;
          cross_asset_signal_score: number | string | null;
          cross_asset_confirmation_score: number | string | null;
          cross_asset_contradiction_penalty: number | string | null;
          cross_asset_missing_penalty: number | string | null;
          cross_asset_stale_penalty: number | string | null;
          cross_asset_net_contribution: number | string | null;
          composite_pre_cross_asset: number | string | null;
          composite_post_cross_asset: number | string | null;
          integration_mode: string;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          base_signal_score: number | string | null;
          cross_asset_signal_score: number | string | null;
          cross_asset_confirmation_score: number | string | null;
          cross_asset_contradiction_penalty: number | string | null;
          cross_asset_missing_penalty: number | string | null;
          cross_asset_stale_penalty: number | string | null;
          cross_asset_net_contribution: number | string | null;
          composite_pre_cross_asset: number | string | null;
          composite_post_cross_asset: number | string | null;
          integration_mode: string;
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_family_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          family_signal_score: number | string | null;
          family_confirmation_score: number | string | null;
          family_contradiction_penalty: number | string | null;
          family_missing_penalty: number | string | null;
          family_stale_penalty: number | string | null;
          family_net_contribution: number | string | null;
          family_rank: number | null;
          top_symbols: string[];
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          family_signal_score: number | string | null;
          family_confirmation_score: number | string | null;
          family_contradiction_penalty: number | string | null;
          family_missing_penalty: number | string | null;
          family_stale_penalty: number | string | null;
          family_net_contribution: number | string | null;
          family_rank: number | null;
          top_symbols: string[];
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          base_signal_score: number | string | null;
          cross_asset_signal_score: number | string | null;
          cross_asset_confirmation_score: number | string | null;
          cross_asset_contradiction_penalty: number | string | null;
          cross_asset_missing_penalty: number | string | null;
          cross_asset_stale_penalty: number | string | null;
          cross_asset_net_contribution: number | string | null;
          composite_pre_cross_asset: number | string | null;
          composite_post_cross_asset: number | string | null;
          integration_mode: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
        };
        Relationships: [];
      };
      run_composite_integration_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
          base_signal_score: number | string | null;
          cross_asset_signal_score: number | string | null;
          cross_asset_net_contribution: number | string | null;
          composite_pre_cross_asset: number | string | null;
          composite_post_cross_asset: number | string | null;
          dominant_dependency_family: string | null;
          cross_asset_confidence_score: number | string | null;
          created_at: string;
        };
        Relationships: [];
      };
      dependency_weighting_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          priority_weight_scale: number | string;
          direct_dependency_bonus: number | string;
          secondary_dependency_penalty: number | string;
          missing_penalty_scale: number | string;
          stale_penalty_scale: number | string;
          family_weight_overrides: Record<string, unknown>;
          type_weight_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          priority_weight_scale: number | string;
          direct_dependency_bonus: number | string;
          secondary_dependency_penalty: number | string;
          missing_penalty_scale: number | string;
          stale_penalty_scale: number | string;
          family_weight_overrides: Record<string, unknown>;
          type_weight_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>;
        }>;
        Update: Partial<{
          profile_name: string; is_active: boolean;
          priority_weight_scale: number | string;
          direct_dependency_bonus: number | string;
          secondary_dependency_penalty: number | string;
          missing_penalty_scale: number | string;
          stale_penalty_scale: number | string;
          family_weight_overrides: Record<string, unknown>;
          type_weight_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      cross_asset_family_weighted_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          weighting_profile_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          priority_weight: number | string | null;
          family_weight: number | string | null;
          type_weight: number | string | null;
          coverage_weight: number | string | null;
          weighted_family_net_contribution: number | string | null;
          weighted_family_rank: number | null;
          top_symbols: string[];
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          weighting_profile_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          priority_weight: number | string | null;
          family_weight: number | string | null;
          type_weight: number | string | null;
          coverage_weight: number | string | null;
          weighted_family_net_contribution: number | string | null;
          weighted_family_rank: number | null;
          top_symbols: string[];
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_symbol_weighted_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          weighting_profile_id: string | null;
          symbol: string; dependency_family: string;
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
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          weighting_profile_id: string | null;
          symbol: string; dependency_family: string;
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
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_family_weighted_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
        };
        Relationships: [];
      };
      cross_asset_symbol_weighted_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          symbol: string; dependency_family: string;
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
        };
        Relationships: [];
      };
      run_cross_asset_weighted_integration_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
          context_snapshot_id: string | null;
          base_signal_score: number | string | null;
          cross_asset_net_contribution: number | string | null;
          weighted_cross_asset_net_contribution: number | string | null;
          dominant_dependency_family: string | null;
          weighted_dominant_dependency_family: string | null;
          created_at: string;
        };
        Relationships: [];
      };
      regime_cross_asset_interpretation_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string;
          regime_key: string; is_active: boolean;
          family_weight_overrides: Record<string, unknown>;
          type_weight_overrides: Record<string, unknown>;
          confirmation_scale: number | string;
          contradiction_scale: number | string;
          missing_penalty_scale: number | string;
          stale_penalty_scale: number | string;
          dominance_threshold: number | string;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; profile_name: string;
          regime_key: string; is_active: boolean;
          family_weight_overrides: Record<string, unknown>;
          type_weight_overrides: Record<string, unknown>;
          confirmation_scale: number | string;
          contradiction_scale: number | string;
          missing_penalty_scale: number | string;
          stale_penalty_scale: number | string;
          dominance_threshold: number | string;
          metadata: Record<string, unknown>;
        }>;
        Update: Partial<{
          profile_name: string; regime_key: string; is_active: boolean;
          family_weight_overrides: Record<string, unknown>;
          type_weight_overrides: Record<string, unknown>;
          confirmation_scale: number | string;
          contradiction_scale: number | string;
          missing_penalty_scale: number | string;
          stale_penalty_scale: number | string;
          dominance_threshold: number | string;
          metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      cross_asset_family_regime_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          regime_key: string;
          interpretation_profile_id: string | null;
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
          interpretation_state: string;
          top_symbols: string[];
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          regime_key: string;
          interpretation_profile_id: string | null;
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
          interpretation_state: string;
          top_symbols: string[];
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_symbol_regime_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          regime_key: string;
          interpretation_profile_id: string | null;
          symbol: string; dependency_family: string;
          dependency_type: string | null;
          graph_priority: number | null;
          is_direct_dependency: boolean;
          raw_symbol_score: number | string | null;
          weighted_symbol_score: number | string | null;
          regime_family_weight: number | string | null;
          regime_type_weight: number | string | null;
          regime_adjusted_symbol_score: number | string | null;
          symbol_rank: number | null;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          regime_key: string;
          interpretation_profile_id: string | null;
          symbol: string; dependency_family: string;
          dependency_type: string | null;
          graph_priority: number | null;
          is_direct_dependency: boolean;
          raw_symbol_score: number | string | null;
          weighted_symbol_score: number | string | null;
          regime_family_weight: number | string | null;
          regime_type_weight: number | string | null;
          regime_adjusted_symbol_score: number | string | null;
          symbol_rank: number | null;
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_family_regime_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
          interpretation_state: string;
          top_symbols: string[];
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_symbol_regime_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          regime_key: string;
          symbol: string; dependency_family: string;
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
        };
        Relationships: [];
      };
      run_cross_asset_regime_integration_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_replay_validation_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null;
          replay_regime_key: string | null;
          context_hash_match: boolean; regime_match: boolean;
          raw_attribution_match: boolean; weighted_attribution_match: boolean;
          regime_attribution_match: boolean;
          dominant_family_match: boolean;
          weighted_dominant_family_match: boolean;
          regime_dominant_family_match: boolean;
          raw_delta: Record<string, unknown>;
          weighted_delta: Record<string, unknown>;
          regime_delta: Record<string, unknown>;
          drift_reason_codes: string[];
          validation_state: string;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null;
          replay_regime_key: string | null;
          context_hash_match: boolean; regime_match: boolean;
          raw_attribution_match: boolean; weighted_attribution_match: boolean;
          regime_attribution_match: boolean;
          dominant_family_match: boolean;
          weighted_dominant_family_match: boolean;
          regime_dominant_family_match: boolean;
          raw_delta: Record<string, unknown>;
          weighted_delta: Record<string, unknown>;
          regime_delta: Record<string, unknown>;
          drift_reason_codes: string[];
          validation_state: string;
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_family_replay_stability_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_replay_validation_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null;
          replay_regime_key: string | null;
          context_hash_match: boolean; regime_match: boolean;
          raw_attribution_match: boolean; weighted_attribution_match: boolean;
          regime_attribution_match: boolean;
          dominant_family_match: boolean;
          weighted_dominant_family_match: boolean;
          regime_dominant_family_match: boolean;
          drift_reason_codes: string[];
          validation_state: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_replay_stability_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_replay_stability_aggregate: {
        Row: {
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
        };
        Relationships: [];
      };
      cross_asset_lead_lag_pair_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          base_symbol: string; dependency_symbol: string;
          dependency_family: string; dependency_type: string | null;
          lag_bucket: string;
          best_lag_hours: number | null;
          timing_strength: number | string | null;
          correlation_at_best_lag: number | string | null;
          base_return_series_key: string | null;
          dependency_return_series_key: string | null;
          window_label: string;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          base_symbol: string; dependency_symbol: string;
          dependency_family: string; dependency_type: string | null;
          lag_bucket: string;
          best_lag_hours: number | null;
          timing_strength: number | string | null;
          correlation_at_best_lag: number | string | null;
          base_return_series_key: string | null;
          dependency_return_series_key: string | null;
          window_label: string;
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_family_timing_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          dependency_family: string;
          lead_pair_count: number; coincident_pair_count: number; lag_pair_count: number;
          avg_best_lag_hours: number | string | null;
          avg_timing_strength: number | string | null;
          dominant_timing_class: string;
          top_leading_symbols: string[];
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          dependency_family: string;
          lead_pair_count: number; coincident_pair_count: number; lag_pair_count: number;
          avg_best_lag_hours: number | string | null;
          avg_timing_strength: number | string | null;
          dominant_timing_class: string;
          top_leading_symbols: string[];
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_lead_lag_pair_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          base_symbol: string; dependency_symbol: string;
          dependency_family: string; dependency_type: string | null;
          lag_bucket: string;
          best_lag_hours: number | null;
          timing_strength: number | string | null;
          correlation_at_best_lag: number | string | null;
          window_label: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_timing_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string | null; context_snapshot_id: string | null;
          dependency_family: string;
          lead_pair_count: number; coincident_pair_count: number; lag_pair_count: number;
          avg_best_lag_hours: number | string | null;
          avg_timing_strength: number | string | null;
          dominant_timing_class: string;
          top_leading_symbols: string[];
          created_at: string;
        };
        Relationships: [];
      };
      run_cross_asset_timing_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
          context_snapshot_id: string | null;
          lead_pair_count: number; coincident_pair_count: number; lag_pair_count: number;
          dominant_leading_family: string | null;
          strongest_leading_symbol: string | null;
          avg_timing_strength: number | string | null;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_timing_attribution_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          lead_weight: number | string;
          coincident_weight: number | string;
          lag_weight: number | string;
          insufficient_data_weight: number | string;
          lead_bonus_scale: number | string;
          lag_penalty_scale: number | string;
          family_weight_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          lead_weight: number | string;
          coincident_weight: number | string;
          lag_weight: number | string;
          insufficient_data_weight: number | string;
          lead_bonus_scale: number | string;
          lag_penalty_scale: number | string;
          family_weight_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>;
        }>;
        Update: Partial<{
          profile_name: string; is_active: boolean;
          lead_weight: number | string;
          coincident_weight: number | string;
          lag_weight: number | string;
          insufficient_data_weight: number | string;
          lead_bonus_scale: number | string;
          lag_penalty_scale: number | string;
          family_weight_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      cross_asset_family_timing_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          timing_profile_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          dominant_timing_class: string;
          lead_pair_count: number; coincident_pair_count: number; lag_pair_count: number;
          timing_class_weight: number | string | null;
          timing_bonus: number | string | null;
          timing_penalty: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          timing_family_rank: number | null;
          top_leading_symbols: string[];
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          timing_profile_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          dominant_timing_class: string;
          lead_pair_count: number; coincident_pair_count: number; lag_pair_count: number;
          timing_class_weight: number | string | null;
          timing_bonus: number | string | null;
          timing_penalty: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          timing_family_rank: number | null;
          top_leading_symbols: string[];
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_symbol_timing_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          timing_profile_id: string | null;
          symbol: string; dependency_family: string;
          dependency_type: string | null;
          lag_bucket: string;
          best_lag_hours: number | null;
          raw_symbol_score: number | string | null;
          weighted_symbol_score: number | string | null;
          regime_adjusted_symbol_score: number | string | null;
          timing_class_weight: number | string | null;
          timing_adjusted_symbol_score: number | string | null;
          symbol_rank: number | null;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          timing_profile_id: string | null;
          symbol: string; dependency_family: string;
          dependency_type: string | null;
          lag_bucket: string;
          best_lag_hours: number | null;
          raw_symbol_score: number | string | null;
          weighted_symbol_score: number | string | null;
          regime_adjusted_symbol_score: number | string | null;
          timing_class_weight: number | string | null;
          timing_adjusted_symbol_score: number | string | null;
          symbol_rank: number | null;
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_family_timing_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          dominant_timing_class: string;
          lead_pair_count: number; coincident_pair_count: number; lag_pair_count: number;
          timing_class_weight: number | string | null;
          timing_bonus: number | string | null;
          timing_penalty: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          timing_family_rank: number | null;
          top_leading_symbols: string[];
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_symbol_timing_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          symbol: string; dependency_family: string;
          dependency_type: string | null;
          lag_bucket: string;
          best_lag_hours: number | null;
          raw_symbol_score: number | string | null;
          weighted_symbol_score: number | string | null;
          regime_adjusted_symbol_score: number | string | null;
          timing_class_weight: number | string | null;
          timing_adjusted_symbol_score: number | string | null;
          symbol_rank: number | null;
          created_at: string;
        };
        Relationships: [];
      };
      run_cross_asset_timing_attribution_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_timing_integration_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          integration_mode: string;
          integration_weight: number | string;
          lead_weight_scale: number | string;
          coincident_weight_scale: number | string;
          lag_weight_scale: number | string;
          insufficient_data_weight_scale: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          integration_mode: string;
          integration_weight: number | string;
          lead_weight_scale: number | string;
          coincident_weight_scale: number | string;
          lag_weight_scale: number | string;
          insufficient_data_weight_scale: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>;
        }>;
        Update: Partial<{
          profile_name: string; is_active: boolean;
          integration_mode: string;
          integration_weight: number | string;
          lead_weight_scale: number | string;
          coincident_weight_scale: number | string;
          lag_weight_scale: number | string;
          insufficient_data_weight_scale: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      cross_asset_timing_composite_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          timing_integration_profile_id: string | null;
          base_signal_score: number | string | null;
          cross_asset_net_contribution: number | string | null;
          weighted_cross_asset_net_contribution: number | string | null;
          regime_adjusted_cross_asset_contribution: number | string | null;
          timing_adjusted_cross_asset_contribution: number | string | null;
          composite_pre_timing: number | string | null;
          timing_net_contribution: number | string | null;
          composite_post_timing: number | string | null;
          dominant_timing_class: string;
          integration_mode: string;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          timing_integration_profile_id: string | null;
          base_signal_score: number | string | null;
          cross_asset_net_contribution: number | string | null;
          weighted_cross_asset_net_contribution: number | string | null;
          regime_adjusted_cross_asset_contribution: number | string | null;
          timing_adjusted_cross_asset_contribution: number | string | null;
          composite_pre_timing: number | string | null;
          timing_net_contribution: number | string | null;
          composite_post_timing: number | string | null;
          dominant_timing_class: string;
          integration_mode: string;
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_family_timing_composite_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          dominant_timing_class: string;
          timing_adjusted_family_contribution: number | string | null;
          integration_weight_applied: number | string | null;
          timing_integration_contribution: number | string | null;
          family_rank: number | null;
          top_symbols: string[];
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          dominant_timing_class: string;
          timing_adjusted_family_contribution: number | string | null;
          integration_weight_applied: number | string | null;
          timing_integration_contribution: number | string | null;
          family_rank: number | null;
          top_symbols: string[];
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_timing_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          base_signal_score: number | string | null;
          cross_asset_net_contribution: number | string | null;
          weighted_cross_asset_net_contribution: number | string | null;
          regime_adjusted_cross_asset_contribution: number | string | null;
          timing_adjusted_cross_asset_contribution: number | string | null;
          composite_pre_timing: number | string | null;
          timing_net_contribution: number | string | null;
          composite_post_timing: number | string | null;
          dominant_timing_class: string;
          integration_mode: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_timing_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          dominant_timing_class: string;
          timing_adjusted_family_contribution: number | string | null;
          integration_weight_applied: number | string | null;
          timing_integration_contribution: number | string | null;
          family_rank: number | null;
          top_symbols: string[];
          created_at: string;
        };
        Relationships: [];
      };
      run_cross_asset_final_integration_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_timing_replay_validation_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null; replay_regime_key: string | null;
          source_dominant_timing_class: string | null;
          replay_dominant_timing_class: string | null;
          context_hash_match: boolean; regime_match: boolean;
          timing_class_match: boolean;
          timing_attribution_match: boolean; timing_composite_match: boolean;
          timing_dominant_family_match: boolean;
          timing_net_delta: Record<string, unknown>;
          timing_composite_delta: Record<string, unknown>;
          drift_reason_codes: string[];
          validation_state: string;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null; replay_regime_key: string | null;
          source_dominant_timing_class: string | null;
          replay_dominant_timing_class: string | null;
          context_hash_match: boolean; regime_match: boolean;
          timing_class_match: boolean;
          timing_attribution_match: boolean; timing_composite_match: boolean;
          timing_dominant_family_match: boolean;
          timing_net_delta: Record<string, unknown>;
          timing_composite_delta: Record<string, unknown>;
          drift_reason_codes: string[];
          validation_state: string;
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_family_timing_replay_stability_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_timing_replay_validation_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null; replay_regime_key: string | null;
          source_dominant_timing_class: string | null;
          replay_dominant_timing_class: string | null;
          context_hash_match: boolean; regime_match: boolean;
          timing_class_match: boolean;
          timing_attribution_match: boolean; timing_composite_match: boolean;
          timing_dominant_family_match: boolean;
          drift_reason_codes: string[];
          validation_state: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_timing_replay_stability_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_timing_replay_stability_aggregate: {
        Row: {
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
        };
        Relationships: [];
      };
      cross_asset_family_transition_state_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          regime_key: string | null;
          dominant_timing_class: string | null;
          signal_state: string;
          transition_state: string;
          family_contribution: number | string | null;
          timing_adjusted_contribution: number | string | null;
          timing_integration_contribution: number | string | null;
          family_rank: number | null;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          regime_key: string | null;
          dominant_timing_class: string | null;
          signal_state: string;
          transition_state: string;
          family_contribution: number | string | null;
          timing_adjusted_contribution: number | string | null;
          timing_integration_contribution: number | string | null;
          family_rank: number | null;
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_family_transition_event_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string | null; target_run_id: string;
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
          event_type: string;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string | null; target_run_id: string;
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
          event_type: string;
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_family_sequence_summary_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null;
          dependency_family: string;
          window_label: string;
          sequence_signature: string;
          sequence_length: number;
          dominant_sequence_class: string;
          sequence_confidence: number | string | null;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string | null;
          dependency_family: string;
          window_label: string;
          sequence_signature: string;
          sequence_length: number;
          dominant_sequence_class: string;
          sequence_confidence: number | string | null;
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_family_transition_state_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          regime_key: string | null;
          dominant_timing_class: string | null;
          signal_state: string;
          transition_state: string;
          family_contribution: number | string | null;
          timing_adjusted_contribution: number | string | null;
          timing_integration_contribution: number | string | null;
          family_rank: number | null;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_transition_event_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string | null; target_run_id: string;
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
          event_type: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_sequence_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string | null;
          dependency_family: string;
          window_label: string;
          sequence_signature: string;
          sequence_length: number;
          dominant_sequence_class: string;
          sequence_confidence: number | string | null;
          created_at: string;
        };
        Relationships: [];
      };
      run_cross_asset_transition_diagnostics_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
          dominant_dependency_family: string | null;
          prior_dominant_dependency_family: string | null;
          dominant_timing_class: string | null;
          dominant_transition_state: string | null;
          dominant_sequence_class: string | null;
          rotation_event_count: number;
          degradation_event_count: number;
          recovery_event_count: number;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_transition_attribution_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          reinforcing_weight: number | string;
          stable_weight: number | string;
          recovering_weight: number | string;
          rotating_in_weight: number | string;
          rotating_out_weight: number | string;
          deteriorating_weight: number | string;
          insufficient_history_weight: number | string;
          recovery_bonus_scale: number | string;
          degradation_penalty_scale: number | string;
          rotation_bonus_scale: number | string;
          sequence_class_overrides: Record<string, unknown>;
          family_weight_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          reinforcing_weight: number | string;
          stable_weight: number | string;
          recovering_weight: number | string;
          rotating_in_weight: number | string;
          rotating_out_weight: number | string;
          deteriorating_weight: number | string;
          insufficient_history_weight: number | string;
          recovery_bonus_scale: number | string;
          degradation_penalty_scale: number | string;
          rotation_bonus_scale: number | string;
          sequence_class_overrides: Record<string, unknown>;
          family_weight_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>;
        }>;
        Update: Partial<{
          profile_name: string; is_active: boolean;
          reinforcing_weight: number | string;
          stable_weight: number | string;
          recovering_weight: number | string;
          rotating_in_weight: number | string;
          rotating_out_weight: number | string;
          deteriorating_weight: number | string;
          insufficient_history_weight: number | string;
          recovery_bonus_scale: number | string;
          degradation_penalty_scale: number | string;
          rotation_bonus_scale: number | string;
          sequence_class_overrides: Record<string, unknown>;
          family_weight_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      cross_asset_family_transition_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          transition_profile_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          transition_state: string;
          dominant_sequence_class: string;
          transition_state_weight: number | string | null;
          sequence_class_weight: number | string | null;
          transition_bonus: number | string | null;
          transition_penalty: number | string | null;
          transition_adjusted_family_contribution: number | string | null;
          transition_family_rank: number | null;
          top_symbols: string[];
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          transition_profile_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          transition_state: string;
          dominant_sequence_class: string;
          transition_state_weight: number | string | null;
          sequence_class_weight: number | string | null;
          transition_bonus: number | string | null;
          transition_penalty: number | string | null;
          transition_adjusted_family_contribution: number | string | null;
          transition_family_rank: number | null;
          top_symbols: string[];
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_symbol_transition_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          transition_profile_id: string | null;
          symbol: string; dependency_family: string;
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
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          transition_profile_id: string | null;
          symbol: string; dependency_family: string;
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
          metadata: Record<string, unknown>;
        }>;
        Update: Record<string, never>;
        Relationships: [];
      };
      cross_asset_family_transition_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          transition_state: string;
          dominant_sequence_class: string;
          transition_state_weight: number | string | null;
          sequence_class_weight: number | string | null;
          transition_bonus: number | string | null;
          transition_penalty: number | string | null;
          transition_adjusted_family_contribution: number | string | null;
          transition_family_rank: number | null;
          top_symbols: string[];
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_symbol_transition_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          symbol: string; dependency_family: string;
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
        };
        Relationships: [];
      };
      run_cross_asset_transition_attribution_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_transition_integration_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          integration_mode: string; integration_weight: number | string;
          reinforcing_weight_scale: number | string;
          stable_weight_scale: number | string;
          recovering_weight_scale: number | string;
          rotating_in_weight_scale: number | string;
          rotating_out_weight_scale: number | string;
          deteriorating_weight_scale: number | string;
          insufficient_history_weight_scale: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          integration_mode: string; integration_weight: number | string;
          reinforcing_weight_scale: number | string;
          stable_weight_scale: number | string;
          recovering_weight_scale: number | string;
          rotating_in_weight_scale: number | string;
          rotating_out_weight_scale: number | string;
          deteriorating_weight_scale: number | string;
          insufficient_history_weight_scale: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>; created_at: string;
        }>;
        Update: Partial<{
          profile_name: string; is_active: boolean;
          integration_mode: string; integration_weight: number | string;
          reinforcing_weight_scale: number | string;
          stable_weight_scale: number | string;
          recovering_weight_scale: number | string;
          rotating_in_weight_scale: number | string;
          rotating_out_weight_scale: number | string;
          deteriorating_weight_scale: number | string;
          insufficient_history_weight_scale: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      cross_asset_transition_composite_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string; run_id: string;
          context_snapshot_id: string | null;
          transition_integration_profile_id: string | null;
          base_signal_score: number | string | null;
          cross_asset_net_contribution: number | string | null;
          weighted_cross_asset_net_contribution: number | string | null;
          regime_adjusted_cross_asset_contribution: number | string | null;
          timing_adjusted_cross_asset_contribution: number | string | null;
          transition_adjusted_cross_asset_contribution: number | string | null;
          composite_pre_transition: number | string | null;
          transition_net_contribution: number | string | null;
          composite_post_transition: number | string | null;
          dominant_transition_state: string; integration_mode: string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_transition_composite_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string; run_id: string;
          context_snapshot_id: string | null;
          dependency_family: string;
          transition_state: string; dominant_sequence_class: string;
          transition_adjusted_family_contribution: number | string | null;
          integration_weight_applied: number | string | null;
          transition_integration_contribution: number | string | null;
          family_rank: number | null;
          top_symbols: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_transition_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string; run_id: string;
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
          dominant_transition_state: string; integration_mode: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_transition_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string; run_id: string;
          context_snapshot_id: string | null;
          dependency_family: string;
          transition_state: string; dominant_sequence_class: string;
          transition_adjusted_family_contribution: number | string | null;
          integration_weight_applied: number | string | null;
          transition_integration_contribution: number | string | null;
          family_rank: number | null;
          top_symbols: string[];
          created_at: string;
        };
        Relationships: [];
      };
      run_cross_asset_sequencing_integration_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_transition_replay_validation_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null; replay_regime_key: string | null;
          source_dominant_timing_class: string | null;
          replay_dominant_timing_class: string | null;
          source_dominant_transition_state: string | null;
          replay_dominant_transition_state: string | null;
          source_dominant_sequence_class: string | null;
          replay_dominant_sequence_class: string | null;
          context_hash_match: boolean; regime_match: boolean;
          timing_class_match: boolean;
          transition_state_match: boolean; sequence_class_match: boolean;
          transition_attribution_match: boolean;
          transition_composite_match: boolean;
          transition_dominant_family_match: boolean;
          transition_delta: Record<string, unknown>;
          transition_composite_delta: Record<string, unknown>;
          drift_reason_codes: string[]; validation_state: string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_transition_replay_stability_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          transition_state_match: boolean; sequence_class_match: boolean;
          transition_family_rank_match: boolean;
          transition_composite_family_rank_match: boolean;
          drift_reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_transition_replay_validation_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null; replay_regime_key: string | null;
          source_dominant_timing_class: string | null;
          replay_dominant_timing_class: string | null;
          source_dominant_transition_state: string | null;
          replay_dominant_transition_state: string | null;
          source_dominant_sequence_class: string | null;
          replay_dominant_sequence_class: string | null;
          context_hash_match: boolean; regime_match: boolean;
          timing_class_match: boolean;
          transition_state_match: boolean; sequence_class_match: boolean;
          transition_attribution_match: boolean;
          transition_composite_match: boolean;
          transition_dominant_family_match: boolean;
          drift_reason_codes: string[]; validation_state: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_transition_replay_stability_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          transition_state_match: boolean; sequence_class_match: boolean;
          transition_family_rank_match: boolean;
          transition_composite_family_rank_match: boolean;
          drift_reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_transition_replay_stability_aggregate: {
        Row: {
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
        };
        Relationships: [];
      };
      cross_asset_transition_archetype_registry: {
        Row: {
          id: string; archetype_key: string; archetype_label: string;
          archetype_family: string; description: string;
          classification_rules: Record<string, unknown>;
          is_active: boolean;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_archetype_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          regime_key: string | null;
          archetype_key: string;
          transition_state: string; dominant_sequence_class: string;
          dominant_timing_class: string | null;
          family_rank: number | null;
          family_contribution: number | string | null;
          archetype_confidence: number | string | null;
          classification_reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_run_archetype_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          regime_key: string | null;
          dominant_archetype_key: string;
          dominant_dependency_family: string | null;
          dominant_transition_state: string | null;
          dominant_sequence_class: string | null;
          archetype_confidence: number | string | null;
          rotation_event_count: number; recovery_event_count: number;
          degradation_event_count: number; mixed_event_count: number;
          classification_reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_archetype_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          regime_key: string | null;
          archetype_key: string;
          transition_state: string; dominant_sequence_class: string;
          dominant_timing_class: string | null;
          family_rank: number | null;
          family_contribution: number | string | null;
          archetype_confidence: number | string | null;
          classification_reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_run_archetype_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          regime_key: string | null;
          dominant_archetype_key: string;
          dominant_dependency_family: string | null;
          dominant_transition_state: string | null;
          dominant_sequence_class: string | null;
          archetype_confidence: number | string | null;
          rotation_event_count: number; recovery_event_count: number;
          degradation_event_count: number; mixed_event_count: number;
          classification_reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_regime_archetype_summary: {
        Row: {
          workspace_id: string;
          regime_key: string | null;
          archetype_key: string;
          run_count: number;
          avg_confidence: number | string | null;
          latest_seen_at: string | null;
        };
        Relationships: [];
      };
      run_cross_asset_pattern_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
          regime_key: string | null;
          dominant_archetype_key: string;
          dominant_dependency_family: string | null;
          dominant_transition_state: string | null;
          dominant_sequence_class: string | null;
          archetype_confidence: number | string | null;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_archetype_attribution_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          rotation_handoff_weight: number | string;
          reinforcing_continuation_weight: number | string;
          recovering_reentry_weight: number | string;
          deteriorating_breakdown_weight: number | string;
          mixed_transition_noise_weight: number | string;
          insufficient_history_weight: number | string;
          recovery_bonus_scale: number | string;
          breakdown_penalty_scale: number | string;
          rotation_bonus_scale: number | string;
          archetype_family_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          rotation_handoff_weight: number | string;
          reinforcing_continuation_weight: number | string;
          recovering_reentry_weight: number | string;
          deteriorating_breakdown_weight: number | string;
          mixed_transition_noise_weight: number | string;
          insufficient_history_weight: number | string;
          recovery_bonus_scale: number | string;
          breakdown_penalty_scale: number | string;
          rotation_bonus_scale: number | string;
          archetype_family_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>; created_at: string;
        }>;
        Update: Partial<{
          profile_name: string; is_active: boolean;
          rotation_handoff_weight: number | string;
          reinforcing_continuation_weight: number | string;
          recovering_reentry_weight: number | string;
          deteriorating_breakdown_weight: number | string;
          mixed_transition_noise_weight: number | string;
          insufficient_history_weight: number | string;
          recovery_bonus_scale: number | string;
          breakdown_penalty_scale: number | string;
          rotation_bonus_scale: number | string;
          archetype_family_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      cross_asset_family_archetype_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          archetype_profile_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          transition_adjusted_family_contribution: number | string | null;
          archetype_key: string;
          transition_state: string; dominant_sequence_class: string;
          archetype_weight: number | string | null;
          archetype_bonus: number | string | null;
          archetype_penalty: number | string | null;
          archetype_adjusted_family_contribution: number | string | null;
          archetype_family_rank: number | null;
          top_symbols: string[];
          classification_reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_symbol_archetype_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          archetype_profile_id: string | null;
          symbol: string; dependency_family: string; dependency_type: string | null;
          archetype_key: string;
          transition_state: string; dominant_sequence_class: string;
          raw_symbol_score: number | string | null;
          weighted_symbol_score: number | string | null;
          regime_adjusted_symbol_score: number | string | null;
          timing_adjusted_symbol_score: number | string | null;
          transition_adjusted_symbol_score: number | string | null;
          archetype_weight: number | string | null;
          archetype_adjusted_symbol_score: number | string | null;
          symbol_rank: number | null;
          classification_reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_archetype_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          transition_adjusted_family_contribution: number | string | null;
          archetype_key: string;
          transition_state: string; dominant_sequence_class: string;
          archetype_weight: number | string | null;
          archetype_bonus: number | string | null;
          archetype_penalty: number | string | null;
          archetype_adjusted_family_contribution: number | string | null;
          archetype_family_rank: number | null;
          top_symbols: string[];
          classification_reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_symbol_archetype_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          symbol: string; dependency_family: string; dependency_type: string | null;
          archetype_key: string;
          transition_state: string; dominant_sequence_class: string;
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
        };
        Relationships: [];
      };
      run_cross_asset_archetype_attribution_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_archetype_integration_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          integration_mode: string; integration_weight: number | string;
          reinforcing_continuation_scale: number | string;
          recovering_reentry_scale: number | string;
          rotation_handoff_scale: number | string;
          mixed_transition_noise_scale: number | string;
          deteriorating_breakdown_scale: number | string;
          insufficient_history_scale: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          integration_mode: string; integration_weight: number | string;
          reinforcing_continuation_scale: number | string;
          recovering_reentry_scale: number | string;
          rotation_handoff_scale: number | string;
          mixed_transition_noise_scale: number | string;
          deteriorating_breakdown_scale: number | string;
          insufficient_history_scale: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>; created_at: string;
        }>;
        Update: Partial<{
          profile_name: string; is_active: boolean;
          integration_mode: string; integration_weight: number | string;
          reinforcing_continuation_scale: number | string;
          recovering_reentry_scale: number | string;
          rotation_handoff_scale: number | string;
          mixed_transition_noise_scale: number | string;
          deteriorating_breakdown_scale: number | string;
          insufficient_history_scale: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      cross_asset_archetype_composite_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string; run_id: string;
          context_snapshot_id: string | null;
          archetype_integration_profile_id: string | null;
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
          dominant_archetype_key: string; integration_mode: string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_archetype_composite_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string; run_id: string;
          context_snapshot_id: string | null;
          dependency_family: string;
          archetype_key: string;
          transition_state: string; dominant_sequence_class: string;
          archetype_adjusted_family_contribution: number | string | null;
          integration_weight_applied: number | string | null;
          archetype_integration_contribution: number | string | null;
          family_rank: number | null;
          top_symbols: string[];
          classification_reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_archetype_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string; run_id: string;
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
          dominant_archetype_key: string; integration_mode: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_archetype_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string; run_id: string;
          context_snapshot_id: string | null;
          dependency_family: string;
          archetype_key: string;
          transition_state: string; dominant_sequence_class: string;
          archetype_adjusted_family_contribution: number | string | null;
          integration_weight_applied: number | string | null;
          archetype_integration_contribution: number | string | null;
          family_rank: number | null;
          top_symbols: string[];
          classification_reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      run_cross_asset_archetype_integration_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_archetype_replay_validation_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null; replay_regime_key: string | null;
          source_dominant_timing_class: string | null;
          replay_dominant_timing_class: string | null;
          source_dominant_transition_state: string | null;
          replay_dominant_transition_state: string | null;
          source_dominant_sequence_class: string | null;
          replay_dominant_sequence_class: string | null;
          source_dominant_archetype_key: string | null;
          replay_dominant_archetype_key: string | null;
          context_hash_match: boolean; regime_match: boolean;
          timing_class_match: boolean;
          transition_state_match: boolean; sequence_class_match: boolean;
          archetype_match: boolean;
          archetype_attribution_match: boolean;
          archetype_composite_match: boolean;
          archetype_dominant_family_match: boolean;
          archetype_delta: Record<string, unknown>;
          archetype_composite_delta: Record<string, unknown>;
          drift_reason_codes: string[]; validation_state: string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_archetype_replay_stability_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          transition_state_match: boolean; sequence_class_match: boolean;
          archetype_match: boolean;
          archetype_family_rank_match: boolean;
          archetype_composite_family_rank_match: boolean;
          drift_reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_archetype_replay_validation_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null; replay_regime_key: string | null;
          source_dominant_timing_class: string | null;
          replay_dominant_timing_class: string | null;
          source_dominant_transition_state: string | null;
          replay_dominant_transition_state: string | null;
          source_dominant_sequence_class: string | null;
          replay_dominant_sequence_class: string | null;
          source_dominant_archetype_key: string | null;
          replay_dominant_archetype_key: string | null;
          context_hash_match: boolean; regime_match: boolean;
          timing_class_match: boolean;
          transition_state_match: boolean; sequence_class_match: boolean;
          archetype_match: boolean;
          archetype_attribution_match: boolean;
          archetype_composite_match: boolean;
          archetype_dominant_family_match: boolean;
          drift_reason_codes: string[]; validation_state: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_archetype_replay_stability_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          transition_state_match: boolean; sequence_class_match: boolean;
          archetype_match: boolean;
          archetype_family_rank_match: boolean;
          archetype_composite_family_rank_match: boolean;
          drift_reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_archetype_replay_stability_aggregate: {
        Row: {
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
        };
        Relationships: [];
      };
      cross_asset_archetype_cluster_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
          cluster_state: string;
          drift_score: number | string | null;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_archetype_regime_rotation_snapshots: {
        Row: {
          id: string; workspace_id: string;
          regime_key: string;
          window_label: string;
          prior_dominant_archetype_key: string | null;
          current_dominant_archetype_key: string | null;
          rotation_count: number;
          reinforcement_run_count: number;
          recovery_run_count: number;
          degradation_run_count: number;
          mixed_run_count: number;
          rotation_state: string;
          regime_drift_score: number | string | null;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_pattern_drift_event_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string | null;
          source_run_id: string | null; target_run_id: string | null;
          regime_key: string | null;
          prior_cluster_state: string | null;
          current_cluster_state: string;
          prior_dominant_archetype_key: string | null;
          current_dominant_archetype_key: string;
          drift_event_type: string;
          drift_score: number | string | null;
          reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_archetype_cluster_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
          cluster_state: string;
          drift_score: number | string | null;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_archetype_regime_rotation_summary: {
        Row: {
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
          rotation_state: string;
          regime_drift_score: number | string | null;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_pattern_drift_event_summary: {
        Row: {
          workspace_id: string; watchlist_id: string | null;
          source_run_id: string | null; target_run_id: string | null;
          regime_key: string | null;
          prior_cluster_state: string | null;
          current_cluster_state: string;
          prior_dominant_archetype_key: string | null;
          current_dominant_archetype_key: string;
          drift_event_type: string;
          drift_score: number | string | null;
          reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      run_cross_asset_pattern_cluster_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
          regime_key: string | null;
          dominant_archetype_key: string | null;
          cluster_state: string | null;
          drift_score: number | string | null;
          pattern_entropy: number | string | null;
          current_rotation_state: string | null;
          latest_drift_event_type: string | null;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_cluster_attribution_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          stable_weight: number | string;
          rotating_weight: number | string;
          recovering_weight: number | string;
          deteriorating_weight: number | string;
          mixed_weight: number | string;
          insufficient_history_weight: number | string;
          drift_penalty_scale: number | string;
          rotation_bonus_scale: number | string;
          recovery_bonus_scale: number | string;
          entropy_penalty_scale: number | string;
          cluster_family_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          stable_weight: number | string;
          rotating_weight: number | string;
          recovering_weight: number | string;
          deteriorating_weight: number | string;
          mixed_weight: number | string;
          insufficient_history_weight: number | string;
          drift_penalty_scale: number | string;
          rotation_bonus_scale: number | string;
          recovery_bonus_scale: number | string;
          entropy_penalty_scale: number | string;
          cluster_family_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>; created_at: string;
        }>;
        Update: Partial<{
          profile_name: string; is_active: boolean;
          stable_weight: number | string;
          rotating_weight: number | string;
          recovering_weight: number | string;
          deteriorating_weight: number | string;
          mixed_weight: number | string;
          insufficient_history_weight: number | string;
          drift_penalty_scale: number | string;
          rotation_bonus_scale: number | string;
          recovery_bonus_scale: number | string;
          entropy_penalty_scale: number | string;
          cluster_family_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      cross_asset_family_cluster_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          cluster_profile_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          transition_adjusted_family_contribution: number | string | null;
          archetype_adjusted_family_contribution: number | string | null;
          cluster_state: string;
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
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_symbol_cluster_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          cluster_profile_id: string | null;
          symbol: string; dependency_family: string; dependency_type: string | null;
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
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_cluster_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          transition_adjusted_family_contribution: number | string | null;
          archetype_adjusted_family_contribution: number | string | null;
          cluster_state: string;
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
        };
        Relationships: [];
      };
      cross_asset_symbol_cluster_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          symbol: string; dependency_family: string; dependency_type: string | null;
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
        };
        Relationships: [];
      };
      run_cross_asset_cluster_attribution_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_cluster_integration_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          integration_mode: string; integration_weight: number | string;
          stable_scale: number | string;
          recovering_scale: number | string;
          rotating_scale: number | string;
          mixed_scale: number | string;
          deteriorating_scale: number | string;
          insufficient_history_scale: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          integration_mode: string; integration_weight: number | string;
          stable_scale: number | string;
          recovering_scale: number | string;
          rotating_scale: number | string;
          mixed_scale: number | string;
          deteriorating_scale: number | string;
          insufficient_history_scale: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>; created_at: string;
        }>;
        Update: Partial<{
          profile_name: string; is_active: boolean;
          integration_mode: string; integration_weight: number | string;
          stable_scale: number | string;
          recovering_scale: number | string;
          rotating_scale: number | string;
          mixed_scale: number | string;
          deteriorating_scale: number | string;
          insufficient_history_scale: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      cross_asset_cluster_composite_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string; run_id: string;
          context_snapshot_id: string | null;
          cluster_integration_profile_id: string | null;
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
          cluster_state: string;
          dominant_archetype_key: string;
          integration_mode: string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_cluster_composite_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string; run_id: string;
          context_snapshot_id: string | null;
          dependency_family: string;
          cluster_state: string;
          dominant_archetype_key: string;
          cluster_adjusted_family_contribution: number | string | null;
          integration_weight_applied: number | string | null;
          cluster_integration_contribution: number | string | null;
          family_rank: number | null;
          top_symbols: string[];
          reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_cluster_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string; run_id: string;
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
          cluster_state: string;
          dominant_archetype_key: string;
          integration_mode: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_cluster_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string; run_id: string;
          context_snapshot_id: string | null;
          dependency_family: string;
          cluster_state: string;
          dominant_archetype_key: string;
          cluster_adjusted_family_contribution: number | string | null;
          integration_weight_applied: number | string | null;
          cluster_integration_contribution: number | string | null;
          family_rank: number | null;
          top_symbols: string[];
          reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      run_cross_asset_cluster_integration_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_cluster_replay_validation_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null; replay_regime_key: string | null;
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
          context_hash_match: boolean; regime_match: boolean;
          timing_class_match: boolean;
          transition_state_match: boolean; sequence_class_match: boolean;
          archetype_match: boolean;
          cluster_state_match: boolean; drift_score_match: boolean;
          cluster_attribution_match: boolean;
          cluster_composite_match: boolean;
          cluster_dominant_family_match: boolean;
          cluster_delta: Record<string, unknown>;
          cluster_composite_delta: Record<string, unknown>;
          drift_reason_codes: string[]; validation_state: string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_cluster_replay_stability_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_cluster_replay_validation_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null; replay_regime_key: string | null;
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
          context_hash_match: boolean; regime_match: boolean;
          timing_class_match: boolean;
          transition_state_match: boolean; sequence_class_match: boolean;
          archetype_match: boolean;
          cluster_state_match: boolean; drift_score_match: boolean;
          cluster_attribution_match: boolean;
          cluster_composite_match: boolean;
          cluster_dominant_family_match: boolean;
          drift_reason_codes: string[]; validation_state: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_cluster_replay_stability_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_cluster_replay_stability_aggregate: {
        Row: {
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
        };
        Relationships: [];
      };
      cross_asset_state_persistence_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          window_label: string;
          regime_key: string | null;
          dominant_timing_class: string | null;
          dominant_transition_state: string | null;
          dominant_sequence_class: string | null;
          dominant_archetype_key: string | null;
          cluster_state: string | null;
          current_state_signature: string;
          state_age_runs: number; same_state_count: number;
          state_persistence_ratio: number | string | null;
          regime_persistence_ratio: number | string | null;
          cluster_persistence_ratio: number | string | null;
          archetype_persistence_ratio: number | string | null;
          persistence_state: string;
          memory_score: number | string | null;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_regime_memory_snapshots: {
        Row: {
          id: string; workspace_id: string;
          regime_key: string; window_label: string;
          run_count: number;
          same_regime_streak_count: number;
          regime_switch_count: number;
          avg_regime_duration_runs: number | string | null;
          max_regime_duration_runs: number | null;
          regime_memory_score: number | string | null;
          dominant_cluster_state: string | null;
          dominant_archetype_key: string | null;
          persistence_state: string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_persistence_transition_event_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string | null;
          source_run_id: string | null; target_run_id: string;
          regime_key: string | null;
          prior_state_signature: string | null;
          current_state_signature: string;
          prior_persistence_state: string | null;
          current_persistence_state: string;
          prior_memory_score: number | string | null;
          current_memory_score: number | string | null;
          memory_score_delta: number | string | null;
          event_type: string;
          reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_state_persistence_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          window_label: string;
          regime_key: string | null;
          dominant_timing_class: string | null;
          dominant_transition_state: string | null;
          dominant_sequence_class: string | null;
          dominant_archetype_key: string | null;
          cluster_state: string | null;
          current_state_signature: string;
          state_age_runs: number; same_state_count: number;
          state_persistence_ratio: number | string | null;
          regime_persistence_ratio: number | string | null;
          cluster_persistence_ratio: number | string | null;
          archetype_persistence_ratio: number | string | null;
          persistence_state: string;
          memory_score: number | string | null;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_regime_memory_summary: {
        Row: {
          workspace_id: string;
          regime_key: string; window_label: string;
          run_count: number;
          same_regime_streak_count: number;
          regime_switch_count: number;
          avg_regime_duration_runs: number | string | null;
          max_regime_duration_runs: number | null;
          regime_memory_score: number | string | null;
          dominant_cluster_state: string | null;
          dominant_archetype_key: string | null;
          persistence_state: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_persistence_transition_event_summary: {
        Row: {
          workspace_id: string; watchlist_id: string | null;
          source_run_id: string | null; target_run_id: string;
          regime_key: string | null;
          prior_state_signature: string | null;
          current_state_signature: string;
          prior_persistence_state: string | null;
          current_persistence_state: string;
          prior_memory_score: number | string | null;
          current_memory_score: number | string | null;
          memory_score_delta: number | string | null;
          event_type: string;
          reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      run_cross_asset_persistence_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
          regime_key: string | null;
          cluster_state: string | null;
          dominant_archetype_key: string | null;
          persistence_state: string | null;
          memory_score: number | string | null;
          state_age_runs: number | null;
          state_persistence_ratio: number | string | null;
          latest_persistence_event_type: string | null;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_persistence_attribution_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          persistent_weight: number | string;
          recovering_weight: number | string;
          rotating_weight: number | string;
          fragile_weight: number | string;
          breaking_down_weight: number | string;
          mixed_weight: number | string;
          insufficient_history_weight: number | string;
          memory_score_boost_scale: number | string;
          memory_break_penalty_scale: number | string;
          stabilization_bonus_scale: number | string;
          state_age_bonus_scale: number | string;
          persistence_family_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          persistent_weight: number | string;
          recovering_weight: number | string;
          rotating_weight: number | string;
          fragile_weight: number | string;
          breaking_down_weight: number | string;
          mixed_weight: number | string;
          insufficient_history_weight: number | string;
          memory_score_boost_scale: number | string;
          memory_break_penalty_scale: number | string;
          stabilization_bonus_scale: number | string;
          state_age_bonus_scale: number | string;
          persistence_family_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>; created_at: string;
        }>;
        Update: Partial<{
          profile_name: string; is_active: boolean;
          persistent_weight: number | string;
          recovering_weight: number | string;
          rotating_weight: number | string;
          fragile_weight: number | string;
          breaking_down_weight: number | string;
          mixed_weight: number | string;
          insufficient_history_weight: number | string;
          memory_score_boost_scale: number | string;
          memory_break_penalty_scale: number | string;
          stabilization_bonus_scale: number | string;
          state_age_bonus_scale: number | string;
          persistence_family_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      cross_asset_family_persistence_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          persistence_profile_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          transition_adjusted_family_contribution: number | string | null;
          archetype_adjusted_family_contribution: number | string | null;
          cluster_adjusted_family_contribution: number | string | null;
          persistence_state: string;
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
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_symbol_persistence_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          persistence_profile_id: string | null;
          symbol: string; dependency_family: string; dependency_type: string | null;
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
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_persistence_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          transition_adjusted_family_contribution: number | string | null;
          archetype_adjusted_family_contribution: number | string | null;
          cluster_adjusted_family_contribution: number | string | null;
          persistence_state: string;
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
        };
        Relationships: [];
      };
      cross_asset_symbol_persistence_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          symbol: string; dependency_family: string; dependency_type: string | null;
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
        };
        Relationships: [];
      };
      run_cross_asset_persistence_attribution_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_persistence_integration_profiles: {
        Row: {
          id: string; workspace_id: string;
          profile_name: string;
          is_active: boolean;
          integration_mode: string;
          integration_weight: number | string;
          persistent_scale: number | string;
          recovering_scale: number | string;
          rotating_scale: number | string;
          fragile_scale: number | string;
          breaking_down_scale: number | string;
          mixed_scale: number | string;
          insufficient_history_scale: number | string;
          memory_break_extra_suppression: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_persistence_composite_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string; run_id: string;
          context_snapshot_id: string | null;
          persistence_integration_profile_id: string | null;
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
          persistence_state: string;
          memory_score: number | string | null;
          state_age_runs: number | null;
          latest_persistence_event_type: string | null;
          integration_mode: string;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_persistence_composite_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string; run_id: string;
          context_snapshot_id: string | null;
          dependency_family: string;
          persistence_state: string;
          memory_score: number | string | null;
          state_age_runs: number | null;
          latest_persistence_event_type: string | null;
          persistence_adjusted_family_contribution: number | string | null;
          integration_weight_applied: number | string | null;
          persistence_integration_contribution: number | string | null;
          family_rank: number | null;
          top_symbols: string[];
          reason_codes: string[];
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_persistence_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string; run_id: string;
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
          persistence_state: string;
          memory_score: number | string | null;
          state_age_runs: number | null;
          latest_persistence_event_type: string | null;
          integration_mode: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_persistence_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string; run_id: string;
          context_snapshot_id: string | null;
          dependency_family: string;
          persistence_state: string;
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
        };
        Relationships: [];
      };
      run_cross_asset_persistence_integration_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_persistence_replay_validation_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null; replay_regime_key: string | null;
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
          context_hash_match: boolean; regime_match: boolean;
          timing_class_match: boolean;
          transition_state_match: boolean; sequence_class_match: boolean;
          archetype_match: boolean;
          cluster_state_match: boolean;
          persistence_state_match: boolean; memory_score_match: boolean;
          state_age_match: boolean; persistence_event_match: boolean;
          persistence_attribution_match: boolean;
          persistence_composite_match: boolean;
          persistence_dominant_family_match: boolean;
          persistence_delta: Record<string, unknown>;
          persistence_composite_delta: Record<string, unknown>;
          drift_reason_codes: string[]; validation_state: string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_persistence_replay_stability_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          persistence_state_match: boolean; memory_score_match: boolean;
          state_age_match: boolean; persistence_event_match: boolean;
          persistence_family_rank_match: boolean;
          persistence_composite_family_rank_match: boolean;
          drift_reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_persistence_replay_validation_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
          source_context_snapshot_id: string | null;
          replay_context_snapshot_id: string | null;
          source_regime_key: string | null; replay_regime_key: string | null;
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
          context_hash_match: boolean; regime_match: boolean;
          timing_class_match: boolean;
          transition_state_match: boolean; sequence_class_match: boolean;
          archetype_match: boolean;
          cluster_state_match: boolean;
          persistence_state_match: boolean; memory_score_match: boolean;
          state_age_match: boolean; persistence_event_match: boolean;
          persistence_attribution_match: boolean;
          persistence_composite_match: boolean;
          persistence_dominant_family_match: boolean;
          drift_reason_codes: string[]; validation_state: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_persistence_replay_stability_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          persistence_state_match: boolean; memory_score_match: boolean;
          state_age_match: boolean; persistence_event_match: boolean;
          persistence_family_rank_match: boolean;
          persistence_composite_family_rank_match: boolean;
          drift_reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_persistence_replay_stability_aggregate: {
        Row: {
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
        };
        Relationships: [];
      };
      cross_asset_signal_decay_policy_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          regime_half_life_runs: number; timing_half_life_runs: number;
          transition_half_life_runs: number; archetype_half_life_runs: number;
          cluster_half_life_runs: number; persistence_half_life_runs: number;
          fresh_memory_threshold: number | string;
          decaying_memory_threshold: number | string;
          stale_memory_threshold: number | string;
          contradiction_penalty_threshold: number | string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Insert: Partial<{
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          regime_half_life_runs: number; timing_half_life_runs: number;
          transition_half_life_runs: number; archetype_half_life_runs: number;
          cluster_half_life_runs: number; persistence_half_life_runs: number;
          fresh_memory_threshold: number | string;
          decaying_memory_threshold: number | string;
          stale_memory_threshold: number | string;
          contradiction_penalty_threshold: number | string;
          metadata: Record<string, unknown>; created_at: string;
        }>;
        Update: Partial<{
          profile_name: string; is_active: boolean;
          regime_half_life_runs: number; timing_half_life_runs: number;
          transition_half_life_runs: number; archetype_half_life_runs: number;
          cluster_half_life_runs: number; persistence_half_life_runs: number;
          fresh_memory_threshold: number | string;
          decaying_memory_threshold: number | string;
          stale_memory_threshold: number | string;
          contradiction_penalty_threshold: number | string;
          metadata: Record<string, unknown>;
        }>;
        Relationships: [];
      };
      cross_asset_signal_decay_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          decay_policy_profile_id: string | null;
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
          freshness_state: string;
          stale_memory_flag: boolean;
          contradiction_flag: boolean;
          reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_signal_decay_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
          family_freshness_state: string;
          stale_family_memory_flag: boolean;
          contradicted_family_flag: boolean;
          reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_stale_memory_event_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string | null;
          source_run_id: string | null; target_run_id: string;
          regime_key: string | null;
          prior_freshness_state: string | null;
          current_freshness_state: string;
          prior_state_signature: string | null;
          current_state_signature: string;
          prior_memory_score: number | string | null;
          current_memory_score: number | string | null;
          prior_aggregate_decay_score: number | string | null;
          current_aggregate_decay_score: number | string | null;
          event_type: string;
          reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_signal_decay_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
          freshness_state: string;
          stale_memory_flag: boolean;
          contradiction_flag: boolean;
          reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_signal_decay_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
          family_freshness_state: string;
          stale_family_memory_flag: boolean;
          contradicted_family_flag: boolean;
          reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_stale_memory_event_summary: {
        Row: {
          workspace_id: string; watchlist_id: string | null;
          source_run_id: string | null; target_run_id: string;
          regime_key: string | null;
          prior_freshness_state: string | null;
          current_freshness_state: string;
          prior_state_signature: string | null;
          current_state_signature: string;
          prior_memory_score: number | string | null;
          current_memory_score: number | string | null;
          prior_aggregate_decay_score: number | string | null;
          current_aggregate_decay_score: number | string | null;
          event_type: string;
          reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      run_cross_asset_signal_decay_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
          regime_key: string | null;
          persistence_state: string | null;
          memory_score: number | string | null;
          freshness_state: string | null;
          aggregate_decay_score: number | string | null;
          stale_memory_flag: boolean;
          contradiction_flag: boolean;
          latest_stale_memory_event_type: string | null;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_decay_attribution_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          fresh_weight: number | string;
          decaying_weight: number | string;
          stale_weight: number | string;
          contradicted_weight: number | string;
          mixed_weight: number | string;
          insufficient_history_weight: number | string;
          freshness_bonus_scale: number | string;
          stale_penalty_scale: number | string;
          contradiction_penalty_scale: number | string;
          decay_score_penalty_scale: number | string;
          decay_family_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_decay_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          decay_profile_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          transition_adjusted_family_contribution: number | string | null;
          archetype_adjusted_family_contribution: number | string | null;
          cluster_adjusted_family_contribution: number | string | null;
          persistence_adjusted_family_contribution: number | string | null;
          freshness_state: string;
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
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_symbol_decay_attribution_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          decay_profile_id: string | null;
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
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_decay_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          raw_family_net_contribution: number | string | null;
          weighted_family_net_contribution: number | string | null;
          regime_adjusted_family_contribution: number | string | null;
          timing_adjusted_family_contribution: number | string | null;
          transition_adjusted_family_contribution: number | string | null;
          archetype_adjusted_family_contribution: number | string | null;
          cluster_adjusted_family_contribution: number | string | null;
          persistence_adjusted_family_contribution: number | string | null;
          freshness_state: string;
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
        };
        Relationships: [];
      };
      cross_asset_symbol_decay_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
        };
        Relationships: [];
      };
      run_cross_asset_decay_attribution_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_decay_integration_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          integration_mode: string;
          integration_weight: number | string;
          fresh_scale: number | string;
          decaying_scale: number | string;
          stale_scale: number | string;
          contradicted_scale: number | string;
          mixed_scale: number | string;
          insufficient_history_scale: number | string;
          stale_extra_suppression: number | string;
          contradiction_extra_suppression: number | string;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_decay_composite_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          decay_integration_profile_id: string | null;
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
          freshness_state: string;
          aggregate_decay_score: number | string | null;
          stale_memory_flag: boolean;
          contradiction_flag: boolean;
          integration_mode: string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_decay_composite_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_decay_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
          freshness_state: string;
          aggregate_decay_score: number | string | null;
          stale_memory_flag: boolean;
          contradiction_flag: boolean;
          integration_mode: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_decay_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
        };
        Relationships: [];
      };
      run_cross_asset_decay_integration_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_decay_replay_validation_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          decay_delta: Record<string, unknown>;
          decay_composite_delta: Record<string, unknown>;
          drift_reason_codes: string[];
          validation_state: string;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_decay_replay_stability_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_decay_replay_validation_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
          validation_state: string;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_decay_replay_stability_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          source_run_id: string; replay_run_id: string;
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
        };
        Relationships: [];
      };
      cross_asset_decay_replay_stability_aggregate: {
        Row: {
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
        };
        Relationships: [];
      };
      cross_asset_conflict_policy_profiles: {
        Row: {
          id: string; workspace_id: string; profile_name: string; is_active: boolean;
          timing_weight: number | string;
          transition_weight: number | string;
          archetype_weight: number | string;
          cluster_weight: number | string;
          persistence_weight: number | string;
          decay_weight: number | string;
          agreement_threshold: number | string;
          partial_agreement_threshold: number | string;
          conflict_threshold: number | string;
          unreliable_threshold: number | string;
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_layer_agreement_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          conflict_policy_profile_id: string | null;
          dominant_timing_class: string | null;
          dominant_transition_state: string | null;
          dominant_sequence_class: string | null;
          dominant_archetype_key: string | null;
          cluster_state: string | null;
          persistence_state: string | null;
          freshness_state: string | null;
          timing_direction: string | null;
          transition_direction: string | null;
          archetype_direction: string | null;
          cluster_direction: string | null;
          persistence_direction: string | null;
          decay_direction: string | null;
          supportive_weight: number | string | null;
          suppressive_weight: number | string | null;
          neutral_weight: number | string | null;
          missing_weight: number | string | null;
          agreement_score: number | string | null;
          conflict_score: number | string | null;
          layer_consensus_state: string;
          dominant_conflict_source: string | null;
          conflict_reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_layer_agreement_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          transition_state: string | null;
          dominant_sequence_class: string | null;
          archetype_key: string | null;
          cluster_state: string | null;
          persistence_state: string | null;
          freshness_state: string | null;
          family_contribution: number | string | null;
          transition_direction: string | null;
          archetype_direction: string | null;
          cluster_direction: string | null;
          persistence_direction: string | null;
          decay_direction: string | null;
          agreement_score: number | string | null;
          conflict_score: number | string | null;
          family_consensus_state: string;
          dominant_conflict_source: string | null;
          family_rank: number | null;
          conflict_reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_layer_conflict_event_snapshots: {
        Row: {
          id: string; workspace_id: string; watchlist_id: string | null;
          source_run_id: string | null; target_run_id: string;
          prior_consensus_state: string | null;
          current_consensus_state: string;
          prior_dominant_conflict_source: string | null;
          current_dominant_conflict_source: string | null;
          prior_agreement_score: number | string | null;
          current_agreement_score: number | string | null;
          prior_conflict_score: number | string | null;
          current_conflict_score: number | string | null;
          event_type: string;
          reason_codes: string[];
          metadata: Record<string, unknown>; created_at: string;
        };
        Relationships: [];
      };
      cross_asset_layer_agreement_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dominant_timing_class: string | null;
          dominant_transition_state: string | null;
          dominant_sequence_class: string | null;
          dominant_archetype_key: string | null;
          cluster_state: string | null;
          persistence_state: string | null;
          freshness_state: string | null;
          timing_direction: string | null;
          transition_direction: string | null;
          archetype_direction: string | null;
          cluster_direction: string | null;
          persistence_direction: string | null;
          decay_direction: string | null;
          supportive_weight: number | string | null;
          suppressive_weight: number | string | null;
          neutral_weight: number | string | null;
          missing_weight: number | string | null;
          agreement_score: number | string | null;
          conflict_score: number | string | null;
          layer_consensus_state: string;
          dominant_conflict_source: string | null;
          conflict_reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_family_layer_agreement_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          dependency_family: string;
          transition_state: string | null;
          dominant_sequence_class: string | null;
          archetype_key: string | null;
          cluster_state: string | null;
          persistence_state: string | null;
          freshness_state: string | null;
          family_contribution: number | string | null;
          transition_direction: string | null;
          archetype_direction: string | null;
          cluster_direction: string | null;
          persistence_direction: string | null;
          decay_direction: string | null;
          agreement_score: number | string | null;
          conflict_score: number | string | null;
          family_consensus_state: string;
          dominant_conflict_source: string | null;
          family_rank: number | null;
          conflict_reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_layer_conflict_event_summary: {
        Row: {
          workspace_id: string; watchlist_id: string | null;
          source_run_id: string | null; target_run_id: string;
          prior_consensus_state: string | null;
          current_consensus_state: string;
          prior_dominant_conflict_source: string | null;
          current_dominant_conflict_source: string | null;
          prior_agreement_score: number | string | null;
          current_agreement_score: number | string | null;
          prior_conflict_score: number | string | null;
          current_conflict_score: number | string | null;
          event_type: string;
          reason_codes: string[];
          created_at: string;
        };
        Relationships: [];
      };
      run_cross_asset_layer_conflict_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
          layer_consensus_state: string | null;
          agreement_score: number | string | null;
          conflict_score: number | string | null;
          dominant_conflict_source: string | null;
          freshness_state: string | null;
          persistence_state: string | null;
          cluster_state: string | null;
          latest_conflict_event_type: string | null;
          created_at: string;
        };
        Relationships: [];
      };
      cross_asset_conflict_attribution_profiles: {
        Row: {
          id: string;
          workspace_id: string;
          profile_name: string;
          is_active: boolean;
          aligned_supportive_weight: number | string;
          aligned_suppressive_weight: number | string;
          partial_agreement_weight: number | string;
          conflicted_weight: number | string;
          unreliable_weight: number | string;
          insufficient_context_weight: number | string;
          agreement_bonus_scale: number | string;
          conflict_penalty_scale: number | string;
          unreliable_penalty_scale: number | string;
          dominant_conflict_source_penalties: Record<string, unknown>;
          conflict_family_overrides: Record<string, unknown>;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<Database["public"]["Tables"]["cross_asset_conflict_attribution_profiles"]["Row"]> & {
          workspace_id: string; profile_name: string;
        };
        Update: Partial<Database["public"]["Tables"]["cross_asset_conflict_attribution_profiles"]["Row"]>;
        Relationships: [];
      };
      cross_asset_family_conflict_attribution_snapshots: {
        Row: {
          id: string;
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          conflict_profile_id: string | null;
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
          family_consensus_state: string;
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
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<Database["public"]["Tables"]["cross_asset_family_conflict_attribution_snapshots"]["Row"]> & {
          workspace_id: string; watchlist_id: string; run_id: string; dependency_family: string;
        };
        Update: Partial<Database["public"]["Tables"]["cross_asset_family_conflict_attribution_snapshots"]["Row"]>;
        Relationships: [];
      };
      cross_asset_symbol_conflict_attribution_snapshots: {
        Row: {
          id: string;
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          conflict_profile_id: string | null;
          symbol: string; dependency_family: string; dependency_type: string | null;
          family_consensus_state: string;
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
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<Database["public"]["Tables"]["cross_asset_symbol_conflict_attribution_snapshots"]["Row"]> & {
          workspace_id: string; watchlist_id: string; run_id: string; symbol: string; dependency_family: string;
        };
        Update: Partial<Database["public"]["Tables"]["cross_asset_symbol_conflict_attribution_snapshots"]["Row"]>;
        Relationships: [];
      };
      cross_asset_family_conflict_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
          family_consensus_state: string;
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
        };
        Relationships: [];
      };
      cross_asset_symbol_conflict_attribution_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          symbol: string; dependency_family: string; dependency_type: string | null;
          family_consensus_state: string;
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
        };
        Relationships: [];
      };
      run_cross_asset_conflict_attribution_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
      // ── Phase 4.8C: Conflict-Aware Composite Refinement ─────────────────
      cross_asset_conflict_integration_profiles: {
        Row: {
          id: string;
          workspace_id: string;
          profile_name: string;
          is_active: boolean;
          integration_mode: string;
          integration_weight: number | string;
          aligned_supportive_scale: number | string;
          aligned_suppressive_scale: number | string;
          partial_agreement_scale: number | string;
          conflicted_scale: number | string;
          unreliable_scale: number | string;
          insufficient_context_scale: number | string;
          conflict_extra_suppression: number | string;
          unreliable_extra_suppression: number | string;
          dominant_conflict_source_suppression: Record<string, unknown>;
          max_positive_contribution: number | string;
          max_negative_contribution: number | string;
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<Database["public"]["Tables"]["cross_asset_conflict_integration_profiles"]["Row"]> & {
          workspace_id: string; profile_name: string;
        };
        Update: Partial<Database["public"]["Tables"]["cross_asset_conflict_integration_profiles"]["Row"]>;
        Relationships: [];
      };
      cross_asset_conflict_composite_snapshots: {
        Row: {
          id: string;
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
          conflict_integration_profile_id: string | null;
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
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<Database["public"]["Tables"]["cross_asset_conflict_composite_snapshots"]["Row"]> & {
          workspace_id: string; watchlist_id: string; run_id: string; integration_mode: string;
        };
        Update: Partial<Database["public"]["Tables"]["cross_asset_conflict_composite_snapshots"]["Row"]>;
        Relationships: [];
      };
      cross_asset_family_conflict_composite_snapshots: {
        Row: {
          id: string;
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
          metadata: Record<string, unknown>;
          created_at: string;
        };
        Insert: Partial<Database["public"]["Tables"]["cross_asset_family_conflict_composite_snapshots"]["Row"]> & {
          workspace_id: string; watchlist_id: string; run_id: string; dependency_family: string;
        };
        Update: Partial<Database["public"]["Tables"]["cross_asset_family_conflict_composite_snapshots"]["Row"]>;
        Relationships: [];
      };
      cross_asset_conflict_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
        };
        Relationships: [];
      };
      cross_asset_family_conflict_composite_summary: {
        Row: {
          workspace_id: string; watchlist_id: string;
          run_id: string; context_snapshot_id: string | null;
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
        };
        Relationships: [];
      };
      run_cross_asset_conflict_integration_summary: {
        Row: {
          run_id: string; workspace_id: string; watchlist_id: string;
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
        };
        Relationships: [];
      };
    };
    Functions: {
      enqueue_recompute_job: {
        Args: {
          p_workspace_slug: string;
          p_trigger_type?: string;
          p_requested_by?: string | null;
          p_payload?: Record<string, unknown>;
        };
        Returns: Array<{ job_id: string; workspace_id: string; status: string }>;
      };
      requeue_dead_letter: {
        Args: { p_dead_letter_id: number; p_reset_retry_count?: boolean };
        Returns: string;
      };
      enqueue_governed_recompute: {
        Args: {
          p_workspace_slug: string;
          p_watchlist_slug?: string | null;
          p_trigger_type?: string;
          p_requested_by?: string | null;
          p_payload?: Record<string, unknown>;
        };
        Returns: Array<{
          allowed: boolean; reason: string; assigned_priority: number;
          job_id: string | null; workspace_id: string; queue_id: number | null; watchlist_id: string | null;
        }>;
      };
      evaluate_alert_policies: {
        Args: {
          p_workspace_id: string;
          p_watchlist_id?: string | null;
          p_event_type: string;
          p_severity: string;
          p_job_run_id: string;
          p_payload?: Record<string, unknown>;
        };
        Returns: number;
      };
      enqueue_replay_run: {
        Args: {
          p_source_run_id: string;
          p_requested_by?: string | null;
        };
        Returns: Array<{
          allowed: boolean; reason: string; assigned_priority: number;
          job_id: string | null; workspace_id: string; queue_id: number | null;
          watchlist_id: string | null; replayed_from_run_id: string;
        }>;
      };
    };
  };
};

export function createBrowserSupabaseClient(): SupabaseClient<Database> {
  return createClient<Database>(env.NEXT_PUBLIC_SUPABASE_URL, env.NEXT_PUBLIC_SUPABASE_ANON_KEY);
}

export function createServiceSupabaseClient(): SupabaseClient<Database> {
  if (!env.SUPABASE_SERVICE_ROLE_KEY) throw new Error("SUPABASE_SERVICE_ROLE_KEY is required on the server");
  return createClient<Database>(env.NEXT_PUBLIC_SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false }
  });
}
