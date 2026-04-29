-- Phase 3.6B: Rollback Impact Analysis
-- Post-rollback outcome measurement, impact classification, and effectiveness summaries.

-- ── A. Persisted impact snapshot table ───────────────────────────────────────
create table if not exists public.governance_routing_policy_rollback_impact_snapshots (
    id                              uuid primary key default gen_random_uuid(),
    workspace_id                    uuid not null references public.workspaces(id) on delete cascade,
    rollback_execution_id           uuid not null
        references public.governance_routing_policy_rollback_executions(id),
    rollback_candidate_id           uuid not null
        references public.governance_routing_policy_autopromotion_rollback_candidates(id),
    recommendation_key              text not null,
    scope_type                      text not null,
    scope_value                     text not null,
    target_type                     text not null,
    evaluation_window_label         text not null default '30d',
    evaluation_started_at           timestamptz not null default now(),
    impact_classification           text not null
        check (impact_classification in ('improved', 'neutral', 'degraded', 'insufficient_data')),
    before_metrics                  jsonb not null default '{}'::jsonb,
    after_metrics                   jsonb not null default '{}'::jsonb,
    delta_metrics                   jsonb not null default '{}'::jsonb,
    metadata                        jsonb not null default '{}'::jsonb,
    created_at                      timestamptz not null default now()
);

create index if not exists idx_grpris_workspace_exec
    on public.governance_routing_policy_rollback_impact_snapshots
    (workspace_id, rollback_execution_id, created_at desc);

create index if not exists idx_grpris_workspace_candidate
    on public.governance_routing_policy_rollback_impact_snapshots
    (workspace_id, rollback_candidate_id);

-- ── B. Latest impact summary view ────────────────────────────────────────────
create or replace view public.governance_routing_policy_rollback_impact_summary as
with latest_snapshot as (
    select distinct on (workspace_id, rollback_execution_id)
        id,
        workspace_id,
        rollback_execution_id,
        rollback_candidate_id,
        recommendation_key,
        scope_type,
        scope_value,
        target_type,
        impact_classification,
        evaluation_window_label,
        created_at,
        before_metrics,
        after_metrics,
        delta_metrics
    from public.governance_routing_policy_rollback_impact_snapshots
    order by workspace_id, rollback_execution_id, created_at desc
)
select
    ls.workspace_id,
    ls.rollback_execution_id,
    ls.rollback_candidate_id,
    ls.recommendation_key,
    ls.scope_type,
    ls.scope_value,
    ls.target_type,
    ls.impact_classification,
    ls.evaluation_window_label,
    ls.created_at,
    -- before metrics extracted from jsonb
    (ls.before_metrics->>'recurrence_rate')::numeric          as before_recurrence_rate,
    (ls.after_metrics->>'recurrence_rate')::numeric           as after_recurrence_rate,
    (ls.before_metrics->>'reassignment_rate')::numeric        as before_reassignment_rate,
    (ls.after_metrics->>'reassignment_rate')::numeric         as after_reassignment_rate,
    (ls.before_metrics->>'escalation_rate')::numeric          as before_escalation_rate,
    (ls.after_metrics->>'escalation_rate')::numeric           as after_escalation_rate,
    (ls.before_metrics->>'avg_resolve_latency_seconds')::numeric  as before_avg_resolve_latency_seconds,
    (ls.after_metrics->>'avg_resolve_latency_seconds')::numeric   as after_avg_resolve_latency_seconds,
    (ls.before_metrics->>'reopen_rate')::numeric              as before_reopen_rate,
    (ls.after_metrics->>'reopen_rate')::numeric               as after_reopen_rate,
    (ls.before_metrics->>'workload_pressure')::numeric        as before_workload_pressure,
    (ls.after_metrics->>'workload_pressure')::numeric         as after_workload_pressure,
    -- deltas extracted from jsonb
    (ls.delta_metrics->>'recurrence_rate')::numeric           as delta_recurrence_rate,
    (ls.delta_metrics->>'reassignment_rate')::numeric         as delta_reassignment_rate,
    (ls.delta_metrics->>'escalation_rate')::numeric           as delta_escalation_rate,
    (ls.delta_metrics->>'avg_resolve_latency_seconds')::numeric   as delta_avg_resolve_latency_seconds,
    (ls.delta_metrics->>'reopen_rate')::numeric               as delta_reopen_rate,
    (ls.delta_metrics->>'workload_pressure')::numeric         as delta_workload_pressure
from latest_snapshot ls;

-- ── C. Rollback effectiveness summary view ────────────────────────────────────
create or replace view public.governance_routing_policy_rollback_effectiveness_summary as
with latest_snapshot as (
    select distinct on (workspace_id, rollback_execution_id)
        workspace_id,
        rollback_execution_id,
        impact_classification,
        created_at,
        (delta_metrics->>'recurrence_rate')::numeric        as delta_recurrence_rate,
        (delta_metrics->>'escalation_rate')::numeric        as delta_escalation_rate,
        (delta_metrics->>'avg_resolve_latency_seconds')::numeric as delta_resolve_latency,
        (delta_metrics->>'workload_pressure')::numeric      as delta_workload_pressure
    from public.governance_routing_policy_rollback_impact_snapshots
    order by workspace_id, rollback_execution_id, created_at desc
)
select
    workspace_id,
    count(*)                                                as rollback_count,
    count(*) filter (where impact_classification = 'improved')          as improved_count,
    count(*) filter (where impact_classification = 'neutral')           as neutral_count,
    count(*) filter (where impact_classification = 'degraded')          as degraded_count,
    count(*) filter (where impact_classification = 'insufficient_data') as insufficient_data_count,
    case when count(*) > 0
        then count(*) filter (where impact_classification = 'improved')::numeric / count(*)
        else null
    end                                                     as improved_rate,
    case when count(*) > 0
        then count(*) filter (where impact_classification = 'degraded')::numeric / count(*)
        else null
    end                                                     as degraded_rate,
    max(created_at)                                         as latest_rollback_at,
    avg(delta_recurrence_rate)                              as average_delta_recurrence_rate,
    avg(delta_escalation_rate)                              as average_delta_escalation_rate,
    avg(delta_resolve_latency)                              as average_delta_resolve_latency_seconds,
    avg(delta_workload_pressure)                            as average_delta_workload_pressure
from latest_snapshot
group by workspace_id;

-- ── D. Pending evaluation view ────────────────────────────────────────────────
create or replace view public.governance_routing_policy_rollback_pending_evaluation_summary as
with snapshot_presence as (
    select distinct rollback_execution_id
    from public.governance_routing_policy_rollback_impact_snapshots
)
select
    re.workspace_id,
    re.id                                           as rollback_execution_id,
    re.rollback_candidate_id,
    rc.recommendation_key,
    re.scope_type,
    re.scope_value,
    re.execution_target                             as target_type,
    re.executed_at,
    extract(epoch from (now() - re.executed_at)) / 86400.0  as days_since_execution,
    (sp.rollback_execution_id is not null)          as has_impact_snapshot,
    -- sufficient post data: at least 3 days since rollback
    (extract(epoch from (now() - re.executed_at)) / 86400.0 >= 3.0)  as sufficient_post_data,
    case
        when sp.rollback_execution_id is not null
            then 'snapshot_exists'
        when extract(epoch from (now() - re.executed_at)) / 86400.0 < 3.0
            then 'too_recent'
        else 'awaiting_snapshot'
    end                                             as pending_reason_code
from public.governance_routing_policy_rollback_executions re
inner join public.governance_routing_policy_autopromotion_rollback_candidates rc
    on rc.id = re.rollback_candidate_id
left join snapshot_presence sp
    on sp.rollback_execution_id = re.id
order by re.executed_at desc;
