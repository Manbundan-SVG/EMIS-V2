-- Phase 3.7A: Governance Policy Optimization Summaries
-- Advisory optimization layer: measurement + recommendation only.
-- Does NOT modify live threshold/routing policies.

-- ── A. Persisted optimization snapshot table ──────────────────────────────────
create table if not exists public.governance_policy_optimization_snapshots (
    id                  uuid primary key default gen_random_uuid(),
    workspace_id        uuid not null references public.workspaces(id) on delete cascade,
    snapshot_at         timestamptz not null default now(),
    window_label        text not null default '30d',
    recommendation_count integer not null default 0,
    metadata            jsonb not null default '{}'::jsonb
);

create index if not exists idx_gpos_workspace_at
    on public.governance_policy_optimization_snapshots (workspace_id, snapshot_at desc);

-- ── B. Durable recommendations table ─────────────────────────────────────────
create table if not exists public.governance_policy_recommendations (
    id                      uuid primary key default gen_random_uuid(),
    workspace_id            uuid not null references public.workspaces(id) on delete cascade,
    recommendation_key      text not null,
    policy_family           text not null,
    scope_type              text not null,
    scope_value             text not null,
    current_policy          jsonb not null default '{}'::jsonb,
    recommended_policy      jsonb not null default '{}'::jsonb,
    reason_code             text not null,
    confidence              text not null check (confidence in ('low', 'medium', 'high')),
    sample_size             integer not null default 0,
    expected_benefit_score  numeric not null default 0,
    risk_score              numeric not null default 0,
    supporting_metrics      jsonb not null default '{}'::jsonb,
    created_at              timestamptz not null default now(),
    unique (workspace_id, recommendation_key)
);

create index if not exists idx_gpr_workspace_family
    on public.governance_policy_recommendations (workspace_id, policy_family, expected_benefit_score desc);

-- ── C. Feature effectiveness summary view ─────────────────────────────────────
-- Aggregates policy performance from existing live summary surfaces.
create or replace view public.governance_policy_feature_effectiveness_summary as
with

-- threshold policy signals from threshold performance summary
threshold_signals as (
    select
        workspace_id,
        'threshold'                                 as policy_family,
        'regime'                                    as feature_type,
        coalesce(regime, 'unknown')                 as feature_key,
        count(*)                                    as sample_size,
        avg(recurrence_rate)                        as recurrence_rate,
        avg(reopen_rate)                            as reopen_rate,
        avg(escalation_rate)                        as escalation_rate,
        avg(reassignment_rate)                      as reassignment_rate,
        avg(rollback_rate)                          as rollback_rate,
        null::numeric                               as mute_rate,
        avg(approved_review_rate)                   as approved_review_rate,
        avg(application_rate)                       as application_rate,
        avg(avg_ack_latency_seconds)                as avg_ack_latency_seconds,
        avg(avg_resolve_latency_seconds)            as avg_resolve_latency_seconds
    from public.governance_threshold_performance_summary
    group by workspace_id, regime
),

-- routing policy signals from routing recommendation performance
routing_signals as (
    select
        workspace_id,
        'routing'                                   as policy_family,
        'scope_type'                                as feature_type,
        coalesce(scope_type, 'unknown')             as feature_key,
        count(*)                                    as sample_size,
        avg(recurrence_rate)                        as recurrence_rate,
        avg(reopen_rate)                            as reopen_rate,
        avg(escalation_rate)                        as escalation_rate,
        avg(reassignment_rate)                      as reassignment_rate,
        null::numeric                               as rollback_rate,
        null::numeric                               as mute_rate,
        avg(approved_review_rate)                   as approved_review_rate,
        avg(application_rate)                       as application_rate,
        null::numeric                               as avg_ack_latency_seconds,
        null::numeric                               as avg_resolve_latency_seconds
    from public.governance_routing_policy_review_summary
    group by workspace_id, scope_type
),

-- rollback impact signals: policy families that triggered rollbacks
rollback_signals as (
    select
        rps.workspace_id,
        'rollback'                                  as policy_family,
        'impact_classification'                     as feature_type,
        coalesce(rps.impact_classification, 'unknown') as feature_key,
        count(*)                                    as sample_size,
        avg((rps.delta_metrics->>'recurrence_rate')::numeric)   as recurrence_rate,
        avg((rps.delta_metrics->>'reopen_rate')::numeric)       as reopen_rate,
        avg((rps.delta_metrics->>'escalation_rate')::numeric)   as escalation_rate,
        avg((rps.delta_metrics->>'reassignment_rate')::numeric) as reassignment_rate,
        null::numeric                               as rollback_rate,
        null::numeric                               as mute_rate,
        null::numeric                               as approved_review_rate,
        null::numeric                               as application_rate,
        null::numeric                               as avg_ack_latency_seconds,
        avg((rps.delta_metrics->>'avg_resolve_latency_seconds')::numeric) as avg_resolve_latency_seconds
    from public.governance_routing_policy_rollback_impact_snapshots rps
    group by rps.workspace_id, rps.impact_classification
),

-- threshold autopromotion signals
threshold_autopromotion_signals as (
    select
        workspace_id,
        'threshold_autopromotion'                   as policy_family,
        'outcome'                                   as feature_type,
        coalesce(outcome, 'unknown')                as feature_key,
        count(*)                                    as sample_size,
        null::numeric as recurrence_rate,
        null::numeric as reopen_rate,
        null::numeric as escalation_rate,
        null::numeric as reassignment_rate,
        null::numeric as rollback_rate,
        null::numeric as mute_rate,
        null::numeric as approved_review_rate,
        null::numeric as application_rate,
        null::numeric as avg_ack_latency_seconds,
        null::numeric as avg_resolve_latency_seconds
    from public.governance_threshold_autopromotion_summary
    group by workspace_id, outcome
),

-- routing autopromotion signals
routing_autopromotion_signals as (
    select
        workspace_id,
        'routing_autopromotion'                     as policy_family,
        'outcome'                                   as feature_type,
        coalesce(last_outcome, 'unknown')           as feature_key,
        count(*)                                    as sample_size,
        null::numeric as recurrence_rate,
        null::numeric as reopen_rate,
        null::numeric as escalation_rate,
        null::numeric as reassignment_rate,
        null::numeric as rollback_rate,
        null::numeric as mute_rate,
        null::numeric as approved_review_rate,
        null::numeric as application_rate,
        null::numeric as avg_ack_latency_seconds,
        null::numeric as avg_resolve_latency_seconds
    from public.governance_routing_policy_autopromotion_summary
    group by workspace_id, last_outcome
),

all_signals as (
    select * from threshold_signals
    union all select * from routing_signals
    union all select * from rollback_signals
    union all select * from threshold_autopromotion_signals
    union all select * from routing_autopromotion_signals
)

select
    workspace_id,
    policy_family,
    feature_type,
    feature_key,
    sample_size,
    recurrence_rate,
    reopen_rate,
    escalation_rate,
    reassignment_rate,
    rollback_rate,
    mute_rate,
    approved_review_rate,
    application_rate,
    avg_ack_latency_seconds,
    avg_resolve_latency_seconds,
    -- effectiveness_score: 0–1, higher = better outcomes
    greatest(0, least(1,
        0.5
        - coalesce(recurrence_rate, 0) * 0.2
        - coalesce(escalation_rate, 0) * 0.15
        - coalesce(reassignment_rate, 0) * 0.1
        - coalesce(reopen_rate, 0) * 0.1
        + coalesce(application_rate, 0) * 0.15
        + coalesce(approved_review_rate, 0) * 0.1
        + 0.5
    ))                                              as effectiveness_score,
    -- risk_score: 0–1, higher = riskier
    greatest(0, least(1,
        coalesce(rollback_rate, 0) * 0.4
        + coalesce(reassignment_rate, 0) * 0.2
        + coalesce(escalation_rate, 0) * 0.2
        + case when sample_size < 10 then 0.2 else 0 end
    ))                                              as risk_score,
    -- net_policy_fit_score = effectiveness - risk
    greatest(-1, least(1,
        (
            0.5
            - coalesce(recurrence_rate, 0) * 0.2
            - coalesce(escalation_rate, 0) * 0.15
            - coalesce(reassignment_rate, 0) * 0.1
            - coalesce(reopen_rate, 0) * 0.1
            + coalesce(application_rate, 0) * 0.15
            + coalesce(approved_review_rate, 0) * 0.1
            + 0.5
        )
        - (
            coalesce(rollback_rate, 0) * 0.4
            + coalesce(reassignment_rate, 0) * 0.2
            + coalesce(escalation_rate, 0) * 0.2
            + case when sample_size < 10 then 0.2 else 0 end
        )
    ))                                              as net_policy_fit_score
from all_signals
where sample_size > 0;

-- ── D. Context-fit summary view ───────────────────────────────────────────────
-- Identifies which governance policy pattern best fits which incident context.
create or replace view public.governance_policy_context_fit_summary as
with

-- Build routing recommendation context fits from routing optimization surfaces
routing_context as (
    select
        workspace_id,
        context_key,
        'routing'               as best_policy_family,
        concat(
            'routing:',
            coalesce(recommended_team, recommended_user, 'unresolved')
        )                       as best_policy_variant,
        coalesce(team_fit_score, operator_fit_score, 0)  as fit_score,
        sample_size,
        confidence,
        jsonb_build_object(
            'recommended_user',  recommended_user,
            'recommended_team',  recommended_team,
            'operator_fit_score', operator_fit_score,
            'team_fit_score',    team_fit_score
        )                       as supporting_metrics
    from public.governance_routing_context_fit_summary
    where sample_size >= 5
),

-- Build threshold context fits from threshold performance summary by regime
threshold_context as (
    select
        workspace_id,
        concat('regime:', coalesce(regime, 'unknown')) as context_key,
        'threshold'                                     as best_policy_family,
        concat('threshold:regime:', coalesce(regime, 'unknown')) as best_policy_variant,
        greatest(0, least(1,
            0.5
            - coalesce(recurrence_rate, 0) * 0.25
            - coalesce(escalation_rate, 0) * 0.2
            - coalesce(reassignment_rate, 0) * 0.1
            + coalesce(application_rate, 0) * 0.15
            + 0.5
        ))                                              as fit_score,
        sample_size,
        confidence,
        jsonb_build_object(
            'recurrence_rate',   recurrence_rate,
            'escalation_rate',   escalation_rate,
            'reassignment_rate', reassignment_rate,
            'application_rate',  application_rate
        )                       as supporting_metrics
    from public.governance_threshold_performance_summary
    where sample_size >= 5
),

all_contexts as (
    select * from routing_context
    union all select * from threshold_context
)

select
    workspace_id,
    context_key,
    best_policy_family,
    best_policy_variant,
    fit_score,
    sample_size,
    confidence,
    supporting_metrics
from all_contexts
where fit_score is not null
order by fit_score desc;

-- ── E. Policy opportunity summary view ───────────────────────────────────────
-- Surfaces durable recommendations from the recommendations table,
-- or falls back to routing optimization opportunities if none persisted yet.
create or replace view public.governance_policy_opportunity_summary as
select
    gpr.workspace_id,
    gpr.recommendation_key,
    gpr.policy_family,
    gpr.scope_type,
    gpr.scope_value,
    gpr.current_policy,
    gpr.recommended_policy,
    gpr.reason_code,
    gpr.confidence,
    gpr.sample_size,
    gpr.expected_benefit_score,
    gpr.risk_score,
    gpr.supporting_metrics,
    gpr.created_at
from public.governance_policy_recommendations gpr
order by gpr.expected_benefit_score desc, gpr.risk_score asc;
