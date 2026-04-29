-- Phase 3.5C: Guarded Routing Policy Autopromotion
-- Adds autopromotion policies, executions, rollback candidates, summary view, and eligibility view.
-- Operates on top of 3.5A recommendations and 3.5B review/promotion baseline.

-- ── A. Autopromotion policy table ────────────────────────────────────────────
create table if not exists public.governance_routing_policy_autopromotion_policies (
    id                          uuid primary key default gen_random_uuid(),
    workspace_id                uuid not null references public.workspaces(id) on delete cascade,
    enabled                     boolean not null default true,
    scope_type                  text not null,       -- operator|team|root_cause|regime|severity|chronicity
    scope_value                 text not null,       -- the specific value (team name, operator id, etc.)
    promotion_target            text not null default 'rule',  -- rule | override
    min_confidence              text not null default 'high',  -- low|medium|high
    min_approved_review_count   int  not null default 1,
    min_application_count       int  not null default 1,
    min_sample_size             int  not null default 50,
    max_recent_override_rate    numeric(5,4) not null default 0.20,
    max_recent_reassignment_rate numeric(5,4) not null default 0.15,
    cooldown_hours              int  not null default 168,     -- 7 days default
    created_by                  text not null,
    created_at                  timestamptz not null default now(),
    updated_at                  timestamptz not null default now(),
    metadata                    jsonb not null default '{}'::jsonb,
    constraint uq_autopromo_policy_scope unique (workspace_id, scope_type, scope_value)
);

create index if not exists idx_grpap_workspace_enabled
    on public.governance_routing_policy_autopromotion_policies (workspace_id, enabled);

-- ── B. Autopromotion executions table ────────────────────────────────────────
create table if not exists public.governance_routing_policy_autopromotion_executions (
    id                      uuid primary key default gen_random_uuid(),
    workspace_id            uuid not null references public.workspaces(id) on delete cascade,
    policy_id               uuid not null references public.governance_routing_policy_autopromotion_policies(id),
    recommendation_key      text not null,
    proposal_id             uuid references public.governance_routing_policy_promotion_proposals(id),
    application_id          uuid references public.governance_routing_policy_applications(id),
    outcome                 text not null check (outcome in ('promoted', 'skipped', 'blocked')),
    blocked_reason          text,    -- set when outcome=blocked
    skipped_reason          text,    -- set when outcome=skipped (e.g. no_policy_coverage, in_cooldown)
    executed_by             text not null default 'worker_autopromotion',
    executed_at             timestamptz not null default now(),
    prior_policy            jsonb not null default '{}'::jsonb,
    applied_policy          jsonb not null default '{}'::jsonb,
    metadata                jsonb not null default '{}'::jsonb
);

create index if not exists idx_grpae_workspace_rec_key
    on public.governance_routing_policy_autopromotion_executions (workspace_id, recommendation_key, executed_at desc);

create index if not exists idx_grpae_policy_id
    on public.governance_routing_policy_autopromotion_executions (policy_id, executed_at desc);

-- ── C. Rollback candidates table ─────────────────────────────────────────────
create table if not exists public.governance_routing_policy_autopromotion_rollback_candidates (
    id                      uuid primary key default gen_random_uuid(),
    workspace_id            uuid not null references public.workspaces(id) on delete cascade,
    execution_id            uuid not null references public.governance_routing_policy_autopromotion_executions(id),
    recommendation_key      text not null,
    scope_type              text not null,
    scope_value             text not null,
    prior_policy            jsonb not null default '{}'::jsonb,
    applied_policy          jsonb not null default '{}'::jsonb,
    routing_row_id          uuid,    -- id of the routing_rules or routing_overrides row that was written
    routing_table           text,    -- governance_routing_rules | governance_routing_overrides
    resolved                boolean not null default false,
    resolved_at             timestamptz,
    resolved_by             text,
    created_at              timestamptz not null default now(),
    metadata                jsonb not null default '{}'::jsonb
);

create index if not exists idx_grparc_workspace
    on public.governance_routing_policy_autopromotion_rollback_candidates (workspace_id, resolved, created_at desc);

-- ── D. Autopromotion summary view ─────────────────────────────────────────────
create or replace view public.governance_routing_policy_autopromotion_summary as
with latest_exec as (
    select distinct on (workspace_id, recommendation_key)
        id,
        workspace_id,
        policy_id,
        recommendation_key,
        proposal_id,
        application_id,
        outcome,
        blocked_reason,
        skipped_reason,
        executed_by,
        executed_at,
        prior_policy,
        applied_policy
    from public.governance_routing_policy_autopromotion_executions
    order by workspace_id, recommendation_key, executed_at desc
),
exec_counts as (
    select
        workspace_id,
        recommendation_key,
        count(*) filter (where outcome = 'promoted')  as promoted_count,
        count(*) filter (where outcome = 'blocked')   as blocked_count,
        count(*) filter (where outcome = 'skipped')   as skipped_count,
        count(*)                                       as total_executions
    from public.governance_routing_policy_autopromotion_executions
    group by workspace_id, recommendation_key
),
open_rollbacks as (
    select
        workspace_id,
        recommendation_key,
        count(*) as open_rollback_count
    from public.governance_routing_policy_autopromotion_rollback_candidates
    where resolved = false
    group by workspace_id, recommendation_key
)
select
    le.workspace_id,
    le.recommendation_key,
    le.id                   as latest_execution_id,
    le.policy_id            as latest_policy_id,
    le.proposal_id          as latest_proposal_id,
    le.application_id       as latest_application_id,
    le.outcome              as latest_outcome,
    le.blocked_reason       as latest_blocked_reason,
    le.skipped_reason       as latest_skipped_reason,
    le.executed_by          as latest_executed_by,
    le.executed_at          as latest_executed_at,
    coalesce(ec.promoted_count, 0)      as promoted_count,
    coalesce(ec.blocked_count, 0)       as blocked_count,
    coalesce(ec.skipped_count, 0)       as skipped_count,
    coalesce(ec.total_executions, 0)    as total_executions,
    coalesce(orb.open_rollback_count, 0) as open_rollback_count
from latest_exec le
left join exec_counts    ec  on ec.workspace_id = le.workspace_id and ec.recommendation_key = le.recommendation_key
left join open_rollbacks orb on orb.workspace_id = le.workspace_id and orb.recommendation_key = le.recommendation_key;

-- ── E. Autopromotion eligibility view ────────────────────────────────────────
-- Joins 3.5A recommendations with active policies to surface eligible candidates
-- and the blocking reasons for ineligible ones.
create or replace view public.governance_routing_policy_autopromotion_eligibility as
with rec as (
    select
        r.workspace_id,
        r.recommendation_key,
        r.scope_type,
        r.scope_value,
        r.confidence,
        r.sample_size,
        r.expected_benefit_score,
        r.risk_score,
        r.recommended_policy
    from public.governance_routing_policy_recommendations r
),
policy_match as (
    select
        r.workspace_id,
        r.recommendation_key,
        r.scope_type,
        r.scope_value,
        r.confidence,
        r.sample_size,
        r.expected_benefit_score,
        r.risk_score,
        p.id           as policy_id,
        p.enabled      as policy_enabled,
        p.promotion_target,
        p.min_confidence,
        p.min_approved_review_count,
        p.min_application_count,
        p.min_sample_size,
        p.cooldown_hours
    from rec r
    inner join public.governance_routing_policy_autopromotion_policies p
        on p.workspace_id = r.workspace_id
        and p.scope_type  = r.scope_type
        and p.scope_value = r.scope_value
        and p.enabled     = true
),
review_counts as (
    select
        workspace_id,
        recommendation_key,
        count(*) filter (where review_status = 'approved') as approved_review_count
    from public.governance_routing_policy_recommendation_reviews
    group by workspace_id, recommendation_key
),
application_counts as (
    select
        workspace_id,
        recommendation_key,
        count(*) as application_count
    from public.governance_routing_policy_applications
    group by workspace_id, recommendation_key
),
last_exec as (
    select distinct on (workspace_id, recommendation_key)
        workspace_id,
        recommendation_key,
        executed_at,
        outcome
    from public.governance_routing_policy_autopromotion_executions
    where outcome = 'promoted'
    order by workspace_id, recommendation_key, executed_at desc
)
select
    pm.workspace_id,
    pm.recommendation_key,
    pm.scope_type,
    pm.scope_value,
    pm.confidence,
    pm.sample_size,
    pm.expected_benefit_score,
    pm.risk_score,
    pm.policy_id,
    pm.promotion_target,
    coalesce(rc.approved_review_count, 0)  as approved_review_count,
    coalesce(ac.application_count, 0)      as application_count,
    le.executed_at                         as last_promoted_at,
    -- eligible when all guardrails pass
    (
        pm.confidence in (
            case pm.min_confidence
                when 'low'    then 'low'
                when 'medium' then 'medium'
                else 'high'
            end,
            case when pm.min_confidence in ('low', 'medium') then 'medium' else null end,
            case when pm.min_confidence = 'low' then 'high' else null end,
            'high'
        )
        and pm.sample_size          >= pm.min_sample_size
        and coalesce(rc.approved_review_count, 0) >= pm.min_approved_review_count
        and coalesce(ac.application_count, 0)     >= pm.min_application_count
        and (
            le.executed_at is null
            or le.executed_at < now() - (pm.cooldown_hours || ' hours')::interval
        )
    ) as is_eligible,
    -- blocking reason string for UI display
    case
        when pm.sample_size < pm.min_sample_size
            then 'insufficient_sample_size'
        when coalesce(rc.approved_review_count, 0) < pm.min_approved_review_count
            then 'insufficient_approved_reviews'
        when coalesce(ac.application_count, 0) < pm.min_application_count
            then 'insufficient_manual_applications'
        when le.executed_at is not null
            and le.executed_at >= now() - (pm.cooldown_hours || ' hours')::interval
            then 'in_cooldown'
        else null
    end as blocked_reason
from policy_match pm
left join review_counts     rc on rc.workspace_id = pm.workspace_id and rc.recommendation_key = pm.recommendation_key
left join application_counts ac on ac.workspace_id = pm.workspace_id and ac.recommendation_key = pm.recommendation_key
left join last_exec          le on le.workspace_id = pm.workspace_id and le.recommendation_key = pm.recommendation_key;
