-- Phase 3.7C: Governance Policy Autopromotion
-- Policy-gated, cooldown-aware autopromotion for governance policy recommendations.
-- Requires: approved review + successful manual application + confidence + sample size.
-- Every autopromotion generates a rollback candidate.

begin;

-- ── A. Autopromotion policies table ──────────────────────────────────────────
create table if not exists public.governance_policy_autopromotion_policies (
    id                              uuid primary key default gen_random_uuid(),
    workspace_id                    uuid not null references public.workspaces(id) on delete cascade,
    enabled                         boolean not null default true,
    policy_family                   text not null,
    scope_type                      text not null,
    scope_value                     text not null,
    promotion_target                text not null,
    min_confidence                  text not null default 'high'
        check (min_confidence in ('low', 'medium', 'high')),
    min_approved_review_count       integer not null default 1,
    min_application_count           integer not null default 1,
    min_sample_size                 integer not null default 5,
    max_recent_override_rate        numeric not null default 0.25,
    max_recent_reassignment_rate    numeric not null default 0.25,
    cooldown_hours                  integer not null default 72,
    created_by                      text not null,
    created_at                      timestamptz not null default now(),
    metadata                        jsonb not null default '{}'::jsonb
);

create unique index if not exists idx_gpap_scope_target_unique
    on public.governance_policy_autopromotion_policies
    (workspace_id, policy_family, scope_type, scope_value, promotion_target);

-- ── B. Autopromotion executions table ─────────────────────────────────────────
create table if not exists public.governance_policy_autopromotion_executions (
    id                  uuid primary key default gen_random_uuid(),
    workspace_id        uuid not null references public.workspaces(id) on delete cascade,
    recommendation_key  text not null,
    policy_id           uuid not null references public.governance_policy_autopromotion_policies(id) on delete cascade,
    policy_family       text not null,
    promotion_target    text not null,
    scope_type          text not null,
    scope_value         text not null,
    current_policy      jsonb not null default '{}'::jsonb,
    applied_policy      jsonb not null default '{}'::jsonb,
    executed_at         timestamptz not null default now(),
    executed_by         text not null default 'system',
    cooldown_applied    boolean not null default false,
    metadata            jsonb not null default '{}'::jsonb
);

create index if not exists idx_gpape_workspace_key
    on public.governance_policy_autopromotion_executions
    (workspace_id, recommendation_key, executed_at desc);

create index if not exists idx_gpape_workspace_policy
    on public.governance_policy_autopromotion_executions
    (workspace_id, policy_id, executed_at desc);

-- ── C. Rollback candidates table ──────────────────────────────────────────────
create table if not exists public.governance_policy_autopromotion_rollback_candidates (
    id                      uuid primary key default gen_random_uuid(),
    workspace_id            uuid not null references public.workspaces(id) on delete cascade,
    execution_id            uuid not null references public.governance_policy_autopromotion_executions(id) on delete cascade,
    recommendation_key      text not null,
    policy_family           text not null,
    scope_type              text not null,
    scope_value             text not null,
    target_type             text not null,
    prior_policy            jsonb not null default '{}'::jsonb,
    applied_policy          jsonb not null default '{}'::jsonb,
    rollback_reason_code    text,
    rollback_risk_score     numeric not null default 0,
    created_at              timestamptz not null default now(),
    rolled_back             boolean not null default false,
    rolled_back_at          timestamptz,
    metadata                jsonb not null default '{}'::jsonb
);

create index if not exists idx_gparc_workspace_key
    on public.governance_policy_autopromotion_rollback_candidates
    (workspace_id, recommendation_key, created_at desc);

create index if not exists idx_gparc_workspace_exec
    on public.governance_policy_autopromotion_rollback_candidates
    (workspace_id, execution_id);

-- ── D. Autopromotion summary view ─────────────────────────────────────────────
create or replace view public.governance_policy_autopromotion_summary as
with latest_exec as (
    select distinct on (e.workspace_id, e.policy_id)
        e.workspace_id,
        e.policy_id,
        e.recommendation_key    as last_recommendation_key,
        e.executed_at           as latest_execution_at,
        e.metadata              as last_metadata
    from public.governance_policy_autopromotion_executions e
    order by e.workspace_id, e.policy_id, e.executed_at desc, e.id desc
),
exec_counts as (
    select workspace_id, policy_id, count(*) as execution_count
    from public.governance_policy_autopromotion_executions
    group by workspace_id, policy_id
),
rollback_counts as (
    select e.workspace_id, e.policy_id, count(c.id) as rollback_candidate_count
    from public.governance_policy_autopromotion_executions e
    left join public.governance_policy_autopromotion_rollback_candidates c
        on c.execution_id = e.id
    group by e.workspace_id, e.policy_id
)
select
    p.workspace_id,
    p.id                                                as policy_id,
    p.policy_family,
    p.scope_type,
    p.scope_value,
    p.promotion_target,
    p.enabled,
    p.min_confidence,
    p.min_approved_review_count,
    p.min_application_count,
    p.min_sample_size,
    p.cooldown_hours,
    l.latest_execution_at,
    coalesce(ec.execution_count, 0)                     as execution_count,
    coalesce(rc.rollback_candidate_count, 0)            as rollback_candidate_count,
    l.last_recommendation_key
from public.governance_policy_autopromotion_policies p
left join latest_exec l
    on l.workspace_id = p.workspace_id and l.policy_id = p.id
left join exec_counts ec
    on ec.workspace_id = p.workspace_id and ec.policy_id = p.id
left join rollback_counts rc
    on rc.workspace_id = p.workspace_id and rc.policy_id = p.id;

-- ── E. Eligibility view ────────────────────────────────────────────────────────
create or replace view public.governance_policy_autopromotion_eligibility as
with reviews as (
    select
        workspace_id,
        recommendation_key,
        coalesce(review_count, 0)       as approved_review_count,
        coalesce(has_approved_review, false) as has_approved_review
    from public.governance_policy_review_summary
),
promotions as (
    select
        workspace_id,
        recommendation_key,
        coalesce(application_count, 0)  as application_count
    from public.governance_policy_promotion_summary
),
recs as (
    select * from public.governance_policy_recommendations
),
latest_exec as (
    select distinct on (workspace_id, recommendation_key)
        workspace_id,
        recommendation_key,
        executed_at as last_execution_at
    from public.governance_policy_autopromotion_executions
    order by workspace_id, recommendation_key, executed_at desc, id desc
)
select
    r.workspace_id,
    r.recommendation_key,
    p.id                as policy_id,
    p.policy_family,
    p.scope_type,
    p.scope_value,
    p.promotion_target,
    r.confidence,
    r.sample_size,
    coalesce(rv.approved_review_count, 0)  as approved_review_count,
    coalesce(pr.application_count, 0)      as application_count,
    coalesce((r.supporting_metrics->>'recent_override_rate')::numeric, 0)      as recent_override_rate,
    coalesce((r.supporting_metrics->>'recent_reassignment_rate')::numeric, 0)  as recent_reassignment_rate,
    le.last_execution_at,
    case when le.last_execution_at is null then null
         else le.last_execution_at + make_interval(hours => p.cooldown_hours)
    end                 as cooldown_ends_at,
    -- eligibility boolean
    p.enabled
    and (
        r.confidence = p.min_confidence
        or (p.min_confidence = 'medium' and r.confidence in ('medium', 'high'))
        or (p.min_confidence = 'low')
    )
    and coalesce(rv.approved_review_count, 0) >= p.min_approved_review_count
    and coalesce(pr.application_count, 0) >= p.min_application_count
    and r.sample_size >= p.min_sample_size
    and coalesce((r.supporting_metrics->>'recent_override_rate')::numeric, 0) <= p.max_recent_override_rate
    and coalesce((r.supporting_metrics->>'recent_reassignment_rate')::numeric, 0) <= p.max_recent_reassignment_rate
    and (
        le.last_execution_at is null
        or le.last_execution_at <= now() - make_interval(hours => p.cooldown_hours)
    )                   as eligible,
    -- blocked reason code (first failing guard)
    case
        when not p.enabled
            then 'policy_disabled'
        when not (
            r.confidence = p.min_confidence
            or (p.min_confidence = 'medium' and r.confidence in ('medium', 'high'))
            or (p.min_confidence = 'low')
        ) then 'confidence_below_threshold'
        when coalesce(rv.approved_review_count, 0) < p.min_approved_review_count
            then 'insufficient_approved_reviews'
        when coalesce(pr.application_count, 0) < p.min_application_count
            then 'insufficient_manual_applications'
        when r.sample_size < p.min_sample_size
            then 'insufficient_sample_size'
        when coalesce((r.supporting_metrics->>'recent_override_rate')::numeric, 0) > p.max_recent_override_rate
            then 'override_rate_too_high'
        when coalesce((r.supporting_metrics->>'recent_reassignment_rate')::numeric, 0) > p.max_recent_reassignment_rate
            then 'reassignment_rate_too_high'
        when le.last_execution_at is not null
             and le.last_execution_at > now() - make_interval(hours => p.cooldown_hours)
            then 'cooldown_active'
        else null
    end                 as blocked_reason_code
from recs r
join public.governance_policy_autopromotion_policies p
    on  p.workspace_id  = r.workspace_id
    and p.policy_family = r.policy_family
    and p.scope_type    = r.scope_type
    and p.scope_value   = r.scope_value
left join reviews rv
    on  rv.workspace_id       = r.workspace_id
    and rv.recommendation_key = r.recommendation_key
left join promotions pr
    on  pr.workspace_id       = r.workspace_id
    and pr.recommendation_key = r.recommendation_key
left join latest_exec le
    on  le.workspace_id       = r.workspace_id
    and le.recommendation_key = r.recommendation_key;

commit;
