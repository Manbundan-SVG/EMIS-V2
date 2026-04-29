-- Phase 3.6A: Rollback Review + Execution Workflow
-- Adds governed, approval-gated rollback for 3.5C autopromotion rollback candidates.

-- ── A. Rollback review table ──────────────────────────────────────────────────
create table if not exists public.governance_routing_policy_rollback_reviews (
    id                      uuid primary key default gen_random_uuid(),
    workspace_id            uuid not null references public.workspaces(id) on delete cascade,
    rollback_candidate_id   uuid not null
        references public.governance_routing_policy_autopromotion_rollback_candidates(id)
        on delete cascade,
    review_status           text not null check (review_status in ('approved', 'rejected', 'deferred')),
    review_reason           text,
    reviewed_by             text not null,
    reviewed_at             timestamptz not null default now(),
    notes                   text,
    metadata                jsonb not null default '{}'::jsonb
);

create index if not exists idx_grprr_workspace_candidate
    on public.governance_routing_policy_rollback_reviews
    (workspace_id, rollback_candidate_id, reviewed_at desc);

-- ── B. Rollback execution table ───────────────────────────────────────────────
create table if not exists public.governance_routing_policy_rollback_executions (
    id                      uuid primary key default gen_random_uuid(),
    workspace_id            uuid not null references public.workspaces(id) on delete cascade,
    rollback_candidate_id   uuid not null
        references public.governance_routing_policy_autopromotion_rollback_candidates(id),
    execution_target        text not null check (execution_target in ('override', 'rule')),
    scope_type              text not null,
    scope_value             text not null,
    promotion_execution_id  uuid not null
        references public.governance_routing_policy_autopromotion_executions(id),
    restored_policy         jsonb not null,
    replaced_policy         jsonb not null,
    executed_by             text not null,
    executed_at             timestamptz not null default now(),
    metadata                jsonb not null default '{}'::jsonb
);

create index if not exists idx_grpre_workspace_candidate
    on public.governance_routing_policy_rollback_executions
    (workspace_id, rollback_candidate_id, executed_at desc);

-- ── C. Latest rollback review summary view ────────────────────────────────────
create or replace view public.governance_routing_policy_rollback_review_summary as
with latest_review as (
    select distinct on (workspace_id, rollback_candidate_id)
        id,
        workspace_id,
        rollback_candidate_id,
        review_status,
        review_reason,
        reviewed_by,
        reviewed_at,
        notes
    from public.governance_routing_policy_rollback_reviews
    order by workspace_id, rollback_candidate_id, reviewed_at desc
),
review_agg as (
    select
        workspace_id,
        rollback_candidate_id,
        count(*)                                               as review_count,
        count(*) filter (where review_status = 'approved')    as approved_count,
        count(*) filter (where review_status = 'rejected')    as rejected_count,
        count(*) filter (where review_status = 'deferred')    as deferred_count
    from public.governance_routing_policy_rollback_reviews
    group by workspace_id, rollback_candidate_id
)
select
    lr.workspace_id,
    lr.rollback_candidate_id,
    rc.recommendation_key,
    lr.review_status              as latest_review_status,
    lr.review_reason              as latest_review_reason,
    lr.reviewed_by                as latest_reviewed_by,
    lr.reviewed_at                as latest_reviewed_at,
    coalesce(ra.review_count, 0)  as review_count,
    coalesce(ra.approved_count, 0) > 0  as has_approved_review,
    coalesce(ra.rejected_count, 0) > 0  as has_rejected_review,
    coalesce(ra.deferred_count, 0) > 0  as has_deferred_review
from latest_review lr
inner join public.governance_routing_policy_autopromotion_rollback_candidates rc
    on rc.id = lr.rollback_candidate_id
left join review_agg ra
    on ra.workspace_id = lr.workspace_id
    and ra.rollback_candidate_id = lr.rollback_candidate_id;

-- ── D. Rollback execution summary view ───────────────────────────────────────
create or replace view public.governance_routing_policy_rollback_execution_summary as
with latest_exec as (
    select distinct on (workspace_id, rollback_candidate_id)
        id,
        workspace_id,
        rollback_candidate_id,
        executed_by,
        executed_at
    from public.governance_routing_policy_rollback_executions
    order by workspace_id, rollback_candidate_id, executed_at desc
),
exec_counts as (
    select
        workspace_id,
        rollback_candidate_id,
        count(*) as execution_count
    from public.governance_routing_policy_rollback_executions
    group by workspace_id, rollback_candidate_id
)
select
    rc.workspace_id,
    rc.id                                   as rollback_candidate_id,
    rc.recommendation_key,
    rc.scope_type,
    rc.scope_value,
    rc.routing_table                        as target_type,
    -- risk_score sourced from 3.5A recommendation if available
    coalesce(
        (select risk_score from public.governance_routing_policy_recommendations rec
         where rec.workspace_id = rc.workspace_id
           and rec.recommendation_key = rc.recommendation_key
         limit 1),
        0.0
    )                                       as rollback_risk_score,
    rc.resolved                             as rolled_back,
    rc.resolved_at                          as rolled_back_at,
    coalesce(ec.execution_count, 0)         as execution_count,
    le.id                                   as latest_execution_id,
    le.executed_by                          as latest_executed_by,
    le.executed_at                          as latest_executed_at
from public.governance_routing_policy_autopromotion_rollback_candidates rc
left join latest_exec le
    on le.workspace_id = rc.workspace_id
    and le.rollback_candidate_id = rc.id
left join exec_counts ec
    on ec.workspace_id = rc.workspace_id
    and ec.rollback_candidate_id = rc.id;

-- ── E. Pending rollback summary view ─────────────────────────────────────────
create or replace view public.governance_routing_policy_pending_rollback_summary as
with latest_review as (
    select distinct on (workspace_id, rollback_candidate_id)
        workspace_id,
        rollback_candidate_id,
        review_status
    from public.governance_routing_policy_rollback_reviews
    order by workspace_id, rollback_candidate_id, reviewed_at desc
),
latest_exec as (
    select distinct on (workspace_id, rollback_candidate_id)
        workspace_id,
        rollback_candidate_id,
        executed_at
    from public.governance_routing_policy_rollback_executions
    order by workspace_id, rollback_candidate_id, executed_at desc
),
risk_scores as (
    select
        workspace_id,
        recommendation_key,
        risk_score
    from public.governance_routing_policy_recommendations
)
select
    rc.workspace_id,
    rc.id                                   as rollback_candidate_id,
    rc.recommendation_key,
    rc.scope_type,
    rc.scope_value,
    coalesce(rs.risk_score, 0.0)            as rollback_risk_score,
    lr.review_status                        as latest_review_status,
    -- needs_action: not yet resolved and either awaiting review or approved but not rolled back
    (
        rc.resolved = false
        and (lr.review_status is null or lr.review_status in ('approved', 'deferred'))
    )                                       as needs_action,
    rc.created_at,
    le.executed_at                          as latest_execution_at
from public.governance_routing_policy_autopromotion_rollback_candidates rc
left join latest_review lr
    on lr.workspace_id = rc.workspace_id
    and lr.rollback_candidate_id = rc.id
left join latest_exec le
    on le.workspace_id = rc.workspace_id
    and le.rollback_candidate_id = rc.id
left join risk_scores rs
    on rs.workspace_id = rc.workspace_id
    and rs.recommendation_key = rc.recommendation_key
where rc.resolved = false
order by coalesce(rs.risk_score, 0.0) desc, rc.created_at asc;
