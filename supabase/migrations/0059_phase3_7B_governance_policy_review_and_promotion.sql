-- Phase 3.7B: Governance Policy Review + Promotion Workflow
-- Operator-controlled review, approval-gated promotion, and full auditability.
-- Does NOT auto-promote governance policy changes.

begin;

-- ── A. Review table ───────────────────────────────────────────────────────────
create table if not exists public.governance_policy_recommendation_reviews (
    id                  uuid primary key default gen_random_uuid(),
    workspace_id        uuid not null references public.workspaces(id) on delete cascade,
    recommendation_key  text not null,
    policy_family       text not null,
    review_status       text not null check (review_status in ('approved', 'rejected', 'deferred')),
    review_reason       text,
    reviewed_by         text not null,
    reviewed_at         timestamptz not null default now(),
    notes               text,
    metadata            jsonb not null default '{}'::jsonb
);

create index if not exists idx_gprr_workspace_key
    on public.governance_policy_recommendation_reviews
    (workspace_id, recommendation_key, reviewed_at desc);

-- ── B. Promotion proposals table ──────────────────────────────────────────────
create table if not exists public.governance_policy_promotion_proposals (
    id                  uuid primary key default gen_random_uuid(),
    workspace_id        uuid not null references public.workspaces(id) on delete cascade,
    recommendation_key  text not null,
    policy_family       text not null,
    proposal_status     text not null check (proposal_status in ('pending', 'approved', 'rejected', 'applied', 'deferred')),
    promotion_target    text not null check (promotion_target in ('threshold_profile', 'routing_rule', 'routing_override', 'autopromotion_policy')),
    scope_type          text not null,
    scope_value         text not null,
    current_policy      jsonb not null default '{}'::jsonb,
    recommended_policy  jsonb not null default '{}'::jsonb,
    proposed_by         text not null,
    proposed_at         timestamptz not null default now(),
    approved_by         text,
    approved_at         timestamptz,
    applied_at          timestamptz,
    proposal_reason     text,
    metadata            jsonb not null default '{}'::jsonb
);

create index if not exists idx_gppp_workspace_key
    on public.governance_policy_promotion_proposals
    (workspace_id, recommendation_key, proposed_at desc);

create index if not exists idx_gppp_workspace_status
    on public.governance_policy_promotion_proposals
    (workspace_id, proposal_status, proposed_at desc);

-- ── C. Application audit table ────────────────────────────────────────────────
create table if not exists public.governance_policy_applications (
    id                  uuid primary key default gen_random_uuid(),
    workspace_id        uuid not null references public.workspaces(id) on delete cascade,
    proposal_id         uuid not null references public.governance_policy_promotion_proposals(id) on delete cascade,
    recommendation_key  text not null,
    policy_family       text not null,
    applied_target      text not null,
    applied_scope_type  text not null,
    applied_scope_value text not null,
    prior_policy        jsonb not null default '{}'::jsonb,
    applied_policy      jsonb not null default '{}'::jsonb,
    applied_by          text not null,
    applied_at          timestamptz not null default now(),
    rollback_candidate  boolean not null default true,
    metadata            jsonb not null default '{}'::jsonb
);

create index if not exists idx_gpa_workspace_key
    on public.governance_policy_applications
    (workspace_id, recommendation_key, applied_at desc);

-- ── D. Review summary view ─────────────────────────────────────────────────────
create or replace view public.governance_policy_review_summary as
with ranked as (
    select
        r.*,
        row_number() over (
            partition by r.workspace_id, r.recommendation_key
            order by r.reviewed_at desc, r.id desc
        ) as rn
    from public.governance_policy_recommendation_reviews r
),
counts as (
    select
        workspace_id,
        recommendation_key,
        count(*)                                                    as review_count,
        bool_or(review_status = 'approved')                        as has_approved_review,
        bool_or(review_status = 'rejected')                        as has_rejected_review,
        bool_or(review_status = 'deferred')                        as has_deferred_review
    from public.governance_policy_recommendation_reviews
    group by workspace_id, recommendation_key
)
select
    x.workspace_id,
    x.recommendation_key,
    x.policy_family,
    x.review_status      as latest_review_status,
    x.review_reason      as latest_review_reason,
    x.reviewed_by        as latest_reviewed_by,
    x.reviewed_at        as latest_reviewed_at,
    c.review_count,
    c.has_approved_review,
    c.has_rejected_review,
    c.has_deferred_review
from ranked x
join counts c
    on  c.workspace_id       = x.workspace_id
    and c.recommendation_key = x.recommendation_key
where x.rn = 1;

-- ── E. Promotion summary view ──────────────────────────────────────────────────
create or replace view public.governance_policy_promotion_summary as
with ranked as (
    select
        p.*,
        row_number() over (
            partition by p.workspace_id, p.recommendation_key
            order by p.proposed_at desc, p.id desc
        ) as rn
    from public.governance_policy_promotion_proposals p
),
counts as (
    select
        workspace_id,
        recommendation_key,
        count(*) as proposal_count
    from public.governance_policy_promotion_proposals
    group by workspace_id, recommendation_key
),
app_counts as (
    select
        workspace_id,
        recommendation_key,
        count(*) as application_count
    from public.governance_policy_applications
    group by workspace_id, recommendation_key
)
select
    x.workspace_id,
    x.recommendation_key,
    x.policy_family,
    c.proposal_count,
    x.proposal_status  as latest_proposal_status,
    x.promotion_target as latest_promotion_target,
    x.scope_type       as latest_scope_type,
    x.scope_value      as latest_scope_value,
    x.proposed_by      as latest_proposed_by,
    x.proposed_at      as latest_proposed_at,
    x.approved_by      as latest_approved_by,
    x.approved_at      as latest_approved_at,
    x.applied_at       as latest_applied_at,
    coalesce(a.application_count, 0) as application_count
from ranked x
join counts c
    on  c.workspace_id       = x.workspace_id
    and c.recommendation_key = x.recommendation_key
left join app_counts a
    on  a.workspace_id       = x.workspace_id
    and a.recommendation_key = x.recommendation_key
where x.rn = 1;

-- ── F. Pending promotion summary view ─────────────────────────────────────────
create or replace view public.governance_policy_pending_promotion_summary as
select
    s.workspace_id,
    s.recommendation_key,
    s.policy_family,
    s.latest_proposal_status,
    s.latest_promotion_target,
    s.latest_scope_type,
    s.latest_scope_value,
    s.latest_proposed_by,
    s.latest_proposed_at,
    s.application_count,
    (s.latest_proposal_status in ('pending', 'approved')) as needs_action
from public.governance_policy_promotion_summary s
where s.latest_proposal_status in ('pending', 'approved');

commit;
