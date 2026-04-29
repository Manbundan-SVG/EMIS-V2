-- Phase 3.5B: Routing Policy Review + Promotion Workflow
-- Adds operator-controlled review, proposal, and approval-gated application
-- of advisory routing policy recommendations from Phase 3.5A.
-- Read-only in 3.5A becomes governed workflow in 3.5B.

-- ─────────────────────────────────────────────────────────────────────────────
-- A. Review table
-- Many review rows per recommendation — latest derived in view.
-- ─────────────────────────────────────────────────────────────────────────────
create table if not exists public.governance_routing_policy_recommendation_reviews (
  id                 uuid        primary key default gen_random_uuid(),
  workspace_id       uuid        not null references public.workspaces(id) on delete cascade,
  recommendation_key text        not null,
  review_status      text        not null check (review_status in ('approved', 'rejected', 'deferred')),
  review_reason      text        null,
  reviewed_by        text        not null,
  reviewed_at        timestamptz not null default now(),
  notes              text        null,
  metadata           jsonb       not null default '{}'::jsonb
);

create index if not exists idx_routing_policy_reviews_workspace_key
  on public.governance_routing_policy_recommendation_reviews (workspace_id, recommendation_key, reviewed_at desc);

-- ─────────────────────────────────────────────────────────────────────────────
-- B. Promotion proposal table
-- ─────────────────────────────────────────────────────────────────────────────
create table if not exists public.governance_routing_policy_promotion_proposals (
  id                  uuid        primary key default gen_random_uuid(),
  workspace_id        uuid        not null references public.workspaces(id) on delete cascade,
  recommendation_key  text        not null,
  proposal_status     text        not null default 'pending'
                        check (proposal_status in ('pending', 'approved', 'rejected', 'applied', 'deferred')),
  promotion_target    text        not null check (promotion_target in ('override', 'rule')),
  scope_type          text        not null,
  scope_value         text        not null,
  current_policy      jsonb       not null default '{}'::jsonb,
  recommended_policy  jsonb       not null default '{}'::jsonb,
  proposed_by         text        not null,
  proposed_at         timestamptz not null default now(),
  approved_by         text        null,
  approved_at         timestamptz null,
  applied_at          timestamptz null,
  proposal_reason     text        null,
  metadata            jsonb       not null default '{}'::jsonb
);

create index if not exists idx_routing_policy_proposals_workspace_key
  on public.governance_routing_policy_promotion_proposals (workspace_id, recommendation_key, proposed_at desc);

create index if not exists idx_routing_policy_proposals_status
  on public.governance_routing_policy_promotion_proposals (workspace_id, proposal_status);

-- ─────────────────────────────────────────────────────────────────────────────
-- C. Application / execution table
-- ─────────────────────────────────────────────────────────────────────────────
create table if not exists public.governance_routing_policy_applications (
  id                   uuid        primary key default gen_random_uuid(),
  workspace_id         uuid        not null references public.workspaces(id) on delete cascade,
  proposal_id          uuid        not null references public.governance_routing_policy_promotion_proposals(id),
  recommendation_key   text        not null,
  applied_target       text        not null check (applied_target in ('override', 'rule')),
  applied_scope_type   text        not null,
  applied_scope_value  text        not null,
  prior_policy         jsonb       not null default '{}'::jsonb,
  applied_policy       jsonb       not null default '{}'::jsonb,
  applied_by           text        not null,
  applied_at           timestamptz not null default now(),
  rollback_candidate   boolean     not null default true,
  metadata             jsonb       not null default '{}'::jsonb
);

create index if not exists idx_routing_policy_applications_workspace
  on public.governance_routing_policy_applications (workspace_id, applied_at desc);

create index if not exists idx_routing_policy_applications_proposal
  on public.governance_routing_policy_applications (proposal_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- D. Latest review summary view
-- ─────────────────────────────────────────────────────────────────────────────
create or replace view public.governance_routing_policy_review_summary as
with latest as (
  select distinct on (workspace_id, recommendation_key)
    workspace_id,
    recommendation_key,
    review_status as latest_review_status,
    review_reason as latest_review_reason,
    reviewed_by   as latest_reviewed_by,
    reviewed_at   as latest_reviewed_at
  from public.governance_routing_policy_recommendation_reviews
  order by workspace_id, recommendation_key, reviewed_at desc
),
counts as (
  select
    workspace_id,
    recommendation_key,
    count(*)                                                      as review_count,
    bool_or(review_status = 'approved')                          as has_approved_review,
    bool_or(review_status = 'rejected')                          as has_rejected_review,
    bool_or(review_status = 'deferred')                          as has_deferred_review
  from public.governance_routing_policy_recommendation_reviews
  group by workspace_id, recommendation_key
)
select
  l.workspace_id,
  l.recommendation_key,
  l.latest_review_status,
  l.latest_review_reason,
  l.latest_reviewed_by,
  l.latest_reviewed_at,
  c.review_count,
  c.has_approved_review,
  c.has_rejected_review,
  c.has_deferred_review
from latest l
join counts c using (workspace_id, recommendation_key);

-- ─────────────────────────────────────────────────────────────────────────────
-- E. Promotion summary view
-- ─────────────────────────────────────────────────────────────────────────────
create or replace view public.governance_routing_policy_promotion_summary as
with latest_proposal as (
  select distinct on (workspace_id, recommendation_key)
    id               as latest_proposal_id,
    workspace_id,
    recommendation_key,
    proposal_status  as latest_proposal_status,
    promotion_target as latest_promotion_target,
    scope_type       as latest_scope_type,
    scope_value      as latest_scope_value,
    proposed_by      as latest_proposed_by,
    proposed_at      as latest_proposed_at,
    approved_by      as latest_approved_by,
    approved_at      as latest_approved_at,
    applied_at       as latest_applied_at
  from public.governance_routing_policy_promotion_proposals
  order by workspace_id, recommendation_key, proposed_at desc
),
proposal_counts as (
  select
    workspace_id,
    recommendation_key,
    count(*) as proposal_count
  from public.governance_routing_policy_promotion_proposals
  group by workspace_id, recommendation_key
),
app_counts as (
  select
    workspace_id,
    recommendation_key,
    count(*) as application_count
  from public.governance_routing_policy_applications
  group by workspace_id, recommendation_key
)
select
  lp.workspace_id,
  lp.recommendation_key,
  lp.latest_proposal_id,
  coalesce(pc.proposal_count, 0)    as proposal_count,
  lp.latest_proposal_status,
  lp.latest_promotion_target,
  lp.latest_scope_type,
  lp.latest_scope_value,
  lp.latest_proposed_by,
  lp.latest_proposed_at,
  lp.latest_approved_by,
  lp.latest_approved_at,
  lp.latest_applied_at,
  coalesce(ac.application_count, 0) as application_count
from latest_proposal lp
left join proposal_counts pc using (workspace_id, recommendation_key)
left join app_counts ac using (workspace_id, recommendation_key);

-- ─────────────────────────────────────────────────────────────────────────────
-- F. Rollback candidate summary view
-- ─────────────────────────────────────────────────────────────────────────────
create or replace view public.governance_routing_policy_rollback_candidate_summary as
select
  a.workspace_id,
  a.recommendation_key,
  a.id                  as application_id,
  a.applied_scope_type  as scope_type,
  a.applied_scope_value as scope_value,
  a.applied_at,
  -- derive rollback risk from the original 3.5A recommendation if available
  coalesce(r.risk_score, 0.5)       as rollback_risk_score,
  case
    when r.risk_score >= 0.7 then 'high_risk_score'
    when r.risk_score >= 0.4 then 'moderate_risk_score'
    else 'low_risk_score'
  end                                as rollback_reason_code,
  jsonb_build_object(
    'applied_target',    a.applied_target,
    'applied_by',        a.applied_by,
    'prior_policy',      a.prior_policy,
    'applied_policy',    a.applied_policy
  )                                  as supporting_metrics
from public.governance_routing_policy_applications a
left join public.governance_routing_policy_recommendations r
  on r.workspace_id = a.workspace_id
  and r.recommendation_key = a.recommendation_key
where a.rollback_candidate = true;
