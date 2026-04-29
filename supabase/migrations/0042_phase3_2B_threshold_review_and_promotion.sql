begin;

alter table public.governance_threshold_recommendations
  add column if not exists recommendation_key text;

update public.governance_threshold_recommendations
set recommendation_key = md5(
  concat_ws(
    '|',
    coalesce(threshold_profile_id::text, 'unscoped'),
    dimension_type,
    coalesce(dimension_value, 'any'),
    event_type,
    direction,
    reason_code
  )
)
where recommendation_key is null;

alter table public.governance_threshold_recommendations
  alter column recommendation_key set not null;

create unique index if not exists idx_governance_threshold_recommendations_workspace_key
  on public.governance_threshold_recommendations (workspace_id, recommendation_key);

create table if not exists public.governance_threshold_recommendation_reviews (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  recommendation_id uuid not null references public.governance_threshold_recommendations(id) on delete cascade,
  reviewer text not null,
  decision text not null check (decision in ('approved', 'rejected', 'deferred')),
  rationale text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_governance_threshold_recommendation_reviews_workspace_created
  on public.governance_threshold_recommendation_reviews (workspace_id, created_at desc);

create index if not exists idx_governance_threshold_recommendation_reviews_rec
  on public.governance_threshold_recommendation_reviews (recommendation_id, created_at desc);

create table if not exists public.governance_threshold_promotion_proposals (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  recommendation_id uuid not null references public.governance_threshold_recommendations(id) on delete cascade,
  profile_id uuid not null references public.governance_threshold_profiles(id) on delete cascade,
  event_type text not null,
  dimension_type text not null,
  dimension_value text,
  current_value numeric not null,
  proposed_value numeric not null,
  status text not null default 'pending' check (status in ('pending', 'approved', 'blocked', 'executed', 'cancelled')),
  approved_by text,
  approved_at timestamptz,
  blocked_reason text,
  source_metrics jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists idx_governance_threshold_promotion_proposals_recommendation
  on public.governance_threshold_promotion_proposals (recommendation_id);

create index if not exists idx_governance_threshold_promotion_proposals_workspace_status
  on public.governance_threshold_promotion_proposals (workspace_id, status, updated_at desc);

create or replace view public.governance_threshold_review_summary as
select
  p.id as proposal_id,
  p.workspace_id,
  coalesce(w.slug, 'unknown') as workspace_slug,
  p.recommendation_id,
  r.recommendation_key,
  p.profile_id,
  p.event_type,
  p.dimension_type,
  p.dimension_value,
  p.current_value,
  p.proposed_value,
  p.status,
  p.approved_by,
  p.approved_at,
  p.blocked_reason,
  p.source_metrics,
  p.metadata,
  p.created_at,
  p.updated_at,
  r.direction,
  r.reason_code,
  r.confidence,
  rr.id as latest_review_id,
  rr.reviewer as latest_reviewer,
  rr.decision as latest_decision,
  rr.rationale as latest_rationale,
  rr.created_at as latest_reviewed_at
from public.governance_threshold_promotion_proposals p
join public.governance_threshold_recommendations r
  on r.id = p.recommendation_id
join public.workspaces w
  on w.id = p.workspace_id
left join lateral (
  select rv.*
  from public.governance_threshold_recommendation_reviews rv
  where rv.recommendation_id = p.recommendation_id
  order by rv.created_at desc
  limit 1
) rr on true;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0042',
  '0042_phase3_2B_threshold_review_and_promotion',
  current_user,
  jsonb_build_object('phase', '3.2B', 'feature', 'threshold_review_and_promotion')
)
on conflict (version) do nothing;

commit;
