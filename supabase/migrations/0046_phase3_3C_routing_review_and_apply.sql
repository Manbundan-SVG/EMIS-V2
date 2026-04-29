-- Phase 3.3C: routing recommendation review + guarded application workflow

create table if not exists public.governance_routing_recommendation_reviews (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null,
  recommendation_id uuid not null,
  case_id uuid null,
  review_status text not null check (review_status in ('approved', 'rejected', 'deferred')),
  review_reason text null,
  notes text null,
  reviewed_by text null,
  reviewed_at timestamptz not null default now(),
  applied_immediately boolean not null default false,
  metadata jsonb not null default '{}'::jsonb
);

create index if not exists idx_gov_route_reviews_workspace
  on public.governance_routing_recommendation_reviews (workspace_id, reviewed_at desc);

create index if not exists idx_gov_route_reviews_recommendation
  on public.governance_routing_recommendation_reviews (recommendation_id, reviewed_at desc);

create table if not exists public.governance_routing_applications (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null,
  recommendation_id uuid not null,
  review_id uuid null,
  case_id uuid not null,
  previous_assigned_user text null,
  previous_assigned_team text null,
  applied_user text null,
  applied_team text null,
  application_mode text not null default 'manual_reviewed' check (application_mode in ('manual_reviewed', 'manual_direct', 'api_reviewed')),
  application_reason text null,
  applied_by text null,
  applied_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb
);

create index if not exists idx_gov_route_apps_workspace
  on public.governance_routing_applications (workspace_id, applied_at desc);

create index if not exists idx_gov_route_apps_case
  on public.governance_routing_applications (case_id, applied_at desc);

create or replace view public.governance_routing_review_summary as
select
  r.workspace_id,
  r.recommendation_id,
  max(r.reviewed_at) as latest_reviewed_at,
  (array_agg(r.review_status order by r.reviewed_at desc))[1] as latest_review_status,
  count(*) as review_count,
  bool_or(r.applied_immediately) as any_applied_immediately
from public.governance_routing_recommendation_reviews r
group by r.workspace_id, r.recommendation_id;

create or replace view public.governance_routing_application_summary as
select
  a.workspace_id,
  a.case_id,
  a.recommendation_id,
  count(*) as application_count,
  max(a.applied_at) as latest_applied_at,
  (array_agg(a.applied_user order by a.applied_at desc))[1] as latest_applied_user,
  (array_agg(a.applied_team order by a.applied_at desc))[1] as latest_applied_team
from public.governance_routing_applications a
group by a.workspace_id, a.case_id, a.recommendation_id;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0046',
  '0046_phase3_3C_routing_review_and_apply',
  current_user,
  jsonb_build_object('phase', '3.3C', 'feature', 'routing_review_and_apply')
)
on conflict (version) do nothing;
