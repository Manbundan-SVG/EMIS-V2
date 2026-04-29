-- Phase 3.3B: advisory routing recommendations

create table if not exists public.governance_routing_recommendations (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null,
  case_id uuid not null,
  recommendation_key text not null,
  recommended_user text null,
  recommended_team text null,
  fallback_user text null,
  fallback_team text null,
  reason_code text not null,
  confidence text not null check (confidence in ('low', 'medium', 'high')),
  score numeric not null default 0,
  supporting_metrics jsonb not null default '{}'::jsonb,
  model_inputs jsonb not null default '{}'::jsonb,
  alternatives jsonb not null default '[]'::jsonb,
  accepted boolean null,
  accepted_at timestamptz null,
  accepted_by text null,
  override_reason text null,
  applied boolean not null default false,
  applied_at timestamptz null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists governance_routing_recommendations_key_idx
  on public.governance_routing_recommendations (workspace_id, recommendation_key);

create index if not exists governance_routing_recommendations_case_idx
  on public.governance_routing_recommendations (case_id, created_at desc);

create or replace view public.governance_routing_recommendation_summary as
select
  r.id,
  r.workspace_id,
  w.slug as workspace_slug,
  r.case_id,
  c.title as case_title,
  c.status as case_status,
  c.severity,
  r.recommendation_key,
  r.recommended_user,
  r.recommended_team,
  r.fallback_user,
  r.fallback_team,
  r.reason_code,
  r.confidence,
  r.score,
  r.accepted,
  r.accepted_at,
  r.accepted_by,
  r.override_reason,
  r.applied,
  r.applied_at,
  r.supporting_metrics,
  r.model_inputs,
  r.alternatives,
  r.created_at,
  r.updated_at
from public.governance_routing_recommendations r
join public.workspaces w
  on w.id = r.workspace_id
join public.governance_cases c
  on c.id = r.case_id;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0045',
  '0045_phase3_3B_routing_recommendations',
  current_user,
  jsonb_build_object('phase', '3.3B', 'feature', 'routing_recommendations')
)
on conflict (version) do nothing;
