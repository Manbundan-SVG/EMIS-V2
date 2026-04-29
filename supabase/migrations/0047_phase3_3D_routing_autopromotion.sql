-- Phase 3.3D: guarded routing autopromotion

alter table public.governance_routing_rules
  add column if not exists regime text null;

alter table public.governance_routing_overrides
  add column if not exists regime text null;

create table if not exists public.governance_routing_autopromotion_policies (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null,
  enabled boolean not null default true,
  scope_type text not null check (scope_type in ('global','team','watchlist','root_cause','version_tuple','regime')),
  scope_value text null,
  promotion_target text not null check (promotion_target in ('override','rule')),
  min_confidence text not null default 'high' check (min_confidence in ('low', 'medium', 'high')),
  min_acceptance_rate numeric not null default 0.80,
  min_sample_size integer not null default 5,
  max_recent_override_rate numeric not null default 0.20,
  cooldown_hours integer not null default 24,
  created_by text null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists governance_routing_autopromotion_policy_scope_idx
  on public.governance_routing_autopromotion_policies (
    workspace_id,
    scope_type,
    coalesce(scope_value, ''),
    promotion_target
  );

create table if not exists public.governance_routing_autopromotion_executions (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null,
  policy_id uuid not null references public.governance_routing_autopromotion_policies(id) on delete cascade,
  recommendation_id uuid not null,
  target_type text not null check (target_type in ('override','rule')),
  target_key text not null,
  recommended_user text null,
  recommended_team text null,
  confidence text not null,
  acceptance_rate numeric null,
  sample_size integer null,
  override_rate numeric null,
  execution_status text not null default 'executed' check (execution_status in ('executed','skipped','rolled_back')),
  execution_reason text null,
  cooldown_bucket text null,
  prior_state jsonb not null default '{}'::jsonb,
  new_state jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists governance_routing_autopromotion_execution_target_idx
  on public.governance_routing_autopromotion_executions (workspace_id, target_type, target_key, created_at desc);

create table if not exists public.governance_routing_autopromotion_rollback_candidates (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null,
  execution_id uuid not null references public.governance_routing_autopromotion_executions(id) on delete cascade,
  target_type text not null,
  target_key text not null,
  prior_state jsonb not null default '{}'::jsonb,
  rollback_reason text null,
  rolled_back boolean not null default false,
  rolled_back_at timestamptz null,
  created_at timestamptz not null default now()
);

create or replace view public.governance_routing_autopromotion_summary as
select
  e.workspace_id,
  w.slug as workspace_slug,
  e.id as execution_id,
  e.policy_id,
  p.scope_type,
  p.scope_value,
  p.promotion_target,
  e.recommendation_id,
  e.target_type,
  e.target_key,
  e.recommended_user,
  e.recommended_team,
  e.confidence,
  e.acceptance_rate,
  e.sample_size,
  e.override_rate,
  e.execution_status,
  e.execution_reason,
  e.cooldown_bucket,
  e.prior_state,
  e.new_state,
  e.metadata,
  e.created_at,
  rc.id as rollback_candidate_id,
  rc.rollback_reason,
  rc.rolled_back,
  rc.rolled_back_at
from public.governance_routing_autopromotion_executions e
join public.governance_routing_autopromotion_policies p
  on p.id = e.policy_id
join public.workspaces w
  on w.id = e.workspace_id
left join public.governance_routing_autopromotion_rollback_candidates rc
  on rc.execution_id = e.id;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0047',
  '0047_phase3_3D_routing_autopromotion',
  current_user,
  jsonb_build_object('phase', '3.3D', 'feature', 'routing_autopromotion')
)
on conflict (version) do nothing;
