begin;

create table if not exists public.governance_threshold_autopromotion_policies (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  profile_id uuid null references public.governance_threshold_profiles(id) on delete cascade,
  event_type text null,
  dimension_type text null,
  dimension_value text null,
  enabled boolean not null default false,
  min_confidence numeric not null default 0.85,
  min_support integer not null default 20,
  max_step_pct numeric not null default 0.20,
  cooldown_hours integer not null default 168,
  allow_regime_specific boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists idx_governance_threshold_autopromotion_policy_scope
  on public.governance_threshold_autopromotion_policies (
    workspace_id,
    coalesce(profile_id::text, 'all'),
    coalesce(event_type, 'all'),
    coalesce(dimension_type, 'all'),
    coalesce(dimension_value, 'all')
  );

create table if not exists public.governance_threshold_promotion_executions (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  proposal_id uuid not null references public.governance_threshold_promotion_proposals(id) on delete cascade,
  profile_id uuid not null references public.governance_threshold_profiles(id) on delete cascade,
  event_type text not null,
  dimension_type text not null,
  dimension_value text,
  previous_value numeric not null,
  new_value numeric not null,
  executed_by text not null,
  execution_mode text not null check (execution_mode in ('manual', 'automatic')),
  rationale text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_governance_threshold_promotion_executions_workspace_created
  on public.governance_threshold_promotion_executions (workspace_id, created_at desc);

create index if not exists idx_governance_threshold_promotion_executions_profile_event
  on public.governance_threshold_promotion_executions (
    workspace_id,
    profile_id,
    event_type,
    dimension_type,
    coalesce(dimension_value, 'all'),
    created_at desc
  );

create table if not exists public.governance_threshold_rollback_candidates (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  execution_id uuid not null references public.governance_threshold_promotion_executions(id) on delete cascade,
  profile_id uuid not null references public.governance_threshold_profiles(id) on delete cascade,
  rollback_to_value numeric not null,
  reason text not null,
  status text not null default 'open' check (status in ('open', 'rolled_back', 'dismissed')),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_governance_threshold_rollback_candidates_workspace_status
  on public.governance_threshold_rollback_candidates (workspace_id, status, created_at desc);

create or replace view public.governance_threshold_autopromotion_summary as
select
  e.id as execution_id,
  e.workspace_id,
  coalesce(w.slug, 'unknown') as workspace_slug,
  e.proposal_id,
  p.recommendation_id,
  e.profile_id,
  e.event_type,
  e.dimension_type,
  e.dimension_value,
  e.previous_value,
  e.new_value,
  e.execution_mode,
  e.executed_by,
  e.rationale,
  e.metadata,
  e.created_at,
  r.id as rollback_candidate_id,
  r.status as rollback_status,
  r.reason as rollback_reason,
  r.rollback_to_value,
  r.updated_at as rollback_updated_at
from public.governance_threshold_promotion_executions e
join public.governance_threshold_promotion_proposals p
  on p.id = e.proposal_id
join public.workspaces w
  on w.id = e.workspace_id
left join public.governance_threshold_rollback_candidates r
  on r.execution_id = e.id;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0043',
  '0043_phase3_2C_threshold_autopromotion',
  current_user,
  jsonb_build_object('phase', '3.2C', 'feature', 'threshold_autopromotion')
)
on conflict (version) do nothing;

commit;
