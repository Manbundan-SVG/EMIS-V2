create table if not exists public.governance_threshold_feedback (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid null references public.watchlists(id) on delete set null,
  threshold_profile_id uuid null references public.governance_threshold_profiles(id) on delete set null,
  event_type text not null,
  regime text null,
  compute_version text null,
  signal_registry_version text null,
  model_version text null,
  case_id uuid null references public.governance_cases(id) on delete set null,
  degradation_state_id uuid null references public.governance_degradation_states(id) on delete set null,
  threshold_applied_value numeric null,
  trigger_count integer not null default 1 check (trigger_count >= 0),
  ack_count integer not null default 0 check (ack_count >= 0),
  mute_count integer not null default 0 check (mute_count >= 0),
  escalation_count integer not null default 0 check (escalation_count >= 0),
  resolution_count integer not null default 0 check (resolution_count >= 0),
  reopen_count integer not null default 0 check (reopen_count >= 0),
  precision_proxy numeric not null default 0,
  noise_score numeric not null default 0,
  evidence jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_governance_threshold_feedback_workspace_created
  on public.governance_threshold_feedback (workspace_id, created_at desc);

create index if not exists idx_governance_threshold_feedback_profile_event
  on public.governance_threshold_feedback (threshold_profile_id, event_type, created_at desc);

create table if not exists public.governance_threshold_recommendations (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  threshold_profile_id uuid null references public.governance_threshold_profiles(id) on delete set null,
  dimension_type text not null,
  dimension_value text not null,
  event_type text not null,
  current_value numeric null,
  recommended_value numeric null,
  direction text not null,
  reason_code text not null,
  confidence numeric not null default 0,
  supporting_metrics jsonb not null default '{}'::jsonb,
  status text not null default 'open'
    check (status in ('open', 'accepted', 'dismissed', 'superseded')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_governance_threshold_recommendations_workspace_status
  on public.governance_threshold_recommendations (workspace_id, status, created_at desc);

create or replace view public.governance_threshold_performance_summary as
select
  f.workspace_id,
  coalesce(w.slug, 'unknown') as workspace_slug,
  coalesce(f.threshold_profile_id::text, 'unscoped') as threshold_profile_key,
  f.event_type,
  coalesce(f.regime, 'any') as regime,
  coalesce(f.compute_version, 'any') as compute_version,
  coalesce(f.signal_registry_version, 'any') as signal_registry_version,
  coalesce(f.model_version, 'any') as model_version,
  count(*)::bigint as feedback_rows,
  coalesce(sum(f.trigger_count), 0)::bigint as trigger_count,
  coalesce(sum(f.ack_count), 0)::bigint as ack_count,
  coalesce(sum(f.mute_count), 0)::bigint as mute_count,
  coalesce(sum(f.escalation_count), 0)::bigint as escalation_count,
  coalesce(sum(f.resolution_count), 0)::bigint as resolution_count,
  coalesce(sum(f.reopen_count), 0)::bigint as reopen_count,
  avg(f.precision_proxy)::numeric as avg_precision_proxy,
  avg(f.noise_score)::numeric as avg_noise_score,
  max(f.created_at) as latest_feedback_at
from public.governance_threshold_feedback f
join public.workspaces w
  on w.id = f.workspace_id
group by
  f.workspace_id,
  w.slug,
  coalesce(f.threshold_profile_id::text, 'unscoped'),
  f.event_type,
  coalesce(f.regime, 'any'),
  coalesce(f.compute_version, 'any'),
  coalesce(f.signal_registry_version, 'any'),
  coalesce(f.model_version, 'any');

create or replace view public.governance_threshold_learning_summary as
select
  r.workspace_id,
  coalesce(w.slug, 'unknown') as workspace_slug,
  r.id as recommendation_id,
  r.threshold_profile_id,
  r.dimension_type,
  r.dimension_value,
  r.event_type,
  r.current_value,
  r.recommended_value,
  r.direction,
  r.reason_code,
  r.confidence,
  r.supporting_metrics,
  r.status,
  r.created_at,
  r.updated_at
from public.governance_threshold_recommendations r
join public.workspaces w
  on w.id = r.workspace_id
order by r.created_at desc;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0041',
  '0041_phase3_2A_threshold_learning',
  current_user,
  jsonb_build_object('phase', '3.2A', 'feature', 'threshold_learning')
)
on conflict (version) do nothing;
