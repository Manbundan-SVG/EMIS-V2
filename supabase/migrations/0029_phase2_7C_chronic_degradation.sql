-- Phase 2.7C - Chronic instability escalation + recovery semantics
-- Additive governance state lifecycle built on top of governance alerts,
-- anomaly clusters, and regime-aware threshold applications.

create table if not exists public.governance_degradation_states (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid null references public.watchlists(id) on delete set null,
  degradation_type text not null,
  version_tuple text not null,
  regime text null,
  state_status text not null default 'active'
    check (state_status in ('active', 'escalated', 'resolved')),
  severity text not null,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  escalated_at timestamptz null,
  resolved_at timestamptz null,
  event_count integer not null default 0,
  cluster_count integer not null default 0,
  source_summary jsonb not null default '{}'::jsonb,
  resolution_summary jsonb null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists governance_degradation_states_workspace_status_idx
  on public.governance_degradation_states (workspace_id, state_status, degradation_type, last_seen_at desc);

create index if not exists governance_degradation_states_watchlist_status_idx
  on public.governance_degradation_states (watchlist_id, state_status, last_seen_at desc);

create unique index if not exists governance_degradation_states_active_key_idx
  on public.governance_degradation_states (
    workspace_id,
    coalesce(watchlist_id, '00000000-0000-0000-0000-000000000000'::uuid),
    degradation_type,
    version_tuple,
    coalesce(regime, 'all')
  )
  where state_status in ('active', 'escalated');

create table if not exists public.governance_degradation_state_members (
  id uuid primary key default gen_random_uuid(),
  state_id uuid not null references public.governance_degradation_states(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  governance_alert_event_id uuid null references public.governance_alert_events(id) on delete set null,
  anomaly_cluster_id uuid null references public.governance_anomaly_clusters(id) on delete set null,
  job_run_id uuid null references public.job_runs(id) on delete set null,
  member_type text not null,
  member_key text not null,
  observed_at timestamptz not null default now(),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists governance_degradation_state_members_state_idx
  on public.governance_degradation_state_members (state_id, observed_at desc);

create unique index if not exists governance_degradation_state_members_unique_key_idx
  on public.governance_degradation_state_members (state_id, member_type, member_key);

create table if not exists public.governance_recovery_events (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  state_id uuid not null references public.governance_degradation_states(id) on delete cascade,
  watchlist_id uuid null references public.watchlists(id) on delete set null,
  degradation_type text not null,
  version_tuple text not null,
  regime text null,
  recovered_at timestamptz not null default now(),
  recovery_reason text not null,
  prior_severity text not null,
  trailing_metrics jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists governance_recovery_events_workspace_idx
  on public.governance_recovery_events (workspace_id, recovered_at desc);

create or replace view public.governance_degradation_summary as
select
  s.id,
  s.workspace_id,
  w.slug as workspace_slug,
  s.watchlist_id,
  wl.slug as watchlist_slug,
  wl.name as watchlist_name,
  s.degradation_type,
  s.version_tuple,
  s.regime,
  s.state_status,
  s.severity,
  s.first_seen_at,
  s.last_seen_at,
  s.escalated_at,
  s.resolved_at,
  s.event_count,
  s.cluster_count,
  s.source_summary,
  s.resolution_summary,
  s.metadata,
  coalesce(member_counts.member_count, 0) as member_count,
  extract(epoch from (coalesce(s.resolved_at, now()) - s.first_seen_at)) / 3600.0 as state_duration_hours
from public.governance_degradation_states s
join public.workspaces w
  on w.id = s.workspace_id
left join public.watchlists wl
  on wl.id = s.watchlist_id
left join lateral (
  select count(*)::integer as member_count
  from public.governance_degradation_state_members m
  where m.state_id = s.id
) as member_counts
  on true;

comment on view public.governance_degradation_summary is
  'Workspace/watchlist-aware chronic governance degradation states with member counts and lifecycle timing.';

create or replace view public.governance_recovery_event_summary as
select
  r.id,
  r.workspace_id,
  w.slug as workspace_slug,
  r.state_id,
  r.watchlist_id,
  wl.slug as watchlist_slug,
  wl.name as watchlist_name,
  r.degradation_type,
  r.version_tuple,
  r.regime,
  r.recovered_at,
  r.recovery_reason,
  r.prior_severity,
  r.trailing_metrics,
  r.metadata,
  s.first_seen_at as state_first_seen_at,
  s.last_seen_at as state_last_seen_at,
  s.event_count as state_event_count,
  s.cluster_count as state_cluster_count
from public.governance_recovery_events r
join public.workspaces w
  on w.id = r.workspace_id
left join public.watchlists wl
  on wl.id = r.watchlist_id
left join public.governance_degradation_states s
  on s.id = r.state_id;

comment on view public.governance_recovery_event_summary is
  'Resolved governance degradation states and their recovery context.';

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
select
  '0029_phase2_7C_chronic_degradation',
  'phase2_7C_chronic_degradation',
  'codex',
  jsonb_build_object(
    'tables', jsonb_build_array(
      'governance_degradation_states',
      'governance_degradation_state_members',
      'governance_recovery_events'
    ),
    'views', jsonb_build_array(
      'governance_degradation_summary',
      'governance_recovery_event_summary'
    ),
    'features', jsonb_build_array(
      'persistent_degradation_states',
      'degradation_member_tracking',
      'quiet_window_recovery_semantics'
    )
  )
where not exists (
  select 1
  from public.schema_migration_ledger
  where version = '0029_phase2_7C_chronic_degradation'
);
