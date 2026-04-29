-- Phase 2.8 - Recovery / acknowledgment workflows
-- Additive lifecycle controls on top of chronic degradation states.

begin;

create table if not exists public.governance_acknowledgments (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  degradation_state_id uuid not null references public.governance_degradation_states(id) on delete cascade,
  acknowledged_by text not null,
  note text null,
  metadata jsonb not null default '{}'::jsonb,
  acknowledged_at timestamptz not null default now()
);

create index if not exists governance_acknowledgments_workspace_state_idx
  on public.governance_acknowledgments (workspace_id, degradation_state_id, acknowledged_at desc);

create table if not exists public.governance_muting_rules (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  target_type text not null,
  target_key text not null,
  reason text null,
  muted_until timestamptz null,
  created_by text not null,
  is_active boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists governance_muting_rules_workspace_target_idx
  on public.governance_muting_rules (workspace_id, target_type, target_key, created_at desc);

create index if not exists governance_muting_rules_active_idx
  on public.governance_muting_rules (workspace_id, is_active, muted_until);

create table if not exists public.governance_resolution_actions (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  degradation_state_id uuid not null references public.governance_degradation_states(id) on delete cascade,
  action_type text not null,
  performed_by text not null,
  note text null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists governance_resolution_actions_workspace_state_idx
  on public.governance_resolution_actions (workspace_id, degradation_state_id, created_at desc);

create or replace view public.governance_lifecycle_summary as
select
  s.id as degradation_state_id,
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
  ack.id as acknowledgment_id,
  ack.acknowledged_at,
  ack.acknowledged_by,
  ack.note as acknowledgment_note,
  ack.metadata as acknowledgment_metadata,
  mute.id as muting_rule_id,
  mute.target_type as mute_target_type,
  mute.target_key as mute_target_key,
  mute.reason as mute_reason,
  mute.muted_until,
  mute.created_by as muted_by,
  mute.is_active as mute_is_active,
  mute.metadata as mute_metadata,
  res.id as resolution_action_id,
  res.action_type as last_resolution_action,
  res.performed_by as last_resolution_actor,
  res.note as last_resolution_note,
  res.metadata as last_resolution_metadata,
  res.created_at as last_resolution_at
from public.governance_degradation_states s
join public.workspaces w
  on w.id = s.workspace_id
left join public.watchlists wl
  on wl.id = s.watchlist_id
left join lateral (
  select ga.*
  from public.governance_acknowledgments ga
  where ga.degradation_state_id = s.id
  order by ga.acknowledged_at desc
  limit 1
) ack on true
left join lateral (
  select gm.*
  from public.governance_muting_rules gm
  where gm.workspace_id = s.workspace_id
    and gm.is_active = true
    and (gm.muted_until is null or gm.muted_until > now())
    and (
      (gm.target_type = 'degradation_state' and gm.target_key = s.id::text)
      or (gm.target_type = 'degradation_type' and gm.target_key = s.degradation_type)
      or (gm.target_type = 'version_tuple' and gm.target_key = s.version_tuple)
    )
  order by gm.created_at desc
  limit 1
) mute on true
left join lateral (
  select gr.*
  from public.governance_resolution_actions gr
  where gr.degradation_state_id = s.id
  order by gr.created_at desc
  limit 1
) res on true;

comment on view public.governance_lifecycle_summary is
  'Latest acknowledgment, muting, and resolution state attached to each chronic degradation state.';

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
select
  '0030_phase2_8_recovery_and_acknowledgment',
  'phase2_8_recovery_and_acknowledgment',
  'codex',
  jsonb_build_object(
    'tables', jsonb_build_array(
      'governance_acknowledgments',
      'governance_muting_rules',
      'governance_resolution_actions'
    ),
    'views', jsonb_build_array(
      'governance_lifecycle_summary'
    ),
    'features', jsonb_build_array(
      'acknowledgment_tracking',
      'mute_snooze_controls',
      'resolution_action_audit'
    )
  )
where not exists (
  select 1
  from public.schema_migration_ledger
  where version = '0030_phase2_8_recovery_and_acknowledgment'
);

commit;
