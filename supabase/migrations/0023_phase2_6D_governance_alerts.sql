begin;

create table if not exists public.governance_alert_rules (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid references public.workspaces(id) on delete cascade,
  rule_name text not null,
  enabled boolean not null default true,
  event_type text not null check (event_type in (
    'version_regression',
    'replay_degradation',
    'family_instability_spike',
    'regime_conflict_persistence',
    'stability_classification_downgrade'
  )),
  metric_source text not null check (metric_source in (
    'latest_stability_summary',
    'version_health_rankings',
    'version_replay_consistency_summary',
    'version_regime_behavior_summary'
  )),
  metric_name text not null,
  comparator text not null check (comparator in ('gt', 'gte', 'lt', 'lte', 'eq', 'neq')),
  threshold_numeric double precision,
  threshold_text text,
  severity text not null check (severity in ('info', 'medium', 'high', 'critical')),
  cooldown_seconds integer not null default 1800 check (cooldown_seconds >= 0),
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.governance_alert_events (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  run_id uuid references public.job_runs(id) on delete set null,
  rule_name text not null,
  event_type text not null,
  severity text not null,
  dedupe_key text not null,
  metric_source text not null,
  metric_name text not null,
  metric_value_numeric double precision,
  metric_value_text text,
  threshold_numeric double precision,
  threshold_text text,
  compute_version text,
  signal_registry_version text,
  model_version text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create unique index if not exists governance_alert_events_dedupe_idx
  on public.governance_alert_events(dedupe_key);

create index if not exists governance_alert_events_workspace_created_idx
  on public.governance_alert_events(workspace_id, created_at desc);

create index if not exists governance_alert_events_workspace_event_idx
  on public.governance_alert_events(workspace_id, event_type, created_at desc);

create or replace view public.governance_alert_state as
select
  e.workspace_id,
  ws.slug as workspace_slug,
  e.watchlist_id,
  w.slug as watchlist_slug,
  w.name as watchlist_name,
  e.rule_name,
  e.event_type,
  e.severity,
  e.compute_version,
  e.signal_registry_version,
  e.model_version,
  max(e.created_at) as latest_triggered_at,
  count(*)::bigint as trigger_count
from public.governance_alert_events e
join public.workspaces ws
  on ws.id = e.workspace_id
left join public.watchlists w
  on w.id = e.watchlist_id
group by
  e.workspace_id,
  ws.slug,
  e.watchlist_id,
  w.slug,
  w.name,
  e.rule_name,
  e.event_type,
  e.severity,
  e.compute_version,
  e.signal_registry_version,
  e.model_version;

comment on view public.governance_alert_state is
  'Aggregated governance alert state across watchlists and version tuples.';

insert into public.governance_alert_rules (
  workspace_id,
  rule_name,
  enabled,
  event_type,
  metric_source,
  metric_name,
  comparator,
  threshold_numeric,
  severity,
  cooldown_seconds,
  metadata
)
select
  null,
  'default_version_regression',
  true,
  'version_regression',
  'version_health_rankings',
  'governance_health_score',
  'lt',
  0.90,
  'high',
  1800,
  jsonb_build_object('description', 'Version health score regressed below acceptable threshold')
where not exists (
  select 1
  from public.governance_alert_rules
  where workspace_id is null
    and rule_name = 'default_version_regression'
);

insert into public.governance_alert_rules (
  workspace_id,
  rule_name,
  enabled,
  event_type,
  metric_source,
  metric_name,
  comparator,
  threshold_numeric,
  severity,
  cooldown_seconds,
  metadata
)
select
  null,
  'default_replay_degradation',
  true,
  'replay_degradation',
  'version_replay_consistency_summary',
  'avg_input_match_score',
  'lt',
  0.98,
  'critical',
  1800,
  jsonb_build_object('description', 'Replay input consistency degraded for the version tuple')
where not exists (
  select 1
  from public.governance_alert_rules
  where workspace_id is null
    and rule_name = 'default_replay_degradation'
);

insert into public.governance_alert_rules (
  workspace_id,
  rule_name,
  enabled,
  event_type,
  metric_source,
  metric_name,
  comparator,
  threshold_numeric,
  severity,
  cooldown_seconds,
  metadata
)
select
  null,
  'default_family_instability_spike',
  true,
  'family_instability_spike',
  'latest_stability_summary',
  'family_instability_score',
  'gt',
  0.50,
  'medium',
  1800,
  jsonb_build_object('description', 'Family instability exceeded threshold on latest run')
where not exists (
  select 1
  from public.governance_alert_rules
  where workspace_id is null
    and rule_name = 'default_family_instability_spike'
);

insert into public.governance_alert_rules (
  workspace_id,
  rule_name,
  enabled,
  event_type,
  metric_source,
  metric_name,
  comparator,
  threshold_numeric,
  severity,
  cooldown_seconds,
  metadata
)
select
  null,
  'default_regime_conflict_persistence',
  true,
  'regime_conflict_persistence',
  'version_regime_behavior_summary',
  'conflicting_transition_rate',
  'gt',
  0.30,
  'high',
  1800,
  jsonb_build_object('description', 'Conflicting regime transitions persisted for the version tuple')
where not exists (
  select 1
  from public.governance_alert_rules
  where workspace_id is null
    and rule_name = 'default_regime_conflict_persistence'
);

insert into public.governance_alert_rules (
  workspace_id,
  rule_name,
  enabled,
  event_type,
  metric_source,
  metric_name,
  comparator,
  threshold_text,
  severity,
  cooldown_seconds,
  metadata
)
select
  null,
  'default_stability_classification_downgrade',
  true,
  'stability_classification_downgrade',
  'latest_stability_summary',
  'stability_classification',
  'gte',
  'unstable',
  'high',
  1800,
  jsonb_build_object('description', 'Latest stability classification degraded to unstable or worse')
where not exists (
  select 1
  from public.governance_alert_rules
  where workspace_id is null
    and rule_name = 'default_stability_classification_downgrade'
);

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0023_phase2_6D_governance_alerts',
  'Add governance alert rules, events, and state view',
  current_user,
  jsonb_build_object(
    'tables', jsonb_build_array('governance_alert_rules', 'governance_alert_events'),
    'views', jsonb_build_array('governance_alert_state'),
    'focus', jsonb_build_array(
      'stability_triggered_governance_alerts',
      'version_tuple_dedupe',
      'governance_alert_state'
    )
  )
)
on conflict (version) do nothing;

commit;
