begin;

do $$
begin
  if exists (
    select 1
    from pg_constraint
    where conrelid = 'public.governance_alert_rules'::regclass
      and conname = 'governance_alert_rules_event_type_check'
  ) then
    alter table public.governance_alert_rules
      drop constraint governance_alert_rules_event_type_check;
  end if;
end $$;

alter table public.governance_alert_rules
  add constraint governance_alert_rules_event_type_check
  check (event_type in (
    'version_regression',
    'replay_degradation',
    'family_instability_spike',
    'regime_conflict_persistence',
    'stability_classification_downgrade',
    'regime_instability_spike'
  ));

create table if not exists public.governance_threshold_profiles (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid references public.workspaces(id) on delete cascade,
  profile_name text not null,
  is_default boolean not null default false,
  enabled boolean not null default true,
  version_health_floor double precision not null default 0.90,
  family_instability_ceiling double precision not null default 0.50,
  replay_consistency_floor double precision not null default 0.98,
  regime_instability_ceiling double precision not null default 0.25,
  conflicting_transition_ceiling double precision not null default 0.30,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (workspace_id, profile_name)
);

create table if not exists public.regime_threshold_overrides (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid references public.workspaces(id) on delete cascade,
  regime text not null,
  profile_id uuid not null references public.governance_threshold_profiles(id) on delete cascade,
  enabled boolean not null default true,
  version_health_floor double precision,
  family_instability_ceiling double precision,
  replay_consistency_floor double precision,
  regime_instability_ceiling double precision,
  conflicting_transition_ceiling double precision,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (workspace_id, regime, profile_id)
);

create table if not exists public.governance_threshold_applications (
  id uuid primary key default gen_random_uuid(),
  run_id uuid references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  regime text not null default 'default',
  profile_id uuid references public.governance_threshold_profiles(id) on delete set null,
  override_id uuid references public.regime_threshold_overrides(id) on delete set null,
  evaluation_stage text not null default 'stability',
  applied_thresholds jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists governance_threshold_profiles_workspace_idx
  on public.governance_threshold_profiles(workspace_id, enabled, is_default, created_at desc);

create index if not exists regime_threshold_overrides_workspace_idx
  on public.regime_threshold_overrides(workspace_id, regime, enabled, created_at desc);

create index if not exists governance_threshold_applications_run_idx
  on public.governance_threshold_applications(run_id, created_at desc);

create index if not exists governance_threshold_applications_workspace_idx
  on public.governance_threshold_applications(workspace_id, created_at desc);

insert into public.governance_threshold_profiles (
  workspace_id,
  profile_name,
  is_default,
  enabled,
  version_health_floor,
  family_instability_ceiling,
  replay_consistency_floor,
  regime_instability_ceiling,
  conflicting_transition_ceiling,
  metadata
)
select
  null,
  'global_default',
  true,
  true,
  0.90,
  0.50,
  0.98,
  0.25,
  0.30,
  jsonb_build_object(
    'source', 'phase2_7B',
    'description', 'Global default governance thresholds aligned to the baseline 2.6D alert rules'
  )
where not exists (
  select 1
  from public.governance_threshold_profiles
  where workspace_id is null
    and profile_name = 'global_default'
);

insert into public.regime_threshold_overrides (
  workspace_id,
  regime,
  profile_id,
  enabled,
  version_health_floor,
  family_instability_ceiling,
  replay_consistency_floor,
  regime_instability_ceiling,
  conflicting_transition_ceiling,
  metadata
)
select
  null,
  'macro_dominant',
  p.id,
  true,
  0.88,
  0.62,
  0.95,
  0.40,
  0.38,
  jsonb_build_object(
    'source', 'phase2_7B',
    'description', 'Looser turbulence tolerance in macro-led environments'
  )
from public.governance_threshold_profiles p
where p.workspace_id is null
  and p.profile_name = 'global_default'
  and not exists (
    select 1
    from public.regime_threshold_overrides o
    where o.workspace_id is null
      and o.regime = 'macro_dominant'
      and o.profile_id = p.id
  );

insert into public.regime_threshold_overrides (
  workspace_id,
  regime,
  profile_id,
  enabled,
  version_health_floor,
  family_instability_ceiling,
  replay_consistency_floor,
  regime_instability_ceiling,
  conflicting_transition_ceiling,
  metadata
)
select
  null,
  'trend_persistence',
  p.id,
  true,
  0.90,
  0.45,
  0.985,
  0.20,
  0.20,
  jsonb_build_object(
    'source', 'phase2_7B',
    'description', 'Stricter instability thresholds in persistent trend regimes'
  )
from public.governance_threshold_profiles p
where p.workspace_id is null
  and p.profile_name = 'global_default'
  and not exists (
    select 1
    from public.regime_threshold_overrides o
    where o.workspace_id is null
      and o.regime = 'trend_persistence'
      and o.profile_id = p.id
  );

insert into public.regime_threshold_overrides (
  workspace_id,
  regime,
  profile_id,
  enabled,
  version_health_floor,
  family_instability_ceiling,
  replay_consistency_floor,
  regime_instability_ceiling,
  conflicting_transition_ceiling,
  metadata
)
select
  null,
  'risk_off',
  p.id,
  true,
  0.89,
  0.58,
  0.96,
  0.35,
  0.34,
  jsonb_build_object(
    'source', 'phase2_7B',
    'description', 'Allow broader family rotation during risk-off conditions'
  )
from public.governance_threshold_profiles p
where p.workspace_id is null
  and p.profile_name = 'global_default'
  and not exists (
    select 1
    from public.regime_threshold_overrides o
    where o.workspace_id is null
      and o.regime = 'risk_off'
      and o.profile_id = p.id
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
  'default_regime_instability_spike',
  true,
  'regime_instability_spike',
  'latest_stability_summary',
  'regime_instability_score',
  'gt',
  0.25,
  'medium',
  1800,
  jsonb_build_object('description', 'Latest regime instability exceeded tolerated ceiling')
where not exists (
  select 1
  from public.governance_alert_rules
  where workspace_id is null
    and rule_name = 'default_regime_instability_spike'
);

create or replace view public.active_regime_thresholds as
with candidates as (
  select
    p.workspace_id,
    ws.slug as workspace_slug,
    p.id as profile_id,
    null::uuid as override_id,
    p.profile_name,
    'default'::text as regime,
    p.version_health_floor,
    p.family_instability_ceiling,
    p.replay_consistency_floor,
    p.regime_instability_ceiling,
    p.conflicting_transition_ceiling,
    p.metadata as profile_metadata,
    '{}'::jsonb as override_metadata,
    p.is_default,
    p.created_at
  from public.governance_threshold_profiles p
  left join public.workspaces ws on ws.id = p.workspace_id
  where p.enabled = true

  union all

  select
    p.workspace_id,
    ws.slug as workspace_slug,
    p.id as profile_id,
    o.id as override_id,
    p.profile_name,
    o.regime,
    coalesce(o.version_health_floor, p.version_health_floor) as version_health_floor,
    coalesce(o.family_instability_ceiling, p.family_instability_ceiling) as family_instability_ceiling,
    coalesce(o.replay_consistency_floor, p.replay_consistency_floor) as replay_consistency_floor,
    coalesce(o.regime_instability_ceiling, p.regime_instability_ceiling) as regime_instability_ceiling,
    coalesce(o.conflicting_transition_ceiling, p.conflicting_transition_ceiling) as conflicting_transition_ceiling,
    p.metadata as profile_metadata,
    o.metadata as override_metadata,
    p.is_default,
    greatest(p.created_at, o.created_at) as created_at
  from public.governance_threshold_profiles p
  join public.regime_threshold_overrides o on o.profile_id = p.id
  left join public.workspaces ws on ws.id = p.workspace_id
  where p.enabled = true
    and o.enabled = true
),
ranked as (
  select
    *,
    row_number() over (
      partition by coalesce(workspace_id, '00000000-0000-0000-0000-000000000000'::uuid), regime
      order by is_default desc, created_at desc, profile_name asc
    ) as rn
  from candidates
)
select
  workspace_id,
  workspace_slug,
  profile_id,
  override_id,
  profile_name,
  regime,
  version_health_floor,
  family_instability_ceiling,
  replay_consistency_floor,
  regime_instability_ceiling,
  conflicting_transition_ceiling,
  profile_metadata,
  override_metadata
from ranked
where rn = 1;

create or replace view public.governance_threshold_application_summary as
select
  gta.id,
  gta.run_id,
  gta.workspace_id,
  ws.slug as workspace_slug,
  gta.watchlist_id,
  w.slug as watchlist_slug,
  w.name as watchlist_name,
  gta.regime,
  gta.profile_id,
  gtp.profile_name,
  gta.override_id,
  gta.evaluation_stage,
  gta.applied_thresholds,
  gta.metadata,
  gta.created_at
from public.governance_threshold_applications gta
join public.workspaces ws on ws.id = gta.workspace_id
left join public.watchlists w on w.id = gta.watchlist_id
left join public.governance_threshold_profiles gtp on gtp.id = gta.profile_id;

create or replace view public.macro_sync_health as
select
  msr.workspace_id,
  ws.slug as workspace_slug,
  coalesce(msr.metadata->>'provider_mode', 'unknown') as provider_mode,
  max(msr.completed_at) filter (where msr.status = 'completed') as last_completed_at,
  count(*) filter (where msr.status = 'completed')::bigint as completed_runs,
  count(*) filter (where msr.status = 'failed')::bigint as failed_runs,
  max(msr.error) filter (where msr.status = 'failed') as last_error
from public.market_data_sync_runs msr
join public.workspaces ws on ws.id = msr.workspace_id
where msr.source = 'macro_market_sync'
group by msr.workspace_id, ws.slug, coalesce(msr.metadata->>'provider_mode', 'unknown');

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0028_phase2_7B_regime_thresholds_and_macro',
  'Add regime threshold profiles, applications, and macro sync health',
  current_user,
  jsonb_build_object(
    'tables', jsonb_build_array(
      'governance_threshold_profiles',
      'regime_threshold_overrides',
      'governance_threshold_applications'
    ),
    'views', jsonb_build_array(
      'active_regime_thresholds',
      'governance_threshold_application_summary',
      'macro_sync_health'
    ),
    'focus', jsonb_build_array(
      'regime_aware_governance_thresholds',
      'macro_sync_activation',
      'threshold_application_audit'
    )
  )
)
on conflict (version) do nothing;

commit;
