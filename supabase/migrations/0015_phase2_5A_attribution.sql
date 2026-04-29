begin;

create table if not exists public.job_run_attributions (
  id bigserial primary key,
  run_id uuid not null references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  asset_id uuid references public.assets(id) on delete set null,
  asset_symbol text,
  regime text,
  signal_name text not null,
  signal_family text not null,
  raw_value double precision,
  normalized_value double precision,
  weight_applied double precision not null default 0,
  contribution_value double precision not null default 0,
  contribution_direction text not null default 'neutral',
  is_invalidator boolean not null default false,
  active_invalidators jsonb not null default '[]'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (run_id, asset_id, signal_name)
);

create index if not exists idx_job_run_attributions_run_id
  on public.job_run_attributions(run_id, asset_symbol asc, signal_name asc);

create index if not exists idx_job_run_attributions_family
  on public.job_run_attributions(run_id, signal_family, contribution_value desc);

create table if not exists public.job_run_signal_family_attributions (
  id bigserial primary key,
  run_id uuid not null references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  signal_family text not null,
  family_rank integer not null,
  family_weight double precision not null default 0,
  family_score double precision not null default 0,
  family_pct_of_total double precision not null default 0,
  positive_contribution double precision not null default 0,
  negative_contribution double precision not null default 0,
  invalidator_contribution double precision not null default 0,
  active_invalidators jsonb not null default '[]'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (run_id, signal_family)
);

create index if not exists idx_job_run_signal_family_attributions_run_id
  on public.job_run_signal_family_attributions(run_id, family_rank asc);

alter table public.job_runs
  add column if not exists attribution_version text,
  add column if not exists attribution_reconciled boolean not null default false,
  add column if not exists attribution_total double precision,
  add column if not exists attribution_target_total double precision,
  add column if not exists attribution_reconciliation_delta double precision;

create or replace view public.run_attribution_summary as
select
  jr.id as run_id,
  jr.workspace_id,
  jr.watchlist_id,
  jr.status,
  jr.compute_version,
  jr.signal_registry_version,
  jr.model_version,
  jr.attribution_version,
  jr.attribution_reconciled,
  jr.attribution_total,
  jr.attribution_target_total,
  jr.attribution_reconciliation_delta,
  coalesce(
    (
      select jsonb_agg(
        jsonb_build_object(
          'signal_family', sfa.signal_family,
          'family_rank', sfa.family_rank,
          'family_weight', sfa.family_weight,
          'family_score', sfa.family_score,
          'family_pct_of_total', sfa.family_pct_of_total,
          'positive_contribution', sfa.positive_contribution,
          'negative_contribution', sfa.negative_contribution,
          'invalidator_contribution', sfa.invalidator_contribution,
          'active_invalidators', sfa.active_invalidators,
          'metadata', sfa.metadata
        )
        order by sfa.family_rank asc
      )
      from public.job_run_signal_family_attributions sfa
      where sfa.run_id = jr.id
    ),
    '[]'::jsonb
  ) as family_attributions,
  coalesce(
    (
      select jsonb_agg(
        jsonb_build_object(
          'asset_id', a.asset_id,
          'asset_symbol', a.asset_symbol,
          'regime', a.regime,
          'signal_name', a.signal_name,
          'signal_family', a.signal_family,
          'raw_value', a.raw_value,
          'normalized_value', a.normalized_value,
          'weight_applied', a.weight_applied,
          'contribution_value', a.contribution_value,
          'contribution_direction', a.contribution_direction,
          'is_invalidator', a.is_invalidator,
          'active_invalidators', a.active_invalidators,
          'metadata', a.metadata
        )
        order by abs(a.contribution_value) desc, a.asset_symbol asc, a.signal_name asc
      )
      from public.job_run_attributions a
      where a.run_id = jr.id
    ),
    '[]'::jsonb
  ) as signal_attributions
from public.job_runs jr;

do $$
begin
  begin
    alter publication supabase_realtime add table public.job_run_attributions;
  exception
    when duplicate_object then null;
    when undefined_object then null;
  end;

  begin
    alter publication supabase_realtime add table public.job_run_signal_family_attributions;
  exception
    when duplicate_object then null;
    when undefined_object then null;
  end;
end $$;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0015_phase2_5A_attribution',
  'Add run attribution persistence, reconciliation, and summary view',
  current_user,
  jsonb_build_object(
    'tables', jsonb_build_array(
      'job_run_attributions',
      'job_run_signal_family_attributions'
    ),
    'job_runs_columns', jsonb_build_array(
      'attribution_version',
      'attribution_reconciled',
      'attribution_total',
      'attribution_target_total',
      'attribution_reconciliation_delta'
    ),
    'views', jsonb_build_array('run_attribution_summary')
  )
)
on conflict (version) do nothing;

commit;
