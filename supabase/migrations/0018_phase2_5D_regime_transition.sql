begin;

create table if not exists public.regime_transition_events (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null unique references public.job_runs(id) on delete cascade,
  prior_run_id uuid references public.job_runs(id) on delete set null,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  queue_name text not null default 'recompute',
  from_regime text,
  to_regime text,
  transition_detected boolean not null default false,
  transition_classification text not null default 'none',
  stability_score double precision,
  anomaly_likelihood double precision,
  composite_shift double precision,
  composite_shift_abs double precision,
  dominant_family_gained text,
  dominant_family_lost text,
  source text not null default 'worker',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.regime_transition_family_shifts (
  id uuid primary key default gen_random_uuid(),
  transition_event_id uuid not null references public.regime_transition_events(id) on delete cascade,
  run_id uuid not null references public.job_runs(id) on delete cascade,
  prior_run_id uuid references public.job_runs(id) on delete set null,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  signal_family text not null,
  prior_family_score double precision,
  current_family_score double precision,
  family_delta double precision,
  family_delta_abs double precision,
  prior_family_rank integer,
  current_family_rank integer,
  shift_direction text not null default 'unchanged',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_regime_transition_events_workspace_watchlist
  on public.regime_transition_events (workspace_id, watchlist_id, created_at desc);

create index if not exists idx_regime_transition_events_prior_run
  on public.regime_transition_events (prior_run_id);

create index if not exists idx_regime_transition_family_shifts_transition
  on public.regime_transition_family_shifts (transition_event_id, family_delta_abs desc);

create index if not exists idx_regime_transition_family_shifts_run
  on public.regime_transition_family_shifts (run_id, family_delta_abs desc);

create or replace view public.run_regime_stability_summary as
select
  jr.id as run_id,
  jr.workspace_id,
  jr.watchlist_id,
  jr.queue_name,
  jr.status,
  jr.is_replay,
  jr.replayed_from_run_id,
  jr.started_at,
  jr.completed_at,
  rte.prior_run_id,
  rte.from_regime,
  rte.to_regime,
  rte.transition_detected,
  rte.transition_classification,
  rte.stability_score,
  rte.anomaly_likelihood,
  rte.composite_shift,
  rte.composite_shift_abs,
  rte.dominant_family_gained,
  rte.dominant_family_lost,
  rte.metadata,
  rte.created_at,
  rte.updated_at
from public.job_runs jr
left join public.regime_transition_events rte on rte.run_id = jr.id;

create or replace function public.upsert_regime_transition_event(
  p_run_id uuid,
  p_prior_run_id uuid,
  p_workspace_id uuid,
  p_watchlist_id uuid,
  p_queue_name text,
  p_from_regime text,
  p_to_regime text,
  p_transition_detected boolean,
  p_transition_classification text,
  p_stability_score double precision,
  p_anomaly_likelihood double precision,
  p_composite_shift double precision,
  p_composite_shift_abs double precision,
  p_dominant_family_gained text,
  p_dominant_family_lost text,
  p_metadata jsonb default '{}'::jsonb
) returns uuid
language plpgsql
security definer
as $$
declare
  v_id uuid;
begin
  insert into public.regime_transition_events (
    run_id,
    prior_run_id,
    workspace_id,
    watchlist_id,
    queue_name,
    from_regime,
    to_regime,
    transition_detected,
    transition_classification,
    stability_score,
    anomaly_likelihood,
    composite_shift,
    composite_shift_abs,
    dominant_family_gained,
    dominant_family_lost,
    metadata
  ) values (
    p_run_id,
    p_prior_run_id,
    p_workspace_id,
    p_watchlist_id,
    coalesce(p_queue_name, 'recompute'),
    p_from_regime,
    p_to_regime,
    coalesce(p_transition_detected, false),
    coalesce(p_transition_classification, 'none'),
    p_stability_score,
    p_anomaly_likelihood,
    p_composite_shift,
    p_composite_shift_abs,
    p_dominant_family_gained,
    p_dominant_family_lost,
    coalesce(p_metadata, '{}'::jsonb)
  )
  on conflict (run_id) do update
  set prior_run_id = excluded.prior_run_id,
      from_regime = excluded.from_regime,
      to_regime = excluded.to_regime,
      transition_detected = excluded.transition_detected,
      transition_classification = excluded.transition_classification,
      stability_score = excluded.stability_score,
      anomaly_likelihood = excluded.anomaly_likelihood,
      composite_shift = excluded.composite_shift,
      composite_shift_abs = excluded.composite_shift_abs,
      dominant_family_gained = excluded.dominant_family_gained,
      dominant_family_lost = excluded.dominant_family_lost,
      metadata = excluded.metadata,
      updated_at = now()
  returning id into v_id;

  return v_id;
end;
$$;

create or replace function public.replace_regime_transition_family_shifts(
  p_transition_event_id uuid,
  p_run_id uuid,
  p_prior_run_id uuid,
  p_workspace_id uuid,
  p_watchlist_id uuid,
  p_rows jsonb
) returns void
language plpgsql
security definer
as $$
declare
  v_row jsonb;
begin
  delete from public.regime_transition_family_shifts
  where transition_event_id = p_transition_event_id;

  for v_row in
    select * from jsonb_array_elements(coalesce(p_rows, '[]'::jsonb))
  loop
    insert into public.regime_transition_family_shifts (
      transition_event_id,
      run_id,
      prior_run_id,
      workspace_id,
      watchlist_id,
      signal_family,
      prior_family_score,
      current_family_score,
      family_delta,
      family_delta_abs,
      prior_family_rank,
      current_family_rank,
      shift_direction,
      metadata
    ) values (
      p_transition_event_id,
      p_run_id,
      p_prior_run_id,
      p_workspace_id,
      p_watchlist_id,
      v_row->>'signal_family',
      nullif(v_row->>'prior_family_score', '')::double precision,
      nullif(v_row->>'current_family_score', '')::double precision,
      nullif(v_row->>'family_delta', '')::double precision,
      nullif(v_row->>'family_delta_abs', '')::double precision,
      nullif(v_row->>'prior_family_rank', '')::integer,
      nullif(v_row->>'current_family_rank', '')::integer,
      coalesce(v_row->>'shift_direction', 'unchanged'),
      coalesce(v_row->'metadata', '{}'::jsonb)
    );
  end loop;
end;
$$;

comment on view public.run_regime_stability_summary is
  'Run-level regime transition summary, including replay suppression and family redistribution diagnostics.';

do $$
begin
  begin
    alter publication supabase_realtime add table public.regime_transition_events;
  exception
    when duplicate_object then null;
    when undefined_object then null;
  end;

  begin
    alter publication supabase_realtime add table public.regime_transition_family_shifts;
  exception
    when duplicate_object then null;
    when undefined_object then null;
  end;
end;
$$;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0018_phase2_5D_regime_transition',
  'Add regime transition events, family shifts, and run stability summary',
  current_user,
  jsonb_build_object(
    'tables', jsonb_build_array('regime_transition_events', 'regime_transition_family_shifts'),
    'views', jsonb_build_array('run_regime_stability_summary'),
    'focus', jsonb_build_array('regime_transition', 'family_redistribution', 'replay_suppression')
  )
)
on conflict (version) do nothing;

commit;
