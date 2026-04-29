begin;

create table if not exists public.job_run_input_snapshots (
  id bigserial primary key,
  job_run_id uuid not null unique references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  source_window_start timestamptz,
  source_window_end timestamptz,
  asset_count integer not null default 0 check (asset_count >= 0),
  source_coverage jsonb not null default '{}'::jsonb,
  input_values jsonb not null default '{}'::jsonb,
  version_pins jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.job_run_explanations (
  id bigserial primary key,
  job_run_id uuid not null unique references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  explanation_version text not null default 'phase2.4/v1',
  summary text,
  regime_summary jsonb not null default '{}'::jsonb,
  signal_summary jsonb not null default '{}'::jsonb,
  composite_summary jsonb not null default '{}'::jsonb,
  invalidator_summary jsonb not null default '{}'::jsonb,
  top_positive_contributors jsonb not null default '[]'::jsonb,
  top_negative_contributors jsonb not null default '[]'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.job_run_stage_timings (
  id bigserial primary key,
  job_run_id uuid not null references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  stage_name text not null,
  stage_status text not null check (stage_status in ('completed', 'failed', 'skipped')),
  started_at timestamptz not null,
  completed_at timestamptz,
  runtime_ms integer,
  error_summary text,
  failure_code text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (job_run_id, stage_name)
);

create index if not exists idx_job_run_stage_timings_run_id
  on public.job_run_stage_timings(job_run_id, started_at asc);

create index if not exists idx_job_run_input_snapshots_workspace_watchlist
  on public.job_run_input_snapshots(workspace_id, watchlist_id, created_at desc);

create index if not exists idx_job_run_explanations_workspace_watchlist
  on public.job_run_explanations(workspace_id, watchlist_id, created_at desc);

drop trigger if exists trg_job_run_input_snapshots_updated_at on public.job_run_input_snapshots;
create trigger trg_job_run_input_snapshots_updated_at
before update on public.job_run_input_snapshots
for each row execute function public.touch_updated_at();

drop trigger if exists trg_job_run_explanations_updated_at on public.job_run_explanations;
create trigger trg_job_run_explanations_updated_at
before update on public.job_run_explanations
for each row execute function public.touch_updated_at();

alter table public.job_runs
  add column if not exists failure_stage text,
  add column if not exists failure_code text,
  add column if not exists replayed_from_run_id uuid,
  add column if not exists is_replay boolean not null default false,
  add column if not exists input_snapshot_id bigint,
  add column if not exists explanation_version text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'job_runs_replayed_from_run_id_fkey'
      and conrelid = 'public.job_runs'::regclass
  ) then
    alter table public.job_runs
      add constraint job_runs_replayed_from_run_id_fkey
      foreign key (replayed_from_run_id)
      references public.job_runs(id)
      on delete set null;
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'job_runs_input_snapshot_id_fkey'
      and conrelid = 'public.job_runs'::regclass
  ) then
    alter table public.job_runs
      add constraint job_runs_input_snapshot_id_fkey
      foreign key (input_snapshot_id)
      references public.job_run_input_snapshots(id)
      on delete set null;
  end if;
end $$;

create index if not exists idx_job_runs_replayed_from_run_id
  on public.job_runs(replayed_from_run_id);

create index if not exists idx_job_runs_is_replay_created
  on public.job_runs(is_replay, created_at desc);

create or replace function public.jsonb_changed_entries(
  p_current jsonb,
  p_prior jsonb
)
returns jsonb
language sql
immutable
as $$
with keys as (
  select key
  from jsonb_object_keys(coalesce(p_current, '{}'::jsonb)) as key
  union
  select key
  from jsonb_object_keys(coalesce(p_prior, '{}'::jsonb)) as key
),
diff as (
  select
    key,
    coalesce(p_current -> key, 'null'::jsonb) as current_value,
    coalesce(p_prior -> key, 'null'::jsonb) as prior_value
  from keys
  where coalesce(p_current -> key, 'null'::jsonb) is distinct from coalesce(p_prior -> key, 'null'::jsonb)
)
select coalesce(
  jsonb_object_agg(
    key,
    jsonb_build_object(
      'current', current_value,
      'prior', prior_value
    )
  ),
  '{}'::jsonb
)
from diff;
$$;

create or replace function public.enqueue_replay_run(
  p_source_run_id uuid,
  p_requested_by text default null
)
returns table (
  allowed boolean,
  reason text,
  assigned_priority integer,
  job_id uuid,
  workspace_id uuid,
  queue_id bigint,
  watchlist_id uuid,
  replayed_from_run_id uuid
)
language plpgsql
security definer
as $$
declare
  v_source public.job_runs%rowtype;
  v_snapshot public.job_run_input_snapshots%rowtype;
  v_decision record;
  v_job_id uuid;
  v_queue_id bigint;
  v_payload jsonb;
  v_metadata jsonb;
begin
  select *
    into v_source
  from public.job_runs
  where id = p_source_run_id;

  if not found then
    raise exception 'source run % not found', p_source_run_id;
  end if;

  if v_source.queue_name <> 'recompute' then
    raise exception 'replay only supports recompute runs';
  end if;

  if v_source.input_snapshot_id is not null then
    select *
      into v_snapshot
    from public.job_run_input_snapshots
    where id = v_source.input_snapshot_id;
  end if;

  select *
    into v_decision
  from public.should_enqueue_recompute(
    v_source.workspace_id,
    v_source.watchlist_id,
    v_source.queue_name,
    'manual'
  );

  if not coalesce(v_decision.allowed, false) then
    return query
    select
      false,
      v_decision.reason,
      v_decision.assigned_priority,
      null::uuid,
      v_source.workspace_id,
      null::bigint,
      v_source.watchlist_id,
      v_source.id;
    return;
  end if;

  v_payload := coalesce(v_source.payload, '{}'::jsonb) || jsonb_build_object(
    'replay', jsonb_build_object(
      'source_run_id', v_source.id,
      'source_input_snapshot_id', v_source.input_snapshot_id,
      'replay_as_of_ts', v_snapshot.source_window_end,
      'pinned_versions', jsonb_build_object(
        'compute_version', v_source.compute_version,
        'signal_registry_version', v_source.signal_registry_version,
        'model_version', v_source.model_version
      )
    )
  );

  v_metadata := coalesce(v_source.metadata, '{}'::jsonb) || jsonb_build_object(
    'replayed_from_run_id', v_source.id,
    'replay_requested_by', coalesce(p_requested_by, 'ops-replay'),
    'replay_source_status', v_source.status,
    'replay_source_completed_at', coalesce(v_source.completed_at, v_source.finished_at),
    'replay_source_input_snapshot_id', v_source.input_snapshot_id,
    'governance_reason', v_decision.reason,
    'governance_requested_by', 'manual'
  );

  insert into public.job_runs (
    workspace_id,
    watchlist_id,
    queue_name,
    status,
    trigger_type,
    requested_by,
    payload,
    metadata,
    replayed_from_run_id,
    is_replay,
    compute_version,
    signal_registry_version,
    model_version,
    explanation_version
  ) values (
    v_source.workspace_id,
    v_source.watchlist_id,
    v_source.queue_name,
    'queued',
    'manual',
    coalesce(p_requested_by, 'ops-replay'),
    v_payload,
    v_metadata,
    v_source.id,
    true,
    v_source.compute_version,
    v_source.signal_registry_version,
    v_source.model_version,
    v_source.explanation_version
  )
  returning id into v_job_id;

  insert into public.job_queue (
    job_id,
    workspace_id,
    queue_name,
    priority
  ) values (
    v_job_id,
    v_source.workspace_id,
    v_source.queue_name,
    v_decision.assigned_priority
  )
  returning id into v_queue_id;

  update public.job_runs
     set queue_id = v_queue_id
   where id = v_job_id;

  return query
  select
    true,
    'enqueued'::text,
    v_decision.assigned_priority,
    v_job_id,
    v_source.workspace_id,
    v_queue_id,
    v_source.watchlist_id,
    v_source.id;
end;
$$;

create or replace view public.run_inspection as
select
  jr.id as run_id,
  jr.workspace_id,
  w.slug as workspace_slug,
  jr.watchlist_id,
  wl.slug as watchlist_slug,
  wl.name as watchlist_name,
  jr.queue_id,
  jr.queue_name,
  jr.status,
  jr.trigger_type,
  jr.requested_by,
  jr.attempt_count,
  jr.max_attempts,
  jr.claimed_by,
  jr.claimed_at,
  jr.started_at,
  coalesce(jr.completed_at, jr.finished_at) as completed_at,
  jr.runtime_ms,
  jr.compute_version,
  jr.signal_registry_version,
  jr.model_version,
  jr.lineage,
  jr.metadata,
  coalesce(jq.priority, jr.terminal_queue_priority) as priority,
  coalesce(jq.retry_count, jr.terminal_retry_count) as retry_count,
  coalesce(jq.last_error, jr.terminal_last_error) as last_error,
  coalesce(jq.created_at, jr.terminal_queued_at, jr.created_at) as queued_at,
  coalesce(a.alert_count, 0) as alert_count,
  a.last_alert_at,
  coalesce(jr.terminal_queue_status, case when jq.id is not null then 'active' end) as terminal_queue_status,
  jr.terminal_promoted_at,
  jr.failure_stage,
  jr.failure_code,
  jr.is_replay,
  jr.replayed_from_run_id,
  jr.input_snapshot_id,
  jr.explanation_version
from public.job_runs jr
join public.workspaces w on w.id = jr.workspace_id
left join public.watchlists wl on wl.id = jr.watchlist_id
left join public.job_queue jq on jq.id = jr.queue_id
left join (
  select
    job_id,
    count(*) as alert_count,
    max(created_at) as last_alert_at
  from public.alert_events
  where job_id is not null
  group by job_id
) a on a.job_id = jr.id;

create or replace view public.job_run_prior_comparison as
with run_base as (
  select
    jr.id as run_id,
    jr.workspace_id,
    jr.watchlist_id,
    jr.queue_name,
    coalesce(jr.completed_at, jr.finished_at, jr.created_at) as comparison_ts
  from public.job_runs jr
),
prior_success as (
  select
    rb.run_id,
    prior.id as prior_run_id
  from run_base rb
  left join lateral (
    select jr_prev.id
    from public.job_runs jr_prev
    where jr_prev.workspace_id = rb.workspace_id
      and (
        (jr_prev.watchlist_id is null and rb.watchlist_id is null)
        or jr_prev.watchlist_id = rb.watchlist_id
      )
      and jr_prev.queue_name = rb.queue_name
      and jr_prev.status = 'completed'
      and jr_prev.id <> rb.run_id
      and coalesce(jr_prev.completed_at, jr_prev.finished_at, jr_prev.created_at) < rb.comparison_ts
    order by coalesce(jr_prev.completed_at, jr_prev.finished_at, jr_prev.created_at) desc
    limit 1
  ) prior on true
)
select
  rb.run_id,
  ps.prior_run_id,
  rb.workspace_id,
  w.slug as workspace_slug,
  rb.watchlist_id,
  wl.slug as watchlist_slug,
  wl.name as watchlist_name,
  rb.queue_name,
  curr_exp.summary as current_summary,
  prior_exp.summary as prior_summary,
  public.jsonb_changed_entries(
    coalesce(curr_exp.regime_summary -> 'regime_by_asset', '{}'::jsonb),
    coalesce(prior_exp.regime_summary -> 'regime_by_asset', '{}'::jsonb)
  ) as regime_changes,
  public.jsonb_changed_entries(
    coalesce(curr_exp.signal_summary -> 'signals_by_asset', '{}'::jsonb),
    coalesce(prior_exp.signal_summary -> 'signals_by_asset', '{}'::jsonb)
  ) as signal_changes,
  public.jsonb_changed_entries(
    coalesce(curr_exp.composite_summary -> 'scores_by_asset', '{}'::jsonb),
    coalesce(prior_exp.composite_summary -> 'scores_by_asset', '{}'::jsonb)
  ) as composite_changes,
  public.jsonb_changed_entries(
    coalesce(curr_exp.invalidator_summary -> 'invalidators_by_asset', '{}'::jsonb),
    coalesce(prior_exp.invalidator_summary -> 'invalidators_by_asset', '{}'::jsonb)
  ) as invalidator_changes,
  public.jsonb_changed_entries(
    coalesce(curr_input.source_coverage, '{}'::jsonb),
    coalesce(prior_input.source_coverage, '{}'::jsonb)
  ) as input_coverage_changes
from run_base rb
join public.workspaces w on w.id = rb.workspace_id
left join public.watchlists wl on wl.id = rb.watchlist_id
left join prior_success ps on ps.run_id = rb.run_id
left join public.job_run_explanations curr_exp on curr_exp.job_run_id = rb.run_id
left join public.job_run_explanations prior_exp on prior_exp.job_run_id = ps.prior_run_id
left join public.job_run_input_snapshots curr_input on curr_input.job_run_id = rb.run_id
left join public.job_run_input_snapshots prior_input on prior_input.job_run_id = ps.prior_run_id;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0013_phase2_4_run_intelligence',
  'Add run stage timings, explanations, input snapshots, replay entrypoint, and prior-run comparison',
  current_user,
  jsonb_build_object(
    'tables', jsonb_build_array(
      'job_run_stage_timings',
      'job_run_explanations',
      'job_run_input_snapshots'
    ),
    'job_runs_columns', jsonb_build_array(
      'failure_stage',
      'failure_code',
      'replayed_from_run_id',
      'is_replay',
      'input_snapshot_id',
      'explanation_version'
    ),
    'views', jsonb_build_array(
      'run_inspection',
      'job_run_prior_comparison'
    ),
    'functions', jsonb_build_array(
      'jsonb_changed_entries',
      'enqueue_replay_run'
    )
  )
)
on conflict (version) do nothing;

commit;
