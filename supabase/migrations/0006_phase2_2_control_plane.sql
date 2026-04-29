begin;

-- ─── Dead-letter store ────────────────────────────────────────────────────────
-- job_run_id is uuid (matches job_runs.id)
-- watchlist_id is uuid (matches watchlists.id)
-- queue_job_id is bigint (matches job_queue.id bigserial)

create table if not exists public.job_dead_letters (
  id              bigserial   primary key,
  job_run_id      uuid        references public.job_runs(id)      on delete set null,
  queue_job_id    bigint,
  workspace_id    uuid        not null references public.workspaces(id)  on delete cascade,
  watchlist_id    uuid        references public.watchlists(id)    on delete set null,
  job_type        text        not null default 'recompute',
  payload         jsonb       not null default '{}'::jsonb,
  retry_count     integer     not null default 0,
  max_retries     integer     not null default 3,
  last_error      text,
  failure_stage   text,
  failed_at       timestamptz not null default now(),
  requeued_at     timestamptz,
  metadata        jsonb       not null default '{}'::jsonb
);

create index if not exists idx_job_dead_letters_workspace_failed
  on public.job_dead_letters(workspace_id, failed_at desc);

-- ─── Worker heartbeats ────────────────────────────────────────────────────────

create table if not exists public.worker_heartbeats (
  worker_id     text        primary key,
  workspace_id  uuid        references public.workspaces(id) on delete set null,
  hostname      text,
  pid           integer,
  status        text        not null default 'alive',
  capabilities  jsonb       not null default '{}'::jsonb,
  metadata      jsonb       not null default '{}'::jsonb,
  started_at    timestamptz not null default now(),
  last_seen_at  timestamptz not null default now()
);

create index if not exists idx_worker_heartbeats_last_seen
  on public.worker_heartbeats(last_seen_at desc);

-- ─── Additive columns: job_queue ─────────────────────────────────────────────
-- max_retries: mirrors job_runs.max_attempts for the queue layer
-- next_retry_at: populated by schedule_job_retry, consumed by claim logic
-- priority: reserved for future priority queuing

alter table public.job_queue
  add column if not exists max_retries   integer     not null default 3,
  add column if not exists next_retry_at timestamptz,
  add column if not exists priority      integer     not null default 100;

-- ─── Update claim_recompute_job to expose queue_id ───────────────────────────
-- Adds queue_id (bigint) to the return so the worker can call schedule_job_retry.

create or replace function public.claim_recompute_job(p_worker_id text)
returns table (
  job_id        uuid,
  workspace_id  uuid,
  queue_name    text,
  payload       jsonb,
  attempt_count integer,
  max_attempts  integer,
  queue_id      bigint
)
language plpgsql as $$
declare
  v_queue_row public.job_queue%rowtype;
  v_job_row   public.job_runs%rowtype;
begin
  select * into v_queue_row
  from public.job_queue jq
  where jq.locked_at is null
    and jq.available_at <= now()
    and coalesce(jq.next_retry_at, now()) <= now()
    and jq.queue_name = 'recompute'
  order by jq.priority asc, jq.available_at asc, jq.id asc
  for update skip locked
  limit 1;
  if not found then return; end if;

  update public.job_queue jq
     set locked_at        = now(),
         locked_by        = p_worker_id,
         claim_expires_at = now() + interval '10 minutes'
   where jq.id = v_queue_row.id;

  update public.job_runs jr
     set status        = 'claimed',
         attempt_count = jr.attempt_count + 1,
         claimed_by    = p_worker_id,
         claimed_at    = now()
   where jr.id = v_queue_row.job_id
  returning * into v_job_row;

  return query select
    v_job_row.id, v_job_row.workspace_id, v_job_row.queue_name,
    v_job_row.payload, v_job_row.attempt_count, v_job_row.max_attempts,
    v_queue_row.id;
end;
$$;

-- ─── Worker heartbeat upsert ──────────────────────────────────────────────────

create or replace function public.heartbeat_worker(
  p_worker_id    text,
  p_workspace_id uuid    default null,
  p_hostname     text    default null,
  p_pid          integer default null,
  p_status       text    default 'alive',
  p_capabilities jsonb   default '{}'::jsonb,
  p_metadata     jsonb   default '{}'::jsonb
) returns void language plpgsql security definer as $$
begin
  insert into public.worker_heartbeats
    (worker_id, workspace_id, hostname, pid, status, capabilities, metadata, started_at, last_seen_at)
  values
    (p_worker_id, p_workspace_id, p_hostname, p_pid, p_status, p_capabilities, p_metadata, now(), now())
  on conflict (worker_id) do update set
    workspace_id = excluded.workspace_id,
    hostname     = excluded.hostname,
    pid          = excluded.pid,
    status       = excluded.status,
    capabilities = excluded.capabilities,
    metadata     = excluded.metadata,
    last_seen_at = now();
end;
$$;

-- ─── Retry scheduling with exponential backoff ────────────────────────────────
-- Works with the existing locked_at / locked_by / claim_expires_at locking model.
-- On terminal failure: inserts into job_dead_letters, updates job_runs to dead_lettered.
-- On retry: resets lock, updates available_at with backoff delay.

create or replace function public.schedule_job_retry(
  p_job_queue_id  bigint,
  p_error         text,
  p_failure_stage text default 'worker'
)
returns table (
  job_queue_id   bigint,
  action         text,
  next_retry_at  timestamptz,
  retry_count    integer
)
language plpgsql security definer as $$
declare
  v_queue  public.job_queue%rowtype;
  v_job    public.job_runs%rowtype;
  v_new_retry_count integer;
  v_delay_seconds   integer;
  v_max_retries     integer;
begin
  select * into v_queue from public.job_queue where id = p_job_queue_id for update;
  if not found then return; end if;

  select * into v_job from public.job_runs where id = v_queue.job_id;

  v_new_retry_count := coalesce(v_queue.retry_count, 0) + 1;
  v_max_retries     := coalesce(v_queue.max_retries, v_job.max_attempts, 3);

  if v_new_retry_count >= v_max_retries then
    -- ── Terminal: dead-letter ──────────────────────────────────────────────
    insert into public.job_dead_letters (
      job_run_id, queue_job_id, workspace_id, watchlist_id,
      job_type, payload, retry_count, max_retries,
      last_error, failure_stage, metadata
    ) values (
      v_job.id, v_queue.id, v_job.workspace_id, v_job.watchlist_id,
      coalesce(v_job.queue_name, 'recompute'),
      coalesce(v_job.payload, '{}'::jsonb),
      v_new_retry_count, v_max_retries,
      p_error, p_failure_stage,
      jsonb_build_object(
        'locked_by', v_queue.locked_by,
        'claimed_at', v_job.claimed_at
      )
    );

    update public.job_queue set
      locked_at        = null,
      locked_by        = null,
      claim_expires_at = null,
      retry_count      = v_new_retry_count,
      last_error       = p_error
    where id = p_job_queue_id;

    update public.job_runs set
      status        = 'dead_lettered',
      error_message = p_error,
      finished_at   = now(),
      updated_at    = now()
    where id = v_queue.job_id;

    return query select p_job_queue_id, 'dead_letter'::text, null::timestamptz, v_new_retry_count;

  else
    -- ── Retry with exponential backoff ────────────────────────────────────
    -- delay = 30 * 2^(retry-1): 30s, 60s, 120s, 240s … capped at ~4h
    v_delay_seconds := cast(power(2, least(v_new_retry_count - 1, 8)) as integer) * 30;

    update public.job_queue set
      locked_at        = null,
      locked_by        = null,
      claim_expires_at = null,
      available_at     = now() + make_interval(secs => v_delay_seconds),
      next_retry_at    = now() + make_interval(secs => v_delay_seconds),
      retry_count      = v_new_retry_count,
      last_error       = p_error
    where id = p_job_queue_id;

    update public.job_runs set
      status        = 'queued',
      error_message = p_error,
      updated_at    = now()
    where id = v_queue.job_id;

    return query select
      p_job_queue_id, 'retry'::text,
      now() + make_interval(secs => v_delay_seconds),
      v_new_retry_count;
  end if;
end;
$$;

-- ─── Requeue from dead-letter ─────────────────────────────────────────────────
-- Creates a new job_runs + job_queue pair preserving workspace/watchlist context.

create or replace function public.requeue_dead_letter(
  p_dead_letter_id   bigint,
  p_reset_retry_count boolean default false
) returns uuid  -- new job_runs.id
language plpgsql security definer as $$
declare
  v_dl         public.job_dead_letters%rowtype;
  v_new_job_id uuid;
  v_new_q_id   bigint;
begin
  select * into v_dl from public.job_dead_letters where id = p_dead_letter_id for update;
  if not found then
    raise exception 'dead letter % not found', p_dead_letter_id;
  end if;

  insert into public.job_runs (
    workspace_id, watchlist_id, queue_name, status,
    trigger_type, requested_by, payload,
    attempt_count, max_attempts, metadata
  ) values (
    v_dl.workspace_id, v_dl.watchlist_id, 'recompute', 'queued',
    'api', 'dead_letter_requeue',
    coalesce(v_dl.payload, '{}'::jsonb),
    0, v_dl.max_retries,
    coalesce(v_dl.metadata, '{}'::jsonb) ||
      jsonb_build_object('requeued_from_dead_letter_id', v_dl.id)
  ) returning id into v_new_job_id;

  insert into public.job_queue (
    job_id, workspace_id, queue_name, available_at,
    retry_count, max_retries, priority
  ) values (
    v_new_job_id, v_dl.workspace_id, 'recompute', now(),
    case when p_reset_retry_count then 0 else v_dl.retry_count end,
    v_dl.max_retries, 100
  ) returning id into v_new_q_id;

  update public.job_dead_letters set
    requeued_at = now(),
    metadata    = coalesce(metadata, '{}'::jsonb) ||
                  jsonb_build_object('new_job_id', v_new_job_id, 'new_queue_id', v_new_q_id)
  where id = p_dead_letter_id;

  return v_new_job_id;
end;
$$;

-- ─── Cron-friendly scheduled enqueue (idempotent) ────────────────────────────
-- Safe to call every N minutes from pg_cron: skips if an active job already exists.
-- Example pg_cron entry (add in Supabase dashboard):
--   select cron.schedule('emis-demo-5m','*/5 * * * *',
--     $$select public.enqueue_scheduled_recompute('demo','core')$$);

create or replace function public.enqueue_scheduled_recompute(
  p_workspace_slug text,
  p_watchlist_slug text  default null,
  p_job_type       text  default 'recompute'
) returns uuid
language plpgsql security definer as $$
declare
  v_workspace_id uuid;
  v_existing     uuid;
  v_result       record;
begin
  select id into v_workspace_id from public.workspaces where slug = p_workspace_slug;
  if v_workspace_id is null then
    raise exception 'workspace % not found', p_workspace_slug;
  end if;

  -- Idempotent: skip if already queued or claimed
  select jr.id into v_existing
  from public.job_runs jr
  join public.job_queue jq on jq.job_id = jr.id
  where jr.workspace_id = v_workspace_id
    and jr.status in ('queued', 'claimed')
  limit 1;

  if v_existing is not null then
    return v_existing;
  end if;

  select * into v_result
  from public.enqueue_recompute_job(
    p_workspace_slug, 'cron', 'scheduler',
    jsonb_build_object('source', 'scheduled', 'watchlist_slug', p_watchlist_slug)
  );
  return v_result.job_id;
end;
$$;

-- ─── Queue metric views ───────────────────────────────────────────────────────

create or replace view public.queue_depth_by_watchlist as
select
  jr.workspace_id,
  jr.watchlist_id,
  count(*) filter (where jq.locked_at is null and jq.available_at <= now()) as queued_count,
  count(*) filter (where jq.locked_at is not null)                           as claimed_count,
  count(*) filter (where jr.status = 'failed')                               as failed_count,
  count(*) filter (where jr.status = 'dead_lettered')                        as dead_letter_count,
  max(jr.created_at)                                                          as newest_job_at,
  min(jr.created_at) filter (where jq.locked_at is null)                     as oldest_queued_at
from public.job_runs jr
join public.job_queue jq on jq.job_id = jr.id
group by jr.workspace_id, jr.watchlist_id;

create or replace view public.queue_runtime_summary as
select
  workspace_id,
  watchlist_id,
  count(*)                                                                            as total_runs,
  avg(extract(epoch from (coalesce(finished_at, now()) - coalesce(started_at, created_at))))
                                                                                     as avg_runtime_seconds,
  max(finished_at)                                                                   as last_completed_at,
  count(*) filter (where status in ('failed', 'dead_lettered'))                      as failed_runs,
  count(*) filter (where status = 'completed')                                       as completed_runs
from public.job_runs
where started_at is not null
group by workspace_id, watchlist_id;

create or replace view public.stale_workers as
select
  worker_id, workspace_id, hostname, pid, status, last_seen_at,
  extract(epoch from (now() - last_seen_at)) as seconds_since_seen
from public.worker_heartbeats
where last_seen_at < now() - interval '90 seconds';

-- ─── Realtime ─────────────────────────────────────────────────────────────────

alter publication supabase_realtime add table public.job_dead_letters;
alter publication supabase_realtime add table public.worker_heartbeats;

-- ─── RLS on new tables ───────────────────────────────────────────────────────
-- Uses current_workspace_id() helper from Phase 2.1.

alter table public.job_dead_letters  enable row level security;
alter table public.worker_heartbeats enable row level security;

do $$ begin
  if not exists (select 1 from pg_policies where tablename = 'job_dead_letters' and policyname = 'dead_letters_workspace_select') then
    create policy dead_letters_workspace_select on public.job_dead_letters
      for select using (workspace_id = public.current_workspace_id());
  end if;
  if not exists (select 1 from pg_policies where tablename = 'worker_heartbeats' and policyname = 'heartbeats_workspace_select') then
    create policy heartbeats_workspace_select on public.worker_heartbeats
      for select using (workspace_id is null or workspace_id = public.current_workspace_id());
  end if;
end $$;

commit;
