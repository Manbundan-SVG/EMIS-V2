-- EMIS Phase 2.1 operational durability
-- Additive only: does NOT modify existing functions or enable RLS on existing tables.
-- Depends on: 0001_phase1_schema, 0003_phase2_jobs, 0004_phase2_schema_evolution

-- ─── Alert tables ────────────────────────────────────────────────────────────

create table if not exists public.alert_rules (
  id          uuid        primary key default gen_random_uuid(),
  workspace_id uuid        not null references public.workspaces(id) on delete cascade,
  rule_key    text        not null,
  channel     text        not null default 'in_app',
  is_enabled  boolean     not null default true,
  min_severity text       not null default 'info',
  config      jsonb       not null default '{}'::jsonb,
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now(),
  unique(workspace_id, rule_key, channel)
);

create table if not exists public.alert_events (
  id                uuid        primary key default gen_random_uuid(),
  workspace_id      uuid        not null references public.workspaces(id) on delete cascade,
  job_id            uuid        references public.job_runs(id) on delete set null,
  alert_type        text        not null,
  severity          text        not null default 'info',
  title             text        not null,
  message           text        not null,
  payload           jsonb       not null default '{}'::jsonb,
  delivered_channels jsonb      not null default '[]'::jsonb,
  created_at        timestamptz not null default now()
);

create index if not exists idx_alert_events_workspace_created
  on public.alert_events(workspace_id, created_at desc);

-- ─── Additive columns: job_queue ─────────────────────────────────────────────
-- These augment the existing Phase 2 schema without breaking anything.

alter table public.job_queue
  add column if not exists retry_count      integer     not null default 0,
  add column if not exists last_error       text,
  add column if not exists completed_at     timestamptz,
  add column if not exists claim_expires_at timestamptz;

-- ─── Additive columns: job_runs ──────────────────────────────────────────────

alter table public.job_runs
  add column if not exists watchlist_id uuid,
  add column if not exists metadata     jsonb not null default '{}'::jsonb;

-- ─── Update claim_recompute_job to stamp claim_expires_at ────────────────────
-- Re-creates the existing function in-place; only change is setting claim_expires_at.

create or replace function public.claim_recompute_job(p_worker_id text)
returns table (job_id uuid, workspace_id uuid, queue_name text, payload jsonb, attempt_count integer, max_attempts integer)
language plpgsql as $$
declare
  v_queue_row public.job_queue%rowtype;
  v_job_row   public.job_runs%rowtype;
begin
  select * into v_queue_row
  from public.job_queue jq
  where jq.queue_name = 'recompute' and jq.locked_at is null and jq.available_at <= now()
  order by jq.available_at asc, jq.id asc
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

  return query select v_job_row.id, v_job_row.workspace_id, v_job_row.queue_name,
                      v_job_row.payload, v_job_row.attempt_count, v_job_row.max_attempts;
end;
$$;

-- ─── Stale-claim reaper ───────────────────────────────────────────────────────
-- Unlocks job_queue rows whose claim window has expired, re-queuing them.

create or replace function public.reap_stale_jobs(
  p_stale_minutes          integer default 10,
  p_requeue_delay_seconds  integer default 15
)
returns integer
language plpgsql security definer as $$
declare
  v_count integer := 0;
begin
  -- Reset stale locks in job_queue back to available
  with reaped as (
    update public.job_queue jq
       set locked_at        = null,
           locked_by        = null,
           claim_expires_at = null,
           available_at     = now() + make_interval(secs => p_requeue_delay_seconds),
           retry_count      = jq.retry_count + 1,
           last_error       = coalesce(jq.last_error, 'stale claim reaped')
     where jq.locked_at is not null
       and coalesce(
             jq.claim_expires_at,
             jq.locked_at + make_interval(mins => p_stale_minutes)
           ) < now()
    returning jq.job_id
  )
  update public.job_runs jr
     set status     = 'queued',
         claimed_at = null,
         claimed_by = null,
         metadata   = coalesce(jr.metadata, '{}'::jsonb)
                      || jsonb_build_object('reaped_at', now()::text)
    from reaped r
   where jr.id = r.job_id;

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

-- ─── Alert emission on job completion / failure ───────────────────────────────

create or replace function public.emit_job_completion_alert(
  p_job_id  uuid,
  p_status  text,
  p_payload jsonb default '{}'::jsonb
)
returns uuid
language plpgsql security definer as $$
declare
  v_workspace_id uuid;
  v_alert_id     uuid;
begin
  -- Look up workspace from job_runs (job_queue row may already be deleted)
  select workspace_id into v_workspace_id from public.job_runs where id = p_job_id;
  if v_workspace_id is null then return null; end if;

  insert into public.alert_events (
    workspace_id, job_id, alert_type, severity, title, message, payload
  ) values (
    v_workspace_id,
    p_job_id,
    'job_' || p_status,
    case when p_status = 'completed' then 'info' else 'high' end,
    case when p_status = 'completed' then 'Recompute completed' else 'Recompute failed' end,
    case when p_status = 'completed'
         then 'Job finished and composite outputs were updated.'
         else 'Job failed and requires review.'
    end,
    coalesce(p_payload, '{}'::jsonb)
  ) returning id into v_alert_id;

  return v_alert_id;
end;
$$;

-- ─── RLS helper (PostgREST JWT claim) ────────────────────────────────────────

create or replace function public.current_workspace_id()
returns uuid language sql stable as $$
  select nullif(current_setting('request.jwt.claim.workspace_id', true), '')::uuid
$$;

-- ─── RLS on NEW tables only ───────────────────────────────────────────────────
-- Existing tables (job_runs, job_queue, composite_scores) are NOT touched —
-- enabling RLS on them would block the direct-psycopg worker and anon realtime.

alter table public.alert_rules  enable row level security;
alter table public.alert_events enable row level security;

do $$ begin
  if not exists (select 1 from pg_policies where tablename = 'alert_rules' and policyname = 'alert_rules_workspace_select') then
    create policy alert_rules_workspace_select on public.alert_rules
      for select using (workspace_id = public.current_workspace_id());
  end if;
  if not exists (select 1 from pg_policies where tablename = 'alert_rules' and policyname = 'alert_rules_workspace_write') then
    create policy alert_rules_workspace_write on public.alert_rules
      for all using (workspace_id = public.current_workspace_id())
      with check (workspace_id = public.current_workspace_id());
  end if;
  if not exists (select 1 from pg_policies where tablename = 'alert_events' and policyname = 'alert_events_workspace_select') then
    create policy alert_events_workspace_select on public.alert_events
      for select using (workspace_id = public.current_workspace_id());
  end if;
end $$;

-- ─── Realtime ─────────────────────────────────────────────────────────────────

alter publication supabase_realtime add table public.alert_events;
