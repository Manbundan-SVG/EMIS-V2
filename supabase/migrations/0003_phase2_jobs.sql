create table if not exists public.job_runs (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  queue_name text not null default 'recompute',
  status text not null check (status in ('queued','claimed','running','completed','failed','dead_lettered')),
  trigger_type text not null check (trigger_type in ('api','seed','cron','manual')),
  requested_by text,
  payload jsonb not null default '{}'::jsonb,
  attempt_count integer not null default 0,
  max_attempts integer not null default 3,
  claimed_by text,
  claimed_at timestamptz,
  started_at timestamptz,
  finished_at timestamptz,
  error_message text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
create table if not exists public.job_queue (
  id bigserial primary key,
  job_id uuid not null unique references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  queue_name text not null default 'recompute',
  available_at timestamptz not null default now(),
  locked_at timestamptz,
  locked_by text,
  created_at timestamptz not null default now()
);
create table if not exists public.queue_dead_letters (
  id bigserial primary key,
  job_id uuid not null references public.job_runs(id) on delete cascade,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  error_message text,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);
create index if not exists idx_job_runs_workspace_created on public.job_runs(workspace_id, created_at desc);
create index if not exists idx_job_runs_status_created on public.job_runs(status, created_at asc);
create index if not exists idx_job_queue_available on public.job_queue(queue_name, available_at asc) where locked_at is null;
create index if not exists idx_job_queue_locked on public.job_queue(queue_name, locked_at asc) where locked_at is not null;

create or replace function public.touch_updated_at() returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_job_runs_touch on public.job_runs;
create trigger trg_job_runs_touch before update on public.job_runs for each row execute function public.touch_updated_at();

create or replace function public.enqueue_recompute_job(
  p_workspace_slug text,
  p_trigger_type text default 'api',
  p_requested_by text default null,
  p_payload jsonb default '{}'::jsonb
) returns table (job_id uuid, workspace_id uuid, status text) language plpgsql security definer as $$
declare
  v_workspace_id uuid;
  v_job_id uuid;
begin
  select id into v_workspace_id from public.workspaces where slug = p_workspace_slug;
  if v_workspace_id is null then
    raise exception 'Workspace not found for slug=%', p_workspace_slug;
  end if;
  insert into public.job_runs (workspace_id, status, trigger_type, requested_by, payload)
  values (v_workspace_id, 'queued', p_trigger_type, p_requested_by, coalesce(p_payload, '{}'::jsonb))
  returning id into v_job_id;
  insert into public.job_queue (job_id, workspace_id, queue_name) values (v_job_id, v_workspace_id, 'recompute');
  return query select v_job_id, v_workspace_id, 'queued'::text;
end;
$$;

create or replace function public.claim_recompute_job(p_worker_id text)
returns table (job_id uuid, workspace_id uuid, queue_name text, payload jsonb, attempt_count integer, max_attempts integer)
language plpgsql as $$
declare
  v_queue_row public.job_queue%rowtype;
  v_job_row public.job_runs%rowtype;
begin
  select * into v_queue_row
  from public.job_queue jq
  where jq.queue_name = 'recompute' and jq.locked_at is null and jq.available_at <= now()
  order by jq.available_at asc, jq.id asc
  for update skip locked
  limit 1;
  if not found then return; end if;
  update public.job_queue jq set locked_at = now(), locked_by = p_worker_id where jq.id = v_queue_row.id;
  update public.job_runs jr set status = 'claimed', attempt_count = jr.attempt_count + 1, claimed_by = p_worker_id, claimed_at = now()
  where jr.id = v_queue_row.job_id returning * into v_job_row;
  return query select v_job_row.id, v_job_row.workspace_id, v_job_row.queue_name, v_job_row.payload, v_job_row.attempt_count, v_job_row.max_attempts;
end;
$$;

create or replace function public.complete_recompute_job(p_job_id uuid) returns void language plpgsql as $$
begin
  update public.job_runs set status = 'completed', finished_at = now(), updated_at = now(), error_message = null where id = p_job_id;
  delete from public.job_queue where job_id = p_job_id;
end;
$$;

create or replace function public.fail_recompute_job(p_job_id uuid, p_error_message text) returns void language plpgsql as $$
declare
  v_job public.job_runs%rowtype;
begin
  select * into v_job from public.job_runs where id = p_job_id for update;
  if not found then raise exception 'Job not found: %', p_job_id; end if;
  if v_job.attempt_count >= v_job.max_attempts then
    update public.job_runs set status = 'dead_lettered', finished_at = now(), error_message = p_error_message, updated_at = now() where id = p_job_id;
    insert into public.queue_dead_letters (job_id, workspace_id, error_message, payload) values (v_job.id, v_job.workspace_id, p_error_message, v_job.payload);
    delete from public.job_queue where job_id = p_job_id;
  else
    update public.job_runs set status = 'failed', error_message = p_error_message, updated_at = now() where id = p_job_id;
    update public.job_queue set locked_at = null, locked_by = null, available_at = now() + interval '15 seconds' where job_id = p_job_id;
  end if;
end;
$$;
