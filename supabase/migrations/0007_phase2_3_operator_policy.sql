begin;

-- Phase 2.3: queue governance, alert policy, run inspection, SLA, lineage
-- This migration adapts the pack to the live EMIS UUID schema and existing
-- Phase 2.2 queue model rather than introducing a second queue contract.

alter table public.alert_events
  add column if not exists metadata jsonb not null default '{}'::jsonb;

alter table public.job_runs
  add column if not exists queue_id bigint,
  add column if not exists lineage jsonb not null default '{}'::jsonb,
  add column if not exists compute_version text,
  add column if not exists signal_registry_version text,
  add column if not exists model_version text,
  add column if not exists runtime_ms integer,
  add column if not exists completed_at timestamptz;

create index if not exists idx_job_runs_queue_id
  on public.job_runs(queue_id);

create index if not exists idx_job_runs_workspace_watchlist_created
  on public.job_runs(workspace_id, watchlist_id, created_at desc);

create index if not exists idx_alert_events_workspace_type_created
  on public.alert_events(workspace_id, alert_type, created_at desc);

create table if not exists public.queue_governance_rules (
  id bigserial primary key,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete cascade,
  job_type text not null default 'recompute',
  enabled boolean not null default true,
  max_concurrent integer not null default 1 check (max_concurrent >= 1),
  dedupe_window_seconds integer not null default 300 check (dedupe_window_seconds >= 0),
  suppress_if_queued boolean not null default true,
  suppress_if_claimed boolean not null default true,
  manual_priority integer not null default 50,
  scheduled_priority integer not null default 100,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique nulls not distinct (workspace_id, watchlist_id, job_type)
);

create table if not exists public.alert_policy_rules (
  id bigserial primary key,
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete cascade,
  enabled boolean not null default true,
  event_type text not null,
  severity text not null default 'info',
  channel text not null default 'in_app',
  notify_on_terminal_only boolean not null default true,
  cooldown_seconds integer not null default 300 check (cooldown_seconds >= 0),
  dedupe_key_template text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique nulls not distinct (workspace_id, watchlist_id, event_type, channel)
);

create index if not exists idx_queue_governance_rules_workspace_watchlist
  on public.queue_governance_rules(workspace_id, watchlist_id, job_type);

create index if not exists idx_alert_policy_rules_workspace_watchlist
  on public.alert_policy_rules(workspace_id, watchlist_id, event_type);

drop trigger if exists trg_queue_governance_rules_updated_at on public.queue_governance_rules;
create trigger trg_queue_governance_rules_updated_at
before update on public.queue_governance_rules
for each row execute function public.touch_updated_at();

drop trigger if exists trg_alert_policy_rules_updated_at on public.alert_policy_rules;
create trigger trg_alert_policy_rules_updated_at
before update on public.alert_policy_rules
for each row execute function public.touch_updated_at();

drop function if exists public.claim_recompute_job(text);

create or replace function public.claim_recompute_job(p_worker_id text)
returns table (
  job_id uuid,
  workspace_id uuid,
  queue_name text,
  payload jsonb,
  attempt_count integer,
  max_attempts integer,
  queue_id bigint,
  watchlist_id uuid,
  trigger_type text
)
language plpgsql as $$
declare
  v_queue_row public.job_queue%rowtype;
  v_job_row public.job_runs%rowtype;
begin
  select jq.*
    into v_queue_row
  from public.job_queue jq
  join public.job_runs jr on jr.id = jq.job_id
  where jq.locked_at is null
    and jq.available_at <= now()
    and coalesce(jq.next_retry_at, now()) <= now()
    and jq.queue_name = 'recompute'
    and jr.status = 'queued'
  order by jq.priority asc, jq.available_at asc, jq.id asc
  for update of jq skip locked
  limit 1;

  if not found then
    return;
  end if;

  update public.job_queue
     set locked_at = now(),
         locked_by = p_worker_id,
         claim_expires_at = now() + interval '10 minutes'
   where id = v_queue_row.id;

  update public.job_runs as jr
     set status = 'claimed',
         attempt_count = jr.attempt_count + 1,
         claimed_by = p_worker_id,
         claimed_at = now(),
         queue_id = coalesce(jr.queue_id, v_queue_row.id)
   where id = v_queue_row.job_id
  returning * into v_job_row;

  return query
  select
    v_job_row.id,
    v_job_row.workspace_id,
    v_job_row.queue_name,
    v_job_row.payload,
    v_job_row.attempt_count,
    v_job_row.max_attempts,
    v_queue_row.id,
    v_job_row.watchlist_id,
    v_job_row.trigger_type;
end;
$$;

create or replace function public.complete_recompute_job(p_job_id uuid)
returns void
language plpgsql as $$
declare
  v_queue_id bigint;
begin
  select id into v_queue_id
  from public.job_queue
  where job_id = p_job_id;

  update public.job_runs
     set status = 'completed',
         queue_id = coalesce(queue_id, v_queue_id),
         completed_at = coalesce(completed_at, now()),
         finished_at = now(),
         updated_at = now(),
         error_message = null
   where id = p_job_id;

  delete from public.job_queue
   where job_id = p_job_id;
end;
$$;

create or replace function public.schedule_job_retry(
  p_job_queue_id bigint,
  p_error text,
  p_failure_stage text default 'worker'
)
returns table (
  job_queue_id bigint,
  action text,
  next_retry_at timestamptz,
  retry_count integer
)
language plpgsql security definer as $$
declare
  v_queue public.job_queue%rowtype;
  v_job public.job_runs%rowtype;
  v_new_retry_count integer;
  v_delay_seconds integer;
  v_max_retries integer;
begin
  select * into v_queue
  from public.job_queue
  where id = p_job_queue_id
  for update;

  if not found then
    return;
  end if;

  select * into v_job
  from public.job_runs
  where id = v_queue.job_id;

  v_new_retry_count := coalesce(v_queue.retry_count, 0) + 1;
  v_max_retries := coalesce(v_queue.max_retries, v_job.max_attempts, 3);

  if v_new_retry_count >= v_max_retries then
    insert into public.job_dead_letters (
      job_run_id,
      queue_job_id,
      workspace_id,
      watchlist_id,
      job_type,
      payload,
      retry_count,
      max_retries,
      last_error,
      failure_stage,
      metadata
    ) values (
      v_job.id,
      v_queue.id,
      v_job.workspace_id,
      v_job.watchlist_id,
      coalesce(v_job.queue_name, 'recompute'),
      coalesce(v_job.payload, '{}'::jsonb),
      v_new_retry_count,
      v_max_retries,
      p_error,
      p_failure_stage,
      jsonb_build_object(
        'locked_by', v_queue.locked_by,
        'claimed_at', v_job.claimed_at
      )
    );

    update public.job_runs
       set status = 'dead_lettered',
           queue_id = coalesce(queue_id, v_queue.id),
           error_message = p_error,
           completed_at = coalesce(completed_at, now()),
           finished_at = coalesce(finished_at, now()),
           updated_at = now()
     where id = v_queue.job_id;

    delete from public.job_queue
     where id = p_job_queue_id;

    return query
    select p_job_queue_id, 'dead_letter'::text, null::timestamptz, v_new_retry_count;
    return;
  end if;

  v_delay_seconds := cast(power(2, least(v_new_retry_count - 1, 8)) as integer) * 30;

  update public.job_queue
     set locked_at = null,
         locked_by = null,
         claim_expires_at = null,
         available_at = now() + make_interval(secs => v_delay_seconds),
         next_retry_at = now() + make_interval(secs => v_delay_seconds),
         retry_count = v_new_retry_count,
         last_error = p_error
   where id = p_job_queue_id;

  update public.job_runs
     set status = 'queued',
         queue_id = coalesce(queue_id, v_queue.id),
         error_message = p_error,
         updated_at = now()
   where id = v_queue.job_id;

  return query
  select
    p_job_queue_id,
    'retry'::text,
    now() + make_interval(secs => v_delay_seconds),
    v_new_retry_count;
end;
$$;

create or replace function public.requeue_dead_letter(
  p_dead_letter_id bigint,
  p_reset_retry_count boolean default true
)
returns uuid
language plpgsql security definer as $$
declare
  v_dl public.job_dead_letters%rowtype;
  v_new_job_id uuid;
  v_new_queue_id bigint;
begin
  select * into v_dl
  from public.job_dead_letters
  where id = p_dead_letter_id
  for update;

  if not found then
    raise exception 'dead letter % not found', p_dead_letter_id;
  end if;

  insert into public.job_runs (
    workspace_id,
    watchlist_id,
    queue_name,
    status,
    trigger_type,
    requested_by,
    payload,
    attempt_count,
    max_attempts,
    metadata
  ) values (
    v_dl.workspace_id,
    v_dl.watchlist_id,
    'recompute',
    'queued',
    'manual',
    'dead_letter_requeue',
    coalesce(v_dl.payload, '{}'::jsonb),
    0,
    v_dl.max_retries,
    coalesce(v_dl.metadata, '{}'::jsonb) || jsonb_build_object(
      'requeued_from_dead_letter_id', v_dl.id,
      'previous_retry_count', v_dl.retry_count,
      'reset_retry_count_requested', p_reset_retry_count
    )
  )
  returning id into v_new_job_id;

  insert into public.job_queue (
    job_id,
    workspace_id,
    queue_name,
    available_at,
    retry_count,
    max_retries,
    priority
  ) values (
    v_new_job_id,
    v_dl.workspace_id,
    'recompute',
    now(),
    0,
    v_dl.max_retries,
    50
  )
  returning id into v_new_queue_id;

  update public.job_runs
     set queue_id = v_new_queue_id
   where id = v_new_job_id;

  return v_new_job_id;
end;
$$;

create or replace function public.should_enqueue_recompute(
  p_workspace_id uuid,
  p_watchlist_id uuid default null,
  p_job_type text default 'recompute',
  p_requested_by text default 'manual'
)
returns table (
  allowed boolean,
  reason text,
  assigned_priority integer,
  dedupe_window_seconds integer,
  max_concurrent integer
)
language plpgsql as $$
declare
  v_rule public.queue_governance_rules%rowtype;
  v_recent_count integer := 0;
  v_claimed_count integer := 0;
  v_priority integer := case when p_requested_by = 'scheduled' then 100 else 50 end;
begin
  select *
    into v_rule
  from public.queue_governance_rules
  where workspace_id = p_workspace_id
    and job_type = p_job_type
    and enabled = true
    and (watchlist_id = p_watchlist_id or watchlist_id is null)
  order by case when watchlist_id is null then 1 else 0 end, id desc
  limit 1;

  if not found then
    return query select true, 'no_rule'::text, v_priority, 300, 1;
    return;
  end if;

  v_priority := case when p_requested_by = 'scheduled' then v_rule.scheduled_priority else v_rule.manual_priority end;

  select count(*)
    into v_recent_count
  from public.job_runs jr
  join public.job_queue jq on jq.job_id = jr.id
  where jr.workspace_id = p_workspace_id
    and jr.queue_name = p_job_type
    and (
      (p_watchlist_id is null and jr.watchlist_id is null)
      or jr.watchlist_id = p_watchlist_id
    )
    and jr.status = 'queued'
    and jq.created_at >= now() - make_interval(secs => v_rule.dedupe_window_seconds);

  if v_rule.suppress_if_queued and v_recent_count > 0 then
    return query
    select false, 'suppressed_duplicate_queued'::text, v_priority, v_rule.dedupe_window_seconds, v_rule.max_concurrent;
    return;
  end if;

  select count(*)
    into v_claimed_count
  from public.job_runs jr
  left join public.job_queue jq on jq.job_id = jr.id
  where jr.workspace_id = p_workspace_id
    and jr.queue_name = p_job_type
    and (
      (p_watchlist_id is null and jr.watchlist_id is null)
      or jr.watchlist_id = p_watchlist_id
    )
    and jr.status in ('claimed', 'running');

  if v_rule.suppress_if_claimed and v_claimed_count >= v_rule.max_concurrent then
    return query
    select false, 'suppressed_max_concurrency'::text, v_priority, v_rule.dedupe_window_seconds, v_rule.max_concurrent;
    return;
  end if;

  return query
  select true, 'allowed'::text, v_priority, v_rule.dedupe_window_seconds, v_rule.max_concurrent;
end;
$$;

create or replace function public.enqueue_governed_recompute(
  p_workspace_slug text,
  p_watchlist_slug text default null,
  p_trigger_type text default 'api',
  p_requested_by text default null,
  p_payload jsonb default '{}'::jsonb
)
returns table (
  allowed boolean,
  reason text,
  assigned_priority integer,
  job_id uuid,
  workspace_id uuid,
  queue_id bigint,
  watchlist_id uuid
)
language plpgsql security definer as $$
declare
  v_workspace_id uuid;
  v_watchlist_id uuid;
  v_job_id uuid;
  v_queue_id bigint;
  v_decision record;
  v_requested_by_class text := case when p_trigger_type = 'cron' then 'scheduled' else 'manual' end;
begin
  select id into v_workspace_id
  from public.workspaces
  where slug = p_workspace_slug;

  if v_workspace_id is null then
    raise exception 'workspace % not found', p_workspace_slug;
  end if;

  if p_watchlist_slug is not null then
    select id into v_watchlist_id
    from public.watchlists wl
    where wl.workspace_id = v_workspace_id
      and wl.slug = p_watchlist_slug;

    if v_watchlist_id is null then
      raise exception 'watchlist % not found in workspace %', p_watchlist_slug, p_workspace_slug;
    end if;
  end if;

  select *
    into v_decision
  from public.should_enqueue_recompute(v_workspace_id, v_watchlist_id, 'recompute', v_requested_by_class);

  if not coalesce(v_decision.allowed, false) then
    return query
    select false, v_decision.reason, v_decision.assigned_priority, null::uuid, v_workspace_id, null::bigint, v_watchlist_id;
    return;
  end if;

  insert into public.job_runs (
    workspace_id,
    watchlist_id,
    queue_name,
    status,
    trigger_type,
    requested_by,
    payload,
    metadata
  ) values (
    v_workspace_id,
    v_watchlist_id,
    'recompute',
    'queued',
    p_trigger_type,
    p_requested_by,
    coalesce(p_payload, '{}'::jsonb),
    jsonb_build_object(
      'watchlist_slug', p_watchlist_slug,
      'governance_reason', v_decision.reason,
      'governance_requested_by', v_requested_by_class
    )
  )
  returning id into v_job_id;

  insert into public.job_queue (
    job_id,
    workspace_id,
    queue_name,
    priority
  ) values (
    v_job_id,
    v_workspace_id,
    'recompute',
    v_decision.assigned_priority
  )
  returning id into v_queue_id;

  update public.job_runs
     set queue_id = v_queue_id
   where id = v_job_id;

  return query
  select true, 'enqueued'::text, v_decision.assigned_priority, v_job_id, v_workspace_id, v_queue_id, v_watchlist_id;
end;
$$;

create or replace function public.enqueue_scheduled_recompute(
  p_workspace_slug text,
  p_watchlist_slug text default null,
  p_job_type text default 'recompute'
)
returns uuid
language plpgsql security definer as $$
declare
  v_result record;
begin
  select *
    into v_result
  from public.enqueue_governed_recompute(
    p_workspace_slug,
    p_watchlist_slug,
    'cron',
    'scheduler',
    jsonb_build_object('source', 'scheduled', 'job_type', p_job_type, 'watchlist_slug', p_watchlist_slug)
  );

  if not coalesce(v_result.allowed, false) then
    return null;
  end if;

  return v_result.job_id;
end;
$$;

create or replace function public.evaluate_alert_policies(
  p_workspace_id uuid,
  p_watchlist_id uuid,
  p_event_type text,
  p_severity text,
  p_job_run_id uuid,
  p_payload jsonb default '{}'::jsonb
)
returns integer
language plpgsql security definer as $$
declare
  v_rule record;
  v_inserted integer := 0;
  v_dedupe_key text;
  v_last_event_at timestamptz;
begin
  for v_rule in
    select *
    from public.alert_policy_rules
    where workspace_id = p_workspace_id
      and enabled = true
      and event_type = p_event_type
      and (
        watchlist_id = p_watchlist_id
        or watchlist_id is null
      )
    order by case when watchlist_id is null then 1 else 0 end, id desc
  loop
    if v_rule.notify_on_terminal_only and p_event_type not in ('job_completed', 'job_failed', 'job_dead_letter') then
      continue;
    end if;

    v_dedupe_key := coalesce(
      v_rule.dedupe_key_template,
      p_event_type || ':' || coalesce(p_watchlist_id::text, 'all') || ':' || p_severity || ':' || v_rule.channel
    );

    select max(created_at)
      into v_last_event_at
    from public.alert_events
    where workspace_id = p_workspace_id
      and alert_type = p_event_type
      and coalesce(metadata ->> 'dedupe_key', '') = v_dedupe_key;

    if v_last_event_at is not null
       and v_last_event_at > now() - make_interval(secs => v_rule.cooldown_seconds) then
      continue;
    end if;

    insert into public.alert_events (
      workspace_id,
      job_id,
      alert_type,
      severity,
      title,
      message,
      payload,
      metadata
    ) values (
      p_workspace_id,
      p_job_run_id,
      p_event_type,
      v_rule.severity,
      initcap(replace(p_event_type, '_', ' ')),
      coalesce(p_payload ->> 'message', 'Policy-matched event'),
      coalesce(p_payload, '{}'::jsonb),
      jsonb_build_object(
        'watchlist_id', p_watchlist_id,
        'channel', v_rule.channel,
        'dedupe_key', v_dedupe_key,
        'policy_rule_id', v_rule.id,
        'requested_severity', p_severity
      )
    );

    v_inserted := v_inserted + 1;
  end loop;

  return v_inserted;
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
  jq.priority,
  jq.retry_count,
  jq.last_error,
  jq.created_at as queued_at,
  coalesce(a.alert_count, 0) as alert_count,
  a.last_alert_at
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

create or replace view public.watchlist_sla_summary as
select
  jr.workspace_id,
  w.slug as workspace_slug,
  jr.watchlist_id,
  wl.slug as watchlist_slug,
  wl.name as watchlist_name,
  count(*) filter (
    where jr.status = 'completed'
      and jr.created_at >= now() - interval '24 hours'
  ) as completed_24h,
  count(*) filter (
    where jr.status in ('failed', 'dead_lettered')
      and jr.created_at >= now() - interval '24 hours'
  ) as failed_24h,
  max(coalesce(jr.completed_at, jr.finished_at)) filter (where jr.status = 'completed') as last_success_at,
  extract(epoch from (
    now() - max(coalesce(jr.completed_at, jr.finished_at)) filter (where jr.status = 'completed')
  ))::bigint as seconds_since_last_success,
  avg(jr.runtime_ms) filter (
    where jr.runtime_ms is not null
      and jr.created_at >= now() - interval '24 hours'
  )::integer as avg_runtime_ms_24h
from public.job_runs jr
join public.workspaces w on w.id = jr.workspace_id
left join public.watchlists wl on wl.id = jr.watchlist_id
group by jr.workspace_id, w.slug, jr.watchlist_id, wl.slug, wl.name;

create or replace view public.queue_governance_state as
select
  jr.workspace_id,
  w.slug as workspace_slug,
  jr.watchlist_id,
  wl.slug as watchlist_slug,
  wl.name as watchlist_name,
  jr.queue_name as job_type,
  count(*) filter (where jr.status = 'queued') as queued_count,
  count(*) filter (where jr.status in ('claimed', 'running')) as claimed_count,
  min(jq.created_at) filter (where jr.status = 'queued') as oldest_queued_at,
  min(jq.priority) filter (where jr.status = 'queued') as highest_priority_queued
from public.job_runs jr
join public.workspaces w on w.id = jr.workspace_id
left join public.watchlists wl on wl.id = jr.watchlist_id
left join public.job_queue jq on jq.job_id = jr.id
where jr.status in ('queued', 'claimed', 'running')
group by jr.workspace_id, w.slug, jr.watchlist_id, wl.slug, wl.name, jr.queue_name;

do $$
begin
  if not exists (
    select 1
    from pg_publication_rel pr
    join pg_publication p on p.oid = pr.prpubid
    join pg_class c on c.oid = pr.prrelid
    join pg_namespace n on n.oid = c.relnamespace
    where p.pubname = 'supabase_realtime'
      and n.nspname = 'public'
      and c.relname = 'queue_governance_rules'
  ) then
    alter publication supabase_realtime add table public.queue_governance_rules;
  end if;

  if not exists (
    select 1
    from pg_publication_rel pr
    join pg_publication p on p.oid = pr.prpubid
    join pg_class c on c.oid = pr.prrelid
    join pg_namespace n on n.oid = c.relnamespace
    where p.pubname = 'supabase_realtime'
      and n.nspname = 'public'
      and c.relname = 'alert_policy_rules'
  ) then
    alter publication supabase_realtime add table public.alert_policy_rules;
  end if;
end $$;

alter table public.queue_governance_rules enable row level security;
alter table public.alert_policy_rules enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where tablename = 'queue_governance_rules'
      and policyname = 'queue_governance_rules_workspace_select'
  ) then
    create policy queue_governance_rules_workspace_select
      on public.queue_governance_rules
      for select
      using (workspace_id = public.current_workspace_id());
  end if;

  if not exists (
    select 1
    from pg_policies
    where tablename = 'alert_policy_rules'
      and policyname = 'alert_policy_rules_workspace_select'
  ) then
    create policy alert_policy_rules_workspace_select
      on public.alert_policy_rules
      for select
      using (workspace_id = public.current_workspace_id());
  end if;
end $$;

commit;
