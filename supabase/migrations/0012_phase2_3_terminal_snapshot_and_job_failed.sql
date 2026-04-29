begin;

alter table public.job_runs
  add column if not exists terminal_queue_priority integer,
  add column if not exists terminal_retry_count integer,
  add column if not exists terminal_last_error text,
  add column if not exists terminal_queue_status text,
  add column if not exists terminal_promoted_at timestamptz,
  add column if not exists terminal_queued_at timestamptz;

update public.alert_policy_rules
set notify_on_terminal_only = false,
    updated_at = now(),
    metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object('policy', coalesce(metadata->>'policy', 'default_job_failed_non_terminal'))
where event_type = 'job_failed'
  and notify_on_terminal_only = true;

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
           terminal_queue_priority = v_queue.priority,
           terminal_retry_count = v_new_retry_count,
           terminal_last_error = p_error,
           terminal_queue_status = 'dead_lettered',
           terminal_promoted_at = now(),
           terminal_queued_at = coalesce(terminal_queued_at, v_queue.created_at),
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
  jr.terminal_promoted_at
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

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0012_phase2_3_terminal_snapshot_and_job_failed',
  'Snapshot terminal queue evidence on job_runs and make job_failed non-terminal',
  current_user,
  jsonb_build_object(
    'job_runs_columns', jsonb_build_array(
      'terminal_queue_priority',
      'terminal_retry_count',
      'terminal_last_error',
      'terminal_queue_status',
      'terminal_promoted_at',
      'terminal_queued_at'
    ),
    'job_failed_notify_on_terminal_only', false
  )
)
on conflict (version) do nothing;

commit;
