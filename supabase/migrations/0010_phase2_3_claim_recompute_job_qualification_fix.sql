begin;

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
   where jr.id = v_queue_row.job_id
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

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0010_phase2_3_claim_recompute_job_qualification_fix',
  'Qualify claim_recompute_job update fields to avoid OUT-parameter ambiguity',
  current_user,
  jsonb_build_object('fix', 'qualify job_runs.attempt_count and job_runs.queue_id')
)
on conflict (version) do nothing;

commit;
