begin;

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
  select w.id into v_workspace_id
  from public.workspaces w
  where w.slug = p_workspace_slug;

  if v_workspace_id is null then
    raise exception 'workspace % not found', p_workspace_slug;
  end if;

  if p_watchlist_slug is not null then
    select wl.id into v_watchlist_id
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

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0009_phase2_3_enqueue_governed_watchlist_fix',
  'Fix ambiguous workspace_id reference in enqueue_governed_recompute watchlist lookup',
  current_user,
  jsonb_build_object('fix', 'qualified_watchlists.workspace_id')
)
on conflict (version) do nothing;

commit;
