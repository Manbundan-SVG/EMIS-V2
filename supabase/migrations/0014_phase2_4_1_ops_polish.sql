begin;

create or replace view public.ops_run_summary as
select
  jr.id as run_id,
  jr.workspace_id,
  jr.watchlist_id,
  jr.status,
  jr.is_replay,
  jr.replayed_from_run_id,
  jr.failure_stage,
  jr.failure_code,
  jr.runtime_ms,
  jr.started_at,
  coalesce(jr.completed_at, jr.finished_at) as completed_at,
  coalesce(jr.terminal_queue_status, jr.status) as terminal_or_run_status
from public.job_runs jr;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0014_phase2_4_1_ops_polish',
  'Add lightweight ops run summary view for Phase 2.4.1 polish',
  current_user,
  jsonb_build_object(
    'views', jsonb_build_array('ops_run_summary')
  )
)
on conflict (version) do nothing;

commit;
