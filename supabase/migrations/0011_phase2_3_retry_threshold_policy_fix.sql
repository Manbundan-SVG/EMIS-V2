begin;

update public.alert_policy_rules
set notify_on_terminal_only = false,
    updated_at = now()
where event_type = 'job_retry_threshold';

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0011_phase2_3_retry_threshold_policy_fix',
  'Allow job_retry_threshold alert policies to evaluate on retry edges',
  current_user,
  jsonb_build_object('event_type', 'job_retry_threshold', 'notify_on_terminal_only', false)
)
on conflict (version) do nothing;

commit;
