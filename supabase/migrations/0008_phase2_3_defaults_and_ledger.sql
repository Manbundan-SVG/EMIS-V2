begin;

create table if not exists public.schema_migration_ledger (
  version text primary key,
  name text not null,
  applied_at timestamptz not null default now(),
  applied_by text,
  metadata jsonb not null default '{}'::jsonb
);

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0007_phase2_3_operator_policy',
  'Phase 2.3 operator policy and control-plane upgrade',
  current_user,
  jsonb_build_object('source', 'backfill', 'notes', 'Applied before ledger was introduced')
)
on conflict (version) do nothing;

insert into public.queue_governance_rules (
  workspace_id,
  watchlist_id,
  job_type,
  enabled,
  max_concurrent,
  dedupe_window_seconds,
  suppress_if_queued,
  suppress_if_claimed,
  manual_priority,
  scheduled_priority
)
select
  w.id,
  null,
  'recompute',
  true,
  1,
  120,
  true,
  true,
  25,
  100
from public.workspaces w
on conflict (workspace_id, watchlist_id, job_type) do update
set enabled = excluded.enabled,
    max_concurrent = excluded.max_concurrent,
    dedupe_window_seconds = excluded.dedupe_window_seconds,
    suppress_if_queued = excluded.suppress_if_queued,
    suppress_if_claimed = excluded.suppress_if_claimed,
    manual_priority = excluded.manual_priority,
    scheduled_priority = excluded.scheduled_priority,
    updated_at = now();

insert into public.alert_policy_rules (
  workspace_id,
  watchlist_id,
  enabled,
  event_type,
  severity,
  channel,
  notify_on_terminal_only,
  cooldown_seconds,
  dedupe_key_template,
  metadata
)
select
  w.id,
  null,
  seed.enabled,
  seed.event_type,
  seed.severity,
  'in_app',
  seed.notify_on_terminal_only,
  seed.cooldown_seconds,
  seed.dedupe_key_template,
  seed.metadata
from public.workspaces w
cross join (
  values
    (
      true,
      'job_dead_letter',
      'critical',
      true,
      1800,
      'job_dead_letter:all:in_app',
      jsonb_build_object('policy', 'default_dead_letter')
    ),
    (
      true,
      'job_retry_threshold',
      'medium',
      false,
      1800,
      'job_retry_threshold:all:in_app',
      jsonb_build_object('policy', 'default_retry_threshold', 'retry_threshold', 2)
    ),
    (
      true,
      'job_completed',
      'info',
      true,
      86400,
      'job_completed:all:in_app',
      jsonb_build_object('policy', 'default_success_cooldown')
    ),
    (
      true,
      'job_failed',
      'high',
      false,
      1800,
      'job_failed:all:in_app',
      jsonb_build_object('policy', 'future_terminal_failure')
    )
) as seed(enabled, event_type, severity, notify_on_terminal_only, cooldown_seconds, dedupe_key_template, metadata)
on conflict (workspace_id, watchlist_id, event_type, channel) do update
set enabled = excluded.enabled,
    severity = excluded.severity,
    notify_on_terminal_only = excluded.notify_on_terminal_only,
    cooldown_seconds = excluded.cooldown_seconds,
    dedupe_key_template = excluded.dedupe_key_template,
    metadata = excluded.metadata,
    updated_at = now();

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0008_phase2_3_defaults_and_ledger',
  'Seed default queue governance and alert policy rules; add migration ledger',
  current_user,
  jsonb_build_object(
    'queue_governance_defaults', jsonb_build_object(
      'job_type', 'recompute',
      'dedupe_window_seconds', 120,
      'max_concurrent', 1,
      'manual_priority', 25,
      'scheduled_priority', 100
    ),
    'alert_policy_defaults', jsonb_build_array(
      'job_dead_letter',
      'job_retry_threshold',
      'job_completed',
      'job_failed'
    )
  )
)
on conflict (version) do nothing;

commit;
