create extension if not exists pgcrypto;

create table if not exists public.governance_escalation_policies (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  severity text null,
  chronicity_class text null,
  root_cause_code text null,
  min_case_age_minutes integer null,
  min_ack_age_minutes integer null,
  min_repeat_count integer null,
  min_operator_pressure numeric null,
  escalation_level text not null,
  escalate_to_team text null,
  escalate_to_user text null,
  cooldown_minutes integer not null default 240,
  is_enabled boolean not null default true,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create unique index if not exists idx_governance_escalation_policies_match
  on public.governance_escalation_policies (
    workspace_id,
    coalesce(severity, ''),
    coalesce(chronicity_class, ''),
    coalesce(root_cause_code, ''),
    escalation_level
  );

create table if not exists public.governance_escalation_state (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  case_id uuid not null unique references public.governance_cases(id) on delete cascade,
  escalation_level text not null,
  status text not null default 'active',
  escalated_to_team text null,
  escalated_to_user text null,
  reason text null,
  source_policy_id uuid null references public.governance_escalation_policies(id) on delete set null,
  escalated_at timestamptz not null default now(),
  last_evaluated_at timestamptz not null default now(),
  repeated_count integer not null default 1,
  cleared_at timestamptz null,
  metadata jsonb not null default '{}'::jsonb
);

create table if not exists public.governance_escalation_events (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  case_id uuid not null references public.governance_cases(id) on delete cascade,
  escalation_state_id uuid null references public.governance_escalation_state(id) on delete set null,
  event_type text not null,
  escalation_level text null,
  escalated_to_team text null,
  escalated_to_user text null,
  reason text null,
  source_policy_id uuid null references public.governance_escalation_policies(id) on delete set null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

insert into public.governance_escalation_policies (
  workspace_id,
  severity,
  chronicity_class,
  root_cause_code,
  min_case_age_minutes,
  min_ack_age_minutes,
  min_repeat_count,
  min_operator_pressure,
  escalation_level,
  escalate_to_team,
  escalate_to_user,
  cooldown_minutes,
  is_enabled,
  metadata
)
select *
from (
  select
    w.id as workspace_id,
    seeded.severity,
    seeded.chronicity_class,
    seeded.root_cause_code,
    seeded.min_case_age_minutes,
    seeded.min_ack_age_minutes,
    seeded.min_repeat_count,
    seeded.min_operator_pressure,
    seeded.escalation_level,
    seeded.escalate_to_team,
    seeded.escalate_to_user,
    seeded.cooldown_minutes,
    true as is_enabled,
    jsonb_build_object('seeded_by', '0039_phase3_1C_escalation_workflows') as metadata
  from public.workspaces w
  cross join (
    values
      ('critical', null::text, null::text, 30, 15, 1, null::numeric, 'lead_review', 'platform', null::text, 240),
      ('high', 'recurring', 'version_regression', 60, 30, 2, null::numeric, 'senior_review', 'research', null::text, 240),
      ('high', null::text, 'provider_failure', 45, 20, 1, null::numeric, 'platform_lead', 'platform', null::text, 180),
      ('high', null::text, null::text, 120, 60, 1, 10::numeric, 'load_shed', 'triage', null::text, 240)
  ) as seeded(
    severity,
    chronicity_class,
    root_cause_code,
    min_case_age_minutes,
    min_ack_age_minutes,
    min_repeat_count,
    min_operator_pressure,
    escalation_level,
    escalate_to_team,
    escalate_to_user,
    cooldown_minutes
  )
) defaults
on conflict do nothing;

create or replace view public.governance_escalation_summary as
select
  s.id,
  s.workspace_id,
  w.slug as workspace_slug,
  s.case_id,
  c.watchlist_id,
  wl.slug as watchlist_slug,
  c.title as case_title,
  c.status as case_status,
  c.severity,
  c.current_assignee,
  c.current_team,
  c.repeat_count,
  gsl.root_cause_code,
  s.escalation_level,
  s.status,
  s.escalated_to_team,
  s.escalated_to_user,
  s.reason,
  s.source_policy_id,
  s.escalated_at,
  s.last_evaluated_at,
  s.repeated_count,
  s.cleared_at,
  s.metadata
from public.governance_escalation_state s
join public.governance_cases c
  on c.id = s.case_id
join public.workspaces w
  on w.id = s.workspace_id
left join public.watchlists wl
  on wl.id = c.watchlist_id
left join public.governance_case_summary_latest gsl
  on gsl.case_id = s.case_id;

create index if not exists idx_governance_escalation_state_case
  on public.governance_escalation_state (case_id, status);

create index if not exists idx_governance_escalation_events_case
  on public.governance_escalation_events (case_id, created_at desc);

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0039',
  '0039_phase3_1C_escalation_workflows',
  current_user,
  jsonb_build_object('phase', '3.1C', 'feature', 'escalation_workflows')
)
on conflict (version) do nothing;
