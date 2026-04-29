-- Phase 3.4A: incident analytics foundation

create table if not exists public.governance_incident_analytics_snapshots (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null,
  snapshot_date date not null default current_date,
  open_case_count integer not null default 0,
  acknowledged_case_count integer not null default 0,
  resolved_case_count integer not null default 0,
  reopened_case_count integer not null default 0,
  recurring_case_count integer not null default 0,
  escalated_case_count integer not null default 0,
  high_severity_open_count integer not null default 0,
  stale_case_count integer not null default 0,
  mean_ack_hours numeric null,
  mean_resolve_hours numeric null,
  created_at timestamptz not null default now()
);

create unique index if not exists governance_incident_analytics_snapshots_workspace_date_idx
  on public.governance_incident_analytics_snapshots(workspace_id, snapshot_date);

create or replace view public.governance_incident_analytics_summary as
with case_base as (
  select
    c.workspace_id,
    c.id as case_id,
    c.status,
    c.severity,
    c.repeat_count,
    c.reopened_from_case_id,
    c.opened_at,
    c.acknowledged_at,
    coalesce(c.closed_at, c.resolved_at) as closed_or_resolved_at,
    case
      when c.acknowledged_at is not null and c.opened_at is not null then
        extract(epoch from (c.acknowledged_at - c.opened_at)) / 3600.0
      else null
    end as ack_hours,
    case
      when coalesce(c.closed_at, c.resolved_at) is not null and c.opened_at is not null then
        extract(epoch from (coalesce(c.closed_at, c.resolved_at) - c.opened_at)) / 3600.0
      else null
    end as resolve_hours
  from public.governance_cases c
)
select
  workspace_id,
  count(*) filter (where status in ('open', 'acknowledged', 'in_progress'))::integer as open_case_count,
  count(*) filter (
    where acknowledged_at is not null
      and status in ('open', 'acknowledged', 'in_progress')
  )::integer as acknowledged_case_count,
  count(*) filter (where status in ('resolved', 'closed'))::integer as resolved_case_count,
  count(*) filter (where reopened_from_case_id is not null)::integer as reopened_case_count,
  count(*) filter (where repeat_count > 1)::integer as recurring_case_count,
  count(*) filter (
    where severity in ('high', 'critical')
      and status in ('open', 'acknowledged', 'in_progress')
  )::integer as high_severity_open_count,
  round(avg(ack_hours)::numeric, 4) as mean_ack_hours,
  round(avg(resolve_hours)::numeric, 4) as mean_resolve_hours
from case_base
group by workspace_id;

create or replace view public.governance_root_cause_trend_summary as
select
  c.workspace_id,
  coalesce(s.root_cause_code, 'unknown') as root_cause_code,
  count(*)::integer as case_count,
  count(*) filter (where c.reopened_from_case_id is not null)::integer as reopened_count,
  count(*) filter (where c.repeat_count > 1)::integer as recurring_count,
  count(*) filter (where c.severity in ('high', 'critical'))::integer as severe_count,
  round(
    avg(
      extract(
        epoch from (coalesce(c.closed_at, c.resolved_at, now()) - c.opened_at)
      ) / 3600.0
    )::numeric,
    4
  ) as avg_case_age_hours
from public.governance_cases c
left join public.governance_case_summary_latest s
  on s.case_id = c.id
group by c.workspace_id, coalesce(s.root_cause_code, 'unknown');

create or replace view public.governance_recurrence_burden_summary as
select
  workspace_id,
  watchlist_id,
  count(*) filter (where repeat_count > 1)::integer as recurring_case_count,
  max(repeat_count)::integer as max_repeat_count,
  count(distinct recurrence_group_id)::integer as recurrence_group_count,
  count(*) filter (where reopened_from_case_id is not null)::integer as reopened_case_count
from public.governance_cases
group by workspace_id, watchlist_id;

create or replace view public.governance_escalation_effectiveness_summary as
with escalation_counts as (
  select
    c.workspace_id,
    count(*) filter (
      where es.escalation_level is not null
        and coalesce(es.status, 'active') <> 'cleared'
    )::integer as escalated_case_count,
    count(*) filter (
      where es.escalation_level is not null
        and coalesce(es.status, 'active') <> 'cleared'
        and c.status in ('resolved', 'closed')
    )::integer as escalated_resolved_count,
    count(*) filter (
      where es.escalation_level is not null
        and coalesce(es.status, 'active') <> 'cleared'
        and c.reopened_from_case_id is not null
    )::integer as escalated_reopened_count
  from public.governance_cases c
  left join public.governance_escalation_state es
    on es.case_id = c.id
  group by c.workspace_id
)
select
  workspace_id,
  escalated_case_count,
  escalated_resolved_count,
  escalated_reopened_count,
  case
    when escalated_case_count = 0 then 0::numeric
    else round((escalated_resolved_count::numeric / escalated_case_count::numeric), 4)
  end as escalation_resolution_rate,
  case
    when escalated_case_count = 0 then 0::numeric
    else round((escalated_reopened_count::numeric / escalated_case_count::numeric), 4)
  end as escalation_reopen_rate
from escalation_counts;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0048',
  '0048_phase3_4A_incident_analytics',
  current_user,
  jsonb_build_object('phase', '3.4A', 'feature', 'incident_analytics_foundation')
)
on conflict (version) do nothing;
