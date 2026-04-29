begin;

create table if not exists public.governance_promotion_impact_snapshots (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  promotion_type text not null check (promotion_type in ('threshold', 'routing')),
  execution_id uuid not null,
  scope_type text not null,
  scope_value text null,
  impact_classification text not null check (
    impact_classification in ('improved', 'neutral', 'degraded', 'rollback_candidate', 'insufficient_data')
  ),
  pre_window_start timestamptz not null,
  pre_window_end timestamptz not null,
  post_window_start timestamptz not null,
  post_window_end timestamptz not null,
  recurrence_rate_before numeric null,
  recurrence_rate_after numeric null,
  escalation_rate_before numeric null,
  escalation_rate_after numeric null,
  resolution_latency_before_ms numeric null,
  resolution_latency_after_ms numeric null,
  reassignment_rate_before numeric null,
  reassignment_rate_after numeric null,
  rollback_risk_score numeric null,
  supporting_metrics jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists governance_promotion_impact_snapshot_exec_idx
  on public.governance_promotion_impact_snapshots (workspace_id, promotion_type, execution_id);

create index if not exists governance_promotion_impact_snapshot_workspace_idx
  on public.governance_promotion_impact_snapshots (workspace_id, promotion_type, created_at desc);

create or replace view public.governance_threshold_promotion_impact_summary as
select
  s.workspace_id,
  w.slug as workspace_slug,
  s.execution_id,
  s.scope_type,
  s.scope_value,
  s.impact_classification,
  s.pre_window_start,
  s.pre_window_end,
  s.post_window_start,
  s.post_window_end,
  s.recurrence_rate_before,
  s.recurrence_rate_after,
  s.escalation_rate_before,
  s.escalation_rate_after,
  s.resolution_latency_before_ms,
  s.resolution_latency_after_ms,
  s.rollback_risk_score,
  s.supporting_metrics,
  s.created_at,
  s.updated_at
from public.governance_promotion_impact_snapshots s
join public.workspaces w
  on w.id = s.workspace_id
where s.promotion_type = 'threshold';

create or replace view public.governance_routing_promotion_impact_summary as
select
  s.workspace_id,
  w.slug as workspace_slug,
  s.execution_id,
  s.scope_type,
  s.scope_value,
  s.impact_classification,
  s.pre_window_start,
  s.pre_window_end,
  s.post_window_start,
  s.post_window_end,
  s.recurrence_rate_before,
  s.recurrence_rate_after,
  s.escalation_rate_before,
  s.escalation_rate_after,
  s.resolution_latency_before_ms,
  s.resolution_latency_after_ms,
  s.reassignment_rate_before,
  s.reassignment_rate_after,
  s.rollback_risk_score,
  s.supporting_metrics,
  s.created_at,
  s.updated_at
from public.governance_promotion_impact_snapshots s
join public.workspaces w
  on w.id = s.workspace_id
where s.promotion_type = 'routing';

create or replace view public.governance_promotion_rollback_risk_summary as
select
  s.workspace_id,
  w.slug as workspace_slug,
  s.promotion_type,
  s.execution_id,
  s.scope_type,
  s.scope_value,
  s.impact_classification,
  s.rollback_risk_score,
  s.supporting_metrics,
  s.created_at,
  s.updated_at
from public.governance_promotion_impact_snapshots s
join public.workspaces w
  on w.id = s.workspace_id
where s.impact_classification in ('degraded', 'rollback_candidate')
order by s.rollback_risk_score desc nulls last, s.created_at desc;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0050',
  '0050_phase3_4C_promotion_impact',
  current_user,
  jsonb_build_object(
    'phase', '3.4C',
    'description', 'Promotion impact analysis for threshold and routing autopromotions'
  )
)
on conflict (version) do update
set
  name = excluded.name,
  applied_at = now(),
  applied_by = excluded.applied_by,
  metadata = excluded.metadata;

commit;
