-- Phase 3.5A — Adaptive Routing Optimization (advisory only, no live policy changes)

-- ── 1. Persisted snapshot ────────────────────────────────────────────────────

create table if not exists public.governance_routing_optimization_snapshots (
  id                   uuid        primary key default gen_random_uuid(),
  workspace_id         uuid        not null,
  snapshot_at          timestamptz not null default now(),
  window_label         text        not null default '30d',
  recommendation_count integer     not null default 0,
  metadata             jsonb       not null default '{}'::jsonb
);

create index if not exists governance_routing_optimization_snapshots_ws_at
  on public.governance_routing_optimization_snapshots (workspace_id, snapshot_at desc);

-- ── 2. Durable recommendation artifacts ──────────────────────────────────────

create table if not exists public.governance_routing_policy_recommendations (
  id                    uuid        primary key default gen_random_uuid(),
  workspace_id          uuid        not null,
  recommendation_key    text        not null,
  scope_type            text        not null,
  scope_value           text        not null,
  current_policy        jsonb       not null default '{}'::jsonb,
  recommended_policy    jsonb       not null default '{}'::jsonb,
  reason_code           text        not null,
  confidence            text        not null check (confidence in ('low','medium','high')),
  sample_size           integer     not null default 0,
  expected_benefit_score numeric    not null default 0,
  risk_score            numeric     not null default 0,
  supporting_metrics    jsonb       not null default '{}'::jsonb,
  created_at            timestamptz not null default now(),
  unique (workspace_id, recommendation_key)
);

create index if not exists governance_routing_policy_recommendations_ws_benefit
  on public.governance_routing_policy_recommendations (workspace_id, expected_benefit_score desc);

-- ── 3. Feature effectiveness summary view ────────────────────────────────────
-- Aggregates routing outcomes by 5 feature dimensions using UNION ALL.
-- effectiveness_score = resolution_share*0.40 - reassign*0.25 - reopen*0.25 - escalate*0.10

create or replace view public.governance_routing_feature_effectiveness_summary as
with rec_acceptance as (
  select
    r.workspace_id,
    r.case_id,
    bool_or(r.accepted = true)               as any_accepted,
    bool_or(r.override_reason is not null)   as any_overridden
  from public.governance_routing_recommendations r
  group by r.workspace_id, r.case_id
),
outcomes_enriched as (
  select
    o.workspace_id,
    o.case_id,
    o.outcome_type,
    o.outcome_value,
    o.root_cause_code,
    o.severity,
    o.watchlist_id,
    o.assigned_team,
    o.assigned_to,
    o.repeat_count,
    coalesce(ra.any_accepted,  false) as recommendation_accepted,
    coalesce(ra.any_overridden, false) as recommendation_overridden
  from public.governance_routing_outcomes o
  left join rec_acceptance ra
    on ra.workspace_id = o.workspace_id and ra.case_id = o.case_id
),
by_root_cause as (
  select
    workspace_id,
    'root_cause_code'::text                                                             as feature_type,
    coalesce(root_cause_code, 'unknown')                                                as feature_key,
    count(distinct case_id)                                                             as case_count,
    count(distinct case_id) filter (where recommendation_accepted)                     as accepted_recommendation_count,
    count(distinct case_id) filter (where recommendation_overridden)                   as override_count,
    sum(case when outcome_type = 'reassigned'  then 1 else 0 end)                      as reassignment_count,
    sum(case when outcome_type = 'reopened'    then 1 else 0 end)                      as reopen_count,
    sum(case when outcome_type = 'escalated'   then 1 else 0 end)                      as escalation_count,
    avg(case when outcome_type = 'acknowledged' then outcome_value end)                as avg_ack_latency_seconds,
    avg(case when outcome_type = 'resolved'     then outcome_value end)                as avg_resolve_latency_seconds
  from outcomes_enriched
  group by workspace_id, root_cause_code
),
by_severity as (
  select
    workspace_id,
    'severity'::text                                                                    as feature_type,
    coalesce(severity, 'unknown')                                                       as feature_key,
    count(distinct case_id)                                                             as case_count,
    count(distinct case_id) filter (where recommendation_accepted)                     as accepted_recommendation_count,
    count(distinct case_id) filter (where recommendation_overridden)                   as override_count,
    sum(case when outcome_type = 'reassigned'  then 1 else 0 end)                      as reassignment_count,
    sum(case when outcome_type = 'reopened'    then 1 else 0 end)                      as reopen_count,
    sum(case when outcome_type = 'escalated'   then 1 else 0 end)                      as escalation_count,
    avg(case when outcome_type = 'acknowledged' then outcome_value end)                as avg_ack_latency_seconds,
    avg(case when outcome_type = 'resolved'     then outcome_value end)                as avg_resolve_latency_seconds
  from outcomes_enriched
  group by workspace_id, severity
),
by_watchlist as (
  select
    workspace_id,
    'watchlist_id'::text                                                                as feature_type,
    coalesce(watchlist_id::text, 'workspace')                                          as feature_key,
    count(distinct case_id)                                                             as case_count,
    count(distinct case_id) filter (where recommendation_accepted)                     as accepted_recommendation_count,
    count(distinct case_id) filter (where recommendation_overridden)                   as override_count,
    sum(case when outcome_type = 'reassigned'  then 1 else 0 end)                      as reassignment_count,
    sum(case when outcome_type = 'reopened'    then 1 else 0 end)                      as reopen_count,
    sum(case when outcome_type = 'escalated'   then 1 else 0 end)                      as escalation_count,
    avg(case when outcome_type = 'acknowledged' then outcome_value end)                as avg_ack_latency_seconds,
    avg(case when outcome_type = 'resolved'     then outcome_value end)                as avg_resolve_latency_seconds
  from outcomes_enriched
  group by workspace_id, watchlist_id
),
by_team as (
  select
    workspace_id,
    'current_team'::text                                                                as feature_type,
    coalesce(assigned_team, 'unassigned')                                              as feature_key,
    count(distinct case_id)                                                             as case_count,
    count(distinct case_id) filter (where recommendation_accepted)                     as accepted_recommendation_count,
    count(distinct case_id) filter (where recommendation_overridden)                   as override_count,
    sum(case when outcome_type = 'reassigned'  then 1 else 0 end)                      as reassignment_count,
    sum(case when outcome_type = 'reopened'    then 1 else 0 end)                      as reopen_count,
    sum(case when outcome_type = 'escalated'   then 1 else 0 end)                      as escalation_count,
    avg(case when outcome_type = 'acknowledged' then outcome_value end)                as avg_ack_latency_seconds,
    avg(case when outcome_type = 'resolved'     then outcome_value end)                as avg_resolve_latency_seconds
  from outcomes_enriched
  group by workspace_id, assigned_team
),
by_chronicity as (
  select
    workspace_id,
    'chronicity_bucket'::text                                                           as feature_type,
    case when repeat_count > 0 then 'recurring' else 'first_occurrence' end            as feature_key,
    count(distinct case_id)                                                             as case_count,
    count(distinct case_id) filter (where recommendation_accepted)                     as accepted_recommendation_count,
    count(distinct case_id) filter (where recommendation_overridden)                   as override_count,
    sum(case when outcome_type = 'reassigned'  then 1 else 0 end)                      as reassignment_count,
    sum(case when outcome_type = 'reopened'    then 1 else 0 end)                      as reopen_count,
    sum(case when outcome_type = 'escalated'   then 1 else 0 end)                      as escalation_count,
    avg(case when outcome_type = 'acknowledged' then outcome_value end)                as avg_ack_latency_seconds,
    avg(case when outcome_type = 'resolved'     then outcome_value end)                as avg_resolve_latency_seconds
  from outcomes_enriched
  group by workspace_id, case when repeat_count > 0 then 'recurring' else 'first_occurrence' end
),
all_dims as (
  select * from by_root_cause
  union all select * from by_severity
  union all select * from by_watchlist
  union all select * from by_team
  union all select * from by_chronicity
)
select
  workspace_id,
  feature_type,
  feature_key,
  case_count,
  accepted_recommendation_count,
  override_count,
  reassignment_count,
  reopen_count,
  escalation_count,
  avg_ack_latency_seconds,
  avg_resolve_latency_seconds,
  -- effectiveness_score: resolution quality minus operational friction
  case when case_count > 0 then
    round((
      (case_count - reassignment_count - reopen_count - escalation_count)::numeric
        / nullif(case_count, 0) * 0.40
      - reassignment_count::numeric / nullif(case_count, 0) * 0.25
      - reopen_count::numeric       / nullif(case_count, 0) * 0.25
      - escalation_count::numeric   / nullif(case_count, 0) * 0.10
    ), 4)
  else 0.0 end                                                                         as effectiveness_score,
  -- workload_penalty: override burden as a proxy for routing friction
  case when case_count > 0 then
    round(override_count::numeric / nullif(case_count, 0), 4)
  else 0.0 end                                                                         as workload_penalty_score,
  -- net_fit_score
  case when case_count > 0 then
    round((
      (case_count - reassignment_count - reopen_count - escalation_count)::numeric
        / nullif(case_count, 0) * 0.40
      - reassignment_count::numeric / nullif(case_count, 0) * 0.25
      - reopen_count::numeric       / nullif(case_count, 0) * 0.25
      - escalation_count::numeric   / nullif(case_count, 0) * 0.10
      - override_count::numeric     / nullif(case_count, 0) * 0.10
    ), 4)
  else 0.0 end                                                                         as net_fit_score
from all_dims;

-- ── 4. Context-fit summary view ──────────────────────────────────────────────
-- Identifies the best operator/team for each (root_cause × severity) context,
-- derived from governance_routing_recommendation_inputs.

create or replace view public.governance_routing_context_fit_summary as
with raw as (
  select
    workspace_id,
    routing_target,
    coalesce(root_cause_code, 'unknown') as root_cause_code,
    coalesce(severity, 'unknown')        as severity,
    coalesce(resolved_count, 0)          as resolved_count,
    coalesce(reassigned_count, 0)        as reassigned_count,
    coalesce(escalated_count, 0)         as escalated_count,
    coalesce(reopened_count, 0)          as reopened_count,
    avg_ack_hours,
    avg_resolve_hours
  from public.governance_routing_recommendation_inputs
  where root_cause_code is not null
    and severity is not null
),
scored as (
  select
    workspace_id,
    root_cause_code || '|' || severity as context_key,
    routing_target,
    resolved_count + reassigned_count + escalated_count + reopened_count as sample_size,
    round(
      case
        when (resolved_count + reassigned_count + escalated_count + reopened_count) > 0 then
          (resolved_count::numeric
            / nullif(resolved_count + reassigned_count + escalated_count + reopened_count, 0) * 0.40)
          - (reassigned_count::numeric
            / nullif(resolved_count + reassigned_count + escalated_count + reopened_count, 0) * 0.25)
          - (reopened_count::numeric
            / nullif(resolved_count + reassigned_count + escalated_count + reopened_count, 0) * 0.25)
          - (escalated_count::numeric
            / nullif(resolved_count + reassigned_count + escalated_count + reopened_count, 0) * 0.10)
        else 0.0
      end,
    4) as fit_score
  from raw
),
-- find best operator per context
ranked as (
  select
    workspace_id,
    context_key,
    routing_target,
    sample_size,
    fit_score,
    row_number() over (
      partition by workspace_id, context_key
      order by fit_score desc nulls last, sample_size desc
    ) as rank
  from scored
  where sample_size >= 3
)
select
  workspace_id,
  context_key,
  routing_target                                                                        as recommended_user,
  null::text                                                                            as recommended_team,
  fit_score                                                                             as operator_fit_score,
  null::numeric                                                                         as team_fit_score,
  sample_size,
  case
    when sample_size >= 20 then 'high'
    when sample_size >= 5  then 'medium'
    else                        'low'
  end                                                                                   as confidence,
  jsonb_build_object(
    'fit_score', fit_score,
    'sample_size', sample_size
  )                                                                                     as supporting_metrics
from ranked
where rank = 1;

-- ── 5. Policy opportunity summary view ───────────────────────────────────────
-- Derives from the durable recommendations table, ordered by expected benefit.

create or replace view public.governance_routing_policy_opportunity_summary as
select
  workspace_id,
  recommendation_key,
  scope_type,
  scope_value,
  current_policy,
  recommended_policy,
  reason_code,
  confidence,
  sample_size,
  expected_benefit_score,
  risk_score,
  supporting_metrics,
  created_at
from public.governance_routing_policy_recommendations
order by expected_benefit_score desc nulls last, risk_score asc nulls last, created_at desc;
