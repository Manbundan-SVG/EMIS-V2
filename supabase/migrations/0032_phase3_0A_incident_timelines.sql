begin;

create table if not exists public.governance_incident_timeline_events (
    id bigserial primary key,
    case_id uuid not null references public.governance_cases(id) on delete cascade,
    workspace_id uuid not null references public.workspaces(id) on delete cascade,
    event_type text not null,
    event_source text not null,
    event_at timestamptz not null default now(),
    actor text null,
    title text not null,
    detail text null,
    metadata jsonb not null default '{}'::jsonb,
    source_table text null,
    source_id text null,
    created_at timestamptz not null default now()
);

create index if not exists idx_governance_incident_timeline_case_event_at
    on public.governance_incident_timeline_events(case_id, event_at desc, id desc);

create index if not exists idx_governance_incident_timeline_workspace_event_at
    on public.governance_incident_timeline_events(workspace_id, event_at desc, id desc);

insert into public.governance_incident_timeline_events (
    case_id,
    workspace_id,
    event_type,
    event_source,
    event_at,
    actor,
    title,
    detail,
    metadata,
    source_table,
    source_id
)
select
    e.case_id,
    e.workspace_id,
    e.event_type,
    'governance_case_event' as event_source,
    e.created_at as event_at,
    e.actor,
    initcap(replace(e.event_type, '_', ' ')) as title,
    nullif(e.payload ->> 'note', '') as detail,
    coalesce(e.payload, '{}'::jsonb) as metadata,
    'governance_case_events' as source_table,
    e.id::text as source_id
from public.governance_case_events e
where not exists (
    select 1
    from public.governance_incident_timeline_events te
    where te.source_table = 'governance_case_events'
      and te.source_id = e.id::text
);

create or replace view public.governance_incident_timeline_summary as
with evidence_rollup as (
    select
        e.case_id,
        count(*)::int as evidence_count,
        count(*) filter (where e.evidence_type = 'run')::int as run_evidence_count,
        count(*) filter (where e.evidence_type = 'anomaly_cluster')::int as cluster_evidence_count,
        count(*) filter (where e.evidence_type = 'version_tuple')::int as version_evidence_count
    from public.governance_case_evidence e
    group by e.case_id
),
latest_assignment as (
    select distinct on (a.case_id)
        a.case_id,
        a.assigned_to,
        a.assigned_team,
        a.assigned_at
    from public.governance_assignments a
    order by a.case_id, a.assigned_at desc, a.id desc
),
latest_timeline as (
    select distinct on (te.case_id)
        te.case_id,
        te.event_type as latest_event_type,
        te.event_at as latest_event_at
    from public.governance_incident_timeline_events te
    order by te.case_id, te.event_at desc, te.id desc
)
select
    c.id as case_id,
    c.workspace_id,
    ws.slug as workspace_slug,
    c.degradation_state_id,
    c.watchlist_id,
    w.slug as watchlist_slug,
    w.name as watchlist_name,
    c.version_tuple,
    c.status,
    c.severity,
    c.title,
    c.summary,
    c.opened_at,
    c.acknowledged_at,
    c.resolved_at,
    c.closed_at,
    c.reopened_count,
    c.current_assignee,
    c.current_team,
    c.metadata,
    coalesce(er.evidence_count, 0) as evidence_count,
    coalesce(er.run_evidence_count, 0) as run_evidence_count,
    coalesce(er.cluster_evidence_count, 0) as cluster_evidence_count,
    coalesce(er.version_evidence_count, 0) as version_evidence_count,
    la.assigned_to,
    la.assigned_team,
    la.assigned_at as last_assigned_at,
    (
        select count(*)::int
        from public.governance_incident_timeline_events te
        where te.case_id = c.id
    ) as timeline_event_count,
    lt.latest_event_type,
    lt.latest_event_at
from public.governance_cases c
join public.workspaces ws on ws.id = c.workspace_id
left join public.watchlists w on w.id = c.watchlist_id
left join evidence_rollup er on er.case_id = c.id
left join latest_assignment la on la.case_id = c.id
left join latest_timeline lt on lt.case_id = c.id;

create or replace view public.governance_incident_detail as
select *
from public.governance_incident_timeline_summary;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
    '0032',
    '0032_phase3_0A_incident_timelines',
    current_user,
    jsonb_build_object(
        'phase', '3.0A',
        'description', 'Incident timeline persistence and detail views'
    )
)
on conflict (version) do update
set
    name = excluded.name,
    applied_at = now(),
    applied_by = excluded.applied_by,
    metadata = excluded.metadata;

commit;
