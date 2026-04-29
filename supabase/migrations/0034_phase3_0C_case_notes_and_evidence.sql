begin;

alter table if exists public.governance_case_notes
  add column if not exists note_type text not null default 'investigation',
  add column if not exists visibility text not null default 'internal',
  add column if not exists edited_at timestamptz null;

create index if not exists governance_case_notes_case_created_idx
  on public.governance_case_notes (case_id, created_at desc);

alter table if exists public.governance_case_evidence
  add column if not exists title text null,
  add column if not exists summary text null;

update public.governance_case_evidence
set
  title = coalesce(title, initcap(replace(evidence_type, '_', ' '))),
  summary = coalesce(
    summary,
    concat('Linked ', replace(evidence_type, '_', ' '), ' evidence ', reference_id)
  )
where title is null
   or summary is null;

create or replace view public.governance_case_evidence_summary as
with evidence_type_counts as (
  select
    counts.case_id,
    jsonb_object_agg(counts.evidence_type, counts.evidence_type_count order by counts.evidence_type) as evidence_type_counts
  from (
    select
      case_id,
      evidence_type,
      count(*)::int as evidence_type_count
    from public.governance_case_evidence
    group by case_id, evidence_type
  ) counts
  group by counts.case_id
)
select
  c.id as case_id,
  c.workspace_id,
  count(e.id)::int as evidence_count,
  max(e.created_at) as latest_evidence_at,
  latest_run.reference_id as latest_run_id,
  latest_replay_delta.reference_id as latest_replay_delta_id,
  latest_regime_transition.reference_id as latest_regime_transition_id,
  latest_threshold_application.reference_id as latest_threshold_application_id,
  coalesce(type_counts.evidence_type_counts, '{}'::jsonb) as evidence_type_counts,
  coalesce(
    jsonb_agg(
      jsonb_build_object(
        'id', e.id,
        'case_id', e.case_id,
        'workspace_id', e.workspace_id,
        'evidence_type', e.evidence_type,
        'reference_id', e.reference_id,
        'title', e.title,
        'summary', e.summary,
        'payload', e.payload,
        'created_at', e.created_at
      )
      order by e.created_at desc, e.id desc
    ) filter (where e.id is not null),
    '[]'::jsonb
  ) as evidence_items
from public.governance_cases c
left join public.governance_case_evidence e
  on e.case_id = c.id
left join evidence_type_counts type_counts
  on type_counts.case_id = c.id
left join lateral (
  select reference_id
  from public.governance_case_evidence ge
  where ge.case_id = c.id
    and ge.evidence_type = 'run'
  order by ge.created_at desc, ge.id desc
  limit 1
) latest_run on true
left join lateral (
  select reference_id
  from public.governance_case_evidence ge
  where ge.case_id = c.id
    and ge.evidence_type = 'replay_delta'
  order by ge.created_at desc, ge.id desc
  limit 1
) latest_replay_delta on true
left join lateral (
  select reference_id
  from public.governance_case_evidence ge
  where ge.case_id = c.id
    and ge.evidence_type = 'regime_transition'
  order by ge.created_at desc, ge.id desc
  limit 1
) latest_regime_transition on true
left join lateral (
  select reference_id
  from public.governance_case_evidence ge
  where ge.case_id = c.id
    and ge.evidence_type = 'threshold_application'
  order by ge.created_at desc, ge.id desc
  limit 1
) latest_threshold_application on true
group by
  c.id,
  c.workspace_id,
  type_counts.evidence_type_counts,
  latest_run.reference_id,
  latest_replay_delta.reference_id,
  latest_regime_transition.reference_id,
  latest_threshold_application.reference_id;

comment on view public.governance_case_evidence_summary is
  'Aggregated evidence rollup for governance case detail, handoff, and operator investigation workflows.';

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0034',
  '0034_phase3_0C_case_notes_and_evidence',
  current_user,
  jsonb_build_object(
    'phase', '3.0C',
    'description', 'Structured case notes, enriched evidence rows, and evidence summary view',
    'tables', jsonb_build_array(
      'governance_case_notes',
      'governance_case_evidence'
    ),
    'views', jsonb_build_array(
      'governance_case_evidence_summary'
    )
  )
)
on conflict (version) do update
set
  name = excluded.name,
  applied_at = now(),
  applied_by = excluded.applied_by,
  metadata = excluded.metadata;

commit;
