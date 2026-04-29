begin;

create table if not exists public.governance_case_summaries (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  case_id uuid not null references public.governance_cases(id) on delete cascade,
  summary_version text not null default 'v1',
  status_summary text null,
  root_cause_code text null,
  root_cause_confidence numeric null,
  root_cause_summary text null,
  evidence_summary text null,
  recurrence_summary text null,
  operator_summary text null,
  closure_summary text null,
  recommended_next_action text null,
  source_note_ids jsonb not null default '[]'::jsonb,
  source_evidence_ids jsonb not null default '[]'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  generated_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(case_id, summary_version)
);

create index if not exists governance_case_summaries_case_idx
  on public.governance_case_summaries(case_id, generated_at desc);

create or replace view public.governance_case_summary_latest as
select distinct on (case_id)
  id,
  workspace_id,
  case_id,
  summary_version,
  status_summary,
  root_cause_code,
  root_cause_confidence,
  root_cause_summary,
  evidence_summary,
  recurrence_summary,
  operator_summary,
  closure_summary,
  recommended_next_action,
  source_note_ids,
  source_evidence_ids,
  metadata,
  generated_at,
  updated_at
from public.governance_case_summaries
order by case_id, generated_at desc, id desc;

comment on view public.governance_case_summary_latest is
  'Latest persisted operator-readable summary for each governance case.';

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0035',
  '0035_phase3_0D_case_summaries',
  current_user,
  jsonb_build_object(
    'phase', '3.0D',
    'description', 'Persisted case summaries and root-cause reporting',
    'tables', jsonb_build_array(
      'governance_case_summaries'
    ),
    'views', jsonb_build_array(
      'governance_case_summary_latest'
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
