with ranked as (
  select
    id,
    row_number() over (
      partition by workspace_id, severity, coalesce(chronicity_class, '')
      order by created_at asc, id asc
    ) as duplicate_rank
  from public.governance_sla_policies
)
delete from public.governance_sla_policies p
using ranked r
where p.id = r.id
  and r.duplicate_rank > 1;

create unique index if not exists idx_governance_sla_policies_workspace_severity_class
  on public.governance_sla_policies (workspace_id, severity, coalesce(chronicity_class, ''));

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0038',
  '0038_phase3_1B_sla_policy_uniqueness_fix',
  current_user,
  jsonb_build_object('phase', '3.1B', 'feature', 'sla_policy_uniqueness_fix')
)
on conflict (version) do nothing;
