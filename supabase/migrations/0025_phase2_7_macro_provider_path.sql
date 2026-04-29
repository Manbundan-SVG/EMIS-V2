begin;

alter table public.market_data_sync_runs
  add column if not exists macro_point_count integer not null default 0;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0025_phase2_7_macro_provider_path',
  'Add macro sync count to market sync audit for FRED provider path',
  current_user,
  jsonb_build_object(
    'tables', jsonb_build_array('market_data_sync_runs'),
    'columns', jsonb_build_array('macro_point_count'),
    'focus', jsonb_build_array(
      'fred_macro_provider_audit',
      'macro_sync_observability'
    )
  )
)
on conflict (version) do nothing;

commit;
