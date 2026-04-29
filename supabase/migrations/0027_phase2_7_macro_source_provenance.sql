begin;

update public.macro_series_points
set source = 'legacy_unspecified'
where source is null;

alter table public.macro_series_points
  alter column source set not null;

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0027_phase2_7_macro_source_provenance',
  'Backfill and enforce macro series source provenance',
  current_user,
  jsonb_build_object(
    'tables', jsonb_build_array('macro_series_points'),
    'columns', jsonb_build_array('source'),
    'focus', jsonb_build_array(
      'macro_source_backfill',
      'provenance_not_null_enforcement'
    )
  )
)
on conflict (version) do nothing;

commit;
