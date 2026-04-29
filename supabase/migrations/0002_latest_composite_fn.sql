create or replace function get_latest_composite_per_asset(p_workspace_id uuid)
returns table (
  asset_id      uuid,
  workspace_id  uuid,
  as_of         timestamptz,
  long_score    numeric,
  short_score   numeric,
  regime        text,
  invalidators  jsonb
)
language sql
stable
as $$
  select distinct on (asset_id)
    asset_id,
    workspace_id,
    as_of,
    long_score,
    short_score,
    regime,
    invalidators
  from composite_scores
  where workspace_id = p_workspace_id
  order by asset_id, as_of desc;
$$;
