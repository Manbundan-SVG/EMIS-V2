begin;

create table if not exists public.governance_anomaly_clusters (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.workspaces(id) on delete cascade,
  watchlist_id uuid references public.watchlists(id) on delete set null,
  version_tuple text not null,
  cluster_key text not null,
  alert_type text not null,
  regime text,
  severity text not null check (severity in ('low', 'medium', 'high')),
  status text not null default 'open' check (status in ('open', 'resolved', 'suppressed')),
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  event_count integer not null default 1 check (event_count >= 1),
  latest_event_id uuid references public.governance_alert_events(id) on delete set null,
  latest_run_id uuid references public.job_runs(id) on delete set null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists governance_anomaly_clusters_open_key_idx
  on public.governance_anomaly_clusters(workspace_id, cluster_key)
  where status = 'open';

create index if not exists governance_anomaly_clusters_workspace_last_seen_idx
  on public.governance_anomaly_clusters(workspace_id, last_seen_at desc);

create index if not exists governance_anomaly_clusters_watchlist_status_idx
  on public.governance_anomaly_clusters(workspace_id, watchlist_id, status, last_seen_at desc);

create table if not exists public.governance_anomaly_cluster_members (
  id uuid primary key default gen_random_uuid(),
  cluster_id uuid not null references public.governance_anomaly_clusters(id) on delete cascade,
  governance_alert_event_id uuid references public.governance_alert_events(id) on delete cascade,
  run_id uuid references public.job_runs(id) on delete set null,
  created_at timestamptz not null default now(),
  unique(cluster_id, governance_alert_event_id)
);

create index if not exists governance_anomaly_cluster_members_cluster_idx
  on public.governance_anomaly_cluster_members(cluster_id, created_at desc);

create or replace view public.governance_anomaly_cluster_state as
select
  c.id,
  c.workspace_id,
  ws.slug as workspace_slug,
  c.watchlist_id,
  w.slug as watchlist_slug,
  w.name as watchlist_name,
  c.version_tuple,
  c.cluster_key,
  c.alert_type,
  c.regime,
  c.severity,
  c.status,
  c.first_seen_at,
  c.last_seen_at,
  c.event_count,
  c.latest_event_id,
  c.latest_run_id,
  c.metadata,
  c.created_at,
  c.updated_at
from public.governance_anomaly_clusters c
join public.workspaces ws
  on ws.id = c.workspace_id
left join public.watchlists w
  on w.id = c.watchlist_id;

create or replace view public.watchlist_anomaly_summary as
select
  c.workspace_id,
  ws.slug as workspace_slug,
  c.watchlist_id,
  w.slug as watchlist_slug,
  w.name as watchlist_name,
  count(*) filter (where c.status = 'open')::bigint as open_cluster_count,
  count(*)::bigint as total_cluster_count,
  count(*) filter (where c.status = 'open' and c.severity = 'high')::bigint as high_open_cluster_count,
  coalesce(sum(c.event_count) filter (where c.status = 'open'), 0)::bigint as open_event_count,
  max(c.last_seen_at) as last_seen_at
from public.governance_anomaly_clusters c
join public.workspaces ws
  on ws.id = c.workspace_id
left join public.watchlists w
  on w.id = c.watchlist_id
group by
  c.workspace_id,
  ws.slug,
  c.watchlist_id,
  w.slug,
  w.name;

comment on view public.governance_anomaly_cluster_state is
  'Open and historical governance anomaly clusters enriched with workspace and watchlist metadata.';

comment on view public.watchlist_anomaly_summary is
  'Watchlist-level summary of governance anomaly clusters for the ops dashboard.';

insert into public.schema_migration_ledger (version, name, applied_by, metadata)
values (
  '0026_phase2_7A_anomaly_clustering',
  'Add governance anomaly clustering and watchlist anomaly summary views',
  current_user,
  jsonb_build_object(
    'tables', jsonb_build_array('governance_anomaly_clusters', 'governance_anomaly_cluster_members'),
    'views', jsonb_build_array('governance_anomaly_cluster_state', 'watchlist_anomaly_summary'),
    'focus', jsonb_build_array(
      'governance_alert_clustering',
      'watchlist_anomaly_monitoring',
      'version_tuple_anomaly_grouping'
    )
  )
)
on conflict (version) do nothing;

commit;
