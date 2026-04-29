-- Apply after migrations if you want persistent demo watchlists.
insert into watchlists (workspace_id, slug, name)
select id, 'core-macro', 'Core Macro'
from workspaces
where slug = 'default'
on conflict (workspace_id, slug) do update set name = excluded.name;

insert into watchlists (workspace_id, slug, name)
select id, 'alts-leverage', 'Alts Leverage'
from workspaces
where slug = 'default'
on conflict (workspace_id, slug) do update set name = excluded.name;

insert into watchlists (workspace_id, slug, name)
select id, 'macro-defensives', 'Macro Defensives'
from workspaces
where slug = 'default'
on conflict (workspace_id, slug) do update set name = excluded.name;
