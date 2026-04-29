import type { WatchlistResponse } from '@emis/types';
import { getAdminClient, requireWorkspaceBySlug } from './shared';

type WatchlistRow = {
  id: string;
  slug: string;
  name: string;
};

type WatchlistAssetRow = {
  watchlist_id: string;
  asset_id: string;
};

type AssetRow = {
  id: string;
  symbol: string | null;
  name: string | null;
  asset_class: string | null;
};

export async function getWatchlistsWithAssets(workspaceSlug: string): Promise<WatchlistResponse> {
  const supabase = getAdminClient();
  const workspace = await requireWorkspaceBySlug(workspaceSlug);

  const { data: watchlists, error: watchlistsError } = await supabase
    .from('watchlists')
    .select('id, slug, name')
    .eq('workspace_id', workspace.id)
    .order('name', { ascending: true });

  if (watchlistsError) {
    throw new Error(watchlistsError.message);
  }

  const watchlistRows = (watchlists ?? []) as WatchlistRow[];
  const watchlistIds = watchlistRows.map((row) => row.id);
  if (watchlistIds.length === 0) {
    return { workspace, watchlists: [] };
  }

  const { data: watchlistAssets, error: watchlistAssetsError } = await supabase
    .from('watchlist_assets')
    .select('watchlist_id, asset_id')
    .in('watchlist_id', watchlistIds);

  if (watchlistAssetsError) {
    throw new Error(watchlistAssetsError.message);
  }

  const watchlistAssetRows = (watchlistAssets ?? []) as WatchlistAssetRow[];
  const assetIds = [...new Set(watchlistAssetRows.map((row) => row.asset_id))];
  const { data: assets, error: assetsError } = await supabase
    .from('assets')
    .select('id, symbol, name, asset_class')
    .in('id', assetIds)
    .order('symbol', { ascending: true });

  if (assetsError) {
    throw new Error(assetsError.message);
  }

  const assetsById = new Map(
    ((assets ?? []) as AssetRow[]).map((asset) => [asset.id, asset] as const),
  );

  return {
    workspace,
    watchlists: watchlistRows.map((watchlist) => ({
      id: watchlist.id,
      slug: watchlist.slug,
      name: watchlist.name,
      assets: watchlistAssetRows
        .filter((row) => row.watchlist_id === watchlist.id)
        .map((row) => assetsById.get(row.asset_id))
        .filter((row): row is NonNullable<typeof row> => Boolean(row))
        .map((asset) => ({
          id: asset.id,
          symbol: asset.symbol ?? 'UNKNOWN',
          name: asset.name ?? 'Unknown Asset',
          asset_class: asset.asset_class ?? 'unknown',
        })),
    })),
  };
}
