export type SignalFamily =
  | 'trend'
  | 'leverage'
  | 'liquidation'
  | 'macro';

export type Horizon = 'intraday' | 'swing' | 'daily';

export type Regime =
  | 'trend_persistence'
  | 'deleveraging'
  | 'mean_reversion'
  | 'macro_dominant'
  | 'risk_on'
  | 'risk_off';

export interface SignalDefinition {
  key: string;
  family: SignalFamily;
  horizon: Horizon;
  description: string;
  inputs: string[];
  outputs: string[];
}

export interface WorkspaceRecord {
  id: string;
  slug: string;
  name: string;
  created_at: string;
}

export interface AssetRecord {
  id: string;
  symbol: string;
  name: string;
  asset_class: string;
  is_active: boolean;
  created_at: string;
}

export interface WatchlistRecord {
  id: string;
  workspace_id: string;
  slug: string;
  name: string;
  created_at: string;
}

export interface WatchlistAssetRecord {
  watchlist_id: string;
  asset_id: string;
  created_at: string;
}

export interface SignalValueRecord {
  asset_id: string;
  signal_key: string;
  value: number;
  as_of: string;
  regime: Regime | string;
  explanation: Record<string, unknown>;
}

export interface CompositeScoreRecord {
  asset_id: string;
  workspace_id: string;
  long_score: number;
  short_score: number;
  regime: Regime | string;
  invalidators: string[];
  as_of: string;
}

export interface WatchlistCompositeRow {
  asset_id: string;
  asset_symbol: string;
  asset_name: string;
  asset_class: string;
  watchlist_slug: string;
  watchlist_name: string;
  as_of: string;
  long_score: number;
  short_score: number;
  regime: Regime | string;
  invalidators: string[];
}

export interface WatchlistResponse {
  workspace: Pick<WorkspaceRecord, 'id' | 'slug' | 'name'>;
  watchlists: Array<{
    id: string;
    slug: string;
    name: string;
    assets: Array<{
      id: string;
      symbol: string;
      name: string;
      asset_class: string;
    }>;
  }>;
}

export interface SignalLatestRow {
  asset_id: string;
  asset_symbol: string;
  asset_name: string;
  signal_key: string;
  value: number;
  as_of: string;
  regime: string;
  explanation: Record<string, unknown>;
}

export interface SignalsResponse {
  registry: SignalDefinition[];
  latest: SignalLatestRow[];
}

export interface CompositeSnapshotResponse {
  rows: WatchlistCompositeRow[];
  generated_at: string;
}

export * from "./ops";
