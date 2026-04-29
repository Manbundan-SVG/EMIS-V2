import type { SignalLatestRow, SignalsResponse } from '@emis/types';
import { signalRegistry } from '@emis/signal-registry';
import { getAdminClient } from './shared';

type SignalValueRow = {
  asset_id: string;
  signal_name: string;
  score: number | string | null;
  ts: string;
  explanation: Record<string, unknown> | null;
};

type AssetRow = {
  id: string;
  symbol: string | null;
  name: string | null;
};

export async function getLatestSignalRows(limit = 100): Promise<SignalLatestRow[]> {
  const supabase = getAdminClient();

  const { data, error } = await supabase
    .from('signal_values')
    .select('asset_id, signal_name, score, ts, explanation')
    .order('ts', { ascending: false })
    .limit(limit);

  if (error) {
    throw new Error(error.message);
  }

  const signalRows = (data ?? []) as SignalValueRow[];
  const assetIds = [...new Set(signalRows.map((row) => row.asset_id))];
  if (assetIds.length === 0) {
    return [];
  }

  const { data: assets, error: assetsError } = await supabase
    .from('assets')
    .select('id, symbol, name')
    .in('id', assetIds);

  if (assetsError) {
    throw new Error(assetsError.message);
  }

  const assetsById = new Map(
    ((assets ?? []) as AssetRow[]).map((asset) => [asset.id, asset] as const),
  );

  return signalRows.map((row) => {
    const asset = assetsById.get(row.asset_id);

    return {
      asset_id: row.asset_id,
      asset_symbol: asset?.symbol ?? 'UNKNOWN',
      asset_name: asset?.name ?? 'Unknown Asset',
      signal_key: row.signal_name,
      value: Number(row.score),
      as_of: row.ts,
      regime: '',
      explanation: row.explanation ?? {},
    };
  });
}

export async function getSignalsResponse(): Promise<SignalsResponse> {
  return {
    registry: signalRegistry,
    latest: await getLatestSignalRows(),
  };
}
