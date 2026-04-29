import { NextResponse } from 'next/server';
import { env } from '../../../lib/env';
import { getWatchlistsWithAssets } from '../../../lib/queries/watchlists';

export async function GET() {
  try {
    const response = await getWatchlistsWithAssets(env.NEXT_PUBLIC_DEFAULT_WORKSPACE_SLUG);
    return NextResponse.json(response);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : 'Unknown watchlists error' },
      { status: 500 },
    );
  }
}
