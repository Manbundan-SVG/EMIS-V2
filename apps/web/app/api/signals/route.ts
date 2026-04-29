import { NextResponse } from 'next/server';
import { getSignalsResponse } from '../../../lib/queries/signals';

export async function GET() {
  try {
    const response = await getSignalsResponse();
    return NextResponse.json(response);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : 'Unknown signals error' },
      { status: 500 },
    );
  }
}
