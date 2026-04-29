import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";
import { listLatestComposites } from "@/lib/queries/composites";

const querySchema = z.object({
  workspaceSlug: z.string().default("demo"),
  limit: z.coerce.number().int().positive().max(100).default(25)
});

export async function GET(request: NextRequest) {
  try {
    const query = querySchema.parse(Object.fromEntries(request.nextUrl.searchParams.entries()));
    const composites = await listLatestComposites(query.workspaceSlug, query.limit);
    return NextResponse.json({ ok: true, composites });
  } catch (error) {
    return NextResponse.json({ ok: false, error: error instanceof Error ? error.message : "Unknown composite query error" }, { status: 400 });
  }
}
