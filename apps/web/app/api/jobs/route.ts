import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";
import { listRecentJobs } from "@/lib/queries/jobs";

const querySchema = z.object({
  workspaceSlug: z.string().default("demo"),
  limit: z.coerce.number().int().positive().max(100).default(20)
});

export async function GET(request: NextRequest) {
  try {
    const query = querySchema.parse(Object.fromEntries(request.nextUrl.searchParams.entries()));
    const jobs = await listRecentJobs(query.workspaceSlug, query.limit);
    return NextResponse.json({ ok: true, jobs });
  } catch (error) {
    return NextResponse.json({ ok: false, error: error instanceof Error ? error.message : "Unknown job query error" }, { status: 400 });
  }
}
