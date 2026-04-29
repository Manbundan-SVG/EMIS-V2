import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";
import { listDeadLetters, requeueDeadLetter } from "@/lib/queries/dead_letters";

const getSchema = z.object({
  workspace: z.string().default("demo"),
  limit: z.coerce.number().int().positive().max(100).default(50),
});

const postSchema = z.object({
  deadLetterId: z.number().int().positive(),
  resetRetryCount: z.boolean().default(false),
});

export async function GET(request: NextRequest) {
  try {
    const q = getSchema.parse(Object.fromEntries(request.nextUrl.searchParams.entries()));
    const rows = await listDeadLetters(q.workspace, q.limit);
    return NextResponse.json({ ok: true, rows });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = postSchema.parse(await request.json());
    const newJobId = await requeueDeadLetter(body.deadLetterId, body.resetRetryCount);
    return NextResponse.json({ ok: true, newJobId });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 400 },
    );
  }
}
