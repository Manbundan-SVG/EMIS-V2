import { NextRequest, NextResponse } from "next/server";
import { z } from "zod";
import { listAlertPolicyRules } from "@/lib/queries/governance";
import { createServiceSupabaseClient } from "@/lib/supabase";

const postSchema = z.object({
  workspaceId: z.string().uuid(),
  watchlistId: z.string().uuid().nullable().optional(),
  eventType: z.string().min(1),
  severity: z.string().min(1),
  jobRunId: z.string().uuid(),
  payload: z.record(z.unknown()).optional(),
});

export async function GET(request: NextRequest) {
  const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
  try {
    const rules = await listAlertPolicyRules(workspace);
    return NextResponse.json({ ok: true, rules });
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
    const supabase = createServiceSupabaseClient();
    const { data, error } = await supabase.rpc("evaluate_alert_policies", {
      p_workspace_id: body.workspaceId,
      p_watchlist_id: body.watchlistId ?? null,
      p_event_type: body.eventType,
      p_severity: body.severity,
      p_job_run_id: body.jobRunId,
      p_payload: body.payload ?? {},
    });

    if (error) {
      throw new Error(`Alert policy evaluation error: ${error.message}`);
    }

    return NextResponse.json({ ok: true, inserted: data ?? 0 });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 400 },
    );
  }
}
