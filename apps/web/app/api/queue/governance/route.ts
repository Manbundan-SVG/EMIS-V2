import { NextRequest, NextResponse } from "next/server";
import { listAlertPolicyRules, listQueueGovernanceRules } from "@/lib/queries/governance";
import { getQueueGovernanceState } from "@/lib/queries/metrics";

export async function GET(request: NextRequest) {
  const workspace = request.nextUrl.searchParams.get("workspace") ?? "demo";
  try {
    const [state, rules, alertPolicies] = await Promise.all([
      getQueueGovernanceState(workspace),
      listQueueGovernanceRules(workspace),
      listAlertPolicyRules(workspace),
    ]);

    return NextResponse.json({ ok: true, state, rules, alertPolicies });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
