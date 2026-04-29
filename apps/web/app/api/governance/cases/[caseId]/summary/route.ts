import { NextRequest, NextResponse } from "next/server";
import {
  getGovernanceCaseGeneratedSummary,
  refreshGovernanceCaseGeneratedSummary,
} from "@/lib/queries/governance_cases";

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ caseId: string }> },
) {
  try {
    const { caseId } = await context.params;
    const refresh = request.nextUrl.searchParams.get("refresh") === "true";
    const summary = refresh
      ? await refreshGovernanceCaseGeneratedSummary(caseId)
      : await getGovernanceCaseGeneratedSummary(caseId);

    return NextResponse.json({ ok: true, caseId, summary });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
