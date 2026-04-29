import { NextRequest, NextResponse } from "next/server";
import { getGovernanceCaseDetail } from "@/lib/queries/governance_cases";

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ caseId: string }> },
) {
  try {
    const { caseId } = await context.params;
    const incident = await getGovernanceCaseDetail(caseId);

    if (!incident) {
      return NextResponse.json({ ok: false, error: "not_found" }, { status: 404 });
    }

    return NextResponse.json({ ok: true, incident });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error instanceof Error ? error.message : "unknown error" },
      { status: 500 },
    );
  }
}
