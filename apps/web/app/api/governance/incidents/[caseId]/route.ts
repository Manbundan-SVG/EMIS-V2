import { NextRequest, NextResponse } from "next/server";
import { getIncidentDetail } from "@/lib/queries/incidents";

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ caseId: string }> },
) {
  try {
    const { caseId } = await context.params;
    const incident = await getIncidentDetail(caseId);

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
