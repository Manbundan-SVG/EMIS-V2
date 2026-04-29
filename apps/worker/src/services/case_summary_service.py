from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CaseSummaryResult:
    status_summary: str | None
    root_cause_code: str | None
    root_cause_confidence: float | None
    root_cause_summary: str | None
    evidence_summary: str | None
    recurrence_summary: str | None
    operator_summary: str | None
    closure_summary: str | None
    recommended_next_action: str | None
    source_note_ids: list[str]
    source_evidence_ids: list[str]
    metadata: dict[str, Any]


class CaseSummaryService:
    def build_summary(
        self,
        *,
        case_row: dict[str, Any],
        notes: list[dict[str, Any]],
        evidence: list[dict[str, Any]],
        recurrence: dict[str, Any] | None,
        lifecycle: dict[str, Any] | None,
    ) -> CaseSummaryResult:
        ordered_notes = sorted(
            notes,
            key=lambda note: (
                str(note.get("created_at") or ""),
                str(note.get("id") or ""),
            ),
            reverse=True,
        )
        ordered_evidence = sorted(
            evidence,
            key=lambda item: (
                str(item.get("created_at") or ""),
                str(item.get("id") or ""),
            ),
            reverse=True,
        )

        root_cause_code = self._classify_root_cause(case_row, ordered_notes, ordered_evidence)
        root_cause_summary = self._build_root_cause_summary(root_cause_code, ordered_evidence)

        return CaseSummaryResult(
            status_summary=self._build_status_summary(case_row, lifecycle),
            root_cause_code=root_cause_code,
            root_cause_confidence=0.85 if root_cause_code and root_cause_code != "mixed_or_unresolved" else (0.55 if root_cause_code else None),
            root_cause_summary=root_cause_summary,
            evidence_summary=self._build_evidence_summary(ordered_evidence),
            recurrence_summary=self._build_recurrence_summary(recurrence),
            operator_summary=self._build_operator_summary(ordered_notes),
            closure_summary=self._build_closure_summary(ordered_notes, case_row, lifecycle),
            recommended_next_action=self._build_next_action(case_row, root_cause_code),
            source_note_ids=[str(note["id"]) for note in ordered_notes if note.get("id")],
            source_evidence_ids=[str(item["id"]) for item in ordered_evidence if item.get("id")],
            metadata={
                "note_count": len(ordered_notes),
                "evidence_count": len(ordered_evidence),
                "status": case_row.get("status"),
                "severity": case_row.get("severity"),
            },
        )

    def _classify_root_cause(
        self,
        case_row: dict[str, Any],
        notes: list[dict[str, Any]],
        evidence: list[dict[str, Any]],
    ) -> str | None:
        evidence_types = {str(item.get("evidence_type") or "") for item in evidence}
        note_text = " ".join(str(note.get("note") or "") for note in notes).lower()
        summaries = " ".join(str(item.get("summary") or "") for item in evidence).lower()
        combined = f"{note_text} {summaries}"

        if "replay_delta" in evidence_types or "replay" in combined:
            return "replay_inconsistency"
        if "threshold_application" in evidence_types and "regime" in combined:
            return "regime_conflict"
        if "regime_transition" in evidence_types or "regime" in combined:
            return "regime_conflict"
        if "version_tuple" in evidence_types or "version" in combined or "canary" in combined:
            return "version_regression"
        if "scope" in combined:
            return "watchlist_scope_drift"
        if str(case_row.get("severity") or "") in {"critical", "high"}:
            return "family_instability"
        if evidence or notes:
            return "mixed_or_unresolved"
        return None

    def _build_root_cause_summary(self, code: str | None, evidence: list[dict[str, Any]]) -> str | None:
        if not code:
            return None
        if code == "version_regression":
            return "Evidence points to a version-linked behavior change rather than a transient market-only issue."
        if code == "regime_conflict":
            return "Signals and thresholds suggest the incident is tied to a regime transition or regime conflict."
        if code == "replay_inconsistency":
            return "Replay-linked evidence suggests the incident is driven by inconsistent replay behavior."
        if code == "watchlist_scope_drift":
            return "Evidence suggests the effective watchlist or dependency scope drifted from prior healthy behavior."
        if code == "family_instability":
            return f"Case severity and evidence profile indicate sustained family instability across {len(evidence)} linked artifacts."
        return "Available evidence is insufficient to isolate a single root cause, so the case remains mixed or unresolved."

    def _build_evidence_summary(self, evidence: list[dict[str, Any]]) -> str | None:
        if not evidence:
            return None
        labels = [str(item.get("title") or item.get("evidence_type") or "evidence") for item in evidence[:3]]
        return "Key evidence: " + ", ".join(labels)

    def _build_recurrence_summary(self, recurrence: dict[str, Any] | None) -> str | None:
        if not recurrence:
            return None
        repeat_count = int(recurrence.get("repeat_count") or recurrence.get("repeatCount") or 1)
        if recurrence.get("is_reopened"):
            return f"Case reopened from a prior related incident and is now on repeat count {repeat_count}."
        if recurrence.get("is_recurring"):
            return f"Case is part of a recurrence group with repeat count {repeat_count}."
        return None

    def _build_operator_summary(self, notes: list[dict[str, Any]]) -> str | None:
        for note_type in ("handoff", "root_cause", "investigation"):
            for note in notes:
                if str(note.get("note_type") or "") == note_type and note.get("note"):
                    return str(note["note"])
        return None

    def _build_closure_summary(
        self,
        notes: list[dict[str, Any]],
        case_row: dict[str, Any],
        lifecycle: dict[str, Any] | None,
    ) -> str | None:
        for note in notes:
            if str(note.get("note_type") or "") == "closure" and note.get("note"):
                return str(note["note"])
        if str(case_row.get("status") or "") in {"resolved", "closed"}:
            recovery_reason = None
            if lifecycle:
                recovery_reason = lifecycle.get("last_resolution_note") or lifecycle.get("last_resolution_action")
            if recovery_reason:
                return str(recovery_reason)
            return "Case is resolved, but no explicit closure note was recorded."
        return None

    def _build_next_action(self, case_row: dict[str, Any], root_cause_code: str | None) -> str | None:
        status = str(case_row.get("status") or "")
        if status in {"resolved", "closed"}:
            return "Monitor for recurrence and review related prior cases before closing the loop completely."
        if root_cause_code == "version_regression":
            return "Compare the current version tuple against the last stable baseline and inspect replay drift."
        if root_cause_code == "regime_conflict":
            return "Review regime transitions and threshold applications before escalating the case."
        if root_cause_code == "replay_inconsistency":
            return "Inspect replay delta evidence and confirm whether the source and replay inputs diverged."
        if root_cause_code == "watchlist_scope_drift":
            return "Verify the compute scope hash and dependency asset set against the last healthy run."
        return "Continue investigation, attach more supporting evidence, and leave a clear handoff note if ownership changes."

    def _build_status_summary(self, case_row: dict[str, Any], lifecycle: dict[str, Any] | None) -> str | None:
        status = str(case_row.get("status") or "unknown")
        severity = str(case_row.get("severity") or "unknown")
        owner = case_row.get("current_assignee") or case_row.get("current_team")
        if lifecycle and lifecycle.get("acknowledged_by"):
            return (
                f"Case is {status} with severity {severity}, owned by {owner or 'unassigned'}, "
                f"and has already been acknowledged."
            )
        return f"Case is {status} with severity {severity} and is currently owned by {owner or 'unassigned'}."
