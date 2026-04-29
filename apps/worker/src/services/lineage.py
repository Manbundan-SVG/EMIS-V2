from __future__ import annotations

from dataclasses import asdict, dataclass
from time import time
from typing import Any


@dataclass
class ComputeLineage:
    compute_version: str
    signal_registry_version: str
    model_version: str
    pipeline_name: str
    source_window: str
    started_unix_ms: int

    @classmethod
    def start(
        cls,
        *,
        compute_version: str,
        signal_registry_version: str,
        model_version: str,
        pipeline_name: str,
        source_window: str,
    ) -> "ComputeLineage":
        return cls(
            compute_version=compute_version,
            signal_registry_version=signal_registry_version,
            model_version=model_version,
            pipeline_name=pipeline_name,
            source_window=source_window,
            started_unix_ms=int(time() * 1000),
        )

    def as_payload(self) -> dict[str, Any]:
        return asdict(self)

    def finish(self, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = self.as_payload()
        payload["completed_unix_ms"] = int(time() * 1000)
        payload["runtime_ms"] = payload["completed_unix_ms"] - self.started_unix_ms
        if extra:
            payload.update(extra)
        return payload
