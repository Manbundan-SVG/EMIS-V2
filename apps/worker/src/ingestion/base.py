from dataclasses import dataclass
from typing import Any


@dataclass
class IngestionEvent:
    source: str
    asset: str
    payload: dict[str, Any]
    observed_at: str
