"""时间解析、比较与可注入时钟。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .errors import ValidationError


def parse_instant(value: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError("时间不能为空")
    normalized = value.strip().replace("Z", "+00:00")
    try:
        result = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValidationError("时间必须使用 ISO 8601 格式") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValidationError("时间必须包含时区")
    return result.astimezone(timezone.utc)


def canonical_instant(value: str) -> str:
    return parse_instant(value).isoformat().replace("+00:00", "Z")


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class Clock:
    fixed: str | None = None

    def now(self) -> str:
        return canonical_instant(self.fixed) if self.fixed else now_utc()

    def is_due(self, due_at: str) -> bool:
        return parse_instant(due_at) <= parse_instant(self.now())
