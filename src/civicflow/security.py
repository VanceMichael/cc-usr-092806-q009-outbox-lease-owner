"""权限上下文和字段裁剪。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Mapping

from .errors import PermissionDenied


@dataclass(frozen=True)
class AccessContext:
    actor_id: str
    permissions: frozenset[str] = field(default_factory=frozenset)
    scopes: frozenset[str] = field(default_factory=frozenset)
    reveal_sensitive: bool = False

    @classmethod
    def system(cls, actor_id: str = "system") -> "AccessContext":
        return cls(actor_id=actor_id, permissions=frozenset({"*"}), scopes=frozenset({"*"}), reveal_sensitive=True)

    def allows(self, permission: str) -> bool:
        return "*" in self.permissions or permission in self.permissions

    def require(self, permission: str) -> None:
        if not self.allows(permission):
            raise PermissionDenied(f"缺少权限: {permission}")

    def has_scope(self, scope: str) -> bool:
        return "*" in self.scopes or scope in self.scopes


def redact_record(record: Mapping[str, object], restricted_fields: Iterable[str], context: AccessContext) -> dict:
    result = dict(record)
    if context.reveal_sensitive:
        return result
    for field_name in restricted_fields:
        if field_name in result:
            result[field_name] = "***"
    return result


def assert_distinct(submitter: str, reviewer: str) -> None:
    if submitter == reviewer:
        raise PermissionDenied("提交者不能审核自己的事项")
