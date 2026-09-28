"""inbox_records 领域服务。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .repository import EntityRepository
from .security import AccessContext, redact_record


ENTITY_TYPE = 'inbox_records'
STATES = ('received', 'accepted', 'duplicate', 'conflict', 'rejected',)
FIELDS = ('source', 'source_key', 'sequence', 'payload_digest', 'occurred_at',)
RESTRICTED_FIELDS = ('source',)
TRANSITIONS = {
    'received': {'accepted'},
    'accepted': {'duplicate'},
    'duplicate': {'conflict'},
    'conflict': {'rejected'},
    'rejected': {'rejected'},
}


@dataclass(frozen=True)
class InboxRecordService:
    """封装 inbox_records 的创建、转换、查询和历史读取。"""

    _repository: EntityRepository

    @property
    def entity_type(self) -> str:
        return ENTITY_TYPE

    def create(self, context: AccessContext, values: Mapping[str, object], *, request_key: str) -> dict:
        context.require("write:inbox_records")
        payload = self._validate(values, partial=False)
        payload["state"] = STATES[0]
        return self._repository.create(self.entity_type, payload, actor=context.actor_id, request_key=request_key)

    def revise(self, context: AccessContext, entity_id: str, values: Mapping[str, object], *, expected_version: int, request_key: str) -> dict:
        context.require("write:inbox_records")
        payload = self._validate(values, partial=True)
        if "state" in payload:
            raise ValidationError("状态必须通过 transition 修改")
        return self._repository.update(self.entity_type, entity_id, payload, actor=context.actor_id, expected_version=expected_version, request_key=request_key)

    def transition(self, context: AccessContext, entity_id: str, target: str, *, expected_version: int, reason: str, request_key: str) -> dict:
        context.require("transition:inbox_records")
        current = self._repository.get(self.entity_type, entity_id)
        allowed = TRANSITIONS.get(str(current["state"]), set())
        if target not in allowed:
            raise ConflictError(f"不允许从 {current['state']} 转到 {target}")
        if not reason.strip():
            raise ValidationError("状态变更必须说明原因")
        return self._repository.update(self.entity_type, entity_id, {"state": target, "transition_reason": reason.strip()}, actor=context.actor_id, expected_version=expected_version, request_key=request_key)

    def get(self, context: AccessContext, entity_id: str) -> dict:
        context.require("read:inbox_records")
        record = self._repository.get(self.entity_type, entity_id)
        return redact_record(record, RESTRICTED_FIELDS, context)

    def list_current(self, context: AccessContext, *, state: str | None = None, limit: int = 100) -> list[dict]:
        context.require("read:inbox_records")
        if state is not None and state not in STATES:
            raise ValidationError("未知状态")
        rows = self._repository.list(self.entity_type, state=state, limit=limit)
        return [redact_record(row, RESTRICTED_FIELDS, context) for row in rows]

    def history(self, context: AccessContext, entity_id: str) -> list[dict]:
        context.require("history:inbox_records")
        rows = self._repository.history(self.entity_type, entity_id)
        if not rows:
            raise NotFoundError(f"{self.entity_type}/{entity_id} 不存在")
        return [redact_record(row, RESTRICTED_FIELDS, context) for row in rows]

    def snapshot(self, context: AccessContext, entity_id: str, *, as_of: str) -> dict:
        context.require("history:inbox_records")
        record = self._repository.snapshot(self.entity_type, entity_id, as_of=as_of)
        return redact_record(record, RESTRICTED_FIELDS, context)

    def bulk_get(self, context: AccessContext, entity_ids: Iterable[str]) -> list[dict]:
        context.require("read:inbox_records")
        result = []
        for entity_id in dict.fromkeys(entity_ids):
            result.append(self.get(context, entity_id))
        return result

    def find_by_source(self, value: object, *, limit: int = 100) -> list[dict]:
        """按 source 查询并保持稳定顺序。"""
        return self._repository.search(self.entity_type, 'source', value, limit=limit)

    def find_by_source_key(self, value: object, *, limit: int = 100) -> list[dict]:
        """按 source_key 查询并保持稳定顺序。"""
        return self._repository.search(self.entity_type, 'source_key', value, limit=limit)

    def find_by_sequence(self, value: object, *, limit: int = 100) -> list[dict]:
        """按 sequence 查询并保持稳定顺序。"""
        return self._repository.search(self.entity_type, 'sequence', value, limit=limit)

    def find_by_payload_digest(self, value: object, *, limit: int = 100) -> list[dict]:
        """按 payload_digest 查询并保持稳定顺序。"""
        return self._repository.search(self.entity_type, 'payload_digest', value, limit=limit)

    def find_by_occurred_at(self, value: object, *, limit: int = 100) -> list[dict]:
        """按 occurred_at 查询并保持稳定顺序。"""
        return self._repository.search(self.entity_type, 'occurred_at', value, limit=limit)

    def _validate(self, values: Mapping[str, object], *, partial: bool) -> dict:
        unknown = set(values) - set(FIELDS) - {"state", "transition_reason"}
        if unknown:
            raise ValidationError("未知字段: " + ", ".join(sorted(unknown)))
        if not partial:
            missing = [field for field in FIELDS if field not in values]
            if missing:
                raise ValidationError("缺少字段: " + ", ".join(missing))
        payload = dict(values)
        for key, value in payload.items():
            if value is None:
                raise ValidationError(f"{key} 不能为空")
            if isinstance(value, str):
                payload[key] = value.strip()
                if not payload[key]:
                    raise ValidationError(f"{key} 不能为空字符串")
        return payload
