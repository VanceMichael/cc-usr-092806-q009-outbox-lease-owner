"""版本化实体持久化。"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .identifiers import new_id, require_safe
from .idempotency import IdempotencyStore
from .jsonutil import canonical_json
from .timeutil import Clock, canonical_instant


@dataclass(frozen=True)
class EntityRepository:
    database: Database
    clock: Clock
    audit: AuditLog
    idempotency: IdempotencyStore

    def create(self, entity_type: str, payload: dict, *, actor: str, request_key: str) -> dict:
        require_safe(entity_type, "实体类型")
        with self.database.transaction() as connection:
            def operation() -> dict:
                entity_id = new_id(entity_type)
                now = self.clock.now()
                state = str(payload.get("state", "draft"))
                body = dict(payload)
                body["state"] = state
                connection.execute("INSERT INTO entities(entity_type,entity_id,version,state,payload_json,created_at,updated_at,created_by,updated_by) VALUES(?,?,?,?,?,?,?,?,?)", (entity_type, entity_id, 1, state, canonical_json(body), now, now, actor, actor))
                connection.execute("INSERT INTO entity_versions(entity_type,entity_id,version,state,payload_json,valid_from,actor_id,request_key) VALUES(?,?,?,?,?,?,?,?)", (entity_type, entity_id, 1, state, canonical_json(body), now, actor, request_key))
                self.audit.append(connection, actor_id=actor, action="create", entity_type=entity_type, entity_id=entity_id, version=1, detail=body)
                return self._row_to_dict(connection.execute("SELECT * FROM entities WHERE entity_type=? AND entity_id=?", (entity_type, entity_id)).fetchone())
            return self.idempotency.execute(connection, scope=f"create:{entity_type}", request_key=request_key, request=payload, operation=operation)

    def update(self, entity_type: str, entity_id: str, changes: dict, *, actor: str, expected_version: int, request_key: str) -> dict:
        if not changes:
            raise ValidationError("修改内容不能为空")
        with self.database.transaction() as connection:
            def operation() -> dict:
                row = connection.execute("SELECT * FROM entities WHERE entity_type=? AND entity_id=?", (entity_type, entity_id)).fetchone()
                if not row:
                    raise NotFoundError(f"{entity_type}/{entity_id} 不存在")
                if row["version"] != expected_version:
                    raise ConflictError(f"版本冲突，当前为 {row['version']}")
                payload = json.loads(row["payload_json"]); payload.update(changes)
                version = expected_version + 1
                state = str(payload.get("state", row["state"]))
                now = self.clock.now()
                changed = connection.execute("UPDATE entities SET version=?,state=?,payload_json=?,updated_at=?,updated_by=? WHERE entity_type=? AND entity_id=? AND version=?", (version, state, canonical_json(payload), now, actor, entity_type, entity_id, expected_version)).rowcount
                if changed != 1:
                    raise ConflictError("并发修改导致版本变化")
                connection.execute("INSERT INTO entity_versions(entity_type,entity_id,version,state,payload_json,valid_from,actor_id,request_key) VALUES(?,?,?,?,?,?,?,?)", (entity_type, entity_id, version, state, canonical_json(payload), now, actor, request_key))
                self.audit.append(connection, actor_id=actor, action="update", entity_type=entity_type, entity_id=entity_id, version=version, detail=changes)
                return self._row_to_dict(connection.execute("SELECT * FROM entities WHERE entity_type=? AND entity_id=?", (entity_type, entity_id)).fetchone())
            return self.idempotency.execute(connection, scope=f"update:{entity_type}:{entity_id}", request_key=request_key, request={"changes": changes, "expected_version": expected_version}, operation=operation)

    def get(self, entity_type: str, entity_id: str) -> dict:
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM entities WHERE entity_type=? AND entity_id=?", (entity_type, entity_id)).fetchone()
            if not row:
                raise NotFoundError(f"{entity_type}/{entity_id} 不存在")
            return self._row_to_dict(row)

    def list(self, entity_type: str, *, state: str | None = None, limit: int = 100) -> list[dict]:
        if limit < 1 or limit > 500:
            raise ValidationError("limit 必须在 1 到 500 之间")
        sql = "SELECT * FROM entities WHERE entity_type=?"; params: list[object] = [entity_type]
        if state is not None:
            sql += " AND state=?"; params.append(state)
        sql += " ORDER BY updated_at, entity_id LIMIT ?"; params.append(limit)
        with self.database.connect() as connection:
            return [self._row_to_dict(row) for row in connection.execute(sql, params)]

    def search(self, entity_type: str, field: str, value: object, *, limit: int = 100) -> list[dict]:
        rows = self.list(entity_type, limit=500)
        return [row for row in rows if row.get(field) == value][:limit]

    def history(self, entity_type: str, entity_id: str) -> list[dict]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT * FROM entity_versions WHERE entity_type=? AND entity_id=? ORDER BY version", (entity_type, entity_id)).fetchall()
            return [self._version_to_dict(row) for row in rows]

    def snapshot(self, entity_type: str, entity_id: str, *, as_of: str) -> dict:
        instant = canonical_instant(as_of)
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM entity_versions WHERE entity_type=? AND entity_id=? AND valid_from<=? ORDER BY valid_from DESC,version DESC LIMIT 1", (entity_type, entity_id, instant)).fetchone()
            if not row:
                raise NotFoundError("指定时点没有可见版本")
            return self._version_to_dict(row)

    @staticmethod
    def _row_to_dict(row) -> dict:
        payload = json.loads(row["payload_json"]); payload.update({"entity_type": row["entity_type"], "entity_id": row["entity_id"], "version": row["version"], "state": row["state"], "created_at": row["created_at"], "updated_at": row["updated_at"], "created_by": row["created_by"], "updated_by": row["updated_by"]}); return payload

    @staticmethod
    def _version_to_dict(row) -> dict:
        payload = json.loads(row["payload_json"]); payload.update({"entity_type": row["entity_type"], "entity_id": row["entity_id"], "version": row["version"], "state": row["state"], "valid_from": row["valid_from"], "actor_id": row["actor_id"], "request_key": row["request_key"]}); return payload
