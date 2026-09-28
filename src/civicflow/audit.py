"""追加式审计链。"""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass

from .errors import InvariantViolation
from .jsonutil import canonical_json
from .timeutil import Clock


@dataclass(frozen=True)
class AuditLog:
    clock: Clock

    def append(self, connection: sqlite3.Connection, *, actor_id: str, action: str, entity_type: str, entity_id: str, version: int, detail: dict) -> str:
        row = connection.execute("SELECT entry_digest FROM audit_entries ORDER BY audit_id DESC LIMIT 1").fetchone()
        previous = row["entry_digest"] if row else "0" * 64
        occurred_at = self.clock.now()
        body = canonical_json({"occurred_at": occurred_at, "actor_id": actor_id, "action": action, "entity_type": entity_type, "entity_id": entity_id, "version": version, "detail": detail, "previous": previous})
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        connection.execute("INSERT INTO audit_entries(occurred_at,actor_id,action,entity_type,entity_id,version,detail_json,previous_digest,entry_digest) VALUES(?,?,?,?,?,?,?,?,?)", (occurred_at, actor_id, action, entity_type, entity_id, version, canonical_json(detail), previous, digest))
        return digest

    def verify(self, connection: sqlite3.Connection) -> int:
        previous = "0" * 64
        count = 0
        for row in connection.execute("SELECT * FROM audit_entries ORDER BY audit_id"):
            body = canonical_json({"occurred_at": row["occurred_at"], "actor_id": row["actor_id"], "action": row["action"], "entity_type": row["entity_type"], "entity_id": row["entity_id"], "version": row["version"], "detail": __import__("json").loads(row["detail_json"]), "previous": previous})
            expected = hashlib.sha256(body.encode("utf-8")).hexdigest()
            if row["previous_digest"] != previous or row["entry_digest"] != expected:
                raise InvariantViolation(f"审计链在 {row['audit_id']} 处不连续")
            previous = expected
            count += 1
        return count
