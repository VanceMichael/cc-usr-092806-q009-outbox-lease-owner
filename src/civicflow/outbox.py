"""可靠通知发件箱。

租约归属（lease_owner）持久化在数据库中：完成、失败都严格校验持有者身份与
租约有效期；非持有者的确认一律拒绝；同一持有者对已送达消息的重复确认保持
幂等；租约到期后其他进程才可重新领取。每次领取、完成、失败都追加审计记录，
实际发送者随 delivered_by 永久保存。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .database import Database
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .identifiers import new_id
from .jsonutil import canonical_json
from .timeutil import Clock, parse_instant


MAX_ATTEMPTS = 5


@dataclass(frozen=True)
class Outbox:
    database: Database
    clock: Clock

    def enqueue(self, *, topic: str, aggregate_id: str, payload: dict, available_at: str | None = None) -> str:
        message_id = new_id("msg"); available_at = available_at or self.clock.now()
        with self.database.transaction() as connection:
            connection.execute("INSERT INTO outbox_messages(message_id,topic,aggregate_id,payload_json,available_at,status) VALUES(?,?,?,?,?,?)", (message_id, topic, aggregate_id, canonical_json(payload), available_at, "pending"))
            self._audit(connection, message_id, action="enqueued", actor="system", attempts=0, detail={"topic": topic, "aggregate_id": aggregate_id})
        return message_id

    def lease(self, *, owner: str, seconds: int = 30, limit: int = 20) -> list[dict]:
        owner = self._require_owner(owner)
        if seconds < 1 or limit < 1:
            raise ValidationError("租约参数不合法")
        now = self.clock.now()
        lease_until = (parse_instant(now) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
        reclaimable = (
            "(status IN ('pending','failed') AND (lease_until IS NULL OR lease_until<?)) "
            "OR (status='leased' AND lease_until IS NOT NULL AND lease_until<=?)"
        )
        with self.database.transaction() as connection:
            rows = connection.execute(
                f"SELECT * FROM outbox_messages WHERE available_at<=? AND ({reclaimable}) "
                "ORDER BY available_at,message_id LIMIT ?",
                (now, now, now, limit),
            ).fetchall()
            result = []
            for row in rows:
                changed = connection.execute(
                    "UPDATE outbox_messages SET status='leased',lease_owner=?,lease_until=?,attempts=attempts+1 "
                    f"WHERE message_id=? AND available_at<=? AND ({reclaimable})",
                    (owner, lease_until, row["message_id"], now, now, now),
                ).rowcount
                if changed:
                    claimed = connection.execute("SELECT * FROM outbox_messages WHERE message_id=?", (row["message_id"],)).fetchone()
                    self._audit(connection, row["message_id"], action="leased", actor=owner, attempts=claimed["attempts"], detail={"lease_until": lease_until, "reclaimed": row["status"] == "leased"})
                    result.append(dict(claimed))
            return result

    def complete(self, message_id: str, owner: str) -> None:
        owner = self._require_owner(owner)
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM outbox_messages WHERE message_id=?", (message_id,)).fetchone()
            if not row:
                raise NotFoundError("消息不存在")
            if row["status"] == "delivered":
                # 只有实际发送者的重复确认幂等成功，其他进程一律拒绝，防止无授权确认。
                if row["delivered_by"] != owner:
                    raise PermissionDenied("消息已由其他进程送达，非持有者不能确认")
                return
            if row["status"] != "leased":
                raise ConflictError("消息没有有效租约")
            if row["lease_owner"] != owner:
                raise PermissionDenied("不是当前租约持有者，不能确认发送")
            if row["lease_until"] is None or parse_instant(row["lease_until"]) <= parse_instant(self.clock.now()):
                raise ConflictError("租约已过期，不能确认发送")
            delivered_at = self.clock.now()
            connection.execute(
                "UPDATE outbox_messages SET status='delivered',delivered_at=?,delivered_by=?,lease_until=NULL,lease_owner=NULL WHERE message_id=?",
                (delivered_at, owner, message_id),
            )
            self._audit(connection, message_id, action="delivered", actor=owner, attempts=row["attempts"], detail={"delivered_at": delivered_at})

    def fail(self, message_id: str, owner: str) -> None:
        owner = self._require_owner(owner)
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM outbox_messages WHERE message_id=?", (message_id,)).fetchone()
            if not row:
                raise NotFoundError("消息不存在")
            if row["status"] == "delivered":
                raise ConflictError("消息已送达，不能标记失败")
            if row["status"] != "leased":
                raise ConflictError("消息没有有效租约")
            if row["lease_owner"] != owner:
                raise PermissionDenied("不是当前租约持有者，不能标记失败")
            if row["lease_until"] is None or parse_instant(row["lease_until"]) <= parse_instant(self.clock.now()):
                raise ConflictError("租约已过期，不能标记失败")
            status = "dead" if row["attempts"] >= MAX_ATTEMPTS else "failed"
            connection.execute(
                "UPDATE outbox_messages SET status=?,lease_until=NULL,lease_owner=NULL WHERE message_id=?",
                (status, message_id),
            )
            self._audit(connection, message_id, action=status, actor=owner, attempts=row["attempts"], detail={})

    def audit_trail(self, message_id: str) -> list[dict]:
        with self.database.connect() as connection:
            rows = connection.execute("SELECT * FROM outbox_audit WHERE message_id=? ORDER BY audit_id", (message_id,)).fetchall()
            return [dict(row) for row in rows]

    @staticmethod
    def _require_owner(owner: str) -> str:
        if not isinstance(owner, str) or not owner.strip():
            raise ValidationError("租约持有者不能为空")
        return owner.strip()

    def _audit(self, connection, message_id: str, *, action: str, actor: str, attempts: int, detail: dict) -> None:
        connection.execute(
            "INSERT INTO outbox_audit(occurred_at,message_id,action,actor_id,attempts,detail_json) VALUES(?,?,?,?,?,?)",
            (self.clock.now(), message_id, action, actor, attempts, canonical_json(detail)),
        )
