"""可靠通知发件箱。

租约归属（lease_owner）与实际送达者（delivered_by）都持久化在数据库中，
完成、失败、租约过期和服务重启后都严格校验持有者身份：

* 只有持有效（未过期）租约的进程才能确认送达或标记失败；
* 非持有者的完成请求一律拒绝（PermissionDenied）；
* 同一持有者重复完成保持幂等，不会重复发送；
* 租约到期后消息才允许被其他进程重新领取，旧持有者的延迟确认会被拒绝；
* 领取、送达、失败、死亡和历史恢复全部写入追加式审计链。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .identifiers import new_id
from .jsonutil import canonical_json
from .timeutil import Clock, parse_instant

MAX_ATTEMPTS = 5
# 旧版本没有持久化发送者，迁移时只能以哨兵身份占位，避免任何真实进程冒认。
LEGACY_SENDER = "migration:legacy-sender"
MIGRATION_ACTOR = "migration:outbox"


def _require_owner(owner: str) -> str:
    if not isinstance(owner, str) or not owner.strip():
        raise ValidationError("租约持有者不能为空")
    return owner.strip()


@dataclass(frozen=True)
class Outbox:
    database: Database
    clock: Clock
    audit: AuditLog

    def enqueue(self, *, topic: str, aggregate_id: str, payload: dict, available_at: str | None = None) -> str:
        message_id = new_id("msg"); available_at = available_at or self.clock.now()
        with self.database.transaction() as connection:
            connection.execute("INSERT INTO outbox_messages(message_id,topic,aggregate_id,payload_json,available_at,status) VALUES(?,?,?,?,?,?)", (message_id, topic, aggregate_id, canonical_json(payload), available_at, "pending"))
        return message_id

    def get(self, message_id: str) -> dict:
        with self.database.transaction(immediate=False) as connection:
            row = connection.execute("SELECT * FROM outbox_messages WHERE message_id=?", (message_id,)).fetchone()
            if not row:
                raise NotFoundError("消息不存在")
            return dict(row)

    def lease(self, *, owner: str, seconds: int = 30, limit: int = 20) -> list[dict]:
        owner = _require_owner(owner)
        if seconds < 1 or limit < 1:
            raise ValidationError("租约参数不合法")
        now = self.clock.now()
        lease_until = (parse_instant(now) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
        with self.database.transaction() as connection:
            rows = connection.execute(
                "SELECT message_id FROM outbox_messages "
                "WHERE available_at<=? AND ("
                "status IN ('pending','failed') OR (status='leased' AND lease_until<=?)"
                ") ORDER BY available_at,message_id LIMIT ?",
                (now, now, limit),
            ).fetchall()
            leased = []
            for row in rows:
                # 重新校验状态与租约时间：并发进程即便拿到候选，也只有一个能更新成功。
                changed = connection.execute(
                    "UPDATE outbox_messages SET status='leased',lease_owner=?,lease_until=?,attempts=attempts+1 "
                    "WHERE message_id=? AND available_at<=? AND ("
                    "status IN ('pending','failed') OR (status='leased' AND lease_until<=?)"
                    ")",
                    (owner, lease_until, row["message_id"], now, now),
                ).rowcount
                if changed:
                    item = dict(connection.execute("SELECT * FROM outbox_messages WHERE message_id=?", (row["message_id"],)).fetchone())
                    leased.append(item)
                    self.audit.append(connection, actor_id=owner, action="outbox.leased", entity_type="outbox_message", entity_id=row["message_id"], version=0, detail={"lease_until": lease_until, "attempts": item["attempts"]})
            return leased

    def complete(self, message_id: str, *, owner: str) -> None:
        owner = _require_owner(owner)
        with self.database.transaction() as connection:
            row = connection.execute("SELECT status,lease_owner,lease_until,attempts,delivered_by FROM outbox_messages WHERE message_id=?", (message_id,)).fetchone()
            if not row:
                raise NotFoundError("消息不存在")
            # 已送达：同一持有者重复确认保持幂等；其他任何人确认都拒绝。
            if row["status"] == "delivered":
                if row["delivered_by"] == owner:
                    return
                raise PermissionDenied("消息已由其他进程确认送达")
            if row["status"] != "leased":
                raise ConflictError("消息没有有效租约")
            if row["lease_owner"] is None:
                # 旧版本残留的无主租约必须先经 recover_legacy 恢复，禁止冒认。
                raise ConflictError("消息租约缺少持久化持有者，需等待迁移恢复")
            if row["lease_owner"] != owner:
                raise PermissionDenied("消息由其他进程持有租约")
            if row["lease_until"] is None or parse_instant(row["lease_until"]) <= parse_instant(self.clock.now()):
                raise ConflictError("租约已过期，消息可能已被其他进程重新领取")
            now = self.clock.now()
            connection.execute(
                "UPDATE outbox_messages SET status='delivered',delivered_at=?,delivered_by=?,lease_until=NULL "
                "WHERE message_id=?",
                (now, owner, message_id),
            )
            self.audit.append(connection, actor_id=owner, action="outbox.delivered", entity_type="outbox_message", entity_id=message_id, version=0, detail={"delivered_at": now, "delivered_by": owner, "attempts": row["attempts"]})

    def fail(self, message_id: str, *, owner: str) -> None:
        owner = _require_owner(owner)
        with self.database.transaction() as connection:
            row = connection.execute("SELECT status,lease_owner,lease_until,attempts FROM outbox_messages WHERE message_id=?", (message_id,)).fetchone()
            if not row:
                raise NotFoundError("消息不存在")
            if row["status"] == "delivered":
                raise ConflictError("消息已送达，不能标记失败")
            if row["status"] != "leased":
                raise ConflictError("消息没有有效租约")
            if row["lease_owner"] is None:
                raise ConflictError("消息租约缺少持久化持有者，需等待迁移恢复")
            if row["lease_owner"] != owner:
                raise PermissionDenied("消息由其他进程持有租约")
            if row["lease_until"] is None or parse_instant(row["lease_until"]) <= parse_instant(self.clock.now()):
                raise ConflictError("租约已过期，失败回执不再有效")
            new_status = "dead" if row["attempts"] >= MAX_ATTEMPTS else "failed"
            connection.execute(
                "UPDATE outbox_messages SET status=?,lease_owner=NULL,lease_until=NULL WHERE message_id=?",
                (new_status, message_id),
            )
            self.audit.append(connection, actor_id=owner, action=f"outbox.{new_status}", entity_type="outbox_message", entity_id=message_id, version=0, detail={"attempts": row["attempts"]})

    def recover_legacy(self) -> None:
        """迁移旧版本发件箱数据（幂等）。

        * 旧库没有 lease_owner：残留在 leased 的消息无法确认持有者，恢复为 failed
          重新投送，绝不允许无主确认；
        * 旧库没有 delivered_by：已送达消息用迁移哨兵占位，保留原 delivered_at，
          任何真实进程的重复确认都会因持有者不符而被拒绝。
        pending/failed 消息原样保留，attempts 与状态不变。
        """
        with self.database.transaction() as connection:
            orphaned = connection.execute(
                "SELECT message_id,attempts FROM outbox_messages WHERE status='leased' AND lease_owner IS NULL"
            ).fetchall()
            for row in orphaned:
                connection.execute(
                    "UPDATE outbox_messages SET status='failed',lease_until=NULL,lease_owner=NULL WHERE message_id=?",
                    (row["message_id"],),
                )
                self.audit.append(connection, actor_id=MIGRATION_ACTOR, action="outbox.recovered", entity_type="outbox_message", entity_id=row["message_id"], version=0, detail={"from_status": "leased", "to_status": "failed", "attempts": row["attempts"]})
            unknown_senders = connection.execute(
                "SELECT message_id,delivered_at FROM outbox_messages WHERE status='delivered' AND delivered_by IS NULL"
            ).fetchall()
            for row in unknown_senders:
                connection.execute(
                    "UPDATE outbox_messages SET delivered_by=? WHERE message_id=?",
                    (LEGACY_SENDER, row["message_id"]),
                )
                self.audit.append(connection, actor_id=MIGRATION_ACTOR, action="outbox.delivered_backfill", entity_type="outbox_message", entity_id=row["message_id"], version=0, detail={"delivered_at": row["delivered_at"], "delivered_by": LEGACY_SENDER})
