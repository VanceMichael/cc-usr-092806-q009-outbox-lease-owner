"""可靠通知发件箱。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .database import Database
from .errors import ConflictError, NotFoundError
from .identifiers import new_id
from .jsonutil import canonical_json
from .timeutil import Clock, parse_instant


@dataclass(frozen=True)
class Outbox:
    database: Database
    clock: Clock

    def enqueue(self, *, topic: str, aggregate_id: str, payload: dict, available_at: str | None = None) -> str:
        message_id = new_id("msg"); available_at = available_at or self.clock.now()
        with self.database.transaction() as connection:
            connection.execute("INSERT INTO outbox_messages(message_id,topic,aggregate_id,payload_json,available_at,status) VALUES(?,?,?,?,?,?)", (message_id, topic, aggregate_id, canonical_json(payload), available_at, "pending"))
        return message_id

    def lease(self, *, owner: str, seconds: int = 30, limit: int = 20) -> list[dict]:
        now = parse_instant(self.clock.now()); lease_until = (now + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
        with self.database.transaction() as connection:
            rows = connection.execute("SELECT * FROM outbox_messages WHERE status IN ('pending','failed') AND available_at<=? AND (lease_until IS NULL OR lease_until<?) ORDER BY available_at,message_id LIMIT ?", (self.clock.now(), self.clock.now(), limit)).fetchall()
            result = []
            for row in rows:
                changed = connection.execute("UPDATE outbox_messages SET status='leased',lease_until=?,attempts=attempts+1 WHERE message_id=? AND (lease_until IS NULL OR lease_until<?)", (lease_until, row["message_id"], self.clock.now())).rowcount
                if changed:
                    item = dict(row); item["lease_owner"] = owner; item["lease_until"] = lease_until; result.append(item)
            return result

    def complete(self, message_id: str) -> None:
        with self.database.transaction() as connection:
            changed = connection.execute("UPDATE outbox_messages SET status='delivered',delivered_at=?,lease_until=NULL WHERE message_id=? AND status='leased'", (self.clock.now(), message_id)).rowcount
            if changed != 1:
                raise ConflictError("消息没有有效租约")

    def fail(self, message_id: str) -> None:
        with self.database.transaction() as connection:
            row = connection.execute("SELECT attempts FROM outbox_messages WHERE message_id=?", (message_id,)).fetchone()
            if not row:
                raise NotFoundError("消息不存在")
            status = "dead" if row["attempts"] >= 5 else "failed"
            connection.execute("UPDATE outbox_messages SET status=?,lease_until=NULL WHERE message_id=?", (status, message_id))
