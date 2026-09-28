"""带来源序号的消息接入。"""

from __future__ import annotations

from dataclasses import dataclass

from .database import Database
from .errors import ConflictError, ValidationError
from .identifiers import require_safe
from .jsonutil import canonical_json, digest_json
from .timeutil import Clock, canonical_instant


@dataclass(frozen=True)
class Inbox:
    database: Database
    clock: Clock

    def receive(self, *, source: str, source_key: str, sequence: int, payload: dict, occurred_at: str) -> dict:
        require_safe(source, "来源"); require_safe(source_key, "来源标识")
        if sequence < 0:
            raise ValidationError("来源序号不能为负数")
        occurred_at = canonical_instant(occurred_at); digest = digest_json(payload)
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM inbox_messages WHERE source=? AND source_key=? AND sequence=?", (source, source_key, sequence)).fetchone()
            if row:
                if row["payload_digest"] != digest:
                    connection.execute("INSERT INTO inbox_conflicts(source,source_key,sequence,existing_digest,incoming_digest,received_at) VALUES(?,?,?,?,?,?)", (source, source_key, sequence, row["payload_digest"], digest, self.clock.now()))
                    raise ConflictError("相同来源序号出现不同内容")
                return {"status": "duplicate", "digest": digest}
            connection.execute("INSERT INTO inbox_messages(source,source_key,sequence,payload_digest,payload_json,occurred_at,received_at,status) VALUES(?,?,?,?,?,?,?,?)", (source, source_key, sequence, digest, canonical_json(payload), occurred_at, self.clock.now(), "accepted"))
            return {"status": "accepted", "digest": digest}

    def timeline(self, source: str, source_key: str) -> list[dict]:
        with self.database.connect() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM inbox_messages WHERE source=? AND source_key=? ORDER BY occurred_at,sequence", (source, source_key))]
