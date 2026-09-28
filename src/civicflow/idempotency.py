"""请求幂等记录。"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Callable, TypeVar

from .errors import ConflictError
from .jsonutil import canonical_json, digest_json
from .timeutil import Clock

T = TypeVar("T")


@dataclass(frozen=True)
class IdempotencyStore:
    clock: Clock

    def execute(self, connection: sqlite3.Connection, *, scope: str, request_key: str, request: dict, operation: Callable[[], T]) -> T:
        request_digest = digest_json(request)
        row = connection.execute("SELECT request_digest,response_json FROM idempotency_keys WHERE scope=? AND request_key=?", (scope, request_key)).fetchone()
        if row:
            if row["request_digest"] != request_digest:
                raise ConflictError("相同请求标识对应不同内容")
            return json.loads(row["response_json"])
        response = operation()
        connection.execute("INSERT INTO idempotency_keys(scope,request_key,request_digest,response_json,created_at) VALUES(?,?,?,?,?)", (scope, request_key, request_digest, canonical_json(response), self.clock.now()))
        return response
