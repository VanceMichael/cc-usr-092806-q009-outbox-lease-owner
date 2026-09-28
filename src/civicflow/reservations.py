"""容量资源预约与冲突控制。"""

from __future__ import annotations

from dataclasses import dataclass

from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .identifiers import new_id
from .timeutil import canonical_instant, parse_instant


@dataclass(frozen=True)
class ReservationBook:
    database: Database

    def reserve(self, *, resource_id: str, subject_id: str, quantity: int, capacity: int, start_at: str, end_at: str, actor: str) -> dict:
        start_at = canonical_instant(start_at); end_at = canonical_instant(end_at)
        if parse_instant(start_at) >= parse_instant(end_at):
            raise ValidationError("预约结束时间必须晚于开始时间")
        if quantity <= 0 or capacity <= 0 or quantity > capacity:
            raise ValidationError("预约数量或容量不合法")
        with self.database.transaction() as connection:
            row = connection.execute("SELECT COALESCE(SUM(quantity),0) AS used FROM resource_reservations WHERE resource_id=? AND status IN ('held','confirmed') AND start_at<? AND end_at>?", (resource_id, end_at, start_at)).fetchone()
            if int(row["used"]) + quantity > capacity:
                raise ConflictError("资源容量不足")
            reservation_id = new_id("reservation")
            connection.execute("INSERT INTO resource_reservations(reservation_id,resource_id,subject_id,quantity,start_at,end_at,status,version,created_by) VALUES(?,?,?,?,?,?,?,?,?)", (reservation_id, resource_id, subject_id, quantity, start_at, end_at, "confirmed", 1, actor))
            return {"reservation_id": reservation_id, "status": "confirmed", "version": 1}

    def release(self, reservation_id: str, *, expected_version: int) -> dict:
        with self.database.transaction() as connection:
            changed = connection.execute("UPDATE resource_reservations SET status='released',version=version+1 WHERE reservation_id=? AND version=? AND status IN ('held','confirmed')", (reservation_id, expected_version)).rowcount
            if changed != 1:
                row = connection.execute("SELECT * FROM resource_reservations WHERE reservation_id=?", (reservation_id,)).fetchone()
                if not row:
                    raise NotFoundError("预约不存在")
                raise ConflictError("预约版本或状态已经变化")
            row = connection.execute("SELECT * FROM resource_reservations WHERE reservation_id=?", (reservation_id,)).fetchone()
            return dict(row)

    def usage(self, resource_id: str, *, at: str) -> int:
        instant = canonical_instant(at)
        with self.database.connect() as connection:
            row = connection.execute("SELECT COALESCE(SUM(quantity),0) AS used FROM resource_reservations WHERE resource_id=? AND status IN ('held','confirmed') AND start_at<=? AND end_at>?", (resource_id, instant, instant)).fetchone()
            return int(row["used"])
