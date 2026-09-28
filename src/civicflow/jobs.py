"""可恢复的定时任务队列。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .identifiers import new_id
from .jsonutil import canonical_json
from .timeutil import Clock, canonical_instant, parse_instant


@dataclass(frozen=True)
class JobQueue:
    database: Database
    clock: Clock

    def schedule(self, *, job_type: str, subject_id: str, run_at: str, payload: dict) -> str:
        job_id = new_id("job"); run_at = canonical_instant(run_at)
        with self.database.transaction() as connection:
            connection.execute("INSERT INTO scheduled_jobs(job_id,job_type,subject_id,run_at,payload_json,status) VALUES(?,?,?,?,?,'waiting')", (job_id, job_type, subject_id, run_at, canonical_json(payload)))
        return job_id

    def claim_due(self, *, seconds: int = 30, limit: int = 20) -> list[dict]:
        if seconds < 1 or limit < 1:
            raise ValidationError("租约参数不合法")
        lease_until = (parse_instant(self.clock.now()) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
        with self.database.transaction() as connection:
            rows = connection.execute("SELECT * FROM scheduled_jobs WHERE run_at<=? AND status IN ('waiting','retry') AND (lease_until IS NULL OR lease_until<?) ORDER BY run_at,job_id LIMIT ?", (self.clock.now(), self.clock.now(), limit)).fetchall()
            result = []
            for row in rows:
                changed = connection.execute("UPDATE scheduled_jobs SET status='running',lease_until=?,attempt=attempt+1 WHERE job_id=? AND (lease_until IS NULL OR lease_until<?)", (lease_until, row["job_id"], self.clock.now())).rowcount
                if changed:
                    item = dict(row); item["lease_until"] = lease_until; result.append(item)
            return result

    def finish(self, job_id: str) -> None:
        with self.database.transaction() as connection:
            changed = connection.execute("UPDATE scheduled_jobs SET status='succeeded',lease_until=NULL,last_error='' WHERE job_id=? AND status='running'", (job_id,)).rowcount
            if changed != 1:
                raise ConflictError("任务没有有效运行租约")

    def retry(self, job_id: str, *, error: str, retry_at: str) -> None:
        retry_at = canonical_instant(retry_at)
        with self.database.transaction() as connection:
            row = connection.execute("SELECT attempt FROM scheduled_jobs WHERE job_id=?", (job_id,)).fetchone()
            if not row:
                raise NotFoundError("任务不存在")
            status = "failed" if row["attempt"] >= 5 else "retry"
            connection.execute("UPDATE scheduled_jobs SET status=?,run_at=?,lease_until=NULL,last_error=? WHERE job_id=?", (status, retry_at, error[:500], job_id))
