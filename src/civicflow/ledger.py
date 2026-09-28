"""不可变资金分录与冲正。"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN

from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .identifiers import new_id, require_safe
from .timeutil import Clock


def to_minor(value: str | int | Decimal, exponent: int = 2) -> int:
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValidationError("金额格式错误") from exc
    if not number.is_finite():
        raise ValidationError("金额必须是有限数")
    quantum = Decimal(1).scaleb(-exponent)
    return int(number.quantize(quantum, rounding=ROUND_HALF_EVEN).scaleb(exponent))


@dataclass(frozen=True)
class Ledger:
    database: Database
    clock: Clock

    def post(self, *, journal_key: str, account: str, currency: str, amount: str, direction: str, reference: str, actor: str) -> dict:
        require_safe(journal_key, "账簿标识"); require_safe(currency, "币种")
        if direction not in {"debit", "credit"}:
            raise ValidationError("方向必须是 debit 或 credit")
        minor = to_minor(amount)
        if minor <= 0:
            raise ValidationError("金额必须大于零")
        entry_id = new_id("entry")
        with self.database.transaction() as connection:
            duplicate = connection.execute("SELECT entry_id FROM journal_entries WHERE journal_key=? AND reference=? AND direction=?", (journal_key, reference, direction)).fetchone()
            if duplicate:
                raise ConflictError("相同参考号和方向已经入账")
            connection.execute("INSERT INTO journal_entries(entry_id,journal_key,account,currency,amount_minor,direction,reference,occurred_at,posted_by) VALUES(?,?,?,?,?,?,?,?,?)", (entry_id, journal_key, account, currency, minor, direction, reference, self.clock.now(), actor))
        return {"entry_id": entry_id, "amount_minor": minor, "direction": direction}

    def reverse(self, entry_id: str, *, reference: str, actor: str) -> dict:
        with self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM journal_entries WHERE entry_id=?", (entry_id,)).fetchone()
            if not row:
                raise NotFoundError("原分录不存在")
            existing = connection.execute("SELECT entry_id FROM journal_entries WHERE reversed_entry_id=?", (entry_id,)).fetchone()
            if existing:
                return {"entry_id": existing["entry_id"], "replayed": True}
            reversal = new_id("entry"); direction = "credit" if row["direction"] == "debit" else "debit"
            connection.execute("INSERT INTO journal_entries(entry_id,journal_key,account,currency,amount_minor,direction,reference,reversed_entry_id,occurred_at,posted_by) VALUES(?,?,?,?,?,?,?,?,?,?)", (reversal, row["journal_key"], row["account"], row["currency"], row["amount_minor"], direction, reference, entry_id, self.clock.now(), actor))
            return {"entry_id": reversal, "replayed": False}

    def balance(self, journal_key: str, *, currency: str) -> int:
        with self.database.connect() as connection:
            row = connection.execute("SELECT COALESCE(SUM(CASE direction WHEN 'debit' THEN amount_minor ELSE -amount_minor END),0) AS value FROM journal_entries WHERE journal_key=? AND currency=?", (journal_key, currency)).fetchone()
            return int(row["value"])
