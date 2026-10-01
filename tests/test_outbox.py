"""发件箱租约归属与恢复逻辑的自动化验证。

覆盖：双进程竞争领取、错误持有者确认/失败、同一持有者重复完成幂等、
租约过期后重新领取、服务重启后归属校验、历史消息迁移不丢数据。
"""

from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.errors import ConflictError, PermissionDenied, ValidationError


def open_app(path, fixed_now="2026-09-28T04:00:00Z"):
    return CivicFlow.open(path, fixed_now=fixed_now)


class OutboxLeaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "outbox.sqlite3"
        self.app = open_app(self.db_path)

    def tearDown(self):
        self.temp.cleanup()

    def _row(self, message_id):
        with self.app.database.connect() as connection:
            return dict(connection.execute("SELECT * FROM outbox_messages WHERE message_id=?", (message_id,)).fetchone())

    def test_owner_is_persisted_not_just_in_memory(self):
        message = self.app.outbox.enqueue(topic="warn", aggregate_id="case:1", payload={})
        leased = self.app.outbox.lease(owner="duty-a")
        self.assertEqual(leased[0]["lease_owner"], "duty-a")
        self.assertEqual(self._row(message)["lease_owner"], "duty-a")

    def test_second_worker_cannot_claim_or_complete_leased_message(self):
        message = self.app.outbox.enqueue(topic="warn", aggregate_id="case:1", payload={})
        self.app.outbox.lease(owner="duty-a")
        # 租约有效期内其他进程领不到，也不能强行确认。
        self.assertEqual(self.app.outbox.lease(owner="duty-b"), [])
        with self.assertRaises(PermissionDenied):
            self.app.outbox.complete(message, owner="duty-b")
        with self.assertRaises(PermissionDenied):
            self.app.outbox.fail(message, owner="duty-b")
        row = self._row(message)
        self.assertEqual(row["status"], "leased")
        self.assertEqual(row["lease_owner"], "duty-a")

    def test_same_holder_duplicate_complete_is_idempotent(self):
        message = self.app.outbox.enqueue(topic="rescue", aggregate_id="case:1", payload={})
        self.app.outbox.lease(owner="duty-a")
        self.app.outbox.complete(message, owner="duty-a")
        # 网络抖动导致的重复确认：同一持有者幂等成功，不重复发送、不重复审计。
        self.app.outbox.complete(message, owner="duty-a")
        row = self._row(message)
        self.assertEqual(row["status"], "delivered")
        self.assertEqual(row["delivered_by"], "duty-a")
        delivered_events = [e for e in self.app.outbox.audit_trail(message) if e["action"] == "delivered"]
        self.assertEqual(len(delivered_events), 1)
        self.assertEqual(delivered_events[0]["actor_id"], "duty-a")

    def test_delivered_message_cannot_be_confirmed_by_anyone_else(self):
        message = self.app.outbox.enqueue(topic="rescue", aggregate_id="case:1", payload={})
        self.app.outbox.lease(owner="duty-a")
        self.app.outbox.complete(message, owner="duty-a")
        with self.assertRaises(PermissionDenied):
            self.app.outbox.complete(message, owner="duty-b")

    def test_lease_expiry_allows_reclaim_then_only_new_holder_may_confirm(self):
        message = self.app.outbox.enqueue(topic="warn", aggregate_id="case:1", payload={})
        self.app.outbox.lease(owner="duty-a", seconds=30)
        # 时间推进到租约到期后重启一个值班进程。
        later = open_app(self.db_path, fixed_now="2026-09-28T04:01:00Z")
        # 租约已过期但还没被重新领取：原持有者不能再确认。
        with self.assertRaises(ConflictError):
            later.outbox.complete(message, owner="duty-a")
        reclaimed = later.outbox.lease(owner="duty-b", seconds=30)
        self.assertEqual([item["message_id"] for item in reclaimed], [message])
        self.assertEqual(reclaimed[0]["lease_owner"], "duty-b")
        self.assertEqual(reclaimed[0]["attempts"], 2)
        # 原持有者已不是当前租约持有者，不能再确认；新持有者可以。
        with self.assertRaises(PermissionDenied):
            later.outbox.complete(message, owner="duty-a")
        later.outbox.complete(message, owner="duty-b")
        self.assertEqual(self._row(message)["delivered_by"], "duty-b")

    def test_message_cannot_be_reclaimed_before_expiry(self):
        message = self.app.outbox.enqueue(topic="warn", aggregate_id="case:1", payload={})
        self.app.outbox.lease(owner="duty-a", seconds=30)
        later = open_app(self.db_path, fixed_now="2026-09-28T04:00:20Z")
        self.assertEqual(later.outbox.lease(owner="duty-b"), [])
        later.outbox.complete(message, owner="duty-a")

    def test_restart_preserves_lease_ownership(self):
        message = self.app.outbox.enqueue(topic="warn", aggregate_id="case:1", payload={})
        self.app.outbox.lease(owner="duty-a", seconds=300)
        # 服务重启：归属持久化，非持有者仍被拒绝，持有者可以继续完成。
        restarted = open_app(self.db_path)
        with self.assertRaises(PermissionDenied):
            restarted.outbox.complete(message, owner="duty-b")
        restarted.outbox.complete(message, owner="duty-a")
        self.assertEqual(self._row(message)["status"], "delivered")

    def test_failed_message_becomes_retryable_then_dead(self):
        message = self.app.outbox.enqueue(topic="warn", aggregate_id="case:1", payload={})
        for expected_attempt in range(1, 5):
            app = open_app(self.db_path, fixed_now=f"2026-09-28T04:{expected_attempt:02d}:00Z")
            claimed = app.outbox.lease(owner="duty-a", seconds=30)
            self.assertEqual(claimed[0]["attempts"], expected_attempt)
            app.outbox.fail(message, owner="duty-a")
            self.assertEqual(self._row(message)["status"], "failed")
        # 第 5 次领取后失败进入 dead，不再被领取。
        app = open_app(self.db_path, fixed_now="2026-09-28T04:05:00Z")
        self.assertEqual(app.outbox.lease(owner="duty-a")[0]["attempts"], 5)
        app.outbox.fail(message, owner="duty-a")
        self.assertEqual(self._row(message)["status"], "dead")
        self.assertEqual(app.outbox.lease(owner="duty-a"), [])

    def test_empty_owner_rejected(self):
        self.app.outbox.enqueue(topic="warn", aggregate_id="c", payload={})
        with self.assertRaises(ValidationError):
            self.app.outbox.lease(owner="   ")

    def test_concurrent_two_processes_no_double_send(self):
        total = 50
        ids = [self.app.outbox.enqueue(topic="warn", aggregate_id=f"case:{i}", payload={"i": i}) for i in range(total)]
        workers = ["duty-a", "duty-b"]
        claimed = {name: [] for name in workers}
        errors = []

        def run(name):
            # 每个线程独立打开同一个数据库文件，模拟两个值班进程。
            app = CivicFlow.open(self.db_path)
            try:
                items = app.outbox.lease(owner=name, seconds=60, limit=total)
                claimed[name] = [item["message_id"] for item in items]
                for message_id in claimed[name]:
                    app.outbox.complete(message_id, owner=name)
                    # 网络抖动重放：同一持有者重复完成必须幂等。
                    app.outbox.complete(message_id, owner=name)
            except BaseException as exc:  # pragma: no cover - 测试失败时记录
                errors.append(exc)

        threads = [threading.Thread(target=run, args=(name,)) for name in workers]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        a, b = set(claimed["duty-a"]), set(claimed["duty-b"])
        self.assertTrue(a & b == set(), "同一条消息被两个进程重复领取")
        self.assertEqual(a | b, set(ids), "有消息在竞争中丢失")
        with self.app.database.connect() as connection:
            delivered = connection.execute("SELECT message_id,delivered_by FROM outbox_messages WHERE status='delivered'").fetchall()
        self.assertEqual(len(delivered), total)
        self.assertTrue({row["delivered_by"] for row in delivered} <= set(workers))
        # 每条消息仅有一条 delivered 审计：实际发送者可追溯，无重复发送。
        with self.app.database.connect() as connection:
            counts = dict(connection.execute("SELECT message_id, COUNT(*) FROM outbox_audit WHERE action='delivered' GROUP BY message_id").fetchall())
        self.assertTrue(all(count == 1 for count in counts.values()))


class OutboxMigrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "legacy.sqlite3"

    def tearDown(self):
        self.temp.cleanup()

    def _build_legacy_database(self):
        # 旧版表结构：没有 lease_owner / delivered_by，也没有 outbox_audit。
        connection = sqlite3.connect(self.db_path)
        connection.executescript(
            """
            CREATE TABLE outbox_messages (
                message_id TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                available_at TEXT NOT NULL,
                lease_until TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                delivered_at TEXT
            );
            """
        )
        rows = [
            ("msg-pending", "warn", "case:1", "{}", "2026-09-28T03:00:00Z", None, 0, "pending", None),
            ("msg-failed", "warn", "case:2", "{}", "2026-09-28T03:00:00Z", None, 3, "failed", None),
            ("msg-leased-crash", "rescue", "case:3", "{}", "2026-09-28T03:00:00Z", "2026-09-28T03:10:00Z", 2, "leased", None),
            ("msg-delivered", "rescue", "case:4", "{}", "2026-09-28T03:00:00Z", None, 1, "delivered", "2026-09-28T03:05:00Z"),
        ]
        connection.executemany(
            "INSERT INTO outbox_messages VALUES (?,?,?,?,?,?,?,?,?)", rows
        )
        connection.commit()
        connection.close()

    def test_legacy_messages_migrated_without_loss(self):
        self._build_legacy_database()
        app = open_app(self.db_path)
        with app.database.connect() as connection:
            migrated = {row["message_id"]: dict(row) for row in connection.execute("SELECT * FROM outbox_messages")}

        self.assertEqual(set(migrated), {"msg-pending", "msg-failed", "msg-leased-crash", "msg-delivered"})
        # attempts、payload、delivered_at 等历史数据保持一致。
        self.assertEqual(migrated["msg-pending"]["attempts"], 0)
        self.assertEqual(migrated["msg-failed"]["attempts"], 3)
        self.assertEqual(migrated["msg-delivered"]["delivered_at"], "2026-09-28T03:05:00Z")
        # 崩溃遗留的无主租约恢复为 failed，可被重新领取但不丢 attempts。
        recovered = migrated["msg-leased-crash"]
        self.assertEqual(recovered["status"], "failed")
        self.assertIsNone(recovered["lease_until"])
        self.assertIsNone(recovered["lease_owner"])
        self.assertEqual(recovered["attempts"], 2)
        # 已送达消息仍是 delivered，任何人都不能冒认为发送者再确认。
        self.assertEqual(migrated["msg-delivered"]["status"], "delivered")
        with self.assertRaises(PermissionDenied):
            app.outbox.complete("msg-delivered", owner="duty-b")

        # pending 与恢复的 failed 都能被领取，且 attempts 延续历史计数。
        claimed = {item["message_id"]: item for item in app.outbox.lease(owner="duty-a", seconds=60, limit=10)}
        self.assertEqual(set(claimed), {"msg-pending", "msg-failed", "msg-leased-crash"})
        self.assertEqual(claimed["msg-failed"]["attempts"], 4)
        self.assertEqual(claimed["msg-leased-crash"]["attempts"], 3)

        # 每条历史消息迁移后都有审计记录。
        with app.database.connect() as connection:
            audit = connection.execute("SELECT message_id, action, actor_id FROM outbox_audit WHERE action='migrated'").fetchall()
        self.assertEqual({row["message_id"] for row in audit}, set(migrated))

    def test_migration_is_idempotent_across_reopens(self):
        self._build_legacy_database()
        open_app(self.db_path)
        open_app(self.db_path)
        with CivicFlow.open(self.db_path).database.connect() as connection:
            count = connection.execute("SELECT COUNT(*) AS n FROM outbox_audit WHERE action='migrated'").fetchone()["n"]
        self.assertEqual(count, 4)


if __name__ == "__main__":
    unittest.main()
