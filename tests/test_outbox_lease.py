"""发件箱租约归属、恢复与迁移的自动化验证。

覆盖应急通知场景：双进程竞争、错误持有者、重复完成幂等、租约过期重领、
服务重启后身份校验、旧库历史消息迁移。
"""

from __future__ import annotations

import multiprocessing
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.errors import ConflictError, PermissionDenied
from civicflow.outbox import LEGACY_SENDER, MIGRATION_ACTOR


def _lease_worker(db_path: str, owner: str, barrier: multiprocessing.Barrier, queue: multiprocessing.Queue) -> None:
    """独立 OS 进程：在同一屏障放开的瞬间领取一批消息。"""
    app = CivicFlow.open(db_path)
    barrier.wait()
    batch = app.outbox.lease(owner=owner, seconds=120, limit=5)
    queue.put((owner, [(item["message_id"], item["attempts"]) for item in batch]))


def open_app(db_path: Path, fixed_now: str | None = None) -> CivicFlow:
    return CivicFlow.open(db_path, fixed_now=fixed_now)


class OutboxLeaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "outbox.sqlite3"
        self.app = open_app(self.db_path, fixed_now="2026-09-28T04:00:00Z")

    def tearDown(self):
        self.temp.cleanup()

    def _audit(self, app: CivicFlow | None = None) -> list[dict]:
        app = app or self.app
        with app.database.connect() as connection:
            return [dict(row) for row in connection.execute(
                "SELECT actor_id,action,entity_id,detail_json FROM audit_entries "
                "WHERE entity_type='outbox_message' ORDER BY audit_id"
            )]

    # 1. 双进程竞争：同一条通知只能被一个进程领取。
    def test_two_processes_compete_for_same_messages(self):
        ids = [self.app.outbox.enqueue(topic="alert", aggregate_id="case:1", payload={"n": n}) for n in range(20)]
        worker_a = open_app(self.db_path)
        worker_b = open_app(self.db_path)
        claimed: dict[str, str] = {}
        lock = threading.Lock()

        def worker(app: CivicFlow, owner: str) -> None:
            for _ in range(10):
                batch = app.outbox.lease(owner=owner, seconds=30, limit=3)
                if not batch:
                    break
                with lock:
                    for item in batch:
                        claimed[item["message_id"]] = item["lease_owner"]

        t1 = threading.Thread(target=worker, args=(worker_a, "duty-A"))
        t2 = threading.Thread(target=worker, args=(worker_b, "duty-B"))
        t1.start(); t2.start(); t1.join(); t2.join()

        self.assertEqual(set(claimed), set(ids))            # 无丢失、无重复领取
        self.assertEqual(len(claimed), len(ids))
        # 数据库中持久化的归属与领取者一致
        for message_id in ids:
            row = self.app.outbox.get(message_id)
            self.assertEqual(row["status"], "leased")
            self.assertEqual(row["lease_owner"], claimed[message_id])
            self.assertEqual(row["attempts"], 1)            # 竞争中不会被重复计数

        # 双方都不能再领取（租约未过期）
        self.assertEqual(worker_a.outbox.lease(owner="duty-A"), [])
        self.assertEqual(worker_b.outbox.lease(owner="duty-B"), [])

    # 1b. 双独立 OS 进程在同一瞬间并发：领取集合互不重叠，每条最多被领取一次。
    def test_two_os_processes_compete_without_duplicate_claim(self):
        ids = {self.app.outbox.enqueue(topic="alert", aggregate_id="case:1", payload={"n": n}) for n in range(15)}
        context = multiprocessing.get_context("spawn")
        queue: multiprocessing.Queue = context.Queue()
        barrier = context.Barrier(2)
        p1 = context.Process(target=_lease_worker, args=(str(self.db_path), "proc-A", barrier, queue))
        p2 = context.Process(target=_lease_worker, args=(str(self.db_path), "proc-B", barrier, queue))
        p1.start(); p2.start(); p1.join(60); p2.join(60)
        self.assertEqual(p1.exitcode, 0)
        self.assertEqual(p2.exitcode, 0)
        results = [queue.get() for _ in range(2)]
        claims = {owner: items for owner, items in results}
        a_ids = {mid for mid, _ in claims["proc-A"]}
        b_ids = {mid for mid, _ in claims["proc-B"]}
        self.assertTrue(a_ids and b_ids)                      # 双方都实际参与了竞争
        self.assertTrue(a_ids.isdisjoint(b_ids))             # 关键：无重复领取
        self.assertTrue(a_ids <= ids and b_ids <= ids)
        self.assertTrue(all(attempts == 1 for _, attempts in claims["proc-A"] + claims["proc-B"]))
        owners = {row["message_id"]: row["lease_owner"]
                  for row in self.app.database.connect().execute("SELECT message_id,lease_owner FROM outbox_messages")
                  if row["lease_owner"] is not None}
        self.assertEqual({mid for mid in a_ids | b_ids}, set(owners))
        self.assertEqual({owners[mid] for mid in a_ids}, {"proc-A"})
        self.assertEqual({owners[mid] for mid in b_ids}, {"proc-B"})

    # 2. 错误持有者：非领取者不能完成或标记失败。
    def test_non_holder_cannot_complete_or_fail(self):
        message = self.app.outbox.enqueue(topic="warn", aggregate_id="c", payload={})
        self.app.outbox.lease(owner="duty-A")
        with self.assertRaises(PermissionDenied):
            self.app.outbox.complete(message, owner="duty-B")
        with self.assertRaises(PermissionDenied):
            self.app.outbox.fail(message, owner="duty-B")
        row = self.app.outbox.get(message)
        self.assertEqual(row["status"], "leased")
        self.assertEqual(row["lease_owner"], "duty-A")
        self.assertIsNone(row["delivered_at"])

    # 3. 同一持有者重复完成保持幂等，审计只记一次送达，消息不会重复发送。
    def test_same_holder_repeated_complete_is_idempotent(self):
        message = self.app.outbox.enqueue(topic="rescue", aggregate_id="c", payload={})
        self.app.outbox.lease(owner="duty-A")
        self.app.outbox.complete(message, owner="duty-A")
        # 网络抖动导致的重复确认
        self.app.outbox.complete(message, owner="duty-A")
        self.app.outbox.complete(message, owner="duty-A")
        row = self.app.outbox.get(message)
        self.assertEqual(row["status"], "delivered")
        self.assertEqual(row["delivered_by"], "duty-A")
        delivered = [e for e in self._audit() if e["entity_id"] == message and e["action"] == "outbox.delivered"]
        self.assertEqual(len(delivered), 1)
        self.assertEqual(delivered[0]["actor_id"], "duty-A")
        # 已送达消息不能再被领取或标失败
        self.assertEqual(self.app.outbox.lease(owner="duty-A"), [])
        with self.assertRaises(ConflictError):
            self.app.outbox.fail(message, owner="duty-A")

    # 3b. 已送达后，其他进程的迟到确认必须被拒绝（杜绝无授权确认）。
    def test_late_complete_by_other_holder_after_delivery_rejected(self):
        message = self.app.outbox.enqueue(topic="rescue", aggregate_id="c", payload={})
        self.app.outbox.lease(owner="duty-A")
        self.app.outbox.complete(message, owner="duty-A")
        with self.assertRaises(PermissionDenied):
            self.app.outbox.complete(message, owner="duty-B")

    # 4. 租约过期：原持有者的迟到完成被拒，其他进程才能重新领取。
    def test_lease_expiry_allows_reclaim_only_after_timeout(self):
        message = self.app.outbox.enqueue(topic="transfer", aggregate_id="c", payload={})
        self.app.outbox.lease(owner="duty-A", seconds=30)
        # 未到期：别人不能抢，原持有者可正常完成路径之外也不能重领
        soon = open_app(self.db_path, fixed_now="2026-09-28T04:00:20Z")
        self.assertEqual(soon.outbox.lease(owner="duty-B"), [])
        # 到期后：原持有者的迟到确认被拒绝
        later = open_app(self.db_path, fixed_now="2026-09-28T04:00:31Z")
        with self.assertRaises(ConflictError):
            later.outbox.complete(message, owner="duty-A")
        # B 重新领取，attempts 累加，归属切换
        reclaimed = later.outbox.lease(owner="duty-B", seconds=30)
        self.assertEqual([item["message_id"] for item in reclaimed], [message])
        row = later.outbox.get(message)
        self.assertEqual(row["lease_owner"], "duty-B")
        self.assertEqual(row["attempts"], 2)
        # 旧持有者此时再确认仍然被拒
        with self.assertRaises(PermissionDenied):
            later.outbox.complete(message, owner="duty-A")
        later.outbox.complete(message, owner="duty-B")
        self.assertEqual(later.outbox.get(message)["delivered_by"], "duty-B")

    # 5. 服务重启：归属持久化，重启后仍严格校验持有者。
    def test_ownership_survives_restart(self):
        message = self.app.outbox.enqueue(topic="warning", aggregate_id="c", payload={})
        self.app.outbox.lease(owner="duty-A", seconds=300)
        restarted = open_app(self.db_path, fixed_now="2026-09-28T04:01:00Z")
        row = restarted.outbox.get(message)
        self.assertEqual(row["status"], "leased")
        self.assertEqual(row["lease_owner"], "duty-A")
        with self.assertRaises(PermissionDenied):
            restarted.outbox.complete(message, owner="duty-B")
        restarted.outbox.complete(message, owner="duty-A")
        self.assertEqual(restarted.outbox.get(message)["status"], "delivered")

    # 5b. 持有者进程在租约内失败：消息回到 failed，可被重新领取，attempts 保留。
    def test_failed_message_requeued_with_attempts_preserved(self):
        message = self.app.outbox.enqueue(topic="warning", aggregate_id="c", payload={})
        self.app.outbox.lease(owner="duty-A", seconds=300)
        self.app.outbox.fail(message, owner="duty-A")
        row = self.app.outbox.get(message)
        self.assertEqual(row["status"], "failed")
        self.assertIsNone(row["lease_owner"])
        self.assertEqual(row["attempts"], 1)
        again = self.app.outbox.lease(owner="duty-B", seconds=300)
        self.assertEqual(again[0]["message_id"], message)
        self.assertEqual(again[0]["attempts"], 2)
        self.app.outbox.fail(message, owner="duty-B")
        actions = [e["action"] for e in self._audit() if e["entity_id"] == message]
        self.assertEqual(actions, ["outbox.leased", "outbox.failed", "outbox.leased", "outbox.failed"])

    # 6. 历史消息迁移：旧库待发/失败/已送达/在租消息都不丢失，状态、次数、审计一致。
    def test_legacy_database_migration(self):
        self.temp.cleanup()  # 改用手工构造的旧版数据库
        Path(self.temp.name).mkdir(exist_ok=True)
        legacy = sqlite3.connect(self.db_path)
        legacy.executescript(
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
            ("msg:pending", "a", "2026-09-28T03:00:00Z", None, 0, "pending", None),
            ("msg:failed", "a", "2026-09-28T03:00:00Z", None, 2, "failed", None),
            ("msg:delivered", "a", "2026-09-28T03:00:00Z", None, 1, "delivered", "2026-09-28T03:05:00Z"),
            ("msg:orphan-leased", "a", "2026-09-28T03:00:00Z", "2026-09-28T05:00:00Z", 1, "leased", None),
        ]
        legacy.executemany(
            "INSERT INTO outbox_messages(message_id,aggregate_id,payload_json,available_at,lease_until,attempts,status,delivered_at,topic) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            [(mid, "c", "{}", available_at, lease_until, attempts, status, delivered_at, "alert") for mid, _, available_at, lease_until, attempts, status, delivered_at in rows],
        )
        legacy.commit(); legacy.close()

        app = open_app(self.db_path, fixed_now="2026-09-28T04:30:00Z")

        # 四条消息全部保留
        self.assertEqual({r["message_id"] for r in app.database.connect().execute("SELECT message_id FROM outbox_messages")},
                         {"msg:pending", "msg:failed", "msg:delivered", "msg:orphan-leased"})

        pending = app.outbox.get("msg:pending")
        self.assertEqual(pending["status"], "pending")
        self.assertEqual(pending["attempts"], 0)

        failed = app.outbox.get("msg:failed")
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["attempts"], 2)          # 重试次数不重置

        delivered = app.outbox.get("msg:delivered")
        self.assertEqual(delivered["status"], "delivered")
        self.assertEqual(delivered["delivered_at"], "2026-09-28T03:05:00Z")
        self.assertEqual(delivered["delivered_by"], LEGACY_SENDER)
        # 任何真实进程都不能冒认这条历史送达
        with self.assertRaises(PermissionDenied):
            app.outbox.complete("msg:delivered", owner="duty-A")

        # 无主在租消息被恢复为 failed 并可重新投送，attempts 在原值上累加
        orphan = app.outbox.get("msg:orphan-leased")
        self.assertEqual(orphan["status"], "failed")
        self.assertIsNone(orphan["lease_owner"])
        self.assertEqual(orphan["attempts"], 1)
        reclaimed = app.outbox.lease(owner="duty-A", seconds=60)
        reclaimed_ids = {item["message_id"] for item in reclaimed}
        self.assertEqual(reclaimed_ids, {"msg:pending", "msg:failed", "msg:orphan-leased"})
        for item in reclaimed:
            app.outbox.complete(item["message_id"], owner="duty-A")

        # 迁移与重投都有审计记录，且审计链完整可校验
        entries = self._audit(app)
        actions = {(e["entity_id"], e["action"]) for e in entries}
        self.assertIn(("msg:orphan-leased", "outbox.recovered"), actions)
        self.assertIn(("msg:delivered", "outbox.delivered_backfill"), actions)
        self.assertTrue(all(e["actor_id"] != "" for e in entries))
        self.assertEqual({e["actor_id"] for e in entries if e["action"].startswith("outbox.recovered") or e["action"] == "outbox.delivered_backfill"}, {MIGRATION_ACTOR})
        self.assertGreaterEqual(app.verify()["audit_entries"], len(entries))

        # 再次重启迁移必须幂等：不产生重复恢复记录
        before = len(self._audit(app))
        reopen = open_app(self.db_path, fixed_now="2026-09-28T04:31:00Z")
        self.assertEqual(len(self._audit(reopen)), before)


if __name__ == "__main__":
    unittest.main()
