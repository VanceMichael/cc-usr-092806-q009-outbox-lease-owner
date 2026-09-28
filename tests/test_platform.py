from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.approvals import ApprovalService
from civicflow.cases import CaseService
from civicflow.errors import ConflictError, PermissionDenied, ValidationError
from civicflow.ledger import to_minor
from civicflow.security import AccessContext, assert_distinct


class PlatformTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.app = CivicFlow.open(Path(self.temp.name) / "test.sqlite3", fixed_now="2026-09-28T12:00:00+08:00")
        self.system = AccessContext.system("tester")

    def tearDown(self):
        self.temp.cleanup()

    def test_entity_history_and_snapshot(self):
        service = CaseService(self.app.repository)
        created = service.create(self.system, {"case_type": "联合处置", "subject": "事项", "owner_org": "org:a", "priority": "high", "opened_at": self.app.clock.now()}, request_key="case-1")
        later_app = CivicFlow.open(Path(self.temp.name) / "test.sqlite3", fixed_now="2026-09-28T12:00:01+08:00")
        service = CaseService(later_app.repository)
        updated = service.revise(self.system, created["entity_id"], {"priority": "normal"}, expected_version=1, request_key="case-2")
        self.assertEqual(updated["version"], 2)
        self.assertEqual(len(service.history(self.system, created["entity_id"])), 2)
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=created["created_at"])["version"], 1)

    def test_idempotent_create_and_conflict(self):
        service = CaseService(self.app.repository)
        values = {"case_type": "协作", "subject": "同一事项", "owner_org": "org:a", "priority": "high", "opened_at": self.app.clock.now()}
        first = service.create(self.system, values, request_key="same")
        second = service.create(self.system, values, request_key="same")
        self.assertEqual(first["entity_id"], second["entity_id"])
        with self.assertRaises(ConflictError):
            service.create(self.system, {**values, "subject": "不同事项"}, request_key="same")

    def test_optimistic_version(self):
        service = CaseService(self.app.repository)
        row = service.create(self.system, {"case_type": "协作", "subject": "版本", "owner_org": "org:a", "priority": "high", "opened_at": self.app.clock.now()}, request_key="v1")
        service.revise(self.system, row["entity_id"], {"priority": "normal"}, expected_version=1, request_key="v2")
        with self.assertRaises(ConflictError):
            service.revise(self.system, row["entity_id"], {"priority": "low"}, expected_version=1, request_key="v3")

    def test_inbox_duplicate_and_conflict(self):
        first = self.app.inbox.receive(source="port", source_key="case-1", sequence=1, payload={"status": "ok"}, occurred_at=self.app.clock.now())
        second = self.app.inbox.receive(source="port", source_key="case-1", sequence=1, payload={"status": "ok"}, occurred_at=self.app.clock.now())
        self.assertEqual(first["status"], "accepted")
        self.assertEqual(second["status"], "duplicate")
        with self.assertRaises(ConflictError):
            self.app.inbox.receive(source="port", source_key="case-1", sequence=1, payload={"status": "changed"}, occurred_at=self.app.clock.now())

    def test_reservation_capacity_and_boundary(self):
        one = self.app.reservations.reserve(resource_id="lane:1", subject_id="a", quantity=6, capacity=10, start_at="2026-09-28T09:00:00+08:00", end_at="2026-09-28T10:00:00+08:00", actor="a")
        with self.assertRaises(ConflictError):
            self.app.reservations.reserve(resource_id="lane:1", subject_id="b", quantity=5, capacity=10, start_at="2026-09-28T09:30:00+08:00", end_at="2026-09-28T10:30:00+08:00", actor="b")
        self.app.reservations.reserve(resource_id="lane:1", subject_id="c", quantity=10, capacity=10, start_at="2026-09-28T10:00:00+08:00", end_at="2026-09-28T11:00:00+08:00", actor="c")
        self.assertEqual(self.app.reservations.usage("lane:1", at="2026-09-28T09:30:00+08:00"), 6)
        self.assertEqual(one["version"], 1)

    def test_ledger_balance_and_reversal(self):
        debit = self.app.ledger.post(journal_key="j1", account="cash", currency="CNY", amount="10.005", direction="debit", reference="d", actor="a")
        self.app.ledger.post(journal_key="j1", account="cash", currency="CNY", amount="10.00", direction="credit", reference="c", actor="a")
        self.assertEqual(debit["amount_minor"], 1000)
        self.assertEqual(self.app.ledger.balance("j1", currency="CNY"), 0)
        reversal = self.app.ledger.reverse(debit["entry_id"], reference="reverse", actor="a")
        replay = self.app.ledger.reverse(debit["entry_id"], reference="ignored", actor="a")
        self.assertEqual(reversal["entry_id"], replay["entry_id"])
        self.assertEqual(self.app.ledger.balance("j1", currency="CNY"), -1000)

    def test_money_rejects_non_finite(self):
        with self.assertRaises(ValidationError):
            to_minor("NaN")

    def test_jobs_recover_from_lease(self):
        job = self.app.jobs.schedule(job_type="deadline", subject_id="case:1", run_at="2026-09-28T03:00:00Z", payload={"kind": "notice"})
        claimed = self.app.jobs.claim_due()
        self.assertEqual([item["job_id"] for item in claimed], [job])
        self.app.jobs.finish(job)
        self.assertEqual(self.app.jobs.claim_due(), [])

    def test_outbox_delivery_requires_lease(self):
        message = self.app.outbox.enqueue(topic="case", aggregate_id="case:1", payload={"ok": True})
        with self.assertRaises(ConflictError):
            self.app.outbox.complete(message)
        leased = self.app.outbox.lease(owner="worker")
        self.assertEqual(leased[0]["message_id"], message)
        self.app.outbox.complete(message)

    def test_permissions_and_self_review(self):
        limited = AccessContext(actor_id="reader", permissions=frozenset({"read:cases"}))
        with self.assertRaises(PermissionDenied):
            CaseService(self.app.repository).create(limited, {"case_type": "x", "subject": "y", "owner_org": "o", "priority": "p", "opened_at": self.app.clock.now()}, request_key="no")
        with self.assertRaises(PermissionDenied):
            assert_distinct("same", "same")

    def test_restricted_fields_are_redacted(self):
        approvals = ApprovalService(self.app.repository)
        row = approvals.create(self.system, {"case_id": "case:1", "step": "复核", "reviewer": "person:secret", "decision": "pending", "reason": "等待"}, request_key="approval")
        reader = AccessContext(actor_id="reader", permissions=frozenset({"read:approvals"}))
        visible = approvals.get(reader, row["entity_id"])
        self.assertEqual(visible["reviewer"], "***")

    def test_audit_chain_verifies(self):
        CaseService(self.app.repository).create(self.system, {"case_type": "审计", "subject": "链", "owner_org": "org:a", "priority": "normal", "opened_at": self.app.clock.now()}, request_key="audit")
        self.assertEqual(self.app.verify()["audit_entries"], 1)


if __name__ == "__main__":
    unittest.main()
