"""Воспроизводимые сценарии. python -m unittest discover -s backend/tests -v"""
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from backend.orchestrator.engine import Engine, now

DAY = "2026-11-10"


class Orchestration(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.db = Path(self.folder.name)/"state.sqlite"
        self.e = Engine(self.db)

    def add(self, kind="P1", day=DAY, **kw):
        return self.e.submit(dict(kind=kind, day=day, actor="Тест", **kw))

    def finish(self, job):
        return self.e.publish(job["id"], job["token"])

    def test_T1_concurrent_arrivals(self):
        with ThreadPoolExecutor(12) as pool:
            ids = list(pool.map(lambda i:self.add(channel="schedule" if i%2 else "manual",key=str(i)),range(20)))
        self.assertEqual(len(set(ids)),1)
        self.add(key="0",channel="manual")
        self.assertEqual(len(self.e.snapshot()["requests"]),20)
        jobs=self.e.claim()
        self.assertEqual(len(jobs),1)
        self.finish(jobs[0])
        self.assertEqual(self.e.snapshot()["results"],{})

    def test_T2_shared_read(self):
        self.add(); self.add("P2")
        self.assertEqual(len(self.e.claim()),2)

    def test_T3_dates_and_shared_workspace(self):
        self.add(shared=True); self.add(day="2026-11-11", shared=True); self.add(day="2026-11-12")
        self.assertEqual([j["id"] for j in self.e.claim()],[1,3])

    def test_T4_global_reference(self):
        self.add("FX"); self.add(day="2026-11-11")
        j,=self.e.claim()
        self.finish(j)
        reader,=self.e.claim()
        self.assertEqual(reader["id"],2)

    def test_dependency_previous_day_keeps_child_partition(self):
        parent = self.add("Z1", day="2026-11-22")
        self.add("P1", day="2026-11-23", parent=parent)
        writer, = self.e.claim()
        self.finish(writer)
        child, = self.e.claim()
        self.assertIn(["deals","2026-11-23","R"],self.e.snapshot()["jobs"][child["id"]-1]["resources"])

    def test_executor_records_calculation_exception(self):
        from unittest.mock import patch
        from backend.orchestrator.worker import execute

        jid = self.add()
        with patch.object(self.e,"publish",side_effect=ValueError("invalid input")):
            job, = self.e.claim()
            execute(self.e,job,0)
        snapshot = self.e.snapshot()
        self.assertEqual(snapshot["jobs"][jid-1]["status"], "failed")
        self.assertIn("invalid input", snapshot["events"][-1]["message"])

    def test_T5_complete_input(self):
        self.add("Z1"); self.add()
        writer,=self.e.claim()
        self.assertFalse(self.e.snapshot()["results"])
        self.finish(writer)
        reader,=self.e.claim()
        self.assertEqual(reader["id"],2)

    def test_T6_different_parameters(self):
        self.add(factor=1); self.add(factor=2)
        a,=self.e.claim(); self.finish(a)
        b,=self.e.claim(); self.finish(b)
        self.assertEqual([j["status"] for j in self.e.snapshot()["jobs"]],["done","done"])
        self.assertFalse(self.e.snapshot()["results"])

    def test_T7_no_hold_and_wait(self):
        self.add("Z1"); self.add("Z2")
        a,=self.e.claim()
        self.assertIsNone(self.e.snapshot()["jobs"][1]["token"])
        self.finish(a); b,=self.e.claim(); self.finish(b)
        self.assertTrue(all(j["status"]=="done" for j in self.e.snapshot()["jobs"]))

    def test_T8_fencing_and_restart(self):
        self.add(); old,=self.e.claim()
        with self.e.tx() as s:
            s.conn.execute("UPDATE run SET lease_expires_at=datetime('now','-1 second')")
        self.e=Engine(self.db)
        self.assertFalse(self.e.claim())
        self.assertIsNone(self.finish(old))
        self.assertEqual(self.e.snapshot()["jobs"][0]["status"],"failed")

    def test_T9_retry_publish(self):
        self.add(); old,=self.e.claim()
        self.assertEqual(self.finish(old),self.finish(old))
        self.assertEqual(len([e for e in self.e.snapshot()["events"] if "Завершение этапа" in e["message"]]),1)

    def test_T11_starvation(self):
        self.add("Z1"); active,=self.e.claim()
        self.add("Z2")
        self.add("P2",channel="schedule")
        self.e.claim(); self.finish(active)
        eligible=self.e.claim()
        self.assertEqual([j["id"] for j in eligible],[2])

    def test_T14_history(self):
        self.add(); job,=self.e.claim(); self.finish(job)
        s=self.e.snapshot()
        self.assertEqual([e["seq"] for e in s["events"]],[1,2,3])
        self.assertEqual(s["events"][1]["job"], job["id"])

    def test_T16_wakeup(self):
        self.add("Z1"); self.add("P1"); j,=self.e.claim()
        self.finish(j)
        self.assertEqual(self.e.claim()[0]["id"],2)

    def test_T17_dependency_and_failure(self):
        self.add("Z1"); self.add(parent=1)
        a,=self.e.claim();self.e.execution_error(a["id"],a["token"],RuntimeError("test"))
        self.assertFalse(self.e.claim())
        self.assertEqual(self.e.snapshot()["jobs"][1]["status"],"rejected")

    def test_schedule_and_cancel(self):
        from unittest.mock import patch
        with patch("backend.orchestrator.engine.time.time",return_value=now()-10):
            self.e.schedule(dict(enabled=True,day=DAY,kind="P2",period=5))
        job,=self.e.claim()
        self.assertEqual(self.e.snapshot()["jobs"][0]["channel"],"schedule")
        self.e.control(job["id"],"cancel")
        self.assertIsNone(self.finish(job))

    def test_T10_decision_survives_data_load(self):
        jid = self.add(approval=True)
        job, = self.e.claim()
        self.finish(job)
        self.e.decide(jid,"approve","Риск-менеджер","accept")
        self.e.decide(jid,"approve","Директор","publish")
        self.add("Z1")
        writer, = self.e.claim()
        self.finish(writer)
        self.assertEqual(self.e.snapshot()["jobs"][0]["status"],"done")

    def test_T12_repeat_and_selfapproval(self):
        jid = self.add(approval=True)
        job, = self.e.claim()
        self.finish(job)
        with self.assertRaises(ValueError):
            self.e.decide(jid,"approve","Тест","mine")
        for _ in range(2):
            self.e.decide(jid,"approve","Риск-менеджер","once")
        self.assertEqual(len(self.e.snapshot()["signals"]),1)

    def test_T13_rejection(self):
        jid = self.add(approval=True)
        job, = self.e.claim()
        self.finish(job)
        successor = self.e.decide(jid,"reject","Риск-менеджер","no","Требуется проверка")
        self.assertEqual(successor,2)
        self.assertTrue(self.e.snapshot()["jobs"][1]["rerun"])
        before = self.e.snapshot()
        self.assertEqual(before["jobs"][0]["rerun_to"],successor)
        self.assertEqual(before["jobs"][1]["rerun_from"],jid)
        self.assertEqual(self.e.decide(jid,"reject","Риск-менеджер","no","Требуется проверка"), successor)
        self.assertEqual(self.e.snapshot()["events"], before["events"])

    def test_approved_reference_is_global(self):
        self.add("FX")
        initial, = self.e.claim()
        old = self.finish(initial)
        jid = self.add("FX", approval=True)
        candidate, = self.e.claim()
        new = self.finish(candidate)
        self.e.decide(jid, "approve", "Риск-менеджер", "review", version=new)
        self.assertEqual(self.e.snapshot()["jobs"][1]["status"],"review")
        self.e.decide(jid, "approve", "Директор", "sign", version=new)
        self.assertEqual(self.e.snapshot()["jobs"][1]["status"],"done")
        self.add(day="2027-01-01")
        consumer, = self.e.claim()
        self.assertTrue(consumer["run_id"])

    def test_opaque_result_is_never_inspected(self):
        self.add()
        job, = self.e.claim()
        self.assertTrue(self.e.publish(job["id"],job["token"],object()))
        self.assertFalse(self.e.snapshot()["results"])

    def test_two_stage_publication(self):
        jid = self.add(approval=True)
        job, = self.e.claim()
        version = self.finish(job)
        self.assertFalse(self.e.snapshot()["heads"])
        with self.assertRaises(ValueError):
            self.e.decide(jid,"approve","Директор","early",version=version)
        self.e.decide(jid,"approve","Риск-менеджер","review",version=version)
        self.assertFalse(self.e.snapshot()["heads"])
        self.e.decide(jid,"approve","Директор","sign",version=version)
        self.assertEqual(self.e.snapshot()["jobs"][0]["status"],"done")

    def test_superseded_candidate(self):
        jid = self.add(approval=True)
        job, = self.e.claim()
        self.finish(job)
        self.e.decide(jid,"approve","Риск-менеджер","review")
        self.add(rerun=True)
        newer, = self.e.claim()
        published = self.finish(newer)
        self.add("FX")
        writer, = self.e.claim()
        ref = self.finish(writer)
        self.e.decide(jid,"approve","Директор","late")
        self.assertEqual(self.e.snapshot()["jobs"][0]["status"],"done")

    def test_request_idempotency_after_restart(self):
        jid=self.add(key="command")
        self.e=Engine(self.db)
        self.assertEqual(self.add(key="command"),jid)
        with self.assertRaises(ValueError):
            self.add("FX",key="command")


if __name__=="__main__":
    unittest.main()

