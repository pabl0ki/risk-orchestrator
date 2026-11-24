"""Воспроизводимые сценарии. python -m unittest discover -s tests -v"""
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from backend.engine import Engine, calculate, now

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
        return self.e.publish(job["id"], job["token"], calculate(job))

    def test_T1_concurrent_arrivals(self):
        with ThreadPoolExecutor(12) as pool:
            ids = list(pool.map(lambda i:self.add(channel="schedule" if i%2 else "manual",key=str(i)),range(20)))
        self.assertEqual(len(set(ids)),1)
        self.add(key="0",channel="manual")
        self.assertEqual(len(self.e.snapshot()["requests"]),20)
        jobs=self.e.claim()
        self.assertEqual(len(jobs),1)
        self.finish(jobs[0])
        self.assertEqual(len(self.e.snapshot()["results"]),1)

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
        self.assertEqual(reader["inputs"]["reference"]["id"],"v1.1")

    def test_dependency_previous_day_keeps_child_partition(self):
        parent = self.add("Z1", day="2026-11-22")
        self.add("P1", day="2026-11-23", parent=parent)
        writer, = self.e.claim()
        self.finish(writer)
        child, = self.e.claim()
        self.assertEqual(child["inputs"]["deals"]["id"], "base/deals/2026-11-23")

    def test_executor_records_calculation_exception(self):
        from unittest.mock import patch
        from backend.worker import execute

        jid = self.add()
        with patch("backend.worker.calculate", side_effect=ValueError("invalid input")):
            for _ in range(2):
                job, = self.e.claim()
                execute(self.e, job, 0)
        snapshot = self.e.snapshot()
        self.assertEqual(snapshot["jobs"][jid-1]["status"], "failed")
        self.assertIn("ValueError: invalid input", snapshot["events"][-1]["message"])

    def test_T5_complete_input(self):
        self.add("Z1"); self.add()
        writer,=self.e.claim()
        self.assertFalse(self.e.snapshot()["results"])
        self.finish(writer)
        reader,=self.e.claim()
        self.assertEqual(reader["inputs"]["deals"]["rows"],calculate(writer))

    def test_T6_version_history(self):
        self.add(factor=1); self.add(factor=2)
        a,=self.e.claim(); self.finish(a)
        b,=self.e.claim(); self.finish(b)
        versions=self.e.snapshot()["results"]
        self.assertEqual(versions["v2.1"]["rows"],[v*2 for v in versions["v1.1"]["rows"]])

    def test_T7_no_hold_and_wait(self):
        self.add("Z1"); self.add("Z2")
        a,=self.e.claim()
        self.assertIsNone(self.e.snapshot()["jobs"][1]["token"])
        self.finish(a); b,=self.e.claim(); self.finish(b)
        self.assertEqual(len(self.e.snapshot()["results"]),2)

    def test_T8_fencing_and_restart(self):
        self.add(); old,=self.e.claim()
        with self.e.tx() as s:
            s["jobs"][0]["lease"]=now()-1
        self.e=Engine(self.db)
        fresh,=self.e.claim()
        self.assertIsNone(self.finish(old))
        self.assertEqual(self.finish(fresh),"v1.2")

    def test_T9_retry_publish(self):
        self.add(); old,=self.e.claim()
        self.e.control(1,"fault")
        self.assertFalse(self.e.snapshot()["results"])
        job,=self.e.claim()
        self.assertEqual(self.finish(job),self.finish(job))
        self.assertIsNone(self.finish(old))
        self.assertEqual(len(self.e.snapshot()["results"]),1)

    def test_T11_starvation(self):
        self.add("Z1"); active,=self.e.claim()
        self.add("Z2")
        with self.e.tx() as s:
            s["jobs"][1]["created"]-=10
        self.add("P2",channel="schedule")
        self.e.claim(); self.finish(active)
        eligible=self.e.claim()
        self.assertEqual([j["id"] for j in eligible],[2])

    def test_T14_history(self):
        self.add(); job,=self.e.claim(); self.finish(job)
        s=self.e.snapshot()
        self.assertEqual([e["seq"] for e in s["events"]],[1,2,3])
        self.assertEqual(s["results"]["v1.1"]["inputs"]["deals"],"base/deals/"+DAY)

    def test_T16_wakeup(self):
        self.add("Z1"); self.add("P1"); j,=self.e.claim()
        self.finish(j)
        self.assertEqual(self.e.claim()[0]["id"],2)

    def test_T17_dependency_and_failure(self):
        self.add("Z1"); self.add(parent=1)
        a,=self.e.claim();self.e.control(a["id"],"fault")
        b,=self.e.claim();self.finish(b)
        child,=self.e.claim()
        self.assertEqual(child["inputs"]["deals"]["id"],"v1.2")
        self.finish(child)
        parent=self.add("Z1",rerun=True); child_id=self.add(parent=parent)
        for _ in range(2):
            self.e.claim();self.e.control(parent,"fault")
        self.e.claim()
        self.assertEqual(self.e.snapshot()["jobs"][child_id-1]["status"],"rejected")

    def test_schedule_and_cancel(self):
        self.e.schedule(dict(enabled=True,day=DAY,kind="P2",period=5))
        with self.e.tx() as s:s["schedule"]["due"]=now()-1
        job,=self.e.claim()
        self.assertEqual(job["channel"],"schedule")
        self.e.control(job["id"],"cancel")
        self.assertIsNone(self.finish(job))

    def test_T10_invalidate_approval(self):
        jid = self.add(approval=True)
        job, = self.e.claim()
        self.finish(job)
        self.e.decide(jid,"approve","Риск-менеджер","accept")
        self.e.decide(jid,"approve","Директор","publish")
        self.add("Z1")
        writer, = self.e.claim()
        self.finish(writer)
        self.assertEqual(self.e.snapshot()["results"]["v1.1"]["approval"], "stale")

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
        self.assertTrue(any(e["job"] == jid and "RUN-2" in e["message"] for e in before["events"]))
        self.assertTrue(any(e["job"] == successor and "RUN-1" in e["message"] for e in before["events"]))
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
        self.assertEqual(self.e.snapshot()["heads"], {"reference/*": old})
        self.e.decide(jid, "approve", "Директор", "sign", version=new)
        self.assertEqual(self.e.snapshot()["heads"], {"reference/*": new})
        self.add(day="2027-01-01")
        consumer, = self.e.claim()
        self.assertEqual(consumer["inputs"]["reference"]["id"], new)

    def test_row_mismatch_blocks_each_decision(self):
        jid = self.add(approval=True)
        job, = self.e.claim()
        rid = self.finish(job)
        for actor in ("Риск-менеджер", "Директор"):
            with self.subTest(actor=actor):
                with self.e.tx() as state:
                    state["results"][rid]["rows"][0] += 1
                state = self.e.snapshot()
                checks = self.e.approval_checks(state, state["jobs"][jid-1], state["results"][rid])
                self.assertTrue(checks["current"])
                self.assertFalse(checks["verified"])
                with self.assertRaisesRegex(ValueError, "Построчная сверка") as error:
                    self.e.decide(jid, "approve", actor, actor, version=rid)
                self.assertEqual(str(error.exception), checks["block_reason"])
                self.assertFalse(self.e.snapshot()["heads"])
                with self.e.tx() as state:
                    state["results"][rid]["rows"][0] -= 1
                self.e.decide(jid, "approve", actor, actor, version=rid)

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
        self.assertEqual(list(self.e.snapshot()["heads"].values()),[version])

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
        with self.assertRaises(ValueError) as error:
            self.e.decide(jid,"approve","Директор","late")
        self.assertIn(f"«Справочник»: в расчёте — base/reference/*, сейчас — {ref}", str(error.exception))
        self.assertIn(f"«Кредитный результат»: при расчёте — ещё не опубликовано, сейчас — {published}", str(error.exception))
        self.assertEqual(self.e.snapshot()["heads"]["credit/"+DAY],published)

    def test_T15_route_revision(self):
        self.add()
        self.e.revise("P1")
        self.add()
        self.assertEqual([j["route"] for j in self.e.snapshot()["jobs"]],[1,2])


if __name__=="__main__":
    unittest.main()

