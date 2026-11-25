"""Simulate an opaque executor; only execution status crosses the boundary."""
import time
from multiprocessing import parent_process
from concurrent.futures import ThreadPoolExecutor
from .engine import Engine, SLOTS
from .sweep_queue import SWEEP_INTERVAL


def execute(engine, job, duration):
    deadline = time.monotonic()+duration
    try:
        while True:
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(10,remaining))
            if not engine.heartbeat(job["id"],job["token"]):
                return
        engine.publish(job["id"],job["token"])
    except Exception as exc:
        engine.execution_error(job["id"],job["token"],exc)


def run_worker(database, duration, stop):
    engine = Engine(database)
    next_sweep = next_watchdog = 0
    last_revision = None
    with ThreadPoolExecutor(max_workers=SLOTS) as pool:
        while not stop.is_set() and parent_process().is_alive():
            now = time.monotonic()
            revision = engine.queue_revision()
            maintenance = now >= next_sweep
            if maintenance or revision != last_revision or now >= next_watchdog:
                for job in engine.claim(maintenance):
                    pool.submit(execute,engine,job,duration)
                last_revision = revision
                next_watchdog = now + 10
            if maintenance:
                next_sweep = now + SWEEP_INTERVAL
            stop.wait(.2)
