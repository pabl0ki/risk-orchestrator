"""Фоновый исполнитель модульного монолита."""
import time
from multiprocessing import parent_process
from concurrent.futures import ThreadPoolExecutor

from backend.orchestrator.engine import Engine, SLOTS
from backend.orchestrator.calculations import calculate


def execute(engine, job, duration):
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        time.sleep(min(1, max(0, deadline - time.monotonic())))
        if not engine.heartbeat(job["id"], job["token"]):
            return
    try:
        engine.publish(job["id"], job["token"], calculate(job))
    except Exception as error:
        engine.execution_error(job["id"], job["token"], error)


def run_worker(database, duration, stop):
    engine = Engine(database)
    with ThreadPoolExecutor(max_workers=SLOTS) as pool:
        while not stop.is_set() and parent_process().is_alive():
            for job in engine.claim():
                pool.submit(execute, engine, job, duration)
            stop.wait(0.2)
