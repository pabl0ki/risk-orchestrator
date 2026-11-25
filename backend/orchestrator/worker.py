"""Фоновый исполнитель."""
import time
import threading
from multiprocessing import parent_process
from concurrent.futures import ThreadPoolExecutor

from backend.orchestrator.engine import Engine, SLOTS, HEARTBEAT_INTERVAL
from backend.orchestrator.calculations import calculate


def execute(engine, job, duration):
    deadline = time.monotonic() + duration
    stop_heartbeat = threading.Event()
    
    def heartbeat_loop():
        while not stop_heartbeat.is_set():
            if not engine.heartbeat(job["id"], job["token"]):
                break
            stop_heartbeat.wait(HEARTBEAT_INTERVAL)
    
    hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    hb_thread.start()
    
    try:
        time.sleep(duration)  # имитация работы расчётного ядра
        engine.publish(job["id"], job["token"])  # без rows
    except Exception as error:
        engine.execution_error(job["id"], job["token"], error)
    finally:
        stop_heartbeat.set()
        hb_thread.join()


def run_worker(database, duration, stop):
    engine = Engine(database)
    with ThreadPoolExecutor(max_workers=SLOTS) as pool:
        while not stop.is_set() and parent_process().is_alive():
            for job in engine.claim():
                pool.submit(execute, engine, job, duration)
            stop.wait(0.2)