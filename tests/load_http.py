"""100 параллельных HTTP-заявок на изолированный локальный стенд."""
import argparse
import json
import math
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
GROUPS = 20
COPIES = 5
COUNT = GROUPS * COPIES
DURATION = 3


def request(url, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = Request(url, data=data, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=30) as response:
        return json.load(response)


def wait_state(url, ready, deadline=120):
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        state = request(url + "/api/state")
        if ready(state):
            return state
        time.sleep(0.2)
    raise TimeoutError("Истекло время ожидания состояния стенда")


def run(url):
    gate = threading.Barrier(COUNT + 1, timeout=30)

    def submit(i):
        group = i % GROUPS
        payload = dict(kind="P1", day=(date(2026, 11, 1) + timedelta(days=group)).isoformat(),
                       key=f"http-load-{i}")
        gate.wait()
        start = time.perf_counter()
        jid = request(url + "/api/run", payload)["id"]
        return group, jid, (time.perf_counter() - start) * 1000

    with ThreadPoolExecutor(max_workers=COUNT) as pool:
        futures = [pool.submit(submit, i) for i in range(COUNT)]
        start = time.perf_counter()
        gate.wait()
        responses = [future.result() for future in futures]
        batch_ms = (time.perf_counter() - start) * 1000

    state = wait_state(url, lambda s: all(j["status"] == "review" for j in s["jobs"]))
    jobs = state["jobs"]
    latencies = sorted(ms for _, _, ms in responses)
    for group in range(GROUPS):
        assert len({jid for g, jid, _ in responses if g == group}) == 1, "Дубли разошлись"
    assert len(jobs) == GROUPS and all(len(j["initiators"]) == COPIES for j in jobs)
    # Считаем пересечение интервалов по серверным отметкам, включая короткие пики между опросами.
    peak = max(sum(j["start"] <= other["start"] < j["calculated_at"] for j in jobs)
               for other in jobs)
    assert peak == 4, f"Ожидалось четыре одновременно занятых слота, получено {peak}"
    return dict(utc=datetime.now(timezone.utc).isoformat(), endpoint="POST /api/run",
                concurrent_clients=COUNT, business_dates=GROUPS, requests_per_date=COPIES,
                simulated_duration_s=DURATION, successful_responses=len(responses), executions=len(jobs),
                merged=COUNT-len(jobs), completed_calculations=len(jobs), final_status="review",
                peak_running=peak, registration_batch_ms=round(batch_ms, 1),
                latency_ms=dict(median=round(statistics.median(latencies), 1),
                                p95=round(latencies[math.ceil(.95*COUNT)-1], 1),
                                maximum=round(max(latencies), 1)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT/"Демонстрация"/"протоколы"/"Нагрузка HTTP.json")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        process = subprocess.Popen([sys.executable, "-B", "start.py", "--db", str(Path(tmp)/"load.sqlite3"),
                                    "--port", "0", "--duration", str(DURATION)], cwd=ROOT,
                                   stdout=subprocess.PIPE, text=True, errors="replace")
        try:
            url = next((line.strip() for line in process.stdout if line.startswith("http://127.0.0.1:")), None)
            if not url:
                raise RuntimeError("Сервер не запустился")
            for attempt in range(50):
                try:
                    request(url+"/api/instance")
                    break
                except OSError:
                    if attempt == 49 or process.poll() is not None:
                        raise
                    time.sleep(.1)
            result = run(url)
        finally:
            process.terminate()
            process.wait(timeout=15)
            process.stdout.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(result, ensure_ascii=False, indent=2)
    args.output.write_text(text+"\n", encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
