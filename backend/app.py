"""Локальный API, статический интерфейс и запуск отдельного процесса worker."""
import argparse
import errno
import hashlib
import json
import multiprocessing
import socket
import webbrowser
from contextlib import asynccontextmanager
from pathlib import Path
from http.client import HTTPConnection, HTTPException as HTTPClientError

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse

from backend.orchestrator.engine import CATALOG, LABELS, Engine
from backend.orchestrator.scenarios import SCENARIOS, launch_batch
from backend.orchestrator.worker import run_worker

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
USERS = {"admin": "Оператор", "analyst": "Анна", "reviewer": "Риск-менеджер", "director": "Директор"}


def instance_id(database):
    return hashlib.sha256(str(Path(database).resolve()).encode()).hexdigest()


def matches_instance(port, database):
    connection = HTTPConnection("127.0.0.1", port, timeout=1)
    try:
        connection.request("GET", "/api/instance")
        response = connection.getresponse()
        return response.status == 200 and json.loads(response.read(1024)) == {
            "application": "risk-stage4", "database": instance_id(database)}
    except (OSError, ValueError, HTTPClientError):
        return False
    finally:
        connection.close()


def create_app(database, duration):
    engine = Engine(database)

    @asynccontextmanager
    async def lifespan(app):
        ctx = multiprocessing.get_context("spawn")
        stop = ctx.Event()
        worker = ctx.Process(target=run_worker, args=(database, duration, stop), daemon=True)
        worker.start()
        app.state.worker = worker
        try:
            yield
        finally:
            stop.set()
            worker.join(3)
            if worker.is_alive():
                worker.terminate()
                worker.join()

    api = FastAPI(title="Risk Orchestrator", lifespan=lifespan)

    @api.get("/api/instance")
    def instance():
        return {"application": "risk-stage4", "database": instance_id(database)}

    @api.exception_handler(ValueError)
    async def invalid(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=400)

    def role(request):
        return request.cookies.get("demo_role", "admin")

    def require(request, *roles):
        if role(request) not in roles:
            raise HTTPException(403, "Действие недоступно для этой роли.")
        return USERS[role(request)]

    @api.post("/api/role/{selected}")
    def switch(selected: str, response: Response):
        if selected not in USERS:
            raise ValueError("Неизвестная роль")
        response.set_cookie("demo_role", selected, httponly=True, samesite="strict")
        return {"role": selected}

    @api.get("/api/state")
    def state(request: Request):
        user = require(request, *USERS)
        snapshot = engine.snapshot()
        jobs = snapshot["jobs"]
        if role(request) == "analyst":
            owned = {r["job"] for r in snapshot["requests"] if r["actor"] == user}
            jobs = [j for j in jobs if j["id"] in owned]
        if role(request) in ("reviewer", "director"):
            stage = "pending" if role(request) == "reviewer" else "reviewed"
            jobs = [j for j in jobs if j["status"] == "review" and j["approval_status"] == stage]
        ids = {j["id"] for j in jobs}
        for j in jobs:
            j.pop("token", None)
            if role(request) in ("reviewer","director"):
                for field in ("inputs","factor","resources","result"):
                    j.pop(field,None)
            else:
                j["version"] = {"approval":j["approval_status"]}
            actors = [r["actor"] for r in snapshot["requests"] if r["job"] == j["id"]]
            j["initiators"] = [j["actor"]]
            j["subscribers"] = list(dict.fromkeys(actor for actor in actors if actor != j["actor"]))
            j["request_count"] = len(actors)
        return dict(jobs=jobs, role=role(request), worker_alive=request.app.state.worker.is_alive(),
                    catalog={k:v[0] for k,v in CATALOG.items()},
                    scenarios=SCENARIOS if role(request)=="admin" else {},
                    batches=snapshot.get("demo_batches", []) if role(request)=="admin" else [],
                    labels=LABELS, events=[e for e in snapshot["events"] if e["job"] in ids or role(request)=="admin"])

    @api.post("/api/demo")
    def demo(request: Request, scenario: str = "basic"):
        require(request, "admin")
        return launch_batch(engine, scenario)

    @api.post("/api/run")
    def launch(request: Request, data: dict):
        actor = require(request, "admin","analyst")
        kind = data.get("kind")
        if not isinstance(kind, str) or kind not in CATALOG or not isinstance(data.get("day"), str):
            raise ValueError("Выберите процесс и бизнес-дату.")
        return {"id":engine.submit(dict(kind=kind,day=data["day"], actor=actor,
                                        parent=data.get("parent"), approval=True, key=data.get("key")))}

    @api.post("/api/jobs/{jid}/cancel")
    def cancel(jid: int, request: Request):
        require(request,"admin")
        engine.control(jid,"cancel")
        return {"id":jid}

    @api.post("/api/jobs/{jid}/decision")
    def decide(jid: int, request: Request, data: dict):
        actor = require(request,"reviewer","director")
        if not all(isinstance(data.get(k), str) and data[k] for k in ("decision","key")):
            raise ValueError("Нужны решение и ID команды.")
        return {"successor":engine.decide(jid,data["decision"],actor,data["key"],data.get("comment",""),data.get("version"))}

    @api.get("/")
    def index():
        return FileResponse(FRONTEND / "index.html")

    @api.get("/{asset}")
    def asset(asset: str):
        if asset not in ("app.js","style.css"):
            raise HTTPException(404)
        return FileResponse(FRONTEND / asset)

    @api.post("/api/external-events")
    def external_event(request: Request, data: dict):
        """Принимает внешнее событие (согласование/утверждение)."""
        actor = require(request, "reviewer", "director")
        if not isinstance(data.get("run_id"),int) or not isinstance(data.get("event_id"),str):
            raise ValueError("Нужны run_id и event_id.")
        if data.get("actor",actor) != actor:
            raise HTTPException(403,"Роль не соответствует автору решения.")
        return engine.external(data["run_id"],actor,data.get("decision"),data["event_id"],data.get("comment",""))

    return api


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=str(ROOT / "data" / "stage4.sqlite3"))
    parser.add_argument("--port", type=int)
    parser.add_argument("--duration", type=float, default=4)
    parser.add_argument("--open", action="store_true")
    args = parser.parse_args()
    with socket.socket() as sock:
        ports = range(8766, 8776) if args.port is None else [args.port]
        for port in ports:
            try:
                sock.bind(("127.0.0.1", port))
                break
            except OSError as exc:
                if exc.errno != errno.EADDRINUSE and getattr(exc, "winerror", None) != 10048:
                    raise
                if matches_instance(port, args.db):
                    url = f"http://127.0.0.1:{port}"
                    print(f"Стенд уже работает: {url}", flush=True)
                    if args.open:
                        webbrowser.open(url)
                    raise SystemExit(0)
        else:
            raise SystemExit("Порт занят. Укажите свободный порт: python start.py --port 0 --open")
        url = f"http://127.0.0.1:{sock.getsockname()[1]}"
        print(url, flush=True)
        if args.open:
            import threading
            threading.Timer(1, webbrowser.open, args=(url,)).start()
        server = uvicorn.Server(uvicorn.Config(create_app(args.db, max(.05, args.duration)), log_level="warning"))
        server.run(sockets=[sock])
