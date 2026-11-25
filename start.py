"""Запускает стенд с зависимостями, установленными в папку проекта."""
import os
import importlib
import runpy
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

if __name__ == "__main__":
    libraries = Path(os.environ.get("STAGE4_LIB", ROOT / ".runtime" / sys.implementation.cache_tag))
    sys.path.insert(0, str(libraries))
    sys.path.insert(0, str(ROOT))
    try:
        import fastapi
        import uvicorn
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "--target", str(libraries),
                        "-r", str(ROOT / "requirements.txt")], check=True)
        importlib.invalidate_caches()
    runpy.run_module("backend.app", run_name="__main__", alter_sys=True)
