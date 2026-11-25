@echo off
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" start.py --open
  goto done
)
where py >nul 2>&1
if not errorlevel 1 (
  py -3 start.py --open
  goto done
)
for /d %%R in ("%ProgramFiles%\WindowsApps\OpenAI.CodexPrimaryRuntime.*") do if exist "%%~fR\dependencies\python\python.exe" (
  "%%~fR\dependencies\python\python.exe" start.py --open
  goto done
)
python start.py --open
:done
if errorlevel 1 pause
