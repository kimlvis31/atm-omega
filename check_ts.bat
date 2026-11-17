@echo off
setlocal
if not exist "%~dp0.venv\Scripts\python.exe" (
    echo Project Python environment is missing. Run setup.bat first. 1>&2
    exit /b 1
)
"%~dp0.venv\Scripts\python.exe" -I -X utf8 "%~dp0scripts\check_ts.py" %*
exit /b %errorlevel%
