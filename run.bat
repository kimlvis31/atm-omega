@echo off
setlocal
if not exist "%~dp0.venv\Scripts\python.exe" (
    echo Project environment is missing. Run setup.bat first. 1>&2
    if "%~1"=="" call :interactive_error
    exit /b 1
)
"%~dp0.venv\Scripts\python.exe" -I -X utf8 "%~dp0scripts\run_omega.py" %*
set "OMEGA_EXIT=%errorlevel%"
if not "%OMEGA_EXIT%"=="0" if "%~1"=="" call :interactive_error
exit /b %OMEGA_EXIT%

:interactive_error
powershell -NoProfile -NonInteractive -Command "if ([Console]::IsInputRedirected) { exit 1 } else { exit 0 }" >nul 2>&1
if errorlevel 1 exit /b 0
pause
exit /b 0
