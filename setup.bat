@echo off
setlocal
if defined OMEGA_PYTHON goto custom_python
if exist "%~dp0.venv\Scripts\python.exe" goto project_python
where py >nul 2>&1
if not errorlevel 1 goto python_launcher
where python >nul 2>&1
if not errorlevel 1 goto path_python
echo CPython 3.13 is required. Install it or set OMEGA_PYTHON to its python.exe path. 1>&2
exit /b 1

:custom_python
"%OMEGA_PYTHON%" -I "%~dp0scripts\setup_environment.py" %*
exit /b %errorlevel%

:project_python
"%~dp0.venv\Scripts\python.exe" -I "%~dp0scripts\setup_environment.py" %*
exit /b %errorlevel%

:python_launcher
py -3.13 -I "%~dp0scripts\setup_environment.py" %*
exit /b %errorlevel%

:path_python
python -I "%~dp0scripts\setup_environment.py" %*
exit /b %errorlevel%
