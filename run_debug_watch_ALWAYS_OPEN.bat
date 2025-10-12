@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d "%~dp0"
echo === MailTrace Launcher (ALWAYS OPEN) ===
echo Working dir: %CD%
echo.
if not exist ".\scripts\run_debug_watch.ps1" (
  echo ERROR: scripts\run_debug_watch.ps1 not found
  echo Right-click the ZIP -> Extract All... before running.
  echo Press any key to exit...
  pause >nul
  goto :EOF
)
powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -NoExit -File ".\scripts\run_debug_watch.ps1"
cmd /k
endlocal
