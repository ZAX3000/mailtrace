Write-Host "=== MailTrace Diagnostics ===" -ForegroundColor Cyan
Write-Host "PWD: $pwd"
Write-Host ".ps1 exists: " -NoNewline; Test-Path ".\scripts\run_debug_watch.ps1"
Write-Host "Python: " -NoNewline; try { (python --version) } catch { Write-Host "NOT FOUND" -ForegroundColor Red }
Write-Host "Pip: " -NoNewline; try { (pip --version) } catch { Write-Host "NOT FOUND" -ForegroundColor Red }
Write-Host "ExecutionPolicy (Process): " -NoNewline; Get-ExecutionPolicy -Scope Process
Write-Host "ExecutionPolicy (CurrentUser): " -NoNewline; Get-ExecutionPolicy -Scope CurrentUser
Write-Host "ExecutionPolicy (LocalMachine): " -NoNewline; Get-ExecutionPolicy -Scope LocalMachine
