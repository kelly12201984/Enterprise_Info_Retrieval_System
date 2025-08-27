@echo off
setlocal EnableExtensions EnableDelayedExpansion
set "APPDIR=%~dp0"
cd /d "%APPDIR%"

set "PY=%LocalAppData%\Programs\Python\Python313\python.exe"
if not exist "%PY%" set "PY=py -3"

set "QUERY=%*"
if "%QUERY%"=="" (
  echo Enter search terms or "job:101-23"
  set /p QUERY=^> 
)

if /I "%QUERY:~0,4%"=="job:" (
  set "JOB=%QUERY:~4%"
  set "JOB=!JOB:"=!"
  if "!JOB:~0,1!"==" " set "JOB=!JOB:~1!"
  %PY% "%APPDIR%search.py" --job "!JOB!" --show-files
) else (
  %PY% "%APPDIR%search.py" "%QUERY%" --years 2019-2025 --show-files
)
echo.
pause
endlocal
