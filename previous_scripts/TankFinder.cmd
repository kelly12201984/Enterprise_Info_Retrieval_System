@echo off
cd /d P:\Chris\TankFinder
set QUERY=%*
if "%QUERY%"=="" (
  echo Enter search terms or "job:101-23"
  set /p QUERY=» 
)

rem If user typed job:####-##, route to job mode
echo %QUERY% | findstr /i "^job:" >nul
if %errorlevel%==0 (
  set JOB=%QUERY:job:=%"
  set JOB=%JOB:"=%
  python search.py --job %JOB% --show-files
) else (
  python search.py "%QUERY%" --years 2019-2025 --show-files
)

echo.
pause
