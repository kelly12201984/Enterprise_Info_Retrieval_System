@echo off
setlocal
cd /d "%~dp0"
if not exist "logs" mkdir "logs"
set "LOG=logs\indexer_all_refts.log"
where py >nul 2>&1 && (set "PY=py -3") || (set "PY=python")
echo ==== %DATE% %TIME% : starting full refresh with FTS rebuild ====>> "%LOG%"
%PY% indexer\indexer.py --rebuild-fts --year-min 2015 --year-max 2100 >> "%LOG%" 2>&1
echo ==== %DATE% %TIME% : finished (rc=%ERRORLEVEL%) ====>> "%LOG%"
endlocal
