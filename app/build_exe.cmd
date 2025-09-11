@echo off 
setlocal enableextensions
pushd "%~dp0"

echo.
echo === TankFinder build ===
echo Script dir: %CD%
echo.

if not exist "TankFinderGUI.py" (
  echo ERROR: TankFinderGUI.py not found in %CD%
  popd & endlocal
  pause
  exit /b 1
)

REM ---- timestamp ----
for /f %%i in ('powershell -NoProfile -Command "(Get-Date).ToString(\"yyyyMMdd_HHmm\")"') do set TS=%%i

set APPNAME_BASE=TankFinder
set OUTNAME=%APPNAME_BASE%_%TS%
set TARGET_ROOT=P:\Software\TankFinder
set ARCHIVE_DIR=%TARGET_ROOT%\archive\%OUTNAME%
set ICON=tankfinder.ico

REM ---- clean old PyInstaller output ----
rmdir /s /q dist 2>nul
rmdir /s /q build 2>nul
rmdir /s /q __pycache__ 2>nul

echo Building exe with PyInstaller...
if exist "%ICON%" (
  pyinstaller --onefile --noconsole --clean --name "%OUTNAME%" --icon "%ICON%" TankFinderGUI.py
) else (
  echo [WARN] Icon not found: %ICON% (building without icon)
  pyinstaller --onefile --noconsole --clean --name "%OUTNAME%" TankFinderGUI.py
)
if errorlevel 1 (
  echo BUILD FAILED.
  popd & endlocal
  pause
  exit /b 1
)

set OUT_EXE=%CD%\dist\%OUTNAME%.exe
set OUT_INTERNAL=%CD%\dist\%OUTNAME%.exe._internal

if not exist "%OUT_EXE%" (
  echo ERROR: Built EXE not found: %OUT_EXE%
  popd & endlocal
  pause
  exit /b 1
)

REM ---- ensure target dirs exist ----
if not exist "%TARGET_ROOT%" mkdir "%TARGET_ROOT%" >nul 2>&1
if not exist "%ARCHIVE_DIR%" mkdir "%ARCHIVE_DIR%" >nul 2>&1

echo.
echo Deploying %OUTNAME% to %TARGET_ROOT% ...

REM ---- remove old runtime ----
del /f /q "%TARGET_ROOT%\TankFinder.exe" 2>nul
rmdir /s /q "%TARGET_ROOT%\TankFinder.exe._internal" 2>nul
REM NEW: also remove any legacy exe name so users can’t launch the wrong one
del /f /q "%TARGET_ROOT%\TankFinderGUI.exe" 2>nul
rmdir /s /q "%TARGET_ROOT%\TankFinderGUI.exe._internal" 2>nul

REM ---- copy new runtime (stable name) ----
copy /y "%OUT_EXE%" "%TARGET_ROOT%\TankFinder.exe" >nul
if exist "%OUT_INTERNAL%" (
  xcopy /e /i /y "%OUT_INTERNAL%" "%TARGET_ROOT%\TankFinder.exe._internal" >nul
)

REM ---- archive timestamped build ----
copy /y "%OUT_EXE%" "%ARCHIVE_DIR%\%OUTNAME%.exe" >nul
if exist "%OUT_INTERNAL%" (
  xcopy /e /i /y "%OUT_INTERNAL%" "%ARCHIVE_DIR%\%OUTNAME%.exe._internal" >nul
)

echo.
echo Done.
echo   Built:   %OUTNAME%.exe
echo   Runtime: %TARGET_ROOT%\TankFinder.exe
echo   Sidecar: %TARGET_ROOT%\TankFinder.exe._internal
echo   Archive: %ARCHIVE_DIR%
echo.
popd
endlocal
pause
