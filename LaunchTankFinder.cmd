@echo off
setlocal
rem Resolve to the folder this .cmd sits in (the project root)
set "APPDIR=%~dp0"
pushd "%APPDIR%"

rem 1) Prefer project venv if present
if exist ".venv\Scripts\pythonw.exe" (
  set "PYRUN=.venv\Scripts\pythonw.exe"
) else if exist "%LocalAppData%\Programs\Python\Python313\pythonw.exe" (
  rem 2) Fallback to user install pythonw
  set "PYRUN=%LocalAppData%\Programs\Python\Python313\pythonw.exe"
) else (
  rem 3) Last resort: use py launcher (windowed)
  set "PYRUN="
)

if defined PYRUN (
  start "" "%PYRUN%" "%APPDIR%app\TankFinderGUI.py"
) else (
  start "" py -3w "%APPDIR%app\TankFinderGUI.py"
)

popd
endlocal
exit /b
