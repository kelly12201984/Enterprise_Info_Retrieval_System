@echo off
setlocal
rem --- Resolve project root & GUI target ---
set "APPDIR=%~dp0"
pushd "%APPDIR%"
set "MAIN=%APPDIR%app\TankFinderGUI.py"
set "LOGDIR=%LocalAppData%\TankFinder"

if not exist "%MAIN%" (
  echo TankFinder launcher: missing "%MAIN%"
  powershell -NoP -C "[System.Windows.MessageBox]::Show('TankFinder launcher: missing %MAIN%','TankFinder',0,'Error')" >nul 2>&1
  popd & endlocal & exit /b 1
)

if not exist "%LOGDIR%" mkdir "%LOGDIR%" >nul 2>&1

rem --- Prefer project venv, then user install, then py launcher, then python on PATH ---
set "PYRUN="
if exist ".venv\Scripts\pythonw.exe" (
  set "PYRUN=.venv\Scripts\pythonw.exe"
) else if exist "%LocalAppData%\Programs\Python\Python313\pythonw.exe" (
  set "PYRUN=%LocalAppData%\Programs\Python\Python313\pythonw.exe"
)

rem --- Optional /log mode: run console python and capture stdout/stderr ---
if /I "%~1"=="/log" (
  set "PYCON=%PYRUN:pythonw.exe=python.exe%"
  if exist "%PYCON%" (
    "%PYCON%" "%MAIN%" > "%LOGDIR%\last_launch.log" 2>&1
  ) else if exist "%LocalAppData%\Programs\Python\Python313\python.exe" (
    "%LocalAppData%\Programs\Python\Python313\python.exe" "%MAIN%" > "%LOGDIR%\last_launch.log" 2>&1
  ) else (
    py -3 "%MAIN%" > "%LOGDIR%\last_launch.log" 2>&1
  )
  notepad "%LOGDIR%\last_launch.log"
  popd & endlocal & exit /b
)

rem --- Normal (windowless) launch ---
if defined PYRUN (
  start "" "%PYRUN%" "%MAIN%"
) else (
  rem windowless via py launcher; otherwise minimized console python
  where py >nul 2>&1 && ( start "" py -3w "%MAIN%" & goto :done )
  where python >nul 2>&1 && ( start "TankFinder" /min python "%MAIN%" & goto :done )
  echo No Python runtime found. Install Python 3.11+ or create .venv.
  powershell -NoP -C "[System.Windows.MessageBox]::Show('No Python found. Install Python 3.11+ or create .venv.','TankFinder',0,'Warning')" >nul 2>&1
)
:done
popd
endlocal
exit /b
