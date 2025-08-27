# write a verbose launcher that logs stdout/stderr
Set-Content -Path 'P:\Chris\TankFinder\LaunchTankFinder.cmd' -Encoding Ascii -Value @'
@echo off
setlocal
pushd P:\Chris\TankFinder\app
"C:\Users\aalhad\AppData\Local\Programs\Python\Python313\python.exe" -X faulthandler -u "TankFinderGUI.py" 1>"%LOCALAPPDATA%\TankFinder\last_stdout.txt" 2>"%LOCALAPPDATA%\TankFinder\last_stderr.txt"
set rc=%errorlevel%
popd
echo Exit code: %rc%
echo Logs in: %LOCALAPPDATA%\TankFinder\
pause
endlocal
'@

# run it
.\LaunchTankFinder.cmd
