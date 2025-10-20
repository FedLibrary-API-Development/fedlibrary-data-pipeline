@echo off
setlocal enableextensions
pushd "%~dp0"

for /f "delims=" %%i in ('powershell -NoProfile -Command "$([DateTime]::Now.ToString('yyyyMMdd_HHmmss'))"') do set TS=%%i

if not exist "..\fedlibrary-data-pipeline\logs" mkdir "..\logs"
set "LOG=..\fedlibrary-data-pipeline\logs\pipeline_%COMPUTERNAME%_%TS%.log"

echo [%date% %time%] Starting pipeline > "%LOG%"
set PYTHONUTF8=1
".venv\Scripts\python.exe" -m fedpipeline.main >> "%LOG%" 2>&1
set RC=%ERRORLEVEL%
echo [%date% %time%] Finished with RC=%RC% >> "%LOG%"

popd
endlocal & exit /b %RC%
