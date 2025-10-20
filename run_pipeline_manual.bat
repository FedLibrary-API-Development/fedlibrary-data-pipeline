@echo off
cd /d "%~dp0"
start "" /b python -m fedpipeline.main
echo Showing log output...
powershell -NoProfile -Command "Get-Content -Path pipeline.log -Wait"