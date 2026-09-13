@echo off
setlocal
set "PYTHONUTF8=1"
set "V88_WIN=%~dp0"
if not exist "%V88_WIN%update_v88.py" set "V88_WIN=%USERPROFILE%\Desktop\StockAI\win\"
if exist "%USERPROFILE%\v88env\Scripts\python.exe" (
  "%USERPROFILE%\v88env\Scripts\python.exe" "%V88_WIN%update_v88.py" --start
) else (
  py -3 "%V88_WIN%update_v88.py" --start
)
if errorlevel 1 (
  echo UPDATE FAILED - copy the error above for diagnosis.
  pause
  exit /b 1
)
echo V88 update finished.
pause
