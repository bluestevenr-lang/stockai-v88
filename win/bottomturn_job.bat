@echo off
rem V88 BottomTurn daily job (Win) - ASCII only, no CJK chars
rem GPT-gate hardening: mutex lock, explicit workdir, push-fail logging
setlocal
rem Win outbound to Yahoo/GitHub needs local proxy (mission5实测: direct gets 429)
set HTTP_PROXY=http://127.0.0.1:7897
set HTTPS_PROXY=http://127.0.0.1:7897
rem UTF8 mode: GBK console breaks src prints (emoji) and git subprocess decoding
set PYTHONUTF8=1
set LOGDIR=C:\Users\admin\Desktop\StockAI\win\logs
set LOG=%LOGDIR%\bottomturn.log
set LOCK=%LOGDIR%\bottomturn.lock
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
if exist "%LOCK%" (
  echo %DATE% %TIME% SKIP: another instance running >> "%LOG%"
  exit /b 1
)
echo lock %DATE% %TIME% > "%LOCK%"
echo ==== %DATE% %TIME% bottomturn job start ==== >> "%LOG%"
cd /d C:\Users\admin\Desktop\ai-daily-report-v2 >> "%LOG%" 2>&1
git pull --rebase --autostash origin main >> "%LOG%" 2>&1
C:\Users\admin\v88env\Scripts\python.exe scripts\v88_json_merge.py --postpull >> "%LOG%" 2>&1
C:\Users\admin\v88env\Scripts\python.exe src\bottom_turn.py >> "%LOG%" 2>&1
git add -f data\bottom_turn_pool.json >> "%LOG%" 2>&1
git commit -m "data: bottom-turn pool win daily" >> "%LOG%" 2>&1
git push origin main >> "%LOG%" 2>&1
if errorlevel 1 echo %DATE% %TIME% PUSH FAILED - needs credential check >> "%LOG%"
del "%LOCK%" >nul 2>&1
echo ==== %DATE% %TIME% bottomturn job end ==== >> "%LOG%"
endlocal
