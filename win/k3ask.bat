@echo off
REM k3ask: one-shot K3 ask wrapper for the v88-mobile agent exec tool.
REM Keeps the command short so agents never mangle long paths.
"C:\Users\admin\AppData\Roaming\kimi-desktop\daimon-share\daimon\runtime\python\.venv\Scripts\python.exe" "C:\Users\admin\.openclaw\workspaces\v88-mobile\k3_ask.py" %*
