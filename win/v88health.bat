@echo off
REM v88health: one-shot V88 health-check wrapper for the v88-mobile agent exec tool.
REM Keeps the command short so agents never mangle long paths (mission#3 lesson).
"C:\Users\admin\AppData\Roaming\kimi-desktop\daimon-share\daimon\runtime\python\.venv\Scripts\python.exe" "C:\Users\admin\.openclaw\workspaces\v88-mobile\v88_health.py" %*
