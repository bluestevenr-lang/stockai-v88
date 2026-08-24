@echo off
REM k3quota: one-shot Kimi quota/ledger wrapper for the v88-mobile agent exec tool.
REM Runs the repo copy so kimi_subscription import resolves; script itself never calls the LLM.
"C:\Users\admin\AppData\Roaming\kimi-desktop\daimon-share\daimon\runtime\python\.venv\Scripts\python.exe" "C:\Users\admin\Desktop\StockAI\win\k3_quota.py" %*
