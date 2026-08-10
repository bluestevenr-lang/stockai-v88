@echo off
chcp 65001 >nul
REM ══════════════════════════════════════════════════════════════
REM  V88 manual repository sync helper (legacy filename) - 2026-08-10
REM  GPT/Codex owns the active core. This helper only pulls both repositories.
REM ══════════════════════════════════════════════════════════════

set "STOCKAI=%USERPROFILE%\Desktop\StockAI"
set "REPORT=%USERPROFILE%\Desktop\ai-daily-report-v2"

echo [V88-sync] pulling both repositories...
git -C "%STOCKAI%" pull --rebase --autostash origin main
git -C "%REPORT%" pull --rebase --autostash origin main
echo.
echo [V88-sync] complete. GPT/Codex remains the active V88 owner.
echo.
pause
