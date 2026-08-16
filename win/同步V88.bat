@echo off
REM ============================================================
REM  V88 双向同步（Win <-> GitHub <-> Mac）— 2026-08-16 更新
REM  StockAI(公开仓)：照旧 add+commit+pull+push。
REM  ai-daily-report-v2(私仓)：拉取走 scripts/safe_pull.sh
REM    （Mac 铁律：私仓禁裸 pull，safe_pull 带 JSON 自愈校验）。
REM  PYTHONUTF8=1：修复 Win 下中文 stash/提交导致自愈脚本崩溃。
REM  注意：.env / secrets.toml 已被 .gitignore 排除，密钥永不入库。
REM ============================================================
set PYTHONUTF8=1

set R1=%USERPROFILE%\Desktop\StockAI
echo.
echo [同步] %R1%
git -C %R1% add -A
git -C %R1% diff --cached --quiet || git -C %R1% commit -m "win: sync local changes %date% %time:~0,5%"
git -C %R1% pull --rebase --autostash origin main || echo [警告] pull 遇到冲突，请把日志发给 Kimi 处理
git -C %R1% push origin main

set R2=%USERPROFILE%\Desktop\ai-daily-report-v2
echo.
echo [同步] %R2%
git -C %R2% add -A
git -C %R2% diff --cached --quiet || git -C %R2% commit -m "win: sync local changes %date% %time:~0,5%"
"C:\Program Files\Git\bin\bash.exe" "%R2%\scripts\safe_pull.sh" || echo [警告] safe_pull 未通过，请把上方日志发给 Kimi 处理
git -C %R2% push origin main

echo.
echo [同步] 完成。Mac 端下次启动（内含 pull）即可看到本机修改。
pause
