@echo off
chcp 65001 >nul
REM ══════════════════════════════════════════════════════════════
REM  V88 双向同步（Win ⇄ GitHub ⇄ Mac）— 2026-07-19
REM  职责：把本机(Win)对两仓的修改 commit+push 回 GitHub，
REM        Mac 下次启动 pull 即得；同时拉取远端最新。
REM  用法：在 Win 上手动改过文件后（或让 Claude 改完代码后）双击一次。
REM  注意：.env / secrets.toml 已被 .gitignore 排除，密钥永不入库。
REM ══════════════════════════════════════════════════════════════

for %%R in ("%USERPROFILE%\Desktop\StockAI" "%USERPROFILE%\Desktop\ai-daily-report-v2") do (
  echo.
  echo [同步] %%R
  git -C %%R add -A
  git -C %%R diff --cached --quiet || git -C %%R commit -m "win: 本机修改同步 %date% %time:~0,5%"
  git -C %%R pull --rebase --autostash origin main || echo [警告] pull 遇到冲突，请找 Claude 处理（生成物文件一律取远端）
  git -C %%R push origin main
)
echo.
echo [同步] 完成。Mac 端下次启动（内含 pull）即可看到本机修改。
pause
