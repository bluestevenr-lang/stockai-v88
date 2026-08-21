# ══════════════════════════════════════════════════════════════
#  V88 Windows 一次性初始化（第四终端）— 2026-07-19
#  用法：装好 Git 与 Python 3.12+ 后，PowerShell 里执行：
#    powershell -ExecutionPolicy Bypass -File .\初始化V88.ps1
#  幂等：已存在的仓库只 pull，不会破坏。
# ══════════════════════════════════════════════════════════════
$ErrorActionPreference = "Stop"
$Desktop = "$env:USERPROFILE\Desktop"

function Need($cmd, $hint) {
  if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
    Write-Host "❌ 缺少 $cmd —— $hint" -ForegroundColor Red; exit 1
  }
}
Need git    "安装: https://git-scm.com/download/win"
Need python "安装: https://www.python.org/downloads/ (勾选 Add to PATH,版本≥3.12)"

$pyv = (python -c "import sys;print(sys.version_info[:2]>= (3,12))")
if ($pyv -ne "True") { Write-Host "❌ Python 需≥3.12" -ForegroundColor Red; exit 1 }

# ① 克隆/更新两仓（私仓首次会弹 GitHub 登录，用你的 bluestevenr-lang 账号）
if (-not (Test-Path "$Desktop\StockAI")) {
  git clone https://github.com/bluestevenr-lang/stockai-v88.git "$Desktop\StockAI"
} else { git -C "$Desktop\StockAI" pull --rebase --autostash }
if (-not (Test-Path "$Desktop\ai-daily-report-v2")) {
  git clone https://github.com/bluestevenr-lang/v88-daily-report.git "$Desktop\ai-daily-report-v2"
} else { git -C "$Desktop\ai-daily-report-v2" pull --rebase --autostash }

# ② 依赖
Write-Host "📦 安装 Python 依赖（首次约3-5分钟）..."
python -m pip install --upgrade pip -q
python -m pip install -r "$Desktop\StockAI\requirements.txt" -q
python -m pip install python-dotenv -q

# ③ 密钥（.env 不入 git，需从 Mac 复制或手动粘贴）
$envFile = "$Desktop\StockAI\.env"
if (-not (Test-Path $envFile)) {
  @"
# 从 Mac 的 ~/Desktop/StockAI/.env 复制真实值过来（微信传自己即可）
KIMI_CODE_API_KEY=在这里粘贴sk-kimi开头的订阅密钥
TUSHARE_TOKEN=在这里粘贴
"@ | Out-File -Encoding utf8 $envFile
  Write-Host "⚠️ 已生成 .env 模板：$envFile —— 请粘贴真实密钥后再启动" -ForegroundColor Yellow
}

# ④ 桌面快捷方式 → 启动V88.bat
$WshShell = New-Object -ComObject WScript.Shell
$lnk = $WshShell.CreateShortcut("$Desktop\V88.lnk")
$lnk.TargetPath = "$Desktop\StockAI\win\启动V88.bat"
$lnk.WorkingDirectory = "$Desktop\StockAI"
$lnk.Save()

Write-Host ""
Write-Host "✅ 初始化完成。下一步：" -ForegroundColor Green
Write-Host "  1) 填好 .env 密钥（见上方路径）"
Write-Host "  2) 确认代理端口（Clash for Windows 默认7890；不同则改 win\启动V88.bat 里两行）"
Write-Host "  3) 双击桌面「V88」快捷方式启动"
