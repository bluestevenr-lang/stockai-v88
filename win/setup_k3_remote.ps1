# 配置K3遥控.ps1 —— 把 Win 的 v88-mobile 升级为: K3大脑 + V88遥控能力
# 用法: 双击 "升级K3遥控-双击我.bat"，按提示粘贴 Moonshot API Key
$ErrorActionPreference = 'Stop'
function Log($m) { Write-Host "[K3升级] $m" -ForegroundColor Cyan }

# --- 1. 读取 Moonshot API Key（只写本机配置，永不入 git） ---
$sec = Read-Host '请输入 Moonshot API Key（platform.moonshot.cn 的 API Key 管理页生成）' -AsSecureString
$apiKey = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec))
if (-not $apiKey) { throw 'API Key 不能为空。' }

# --- 2. 下载 cloudflared（手机访问隧道的载体） ---
$ToolsDir = Join-Path $env:USERPROFILE '.openclaw\tools'
New-Item -ItemType Directory -Force -Path $ToolsDir | Out-Null
$CfExe = Join-Path $ToolsDir 'cloudflared.exe'
if (-not (Test-Path $CfExe)) {
    Log '下载 cloudflared ...'
    Invoke-WebRequest -Uri 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe' `
        -OutFile $CfExe -UseBasicParsing
}
Log "cloudflared 就绪: $CfExe"

# --- 3. 同步最新 AGENTS.md 到机器人工作区 ---
$RepoRoot  = Split-Path -Parent $PSScriptRoot
$Workspace = Join-Path $env:USERPROFILE '.openclaw\workspaces\v88-mobile'
$src = Join-Path $RepoRoot 'win\openclaw-v88\AGENTS.md'
if (Test-Path $src) { Copy-Item $src (Join-Path $Workspace 'AGENTS.md') -Force; Log 'AGENTS.md 已更新。' }

# --- 4. 定位 v88-mobile 代理索引 ---
$agentsJson = (& openclaw agents list --json 2>$null) -join "`n"
$agents = @($agentsJson | ConvertFrom-Json)
if (-not ($agents | Where-Object { $_.id -eq 'v88-mobile' })) { throw '找不到 v88-mobile 代理，请先运行安装脚本。' }
$cfg = & openclaw config get --json 2>$null | Out-String | ConvertFrom-Json
$idx = -1
for ($i=0; $i -lt $cfg.agents.list.Count; $i++) { if ($cfg.agents.list[$i].id -eq 'v88-mobile') { $idx = $i; break } }
if ($idx -lt 0) { throw '配置里找不到 v88-mobile 代理。' }

# --- 5. 写入: K3模型 + moonshot密钥 + exec放行(仅限v88ctl) ---
$K3Model = @(@{
    id = 'kimi-k3'; name = 'Kimi K3'; reasoning = $true
    input = @('text','image')
    contextWindow = 1048576; maxTokens = 32768
})
$ToolPolicy = @{
    profile = 'coding'
    allow   = @('read','exec','process')
    deny    = @('write','edit','apply_patch','browser','web_search','web_fetch','message',
                'sessions_spawn','sessions_send','cron','gateway','nodes','computer')
    codeMode = $false
    elevated = @{ enabled = $false }
    exec     = @{ mode = 'allow' }
    fs       = @{ workspaceOnly = $false }
    message  = @{
        allowCrossContextSend = $false
        crossContext = @{ allowWithinProvider = $false; allowAcrossProviders = $false }
        broadcast = @{ enabled = $false }
    }
}
$Batch = @(
    @{ path = 'models.providers.moonshot'; value = @{
        baseUrl = 'https://api.moonshot.cn/v1'
        api     = 'openai-completions'
        apiKey  = $apiKey
        models  = $K3Model
    } },
    @{ path = "agents.list[$idx].model"; value = 'moonshot/kimi-k3' },
    @{ path = "agents.list[$idx].tools"; value = $ToolPolicy },
    @{ path = 'agents.defaults.models["moonshot/kimi-k3"].agentRuntime'; value = @{ id = 'openclaw' } }
)
$BatchPath = Join-Path $env:TEMP 'openclaw_k3_batch.json'
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($BatchPath, ($Batch | ConvertTo-Json -Depth 20), $Utf8NoBom)
try {
    Log '写入 K3 模型与遥控权限 ...'
    & openclaw config set --batch-file $BatchPath | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "config set 失败 ($LASTEXITCODE)" }
} finally { Remove-Item -Force $BatchPath -ErrorAction SilentlyContinue }
& openclaw config validate | Out-Host

# --- 6. 重启网关生效 ---
Log '重启 OpenClaw 网关 ...'
Stop-ScheduledTask -TaskName 'OpenClaw Gateway' -ErrorAction SilentlyContinue
Start-Sleep -Seconds 3
Start-ScheduledTask -TaskName 'OpenClaw Gateway'
Start-Sleep -Seconds 8
$st = (Get-ScheduledTask -TaskName 'OpenClaw Gateway').State
Log "网关状态: $st"

Write-Host ''
Write-Host '===== 完成 =====' -ForegroundColor Green
Write-Host '蓝一已切换为 Kimi K3 大脑，并获准运行 win\v88ctl.ps1（启动/链接/同步/状态）。'
Write-Host '若飞书还没启用: 再双击 "启用OpenClaw飞书-双击我.bat" 填 App ID/Secret。'
Write-Host '测试: 飞书里对蓝一说 "打开V88" —— 它应回一个 trycloudflare 链接。'
