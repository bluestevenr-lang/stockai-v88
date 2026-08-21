# setup_k3_remote.ps1 —— 把 Win 的 v88-mobile 升级为: K3大脑 + V88遥控能力
# 用法: 双击 "升级K3遥控-双击我.bat"，首次粘贴 Moonshot API Key；重复运行自动跳过已有配置
# 现行口径（2026-08-20）：仅接入 Kimi Code 订阅 k3-256k，不再写入 Moonshot 按量 API。
$ErrorActionPreference = 'Stop'
function Log($m) { Write-Host "[K3升级] $m" -ForegroundColor Cyan }

# --- 0. 定位 openclaw CLI ---
# 0.1 新开窗口可能继承残缺 PATH：用注册表 Machine+User PATH 重建（与安装脚本同一招）
$env:Path = "{0};{1}" -f [Environment]::GetEnvironmentVariable('Path','Machine'),
                          [Environment]::GetEnvironmentVariable('Path','User')
$Openclaw = (Get-Command openclaw -ErrorAction SilentlyContinue).Source

# 0.2 常规与包管理器候选
$cand = @(
    "$env:APPDATA\npm\openclaw.cmd",
    "$env:LOCALAPPDATA\npm\openclaw.cmd",
    "$env:ProgramFiles\nodejs\openclaw.cmd",
    "${env:ProgramFiles(x86)}\nodejs\openclaw.cmd",
    "$env:LOCALAPPDATA\Volta\bin\openclaw.cmd"
)
if (-not $Openclaw) {
    foreach ($c in $cand) { if (Test-Path $c) { $Openclaw = $c; break } }
}

# 0.3 从正在运行的网关反解：gateway.cmd 里写着 node/openclaw 的真实路径
if (-not $Openclaw) {
    $gw = Join-Path $env:USERPROFILE '.openclaw\gateway.cmd'
    if (Test-Path $gw) {
        $txt = Get-Content $gw -Raw
        $mc = [regex]::Match($txt, '"([^"]+openclaw[^"]*?\.cmd)"')
        if ($mc.Success -and (Test-Path $mc.Groups[1].Value)) {
            $Openclaw = $mc.Groups[1].Value
        } else {
            # 无引号路径兜底（C:\Users\admin 无空格时 gateway.cmd 可能不加引号）
            $mc2 = [regex]::Match($txt, '([A-Za-z]:\\[^\s"]+openclaw[^\s"]*?\.cmd)')
            if ($mc2.Success -and (Test-Path $mc2.Groups[1].Value)) {
                $Openclaw = $mc2.Groups[1].Value
            }
        }
        if (-not $Openclaw) {
            $mm = [regex]::Matches($txt, '"([^"]+)"') | ForEach-Object { $_.Groups[1].Value }
            $mm += [regex]::Matches($txt, '([A-Za-z]:\\[^\s"]+\.(?:exe|js))') | ForEach-Object { $_.Groups[1].Value }
            $nodeExe = $mm | Where-Object { $_ -match 'node\.exe$' } | Select-Object -First 1
            $cliJs   = $mm | Where-Object { $_ -match '\.js$' } | Select-Object -First 1
            if ($nodeExe -and $cliJs -and (Test-Path $nodeExe) -and (Test-Path $cliJs)) {
                $shim = Join-Path $env:TEMP 'openclaw-shim.cmd'
                Set-Content -Path $shim -Value "@echo off`r`n`"$nodeExe`" `"$cliJs`" %*" -Encoding ascii
                $Openclaw = $shim
            }
        }
    }
}
if (-not $Openclaw) {
    Write-Host '已搜索位置（请拍照发给 Kimi）：' -ForegroundColor Yellow
    $cand | ForEach-Object { Write-Host "  $_" }
    Write-Host "  $env:USERPROFILE\.openclaw\gateway.cmd（反解）"
    throw '找不到 openclaw CLI。'
}
Log "openclaw: $Openclaw"

# --- 1. 读取/复用 Kimi Code 订阅密钥（只进入 OpenClaw 认证库，永不入 git/config） ---
$cfgPath = Join-Path $env:USERPROFILE '.openclaw\openclaw.json'
$existing = Get-Content $cfgPath -Raw -Encoding UTF8 | ConvertFrom-Json
function Find-KimiSubscriptionKey {
    if ($env:KIMI_CODE_API_KEY -and $env:KIMI_CODE_API_KEY.StartsWith('sk-kimi-')) {
        return $env:KIMI_CODE_API_KEY
    }
    $known = @(
        (Join-Path $env:USERPROFILE '.kimi\kimi-claw\openclaw.json'),
        (Join-Path $env:USERPROFILE '.openclaw\openclaw.json')
    )
    foreach ($p in $known) {
        if (-not (Test-Path $p)) { continue }
        $raw = Get-Content $p -Raw -Encoding UTF8
        $m = [regex]::Match($raw, 'sk-kimi-[A-Za-z0-9_-]{16,}')
        if ($m.Success) { return $m.Value }
    }
    return $null
}
$KimiSubscriptionKey = Find-KimiSubscriptionKey
if ($KimiSubscriptionKey) {
    Log '检测到 Kimi Code 订阅认证，跳过输入。'
} else {
    $sec = Read-Host '请输入 Kimi Code 订阅密钥（必须以 sk-kimi- 开头）' -AsSecureString
    $KimiSubscriptionKey = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec))
}
if (-not $KimiSubscriptionKey.StartsWith('sk-kimi-')) {
    throw '这不是 Kimi Code 订阅密钥；已拒绝写入，避免误走 Moonshot 按量 API。'
}

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
$agentsJson = (& $Openclaw agents list --json 2>$null) -join "`n"
$agents = @($agentsJson | ConvertFrom-Json)
if (-not ($agents | Where-Object { $_.id -eq 'v88-mobile' })) { throw '找不到 v88-mobile 代理，请先运行安装脚本。' }
$idx = -1
for ($i=0; $i -lt $existing.agents.list.Count; $i++) { if ($existing.agents.list[$i].id -eq 'v88-mobile') { $idx = $i; break } }
if ($idx -lt 0) { throw '配置里找不到 v88-mobile 代理。' }

# --- 5. 写入: K3-256K模型 + 订阅认证 + exec放行(仅限v88ctl) ---
$K3Model = @(@{
    id = 'k3-256k'; name = 'Kimi K3-256K (subscription)'; reasoning = $true
    input = @('text','image')
    contextWindow = 262144; maxTokens = 32768
})
$ToolPolicy = @{
    profile = 'coding'
    allow   = @('read','exec','process')
    deny    = @('write','edit','apply_patch','browser','web_search','web_fetch','message',
                'sessions_spawn','sessions_send','cron','gateway','nodes','computer')
    codeMode = $false
    elevated = @{ enabled = $false }
    exec     = @{ mode = 'full' }   # OpenClaw 2026.7.1-2 有效枚举: deny/allowlist/ask/auto/full；无 'allow'
    fs       = @{ workspaceOnly = $false }
    message  = @{
        allowCrossContextSend = $false
        crossContext = @{ allowWithinProvider = $false; allowAcrossProviders = $false }
        broadcast = @{ enabled = $false }
    }
}
$Batch = @(
    @{ path = 'models.providers.kimi-coding'; value = @{
        baseUrl = 'https://api.kimi.com/coding/v1'
        api     = 'openai-completions'
        models  = $K3Model
    } },
    @{ path = "agents.list[$idx].model"; value = @{ primary = 'kimi-coding/k3-256k'; fallbacks = @() } },
    @{ path = "agents.list[$idx].tools"; value = $ToolPolicy },
    @{ path = 'agents.defaults.models["kimi-coding/k3-256k"].agentRuntime'; value = @{ id = 'openclaw' } }
)
$BatchPath = Join-Path $env:TEMP 'openclaw_k3_batch.json'
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($BatchPath, ($Batch | ConvertTo-Json -Depth 20), $Utf8NoBom)
try {
    Log '写入 K3-256K 模型与遥控权限 ...'
    $KimiSubscriptionKey | & $Openclaw models auth --agent v88-mobile paste-api-key `
        --provider kimi-coding --profile-id kimi-coding:v88-subscription | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "订阅认证写入失败 ($LASTEXITCODE)" }
    & $Openclaw config set --batch-file $BatchPath --merge | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "config set 失败 ($LASTEXITCODE)" }
} finally { Remove-Item -Force $BatchPath -ErrorAction SilentlyContinue }
& $Openclaw config validate | Out-Host

# --- 6. 验证当前代理（新版 OpenClaw 热加载，无需重启网关） ---
& $Openclaw models status --agent v88-mobile --json | Out-Host

Write-Host ''
Write-Host '===== 完成 =====' -ForegroundColor Green
Write-Host '蓝一已切换为 Kimi Code 订阅 K3-256K，并获准运行 win\v88ctl.ps1（启动/链接/同步/状态）。'
Write-Host '若飞书还没启用: 再双击 "启用OpenClaw飞书-双击我.bat" 填 App ID/Secret。'
Write-Host '测试: 飞书里对蓝一说 "打开V88" —— 它应回一个 trycloudflare 链接。'
