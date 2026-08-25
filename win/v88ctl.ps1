# V88 遥控 wrapper —— OpenClaw 代理唯一允许执行的脚本
# 子命令: start(启动V88) / url(生成手机临时访问链接) / sync(git同步) / status(状态)
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('start','url','sync','status')]
    [string]$Command
)
$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
$ReportRoot = Join-Path $env:USERPROFILE 'Desktop\ai-daily-report-v2'
$ToolsDir = Join-Path $env:USERPROFILE '.openclaw\tools'
$CfExe    = Join-Path $ToolsDir 'cloudflared.exe'
$CfLog    = Join-Path $ToolsDir 'cloudflared.log'
$PyLauncher = 'C:\Users\admin\AppData\Local\Programs\Python\Launcher\py.exe'
$Projection = Join-Path $RepoRoot 'win\openclaw-v88\sync_v88_projection_win.py'
$K3Context = Join-Path $env:USERPROFILE '.openclaw\workspaces\v88-mobile\context'
$GptWorkspace = Join-Path $env:USERPROFILE '.openclaw\workspaces\v88-gpt'
$GptContext = Join-Path $GptWorkspace 'context'
$GptKnowledge = Join-Path $GptWorkspace 'knowledge\v88-claude-memory'

function Test-V88Up {
    try {
        $null = Invoke-WebRequest -Uri 'http://127.0.0.1:8501' -UseBasicParsing -TimeoutSec 4
        return $true
    } catch { return $false }
}

function Start-V88 {
    if (Test-V88Up) { Write-Output 'V88 已在运行 (http://127.0.0.1:8501)'; return }
    $app = Join-Path $RepoRoot 'app_v88_integrated.py'
    if (-not (Test-Path $app)) { throw "找不到 $app" }
    if (-not (Test-Path -LiteralPath $PyLauncher)) { throw "找不到Python启动器 $PyLauncher" }
    Start-Process $PyLauncher -WindowStyle Hidden -WorkingDirectory $RepoRoot -ArgumentList @(
        '-3','-m','streamlit','run','app_v88_integrated.py',
        '--server.address','127.0.0.1','--server.headless','true','--server.port','8501'
    )
    Write-Output 'V88 启动中（首次加载约1-2分钟）...'
    for ($i=0; $i -lt 40; $i++) {
        Start-Sleep -Seconds 3
        if (Test-V88Up) { Write-Output 'V88 已就绪 (http://127.0.0.1:8501)'; return }
    }
    throw 'V88 启动超时（120秒未响应），请人工查看。'
}

function Get-TunnelUrl {
    if (-not (Get-Process cloudflared -ErrorAction SilentlyContinue)) { return $null }
    if (Test-Path $CfLog) {
        $m = Select-String -Path $CfLog -Pattern 'https://[a-z0-9-]+\.trycloudflare\.com' -AllMatches |
             Select-Object -Last 1
        if ($m) { return $m.Matches[0].Value }
    }
    return $null
}

function New-TunnelUrl {
    if (-not (Test-Path $CfExe)) { throw "缺少 $CfExe（请先运行 配置K3遥控.ps1）" }
    $running = Get-Process cloudflared -ErrorAction SilentlyContinue
    if ($running) {
        $u = Get-TunnelUrl
        if ($u) { Write-Output $u; return }
        $running | Stop-Process -Force; Start-Sleep -Seconds 2
    }
    Remove-Item $CfLog -Force -ErrorAction SilentlyContinue
    Start-Process $CfExe -WindowStyle Hidden -ArgumentList @(
        'tunnel','--url','http://127.0.0.1:8501','--logfile',$CfLog,'--loglevel','info'
    )
    for ($i=0; $i -lt 20; $i++) {
        Start-Sleep -Seconds 2
        $u = Get-TunnelUrl
        if ($u) { Write-Output $u; return }
    }
    throw '隧道 URL 生成超时，请人工运行 cloudflared 排查。'
}

function Sync-V88Data {
    if (-not (Test-Path -LiteralPath (Join-Path $ReportRoot '.git'))) {
        throw "找不到 V88 私仓 $ReportRoot"
    }
    if (-not (Test-Path -LiteralPath $Projection)) { throw "找不到投影脚本 $Projection" }
    if (-not (Test-Path -LiteralPath $PyLauncher)) { throw "找不到Python启动器 $PyLauncher" }

    $publicPullOutput = & git -C $RepoRoot pull --ff-only origin main 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "V88 公仓同步失败：$($publicPullOutput | Select-Object -Last 8 | Out-String)"
    }

    $pythonForGit = 'C:/Users/admin/v88env/Scripts/python.exe'
    if (-not (Test-Path -LiteralPath $pythonForGit)) {
        throw "找不到 V88 Python $pythonForGit"
    }
    & git -C $ReportRoot config merge.v88json.name 'V88 JSON newest-timestamp-wins'
    & git -C $ReportRoot config merge.v88json.driver "`"$pythonForGit`" scripts/v88_json_merge.py %O %A %B %P"
    & git -C $ReportRoot config merge.v88jsonl.name 'V88 JSONL lossless-union'
    & git -C $ReportRoot config merge.v88jsonl.driver "`"$pythonForGit`" scripts/v88_jsonl_merge_driver.py %O %A %B %P"

    $pullOutput = & git -C $ReportRoot pull --rebase --autostash origin main 2>&1
    $pullExit = $LASTEXITCODE
    $previousPythonUtf8 = $env:PYTHONUTF8
    $env:PYTHONUTF8 = '1'
    Push-Location $ReportRoot
    try {
        $postPullOutput = & $PyLauncher -3 (Join-Path $ReportRoot 'scripts\v88_json_merge.py') --postpull 2>&1
        $postPullExit = $LASTEXITCODE
    } finally {
        Pop-Location
        $env:PYTHONUTF8 = $previousPythonUtf8
    }
    $unresolved = @(& git -C $ReportRoot diff --name-only --diff-filter=U)
    if ($pullExit -ne 0 -or $postPullExit -ne 0 -or $unresolved.Count -gt 0) {
        throw "V88 私仓安全同步失败：pull=$pullExit postpull=$postPullExit unresolved=$($unresolved.Count)`n$($pullOutput | Select-Object -Last 8 | Out-String)$($postPullOutput | Select-Object -Last 8 | Out-String)"
    }

    foreach ($destination in @($K3Context, $GptContext)) {
        $projectionOutput = & $PyLauncher -3 $Projection --source (Join-Path $ReportRoot 'data') --dest $destination 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "V88 持仓投影失败（$destination）：$($projectionOutput | Out-String)"
        }
    }

    $memorySource = Join-Path $ReportRoot 'claude-memory'
    if (Test-Path -LiteralPath $memorySource) {
        New-Item -ItemType Directory -Force -Path $GptKnowledge | Out-Null
        Get-ChildItem -LiteralPath $memorySource -Force |
            Copy-Item -Destination $GptKnowledge -Recurse -Force
    }

    $portfolioPath = Join-Path $GptContext 'modules\portfolio_pub.json'
    $portfolio = Get-Content -LiteralPath $portfolioPath -Raw | ConvertFrom-Json
    Write-Output (($pullOutput | Select-Object -Last 3 | Out-String).Trim())
    Write-Output "V88 GPT/K3 快照已更新：$(@($portfolio.items).Count) 只持仓，持仓源时间 $($portfolio.updated_at)"
    Write-Output 'Claude 脱敏记忆镜像已同步到 GPT 龙虾知识库。'
}

switch ($Command) {
    'start'  { Start-V88 }
    'url'    {
        Start-V88 | Out-Null
        $u = New-TunnelUrl
        Write-Output "手机访问链接: $u"
        Write-Output '⚠️ 链接即钥匙，请勿转发；重启隧道后旧链接失效。'
    }
    'sync'   { Sync-V88Data }
    'status' {
        $v88 = if (Test-V88Up) { '运行中' } else { '未运行' }
        $u = Get-TunnelUrl
        $gw = (Get-ScheduledTask -TaskName 'OpenClaw Gateway' -ErrorAction SilentlyContinue).State
        Write-Output "V88页面: $v88"
        Write-Output "隧道链接: $(if ($u) { $u } else { '无' })"
        Write-Output "OpenClaw网关: $gw"
    }
}
