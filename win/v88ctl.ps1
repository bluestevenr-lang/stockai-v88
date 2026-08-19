# V88 遥控 wrapper —— OpenClaw 代理唯一允许执行的脚本
# 子命令: start(启动V88) / url(生成手机临时访问链接) / sync(git同步) / status(状态)
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('start','url','sync','status')]
    [string]$Command
)
$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
$ToolsDir = Join-Path $env:USERPROFILE '.openclaw\tools'
$CfExe    = Join-Path $ToolsDir 'cloudflared.exe'
$CfLog    = Join-Path $ToolsDir 'cloudflared.log'

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
    Start-Process python -WindowStyle Minimized -WorkingDirectory $RepoRoot -ArgumentList @(
        '-m','streamlit','run','app_v88_integrated.py',
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

switch ($Command) {
    'start'  { Start-V88 }
    'url'    {
        Start-V88 | Out-Null
        $u = New-TunnelUrl
        Write-Output "手机访问链接: $u"
        Write-Output '⚠️ 链接即钥匙，请勿转发；重启隧道后旧链接失效。'
    }
    'sync'   {
        $out = & git -C $RepoRoot pull --ff-only 2>&1
        Write-Output ($out | Select-Object -Last 5 | Out-String).Trim()
    }
    'status' {
        $v88 = if (Test-V88Up) { '运行中' } else { '未运行' }
        $u = Get-TunnelUrl
        $gw = (Get-ScheduledTask -TaskName 'OpenClaw Gateway' -ErrorAction SilentlyContinue).State
        Write-Output "V88页面: $v88"
        Write-Output "隧道链接: $(if ($u) { $u } else { '无' })"
        Write-Output "OpenClaw网关: $gw"
    }
}
