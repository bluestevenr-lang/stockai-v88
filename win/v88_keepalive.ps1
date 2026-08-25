# V88 24/7守护：阻止系统进入睡眠，并持续拉起Streamlit与Cloudflare临时隧道。
param([switch]$Once)
$ErrorActionPreference = 'Continue'

$Ctl = 'C:\Users\admin\Desktop\StockAI\win\v88ctl.ps1'
$ToolsDir = Join-Path $env:USERPROFILE '.openclaw\tools'
$Log = Join-Path $ToolsDir 'v88-keepalive.log'
New-Item -ItemType Directory -Force -Path $ToolsDir | Out-Null

Add-Type @'
using System;
using System.Runtime.InteropServices;
public static class V88PowerGuard {
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern uint SetThreadExecutionState(uint esFlags);
}
'@

# ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED。
# 不含DISPLAY_REQUIRED，所以屏幕仍可正常熄灭，但Windows不能休眠。
$KeepAwakeFlags = [Convert]::ToUInt32('80000041', 16)
$Loop = 0

function Write-V88Log([string]$Message) {
    Add-Content -LiteralPath $Log -Encoding UTF8 -Value (
        "{0} {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Message
    )
}

Write-V88Log '守护启动：保持系统运行并监控V88/隧道。'
while ($true) {
    $PowerResult = [V88PowerGuard]::SetThreadExecutionState($KeepAwakeFlags)
    if ($Loop -eq 0) {
        Write-V88Log ("系统持续运行请求：{0}" -f $(if ($PowerResult) { '已生效' } else { '失败' }))
    }
    try {
        $GatewayTask = Get-ScheduledTask -TaskName 'OpenClaw Gateway' -ErrorAction Stop
        if ($GatewayTask.State -ne 'Running') {
            Start-ScheduledTask -TaskName 'OpenClaw Gateway'
            Write-V88Log 'OpenClaw网关已自动拉起。'
        }
    } catch {
        Write-V88Log ("OpenClaw网关检查失败：{0}" -f $_.Exception.Message)
    }
    try {
        $Result = & $Ctl url 2>&1
        if ($Loop % 10 -eq 0) {
            Write-V88Log (($Result | Select-Object -Last 2) -join ' | ')
        }
    } catch {
        Write-V88Log ("自愈失败：{0}" -f $_.Exception.Message)
    }
    if ($Loop % 10 -eq 0) {
        try {
            $SyncResult = & $Ctl sync 2>&1
            Write-V88Log ("GitHub/持仓/记忆同步：{0}" -f (($SyncResult | Select-Object -Last 3) -join ' | '))
        } catch {
            Write-V88Log ("GitHub/持仓/记忆同步失败，保留最后快照：{0}" -f $_.Exception.Message)
        }
    }
    $Loop += 1
    if ($Once) { break }
    Start-Sleep -Seconds 60
}
