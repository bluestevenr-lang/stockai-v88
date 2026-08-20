# harden_gateway_task.ps1 —— 把 OpenClaw Gateway 计划任务升级为 7x24 稳健版
# 需要管理员：本脚本会自我提权。由 "注册网关任务-双击我.bat" 调起。
# 摘自 install_openclaw_win.ps1 第 234-257 行（S4U + 开机延迟2分钟 + 失败每5分钟重试12次）
$ErrorActionPreference = 'Stop'

# 自我提权
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
           ).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Start-Process powershell -Verb RunAs -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File',"`"$PSCommandPath`""
    exit 0
}

$GatewayScript = Join-Path $env:USERPROFILE '.openclaw\gateway.cmd'
if (-not (Test-Path $GatewayScript)) { throw "找不到 $GatewayScript —— 先确认 OpenClaw 主体已装好。" }

$TaskName  = 'OpenClaw Gateway'
$Me        = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$Action    = New-ScheduledTaskAction -Execute "$env:SystemRoot\System32\cmd.exe" -Argument "/c `"$GatewayScript`""
$Trigger   = New-ScheduledTaskTrigger -AtStartup
$Trigger.Delay = 'PT2M'
$Principal = New-ScheduledTaskPrincipal -UserId $Me -LogonType S4U -RunLevel Highest
$Settings  = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
    -RestartCount 12 -RestartInterval (New-TimeSpan -Minutes 5) `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger `
    -Principal $Principal -Settings $Settings -Force `
    -Description 'V88 OpenClaw Gateway: 开机+2分钟启动，无需用户登录，失败每5分钟重试。' | Out-Null

Restart-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
Start-Sleep -Seconds 5
$info = Get-ScheduledTask -TaskName $TaskName
Write-Host ""
Write-Host "===== 网关任务已加固 =====" -ForegroundColor Green
Write-Host "任务名: $($info.TaskName)  状态: $($info.State)"
Write-Host "开机自启+2分钟 | 无需登录(S4U) | 崩溃每5分钟重试x12"
Write-Host "现在起重启电脑也不用管它了。"
Read-Host '按回车关闭'
