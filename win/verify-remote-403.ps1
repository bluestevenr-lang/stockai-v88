<#
  V88 遥控常驻 · 会话层 403 三条验收判据
  用法: powershell -NoProfile -ExecutionPolicy Bypass -File win\verify-remote-403.ps1
  判据(缺一不可): Heartbeat sent > 0 / worker 403 = 0 / bridge 存活 > 140s
  背景与证据链见 win\TROUBLESHOOTING-403.md
  注: 本脚本按 PowerShell 5.1 语法写(不用 if 表达式 / 三元 / ??)
#>
$ErrorActionPreference = 'Continue'
$ymd  = Get-Date -Format yyyyMMdd
$logs = Join-Path $env:USERPROFILE 'Desktop\StockAI\win\logs'
function Mark($ok) { if ($ok) { 'PASS' } else { 'FAIL' } }

Write-Output "==== V88 remote-control 403 verify @ $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===="

# 1) bridge 进程、存活时长、所在会话
$bridge = @(Get-CimInstance Win32_Process -Filter "Name='claude.exe'" |
            Where-Object { $_.CommandLine -match 'remote-control' })
$aliveOk = $false
if ($bridge.Count -eq 0) {
  Write-Output "FAIL  bridge: 没有 remote-control 进程"
} else {
  foreach ($b in $bridge) {
    $up  = [math]::Round(((Get-Date) - $b.CreationDate).TotalSeconds)
    $sid = (Get-Process -Id $b.ProcessId).SessionId
    $m = 'WAIT'
    if ($up -gt 140) { $m = 'PASS'; $aliveOk = $true }
    Write-Output "$m  bridge PID $($b.ProcessId)  存活 ${up}s  SessionId=$sid   (要 >140s)"
  }
}

# 2) 子会话日志: 心跳 / worker 403 / 自杀退出
$cse = Get-ChildItem (Join-Path $logs "rc_debug_$ymd*-cse_*.log") -ErrorAction SilentlyContinue |
       Sort-Object LastWriteTime | Select-Object -Last 1
if (-not $cse) {
  Write-Output "FAIL  没找到今天的子会话日志 rc_debug_$ymd*-cse_*.log"
  exit 1
}
Write-Output "      子会话日志: $($cse.Name)  (只看末 400 行,避开白天已死的轮次)"
$recent = Get-Content $cse.FullName -Tail 400
$hb   = @($recent | Select-String 'Heartbeat sent').Count
$w403 = @($recent | Select-String 'worker.*returned 403|PUT worker \(init\) returned 403').Count
$fail = @($recent | Select-String 'consecutive auth failures').Count
Write-Output "$(Mark ($hb -gt 0))  Heartbeat sent = $hb   (要 > 0)"
Write-Output "$(Mark ($w403 -eq 0))  worker 403     = $w403   (要 = 0)"
Write-Output "$(Mark ($fail -eq 0))  auth-fail exit = $fail   (要 = 0)"

# 3) 任务的启动方式(本次定位的关键项)
$t = Get-ScheduledTask -TaskName 'V88-遥控常驻' -ErrorAction SilentlyContinue
if ($t) {
  $trg = ($t.Triggers | ForEach-Object { $_.CimClass.CimClassName }) -join ','
  Write-Output "      任务: LogonType=$($t.Principal.LogonType)  触发器=$trg  State=$($t.State)"
  Write-Output "      注: 经任务计划启动实测全死(S4U/Password/InteractiveToken 三种都试过);"
  Write-Output "          交互 shell 启动 4/4 通。详见 win\TROUBLESHOOTING-403.md"
}

$ok = $aliveOk -and ($hb -gt 0) -and ($w403 -eq 0) -and ($fail -eq 0)
Write-Output ''
if ($ok) { Write-Output '==== 结果: PASS (三条判据全过) ====' }
else     { Write-Output '==== 结果: FAIL / 未达标 ====' }
if (-not $ok) { exit 1 }
