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
$bridgeStartUtc = $null
if ($bridge.Count -eq 0) {
  Write-Output "FAIL  bridge: 没有 remote-control 进程"
} else {
  foreach ($b in $bridge) {
    $up  = [math]::Round(((Get-Date) - $b.CreationDate).TotalSeconds)
    $sid = (Get-Process -Id $b.ProcessId).SessionId
    $m = 'WAIT'
    if ($up -gt 140) { $m = 'PASS'; $aliveOk = $true }
    # 父进程链是本次定位的关键项: explorer 起的通, svchost(Schedule) 起的死
    $ppid = $b.ParentProcessId
    $pp   = Get-CimInstance Win32_Process -Filter "ProcessId=$ppid" -ErrorAction SilentlyContinue
    $gp   = $null
    if ($pp) { $gp = Get-CimInstance Win32_Process -Filter "ProcessId=$($pp.ParentProcessId)" -ErrorAction SilentlyContinue }
    $chain = 'unknown'
    if ($pp -and $gp) { $chain = "$($gp.Name) -> $($pp.Name) -> claude.exe" }
    Write-Output "$m  bridge PID $($b.ProcessId)  存活 ${up}s  SessionId=$sid   (要 >140s)"
    Write-Output "      父进程链: $chain   (要以 explorer.exe 打头; svchost.exe=任务计划=已知必死)"
    $cd = $b.CreationDate.ToUniversalTime()
    if ($bridgeStartUtc -eq $null -or $cd -lt $bridgeStartUtc) { $bridgeStartUtc = $cd }
  }
}

# 2) 子会话日志: 心跳 / worker 403 / 自杀退出
$cse = Get-ChildItem (Join-Path $logs "rc_debug_$ymd*-cse_*.log") -ErrorAction SilentlyContinue |
       Sort-Object LastWriteTime | Select-Object -Last 1
if (-not $cse) {
  Write-Output "FAIL  没找到今天的子会话日志 rc_debug_$ymd*-cse_*.log"
  exit 1
}
# 只取【当前 bridge 起来之后】的行。
# 教训 2026-07-30 21:00: 原先用 -Tail 400 切窗口。断电重启后这 400 行里混进了
# 重启【之前】那一轮(已死)的 worker 403,把一轮完全健康的会话误报成 FAIL
# (实测 hb=22 / w403=43,而按时间切出来的当轮真值是 hb=23 / w403=0)。
# 判据必须按【时间】切,不能按行数切 —— 同一个 cse 日志文件是跨轮次追加的。
$lines  = Get-Content $cse.FullName
$recent = @()
if ($bridgeStartUtc -eq $null) {
  $recent = @($lines | Select-Object -Last 400)
  Write-Output "      子会话日志: $($cse.Name)  (无 bridge 进程,退回看末 400 行)"
} else {
  $cut = $bridgeStartUtc.AddSeconds(-5)
  $inWindow = $false
  $ts = [datetime]::MinValue
  foreach ($ln in $lines) {
    if (-not $inWindow -and $ln.Length -ge 24 -and $ln[10] -eq 'T') {
      if ([datetime]::TryParse($ln.Substring(0,24), [ref]$ts)) {
        if ($ts.ToUniversalTime() -ge $cut) { $inWindow = $true }
      }
    }
    if ($inWindow) { $recent += $ln }
  }
  Write-Output "      子会话日志: $($cse.Name)  (只取 $($cut.ToString('yyyy-MM-ddTHH:mm:ss'))Z 之后 = 当前 bridge 这一轮,共 $($recent.Count) 行)"
}
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
